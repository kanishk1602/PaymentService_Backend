import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";
import dotenv from "dotenv";

// Load environment variables BEFORE importing gateways
dotenv.config();

import { paymentService } from "./gateways/index.js";

// =============================================================================
// APP SETUP
// =============================================================================

const app = express();
const PORT = process.env.PORT || 3002;

// CORS configuration
const allowedOrigins = (process.env.CORS_ORIGINS || "http://localhost:3000")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(cors({ origin: allowedOrigins, credentials: true }));

// JSON parser with raw body for webhook signature verification
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = buf; }
}));

// =============================================================================
// KAFKA SETUP
// =============================================================================

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: [process.env.KAFKA_BROKER || "localhost:9094"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "payment-group" });

/**
 * Publish a message to a Kafka topic
 */
async function publishToKafka(topic, payload) {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(payload) }],
    });
    console.log(`[Kafka] Published to "${topic}":`, {
      gateway: payload.gateway,
      paymentId: payload.paymentIntentId || payload.paymentId,
      userId: payload.userId,
    });
    return true;
  } catch (error) {
    console.error(`[Kafka] Failed to publish to "${topic}":`, error.message);
    return false;
  }
}

/**
 * Connect to Kafka and set up topics/consumers
 */
async function connectToKafka() {
  const admin = kafka.admin();
  
  try {
    await admin.connect();
    
    // Create required topics if they don't exist
    const existingTopics = await admin.listTopics();
    const requiredTopics = ["order-created", "payment-successful"];
    const topicsToCreate = requiredTopics.filter((t) => !existingTopics.includes(t));
    
    if (topicsToCreate.length > 0) {
      await admin.createTopics({
        topics: topicsToCreate.map((topic) => ({
          topic,
          numPartitions: 1,
          replicationFactor: 1,
        })),
      });
      console.log("[Kafka] Created topics:", topicsToCreate);
    }
    
    await admin.disconnect();

    // Connect producer and consumer
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: "order-created", fromBeginning: true });

    console.log("[Kafka] Connected successfully");

    // Listen for order-created events (for logging/monitoring)
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(`[Kafka] Received from "${topic}":`, data.orderId);
      },
    });
  } catch (error) {
    console.error("[Kafka] Connection failed:", error.message);
    process.exit(1);
  }
}

// =============================================================================
// DEDUPLICATION
// Prevents duplicate Kafka events when both webhook and verify are called
// =============================================================================

const processedPayments = new Set();

function isAlreadyProcessed(paymentId) {
  return processedPayments.has(paymentId);
}

function markAsProcessed(paymentId) {
  processedPayments.add(paymentId);
  // Auto-cleanup after 10 minutes to prevent memory leak
  setTimeout(() => processedPayments.delete(paymentId), 10 * 60 * 1000);
}

/**
 * Publish payment success to Kafka with deduplication
 */
async function publishPaymentSuccess(gateway, paymentData) {
  const paymentId = paymentData.paymentIntentId || paymentData.paymentId || paymentData.orderId;
  
  if (isAlreadyProcessed(paymentId)) {
    console.log(`[Dedup] Already processed payment: ${paymentId}`);
    return false;
  }
  
  markAsProcessed(paymentId);
  const payload = paymentService.buildKafkaPayload(gateway, paymentData);
  return publishToKafka("payment-successful", payload);
}

// =============================================================================
// ROUTES: Stripe
// =============================================================================

/**
 * POST /api/stripe/create-checkout-session
 * Creates a Stripe Checkout session and returns the redirect URL
 */
app.post("/api/stripe/create-checkout-session", async (req, res) => {
  try {
    const { cart, email, userId, successUrl, cancelUrl, currency = "usd" } = req.body;

    if (!cart || cart.length === 0) {
      return res.status(400).json({ success: false, message: "Cart is empty" });
    }

    const result = await paymentService.createOrder("stripe", {
      amount: 0, // Stripe calculates from cart
      currency,
      cart,
      email,
      userId,
      successUrl,
      cancelUrl,
    });

    if (result.success) {
      res.json({ success: true, url: result.data.url });
    } else {
      res.status(400).json({ success: false, message: result.error });
    }
  } catch (error) {
    console.error("[Stripe] Create checkout error:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

/**
 * POST /api/stripe/confirm-checkout
 * Called by client after Stripe redirects back. Verifies and publishes to Kafka.
 */
app.post("/api/stripe/confirm-checkout", async (req, res) => {
  try {
    const { sessionId } = req.body;

    if (!sessionId) {
      return res.status(400).json({ success: false, message: "sessionId is required" });
    }

    const result = await paymentService.verifyPayment("stripe", { sessionId });

    if (result.success && result.verified) {
      // Publish to Kafka (with deduplication in case webhook already fired)
      await publishPaymentSuccess("stripe", result.data);
      res.json({ success: true, message: "Payment confirmed" });
    } else {
      res.status(400).json({ success: false, message: result.error || "Payment not completed" });
    }
  } catch (error) {
    console.error("[Stripe] Confirm checkout error:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

/**
 * POST /api/stripe/webhook
 * Handles Stripe webhook events (payment confirmations)
 */
app.post("/api/stripe/webhook", async (req, res) => {
  try {
    const result = await paymentService.handleWebhook("stripe", req);

    if (result.success && result.event === "payment.success" && result.data) {
      await publishPaymentSuccess("stripe", result.data);
    }

    res.json({ received: true });
  } catch (error) {
    console.error("[Stripe] Webhook error:", error.message);
    res.status(400).json({ error: error.message });
  }
});

// =============================================================================
// ROUTES: Razorpay
// =============================================================================

/**
 * POST /api/orders
 * Creates a Razorpay order
 */
app.post("/api/orders", async (req, res) => {
  try {
    const { amount, currency = "INR", notes = {} } = req.body;

    if (!amount || amount <= 0) {
      return res.status(400).json({ success: false, message: "Valid amount is required" });
    }

    const result = await paymentService.createOrder("razorpay", {
      amount,
      currency,
      cart: notes.items ? JSON.parse(notes.items) : [],
      email: notes.email || "",
      userId: notes.userId || "anonymous",
    });

    if (result.success) {
      res.json({
        success: true,
        order: {
          id: result.data.orderId,
          amount: result.data.amount,
          currency: result.data.currency,
          receipt: result.data.receipt,
        },
      });
    } else {
      res.status(400).json({ success: false, message: result.error });
    }
  } catch (error) {
    console.error("[Razorpay] Create order error:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

/**
 * POST /api/verify-payment
 * Verifies Razorpay payment signature and publishes to Kafka
 */
app.post("/api/verify-payment", async (req, res) => {
  try {
    const { orderId, paymentId, signature, email, userId, cart } = req.body;

    if (!orderId || !paymentId || !signature) {
      return res.status(400).json({ success: false, message: "Missing required fields" });
    }

    const result = await paymentService.verifyPayment("razorpay", {
      orderId,
      paymentId,
      signature,
      email,
      userId,
      cart,
    });

    if (result.success && result.verified) {
      // Publish to Kafka
      await publishPaymentSuccess("razorpay", result.data);
      res.json({ success: true, message: "Payment verified" });
    } else {
      res.status(400).json({ success: false, message: result.error || "Invalid signature" });
    }
  } catch (error) {
    console.error("[Razorpay] Verify payment error:", error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// =============================================================================
// ROUTES: Generic (for future gateways)
// =============================================================================

/**
 * GET /api/payment-gateways
 * Lists all available payment gateways
 */
app.get("/api/payment-gateways", (_req, res) => {
  res.json({
    success: true,
    gateways: paymentService.listGateways(),
  });
});

/**
 * POST /api/payments/webhook/:gateway
 * Generic webhook handler for any gateway
 */
app.post("/api/payments/webhook/:gateway", async (req, res) => {
  try {
    const { gateway } = req.params;
    const result = await paymentService.handleWebhook(gateway, req);

    if (result.success && result.event === "payment.success" && result.data) {
      await publishPaymentSuccess(gateway, result.data);
    }

    res.json({ received: true });
  } catch (error) {
    console.error(`[Webhook] ${req.params.gateway} error:`, error.message);
    res.status(500).json({ error: error.message });
  }
});

// =============================================================================
// HEALTH & INFO
// =============================================================================

app.get("/health", (_req, res) => {
  res.json({ status: "ok", gateways: paymentService.listGateways() });
});

app.get("/", (_req, res) => {
  res.json({
    service: "Payment Service",
    version: "2.0.0",
    gateways: paymentService.listGateways(),
    endpoints: {
      stripe: {
        createCheckout: "POST /api/stripe/create-checkout-session",
        confirmCheckout: "POST /api/stripe/confirm-checkout",
        webhook: "POST /api/stripe/webhook",
      },
      razorpay: {
        createOrder: "POST /api/orders",
        verifyPayment: "POST /api/verify-payment",
      },
      generic: {
        listGateways: "GET /api/payment-gateways",
        webhook: "POST /api/payments/webhook/:gateway",
      },
    },
  });
});

// =============================================================================
// ERROR HANDLER & SERVER START
// =============================================================================

app.use((err, _req, res, _next) => {
  console.error("[Error]", err.message);
  res.status(500).json({ success: false, error: err.message });
});

app.listen(PORT, () => {
  console.log(`\n🚀 Payment Service running on port ${PORT}`);
  console.log("📋 Available gateways:");
  paymentService.listGateways().forEach((g) => {
    const status = g.available ? "✅" : "❌";
    console.log(`   ${status} ${g.name} (${g.id})`);
  });
  connectToKafka();
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("\nShutting down...");
  await producer.disconnect();
  await consumer.disconnect();
  process.exit(0);
});
