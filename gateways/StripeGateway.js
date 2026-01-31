import Stripe from 'stripe';
import dotenv from 'dotenv';
import PaymentGateway from './PaymentGateway.js';

// Ensure env vars are loaded
dotenv.config();

/**
 * Stripe Payment Gateway Implementation
 * Implements the PaymentGateway interface for Stripe
 */

//strategy design pattern - optimal for payment gateways

export class StripeGateway extends PaymentGateway {
  constructor(config = {}) {
    super(config);
    
    this.secretKey = config.secretKey || process.env.STRIPE_SECRET_KEY;
    this.webhookSecret = config.webhookSecret || process.env.STRIPE_WEBHOOK_SECRET;
    this.successUrl = config.successUrl || process.env.CLIENT_SUCCESS_URL || 'http://localhost:3000/orders';
    this.cancelUrl = config.cancelUrl || process.env.CLIENT_CANCEL_URL || 'http://localhost:3000';
    
    if (this.isAvailable()) {
      this.client = new Stripe(this.secretKey, {
        apiVersion: '2023-10-16',
      });
    }
  }

  getInfo() {
    return {
      id: 'stripe',
      name: 'Stripe',
      currencies: ['USD', 'EUR', 'GBP', 'INR', 'CAD', 'AUD'],
      available: this.isAvailable(),
    };
  }

  isAvailable() {
    return !!this.secretKey;
  }

  /**
   * Create a Stripe Checkout Session
   */
  async createOrder({ amount, currency = 'inr', cart, email, userId, metadata = {}, successUrl, cancelUrl }) {
    if (!this.isAvailable()) {
      return { success: false, error: 'Stripe is not configured' };
    }

    try {
      // Validate cart
      if (!Array.isArray(cart) || cart.length === 0) {
        return { success: false, error: 'Cart is empty' };
      }

      // Build line items for Stripe Checkout
      const line_items = cart.map(item => ({
        price_data: {
          currency: currency.toLowerCase(),
          product_data: { 
            name: item.name || 'Item',
            // Can add images, description here
          },
          unit_amount: Math.round((item.price || 0) * 100), // Convert to cents
        },
        quantity: item.quantity || 1,
      }));

      // Create Checkout Session
      // Build success URL with session_id parameter
      let finalSuccessUrl;
      if (successUrl) {
        if (successUrl.includes('{CHECKOUT_SESSION_ID}')) {
          finalSuccessUrl = successUrl;
        } else {
          // Append session_id, handling existing query params
          const separator = successUrl.includes('?') ? '&' : '?';
          finalSuccessUrl = `${successUrl}${separator}session_id={CHECKOUT_SESSION_ID}`;
        }
      } else {
        finalSuccessUrl = `${this.successUrl}?session_id={CHECKOUT_SESSION_ID}`;
      }

      console.log('[Stripe] Creating checkout session with successUrl:', finalSuccessUrl);

      const session = await this.client.checkout.sessions.create({
        payment_method_types: ['card'],
        line_items,
        mode: 'payment',
        success_url: finalSuccessUrl,
        cancel_url: cancelUrl || this.cancelUrl,
        customer_email: email || undefined,
        metadata: {
          userId: userId || 'anonymous',
          email: email || '',
          cartItems: JSON.stringify(cart),
          ...metadata,
        },
      });

      return {
        success: true,
        data: {
          gateway: 'stripe',
          sessionId: session.id,
          url: session.url, // Redirect URL for Stripe Checkout
        },
      };
    } catch (error) {
      console.error('Stripe createOrder error:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Create a Payment Intent (alternative to Checkout Session)
   */
  async createPaymentIntent({ amount, currency = 'inr', email, userId, cart, metadata = {} }) {
    if (!this.isAvailable()) {
      return { success: false, error: 'Stripe is not configured' };
    }

    try {
      const paymentIntent = await this.client.paymentIntents.create({
        amount: Math.round(amount * 100),
        currency: currency.toLowerCase(),
        automatic_payment_methods: { enabled: true },
        metadata: {
          userId: userId || 'anonymous',
          email: email || '',
          cartItems: JSON.stringify(cart || []),
          ...metadata,
        },
      });

      return {
        success: true,
        data: {
          gateway: 'stripe',
          paymentIntentId: paymentIntent.id,
          clientSecret: paymentIntent.client_secret,
        },
      };
    } catch (error) {
      console.error('Stripe createPaymentIntent error:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Verify payment by retrieving the session or payment intent
   */
  async verifyPayment({ sessionId, paymentIntentId, email, userId, cart }) {
    if (!this.isAvailable()) {
      return { success: false, verified: false, error: 'Stripe is not configured' };
    }

    try {
      let paymentData = null;

      if (sessionId) {
        // Verify via Checkout Session
        const session = await this.client.checkout.sessions.retrieve(sessionId, {
          expand: ['payment_intent'],
        });

        console.log('[Stripe] Session retrieved:', {
          id: session.id,
          customer_email: session.customer_email,
          metadata_email: session.metadata?.email,
          status: session.payment_status,
        });

        const pi = session.payment_intent;
        if (pi && pi.status === 'succeeded') {
          paymentData = {
            gateway: 'stripe',
            sessionId: session.id,
            paymentIntentId: pi.id,
            amount: pi.amount,
            currency: pi.currency,
            email: session.customer_email || session.metadata?.email || email || '',
            userId: session.metadata?.userId || userId || 'anonymous',
            cart: JSON.parse(session.metadata?.cartItems || '[]') || cart || [],
          };
        }
      } else if (paymentIntentId) {
        // Verify via Payment Intent
        const pi = await this.client.paymentIntents.retrieve(paymentIntentId);
        
        if (pi.status === 'succeeded') {
          paymentData = {
            gateway: 'stripe',
            paymentIntentId: pi.id,
            amount: pi.amount,
            currency: pi.currency,
            email: pi.metadata?.email || email || '',
            userId: pi.metadata?.userId || userId || 'anonymous',
            cart: JSON.parse(pi.metadata?.cartItems || '[]') || cart || [],
          };
        }
      }

      if (paymentData) {
        return {
          success: true,
          verified: true,
          data: paymentData,
        };
      } else {
        return {
          success: true,
          verified: false,
          error: 'Payment not completed',
        };
      }
    } catch (error) {
      console.error('Stripe verifyPayment error:', error);
      return { success: false, verified: false, error: error.message };
    }
  }

  /**
   * Handle Stripe webhook events
   */
  async handleWebhook(req) {
    if (!this.webhookSecret) {
      console.log('Stripe webhook secret not configured');
      return { success: false, event: null, error: 'Webhook secret not configured' };
    }

    try {
      const sig = req.headers['stripe-signature'];
      let event;

      try {
        event = this.client.webhooks.constructEvent(req.rawBody, sig, this.webhookSecret);
      } catch (err) {
        console.error('Stripe webhook signature verification failed:', err.message);
        return { success: false, event: null, error: `Webhook signature verification failed: ${err.message}` };
      }

      console.log(`Stripe webhook received: ${event.type}`);

      // Only handle checkout.session.completed for the Checkout flow
      // This event contains the session metadata (userId, email, cart)
      if (event.type === 'checkout.session.completed') {
        const session = event.data.object;
        
        try {
          // Retrieve full session with payment intent expanded
          const fullSession = await this.client.checkout.sessions.retrieve(session.id, {
            expand: ['payment_intent'],
          });
          
          const pi = fullSession.payment_intent;

          console.log('Checkout session completed:', {
            sessionId: session.id,
            paymentIntentId: pi?.id,
            status: pi?.status,
            email: fullSession.metadata?.email,
            userId: fullSession.metadata?.userId,
          });

          if (pi && pi.status === 'succeeded') {
            return {
              success: true,
              event: 'payment.success',
              data: {
                gateway: 'stripe',
                sessionId: session.id,
                paymentIntentId: pi.id,
                amount: pi.amount,
                currency: pi.currency,
                email: fullSession.metadata?.email || '',
                userId: fullSession.metadata?.userId || 'anonymous',
                cart: JSON.parse(fullSession.metadata?.cartItems || '[]'),
              },
            };
          }
        } catch (e) {
          console.error('Error processing checkout.session.completed:', e);
          return { success: false, event: event.type, error: e.message };
        }
      }

      // For other events, just acknowledge receipt
      // (payment_intent.succeeded, charge.succeeded are skipped to avoid duplicates)
      if (event.type === 'payment_intent.succeeded' || event.type === 'charge.succeeded') {
        console.log(`${event.type} received - skipping (handled by checkout.session.completed)`);
      }

      if (event.type === 'payment_intent.payment_failed') {
        const pi = event.data.object;
        return {
          success: true,
          event: 'payment.failed',
          data: {
            gateway: 'stripe',
            paymentIntentId: pi.id,
            error: pi.last_payment_error?.message || 'Payment failed',
          },
        };
      }

      return { success: true, event: event.type, data: null };
    } catch (error) {
      console.error('Stripe handleWebhook error:', error);
      return { success: false, event: null, error: error.message };
    }
  }

  /**
   * Build Kafka payload for Stripe payments
   */
  buildKafkaPayload(paymentData) {
    return {
      gateway: 'stripe',
      paymentIntentId: paymentData.paymentIntentId,
      sessionId: paymentData.sessionId,
      amount: paymentData.amount,
      currency: paymentData.currency,
      status: 'completed',
      timestamp: new Date().toISOString(),
      email: paymentData.email || '',
      userId: paymentData.userId || 'anonymous',
      cart: paymentData.cart || [],
    };
  }
}

export default StripeGateway;
