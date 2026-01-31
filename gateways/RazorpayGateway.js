import Razorpay from 'razorpay';
import crypto from 'crypto';
import dotenv from 'dotenv';
import PaymentGateway from './PaymentGateway.js';

// Ensure env vars are loaded
dotenv.config();

/**
 * Razorpay Payment Gateway Implementation
 * Implements the PaymentGateway interface for Razorpay
 */
export class RazorpayGateway extends PaymentGateway {
  constructor(config = {}) {
    super(config);
    
    this.keyId = config.keyId || process.env.RAZORPAY_KEY_ID;
    this.keySecret = config.keySecret || process.env.RAZORPAY_KEY_SECRET;
    
    if (this.isAvailable()) {
      this.client = new Razorpay({
        key_id: this.keyId,
        key_secret: this.keySecret,
      });
    }
  }

  getInfo() {
    return {
      id: 'razorpay',
      name: 'Razorpay',
      currencies: ['INR'],
      available: this.isAvailable(),
    };
  }

  isAvailable() {
    return !!(this.keyId && this.keySecret);
  }

  /**
   * Create a Razorpay order
   */
  async createOrder({ amount, currency = 'INR', cart, email, userId, metadata = {} }) {
    if (!this.isAvailable()) {
      return { success: false, error: 'Razorpay is not configured' };
    }

    try {
      const options = {
        amount: Math.round(amount * 100), // Convert to paise
        currency: currency.toUpperCase(),
        receipt: `order_${Date.now()}`,
        payment_capture: 1, // Auto capture
        notes: {
          email: email || '',
          userId: userId || 'anonymous',
          cartItems: JSON.stringify(cart || []),
          ...metadata,
        },
      };

      const order = await this.client.orders.create(options);

      return {
        success: true,
        data: {
          gateway: 'razorpay',
          orderId: order.id,
          amount: order.amount,
          currency: order.currency,
          receipt: order.receipt,
          // Include key_id for frontend to initialize Razorpay
          keyId: this.keyId,
        },
      };
    } catch (error) {
      console.error('Razorpay createOrder error:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Verify Razorpay payment signature
   */
  async verifyPayment({ orderId, paymentId, signature, email, userId, cart }) {
    if (!this.isAvailable()) {
      return { success: false, verified: false, error: 'Razorpay is not configured' };
    }

    try {
      // Create expected signature
      const hmac = crypto.createHmac('sha256', this.keySecret);
      hmac.update(`${orderId}|${paymentId}`);
      const expectedSignature = hmac.digest('hex');

      const verified = expectedSignature === signature;

      if (verified) {
        return {
          success: true,
          verified: true,
          data: {
            gateway: 'razorpay',
            orderId,
            paymentId,
            signature,
            email,
            userId,
            cart,
          },
        };
      } else {
        return {
          success: true,
          verified: false,
          error: 'Invalid payment signature',
        };
      }
    } catch (error) {
      console.error('Razorpay verifyPayment error:', error);
      return { success: false, verified: false, error: error.message };
    }
  }

  /**
   * Handle Razorpay webhook events
   * Note: Razorpay webhooks are less commonly used since verification happens client-side
   */
  async handleWebhook(req) {
    try {
      const webhookSecret = process.env.RAZORPAY_WEBHOOK_SECRET;
      
      if (!webhookSecret) {
        console.log('Razorpay webhook secret not configured');
        return { success: false, event: null, data: null };
      }

      const signature = req.headers['x-razorpay-signature'];
      const body = JSON.stringify(req.body);

      const expectedSignature = crypto
        .createHmac('sha256', webhookSecret)
        .update(body)
        .digest('hex');

      if (signature !== expectedSignature) {
        return { success: false, event: null, error: 'Invalid webhook signature' };
      }

      const event = req.body.event;
      const payload = req.body.payload;

      if (event === 'payment.captured') {
        const payment = payload.payment.entity;
        return {
          success: true,
          event: 'payment.success',
          data: {
            gateway: 'razorpay',
            orderId: payment.order_id,
            paymentId: payment.id,
            amount: payment.amount,
            currency: payment.currency,
            email: payment.email || payment.notes?.email || '',
            userId: payment.notes?.userId || 'anonymous',
            cart: JSON.parse(payment.notes?.cartItems || '[]'),
          },
        };
      }

      return { success: true, event, data: null };
    } catch (error) {
      console.error('Razorpay handleWebhook error:', error);
      return { success: false, event: null, error: error.message };
    }
  }

  /**
   * Build Kafka payload for Razorpay payments
   */
  buildKafkaPayload(paymentData) {
    return {
      gateway: 'razorpay',
      razorpayOrderId: paymentData.orderId,
      paymentId: paymentData.paymentId,
      signature: paymentData.signature,
      status: 'completed',
      timestamp: new Date().toISOString(),
      email: paymentData.email || '',
      userId: paymentData.userId || 'anonymous',
      cart: paymentData.cart || [],
    };
  }
}

export default RazorpayGateway;
