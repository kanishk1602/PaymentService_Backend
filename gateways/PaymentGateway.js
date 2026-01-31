/**
 * PaymentGateway Interface (Abstract Base Class)
 * 
 * This defines the contract that all payment gateways must implement.
 * Uses the Strategy Pattern to allow interchangeable payment providers.
 */
export class PaymentGateway {
  constructor(config = {}) {
    if (new.target === PaymentGateway) {
      throw new Error('PaymentGateway is an abstract class and cannot be instantiated directly');
    }
    this.config = config;
  }

  /**
   * Get gateway information
   * @returns {{ id: string, name: string, currencies: string[], available: boolean }}
   */
  getInfo() {
    throw new Error('Method getInfo() must be implemented');
  }

  /**
   * Check if gateway is properly configured and available
   * @returns {boolean}
   */
  isAvailable() {
    throw new Error('Method isAvailable() must be implemented');
  }

  /**
   * Create a payment order/session
   * @param {Object} params
   * @param {number} params.amount - Amount in base currency units (e.g., dollars, rupees)
   * @param {string} params.currency - Currency code (USD, INR, etc.)
   * @param {Array} params.cart - Cart items
   * @param {string} params.email - Customer email
   * @param {string} params.userId - Customer user ID
   * @param {Object} params.metadata - Additional metadata
   * @returns {Promise<{ success: boolean, data: Object }>}
   */
  async createOrder(params) {
    throw new Error('Method createOrder() must be implemented');
  }

  /**
   * Verify a payment after completion
   * @param {Object} paymentData - Gateway-specific payment verification data
   * @returns {Promise<{ success: boolean, verified: boolean, data: Object }>}
   */
  async verifyPayment(paymentData) {
    throw new Error('Method verifyPayment() must be implemented');
  }

  /**
   * Handle webhook events from the payment provider
   * @param {Object} req - Express request object
   * @returns {Promise<{ success: boolean, event: string, data: Object | null }>}
   */
  async handleWebhook(req) {
    throw new Error('Method handleWebhook() must be implemented');
  }

  /**
   * Build the payload for Kafka payment-successful event
   * @param {Object} paymentData - Payment data from verification or webhook
   * @returns {Object} - Standardized payload for Kafka
   */
  buildKafkaPayload(paymentData) {
    throw new Error('Method buildKafkaPayload() must be implemented');
  }
}

export default PaymentGateway;
