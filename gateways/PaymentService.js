import RazorpayGateway from './RazorpayGateway.js';
import StripeGateway from './StripeGateway.js';

/**
 * PaymentService - Gateway Manager (Context in Strategy Pattern)
 * 
 * This class manages all payment gateways and provides a unified interface
 * to work with multiple payment providers.
 * 
 * Usage:
 *   const paymentService = new PaymentService();
 *   const gateway = paymentService.getGateway('stripe');
 *   const result = await gateway.createOrder({ amount: 100, currency: 'usd', ... });
 */
export class PaymentService {
  constructor() {
    // Registry of all payment gateways
    this.gateways = new Map();
    
    // Initialize default gateways
    this.registerGateway(new RazorpayGateway());
    this.registerGateway(new StripeGateway());
  }

  /**
   * Register a new payment gateway
   * @param {PaymentGateway} gateway - Gateway instance
   */
  registerGateway(gateway) {
    const info = gateway.getInfo();
    this.gateways.set(info.id, gateway);
    console.log(`Registered payment gateway: ${info.name} (${info.id}) - Available: ${info.available}`);
  }

  /**
   * Get a specific gateway by ID
   * @param {string} gatewayId - Gateway identifier (e.g., 'stripe', 'razorpay')
   * @returns {PaymentGateway | null}
   */
  getGateway(gatewayId) {
    return this.gateways.get(gatewayId) || null;
  }

  /**
   * Get all available gateways
   * @returns {PaymentGateway[]}
   */
  getAvailableGateways() {
    return Array.from(this.gateways.values()).filter(g => g.isAvailable());
  }

  /**
   * List all gateways with their info
   * @returns {Array<{ id: string, name: string, currencies: string[], available: boolean }>}
   */
  listGateways() {
    return Array.from(this.gateways.values()).map(g => g.getInfo());
  }

  /**
   * List only available gateways
   * @returns {Array<{ id: string, name: string, currencies: string[], available: boolean }>}
   */
  listAvailableGateways() {
    return this.listGateways().filter(g => g.available);
  }

  /**
   * Create an order using the specified gateway
   * @param {string} gatewayId - Gateway to use
   * @param {Object} params - Order parameters
   * @returns {Promise<{ success: boolean, data?: Object, error?: string }>}
   */
  async createOrder(gatewayId, params) {
    const gateway = this.getGateway(gatewayId);
    
    if (!gateway) {
      return { success: false, error: `Unknown gateway: ${gatewayId}` };
    }
    
    if (!gateway.isAvailable()) {
      return { success: false, error: `Gateway ${gatewayId} is not available` };
    }

    return gateway.createOrder(params);
  }

  /**
   * Verify a payment using the specified gateway
   * @param {string} gatewayId - Gateway to use
   * @param {Object} paymentData - Payment verification data
   * @returns {Promise<{ success: boolean, verified: boolean, data?: Object, error?: string }>}
   */
  async verifyPayment(gatewayId, paymentData) {
    const gateway = this.getGateway(gatewayId);
    
    if (!gateway) {
      return { success: false, verified: false, error: `Unknown gateway: ${gatewayId}` };
    }

    return gateway.verifyPayment(paymentData);
  }

  /**
   * Handle webhook for a specific gateway
   * @param {string} gatewayId - Gateway to use
   * @param {Object} req - Express request object
   * @returns {Promise<{ success: boolean, event?: string, data?: Object, error?: string }>}
   */
  async handleWebhook(gatewayId, req) {
    const gateway = this.getGateway(gatewayId);
    
    if (!gateway) {
      return { success: false, error: `Unknown gateway: ${gatewayId}` };
    }

    return gateway.handleWebhook(req);
  }

  /**
   * Build Kafka payload for a payment
   * @param {string} gatewayId - Gateway that processed the payment
   * @param {Object} paymentData - Payment data
   * @returns {Object} - Standardized Kafka payload
   */
  buildKafkaPayload(gatewayId, paymentData) {
    const gateway = this.getGateway(gatewayId);
    
    if (!gateway) {
      // Return a generic payload
      return {
        gateway: gatewayId,
        status: 'completed',
        timestamp: new Date().toISOString(),
        ...paymentData,
      };
    }

    return gateway.buildKafkaPayload(paymentData);
  }
}

// Export singleton instance
export const paymentService = new PaymentService();

export default PaymentService;
