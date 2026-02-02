import amqplib from 'amqplib'
import { BaseEventBusConnection, ConnectionState } from './base-connection.js'
import { logger } from '@a_jackie_z/logger'

// Re-export ConnectionState for convenience
export { ConnectionState } from './base-connection.js'

export interface EventBusProducerOptions {
  rabbitMqUrl: string;
  onStateChange?: (state: ConnectionState, reconnectCount?: number) => void;
}

export class EventBusProducer extends BaseEventBusConnection<amqplib.ConfirmChannel> {
  constructor(options: EventBusProducerOptions) {
    super(options);
  }

  protected async createChannelAndSetup(): Promise<void> {
    if (!this.channelModel) {
      throw new Error('Channel model is not initialized');
    }

    // Create confirm channel for reliable publishing
    this.channel = await this.channelModel.createConfirmChannel();
    logger.debug('RabbitMQ confirm channel created successfully');
  }

  /**
   * Publish a message to a queue with persistence enabled.
   * Throws immediately if channel is not available.
   *
   * @param queueName The name of the queue to publish to
   * @param data The data to publish
   * @throws Error if channel is not available or publish fails
   */
  async publish(queueName: string, data: any): Promise<void> {
    if (!this.channel) {
      throw new Error('Channel is not available. Ensure connect() is called and connection is established.');
    }

    try {
      // Assert queue with durable option
      await this.channel.assertQueue(queueName, {
        durable: true,        // Queue persists after restart
        exclusive: false,     // Multiple producers/consumers can connect
        autoDelete: false     // Queue is not auto-deleted
      });

      // Send message with persistent flag
      const message = JSON.stringify(data);
      this.channel.sendToQueue(queueName, Buffer.from(message), {
        persistent: true  // Message survives broker restart
      });

      // Wait for broker confirmation
      await this.channel.waitForConfirms();

      logger.debug({ queueName, data }, 'Message published to queue');
    } catch (error) {
      logger.error({ error, queueName }, 'Failed to publish message to queue');
      throw error;
    }
  }

  /**
   * Broadcast a message to all consumers listening on the exchange.
   * Uses fanout exchange to deliver message to all bound queues.
   * Throws immediately if channel is not available.
   *
   * @param exchangeName The name of the fanout exchange to broadcast to
   * @param data The data to broadcast
   * @throws Error if channel is not available or broadcast fails
   */
  async broadcast(exchangeName: string, data: any): Promise<void> {
    if (!this.channel) {
      throw new Error('Channel is not available. Ensure connect() is called and connection is established.');
    }

    try {
      // Assert fanout exchange
      await this.channel.assertExchange(exchangeName, 'fanout', {
        durable: false,       // Exchange doesn't need to persist (stateless routing)
        autoDelete: false     // Exchange is not auto-deleted
      });

      // Broadcast message to exchange (no routing key needed for fanout)
      const message = JSON.stringify(data);
      this.channel.publish(exchangeName, '', Buffer.from(message), {
        persistent: false  // Transient messages for broadcast (not persisted)
      });

      // Wait for broker confirmation
      await this.channel.waitForConfirms();

      logger.debug({ exchangeName, data }, 'Message broadcast to exchange');
    } catch (error) {
      logger.error({ error, exchangeName }, 'Failed to broadcast message to exchange');
      throw error;
    }
  }
}
