import amqplib from 'amqplib'
import { BaseEventBusConnection, ConnectionState } from './base-connection.js'
import { logger } from '@a_jackie_z/logger'

// Type definitions
export type EventHandler<T = any> = (data: T) => Promise<void>;
export type QueueHandlers = Map<string, EventHandler[]>;
export type ExchangeBindings = Map<string, string>; // Map<queueName, exchangeName>

// Re-export ConnectionState for backward compatibility
export { ConnectionState } from './base-connection.js'

export interface EventBusConsumerOptions {
  rabbitMqUrl: string;
  queueHandlers: QueueHandlers;
  onStateChange?: (state: ConnectionState, reconnectCount?: number) => void;
  exchangeBindings?: ExchangeBindings;
}

export class EventBusConsumer extends BaseEventBusConnection<amqplib.Channel> {
  private readonly queueHandlers: QueueHandlers;
  private readonly exchangeBindings?: ExchangeBindings;
  private activeConsumerTags: Set<string> = new Set();

  constructor(options: EventBusConsumerOptions) {
    super(options);
    this.queueHandlers = options.queueHandlers;
    if (options.exchangeBindings !== undefined) {
      this.exchangeBindings = options.exchangeBindings;
    }
  }

  protected async createChannelAndSetup(): Promise<void> {
    if (!this.channelModel) {
      throw new Error('Channel model is not initialized');
    }

    this.channel = await this.channelModel.createChannel();

    // Limit to 1 message processed at a time
    this.channel.prefetch(1);

    // Clear active consumer tags on reconnect
    this.activeConsumerTags.clear();

    // Assert queues and register handlers
    for (const [queueName, handlers] of this.queueHandlers.entries()) {
      // Check if this queue should be bound to an exchange (broadcast mode)
      const exchangeName = this.exchangeBindings?.get(queueName);

      if (exchangeName) {
        // Broadcast mode: assert exchange and create exclusive queue
        await this.channel.assertExchange(exchangeName, 'fanout', {
          durable: false,       // Exchange doesn't need to persist (stateless routing)
          autoDelete: false     // Exchange is not auto-deleted
        });

        // Create exclusive queue that gets deleted when consumer disconnects
        // This ensures each consumer instance receives all broadcast messages
        const { queue: exclusiveQueueName } = await this.channel.assertQueue('', {
          exclusive: true,      // Queue is exclusive to this connection
          autoDelete: true      // Queue is deleted when connection closes
        });

        // Bind the exclusive queue to the fanout exchange
        await this.channel.bindQueue(exclusiveQueueName, exchangeName, '');

        logger.info({ queueName, exchangeName, exclusiveQueueName }, 'Queue bound to exchange for broadcast');

        // Setup consumer for the exclusive queue
        const { consumerTag } = await this.channel.consume(exclusiveQueueName, async (msg) => {
          if (msg) {
            let successCount = 0;
            const content = msg.content.toString();

            try {
              const data = JSON.parse(content);

              // Execute handlers sequentially
              for (const handler of handlers) {
                try {
                  await handler(data);
                  successCount++;
                } catch (error) {
                  logger.error({ error, queueName, exchangeName }, 'Handler failed for broadcast queue');
                }
              }

              // Acknowledge if at least one handler succeeded
              if (successCount > 0) {
                this.channel!.ack(msg);
              } else {
                // All handlers failed, nack without requeue
                logger.error({ queueName, exchangeName, handlerCount: handlers.length }, 'All handlers failed for broadcast queue');
                this.channel!.nack(msg, false, false);
              }
            } catch (error) {
              logger.error({ error, queueName, exchangeName }, 'Failed to parse message from broadcast queue');
              this.channel!.nack(msg, false, false);
            }
          }
        }, {
          noAck: false // Require acknowledgment after processing
        });

        this.activeConsumerTags.add(consumerTag);
        logger.info({ queueName, exchangeName, exclusiveQueueName, consumerTag }, 'Consumer registered for broadcast queue');
      } else {
        // Direct queue mode: traditional point-to-point messaging
        await this.channel.assertQueue(queueName, {
          durable: true,        // Queue persists after restart
          exclusive: false,     // Multiple consumers can connect
          autoDelete: false     // Queue is not auto-deleted
        });

        // Setup consumer for this queue
        const { consumerTag } = await this.channel.consume(queueName, async (msg) => {
          if (msg) {
            let successCount = 0;
            const content = msg.content.toString();

            try {
              const data = JSON.parse(content);

              // Execute handlers sequentially
              for (const handler of handlers) {
                try {
                  await handler(data);
                  successCount++;
                } catch (error) {
                  logger.error({ error, queueName }, 'Handler failed for queue');
                }
              }

              // Acknowledge if at least one handler succeeded
              if (successCount > 0) {
                this.channel!.ack(msg);
              } else {
                // All handlers failed, nack without requeue
                logger.error({ queueName, handlerCount: handlers.length }, 'All handlers failed for queue, message will not be requeued');
                this.channel!.nack(msg, false, false);
              }
            } catch (error) {
              logger.error({ error, queueName }, 'Failed to parse message from queue');
              this.channel!.nack(msg, false, false);
            }
          }
        }, {
          noAck: false // Require acknowledgment after processing
        });

        this.activeConsumerTags.add(consumerTag);
        logger.info({ queueName, consumerTag }, 'Consumer registered for queue');
      }
    }
  }

  override async disconnect() {
    logger.info('Disconnecting consumer');

    // Cancel all active consumers gracefully before base disconnect
    if (this.channel && this.activeConsumerTags.size > 0) {
      const cancelPromises: Promise<void>[] = [];

      for (const consumerTag of this.activeConsumerTags) {
        const cancelPromise = (async () => {
          try {
            logger.debug({ consumerTag }, 'Cancelling consumer');

            // Apply timeout only if channel operation can be initiated
            await Promise.race([
              this.channel!.cancel(consumerTag),
              new Promise<void>((_, reject) =>
                setTimeout(() => reject(new Error('Consumer cancellation timeout')), 5000)
              )
            ]);
          } catch (error: any) {
            // Gracefully handle channel-closed errors (expected during shutdown)
            const errorMessage = error?.message || '';
            if (errorMessage.includes('Channel ended') ||
                errorMessage.includes('Channel closed') ||
                errorMessage.includes('no reply will be forthcoming')) {
              logger.debug({ consumerTag }, 'Consumer already cancelled (channel closed)');
            } else {
              logger.error({ error, consumerTag }, 'Failed to cancel consumer');
            }
          }
        })();

        cancelPromises.push(cancelPromise);
      }

      // Wait for all cancellations to complete or fail
      await Promise.allSettled(cancelPromises);
    }

    this.activeConsumerTags.clear();

    // Call base class disconnect to close channel and connection
    await super.disconnect();
  }
}
