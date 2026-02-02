import { EventBusConsumer, type QueueHandlers, type EventHandler } from '../consumer.js';
import { logger } from '@a_jackie_z/logger';

// Example: Define event handlers
const userCreatedHandler: EventHandler = async (data) => {
  logger.info({ data }, 'Handler 1: User created');
  // Process user creation event
};

const sendWelcomeEmailHandler: EventHandler = async (data) => {
  logger.info({ email: data.email }, 'Handler 2: Sending welcome email');
  // Send welcome email
};

const orderProcessedHandler: EventHandler = async (data) => {
  logger.info({ data }, 'Handler: Order processed');
  // Process order
};

// Setup queue handlers mapping
const queueHandlers: QueueHandlers = new Map([
  ['user_events', [userCreatedHandler, sendWelcomeEmailHandler]],
  ['order_events', [orderProcessedHandler]]
]);

// Create consumer with state change callback
const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
  queueHandlers,
  onStateChange: (state, reconnectCount) => {
    logger.info({ state, reconnectCount, consumerId: 1 }, 'State change');
  }
});

// Connect and start consuming with 2 parallel consumers
async function main() {
  try {
    // Create second consumer with different handlers for testing
    const queueHandlers2: QueueHandlers = new Map([
      ['user_events', [userCreatedHandler, sendWelcomeEmailHandler]],
      ['order_events', [orderProcessedHandler]]
    ]);

    const consumer2 = new EventBusConsumer({
      rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
      queueHandlers: queueHandlers2,
      onStateChange: (state, reconnectCount) => {
        logger.info({ state, reconnectCount, consumerId: 2 }, 'State change');
      }
    });

    // Connect both consumers in parallel
    await Promise.all([
      consumer.connect().then(() => logger.info({ consumerId: 1 }, 'Consumer connected and ready')),
      consumer2.connect().then(() => logger.info({ consumerId: 2 }, 'Consumer connected and ready'))
    ]);

    logger.info('Both consumers are running. Press Ctrl+C to exit.');
    logger.info('Events will be distributed across both consumers');

    // Handle graceful shutdown for both consumers
    process.on('SIGINT', async () => {
      logger.info('Shutting down gracefully');
      await Promise.all([
        consumer.disconnect(),
        consumer2.disconnect()
      ]);
      logger.info('Both consumers disconnected');
      process.exit(0);
    });
  } catch (error) {
    logger.error({ error }, 'Failed to start consumers');
    process.exit(1);
  }
}

main();
