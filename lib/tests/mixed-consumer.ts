import { EventBusConsumer, type QueueHandlers, type ExchangeBindings, type EventHandler } from '../consumer.js';
import { logger } from '@a_jackie_z/logger';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);

// Check if this is a child consumer process
const isChildConsumer = process.env.IS_CHILD_CONSUMER === 'true';
const consumerId = process.env.CONSUMER_ID || '1';

if (isChildConsumer) {
  // Child consumer logic
  // Example: Handlers for direct queue messages (1-1)
  const orderHandler: EventHandler = async (data) => {
    logger.info({ data, consumerId }, `[Consumer ${consumerId}] Processing direct order`);
    // Process order - only one consumer will handle this
  };

  // Example: Handlers for broadcast messages (1-n)
  const announcementHandler: EventHandler = async (data) => {
    logger.info({ data, consumerId }, `[Consumer ${consumerId}] Received broadcast announcement`);
    // All consumers will receive this
  };

  // Setup queue handlers - mix of direct and broadcast queues
  const queueHandlers: QueueHandlers = new Map([
    ['orders', [orderHandler]],              // Direct queue (1-1)
    ['announcements', [announcementHandler]] // Broadcast queue (1-n)
  ]);

  // Setup exchange bindings - only for broadcast queues
  const exchangeBindings: ExchangeBindings = new Map([
    ['announcements', 'announcement_broadcast']  // Bind announcements to fanout exchange
    // 'orders' is NOT bound to an exchange, so it remains a direct queue
  ]);

  // Create consumer supporting both patterns
  const consumer = new EventBusConsumer({
    rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
    queueHandlers,
    onStateChange: (state, reconnectCount) => {
      logger.info({ state, reconnectCount, consumerId }, `[Consumer ${consumerId}] State change`);
    },
    exchangeBindings
  });

  async function runConsumer() {
    try {
      logger.info({ consumerId }, `[Consumer ${consumerId}] Starting mixed mode consumer (1-1 and 1-n)`);

      await consumer.connect();
      logger.info({ consumerId }, `[Consumer ${consumerId}] Connected - listening to direct queues and broadcasts`);

      // Keep the process running
      process.on('SIGINT', async () => {
        logger.info({ consumerId }, `[Consumer ${consumerId}] Disconnecting...`);
        await consumer.disconnect();
        process.exit(0);
      });

      process.on('SIGTERM', async () => {
        logger.info({ consumerId }, `[Consumer ${consumerId}] Disconnecting...`);
        await consumer.disconnect();
        process.exit(0);
      });
    } catch (error) {
      logger.error({ error, consumerId }, `[Consumer ${consumerId}] Failed to start`);
      process.exit(1);
    }
  }

  runConsumer();
} else {
  // Parent process - spawn 2 consumer instances
  logger.info('=== Starting 2 Mixed Mode Consumers Demo ===');
  logger.info('Direct queue (orders): Only ONE consumer will process each message');
  logger.info('Broadcast (announcements): BOTH consumers will receive ALL messages');
  logger.info('Press Ctrl+C to stop all consumers');
  logger.info('');

  const consumers: any[] = [];

  // Spawn Consumer 1
  const consumer1 = spawn(process.execPath, [__filename], {
    env: {
      ...process.env,
      IS_CHILD_CONSUMER: 'true',
      CONSUMER_ID: '1'
    },
    stdio: 'inherit'
  });

  consumers.push(consumer1);
  logger.info('✓ Consumer 1 launched');

  // Spawn Consumer 2
  const consumer2 = spawn(process.execPath, [__filename], {
    env: {
      ...process.env,
      IS_CHILD_CONSUMER: 'true',
      CONSUMER_ID: '2'
    },
    stdio: 'inherit'
  });

  consumers.push(consumer2);
  logger.info('✓ Consumer 2 launched');
  logger.info('');
  logger.info('Both consumers ready - demonstrating 1-1 and 1-n patterns...');

  // Handle parent process signals
  process.on('SIGINT', () => {
    logger.info('');
    logger.info('Stopping all consumers...');
    consumers.forEach(c => c.kill('SIGINT'));
    setTimeout(() => process.exit(0), 2000);
  });

  process.on('SIGTERM', () => {
    logger.info('Stopping all consumers...');
    consumers.forEach(c => c.kill('SIGTERM'));
    setTimeout(() => process.exit(0), 2000);
  });

  // Handle child process exits
  consumer1.on('exit', (code) => {
    logger.info({ code }, 'Consumer 1 exited');
    if (code !== 0) {
      logger.error('Consumer 1 exited with error, stopping all...');
      consumers.forEach(c => c.kill());
      process.exit(1);
    }
  });

  consumer2.on('exit', (code) => {
    logger.info({ code }, 'Consumer 2 exited');
    if (code !== 0) {
      logger.error('Consumer 2 exited with error, stopping all...');
      consumers.forEach(c => c.kill());
      process.exit(1);
    }
  });
}
