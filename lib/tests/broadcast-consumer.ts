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
  const notificationHandler: EventHandler = async (data) => {
    logger.info({ data, consumerId }, `[Consumer ${consumerId}] Received broadcast notification`);
    // Process notification
  };

  const alertHandler: EventHandler = async (data) => {
    logger.info({ data, consumerId }, `[Consumer ${consumerId}] Received broadcast alert`);
    // Process alert
  };

  const queueHandlers: QueueHandlers = new Map([
    ['notifications', [notificationHandler]],
    ['alerts', [alertHandler]]
  ]);

  const exchangeBindings: ExchangeBindings = new Map([
    ['notifications', 'notification_broadcast'],
    ['alerts', 'alert_broadcast']
  ]);

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
      logger.info({ consumerId }, `[Consumer ${consumerId}] Starting...`);
      await consumer.connect();
      logger.info({ consumerId }, `[Consumer ${consumerId}] Connected and listening for broadcasts`);

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
  logger.info('=== Starting 2 Broadcast Consumers Demo ===');
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
  logger.info('Both consumers are now listening for broadcast messages...');

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
