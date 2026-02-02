import { EventBusProducer } from '../producer.ts';
import { logger } from '@a_jackie_z/logger';

async function main() {
  // Create producer with state change callback
  const producer = new EventBusProducer({
    rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
    onStateChange: (state, reconnectCount) => {
      logger.info({ state, reconnectCount }, 'Producer state change');
    }
  });

  try {
    // Connect to RabbitMQ
    await producer.connect();
    logger.info('Producer connected and ready');

    // Publish user events
    logger.debug('Publishing user events');
    await producer.publish('user_events', {
      userId: 1001,
      username: 'john_doe',
      email: 'john@example.com',
      createdAt: new Date().toISOString()
    });

    await producer.publish('user_events', {
      userId: 1002,
      username: 'jane_smith',
      email: 'jane@example.com',
      createdAt: new Date().toISOString()
    });

    // Publish order events
    logger.debug('Publishing order events');
    await producer.publish('order_events', {
      orderId: 5001,
      userId: 1001,
      items: ['laptop', 'mouse'],
      total: 1299.99,
      status: 'processed',
      processedAt: new Date().toISOString()
    });

    await producer.publish('order_events', {
      orderId: 5002,
      userId: 1002,
      items: ['keyboard', 'monitor'],
      total: 599.99,
      status: 'processed',
      processedAt: new Date().toISOString()
    });

    logger.info('All messages published successfully');

    // Graceful shutdown
    logger.info('Disconnecting producer');
    await producer.disconnect();
    logger.info('Producer disconnected');
    process.exit(0);
  } catch (error) {
    logger.error({ error }, 'Error publishing messages');
    await producer.disconnect();
    process.exit(1);
  }
}

main();
