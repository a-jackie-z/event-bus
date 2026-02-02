import { EventBusProducer } from '../producer.js';
import { logger } from '@a_jackie_z/logger';

async function main() {
  const producer = new EventBusProducer({
    rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
    onStateChange: (state, reconnectCount) => {
      logger.info({ state, reconnectCount }, 'Mixed producer state change');
    }
  });

  try {
    await producer.connect();
    logger.info('Mixed producer connected and ready');

    // Send direct messages (1-1 pattern) - only one consumer will process
    logger.debug('Sending direct order message');
    await producer.publish('orders', {
      orderId: 1001,
      customerId: 5001,
      items: ['item1', 'item2'],
      total: 99.99,
      status: 'pending',
      timestamp: new Date().toISOString()
    });

    await producer.publish('orders', {
      orderId: 1002,
      customerId: 5002,
      items: ['item3'],
      total: 49.99,
      status: 'pending',
      timestamp: new Date().toISOString()
    });

    // Broadcast messages (1-n pattern) - all consumers will receive
    logger.debug('Broadcasting announcement');
    await producer.broadcast('announcement_broadcast', {
      title: 'New Product Launch',
      message: 'Exciting new product features available now!',
      category: 'product',
      timestamp: new Date().toISOString()
    });

    await producer.broadcast('announcement_broadcast', {
      title: 'System Update',
      message: 'All services will be updated to version 3.0',
      category: 'system',
      timestamp: new Date().toISOString()
    });

    logger.info('All messages sent successfully (both direct and broadcast)');

    await new Promise(resolve => setTimeout(resolve, 2000));

    await producer.disconnect();
    logger.info('Mixed producer disconnected');
  } catch (error) {
    logger.error({ error }, 'Failed to send mixed messages');
    await producer.disconnect();
    process.exit(1);
  }
}

main();
