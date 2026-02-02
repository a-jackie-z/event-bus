import { EventBusProducer } from '../producer.js';
import { logger } from '@a_jackie_z/logger';

async function main() {
  // Create producer with state change callback
  const producer = new EventBusProducer({
    rabbitMqUrl: 'amqp://rabbitmq:12345678@192.168.2.151:5672',
    onStateChange: (state, reconnectCount) => {
      logger.info({ state, reconnectCount }, 'Broadcast producer state change');
    }
  });

  try {
    // Connect to RabbitMQ
    await producer.connect();
    logger.info('Broadcast producer connected and ready');

    // Broadcast notifications (all consumers will receive these)
    logger.debug('Broadcasting notification 1');
    await producer.broadcast('notification_broadcast', {
      type: 'system',
      message: 'System maintenance scheduled for tonight',
      priority: 'high',
      timestamp: new Date().toISOString()
    });

    logger.debug('Broadcasting notification 2');
    await producer.broadcast('notification_broadcast', {
      type: 'update',
      message: 'New features released in version 2.0',
      priority: 'medium',
      timestamp: new Date().toISOString()
    });

    // Broadcast alerts (all consumers will receive these)
    logger.debug('Broadcasting alert 1');
    await producer.broadcast('alert_broadcast', {
      level: 'warning',
      message: 'High CPU usage detected',
      threshold: 85,
      timestamp: new Date().toISOString()
    });

    logger.debug('Broadcasting alert 2');
    await producer.broadcast('alert_broadcast', {
      level: 'critical',
      message: 'Database connection pool exhausted',
      threshold: 100,
      timestamp: new Date().toISOString()
    });

    logger.info('All broadcast messages sent successfully');

    // Wait a bit before disconnecting to ensure messages are processed
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Disconnect
    await producer.disconnect();
    logger.info('Broadcast producer disconnected');
  } catch (error) {
    logger.error({ error }, 'Failed to broadcast messages');
    await producer.disconnect();
    process.exit(1);
  }
}

main();
