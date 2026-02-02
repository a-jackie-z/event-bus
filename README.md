# @a_jackie_z/event-bus

A robust, TypeScript-first event bus implementation using RabbitMQ. Supports both point-to-point (queue-based) and broadcast (fanout exchange) messaging patterns with automatic reconnection and reliable message delivery.

## Features

- **Dual Messaging Patterns**: Point-to-point (load-balanced queues) and broadcast (fanout exchanges)
- **TypeScript Support**: Full type safety with TypeScript definitions
- **Auto-Reconnection**: Automatic reconnection with 15-second intervals
- **Message Persistence**: Durable queues and persistent messages for reliability
- **Fair Distribution**: Prefetch set to 1 for even load balancing across consumers
- **Connection State Monitoring**: Track connection state changes with callbacks
- **Graceful Shutdown**: Proper cleanup of consumers and connections
- **Error Handling**: Fail-safe message acknowledgment strategies

## Installation

```bash
npm install @a_jackie_z/event-bus
```

## Table of Contents

- [Architecture](#architecture)
- [Pattern Comparison](#pattern-comparison)
- [Quick Start](#quick-start)
- [Point-to-Point Messaging](#point-to-point-messaging)
- [Broadcast Messaging](#broadcast-messaging)
- [Mixed Mode](#mixed-mode)
- [Connection State Management](#connection-state-management)
- [Scaling Strategies](#scaling-strategies)
- [Performance Tips](#performance-tips)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)

## Architecture

### Point-to-Point Pattern (Queue-Based)

```
┌──────────┐                    ┌───────────┐
│ Producer │───── publish() ───>│   Queue   │
└──────────┘                    │ (durable) │
                                └─────┬─────┘
                                      │
                        ┌─────────────┴─────────────┐
                        │    (Load Balanced)        │
                        ▼                           ▼
                  ┌───────────┐             ┌───────────┐
                  │ Consumer1 │             │ Consumer2 │
                  └───────────┘             └───────────┘
                  
Each message is delivered to ONE consumer (round-robin)
```

### Broadcast Pattern (Fanout Exchange)

```
                               ┌─────────────────┐
                               │ Fanout Exchange │
┌──────────┐                   └────────┬────────┘
│ Producer │─── broadcast() ───────────>│
└──────────┘                            │
                        ┌───────────────┴───────────────┐
                        │                               │
                        ▼                               ▼
                  ┌─────────┐                     ┌─────────┐
                  │ Queue1  │                     │ Queue2  │
                  │(exclusive)                    │(exclusive)
                  └────┬────┘                     └────┬────┘
                       │                               │
                       ▼                               ▼
                  ┌───────────┐                   ┌───────────┐
                  │ Consumer1 │                   │ Consumer2 │
                  └───────────┘                   └───────────┘
                  
Each message is delivered to ALL consumers
```

## Pattern Comparison

| Feature | Point-to-Point (Queue) | Broadcast (Fanout Exchange) |
|---------|------------------------|------------------------------|
| **Use Case** | Task distribution, work queues | Event notifications, announcements |
| **Delivery** | One consumer receives each message | All consumers receive every message |
| **Message Persistence** | Durable queues, persistent messages | Transient messages, non-durable exchange |
| **Queue Type** | Shared, durable queue | Exclusive, auto-delete queues per consumer |
| **Consumer Behavior** | Load-balanced (round-robin) | All consumers process independently |
| **Example Scenarios** | Order processing, email sending, image processing | System alerts, cache invalidation, real-time updates |
| **Scaling** | Add consumers for parallel processing | Add consumers for redundancy/availability |
| **Message Guarantee** | At-least-once delivery | Best-effort delivery (no persistence) |

## Quick Start

```typescript
import { EventBusProducer, EventBusConsumer, type QueueHandlers, type EventHandler } from '@a_jackie_z/event-bus';

// Producer: Send messages
const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672'
});

await producer.connect();
await producer.publish('tasks', { taskId: 1, action: 'process' });
await producer.disconnect();

// Consumer: Process messages
const taskHandler: EventHandler = async (data) => {
  console.log('Processing task:', data);
};

const queueHandlers: QueueHandlers = new Map([
  ['tasks', [taskHandler]]
]);

const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers
});

await consumer.connect();
// Consumer runs until disconnect() is called
```

## Point-to-Point Messaging

Use point-to-point messaging for task distribution where each message should be processed by exactly one consumer.

### Producer Example

```typescript
import { EventBusProducer, ConnectionState } from '@a_jackie_z/event-bus';

const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  onStateChange: (state, reconnectCount) => {
    console.log(`Producer state: ${state}`, { reconnectCount });
  }
});

try {
  await producer.connect();
  
  // Publish to durable queue with persistent messages
  await producer.publish('order_events', {
    orderId: 1001,
    userId: 5001,
    items: ['laptop', 'mouse'],
    total: 1299.99,
    status: 'pending',
    timestamp: new Date().toISOString()
  });
  
  await producer.publish('order_events', {
    orderId: 1002,
    userId: 5002,
    items: ['keyboard'],
    total: 89.99,
    status: 'pending',
    timestamp: new Date().toISOString()
  });
  
  console.log('Messages published successfully');
  
  // Graceful shutdown
  await producer.disconnect();
} catch (error) {
  console.error('Failed to publish messages:', error);
  await producer.disconnect();
}
```

### Consumer Example

```typescript
import { EventBusConsumer, type QueueHandlers, type EventHandler } from '@a_jackie_z/event-bus';

// Define handlers for the queue
const processOrderHandler: EventHandler = async (data) => {
  console.log('Processing order:', data.orderId);
  // Process order logic here
};

const notifyUserHandler: EventHandler = async (data) => {
  console.log('Notifying user:', data.userId);
  // Send notification logic here
};

// Map queue names to handlers (multiple handlers per queue)
const queueHandlers: QueueHandlers = new Map([
  ['order_events', [processOrderHandler, notifyUserHandler]]
]);

const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers,
  onStateChange: (state, reconnectCount) => {
    console.log(`Consumer state: ${state}`, { reconnectCount });
  }
});

try {
  await consumer.connect();
  console.log('Consumer connected and listening for messages');
  
  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    await consumer.disconnect();
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    console.log('Shutting down gracefully...');
    await consumer.disconnect();
    process.exit(0);
  });
} catch (error) {
  console.error('Failed to start consumer:', error);
  await consumer.disconnect();
  process.exit(1);
}
```

## Broadcast Messaging

Use broadcast messaging when all consumers need to receive every message (e.g., cache invalidation, system-wide notifications).

### Producer Example

```typescript
import { EventBusProducer } from '@a_jackie_z/event-bus';

const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672'
});

await producer.connect();

// Broadcast to fanout exchange - all consumers receive this
await producer.broadcast('system_notifications', {
  type: 'maintenance',
  message: 'System maintenance scheduled for tonight',
  priority: 'high',
  timestamp: new Date().toISOString()
});

await producer.broadcast('system_notifications', {
  type: 'update',
  message: 'New features available in version 2.0',
  priority: 'medium',
  timestamp: new Date().toISOString()
});

await producer.disconnect();
```

### Consumer Example

```typescript
import { EventBusConsumer, type QueueHandlers, type ExchangeBindings, type EventHandler } from '@a_jackie_z/event-bus';

// Handler for broadcast messages
const notificationHandler: EventHandler = async (data) => {
  console.log('Received system notification:', data);
  // Each consumer instance processes the notification independently
};

// Define queue handlers
const queueHandlers: QueueHandlers = new Map([
  ['notifications', [notificationHandler]]
]);

// Bind queues to exchanges for broadcast
const exchangeBindings: ExchangeBindings = new Map([
  ['notifications', 'system_notifications']  // Queue -> Exchange mapping
]);

const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers,
  exchangeBindings  // This enables broadcast mode for 'notifications' queue
});

await consumer.connect();
console.log('Consumer listening for broadcasts');

// Each consumer instance will receive ALL broadcast messages
```

**Note**: When using `exchangeBindings`, the consumer creates an **exclusive queue** that is automatically deleted when the consumer disconnects. This ensures each consumer instance receives all broadcast messages.

## Mixed Mode

Combine both point-to-point and broadcast patterns in a single consumer.

```typescript
import { EventBusConsumer, type QueueHandlers, type ExchangeBindings, type EventHandler } from '@a_jackie_z/event-bus';

// Point-to-point handler (only one consumer processes each message)
const processOrderHandler: EventHandler = async (data) => {
  console.log('Processing order (load balanced):', data.orderId);
  // Heavy processing - distributed across consumers
};

// Broadcast handler (all consumers receive each message)
const cacheInvalidationHandler: EventHandler = async (data) => {
  console.log('Invalidating cache (all consumers):', data.cacheKey);
  // Cache invalidation - every instance must process
};

const queueHandlers: QueueHandlers = new Map([
  ['orders', [processOrderHandler]],           // Point-to-point queue
  ['cache_invalidation', [cacheInvalidationHandler]]  // Broadcast queue
]);

const exchangeBindings: ExchangeBindings = new Map([
  ['cache_invalidation', 'cache_invalidation_broadcast']  // Only bind broadcast queues
  // 'orders' is NOT bound, so it remains a point-to-point queue
]);

const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers,
  exchangeBindings
});

await consumer.connect();
// Now handles both patterns:
// - orders: Load-balanced across consumers
// - cache_invalidation: All consumers receive every message
```

### Producer for Mixed Mode

```typescript
import { EventBusProducer } from '@a_jackie_z/event-bus';

const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672'
});

await producer.connect();

// Point-to-point: Only one consumer processes this
await producer.publish('orders', {
  orderId: 1001,
  customerId: 5001,
  total: 99.99
});

// Broadcast: All consumers receive this
await producer.broadcast('cache_invalidation_broadcast', {
  cacheKey: 'user:5001',
  action: 'invalidate'
});

await producer.disconnect();
```

## Connection State Management

Monitor connection state changes and handle reconnection events.

### Connection States

```typescript
import { ConnectionState } from '@a_jackie_z/event-bus';

// Available states:
ConnectionState.CONNECTED     // Successfully connected to RabbitMQ
ConnectionState.DISCONNECTED  // Intentionally disconnected
ConnectionState.RECONNECTING  // Attempting to reconnect after connection loss
```

### State Change Callback

```typescript
import { EventBusProducer, ConnectionState } from '@a_jackie_z/event-bus';

const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  onStateChange: (state, reconnectCount) => {
    switch (state) {
      case ConnectionState.CONNECTED:
        console.log('✓ Connected to RabbitMQ');
        break;
      case ConnectionState.DISCONNECTED:
        console.log('✗ Disconnected from RabbitMQ');
        break;
      case ConnectionState.RECONNECTING:
        console.log(`⟳ Reconnecting... (attempt ${reconnectCount})`);
        break;
    }
  }
});

await producer.connect();
```

### Automatic Reconnection

The event bus automatically attempts to reconnect when:
- Connection is lost
- Channel errors occur
- Network issues arise

**Reconnection Behavior**:
- Initial retry after 15 seconds
- Continues retrying indefinitely with 15-second intervals
- Resets retry counter on successful connection
- Preserves queue/exchange configurations on reconnection

## Scaling Strategies

### Horizontal Consumer Scaling

**Point-to-Point Queues** (Load Balancing):
```typescript
// Run multiple consumer instances - messages are distributed
// Consumer 1
const consumer1 = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([['tasks', [handler]]])
});
await consumer1.connect();

// Consumer 2 (same configuration)
const consumer2 = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([['tasks', [handler]]])
});
await consumer2.connect();

// Messages in 'tasks' queue are distributed round-robin between consumers
// If Consumer 1 is processing, Consumer 2 gets the next message
```

**Broadcast Exchanges** (Redundancy):
```typescript
// All consumer instances receive ALL broadcast messages
// Useful for cache invalidation, configuration updates, etc.

// Service Instance 1
const consumer1 = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([['updates', [handler]]]),
  exchangeBindings: new Map([['updates', 'system_updates']])
});
await consumer1.connect();

// Service Instance 2 (receives same broadcasts)
const consumer2 = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([['updates', [handler]]]),
  exchangeBindings: new Map([['updates', 'system_updates']])
});
await consumer2.connect();

// Both instances receive every broadcast message
```

### Queue Partitioning Strategies

**Option 1: Multiple Queues by Category**
```typescript
// Producer distributes by category
await producer.publish('orders_electronics', { category: 'electronics', ... });
await producer.publish('orders_clothing', { category: 'clothing', ... });

// Consumer specializes by category
const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([
    ['orders_electronics', [electronicsHandler]],
    ['orders_clothing', [clothingHandler]]
  ])
});
```

**Option 2: Consistent Hashing for Partitioning**
```typescript
// Hash-based queue assignment
function getQueueForUser(userId: number, partitionCount: number): string {
  const partition = userId % partitionCount;
  return `user_events_partition_${partition}`;
}

// Producer
const queueName = getQueueForUser(userId, 4); // 4 partitions
await producer.publish(queueName, userData);

// Consumer handles specific partitions
const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([
    ['user_events_partition_0', [handler]],
    ['user_events_partition_1', [handler]]
  ])
});
```

**Option 3: Priority Queues**
```typescript
// Separate queues by priority
await producer.publish('tasks_high_priority', { priority: 'high', ... });
await producer.publish('tasks_low_priority', { priority: 'low', ... });

// Run more consumers on high-priority queue
// 3 consumers for high priority
// 1 consumer for low priority
```

### Consumer Groups Pattern

```typescript
// Consumer Group 1: Order Processing (3 instances)
// All share the same 'orders' queue for load balancing
for (let i = 1; i <= 3; i++) {
  const consumer = new EventBusConsumer({
    rabbitMqUrl: 'amqp://username:password@localhost:5672',
    queueHandlers: new Map([['orders', [orderHandler]]])
  });
  await consumer.connect();
}

// Consumer Group 2: Notifications (2 instances)
// All share the same 'notifications' queue
for (let i = 1; i <= 2; i++) {
  const consumer = new EventBusConsumer({
    rabbitMqUrl: 'amqp://username:password@localhost:5672',
    queueHandlers: new Map([['notifications', [notificationHandler]]])
  });
  await consumer.connect();
}
```

### Monitoring for Scaling Decisions

```typescript
// Monitor queue depth to make scaling decisions
// Use RabbitMQ Management API or CLI

// Example: Check queue depth
// rabbitmqadmin list queues name messages

// Scale up when:
// - Queue depth consistently > 1000 messages
// - Consumer processing time increases
// - Message age increases

// Scale down when:
// - Queue depth consistently near 0
// - Multiple consumers idle
// - Processing capacity exceeds demand
```

## Performance Tips

### 1. Connection Reuse

**DO**: Create one producer/consumer per service instance
```typescript
// Good: Single producer for the entire service
class OrderService {
  private producer: EventBusProducer;
  
  async init() {
    this.producer = new EventBusProducer({
      rabbitMqUrl: 'amqp://username:password@localhost:5672'
    });
    await this.producer.connect();
  }
  
  async createOrder(order: Order) {
    await this.producer.publish('orders', order);
  }
}
```

**DON'T**: Create new connections per operation
```typescript
// Bad: Creates new connection for each publish
async function sendMessage(data: any) {
  const producer = new EventBusProducer({ rabbitMqUrl: '...' });
  await producer.connect();
  await producer.publish('queue', data);
  await producer.disconnect();  // Expensive!
}
```

### 2. Message Batching

For high-throughput scenarios, batch multiple operations:

```typescript
// Batch publishing
const messages = [
  { orderId: 1, ... },
  { orderId: 2, ... },
  { orderId: 3, ... }
];

await producer.connect();
for (const message of messages) {
  await producer.publish('orders', message);  // Uses same connection
}
// Confirms are awaited per message, ensuring reliability
```

### 3. Non-Blocking Handlers

Keep handlers async and non-blocking:

```typescript
// Good: Non-blocking handler
const handler: EventHandler = async (data) => {
  // Quick processing
  await database.insert(data);
  
  // Offload heavy work to another queue
  await producer.publish('heavy_processing', data);
};

// Bad: Blocking handler
const slowHandler: EventHandler = async (data) => {
  // Blocks other messages for 10 seconds
  await heavyComputation(data);  // 10 seconds
  await database.insert(data);
};
```

### 4. Prefetch Optimization

The library sets `prefetch=1` by default for fair distribution. This means:
- Each consumer gets one message at a time
- Fast consumers get more messages
- Slow consumers don't get overwhelmed

For specialized scenarios:
```typescript
// Current behavior (prefetch=1):
// - Fair distribution across consumers
// - Prevents consumer overload
// - Ideal for most use cases

// If you need different prefetch values, you would need to
// modify the consumer.ts source code (line: channel.prefetch(1))
```

### 5. Connection Pooling

For microservices with multiple queues:

```typescript
// Single consumer handles multiple queues
const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers: new Map([
    ['queue1', [handler1]],
    ['queue2', [handler2]],
    ['queue3', [handler3]]
  ])
});
// One connection, multiple queues - efficient!
```

### 6. Monitor Queue Depths

```typescript
// Implement monitoring to track performance
const producer = new EventBusProducer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  onStateChange: (state) => {
    // Log state changes for monitoring
    metrics.recordConnectionState(state);
  }
});

// External monitoring with RabbitMQ Management API:
// - Queue depth (messages ready)
// - Consumer count
// - Message rate (in/out)
// - Consumer utilization
```

### 7. Message Size Optimization

```typescript
// Keep messages small for better throughput
// Good: Reference to data
await producer.publish('image_processing', {
  imageId: 12345,
  bucket: 's3://images',
  key: 'photo.jpg'
});

// Bad: Embedding large data
await producer.publish('image_processing', {
  imageData: base64Image  // Could be MBs!
});
```

### 8. Batch Acknowledgments

The library handles acknowledgments efficiently:
- Messages are acknowledged individually
- Failed messages are not requeued (nack without requeue)
- At least one handler must succeed for acknowledgment

## API Reference

### EventBusProducer

Producer for publishing messages to queues or broadcasting to exchanges.

#### Constructor

```typescript
new EventBusProducer(options: EventBusProducerOptions)
```

**Options**:
- `rabbitMqUrl: string` - RabbitMQ connection URL (e.g., `amqp://user:pass@host:5672`)
- `onStateChange?: (state: ConnectionState, reconnectCount?: number) => void` - Callback for connection state changes

#### Methods

##### `connect(): Promise<void>`

Establishes connection to RabbitMQ. Must be called before publishing.

```typescript
await producer.connect();
```

##### `publish(queueName: string, data: any): Promise<void>`

Publishes a message to a durable queue with persistence enabled. Message is delivered to one consumer (load-balanced).

**Parameters**:
- `queueName: string` - Name of the queue
- `data: any` - Message payload (will be JSON serialized)

**Throws**: Error if channel is not available or publish fails

```typescript
await producer.publish('orders', { orderId: 123, amount: 99.99 });
```

##### `broadcast(exchangeName: string, data: any): Promise<void>`

Broadcasts a message to all consumers listening on the fanout exchange. All consumers receive the message.

**Parameters**:
- `exchangeName: string` - Name of the fanout exchange
- `data: any` - Message payload (will be JSON serialized)

**Throws**: Error if channel is not available or broadcast fails

```typescript
await producer.broadcast('notifications', { type: 'alert', message: 'System update' });
```

##### `disconnect(): Promise<void>`

Gracefully closes the connection and stops reconnection attempts.

```typescript
await producer.disconnect();
```

### EventBusConsumer

Consumer for processing messages from queues and broadcast exchanges.

#### Constructor

```typescript
new EventBusConsumer(options: EventBusConsumerOptions)
```

**Options**:
- `rabbitMqUrl: string` - RabbitMQ connection URL
- `queueHandlers: QueueHandlers` - Map of queue names to handler arrays
- `onStateChange?: (state: ConnectionState, reconnectCount?: number) => void` - State change callback
- `exchangeBindings?: ExchangeBindings` - Map of queue names to exchange names (for broadcast mode)

#### Types

```typescript
type EventHandler<T = any> = (data: T) => Promise<void>;
type QueueHandlers = Map<string, EventHandler[]>;
type ExchangeBindings = Map<string, string>; // Map<queueName, exchangeName>
```

#### Methods

##### `connect(): Promise<void>`

Connects to RabbitMQ and starts consuming messages from configured queues.

```typescript
await consumer.connect();
```

##### `disconnect(): Promise<void>`

Gracefully cancels all consumers and closes the connection.

```typescript
await consumer.disconnect();
```

### ConnectionState

Enum representing connection states:

```typescript
enum ConnectionState {
  CONNECTED = 'CONNECTED',        // Successfully connected
  DISCONNECTED = 'DISCONNECTED',  // Intentionally disconnected
  RECONNECTING = 'RECONNECTING'   // Attempting reconnection
}
```

## Best Practices

### Error Handling in Handlers

```typescript
// Good: Handler with error handling
const orderHandler: EventHandler = async (data) => {
  try {
    await processOrder(data);
    await updateInventory(data);
  } catch (error) {
    console.error('Failed to process order:', error);
    // Log error for monitoring
    // Handler failure is caught by the library
    // Message is nacked if all handlers fail
  }
};

// Multiple handlers: At least one must succeed
const queueHandlers: QueueHandlers = new Map([
  ['orders', [
    orderHandler,        // If this fails...
    notificationHandler  // ...but this succeeds, message is acknowledged
  ]]
]);
```

### Graceful Shutdown

```typescript
// Always handle graceful shutdown
const consumer = new EventBusConsumer({
  rabbitMqUrl: 'amqp://username:password@localhost:5672',
  queueHandlers
});

await consumer.connect();

// Handle termination signals
const shutdown = async (signal: string) => {
  console.log(`${signal} received, shutting down gracefully...`);
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// For producers
process.on('beforeExit', async () => {
  await producer.disconnect();
});
```

### Message Persistence Configuration

**Point-to-Point** (Durable and Persistent):
```typescript
// Producer automatically creates durable queues
await producer.publish('tasks', data);
// - Queue survives broker restart (durable: true)
// - Messages survive broker restart (persistent: true)
// - Use for critical tasks that must not be lost
```

**Broadcast** (Transient):
```typescript
// Broadcast messages are transient
await producer.broadcast('notifications', data);
// - Exchange is non-durable
// - Messages are non-persistent
// - Queues are exclusive and auto-delete
// - Use for real-time updates that don't need persistence
```

### Handler Execution

Handlers execute **sequentially** within each message:

```typescript
const queueHandlers: QueueHandlers = new Map([
  ['orders', [
    handler1,  // Executes first
    handler2,  // Executes after handler1 completes
    handler3   // Executes after handler2 completes
  ]]
]);

// Acknowledgment rules:
// - If at least one handler succeeds -> message acknowledged
// - If all handlers fail -> message nacked (not requeued)
// - Failed handlers log errors but don't block subsequent handlers
```

### Connection Management

```typescript
// DO: Initialize once, use throughout application lifecycle
class MessageService {
  private producer: EventBusProducer;
  private consumer: EventBusConsumer;
  
  async initialize() {
    this.producer = new EventBusProducer({
      rabbitMqUrl: process.env.RABBITMQ_URL!,
      onStateChange: this.handleStateChange
    });
    
    this.consumer = new EventBusConsumer({
      rabbitMqUrl: process.env.RABBITMQ_URL!,
      queueHandlers: this.getHandlers()
    });
    
    await Promise.all([
      this.producer.connect(),
      this.consumer.connect()
    ]);
  }
  
  async shutdown() {
    await Promise.all([
      this.producer.disconnect(),
      this.consumer.disconnect()
    ]);
  }
  
  private handleStateChange(state: ConnectionState, reconnectCount?: number) {
    // Log state changes for monitoring/alerting
    logger.info({ state, reconnectCount }, 'RabbitMQ state change');
  }
}
```

### Testing

```typescript
// Use test containers or local RabbitMQ for testing
describe('EventBus', () => {
  let producer: EventBusProducer;
  let consumer: EventBusConsumer;
  
  beforeAll(async () => {
    producer = new EventBusProducer({
      rabbitMqUrl: 'amqp://guest:guest@localhost:5672'
    });
    await producer.connect();
  });
  
  afterAll(async () => {
    await producer.disconnect();
  });
  
  it('should process messages', async () => {
    const received: any[] = [];
    
    const handler: EventHandler = async (data) => {
      received.push(data);
    };
    
    consumer = new EventBusConsumer({
      rabbitMqUrl: 'amqp://guest:guest@localhost:5672',
      queueHandlers: new Map([['test_queue', [handler]]])
    });
    
    await consumer.connect();
    await producer.publish('test_queue', { test: 'data' });
    
    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 100));
    
    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({ test: 'data' });
    
    await consumer.disconnect();
  });
});
```

## License

MIT © Sang Lu

## Author

Sang Lu <connect.with.sang@gmail.com>
