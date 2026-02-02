import amqplib from 'amqplib'
import { logger } from '@a_jackie_z/logger'

export const ConnectionState = {
  CONNECTED: 'CONNECTED',
  DISCONNECTED: 'DISCONNECTED',
  RECONNECTING: 'RECONNECTING'
} as const;

export type ConnectionState = typeof ConnectionState[keyof typeof ConnectionState];

export interface BaseEventBusConnectionOptions {
  rabbitMqUrl: string;
  onStateChange?: (state: ConnectionState, reconnectCount?: number) => void;
}

export abstract class BaseEventBusConnection<T extends amqplib.Channel> {
  protected channelModel: amqplib.ChannelModel | null = null;
  protected channel: T | null = null;
  protected readonly rabbitMqUrl: string;
  protected readonly onStateChange: ((state: ConnectionState, reconnectCount?: number) => void) | undefined;
  protected reconnectTimer: NodeJS.Timeout | null = null;
  protected isReconnecting: boolean = false;
  protected shouldReconnect: boolean = true;
  protected reconnectCount: number = 0;

  constructor(options: BaseEventBusConnectionOptions) {
    this.rabbitMqUrl = options.rabbitMqUrl;
    this.onStateChange = options.onStateChange;
  }

  /**
   * Abstract method for creating and setting up the channel.
   * Subclasses must implement this to define their specific channel type and setup logic.
   */
  protected abstract createChannelAndSetup(): Promise<void>;

  async connect() {
    try {
      this.channelModel = await amqplib.connect(this.rabbitMqUrl);

      // Setup connection error handlers
      this.channelModel.on('error', (error) => {
        logger.error({ error }, 'RabbitMQ connection error');
        this.scheduleReconnect();
      });

      this.channelModel.on('close', () => {
        logger.debug('RabbitMQ connection closed');
        this.scheduleReconnect();
      });

      // Create and setup channel (subclass-specific)
      await this.createChannelAndSetup();

      // Setup channel error handlers
      if (this.channel) {
        this.channel.on('error', (error) => {
          logger.error({ error }, 'RabbitMQ channel error');
          this.scheduleReconnect();
        });

        this.channel.on('close', () => {
          logger.debug('RabbitMQ channel closed');
          this.scheduleReconnect();
        });
      }

      // Reset reconnect count on successful connection
      this.reconnectCount = 0;
      this.isReconnecting = false;

      // Notify state change
      this.notifyStateChange(ConnectionState.CONNECTED);

      logger.info({ state: ConnectionState.CONNECTED }, 'RabbitMQ connection established successfully');
      return this.channel;
    } catch (error) {
      logger.error({ error }, 'Failed to connect to RabbitMQ');
      this.scheduleReconnect();
      throw error;
    }
  }

  protected scheduleReconnect() {
    if (!this.shouldReconnect || this.isReconnecting) {
      return;
    }

    this.isReconnecting = true;

    // Clear existing timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    this.reconnectCount++;

    // Notify state change
    this.notifyStateChange(ConnectionState.RECONNECTING, this.reconnectCount);

    logger.info({ reconnectCount: this.reconnectCount, state: ConnectionState.RECONNECTING }, 'Reconnection attempt scheduled in 15 seconds');

    this.reconnectTimer = setTimeout(async () => {
      await this.reconnect();
    }, 15000);
  }

  protected async reconnect() {
    try {
      logger.info({ reconnectCount: this.reconnectCount }, 'Attempting to reconnect');

      // Nullify existing connections
      this.channel = null;
      this.channelModel = null;

      // Attempt to reconnect
      await this.connect();
    } catch (error) {
      logger.error({ error, reconnectCount: this.reconnectCount }, 'Reconnection failed');
      this.isReconnecting = false;
      this.scheduleReconnect();
    }
  }

  protected notifyStateChange(state: ConnectionState, reconnectCount?: number) {
    if (this.onStateChange) {
      try {
        this.onStateChange(state, reconnectCount);
      } catch (error) {
        logger.error({ error }, 'Error in state change callback');
      }
    }
  }

  async disconnect() {
    logger.info({ state: ConnectionState.DISCONNECTED }, 'Disconnecting from RabbitMQ');

    this.shouldReconnect = false;

    // Notify state change
    this.notifyStateChange(ConnectionState.DISCONNECTED);

    // Clear reconnection timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    // Close channel and connection
    if (this.channel) {
      try {
        await this.channel.close();
      } catch (error: any) {
        // Ignore errors if channel is already closed
        const errorMessage = error?.message || '';
        if (errorMessage.includes('Channel closed') ||
            errorMessage.includes('IllegalOperationError')) {
          logger.debug('Channel already closed');
        } else {
          logger.error({ error }, 'Error closing channel');
        }
      }
    }

    if (this.channelModel) {
      try {
        await this.channelModel.close();
      } catch (error: any) {
        // Ignore errors if connection is already closed or closing
        const errorMessage = error?.message || '';
        if (errorMessage.includes('Connection closed') ||
            errorMessage.includes('Connection closing') ||
            errorMessage.includes('IllegalOperationError')) {
          logger.debug('Connection already closed or closing');
        } else {
          logger.error({ error }, 'Error closing connection');
        }
      }
    }

    logger.info('Disconnected from RabbitMQ');
  }
}
