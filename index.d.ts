export interface ReconnectConfig {
  retries?: number;

  minDelay?: number;

  maxDelay?: number;

  factor?: number;
}

export interface BufferConfig {
  maxSize?: number;
}

export interface PersistenceConfig {
  enabled?: boolean;

  file?: string;
}

export interface PublishOptions {
  ttl?: number;
}

export interface QueueConfig {
  type: "rabbitmq";

  url: string;

  heartbeat?: number;

  prefetch?: number;

  reconnect?: ReconnectConfig;

  buffer?: BufferConfig;

  persistence?: PersistenceConfig;
}

export interface QueueEvents {
  connected: () => void;

  disconnected: () => void;

  reconnecting: (delay: number) => void;

  warning: (message: string) => void;

  error: (error: Error) => void;

  subscribed: (queue: string) => void;

  buffered: () => void;

  offlineQueueFlushed: (count: number) => void;

  persistentQueueFlushed: (count: number) => void;
}

export class QueueClient {
  connect(): Promise<void>;

  publish(queue: string, message: any, options?: PublishOptions): Promise<void>;

  subscribe(
    queue: string,
    handler: (message: any) => Promise<void> | void,
  ): Promise<void>;

  close(): Promise<void>;

  on<K extends keyof QueueEvents>(event: K, listener: QueueEvents[K]): this;
}

export function createClient(config: QueueConfig): Promise<QueueClient>;

export class BatchManager {
  constructor(
    size: number,

    interval: number,

    handler: (messages: any[]) => Promise<void> | void,
  );

  add(message: any): void;

  flush(): Promise<void>;

  close(): Promise<void>;
}

declare const _default: {
  createClient: typeof createClient;

  BatchManager: typeof BatchManager;
};

export default _default;
