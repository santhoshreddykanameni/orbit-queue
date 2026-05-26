export interface QueueConfig {
  type: "rabbitmq";

  url: string;

  prefetch?: number;
}

export class QueueClient {
  connect(): Promise<void>;

  publish(queue: string, message: any): Promise<void>;

  subscribe(queue: string, handler: Function): Promise<void>;

  close(): Promise<void>;
}

export function createClient(config: QueueConfig): Promise<QueueClient>;

export class BatchManager {
  constructor(size: number, interval: number, handler: Function);

  add(message: any): void;

  flush(): Promise<void>;

  close(): Promise<void>;
}
