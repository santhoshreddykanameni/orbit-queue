# @orbit-stream/message-queue

Production-ready RabbitMQ client for Node.js applications with:

- Auto reconnect
- Heartbeat monitoring
- Persistent publishing
- Offline buffering
- Queue restoration
- Consumer recovery
- Batch publishing
- Message TTL support

---

# Installation

```bash
npm install @orbit-stream/message-queue
```

---

# Features

- RabbitMQ connection management
- Automatic reconnect handling
- Durable queues
- Persistent messages
- Message TTL support
- Offline queue buffering
- Queue restoration after reconnect
- Consumer auto recovery
- Batch publishing support
- Exponential reconnect backoff
- Heartbeat monitoring
- EventEmitter-based architecture

---

# Quick Start

## Producer + Consumer

```js
const { createClient } = require("@orbit-stream/message-queue");

(async () => {
  const client = await createClient({
    type: "rabbitmq",

    url: "amqp://localhost",
  });

  client.on("connected", () => {
    console.log("Connected");
  });

  client.on("reconnecting", (delay) => {
    console.log(`Reconnecting in ${delay}ms`);
  });

  client.on("error", (err) => {
    console.error(err);
  });

  await client.subscribe("test-queue", async (message) => {
    console.log("Received:", message);
  });

  await client.publish("test-queue", {
    hello: "world",
  });
})();
```

---

# Configuration

```js
const client = await createClient({
  type: "rabbitmq",

  url: "amqp://localhost",
});
```

---

# Default Configuration

These values are automatically applied internally:

```js
{
  heartbeat: 30,

  prefetch: 100,

  reconnect: {
    retries: Infinity,
    minDelay: 1000,
    maxDelay: 30000,
    factor: 2,
  },

  buffer: {
    maxSize: 100000,
  },

  persistence: {
    enabled: false,
  },
}
```

---

# Override Defaults

```js
const client = await createClient({
  type: "rabbitmq",

  url: "amqp://localhost",

  heartbeat: 10,

  prefetch: 500,
});
```

---

# Publish Message

```js
await client.publish("telemetry", {
  temperature: 24,
});
```

---

# Message TTL

Automatically delete messages after a duration.

## Store for 1 minute only

```js
await client.publish("telemetry", data, {
  ttl: 60000,
});
```

If message is not consumed within 60 seconds:

- RabbitMQ automatically removes it

---

# Durable Messages

Messages are automatically published with:

```js
persistent: true;
```

Queues are automatically created with:

```js
durable: true;
```

This means messages survive:

- RabbitMQ restart
- AmazonMQ maintenance
- Consumer disconnects
- Reconnects

---

# Subscribe to Queue

```js
await client.subscribe("telemetry", async (message) => {
  console.log(message);
});
```

---

# Acknowledgements

Messages are only removed after successful processing.

```txt
Receive message
    ↓
Process message
    ↓
ACK sent
    ↓
Message removed
```

If consumer crashes before ACK:

- RabbitMQ requeues message

---

# Batch Manager

```js
const { BatchManager } = require("@orbit-stream/message-queue");

const batch = new BatchManager(100, 5000, async (messages) => {
  console.log(messages.length);
});

batch.add({
  value: 1,
});
```

## Parameters

| Parameter | Description          |
| --------- | -------------------- |
| size      | Max batch size       |
| interval  | Flush interval in ms |
| handler   | Batch processor      |

---

# Auto Reconnect

The client automatically reconnects when:

- RabbitMQ restarts
- AmazonMQ maintenance occurs
- Network interruptions happen
- DNS temporarily fails
- Heartbeat timeout occurs

---

# Heartbeat Monitoring

Heartbeat monitoring is enabled by default:

```js
heartbeat: 30;
```

This detects dead TCP connections automatically.

Especially important for:

- AmazonMQ
- Kubernetes
- Docker
- Cloud environments

---

# Event Listeners

## Connected

```js
client.on("connected", () => {
  console.log("Connected");
});
```

---

## Reconnecting

```js
client.on("reconnecting", (delay) => {
  console.log(delay);
});
```

---

## Error

```js
client.on("error", (err) => {
  console.error(err);
});
```

---

## Warning

```js
client.on("warning", (msg) => {
  console.warn(msg);
});
```

---

# Close Connection

```js
await client.close();
```

---

# Production Recommendations

## Use heartbeat

```js
heartbeat: 30;
```

---

## Use durable queues

Enabled automatically.

---

## Use persistent messages

Enabled automatically.

---

## Handle errors

```js
client.on("error", console.error);
```

---

## Use TTL for realtime telemetry

```js
ttl: 60000;
```

---

# Example Architecture

```txt
Telemetry Producer
        ↓
RabbitMQ
        ↓
@orbit-stream/message-queue
        ↓
Consumer Services
```

---

# Roadmap

Planned future adapters:

- Amazon SQS
- ActiveMQ
- BullMQ

---

# License

MIT
