const amqp = require("amqplib");

const QueueClient = require("../core/QueueClient");

const ConnectionManager = require("../core/ConnectionManager");

class RabbitMQAdapter extends QueueClient {
  constructor(config) {
    super(config);

    this.conn = null;

    this.publishChannel = null;

    this.consumeChannel = null;

    this.connectionManager = null;

    this._closing = false;

    this.queues = new Set();

    this.consumerTags = new Map();
  }

  async connect() {
    if (this.conn) return;

    this._closing = false;

    this.connectionManager = new ConnectionManager(async () => {
      if (this.conn) return;

      this.conn = await amqp.connect(this.config.url, {
        heartbeat: this.config.heartbeat || 30,
      });

      this.conn.on("error", (err) => {
        this.safeEmitError(err);
      });

      this.conn.on("close", async () => {
        this.connected = false;

        this.conn = null;

        this.publishChannel = null;

        this.consumeChannel = null;

        this.consumerTags.clear();

        this.queues.clear();

        this.emit("disconnected");

        if (!this._closing) {
          this.connectionManager.scheduleReconnect();
        }
      });

      // publisher channel
      this.publishChannel = await this.conn.createConfirmChannel();

      // consumer channel
      this.consumeChannel = await this.conn.createChannel();

      await this.consumeChannel.prefetch(this.config.prefetch || 100);

      // HEALTH CHECK
      this.startHealthMonitor();
    }, this.config);

    this.connectionManager.on("connected", async () => {
      this.warningShown = false;

      try {
        this.connected = true;

        await this.restoreSubscriptions();

        await this.flushPersistentQueue();

        await this.flushOfflineQueue();

        this.emit("connected");
      } catch (err) {
        this.safeEmitError(err);

        this.connectionManager.scheduleReconnect();
      }
    });

    this.connectionManager.on("reconnecting", (delay) => {
      this.emit("reconnecting", delay);
    });
  }

  /* -------------------------------------------------- */
  /* ---------------- HEALTH MONITOR ------------------ */
  /* -------------------------------------------------- */

  startHealthMonitor() {
    if (this.healthInterval) return;

    this.healthInterval = setInterval(() => {
      if (this._closing) return;

      if (!this.conn) {
        if (!this.warningShown) {
          this.warningShown = true;

          this.emit("warning", "RabbitMQ connection missing");
        }

        this.connectionManager.scheduleReconnect();

        return;
      }

      if (!this.consumeChannel) {
        if (!this.warningShown) {
          this.warningShown = true;

          this.emit("warning", "Consumer channel missing");
        }

        this.connectionManager.scheduleReconnect();
      }
    }, 10000);
  }

  stopHealthMonitor() {
    if (this.healthInterval) {
      clearInterval(this.healthInterval);

      this.healthInterval = null;
    }
  }

  /* -------------------------------------------------- */
  /* ---------------- ASSERT QUEUE -------------------- */
  /* -------------------------------------------------- */

  async assertQueue(queue) {
    if (this.queues.has(queue)) return;

    await this.consumeChannel.assertQueue(queue, {
      durable: true,
    });

    this.queues.add(queue);
  }

  /* -------------------------------------------------- */
  /* ---------------- PUBLISH ------------------------- */
  /* -------------------------------------------------- */

  async _publishNow(queue, message) {
    if (!this.connected || !this.publishChannel) {
      throw new Error("RabbitMQ not connected");
    }

    try {
      await this.assertQueue(queue);

      this.publishChannel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify(message)),
        {
          persistent: true,
        },
      );
    } catch (err) {
      this.safeEmitError(err);
    }
  }

  /* -------------------------------------------------- */
  /* ---------------- SUBSCRIBE ----------------------- */
  /* -------------------------------------------------- */

  async subscribe(queue, handler, restore = false) {
    if (!this.connected || !this.consumeChannel) {
      await new Promise((resolve) => this.once("connected", resolve));
    }

    if (!restore) {
      this.subscriptions.set(queue, handler);
    }

    await this.assertQueue(queue);

    const channel = this.consumeChannel;

    const result = await channel.consume(
      queue,
      async (msg) => {
        if (!msg) return;

        try {
          const data = JSON.parse(msg.content.toString());

          await handler(data);

          try {
            channel.ack(msg);
          } catch {}
        } catch (err) {
          this.safeEmitError(err);

          try {
            channel.nack(msg, false, false);
          } catch {}
        }
      },
      {
        noAck: false,
      },
    );

    this.consumerTags.set(queue, result.consumerTag);

    this.emit("subscribed", queue);
  }

  /* -------------------------------------------------- */
  /* ---------------- CLOSE --------------------------- */
  /* -------------------------------------------------- */

  async close() {
    this._closing = true;

    this.stopHealthMonitor();

    try {
      if (this.publishChannel) {
        await this.publishChannel.close();

        this.publishChannel = null;
      }

      if (this.consumeChannel) {
        await this.consumeChannel.close();

        this.consumeChannel = null;
      }

      if (this.conn) {
        await this.conn.close();

        this.conn = null;
      }
    } catch (err) {
      this.safeEmitError(err);
    }

    this.connected = false;
  }
}

module.exports = RabbitMQAdapter;
