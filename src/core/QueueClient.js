const EventEmitter = require("events");

const OfflineQueue = require("./OfflineQueue");

const PersistentQueue = require("./PersistentQueue");

const DEFAULT_CONFIG = {
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
};

class QueueClient extends EventEmitter {
  constructor(config = {}) {
    super();

    this.config = {
      ...DEFAULT_CONFIG,

      ...config,

      reconnect: {
        ...DEFAULT_CONFIG.reconnect,

        ...(config.reconnect || {}),
      },

      buffer: {
        ...DEFAULT_CONFIG.buffer,

        ...(config.buffer || {}),
      },

      persistence: {
        ...DEFAULT_CONFIG.persistence,

        ...(config.persistence || {}),
      },
    };

    this.connected = false;

    this.subscriptions = new Map();

    this.offlineQueue = new OfflineQueue(this.config.buffer);

    this.persistentQueue = new PersistentQueue(this.config.persistence);
  }

  /* -------------------------------------------------- */
  /* ---------------- SAFE ERROR EMIT ----------------- */
  /* -------------------------------------------------- */

  safeEmitError(err) {
    if (this.listenerCount("error") > 0) {
      this.emit("error", err);
    } else {
      console.error("[orbit-queue]", err);
    }
  }

  /* -------------------------------------------------- */
  /* ---------------- PUBLISH ------------------------- */
  /* -------------------------------------------------- */

  async publish(queue, message, options = {}) {
    // buffer if disconnected
    if (!this.connected) {
      const item = {
        queue,
        message,
        options,
      };

      if (this.config.persistence?.enabled) {
        this.persistentQueue.push(item);
      } else {
        this.offlineQueue.push(item);
      }

      this.emit("buffered");

      return;
    }

    // try immediate publish
    try {
      return await this._publishNow(queue, message, options);
    } catch (err) {
      this.safeEmitError(err);

      // re-buffer on failure
      const item = {
        queue,
        message,
        options,
      };

      if (this.config.persistence?.enabled) {
        this.persistentQueue.push(item);
      } else {
        this.offlineQueue.push(item);
      }
    }
  }

  /* -------------------------------------------------- */
  /* ---------------- FLUSH OFFLINE ------------------- */
  /* -------------------------------------------------- */

  async flushOfflineQueue() {
    await this.offlineQueue.drain(async (items) => {
      for (const item of items) {
        try {
          await this._publishNow(item.queue, item.message, item.options);
        } catch (err) {
          this.safeEmitError(err);

          // re-buffer failed item
          this.offlineQueue.push(item);
        }
      }

      this.emit("offlineQueueFlushed", items.length);
    });
  }

  /* -------------------------------------------------- */
  /* ---------------- FLUSH PERSISTENT ---------------- */
  /* -------------------------------------------------- */

  async flushPersistentQueue() {
    await this.persistentQueue.drain(async (items) => {
      for (const item of items) {
        try {
          await this._publishNow(item.queue, item.message, item.options);
        } catch (err) {
          this.safeEmitError(err);

          // re-buffer failed item
          this.persistentQueue.push(item);
        }
      }

      this.emit("persistentQueueFlushed", items.length);
    });
  }

  /* -------------------------------------------------- */
  /* ---------------- RESTORE SUBSCRIPTIONS ----------- */
  /* -------------------------------------------------- */

  async restoreSubscriptions() {
    for (const [queue, handler] of this.subscriptions) {
      try {
        await this.subscribe(queue, handler, true);
      } catch (err) {
        this.safeEmitError(err);
      }
    }
  }
}

module.exports = QueueClient;
