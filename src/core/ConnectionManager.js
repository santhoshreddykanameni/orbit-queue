const EventEmitter = require("events");

class ConnectionManager extends EventEmitter {
  constructor(connectFn, config = {}) {
    super();

    this.connectFn = connectFn;

    this.retries = 0;

    this.reconnectTimer = null;

    this.maxRetries = config.reconnect?.retries ?? Infinity;

    this.minDelay = config.reconnect?.minDelay ?? 1000;

    this.maxDelay = config.reconnect?.maxDelay ?? 30000;

    this.factor = config.reconnect?.factor ?? 2;

    this.connect();
  }

  async connect() {
    try {
      await this.connectFn();

      this.retries = 0;

      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);

        this.reconnectTimer = null;
      }

      this.emit("connected");
    } catch (err) {
      if (this.listenerCount("error") > 0) {
        this.emit("error", err);
      } else {
        console.error("[orbit-queue]", err.message);
      }

      this.scheduleReconnect();
    }
  }

  scheduleReconnect() {
    // already scheduled
    if (this.reconnectTimer) {
      return;
    }

    // max retries reached
    if (this.retries >= this.maxRetries) {
      console.error("[orbit-queue] Max reconnect retries reached");

      return;
    }

    const delay = Math.min(
      this.minDelay * Math.pow(this.factor, this.retries),
      this.maxDelay,
    );

    this.retries++;

    this.emit("reconnecting", delay);

    this.reconnectTimer = setTimeout(async () => {
      // clear timer BEFORE connect
      this.reconnectTimer = null;

      await this.connect();
    }, delay);
  }

  close() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);

      this.reconnectTimer = null;
    }
  }
}

module.exports = ConnectionManager;
