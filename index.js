const { createClient } = require("./src/core/QueueFactory");

const BatchManager = require("./src/batching/BatchManager");

module.exports = {
  createClient,
  BatchManager,
};
