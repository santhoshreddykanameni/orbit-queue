const { createClient } = require("./core/QueueFactory");

const BatchManager = require("./batching/BatchManager");

module.exports = {
  createClient,
  BatchManager,
};
