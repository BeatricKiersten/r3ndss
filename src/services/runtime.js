const { EventEmitter } = require('events');
const { getInstance: getDb } = require('../db/handler');
const { VideoProcessor } = require('./VideoProcessor');
const { UploaderService } = require('./UploaderService');

const db = getDb();
const eventEmitter = new EventEmitter();
const videoProcessor = new VideoProcessor(db, eventEmitter);
const uploaderService = new UploaderService(db);

module.exports = {
  db,
  eventEmitter,
  videoProcessor,
  uploaderService
};
