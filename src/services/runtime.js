const { EventEmitter } = require('events');
const { getInstance: getDb } = require('../db/handler');
const { VideoProcessor } = require('./VideoProcessor');
const { UploaderService } = require('./UploaderService');
const { CleanupService } = require('./CleanupService');
const { RcloneServeService } = require('./RcloneServeService');
const config = require('../config');

const db = getDb();
const eventEmitter = new EventEmitter();
const videoProcessor = new VideoProcessor(db, eventEmitter);
const uploaderService = new UploaderService(db);
const rcloneServeService = new RcloneServeService(db);
const cleanupService = new CleanupService({
  db,
  uploadDir: config.uploadDir,
  uploaderService
});

module.exports = {
  db,
  eventEmitter,
  videoProcessor,
  uploaderService,
  rcloneServeService,
  cleanupService
};
