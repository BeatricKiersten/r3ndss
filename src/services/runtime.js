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

function normalizePipelineError(error, fallbackCode = 'PIPELINE_FAILED') {
  return {
    code: String(error?.code || fallbackCode).trim() || fallbackCode,
    message: String(error?.message || 'Pipeline failed').trim() || 'Pipeline failed',
    details: {
      cause: error?.cause || null,
      signal: error?.signal || null,
      statusCode: error?.statusCode || null
    }
  };
}

function buildPipelineState({
  fileId = null,
  processJobId = null,
  uploadQueue = null,
  uploadStatus = null,
  state = 'queued',
  skipped = false,
  reason = null,
  error = null
} = {}) {
  return {
    state,
    fileId,
    processJobId,
    uploadQueue,
    uploadStatus,
    skipped: Boolean(skipped),
    reason: reason || null,
    error: error ? normalizePipelineError(error) : null
  };
}

async function finalizeExistingFilePipeline(fileId, folderId = 'root', selectedProviders = null, options = {}) {
  const waitForUpload = options.waitForUpload === true;
  const file = await db.getFile(fileId);
  const pendingProviderInfo = await uploaderService.getPendingUploadProviders(fileId, selectedProviders);
  let uploadQueue = null;
  let uploadStatus = null;

  if (pendingProviderInfo.hasPendingProviders && file.localPath) {
    uploadQueue = await uploaderService.queueFileUpload(fileId, file.localPath, folderId, selectedProviders);
    if (waitForUpload && Array.isArray(uploadQueue?.jobs) && uploadQueue.jobs.length > 0) {
      uploadStatus = await uploaderService.waitForFileUploadCompletion(fileId, selectedProviders, options.waitOptions || {});
    }
  } else if (waitForUpload && !pendingProviderInfo.hasPendingProviders) {
    uploadStatus = await uploaderService.getUploadStatus(fileId);
  }

  const hasQueuedUploads = Array.isArray(uploadQueue?.jobs) && uploadQueue.jobs.length > 0;
  const state = pendingProviderInfo.hasPendingProviders
    ? (waitForUpload ? (hasQueuedUploads ? 'uploaded' : 'queued') : 'queued')
    : 'uploaded';

  let reason = null;
  if (!pendingProviderInfo.hasPendingProviders) {
    reason = 'File already exists on selected providers';
  } else if (!file.localPath) {
    reason = 'Existing file is missing local source';
  } else if (!hasQueuedUploads) {
    reason = 'Upload already queued';
  }

  return {
    file,
    pendingProviderInfo,
    uploadQueue,
    uploadStatus,
    pipeline: buildPipelineState({
      fileId,
      uploadQueue,
      uploadStatus,
      state,
      skipped: true,
      reason
    })
  };
}

async function runProcessUploadPipeline(hlsUrl, options = {}) {
  const {
    folderId = 'root',
    outputName = null,
    outputDir = null,
    decryptionKey = null,
    headers = {},
    cookies = null,
    skipIfExists = true,
    selectedProviders = null,
    waitForUpload = false,
    waitOptions = {}
  } = options;

  const processResult = await videoProcessor.processHls(hlsUrl, {
    folderId,
    outputName,
    outputDir,
    decryptionKey,
    headers,
    cookies,
    skipIfExists
  });

  if (processResult?.skipped) {
    const existing = await finalizeExistingFilePipeline(processResult.fileId, folderId, selectedProviders, {
      waitForUpload,
      waitOptions
    });

    return {
      process: processResult,
      uploadQueue: existing.uploadQueue,
      uploadStatus: existing.uploadStatus,
      pendingProviderInfo: existing.pendingProviderInfo,
      pipeline: buildPipelineState({
        fileId: processResult.fileId,
        processJobId: processResult.jobId || null,
        uploadQueue: existing.uploadQueue,
        uploadStatus: existing.uploadStatus,
        state: existing.pipeline.state,
        skipped: true,
        reason: processResult.reason || existing.pipeline.reason
      })
    };
  }

  const uploadQueue = await uploaderService.queueFileUpload(
    processResult.fileId,
    processResult.outputPath,
    folderId,
    selectedProviders
  );

  const uploadStatus = waitForUpload
    ? await uploaderService.waitForFileUploadCompletion(processResult.fileId, selectedProviders, waitOptions)
    : null;

  return {
    process: processResult,
    uploadQueue,
    uploadStatus,
    pipeline: buildPipelineState({
      fileId: processResult.fileId,
      processJobId: processResult.jobId || null,
      uploadQueue,
      uploadStatus,
      state: waitForUpload ? 'uploaded' : 'queued'
    })
  };
}

module.exports = {
  db,
  eventEmitter,
  videoProcessor,
  uploaderService,
  rcloneServeService,
  cleanupService,
  runProcessUploadPipeline,
  finalizeExistingFilePipeline,
  buildPipelineState,
  normalizePipelineError
};
