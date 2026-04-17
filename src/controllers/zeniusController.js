const axios = require('axios');
const { randomUUID } = require('crypto');
const path = require('path');
const fs = require('fs-extra');
const pLimit = require('p-limit');
const { EventEmitter } = require('events');

const config = require('../config');
const { db, videoProcessor, uploaderService, eventEmitter, cleanupService } = require('../services/runtime');
const webhookService = require('../services/webhookService');
const { buildRequestContext, stripWrappingQuotes } = require('../services/requestContext');
const {
  resolveReferer,
  getJson: getUpstreamJson,
  getInstanceDetails,
  ZENIUS_BASE_URL
} = require('../services/zeniusUpstreamService');

const DEFAULT_BATCH_ROOT_CGROUP_ID = '34';
const DEFAULT_BATCH_PARENT_CONTAINER_NAME = '';
const UPSTREAM_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_UPSTREAM_TIMEOUT_MS || '12000', 10);
const UPSTREAM_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_UPSTREAM_MAX_RETRIES || '3', 10);
const UPSTREAM_RETRY_BASE_DELAY_MS = Number.parseInt(process.env.ZENIUS_UPSTREAM_RETRY_BASE_DELAY_MS || '500', 10);
const BATCH_CG_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_CG_FETCH_CONCURRENCY || '4', 10);
const BATCH_CONTAINER_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_CONTAINER_FETCH_CONCURRENCY || '4', 10);
const BATCH_DETAIL_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_DETAIL_FETCH_CONCURRENCY || '4', 10);
const BATCH_INSTANCE_METADATA_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_INSTANCE_METADATA_CONCURRENCY || '6', 10);
const MAX_BATCH_ERRORS = Number.parseInt(process.env.ZENIUS_MAX_BATCH_ERRORS || '100', 10);
const DEFAULT_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_CHUNK_SIZE || '8', 10);
const MAX_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_MAX_CHUNK_SIZE || '20', 10);
const DEFAULT_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_BUDGET_MS || '24000', 10);
const MAX_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_MAX_BUDGET_MS || '28000', 10);
const BATCH_SESSION_TTL_MS = Number.parseInt(process.env.ZENIUS_BATCH_SESSION_TTL_MS || '7200000', 10);
const BATCH_UPSTREAM_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_TIMEOUT_MS || '7000', 10);
const BATCH_UPSTREAM_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_MAX_RETRIES || '1', 10);
const BATCH_DEADLINE_GUARD_MS = Number.parseInt(process.env.ZENIUS_BATCH_DEADLINE_GUARD_MS || '1200', 10);
const BACKGROUND_BATCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_CHUNK_SIZE || '6', 10);
const BACKGROUND_BATCH_RUN_TTL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_RUN_TTL_MS || '21600000', 10);
const BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS || '20000', 10);
const BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS || '10000', 10);
// Max iterations for background batch loop (configurable to prevent infinite loops)
const BACKGROUND_BATCH_MAX_ITERATIONS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_MAX_ITERATIONS || '120', 10);
const INSTANCE_METADATA_CACHE_TTL_MS = Number.parseInt(process.env.ZENIUS_INSTANCE_METADATA_CACHE_TTL_MS || '1800000', 10);
const BATCH_METADATA_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BATCH_METADATA_TIMEOUT_MS || '15000', 10);
const BATCH_METADATA_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BATCH_METADATA_MAX_RETRIES || '4', 10);
// Caps for in-memory arrays to prevent unbounded memory growth
const MAX_QUEUED_ITEMS_IN_MEMORY = Number.parseInt(process.env.ZENIUS_MAX_QUEUED_ITEMS_IN_MEMORY || '500', 10);
const MAX_SKIPPED_ITEMS_IN_MEMORY = Number.parseInt(process.env.ZENIUS_MAX_SKIPPED_ITEMS_IN_MEMORY || '500', 10);
const MAX_ITEM_ERRORS_IN_MEMORY = Number.parseInt(process.env.ZENIUS_MAX_ITEM_ERRORS_IN_MEMORY || '200', 10);
const batchChainSessions = new Map();
const backgroundBatchRuns = new Map();
let backgroundBatchKeepaliveTimer = null;
let backgroundBatchKeepaliveUrl = String(process.env.ZENIUS_BATCH_KEEPALIVE_URL || '').trim();

// DB write retry config
const DB_WRITE_RETRY_ATTEMPTS = Number.parseInt(process.env.ZENIUS_DB_WRITE_RETRY_ATTEMPTS || '2', 10);
const DB_WRITE_RETRY_DELAY_MS = Number.parseInt(process.env.ZENIUS_DB_WRITE_RETRY_DELAY_MS || '500', 10);

/**
 * Retry a DB write operation on transient failure.
 * Returns the result of fn() on success, null on final failure (never throws).
 */
async function withDbRetry(fn, label = 'db-write') {
  let lastError;
  for (let attempt = 1; attempt <= DB_WRITE_RETRY_ATTEMPTS + 1; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt <= DB_WRITE_RETRY_ATTEMPTS) {
        const delay = DB_WRITE_RETRY_DELAY_MS * attempt;
        console.warn(`[Zenius] ${label} attempt ${attempt} failed, retrying in ${delay}ms: ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  console.error(`[Zenius] ${label} failed after ${DB_WRITE_RETRY_ATTEMPTS + 1} attempts: ${lastError?.message}`);
  return null;
}

// Download concurrency control
const DEFAULT_MAX_CONCURRENT_DOWNLOADS = Number.parseInt(process.env.ZENIUS_MAX_CONCURRENT_DOWNLOADS || '10', 10);

class DownloadQueue {
  constructor(maxConcurrent) {
    this._maxConcurrent = Math.max(1, maxConcurrent || DEFAULT_MAX_CONCURRENT_DOWNLOADS);
    this._active = new Map();
    this._queue = [];
    this._processing = false;
    this._nextTaskId = 0;
    this._drainResolvers = [];
  }

  get maxConcurrent() { return this._maxConcurrent; }
  get activeCount() { return this._active.size; }
  get queuedCount() { return this._queue.length; }
  get isProcessing() { return this._processing; }
  get activeTasks() { return Array.from(this._active.values()).map((t) => ({ id: t.id, urlShortId: t.urlShortId, outputName: t.outputName, fileId: t.fileId || null, jobId: t.jobId || null, startTime: t.startTime })); }
  get queuedTasks() { return this._queue.map((t) => ({ urlShortId: t.urlShortId, outputName: t.outputName })); }

  setMaxConcurrent(value) {
    const n = Math.max(1, Math.min(50, Number(value) || DEFAULT_MAX_CONCURRENT_DOWNLOADS));
    this._maxConcurrent = n;
    this._scheduleNext();
    return n;
  }

  _canStart() { return this._active.size < this._maxConcurrent; }

  add(task) {
    const id = ++this._nextTaskId;
    return new Promise((resolve, reject) => {
      const wrapped = { ...task, id, resolve, reject, cancelled: false, startTime: null };
      if (this._canStart() && !this._processing) {
        this._execute(wrapped);
      } else {
        this._queue.push(wrapped);
        this._scheduleNext();
      }
    });
  }

  _scheduleNext() {
    if (this._processing) return;
    setImmediate(() => this._drain());
  }

  async _drain() {
    if (this._processing) return;
    this._processing = true;

    while (this._queue.length > 0 && this._canStart()) {
      const task = this._queue.shift();
      if (!task || task.cancelled) continue;
      this._execute(task);
    }

    this._processing = false;

    if (this._active.size === 0 && this._queue.length === 0) {
      const resolvers = this._drainResolvers;
      this._drainResolvers = [];
      resolvers.forEach((r) => r());
    }
  }

  async _execute(task) {
    task.startTime = Date.now();
    this._active.set(task.id, task);

    try {
      const result = await this._runDownload(task);
      task.resolve(result);
    } catch (error) {
      task.reject(error);
    } finally {
      this._active.delete(task.id);
      this._scheduleNext();
    }
  }

  async _runDownload(task) {
    const resolvedTask = await resolveQueuedDownloadTask(task);
    const { urlShortId, videoUrl, folderId, outputName, ffmpegHeaders, selectedProviders } = resolvedTask;

    console.log(`[DownloadQueue] Starting #${task.id} ${urlShortId} (active: ${this._active.size}/${this._maxConcurrent}, queued: ${this._queue.length})`);

    let fileId = null;
    const startTime = Date.now();

    broadcastDownloadProgress({
      urlShortId,
      taskId: task.id,
      phase: 'starting',
      progress: 0,
      message: 'Starting download'
    });

    try {
      const result = await videoProcessor.processHls(videoUrl, {
        folderId,
        outputName,
        outputDir: config.uploadDir,
        headers: ffmpegHeaders,
        skipIfExists: true,
        retrySourceResolver: async ({ attempt, previousError }) => {
          const refreshedTask = await resolveQueuedDownloadTask({
            ...task,
            videoUrl: null,
            ffmpegHeaders: null
          });

          console.warn('[DownloadQueue] Refetched instance details for retry', {
            taskId: task.id,
            urlShortId,
            attempt,
            previousError: previousError?.message || null,
            previousVideoUrl: videoUrl,
            nextVideoUrl: refreshedTask.videoUrl
          });

          return {
            hlsUrl: refreshedTask.videoUrl,
            headers: refreshedTask.ffmpegHeaders
          };
        }
      });

      fileId = result.fileId;
      task.fileId = result.fileId || null;
      task.jobId = result.jobId || null;

      broadcastDownloadProgress({
        urlShortId,
        taskId: task.id,
        phase: 'downloaded',
        progress: 50,
        fileId,
        message: 'Download complete, queuing upload'
      });

      if (result.skipped) {
        let uploadQueueResult = null;
        if (result.reason === 'File already exists') {
          uploadQueueResult = await uploaderService.queueFileUpload(result.fileId, result.outputPath, folderId, selectedProviders);
          await uploaderService.waitForFileUploadCompletion(result.fileId, selectedProviders);
        }
        console.log(`[DownloadQueue] #${task.id} ${urlShortId} skipped: ${result.reason}`);

        broadcastDownloadComplete({
          urlShortId,
          taskId: task.id,
          fileId,
          skipped: true,
          reason: result.reason,
          durationMs: Date.now() - startTime
        });

        return { success: true, id: task.id, fileId, urlShortId, skipped: true, reason: result.reason, uploadQueue: uploadQueueResult };
      }

      console.log(`[DownloadQueue] #${task.id} ${urlShortId} downloaded, fileId=${fileId}, queuing upload`);

      await uploaderService.queueFileUpload(result.fileId, result.outputPath, folderId, selectedProviders);
      await uploaderService.waitForFileUploadCompletion(result.fileId, selectedProviders);

      broadcastDownloadComplete({
        urlShortId,
        taskId: task.id,
        fileId,
        durationMs: Date.now() - startTime
      });

      return { success: true, id: task.id, fileId, urlShortId };
    } catch (error) {
      console.error(`[DownloadQueue] #${task.id} ${urlShortId} failed:`, error.message);

      broadcastDownloadFailed({
        urlShortId,
        taskId: task.id,
        error: error.message,
        durationMs: Date.now() - startTime
      });

      throw error;
    }
  }

  cancelAll() {
    const cancelled = [];

    for (const task of this._queue) {
      task.cancelled = true;
      task.resolve({ cancelled: true, id: task.id, reason: 'User cancelled all' });
      cancelled.push({ urlShortId: task.urlShortId, fromQueue: true });
    }
    this._queue.length = 0;

    for (const [id, task] of this._active.entries()) {
      cancelled.push({ id, urlShortId: task.urlShortId, fromActive: true });
    }

    return cancelled;
  }

  getStatus() {
    return {
      active: this._active.size,
      max: this._maxConcurrent,
      queued: this._queue.length,
      isProcessing: this._processing,
      activeTasks: this.activeTasks,
      queuedTasks: this.queuedTasks
    };
  }

  async drain() {
    if (this._active.size === 0 && this._queue.length === 0) return;
    return new Promise((resolve) => { this._drainResolvers.push(resolve); });
  }
}

const downloadQueue = new DownloadQueue(DEFAULT_MAX_CONCURRENT_DOWNLOADS);
const downloadEvents = new EventEmitter();
downloadEvents.setMaxListeners(50);

function broadcastDownloadProgress(data) {
  downloadEvents.emit('progress', data);
  eventEmitter.emit('download:progress', data);
}

function broadcastDownloadComplete(data) {
  downloadEvents.emit('completed', data);
  eventEmitter.emit('download:completed', data);
}

function broadcastDownloadFailed(data) {
  downloadEvents.emit('failed', data);
  eventEmitter.emit('download:failed', data);
}

function normalizeShortId(rawValue) {
  const value = String(rawValue || '').trim().replace(/:+$/g, '');
  if (!value) {
    throw new Error('urlShortId is required');
  }

  if (!/^[0-9]+$/.test(value)) {
    throw new Error('urlShortId must be numeric');
  }

  return value;
}

function sanitizeOutputName(rawName, fallback = 'zenius-video') {
  const parsed = path.parse(String(rawName || '').trim()).name;
  const normalized = parsed
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 180);

  return normalized || fallback;
}

function reserveUniqueOutputName(rawName, fallback, urlShortId, usedNames) {
  const baseName = sanitizeOutputName(rawName, fallback);
  if (!usedNames) {
    return baseName;
  }

  const normalizedKey = baseName.toLowerCase();
  if (!usedNames.has(normalizedKey)) {
    usedNames.add(normalizedKey);
    return baseName;
  }

  const withId = sanitizeOutputName(`${baseName}-${urlShortId}`, fallback);
  const withIdKey = withId.toLowerCase();
  if (!usedNames.has(withIdKey)) {
    usedNames.add(withIdKey);
    return withId;
  }

  let counter = 2;
  while (true) {
    const candidate = sanitizeOutputName(`${baseName}-${urlShortId}-${counter}`, fallback);
    const candidateKey = candidate.toLowerCase();
    if (!usedNames.has(candidateKey)) {
      usedNames.add(candidateKey);
      return candidate;
    }
    counter += 1;
  }
}

function sanitizePathSegment(value, fallback = 'unknown') {
  const normalized = String(value || '')
    .trim()
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || fallback;
}

function sanitizePathSegments(values) {
  const list = Array.isArray(values) ? values : [values];
  return list
    .map((value) => sanitizePathSegment(value, ''))
    .filter(Boolean);
}

function buildContainerPath(parentSegments, containerName) {
  return [
    ...sanitizePathSegments(parentSegments),
    sanitizePathSegment(containerName, 'unknown-container')
  ].join('/');
}

function parseBoolean(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (value === 1) return true;
    if (value === 0) return false;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
    if (normalized === '1') return true;
    if (normalized === '0') return false;
  }
  return null;
}

function clampPositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForDownloadQueueCapacity({ cancelled = () => false, maxQueued = null, pollMs = 250 } = {}) {
  const normalizedMaxQueued = Number.isFinite(Number(maxQueued))
    ? Math.max(0, Number(maxQueued))
    : 0;

  while (!cancelled()) {
    const status = downloadQueue.getStatus();
    if (!status.isProcessing && status.active < downloadQueue.maxConcurrent && status.queued <= normalizedMaxQueued) {
      return;
    }
    await sleep(pollMs);
  }
}

function bodyPreview(data) {
  if (typeof data === 'string') {
    return data.slice(0, 500);
  }

  try {
    return JSON.stringify(data).slice(0, 500);
  } catch {
    return String(data).slice(0, 500);
  }
}

function pushBatchError(errors, errorInfo) {
  if (!Array.isArray(errors) || errors.length >= clampPositiveInt(MAX_BATCH_ERRORS, 100)) {
    return;
  }

  errors.push(errorInfo);
}

function normalizeChunkOffset(rawValue) {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function normalizeChunkLimit(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === '') {
    return null;
  }

  const parsed = clampPositiveInt(rawValue, clampPositiveInt(DEFAULT_BATCH_CHAIN_CHUNK_SIZE, 8));
  return Math.min(parsed, clampPositiveInt(MAX_BATCH_CHAIN_CHUNK_SIZE, 20));
}

function normalizeBatchRequestBudgetMs(rawValue) {
  const fallback = clampPositiveInt(DEFAULT_BATCH_REQUEST_BUDGET_MS, 24000);
  const parsed = clampPositiveInt(rawValue, fallback);
  return Math.min(parsed, clampPositiveInt(MAX_BATCH_REQUEST_BUDGET_MS, 28000));
}

function shouldStopForDeadline(deadlineAt, guardMs = BATCH_DEADLINE_GUARD_MS) {
  if (!deadlineAt) return false;
  return Date.now() >= (deadlineAt - clampPositiveInt(guardMs, 1200));
}

function normalizeSessionId(rawValue) {
  const value = String(rawValue || '').trim();
  return value || null;
}

function createBatchChainSession({ rootCgId, targetCgSelector, parentContainerName }) {
  const now = Date.now();
  return {
    id: randomUUID(),
    createdAt: now,
    updatedAt: now,
    expiresAt: now + clampPositiveInt(BATCH_SESSION_TTL_MS, 900000),
    rootCgId,
    rootCgName: null,
    targetCgSelector: String(targetCgSelector || '').trim() || null,
    targetCgId: normalizeTargetCgId(targetCgSelector),
    parentContainerName: sanitizePathSegment(parentContainerName, 'unknown-parent'),
    discoveryInitialized: false,
    discoveryDone: false,
    visitedCgIds: new Set([rootCgId]),
    queueCgIds: [],
    leafCgIds: [],
    leafCgIdSet: new Set(),
    leafCursor: 0,
    traversal: [],
    containerByShortId: new Map(),
    containerVideoInstancesByShortId: new Map(),
    instanceMetadataByShortId: new Map(),
    instanceMetadataPromisesByShortId: new Map(),
    cgPathById: new Map([[rootCgId, []]]),
    errors: []
  };
}

function touchBatchChainSession(session) {
  const now = Date.now();
  session.updatedAt = now;
  session.expiresAt = now + clampPositiveInt(BATCH_SESSION_TTL_MS, 7200000);
}

function cleanupExpiredBatchChainSessions() {
  const now = Date.now();
  const activeSessionIds = new Set();
  for (const run of backgroundBatchRuns.values()) {
    if (run?.status === 'running' && run?.sessionId) {
      activeSessionIds.add(run.sessionId);
    }
  }

  for (const [sessionId, session] of batchChainSessions.entries()) {
    if (activeSessionIds.has(sessionId)) {
      if (session) session.expiresAt = now + clampPositiveInt(BATCH_SESSION_TTL_MS, 7200000);
      continue;
    }
    if (!session || session.expiresAt <= now) {
      batchChainSessions.delete(sessionId);
    }
  }
}

function createBackgroundBatchRun({ rootCgId, targetCgSelector, baseFolderInput, selectedProviders, keepaliveUrl }) {
  const now = Date.now();
  return {
    id: randomUUID(),
    createdAt: now,
    updatedAt: now,
    expiresAt: now + clampPositiveInt(BACKGROUND_BATCH_RUN_TTL_MS, 21600000),
    status: 'running',
    error: null,
    rootCgId,
    targetCgSelector: String(targetCgSelector || '').trim() || null,
    baseFolderInput: String(baseFolderInput || '').trim() || '',
    selectedProviders: Array.isArray(selectedProviders) ? [...selectedProviders] : null,
    keepaliveUrl: String(keepaliveUrl || '').trim() || null,
    sessionId: null,
    rootCgName: null,
    parentContainerName: null,
    totalContainers: 0,
    scannedContainerCount: 0,
    processedContainers: 0,
    discoveredVideoCount: 0,
    queuedCount: 0,
    skippedCount: 0,
    downloadCompletedCount: 0,
    downloadFailedCount: 0,
    downloadPromises: new Set(),
    queued: [],
    queuedOverflow: 0,
    skipped: [],
    skippedOverflow: 0,
    itemErrors: [],
    _itemErrorsOverflow: 0,
    chainErrors: [],
    hasMoreContainers: true,
    nextContainerOffset: 0,
    startedAt: new Date(now).toISOString(),
    finishedAt: null
  };
}

function touchBackgroundBatchRun(run) {
  const now = Date.now();
  run.updatedAt = now;
  run.expiresAt = now + clampPositiveInt(BACKGROUND_BATCH_RUN_TTL_MS, 21600000);
}

function pushBatchItemError(runId, itemError) {
  if (!runId || !backgroundBatchRuns.has(runId)) {
    return;
  }

  const run = backgroundBatchRuns.get(runId);
  if (!Array.isArray(run.itemErrors)) {
    run.itemErrors = [];
  }

  // Cap itemErrors to prevent unbounded memory growth
  if (run.itemErrors.length >= MAX_ITEM_ERRORS_IN_MEMORY) {
    if (!run._itemErrorsOverflow) run._itemErrorsOverflow = 0;
    run._itemErrorsOverflow += 1;
    return;
  }

  run.itemErrors.push({
    batchRunId: runId,
    rootCgId: itemError.rootCgId || run.rootCgId,
    rootCgName: itemError.rootCgName || run.rootCgName,
    containerUrlShortId: itemError.containerUrlShortId || null,
    containerName: itemError.containerName || null,
    instanceUrlShortId: itemError.instanceUrlShortId || null,
    instanceName: itemError.instanceName || null,
    path: itemError.path || '',
    stage: itemError.stage || null,
    error: itemError.error || 'Unknown error'
  });

  touchBackgroundBatchRun(run);
}

function pushCappedItem(target, item, maxItems, overflowKey, owner) {
  if (!Array.isArray(target) || !owner) {
    return;
  }

  if (target.length < maxItems) {
    target.push(item);
    return;
  }

  owner[overflowKey] = Number(owner[overflowKey] || 0) + 1;
}

function summarizeBackgroundBatchRun(run) {
  const scannedContainerCount = Number(run.scannedContainerCount || run.processedContainers || 0);
  const totalContainers = Number(run.totalContainers || 0);
  const discoveredVideoCount = Number(run.discoveredVideoCount || 0);

  return {
    id: run.id,
    status: run.status,
    error: run.error || null,
    rootCgId: run.rootCgId,
    targetCgSelector: run.targetCgSelector,
    baseFolderInput: run.baseFolderInput,
    selectedProviders: run.selectedProviders,
    sessionId: run.sessionId,
    rootCgName: run.rootCgName,
    parentContainerName: run.parentContainerName,
    totalContainers,
    scannedContainerCount,
    processedContainers: scannedContainerCount,
    discoveredVideoCount,
    queuedCount: run.queuedCount,
    skippedCount: run.skippedCount,
    queuedItemsTracked: Array.isArray(run.queued) ? run.queued.length : 0,
    skippedItemsTracked: Array.isArray(run.skipped) ? run.skipped.length : 0,
    queuedItemsOverflow: Number(run.queuedOverflow || 0),
    skippedItemsOverflow: Number(run.skippedOverflow || 0),
    itemErrorsTracked: Array.isArray(run.itemErrors) ? run.itemErrors.length : 0,
    itemErrorsOverflow: Number(run._itemErrorsOverflow || 0),
    downloadCompletedCount: run.downloadCompletedCount || 0,
    downloadFailedCount: run.downloadFailedCount || 0,
    hasMoreContainers: run.hasMoreContainers,
    nextContainerOffset: run.nextContainerOffset,
    containerProgress: {
      processed: scannedContainerCount,
      total: totalContainers,
      percent: totalContainers > 0 ? Math.round((scannedContainerCount / totalContainers) * 100) : 0
    },
    videoProgress: {
      discovered: discoveredVideoCount,
      queued: Number(run.queuedCount || 0),
      skipped: Number(run.skippedCount || 0),
      completed: Number(run.downloadCompletedCount || 0),
      failed: Number(run.downloadFailedCount || 0),
      pending: Math.max(0, Number(run.queuedCount || 0) - Number(run.downloadCompletedCount || 0) - Number(run.downloadFailedCount || 0))
    },
    chainErrors: Array.isArray(run.chainErrors) ? run.chainErrors : [],
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    updatedAt: new Date(run.updatedAt).toISOString()
  };
}

function cleanupExpiredBackgroundBatchRuns() {
  const now = Date.now();
  for (const [runId, run] of backgroundBatchRuns.entries()) {
    if (!run || run.expiresAt <= now) {
      backgroundBatchRuns.delete(runId);
    }
  }
}

function getBackgroundBatchRunsSummary() {
  cleanupExpiredBackgroundBatchRuns();
  return Array.from(backgroundBatchRuns.values())
    .sort((left, right) => right.updatedAt - left.updatedAt)
    .map((run) => summarizeBackgroundBatchRun(run));
}

function hasActiveBackgroundBatchRuns() {
  for (const run of backgroundBatchRuns.values()) {
    if (run?.status === 'running') {
      return true;
    }
  }
  return false;
}

function stopBackgroundBatchKeepalive() {
  if (backgroundBatchKeepaliveTimer) {
    clearInterval(backgroundBatchKeepaliveTimer);
    backgroundBatchKeepaliveTimer = null;
  }
}

async function sendBackgroundBatchKeepalive() {
  if (!backgroundBatchKeepaliveUrl) {
    return;
  }

  try {
    await axios.get(backgroundBatchKeepaliveUrl, {
      timeout: clampPositiveInt(BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS, 10000),
      validateStatus: () => true,
      headers: {
        'User-Agent': 'ZeniusBatchKeepalive/1.0'
      }
    });
  } catch (error) {
    console.warn(`[Zenius] Keepalive request failed: ${error.message}`);
  }
}

function ensureBackgroundBatchKeepalive(keepaliveUrl = '') {
  if (keepaliveUrl) {
    backgroundBatchKeepaliveUrl = keepaliveUrl;
  }

  if (!backgroundBatchKeepaliveUrl || backgroundBatchKeepaliveTimer) {
    return;
  }

  backgroundBatchKeepaliveTimer = setInterval(() => {
    if (!hasActiveBackgroundBatchRuns()) {
      stopBackgroundBatchKeepalive();
      return;
    }

    sendBackgroundBatchKeepalive().catch(() => {});
  }, clampPositiveInt(BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS, 20000));

  sendBackgroundBatchKeepalive().catch(() => {});
}

function resolveBackgroundKeepaliveUrl(req) {
  const explicit = String(process.env.ZENIUS_BATCH_KEEPALIVE_URL || '').trim();
  if (explicit) {
    return explicit;
  }

  const host = String(req.get('host') || '').trim();
  if (!host) {
    return '';
  }

  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const protocol = forwardedProto || req.protocol || 'https';
  return `${protocol}://${host}/zenius`;
}

function markBackgroundBatchRunsCancelled(reason = 'Cancelled by user') {
  let cancelledCount = 0;

  for (const run of backgroundBatchRuns.values()) {
    if (run?.status !== 'running') {
      continue;
    }

    run.status = 'cancelled';
    run.error = reason;
    run.finishedAt = new Date().toISOString();
    touchBackgroundBatchRun(run);
    cancelledCount += 1;
  }

  if (!hasActiveBackgroundBatchRuns()) {
    stopBackgroundBatchKeepalive();
  }

  return cancelledCount;
}

function addLeafCgId(session, cgId) {
  if (!cgId || !/^\d+$/.test(String(cgId).trim())) {
    return;
  }

  const normalized = String(cgId).trim();
  if (session.leafCgIdSet.has(normalized)) {
    return;
  }

  session.leafCgIdSet.add(normalized);
  session.leafCgIds.push(normalized);
}

function extractCgId(pathUrl) {
  const match = String(pathUrl || '').match(/\/cg\/(\d+)(?:\/|$)/);
  return match ? match[1] : null;
}

function extractContainerShortIdFromPath(pathUrl) {
  const match = String(pathUrl || '').match(/\/cgc\/(\d+)(?:\/|$)/);
  return match ? match[1] : null;
}

function resolveContainerShortId(item) {
  const byField = normalizeNumericShortId(item?.['url-short-id']);
  if (byField) {
    return byField;
  }

  return extractContainerShortIdFromPath(item?.['path-url']);
}

function normalizeTargetCgId(rawSelector) {
  const raw = String(rawSelector || '').trim();
  if (!raw) return null;

  if (/^\d+$/.test(raw)) {
    return raw;
  }

  return extractCgId(raw) || extractContainerShortIdFromPath(raw);
}

function unique(values) {
  return Array.from(new Set(values));
}

function normalizeCgId(rawValue, fallback = null) {
  const value = String(rawValue || fallback || '').trim();
  if (!value) {
    throw new Error('CGroup id is required');
  }

  if (!/^\d+$/.test(value)) {
    throw new Error('CGroup id must be numeric');
  }

  return value;
}

function normalizeNumericShortId(rawValue) {
  const value = String(rawValue || '').trim().replace(/:+$/g, '');
  if (!/^\d+$/.test(value)) {
    return null;
  }
  return value;
}

function addDiscoveredContainer(session, containerShortId, item = {}, sourceLeafCgId = null, parentPathSegments = []) {
  const key = normalizeNumericShortId(containerShortId);
  if (!key || session.containerByShortId.has(key)) {
    return false;
  }

  session.containerByShortId.set(key, {
    'url-short-id': key,
    name: String(item?.name || item?.title || '').trim() || null,
    type: String(item?.type || '').trim() || null,
    'path-url': String(item?.['path-url'] || '').trim() || null,
    sourceLeafCgId: sourceLeafCgId ? String(sourceLeafCgId).trim() : null,
    parentPathSegments: sanitizePathSegments(parentPathSegments)
  });

  return true;
}

function rememberCgPath(session, cgId, pathSegments) {
  const normalizedCgId = normalizeNumericShortId(cgId);
  if (!normalizedCgId) {
    return;
  }

  const nextPath = sanitizePathSegments(pathSegments);
  const existingPath = sanitizePathSegments(session.cgPathById.get(normalizedCgId) || []);

  if (existingPath.length > 0 && existingPath.length <= nextPath.length) {
    return;
  }

  session.cgPathById.set(normalizedCgId, nextPath);
}

function resolveContainerParentSegments(session, container) {
  const customParentSegments = session.parentContainerName && session.parentContainerName !== 'unknown-parent'
    ? [session.parentContainerName]
    : [];
  const mappedSegments = sanitizePathSegments(
    container?.parentPathSegments
    || session.cgPathById.get(container?.sourceLeafCgId)
    || []
  );

  if (mappedSegments.length > 0) {
    return [...customParentSegments, ...mappedSegments];
  }

  if (customParentSegments.length > 0) {
    return customParentSegments;
  }

  return [resolveBatchParentContainerName(session)];
}

function resolveBatchParentContainerName(session) {
  if (session.parentContainerName && session.parentContainerName !== 'unknown-parent') {
    return session.parentContainerName;
  }

  if (session.rootCgName) {
    return sanitizePathSegment(session.rootCgName, 'unknown-parent');
  }

  return sanitizePathSegment(session.parentContainerName, 'unknown-parent');
}

async function resolveFolderId(folderInput) {
  const normalizedInput = stripWrappingQuotes(folderInput || 'root');
  if (!normalizedInput || normalizedInput.toLowerCase() === 'root') {
    return 'root';
  }

  const normalizedPath = normalizedInput.replace(/\\/g, '/');

  if (normalizedPath.includes('/')) {
    const folder = await db.ensureFolderPath(normalizedPath);
    return folder.id;
  }

  try {
    await db.getFolder(normalizedPath);
    return normalizedPath;
  } catch {
    const folder = await db.ensureFolderPath(normalizedPath);
    return folder.id;
  }
}

async function normalizeProviders(rawProviders) {
  if (!Array.isArray(rawProviders) || rawProviders.length === 0) {
    return null;
  }

  const providerCatalog = await uploaderService.getProviderCatalog({ includeDisabled: false });
  const allowed = new Set(providerCatalog.map((item) => item.id));

  const unique = new Set();
  for (const provider of rawProviders) {
    if (allowed.has(provider)) {
      unique.add(provider);
    }
  }

  return unique.size > 0 ? Array.from(unique) : null;
}

function getJson(endpoint, options) {
  return getUpstreamJson(endpoint, {
    ...options,
    timeoutMs: clampPositiveInt(options?.timeoutMs, clampPositiveInt(UPSTREAM_TIMEOUT_MS, 12000)),
    maxRetries: clampPositiveInt(options?.maxRetries, clampPositiveInt(UPSTREAM_MAX_RETRIES, 3)),
    retryBaseDelayMs: clampPositiveInt(UPSTREAM_RETRY_BASE_DELAY_MS, 500),
    deadlineGuardMs: clampPositiveInt(BATCH_DEADLINE_GUARD_MS, 1200)
  });
}

async function getCgByShortId(id, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/cgroup/get-cg-by-short-id?id=${encodeURIComponent(id)}`;
  return getJson(endpoint, {
    ...options,
    urlShortId: id,
    fallbackRefererPath: `/cg/${id}`
  });
}

async function getContainerListWithDetails(urlShortId, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/container-list?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint, {
    ...options,
    urlShortId,
    fallbackRefererPath: `/cg/${urlShortId}`
  });
  const list = Array.isArray(payload?.value) ? payload.value : [];

  return {
    urlShortId,
    items: list.map((item) => ({
      'url-short-id': String(item?.['url-short-id'] || '').trim(),
      name: String(item?.name || item?.title || '').trim() || null,
      type: String(item?.type || '').trim(),
      'path-url': String(item?.['path-url'] || '').trim() || null
    }))
  };
}

async function getContainerDetailsValue(urlShortId, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/container-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint, {
    ...options,
    urlShortId,
    fallbackRefererPath: `/cgc/${urlShortId}`
  });

  return payload?.value || {};
}

function isContainerDetailsPayload(value, urlShortId) {
  const relationPathUrl = String(value?.['relation-data']?.['path-url'] || '').trim();
  const relationShortId = normalizeNumericShortId(value?.['relation-data']?.['url-short-id']);
  const hasContainerPath = relationPathUrl.includes('/cgc/');
  const matchesShortId = relationShortId === normalizeNumericShortId(urlShortId);

  return hasContainerPath && matchesShortId;
}

async function getVideoInstanceDetails(urlShortId, options) {
  const value = await getContainerDetailsValue(urlShortId, options);
  const instances = Array.isArray(value?.['content-instances'])
    ? value['content-instances']
    : [];

  return instances
    .filter((item) => {
      const type = String(item?.type || '').trim().toLowerCase();
      return type === 'vidio' || type === 'video';
    })
    .map((item) => ({
      urlShortId: normalizeNumericShortId(item?.['url-short-id']),
      name: String(item?.name || item?.title || item?.['canonical-name'] || '').trim() || null,
      duration: item?.duration || item?.['duration-seconds'] || item?.['video-duration'] || null,
      type: String(item?.type || '').trim()
    }))
    .filter((item) => item.urlShortId);
}

async function fetchInstanceMetadata(urlShortId, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/instance-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint, {
    ...options,
    urlShortId,
    fallbackRefererPath: `/ci/${urlShortId}`
  });
  const value = payload?.value || {};

  return {
    urlShortId,
    name: String(value.name || value['canonical-name'] || '').trim() || null,
    duration: value.duration || value['duration-seconds'] || value['video-duration'] || null,
    'video-url': value['video-url'] || null,
    description: value.description || null,
    'path-url': value['path-url'] || null
  };
}

async function getCachedInstanceMetadata(urlShortId, options, session = null) {
  const normalizedUrlShortId = normalizeNumericShortId(urlShortId);
  if (!normalizedUrlShortId || !session) {
    return fetchInstanceMetadata(urlShortId, options);
  }

  if (session.instanceMetadataByShortId?.has(normalizedUrlShortId)) {
    const cachedEntry = session.instanceMetadataByShortId.get(normalizedUrlShortId);
    if (cachedEntry && typeof cachedEntry === 'object' && cachedEntry.cachedAt) {
      if ((Date.now() - cachedEntry.cachedAt) <= INSTANCE_METADATA_CACHE_TTL_MS) {
        return cachedEntry.value;
      }
      session.instanceMetadataByShortId.delete(normalizedUrlShortId);
    } else if (cachedEntry) {
      // Backward-compatible with old cache shape
      return cachedEntry;
    }
  }

  if (session.instanceMetadataPromisesByShortId?.has(normalizedUrlShortId)) {
    return session.instanceMetadataPromisesByShortId.get(normalizedUrlShortId);
  }

  const metadataPromise = fetchInstanceMetadata(normalizedUrlShortId, options)
    .then((metadata) => {
      session.instanceMetadataByShortId.set(normalizedUrlShortId, {
        value: metadata,
        cachedAt: Date.now()
      });
      return metadata;
    })
    .finally(() => {
      session.instanceMetadataPromisesByShortId?.delete(normalizedUrlShortId);
    });

  session.instanceMetadataPromisesByShortId?.set(normalizedUrlShortId, metadataPromise);
  return metadataPromise;
}

async function initializeBatchChainSessionDiscovery(session, options) {
  if (session.discoveryInitialized) {
    return;
  }

  let rootPayload;
  try {
    rootPayload = await getCgByShortId(session.rootCgId, options);
  } catch (error) {
    try {
      const containerValue = await getContainerDetailsValue(session.rootCgId, options);
      if (isContainerDetailsPayload(containerValue, session.rootCgId)) {
        addDiscoveredContainer(
          session,
          session.rootCgId,
          {
            'url-short-id': session.rootCgId,
            name: containerValue.name,
            type: containerValue.type,
            'path-url': containerValue?.['relation-data']?.['path-url'] || `/cgc/${session.rootCgId}`
          },
          session.rootCgId,
          []
        );
        session.traversal.push({
          source: 'root',
          next: session.rootCgId,
          isParent: false,
          isContainer: true
        });
        session.discoveryDone = true;
        session.discoveryInitialized = true;
        return;
      }
    } catch {
      // Ignore container fallback errors and keep original cgroup error.
    }

    pushBatchError(session.errors, {
      stage: 'cgroup-root',
      urlShortId: session.rootCgId,
      message: error.message
    });

    if (session.targetCgId) {
      addLeafCgId(session, session.targetCgId);
      session.traversal.push({ source: session.rootCgId, next: session.targetCgId, isParent: null });
      session.discoveryInitialized = true;
      return;
    }

    throw error;
  }

  const rootValue = rootPayload?.value || {};
  session.rootCgName = String(rootValue.name || '').trim() || session.rootCgName;

  const rootContainers = Array.isArray(rootPayload?.value?.['cgs-containers'])
    ? rootPayload.value['cgs-containers']
    : [];

  const shouldFilterByTarget = Boolean(session.targetCgId && session.targetCgId !== session.rootCgId);
  let matchedTarget = false;
  let queuedCgCount = 0;
  let directContainerCount = 0;

  for (const item of rootContainers) {
    const itemCgId = extractCgId(item?.['path-url']);
    const itemContainerId = resolveContainerShortId(item);
    const itemIsParent = parseBoolean(item?.['is-parent']);
    const matchesTarget = !shouldFilterByTarget
      || session.targetCgId === itemCgId
      || session.targetCgId === itemContainerId;

    if (!matchesTarget) {
      continue;
    }

    matchedTarget = true;

    if (itemCgId) {
      rememberCgPath(session, itemCgId, [item?.name || item?.title || itemCgId]);
      session.traversal.push({
        source: session.rootCgId,
        next: itemCgId,
        isParent: itemIsParent
      });

      if (!session.visitedCgIds.has(itemCgId)) {
        session.queueCgIds.push(itemCgId);
        queuedCgCount += 1;
      }
      continue;
    }

    if (itemIsParent === false && itemContainerId) {
      const inserted = addDiscoveredContainer(session, itemContainerId, item, session.rootCgId, []);
      if (inserted) {
        directContainerCount += 1;
      }

      session.traversal.push({
        source: session.rootCgId,
        next: itemContainerId,
        isParent: false,
        isContainer: true
      });
    }
  }

  if (shouldFilterByTarget && !matchedTarget) {
    const availableChildIds = unique(
      rootContainers
        .flatMap((item) => [extractCgId(item?.['path-url']), resolveContainerShortId(item)])
        .filter(Boolean)
    );

    pushBatchError(session.errors, {
      stage: 'cgroup-root-match',
      urlShortId: session.targetCgId,
      message: `targetCgId=${session.targetCgId} not present under root ${session.rootCgId}, using target as direct leaf candidate. available=[${availableChildIds.join(', ')}]`
    });
    addLeafCgId(session, session.targetCgId);
    session.traversal.push({ source: session.rootCgId, next: session.targetCgId, isParent: null });
    session.discoveryInitialized = true;
    return;
  }

  if (queuedCgCount === 0 && directContainerCount === 0) {
    addLeafCgId(session, session.rootCgId);
  }

  session.discoveryInitialized = true;
}

async function advanceBatchChainSessionDiscovery(session, options, deadlineAt) {
  await initializeBatchChainSessionDiscovery(session, options);

  const cgConcurrency = clampPositiveInt(BATCH_CG_FETCH_CONCURRENCY, 4);
  const cgFetchLimit = pLimit(cgConcurrency);

  while (session.queueCgIds.length > 0 && !shouldStopForDeadline(deadlineAt, 2000)) {
    const batchIds = [];
    while (session.queueCgIds.length > 0 && batchIds.length < cgConcurrency) {
      const currentCgId = session.queueCgIds.shift();
      if (!currentCgId || session.visitedCgIds.has(currentCgId)) {
        continue;
      }
      session.visitedCgIds.add(currentCgId);
      batchIds.push(currentCgId);
    }

    if (batchIds.length === 0) {
      continue;
    }

    const batchPayloads = await Promise.allSettled(
      batchIds.map((currentCgId) => cgFetchLimit(async () => ({
        currentCgId,
        payload: await getCgByShortId(currentCgId, options)
      })))
    );

    for (let i = 0; i < batchPayloads.length; i += 1) {
      const result = batchPayloads[i];
      const currentCgId = batchIds[i];
      if (!currentCgId) {
        continue;
      }

      if (result.status !== 'fulfilled') {
        pushBatchError(session.errors, {
          stage: 'cgroup-traversal',
          urlShortId: currentCgId,
          message: result.reason?.message || String(result.reason)
        });
        addLeafCgId(session, currentCgId);
        continue;
      }

      const payload = result.value.payload;
      const value = payload?.value || {};
      const valueIsParent = parseBoolean(value['is-parent']);
      const currentPathSegments = sanitizePathSegments(session.cgPathById.get(currentCgId) || []);
      if (!session.rootCgName) {
        session.rootCgName = String(value.name || '').trim() || session.rootCgName;
      }

      const containers = Array.isArray(value['cgs-containers']) ? value['cgs-containers'] : [];
      const nextItems = [];
      let directContainerCount = 0;

      for (const item of containers) {
        const nextId = extractCgId(item?.['path-url']);
        const itemIsParent = parseBoolean(item?.['is-parent']);

        if (nextId) {
          rememberCgPath(session, nextId, [...currentPathSegments, item?.name || item?.title || nextId]);
          session.traversal.push({
            source: value['url-short-id'] || currentCgId,
            next: nextId,
            isParent: itemIsParent
          });
          nextItems.push(nextId);
          continue;
        }

        const containerShortId = resolveContainerShortId(item);
        if (itemIsParent === false && containerShortId) {
          const inserted = addDiscoveredContainer(session, containerShortId, item, currentCgId, currentPathSegments);
          if (inserted) {
            directContainerCount += 1;
          }

          session.traversal.push({
            source: value['url-short-id'] || currentCgId,
            next: containerShortId,
            isParent: false,
            isContainer: true
          });
        }
      }

      const uniqueNextItems = unique(nextItems);
      if ((uniqueNextItems.length === 0 && directContainerCount === 0) || (valueIsParent === false && directContainerCount === 0)) {
        addLeafCgId(session, currentCgId);
      }

      for (const nextId of uniqueNextItems) {
        if (!session.visitedCgIds.has(nextId)) {
          session.queueCgIds.push(nextId);
        }
      }
    }
  }

  const containerConcurrency = clampPositiveInt(BATCH_CONTAINER_FETCH_CONCURRENCY, 4);
  const containerFetchLimit = pLimit(containerConcurrency);

  while (session.leafCursor < session.leafCgIds.length && !shouldStopForDeadline(deadlineAt, 2000)) {
    const leafBatch = session.leafCgIds.slice(session.leafCursor, session.leafCursor + containerConcurrency);
    if (leafBatch.length === 0) {
      break;
    }

    const containerListResults = await Promise.allSettled(
      leafBatch.map((leafCgId) => containerFetchLimit(async () => ({
        leafCgId,
        list: await getContainerListWithDetails(leafCgId, options)
      })))
    );

    for (let i = 0; i < containerListResults.length; i += 1) {
      const result = containerListResults[i];
      const leafCgId = leafBatch[i];
      if (result.status !== 'fulfilled') {
        pushBatchError(session.errors, {
          stage: 'container-list',
          urlShortId: leafCgId,
          message: result.reason?.message || String(result.reason)
        });
        continue;
      }

      const leafPathSegments = sanitizePathSegments(session.cgPathById.get(leafCgId) || []);
      for (const item of result.value.list.items) {
        addDiscoveredContainer(session, item?.['url-short-id'], item, leafCgId, leafPathSegments);
      }
    }

    session.leafCursor += leafBatch.length;
  }

  session.discoveryDone = session.queueCgIds.length === 0 && session.leafCursor >= session.leafCgIds.length;
}

async function buildBatchContainerDetail({ container, options, session, deadlineAt }) {
  const containerName = container.name || container['url-short-id'];
  const containerPath = buildContainerPath(resolveContainerParentSegments(session, container), containerName);
  const usedOutputNames = new Set();
  const containerShortId = container['url-short-id'];

  let videoDetails = [];
  if (session.containerVideoInstancesByShortId.has(containerShortId)) {
    videoDetails = session.containerVideoInstancesByShortId.get(containerShortId) || [];
    console.log(`[Zenius] Reusing preview cache for container ${containerShortId} (${videoDetails.length} instances)`);
  } else {
    try {
      console.log(`[Zenius] Fetching container details for ${containerShortId}`);
      videoDetails = await getVideoInstanceDetails(containerShortId, options);
      session.containerVideoInstancesByShortId.set(containerShortId, videoDetails);
    } catch (error) {
      pushBatchError(session.errors, {
        stage: 'container-details',
        urlShortId: containerShortId,
        message: error.message
      });
    }
  }

  const instancesWithMetadata = [];
  for (const video of videoDetails) {
    const outputName = reserveUniqueOutputName(
      video.name || `zenius-${video.urlShortId}`,
      `zenius-${video.urlShortId}`,
      video.urlShortId,
      usedOutputNames
    );

    instancesWithMetadata.push({
      ...video,
      path: containerPath,
      outputName
    });
  }

  return {
    containerUrlShortId: container['url-short-id'],
    containerName: container.name,
    containerType: container.type,
    containerPathUrl: container['path-url'],
    sourceLeafCgId: container.sourceLeafCgId || null,
    path: containerPath,
    videoInstances: instancesWithMetadata,
    partial: instancesWithMetadata.length < videoDetails.length
  };
}

async function resolveFolderIdWithCache(folderCache, folderInput) {
  const cacheKey = String(folderInput || 'root');
  if (folderCache.has(cacheKey)) {
    return folderCache.get(cacheKey);
  }

  const pendingFolderId = resolveFolderId(cacheKey).catch((error) => {
    folderCache.delete(cacheKey);
    throw error;
  });

  folderCache.set(cacheKey, pendingFolderId);
  return pendingFolderId;
}

async function queueBatchDownloadItem({
  chain,
  container,
  instance,
  requestContext,
  refererPath,
  baseFolderInput,
  selectedProviders,
  runId,
  session,
  folderCache,
  cancelled = () => false
}) {
  if (cancelled()) {
    return { counted: false, cancelled: true };
  }

  await waitForDownloadQueueCapacity({ cancelled });
  if (cancelled()) {
    return { counted: false, cancelled: true };
  }

  const metadata = instance.metadata && typeof instance.metadata === 'object'
    ? { ...instance.metadata }
    : {};
  const rawUrlShortId = instance.urlShortId || metadata.urlShortId;

  if (!rawUrlShortId || !/^\d+$/.test(String(rawUrlShortId).trim())) {
    return {
      counted: true,
      skipped: {
        urlShortId: rawUrlShortId || null,
        reason: 'Invalid or missing urlShortId',
        path: instance.path || container.path || ''
      }
    };
  }

  const urlShortId = normalizeShortId(rawUrlShortId);
  const outputName = sanitizeOutputName(
    instance.outputName || instance.name || `zenius-${urlShortId}`,
    `zenius-${urlShortId}`
  );
  const chainPath = String(instance.path || container.path || '').trim();

  if (cancelled()) {
    return { counted: false, cancelled: true };
  }

  const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';
  const folderId = await resolveFolderIdWithCache(folderCache, finalFolderInput);
  const outputFileName = `${outputName}.mp4`;
  const existingFile = await db.findFileByNameInFolder(folderId, outputFileName);

  if (existingFile) {
    const existingStatus = String(existingFile.status || '').trim().toLowerCase();
    console.log(`[Zenius] Pre-detail duplicate check hit for ${urlShortId}: ${outputFileName} in folder ${folderId} (${existingStatus || 'unknown'})`);

    if (existingStatus === 'failed') {
      console.log(`[Zenius] Retrying failed batch item ${urlShortId}: ${outputFileName} in folder ${folderId}`);
    } else {
      let uploadQueueResult = null;
      let pendingProviderInfo = null;
      let skipReason = 'File already exists';
      if (existingStatus === 'processing' || existingStatus === 'uploading') {
        skipReason = 'File is already being processed';
      } else {
        pendingProviderInfo = await uploaderService.getPendingUploadProviders(existingFile.id, selectedProviders);

        if (!pendingProviderInfo.hasPendingProviders) {
          skipReason = 'File already exists on selected providers';
        } else {
          const hasLocalSource = Boolean(existingFile.localPath && await fs.pathExists(existingFile.localPath));

          if (hasLocalSource) {
            uploadQueueResult = await uploaderService.queueFileUpload(existingFile.id, existingFile.localPath, folderId, selectedProviders);
            if (Array.isArray(uploadQueueResult?.jobs) && uploadQueueResult.jobs.length > 0) {
              await uploaderService.waitForFileUploadCompletion(existingFile.id, selectedProviders);
            }
            skipReason = 'File already exists locally; queued missing providers only';
          } else {
            console.log(`[Zenius] Existing file ${existingFile.id} is missing local source; re-downloading for providers: ${pendingProviderInfo.pendingProviders.join(', ')}`);
          }
        }
      }

      if (!pendingProviderInfo?.hasPendingProviders || uploadQueueResult || existingStatus === 'processing' || existingStatus === 'uploading') {
        return {
          counted: true,
          skipped: {
            urlShortId,
            reason: skipReason,
            path: chainPath,
            fileId: existingFile.id,
            outputName: outputFileName,
            uploadQueue: uploadQueueResult,
            pendingProviders: pendingProviderInfo?.pendingProviders || []
          }
        };
      }
    }
  }

  if (cancelled()) {
    return { counted: false, cancelled: true };
  }

  const queuedDownloadPromise = queueDownload({
    urlShortId,
    folderId,
    outputName,
    selectedProviders,
    requestContext,
    refererPath,
    fallbackRefererPath: container.containerPathUrl || '',
    requestedFilename: outputName
  });

  if (runId && backgroundBatchRuns.has(runId)) {
    const run = backgroundBatchRuns.get(runId);
    run.downloadPromises.add(queuedDownloadPromise);
    queuedDownloadPromise.finally(() => {
      if (backgroundBatchRuns.has(runId)) {
        const activeRun = backgroundBatchRuns.get(runId);
        activeRun.downloadPromises?.delete(queuedDownloadPromise);
        touchBackgroundBatchRun(activeRun);
      }
    });
    touchBackgroundBatchRun(run);
  }

  queuedDownloadPromise.then((result) => {
    if (result.cancelled) {
      console.log(`[Zenius] Batch download ${urlShortId} was cancelled`);
    } else {
      console.log(`[Zenius] Batch download ${urlShortId} completed successfully`);
      if (runId && backgroundBatchRuns.has(runId)) {
        const run = backgroundBatchRuns.get(runId);
        run.downloadCompletedCount = (run.downloadCompletedCount || 0) + 1;
        touchBackgroundBatchRun(run);
      }
    }
  }).catch((error) => {
    console.error(`[Zenius] Batch download pipeline failed for ${urlShortId}:`, error.message);
    if (runId && backgroundBatchRuns.has(runId)) {
      const run = backgroundBatchRuns.get(runId);
      run.downloadFailedCount = (run.downloadFailedCount || 0) + 1;
      touchBackgroundBatchRun(run);
    }
  });

  return {
    counted: true,
    queued: {
      urlShortId,
      name: metadata.name || instance.name || null,
      outputName: `${outputName}.mp4`,
      path: chainPath,
      folderInput: finalFolderInput,
      folderId,
      status: 'queued'
    }
  };
}

async function buildBatchChain({
  rootCgId,
  targetCgSelector,
  parentContainerName,
  requestContext,
  refererPath,
  sessionId,
  containerOffset,
  containerLimit,
  timeBudgetMs
}) {
  cleanupExpiredBatchChainSessions();

  const normalizedRootCgId = normalizeCgId(rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID);
  const normalizedParentContainerName = sanitizePathSegment(stripWrappingQuotes(parentContainerName), 'unknown-parent');
  const normalizedTargetSelector = String(targetCgSelector || '').trim() || null;

  let session = null;
  const normalizedSessionId = normalizeSessionId(sessionId);
  if (normalizedSessionId && batchChainSessions.has(normalizedSessionId)) {
    const existingSession = batchChainSessions.get(normalizedSessionId);
    const sameContext = existingSession
      && existingSession.rootCgId === normalizedRootCgId
      && String(existingSession.targetCgSelector || '') === String(normalizedTargetSelector || '')
      && existingSession.parentContainerName === normalizedParentContainerName;

    if (sameContext) {
      session = existingSession;
    }
  }

  if (!session) {
    session = createBatchChainSession({
      rootCgId: normalizedRootCgId,
      targetCgSelector: normalizedTargetSelector,
      parentContainerName: normalizedParentContainerName
    });
    batchChainSessions.set(session.id, session);
  }

  touchBatchChainSession(session);

  const deadlineAt = Date.now() + normalizeBatchRequestBudgetMs(timeBudgetMs);
  const options = {
    requestContext,
    refererPath,
    deadlineAt,
    timeoutMs: clampPositiveInt(BATCH_UPSTREAM_TIMEOUT_MS, 7000),
    maxRetries: clampPositiveInt(BATCH_UPSTREAM_MAX_RETRIES, 1)
  };

  await advanceBatchChainSessionDiscovery(session, options, deadlineAt);

  touchBatchChainSession(session);

  const mergedContainers = Array.from(session.containerByShortId.values());
  const totalContainers = mergedContainers.length;
  const normalizedOffset = Math.min(normalizeChunkOffset(containerOffset), totalContainers);
  const normalizedLimit = normalizeChunkLimit(containerLimit) || clampPositiveInt(DEFAULT_BATCH_CHAIN_CHUNK_SIZE, 8);
  const pathParentName = resolveBatchParentContainerName(session);

  const details = [];
  const detailConcurrency = clampPositiveInt(BATCH_DETAIL_FETCH_CONCURRENCY, 4);
  const detailLimit = pLimit(detailConcurrency);
  const pendingContainers = [];
  let cursor = normalizedOffset;
  while (
    cursor < totalContainers
    && pendingContainers.length < normalizedLimit
    && !shouldStopForDeadline(deadlineAt, 1600)
  ) {
    pendingContainers.push(mergedContainers[cursor]);
    cursor += 1;
  }

  const resolvedDetails = await Promise.all(
    pendingContainers.map((container) => detailLimit(() => buildBatchContainerDetail({
      container,
      options,
      session,
      deadlineAt
    })))
  );
  details.push(...resolvedDetails);

  const hasMoreKnownContainers = cursor < totalContainers;
  const hasMoreContainers = hasMoreKnownContainers || !session.discoveryDone;
  const nextContainerOffset = hasMoreContainers ? cursor : null;

  return {
    sessionId: session.id,
    rootCgId: session.rootCgId,
    rootCgName: session.rootCgName,
    targetCgSelector: session.targetCgSelector,
    targetCgId: session.targetCgId,
    parentContainerName: pathParentName,
    leafCgId: session.leafCgIds[0] || null,
    leafCgIds: session.leafCgIds,
    traversal: session.traversal,
    discoveredLeafCount: session.leafCgIds.length,
    discoveryQueueRemaining: session.queueCgIds.length,
    discoveryDone: session.discoveryDone,
    totalContainers,
    containerOffset: normalizedOffset,
    containerLimit: normalizedLimit,
    processedContainerCount: details.length,
    hasMoreContainers,
    nextContainerOffset,
    containerList: {
      urlShortId: session.leafCgIds[0] || null,
      urlShortIds: session.leafCgIds,
      totalContainers,
      items: mergedContainers.slice(normalizedOffset, cursor)
    },
    containerDetails: details,
    errors: session.errors,
    timeBudgetMs: normalizeBatchRequestBudgetMs(timeBudgetMs)
  };
}

function joinFolderPaths(prefix, suffix) {
  const left = String(prefix || '').replace(/\\/g, '/').trim();
  const right = String(suffix || '').replace(/\\/g, '/').trim();

  if (!left && !right) return '';
  if (!left) return right;
  if (!right) return left;

  return `${left.replace(/\/+$/g, '')}/${right.replace(/^\/+/, '')}`;
}

// ==================== CONCURRENCY CONTROL ====================

function getActiveDownloadCount() {
  return downloadQueue.activeCount;
}

function canStartNewDownload() {
  return downloadQueue.activeCount < downloadQueue.maxConcurrent;
}

function getQueueStatus() {
  return downloadQueue.getStatus();
}

function getZeniusStatusSnapshot() {
  const backgroundBatches = getBackgroundBatchRunsSummary();
  return {
    ...getQueueStatus(),
    activeBackgroundBatchCount: backgroundBatches.filter((run) => run.status === 'running').length,
    backgroundBatches
  };
}

async function resolveQueuedDownloadTask(task) {
  if (task.videoUrl && task.ffmpegHeaders) {
    return task;
  }

  const urlShortId = normalizeShortId(task.urlShortId);
  const requestContext = task.requestContext || buildRequestContext({});
  const details = await getInstanceDetails({
    urlShortId,
    refererPath: task.refererPath,
    fallbackRefererPath: task.fallbackRefererPath,
    requestContext
  });

  const isSuccessStatus = details.status >= 200 && details.status < 300;
  if (!isSuccessStatus || !details.body?.ok || !details.body?.value) {
    throw new Error(`Failed to fetch instance details (HTTP ${details.status})`);
  }

  const instanceValue = details.body.value;
  const videoUrl = String(instanceValue['video-url'] || '').trim();
  if (!videoUrl) {
    throw new Error('Instance does not contain video-url');
  }

  const outputName = sanitizeOutputName(
    task.outputName || task.requestedFilename || instanceValue.name || instanceValue['canonical-name'] || `zenius-${urlShortId}`,
    `zenius-${urlShortId}`
  );

  const ffmpegHeaders = {
    'User-Agent': requestContext.userAgent,
    Referer: resolveReferer(urlShortId, task.refererPath, instanceValue['path-url'] || task.fallbackRefererPath || '')
  };

  try {
    const refererUrl = new URL(ffmpegHeaders.Referer);
    ffmpegHeaders.Origin = `${refererUrl.protocol}//${refererUrl.host}`;
  } catch {
    ffmpegHeaders.Origin = 'https://www.zenius.net';
  }

  if (requestContext.cookieHeader) {
    ffmpegHeaders.Cookie = requestContext.cookieHeader;
  }

  return {
    ...task,
    urlShortId,
    videoUrl,
    outputName,
    ffmpegHeaders,
    details: instanceValue
  };
}

function queueDownload(task) {
  return downloadQueue.add(task);
}

async function queueBatchDownloadChunk({ chain, requestContext, refererPath, baseFolderInput, selectedProviders, runId = null, cancelled = () => false }) {
  const queued = [];
  const skipped = [];
  let totalInstances = 0;
  const session = chain.sessionId ? batchChainSessions.get(chain.sessionId) || null : null;
  const folderCache = new Map();

  for (const container of chain.containerDetails || []) {
    if (cancelled()) {
      break;
    }

    const containerResults = [];
    for (const instance of container.videoInstances || []) {
      if (cancelled()) {
        break;
      }

      containerResults.push(await queueBatchDownloadItem({
        chain,
        container,
        instance,
        requestContext,
        refererPath,
        baseFolderInput,
        selectedProviders,
        runId,
        session,
        folderCache,
        cancelled
      }));
    }

    for (const result of containerResults) {
      if (cancelled()) {
        break;
      }

      if (!result?.counted) {
        continue;
      }

      totalInstances += 1;
      if (result.queued) {
        queued.push(result.queued);
      }
      if (result.skipped) {
        skipped.push(result.skipped);
      }
    }
  }

  return { queued, skipped, totalInstances };
}

async function processBackgroundBatchRun(run, payload) {
  let dbSession = null;
  try {
    dbSession = await db.createBatchSession({
      id: run.id,
      runId: run.id,
      rootCgId: payload.rootCgId || DEFAULT_BATCH_ROOT_CGROUP_ID,
      rootCgName: null,
      targetCgSelector: payload.targetCgSelector || null,
      parentContainerName: payload.parentContainerName || '',
      status: 'running',
      totalContainers: 0,
      processedContainers: 0,
      queuedCount: 0,
      skippedCount: 0,
      nextContainerOffset: 0,
      hasMore: true,
      sessionData: {},
      queuedItems: [],
      skippedItems: [],
      chainErrors: [],
      expiresAt: run.expiresAt
    });
  } catch (dbError) {
    console.warn(`[Zenius] Failed to persist batch session to DB (will retry in background): ${dbError.message}`);
    // Schedule a retry after a brief delay — don't block the batch from starting
    setTimeout(async () => {
      try {
        dbSession = await db.createBatchSession({
          id: run.id,
          runId: run.id,
          rootCgId: payload.rootCgId || DEFAULT_BATCH_ROOT_CGROUP_ID,
          rootCgName: null,
          targetCgSelector: payload.targetCgSelector || null,
          parentContainerName: payload.parentContainerName || '',
          status: run.status === 'running' ? 'running' : run.status,
          totalContainers: run.totalContainers || 0,
          processedContainers: run.processedContainers || 0,
          queuedCount: run.queuedCount || 0,
          skippedCount: run.skippedCount || 0,
          nextContainerOffset: run.nextContainerOffset || 0,
          hasMore: run.hasMoreContainers !== false,
          sessionData: {},
          queuedItems: [],
          skippedItems: [],
          chainErrors: run.chainErrors || [],
          expiresAt: run.expiresAt
        });
        console.log(`[Zenius] Batch session ${run.id} persisted to DB on retry`);
      } catch (retryErr) {
        console.error(`[Zenius] Batch session ${run.id} DB persist retry also failed: ${retryErr.message}`);
      }
    }, 3000);
  }

  try {
    let currentSessionId = run.sessionId || null;
    let nextOffset = 0;
    let hasMore = true;
    let iteration = 0;

    while (hasMore) {
      if (run.status !== 'running') {
        break;
      }

      iteration += 1;
      if (iteration > BACKGROUND_BATCH_MAX_ITERATIONS) {
        throw new Error(`Background batch iteration limit reached (${BACKGROUND_BATCH_MAX_ITERATIONS}). Increase ZENIUS_BACKGROUND_BATCH_MAX_ITERATIONS if needed.`);
      }

      const chain = await buildBatchChain({
        rootCgId: payload.rootCgId,
        targetCgSelector: payload.targetCgSelector,
        parentContainerName: payload.parentContainerName,
        requestContext: payload.requestContext,
        refererPath: payload.refererPath,
        sessionId: currentSessionId,
        containerOffset: nextOffset,
        containerLimit: payload.containerLimit,
        timeBudgetMs: payload.timeBudgetMs
      });

      currentSessionId = chain.sessionId || currentSessionId;
      run.sessionId = currentSessionId;
      run.rootCgName = chain.rootCgName || run.rootCgName;
      run.parentContainerName = chain.parentContainerName || run.parentContainerName;
      run.totalContainers = Number(chain.totalContainers || 0);
      run.hasMoreContainers = Boolean(chain.hasMoreContainers);
      run.nextContainerOffset = Number.isFinite(Number(chain.nextContainerOffset)) ? Number(chain.nextContainerOffset) : 0;
      run.chainErrors = Array.isArray(chain.errors) ? [...chain.errors] : [];

      const chunkResult = await queueBatchDownloadChunk({
        chain,
        requestContext: payload.requestContext,
        refererPath: payload.refererPath,
        baseFolderInput: payload.baseFolderInput,
        selectedProviders: payload.selectedProviders,
        runId: run.id,
        cancelled: () => run.status !== 'running'
      });

      run.discoveredVideoCount += Number(chunkResult.totalInstances || 0);
      run.queuedCount += chunkResult.queued.length;
      run.skippedCount += chunkResult.skipped.length;
      for (const queuedItem of chunkResult.queued) {
        pushCappedItem(run.queued, queuedItem, MAX_QUEUED_ITEMS_IN_MEMORY, 'queuedOverflow', run);
      }
      for (const skippedItem of chunkResult.skipped) {
        pushCappedItem(run.skipped, skippedItem, MAX_SKIPPED_ITEMS_IN_MEMORY, 'skippedOverflow', run);
      }
      run.scannedContainerCount = chain.nextContainerOffset === null
        ? Number(chain.totalContainers || run.scannedContainerCount || 0)
        : Number.isFinite(Number(chain.nextContainerOffset))
          ? Number(chain.nextContainerOffset)
          : Number(chain.totalContainers || run.scannedContainerCount || 0);
      run.processedContainers = chain.nextContainerOffset === null
        ? Number(chain.totalContainers || run.processedContainers || 0)
        : Number.isFinite(Number(chain.nextContainerOffset))
          ? Number(chain.nextContainerOffset)
          : Number(chain.totalContainers || run.processedContainers || 0);

      touchBackgroundBatchRun(run);

      if (dbSession) {
        const updatedSession = await withDbRetry(
          () => db.updateBatchSession(run.id, {
            status: 'running',
            rootCgName: run.rootCgName,
            parentContainerName: run.parentContainerName,
            totalContainers: run.totalContainers,
            processedContainers: run.processedContainers,
            queuedCount: run.queuedCount,
            skippedCount: run.skippedCount,
            nextContainerOffset: run.nextContainerOffset,
            hasMore: run.hasMoreContainers,
            // Persist only the capped in-memory sample, not the full logical count.
            queuedItems: run.queued,
            skippedItems: run.skipped,
            chainErrors: run.chainErrors,
            sessionData: {
              sessionId: currentSessionId,
              queuedItemsTracked: Array.isArray(run.queued) ? run.queued.length : 0,
              skippedItemsTracked: Array.isArray(run.skipped) ? run.skipped.length : 0,
              queuedItemsOverflow: Number(run.queuedOverflow || 0),
              skippedItemsOverflow: Number(run.skippedOverflow || 0),
              itemErrorsTracked: Array.isArray(run.itemErrors) ? run.itemErrors.length : 0,
              itemErrorsOverflow: Number(run._itemErrorsOverflow || 0)
            }
          }),
          `batch-session-update[${run.id}]`
        );
        if (updatedSession) dbSession = updatedSession;
      }

      eventEmitter.emit('batch:progress', {
        batchRunId: run.id,
        rootCgId: run.rootCgId,
        rootCgName: run.rootCgName,
        totalContainers: run.totalContainers,
        scannedContainerCount: run.scannedContainerCount,
        processedContainers: run.processedContainers,
        discoveredVideoCount: run.discoveredVideoCount,
        queuedCount: run.queuedCount,
        skippedCount: run.skippedCount,
        downloadCompletedCount: run.downloadCompletedCount || 0,
        downloadFailedCount: run.downloadFailedCount || 0,
        containerProgress: {
          processed: run.scannedContainerCount,
          total: run.totalContainers,
          percent: run.totalContainers > 0 ? Math.round((run.scannedContainerCount / run.totalContainers) * 100) : 0
        },
        videoProgress: {
          discovered: run.discoveredVideoCount,
          queued: run.queuedCount,
          skipped: run.skippedCount,
          completed: run.downloadCompletedCount || 0,
          failed: run.downloadFailedCount || 0,
          pending: Math.max(0, run.queuedCount - (run.downloadCompletedCount || 0) - (run.downloadFailedCount || 0))
        },
        hasMore: run.hasMoreContainers,
        sessionId: currentSessionId
      });

      eventEmitter.emit('zenius:batch:progress', {
        batchRunId: run.id,
        status: run.status,
        sessionId: currentSessionId,
        scannedContainerCount: run.scannedContainerCount,
        processedContainers: run.processedContainers,
        totalContainers: run.totalContainers,
        discoveredVideoCount: run.discoveredVideoCount,
        queuedCount: run.queuedCount,
        skippedCount: run.skippedCount,
        downloadCompletedCount: run.downloadCompletedCount || 0,
        downloadFailedCount: run.downloadFailedCount || 0,
        hasMore: run.hasMoreContainers
      });

      hasMore = Boolean(chain.hasMoreContainers);
      nextOffset = Number.isFinite(Number(chain.nextContainerOffset)) ? Number(chain.nextContainerOffset) : 0;
    }

    if (run.status === 'running') {
      const pendingDownloadPromises = Array.from(run.downloadPromises || []);
      if (pendingDownloadPromises.length > 0) {
        await Promise.allSettled(pendingDownloadPromises);
      }

      run.status = 'completed';
      run.finishedAt = new Date().toISOString();
      touchBackgroundBatchRun(run);
    }
  } catch (error) {
    run.status = 'failed';
    run.error = error.message;
    run.finishedAt = new Date().toISOString();
    touchBackgroundBatchRun(run);
    console.error(`[Zenius] Background batch run ${run.id} failed:`, error.message);
  } finally {
    if (dbSession) {
      await withDbRetry(
        () => db.updateBatchSession(run.id, {
          status: run.status,
          error: run.error,
          queuedCount: run.queuedCount,
          skippedCount: run.skippedCount,
          processedContainers: run.processedContainers,
          hasMore: false,
          queuedItems: run.queued,
          skippedItems: run.skipped,
          chainErrors: run.chainErrors
        }),
        `batch-session-finalize[${run.id}]`
      );
    }

    if (run.status === 'completed' || run.status === 'failed') {
      eventEmitter.emit(run.status === 'completed' ? 'zenius:batch:completed' : 'zenius:batch:failed', {
        batchRunId: run.id,
        sessionId: run.sessionId,
        status: run.status,
        error: run.error || null,
        queuedCount: run.queuedCount,
        skippedCount: run.skippedCount,
        downloadCompletedCount: run.downloadCompletedCount || 0,
        downloadFailedCount: run.downloadFailedCount || 0,
        message: run.status === 'completed' ? 'Batch completed' : 'Batch failed'
      });

      webhookService.sendBatchComplete(db, {
        id: run.id,
        rootCgId: run.rootCgId,
        rootCgName: run.rootCgName,
        targetCgSelector: run.targetCgSelector,
        parentContainerName: run.parentContainerName,
        sessionId: run.sessionId,
        status: run.status,
        totalContainers: run.totalContainers,
        processedContainers: run.processedContainers,
        queuedCount: run.queuedCount,
        skippedCount: run.skippedCount,
        scannedContainerCount: run.scannedContainerCount,
        discoveredVideoCount: run.discoveredVideoCount,
        downloadCompletedCount: run.downloadCompletedCount || 0,
        downloadFailedCount: run.downloadFailedCount || 0,
        itemErrors: run.itemErrors || [],
        itemErrorsOverflow: Number(run._itemErrorsOverflow || 0),
        queuedItemsTracked: Array.isArray(run.queued) ? run.queued.length : 0,
        skippedItemsTracked: Array.isArray(run.skipped) ? run.skipped.length : 0,
        queuedItemsOverflow: Number(run.queuedOverflow || 0),
        skippedItemsOverflow: Number(run.skippedOverflow || 0),
        error: run.error,
        chainErrors: run.chainErrors,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt
      }).catch((e) => console.error('[Webhook] Notification error:', e.message));
    }
    if (!hasActiveBackgroundBatchRuns()) {
      stopBackgroundBatchKeepalive();

      // Trigger a cleanup cycle after all batches are done to remove temp files
      // and reset any stuck jobs caused by the batch run.
      if (cleanupService && typeof cleanupService.runNow === 'function') {
        cleanupService.runNow('post-batch').catch((e) =>
          console.warn('[Zenius] Post-batch cleanup failed:', e.message)
        );
      }
    }
  }
}

// ==================== CANCEL & RESET FUNCTIONS ====================

async function cancelAllZeniusDownloads() {
  const cancelled = {
    downloads: [],
    uploads: [],
    deletedUploads: [],
    errors: []
  };

  const isLikelyZeniusFile = (file) => {
    const originalUrl = String(file?.originalUrl || '').toLowerCase();
    const fileName = String(file?.name || '').toLowerCase();
    return originalUrl.includes('zenius') || fileName.includes('zenius');
  };
  
  // 1. Cancel queued downloads via DownloadQueue
  const queueCancelled = downloadQueue.cancelAll();
  cancelled.downloads.push(...queueCancelled);
  
  // 2. Cancel active FFmpeg processes from downloadQueue
  for (const taskInfo of downloadQueue.activeTasks) {
    try {
      const processJobIds = new Set();

      if (taskInfo.jobId) {
        processJobIds.add(taskInfo.jobId);
      }

      if (taskInfo.fileId) {
        const jobs = await db.getJobsByFile(taskInfo.fileId);
        for (const job of jobs) {
          if (job.type === 'process') {
            processJobIds.add(job.id);
          }
        }
      }

      for (const processJobId of processJobIds) {
        await videoProcessor.cancelJob(processJobId);
        await db.updateJob(processJobId, { status: 'cancelled', error: 'Cancelled by user (cancel all)' });
      }
      cancelled.downloads.push({ id: taskInfo.id, urlShortId: taskInfo.urlShortId, jobIds: Array.from(processJobIds), fromActive: true });
    } catch (error) {
      cancelled.errors.push({ id: taskInfo.id, error: error.message });
    }
  }
  
  // 3. Delete queued uploads immediately, then abort active uploads
  try {
    const allFiles = await db.listFiles();
    const activeJobs = await uploaderService.listJobs({ limit: 2000 });
    const zeniusFileIds = new Set(allFiles.filter(isLikelyZeniusFile).map((file) => file.id));
    const zeniusJobs = activeJobs.filter((job) => {
      if (!['pending', 'processing'].includes(job.status)) {
        return false;
      }

      if (job.type === 'process') {
        return true;
      }

      return Boolean(job.fileId && zeniusFileIds.has(job.fileId));
    });

    const pendingUploadJobIds = zeniusJobs
      .filter((job) => job.type !== 'process' && job.status === 'pending')
      .map((job) => job.id);

    const processingUploadJobIds = zeniusJobs
      .filter((job) => job.type !== 'process' && job.status === 'processing')
      .map((job) => job.id);

    const deletedUploadResults = await uploaderService.deleteQueuedJobs(pendingUploadJobIds, {
      reason: 'Deleted by user (cancel all)'
    });
    cancelled.deletedUploads.push(...deletedUploadResults.deleted.map((job) => ({ jobId: job.id, fileId: job.fileId })));

    const uploadCancelResults = await uploaderService.cancelJobs(processingUploadJobIds);
    cancelled.uploads.push(...uploadCancelResults.cancelled.map((item) => ({ jobId: item.jobId })));

    if (deletedUploadResults.skipped.length > 0) {
      cancelled.errors.push(...deletedUploadResults.skipped.map((item) => ({ phase: 'delete-queued-uploads', jobId: item.jobId, error: item.reason })));
    }

    if (uploadCancelResults.failed.length > 0) {
      cancelled.errors.push(...uploadCancelResults.failed.map((item) => ({ phase: 'uploads', jobId: item.jobId, error: item.error })));
    }
  } catch (error) {
    cancelled.errors.push({ phase: 'uploads', error: error.message });
  }
  
  return cancelled;
}

async function resetAllZeniusFiles() {
  const result = {
    deleted: [],
    errors: []
  };
  
  try {
    // Get all files that look like zenius files
    const allFiles = await db.listFiles();
    const zeniusFiles = allFiles.filter(f => 
      f.originalUrl?.includes('zenius.net') || 
      f.name?.startsWith('zenius-') ||
      f.name?.toLowerCase().includes('zenius')
    );
    
    console.log(`[Zenius] Found ${zeniusFiles.length} zenius files to reset`);
    
    for (const file of zeniusFiles) {
      try {
        // Cancel any active jobs first
        const jobs = await db.getJobsByFile(file.id);
        for (const job of jobs) {
          if (job.status === 'pending' || job.status === 'processing') {
            if (job.type === 'process') {
              await videoProcessor.cancelJob(job.id);
            } else {
              await uploaderService.cancelJob(job.id);
            }
          }
        }
        
        // Delete remote resources
        await uploaderService.deleteFileResources(file.id);
        
        // Purge file and jobs from DB
        await db.purgeFileAndJobs(file.id);
        
        result.deleted.push({ fileId: file.id, name: file.name });
      } catch (error) {
        result.errors.push({ fileId: file.id, error: error.message });
      }
    }
    
    // Clear sessions
    batchChainSessions.clear();
    backgroundBatchRuns.clear();
    stopBackgroundBatchKeepalive();
    downloadQueue.cancelAll();
    
  } catch (error) {
    result.errors.push({ phase: 'global', error: error.message });
  }
  
  return result;
}

const zeniusController = {
  async getInstanceDetails(req, res) {
    try {
      const urlShortId = normalizeShortId(req.body?.urlShortId ?? req.body?.instanceId ?? req.query?.urlShortId);
      const requestContext = buildRequestContext(req.body || {}, req);
      const details = await getInstanceDetails({
        urlShortId,
        refererPath: req.body?.refererPath,
        requestContext
      });

      res.json({
        success: true,
        data: {
          urlShortId,
          endpoint: details.endpoint,
          referer: details.referer,
          upstreamStatus: details.status,
          ok: Boolean(details.body?.ok),
          value: details.body?.value || null,
          error: details.body?.error || null,
          body: details.body
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async download(req, res) {
    try {
      const urlShortId = normalizeShortId(req.body?.urlShortId ?? req.body?.instanceId);
      const requestedFolder = String(req.body?.folderId || 'root');
      const folderId = await resolveFolderId(requestedFolder);
      const selectedProviders = await normalizeProviders(req.body?.providers);
      const requestContext = buildRequestContext(req.body || {}, req);

      const queueStatus = getQueueStatus();
      const willQueue = !canStartNewDownload();
      const requestedOutputName = req.body?.filename
        ? `${sanitizeOutputName(req.body.filename, `zenius-${urlShortId}`)}.mp4`
        : null;
      
      queueDownload({
        urlShortId,
        folderId,
        selectedProviders,
        requestContext,
        refererPath: req.body?.refererPath,
        requestedFilename: req.body?.filename
      }).then((result) => {
        if (result.cancelled) {
          console.log(`[Zenius] Download ${urlShortId} was cancelled`);
        } else {
          console.log(`[Zenius] Download ${urlShortId} completed successfully`);
        }
      }).catch((error) => {
        console.error(`[Zenius] Download pipeline failed for ${urlShortId}:`, error.message);
      });

      res.status(202).json({
        success: true,
        message: willQueue ? 'Zenius pipeline queued (waiting for slot)' : 'Zenius pipeline started',
        data: {
          urlShortId,
          outputName: requestedOutputName,
          cleanupLocalFile: true,
          folderInput: stripWrappingQuotes(requestedFolder),
          folderId,
          providers: selectedProviders,
          queueStatus
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getBatchChain(req, res) {
    try {
      const requestContext = buildRequestContext(req.body || {}, req);
      const chain = await buildBatchChain({
        rootCgId: req.body?.rootCgId,
        targetCgSelector: req.body?.targetCgSelector,
        parentContainerName: req.body?.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
        requestContext,
        refererPath: req.body?.refererPath,
        sessionId: req.body?.sessionId,
        containerOffset: req.body?.containerOffset,
        containerLimit: req.body?.containerLimit,
        timeBudgetMs: req.body?.timeBudgetMs
      });

      res.json({
        success: true,
        data: chain
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async downloadBatch(req, res) {
    try {
      const sessionId = normalizeSessionId(req.body?.sessionId);
      if (!sessionId || !batchChainSessions.has(sessionId)) {
        return res.status(400).json({
          success: false,
          error: 'Preview chain required before batch download. Call /batch-chain first to build a preview session, then pass the sessionId here.'
        });
      }

      // Touch the session BEFORE cleanup to ensure it won't be expired
      const session = batchChainSessions.get(sessionId);
      if (session) {
        touchBatchChainSession(session);
      }

      cleanupExpiredBatchChainSessions();
      cleanupExpiredBackgroundBatchRuns();

      const requestContext = buildRequestContext(req.body || {}, req);
      const selectedProviders = await normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');
      const containerLimit = normalizeChunkLimit(req.body?.containerLimit)
        || clampPositiveInt(BACKGROUND_BATCH_CHUNK_SIZE, 6);
      const timeBudgetMs = normalizeBatchRequestBudgetMs(req.body?.timeBudgetMs);
      const keepaliveUrl = resolveBackgroundKeepaliveUrl(req);

      const run = createBackgroundBatchRun({
        rootCgId: normalizeCgId(req.body?.rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID),
        targetCgSelector: req.body?.targetCgSelector,
        baseFolderInput,
        selectedProviders,
        keepaliveUrl
      });

      run.sessionId = sessionId;

      backgroundBatchRuns.set(run.id, run);
      ensureBackgroundBatchKeepalive(keepaliveUrl);

      eventEmitter.emit('zenius:batch:started', {
        batchRunId: run.id,
        sessionId,
        rootCgId: run.rootCgId,
        targetCgSelector: run.targetCgSelector,
        baseFolderInput,
        providers: selectedProviders,
        message: 'Zenius batch download started in background'
      });

      void processBackgroundBatchRun(run, {
        rootCgId: req.body?.rootCgId,
        targetCgSelector: req.body?.targetCgSelector,
        parentContainerName: req.body?.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
        requestContext,
        refererPath: req.body?.refererPath,
        baseFolderInput,
        selectedProviders,
        containerLimit,
        timeBudgetMs
      });

      res.status(202).json({
        success: true,
        message: 'Zenius batch download started in background',
        data: {
          batchRunId: run.id,
          status: summarizeBackgroundBatchRun(run),
          providers: selectedProviders,
          cleanupLocalFile: true,
          baseFolderInput,
          queueStatus: getZeniusStatusSnapshot()
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async cancelAll(req, res) {
    try {
      console.log('[Zenius] Cancelling all downloads and uploads...');
      const result = await cancelAllZeniusDownloads();
      const cancelledBackgroundBatches = markBackgroundBatchRunsCancelled();
      
      res.json({
        success: true,
        message: 'All Zenius downloads stopped and queued uploads removed',
        data: {
          cancelledDownloads: result.downloads.length,
          cancelledUploads: result.uploads.length,
          deletedQueuedUploads: result.deletedUploads.length,
          cancelledBackgroundBatches,
          errors: result.errors,
          details: result
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async resetFiles(req, res) {
    try {
      console.log('[Zenius] Resetting all Zenius files...');
      markBackgroundBatchRunsCancelled('Cancelled by reset all');
      const result = await resetAllZeniusFiles();
      
      res.json({
        success: true,
        message: `Reset ${result.deleted.length} Zenius files`,
        data: {
          deletedCount: result.deleted.length,
          errorCount: result.errors.length,
          deleted: result.deleted,
          errors: result.errors
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  getQueueStatus(req, res) {
    res.json({
      success: true,
      data: getZeniusStatusSnapshot()
    });
  },

  setMaxConcurrent(req, res) {
    try {
      const value = req.body?.maxConcurrent;
      if (typeof value !== 'number' || value < 1 || value > 50) {
        return res.status(400).json({ success: false, error: 'maxConcurrent must be a number between 1 and 50' });
      }
      const actual = downloadQueue.setMaxConcurrent(value);
      console.log(`[Zenius] Max concurrent pipelines set to ${actual}`);
      res.json({
        success: true,
        data: { maxConcurrent: actual, ...getZeniusStatusSnapshot() }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  getUploadConcurrency(req, res) {
    res.json({
      success: true,
      data: uploaderService.getConcurrencyConfig()
    });
  },

  setUploadConcurrency(req, res) {
    try {
      const result = {};
      if (req.body?.maxConcurrentUploads !== undefined) {
        const v = req.body.maxConcurrentUploads;
        if (typeof v !== 'number' || v < 1 || v > 20) {
          return res.status(400).json({ success: false, error: 'maxConcurrentUploads must be a number between 1 and 20' });
        }
        result.maxConcurrentUploads = uploaderService.setMaxConcurrentUploads(v);
        console.log(`[Zenius] Max concurrent uploads set to ${result.maxConcurrentUploads}`);
      }
      if (req.body?.maxConcurrentProviders !== undefined) {
        const v = req.body.maxConcurrentProviders;
        if (typeof v !== 'number' || v < 1 || v > 20) {
          return res.status(400).json({ success: false, error: 'maxConcurrentProviders must be a number between 1 and 20' });
        }
        result.maxConcurrentProviders = uploaderService.setMaxConcurrentProviders(v);
        console.log(`[Zenius] Max concurrent providers set to ${result.maxConcurrentProviders}`);
      }
      res.json({
        success: true,
        data: { ...result, ...uploaderService.getConcurrencyConfig() }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getWebhookConfig(req, res) {
    try {
      await webhookService.loadConfig(db);
      res.json({ success: true, data: webhookService.getConfig() });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async updateWebhookConfig(req, res) {
    try {
      const updates = {
        enabled: typeof req.body?.enabled === 'boolean' ? req.body.enabled : undefined,
        url: typeof req.body?.url === 'string' ? req.body.url : undefined,
        to: typeof req.body?.to === 'string' ? req.body.to : undefined
      };
      const config = await webhookService.updateConfig(db, updates);
      console.log(`[Zenius] Webhook config updated: enabled=${config.enabled}, url=${config.url ? '***' : '(empty)'}, to=${config.to || '(empty)'}`);
      res.json({ success: true, data: config });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async testWebhook(req, res) {
    try {
      const result = await webhookService.sendTest(db);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getBatchSessions(req, res) {
    try {
      await db.cleanupExpiredBatchSessions();
      const activeSessions = await db.getActiveBatchSessions();
      const inMemoryBatches = getBackgroundBatchRunsSummary();
      res.json({
        success: true,
        data: {
          dbSessions: activeSessions,
          inMemoryBatches,
          queueStatus: getZeniusStatusSnapshot()
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getBatchSessionStatus(req, res) {
    try {
      const sessionId = req.params.id;
      const dbSession = await db.getBatchSession(sessionId);
      if (!dbSession) {
        return res.status(404).json({ success: false, error: 'Batch session not found' });
      }
      const inMemoryRun = backgroundBatchRuns.get(sessionId);
      res.json({
        success: true,
        data: {
          dbSession,
          inMemoryRun: inMemoryRun ? summarizeBackgroundBatchRun(inMemoryRun) : null
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
};

/**
 * Persist all in-memory background batch runs to DB.
 * Called on graceful shutdown (SIGTERM/SIGINT) so progress is not lost.
 */
async function persistAllBatchRunsToDb() {
  const runs = Array.from(backgroundBatchRuns.values());
  if (runs.length === 0) return;

  console.log(`[Zenius] Persisting ${runs.length} in-memory batch run(s) to DB before shutdown`);
  const results = { persisted: 0, failed: 0 };

  await Promise.allSettled(runs.map(async (run) => {
    if (!run?.id) return;

    // Mark running batches as interrupted so they can be identified on next boot
    const shutdownStatus = run.status === 'running' ? 'failed' : run.status;
    const shutdownError = run.status === 'running'
      ? 'Server shut down during batch processing'
      : run.error;

    try {
      // Try upsert (update if exists, create if not)
      const existing = await db.getBatchSession(run.id).catch(() => null);
      if (existing) {
        await db.updateBatchSession(run.id, {
          status: shutdownStatus,
          error: shutdownError,
          rootCgName: run.rootCgName,
          parentContainerName: run.parentContainerName,
          totalContainers: run.totalContainers,
          processedContainers: run.processedContainers,
          queuedCount: run.queuedCount,
          skippedCount: run.skippedCount,
          nextContainerOffset: run.nextContainerOffset,
          hasMore: run.hasMoreContainers && shutdownStatus === 'running',
          chainErrors: run.chainErrors
        });
      } else {
        await db.createBatchSession({
          id: run.id,
          runId: run.id,
          rootCgId: run.rootCgId,
          rootCgName: run.rootCgName,
          targetCgSelector: run.targetCgSelector,
          parentContainerName: run.parentContainerName || '',
          status: shutdownStatus,
          error: shutdownError,
          totalContainers: run.totalContainers,
          processedContainers: run.processedContainers,
          queuedCount: run.queuedCount,
          skippedCount: run.skippedCount,
          nextContainerOffset: run.nextContainerOffset,
          hasMore: false,
          sessionData: {},
          queuedItems: [],
          skippedItems: [],
          chainErrors: run.chainErrors || [],
          expiresAt: run.expiresAt
        });
      }
      results.persisted += 1;
    } catch (error) {
      console.error(`[Zenius] Failed to persist batch run ${run.id} on shutdown: ${error.message}`);
      results.failed += 1;
    }
  }));

  console.log(`[Zenius] Shutdown persistence: ${results.persisted} persisted, ${results.failed} failed`);
  return results;
}

module.exports = zeniusController;
module.exports.persistAllBatchRunsToDb = persistAllBatchRunsToDb;
