const axios = require('axios');
const { randomUUID } = require('crypto');
const path = require('path');
const fs = require('fs-extra');
const pLimit = require('p-limit');
const { EventEmitter } = require('events');
const AppError = require('../errors/AppError');

const config = require('../config');
const {
  db,
  uploaderService,
  eventEmitter,
  cleanupService,
  runProcessUploadPipeline,
  finalizeExistingFilePipeline,
  normalizePipelineError
} = require('../services/runtime');
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
const BATCH_CG_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_CG_FETCH_CONCURRENCY || '20', 10);
const BATCH_INSTANCE_METADATA_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_INSTANCE_METADATA_CONCURRENCY || '28', 10);
const MAX_BATCH_ERRORS = Number.parseInt(process.env.ZENIUS_MAX_BATCH_ERRORS || '100', 10);
const DEFAULT_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_CHUNK_SIZE || '32', 10);
const MAX_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_MAX_CHUNK_SIZE || '128', 10);
const DEFAULT_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_BUDGET_MS || '24000', 10);
const MAX_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_MAX_BUDGET_MS || '28000', 10);
const BATCH_SESSION_TTL_MS = Number.parseInt(process.env.ZENIUS_BATCH_SESSION_TTL_MS || '7200000', 10);
const BATCH_UPSTREAM_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_TIMEOUT_MS || '7000', 10);
const BATCH_UPSTREAM_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_MAX_RETRIES || '1', 10);
const BATCH_DEADLINE_GUARD_MS = Number.parseInt(process.env.ZENIUS_BATCH_DEADLINE_GUARD_MS || '1200', 10);
const BACKGROUND_BATCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_CHUNK_SIZE || '24', 10);
const BATCH_DOWNLOAD_MAX_QUEUED_MULTIPLIER = Number.parseInt(process.env.ZENIUS_BATCH_DOWNLOAD_MAX_QUEUED_MULTIPLIER || '4', 10);
const BACKGROUND_BATCH_RUN_TTL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_RUN_TTL_MS || '21600000', 10);
const BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS || '20000', 10);
const BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS || '10000', 10);
// Max iterations for background batch loop (configurable to prevent infinite loops)
const BACKGROUND_BATCH_MAX_ITERATIONS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_MAX_ITERATIONS || '120', 10);
const INSTANCE_METADATA_CACHE_TTL_MS = Number.parseInt(process.env.ZENIUS_INSTANCE_METADATA_CACHE_TTL_MS || '1800000', 10);
const BATCH_METADATA_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BATCH_METADATA_TIMEOUT_MS || '15000', 10);
const BATCH_METADATA_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BATCH_METADATA_MAX_RETRIES || '4', 10);
const BATCH_FOLDER_PREFETCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_FOLDER_PREFETCH_CHUNK_SIZE || '80', 10);
const BATCH_PROVIDER_PREFETCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_PROVIDER_PREFETCH_CHUNK_SIZE || '200', 10);
const BATCH_PREVIEW_STEPS_PER_POLL = Number.parseInt(process.env.ZENIUS_BATCH_PREVIEW_STEPS_PER_POLL || '12', 10);
const BATCH_PREVIEW_CONTAINER_LIMIT = Number.parseInt(process.env.ZENIUS_BATCH_PREVIEW_CONTAINER_LIMIT || '48', 10);
const BATCH_PREVIEW_LOOP_DELAY_MS = Number.parseInt(process.env.ZENIUS_BATCH_PREVIEW_LOOP_DELAY_MS || '40', 10);
const BATCH_PREVIEW_RETRY_LIMIT = Number.parseInt(process.env.ZENIUS_BATCH_PREVIEW_RETRY_LIMIT || '5', 10);
const BATCH_PREVIEW_RETRY_DELAY_MS = Number.parseInt(process.env.ZENIUS_BATCH_PREVIEW_RETRY_DELAY_MS || '1500', 10);
const BATCH_FAST_PREVIEW = parseBoolean(process.env.ZENIUS_BATCH_FAST_PREVIEW) === true;
const PREVIEW_ITEMS_SAMPLE_LIMIT = Number.parseInt(process.env.ZENIUS_PREVIEW_ITEMS_SAMPLE_LIMIT || '500', 10);
const PREVIEW_ACTION_ITEMS_SAMPLE_LIMIT = Number.parseInt(process.env.ZENIUS_PREVIEW_ACTION_ITEMS_SAMPLE_LIMIT || '300', 10);
const PREVIEW_RUN_RECOVERY_GRACE_MS = Number.parseInt(process.env.ZENIUS_PREVIEW_RUN_RECOVERY_GRACE_MS || '300000', 10);
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
const DEFAULT_MAX_CONCURRENT_DOWNLOADS = Number.parseInt(process.env.ZENIUS_MAX_CONCURRENT_DOWNLOADS || '6', 10);

class DownloadQueue {
  constructor(maxConcurrent) {
    this._maxConcurrent = Math.max(1, maxConcurrent || DEFAULT_MAX_CONCURRENT_DOWNLOADS);
    this._active = new Map();
    this._queue = [];
    this._processing = false;
    this._drainScheduled = false;
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
    if (this._processing || this._drainScheduled) return;
    this._drainScheduled = true;
    setImmediate(() => {
      this._drainScheduled = false;
      this._drain();
    });
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
    if (!resolvedTask) {
      return {
        skipped: true,
        reason: 'instance-details-unavailable'
      };
    }

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
      const pipelineResult = await runProcessUploadPipeline(videoUrl, {
        folderId,
        outputName,
        outputDir: config.uploadDir,
        headers: ffmpegHeaders,
        skipIfExists: task.skipIfExists !== false,
        selectedProviders,
        waitForUpload: true
      });

      const result = pipelineResult.process;
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
        console.log(`[DownloadQueue] #${task.id} ${urlShortId} skipped: ${result.reason}`);

        broadcastDownloadComplete({
          urlShortId,
          taskId: task.id,
          fileId,
          skipped: true,
          reason: result.reason,
          durationMs: Date.now() - startTime
        });

        return {
          success: true,
          id: task.id,
          fileId,
          urlShortId,
          skipped: true,
          reason: result.reason,
          uploadQueue: pipelineResult.uploadQueue,
          pipeline: pipelineResult.pipeline
        };
      }

      console.log(`[DownloadQueue] #${task.id} ${urlShortId} downloaded, fileId=${fileId}, upload completed`);

      broadcastDownloadComplete({
        urlShortId,
        taskId: task.id,
        fileId,
        durationMs: Date.now() - startTime
      });

      return {
        success: true,
        id: task.id,
        fileId,
        urlShortId,
        pipeline: pipelineResult.pipeline
      };
    } catch (error) {
      const normalizedError = normalizePipelineError(error, 'DOWNLOAD_PIPELINE_FAILED');
      console.error(`[DownloadQueue] #${task.id} ${urlShortId} failed:`, normalizedError.message);

      broadcastDownloadFailed({
        urlShortId,
        taskId: task.id,
        error: normalizedError.message,
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
      isDrainScheduled: this._drainScheduled,
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

function getBatchDownloadMaxQueued() {
  return Math.max(
    downloadQueue.maxConcurrent,
    downloadQueue.maxConcurrent * clampPositiveInt(BATCH_DOWNLOAD_MAX_QUEUED_MULTIPLIER, 4)
  );
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

function createBatchChainSession({ id = null, rootCgId, targetCgSelector, parentContainerName }) {
  const now = Date.now();
  return {
    id: normalizeSessionId(id) || randomUUID(),
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
    containerDetailsByShortId: new Map(),
    plannedContainers: [],
    plannedItems: [],
    plannedItemByKey: new Map(),
    planCursor: 0,
    planReady: false,
    planContextKey: null,
    planBaseFolderInput: '',
    planSelectedProviders: null,
    planFolderCache: new Map(),
    planExistingFolderMap: null,
    planPrefetchContext: null,
    instanceMetadataByShortId: new Map(),
    instanceMetadataPromisesByShortId: new Map(),
    cgPathById: new Map([[rootCgId, []]]),
    errors: []
  };
}

function normalizePlanningContext(baseFolderInput, selectedProviders) {
  const normalizedBaseFolderInput = stripWrappingQuotes(baseFolderInput || '').trim();
  const normalizedSelectedProviders = Array.isArray(selectedProviders) && selectedProviders.length > 0
    ? [...selectedProviders].sort()
    : null;

  return {
    baseFolderInput: normalizedBaseFolderInput,
    selectedProviders: normalizedSelectedProviders,
    key: JSON.stringify({
      baseFolderInput: normalizedBaseFolderInput,
      selectedProviders: normalizedSelectedProviders
    })
  };
}

function resetBatchPlanState(session, planningContext) {
  session.planContextKey = planningContext.key;
  session.planBaseFolderInput = planningContext.baseFolderInput;
  session.planSelectedProviders = planningContext.selectedProviders;
  session.containerDetailsByShortId = new Map();
  session.plannedContainers = [];
  session.plannedItems = [];
  session.plannedItemByKey = new Map();
  session.planCursor = 0;
  session.planReady = false;
  session.planFolderCache = new Map();
  session.planExistingFolderMap = null;
  session.planPrefetchContext = null;
}

function ensureBatchPlanContext(session, planningContext) {
  if (session.planContextKey !== planningContext.key) {
    resetBatchPlanState(session, planningContext);
  }
}

function createPlannedItemKey(containerShortId, instanceShortId, outputFileName, folderId) {
  return [containerShortId, instanceShortId, outputFileName, folderId].map((part) => String(part || '').trim()).join('::');
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
    if (run?.status !== 'running') {
      continue;
    }

    if (run?.sessionId) {
      activeSessionIds.add(run.sessionId);
    }

    if (run?.type === 'preview' && run?.id) {
      activeSessionIds.add(run.id);
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

function createBackgroundBatchPreviewRun({ rootCgId, targetCgSelector, parentContainerName, baseFolderInput, selectedProviders, keepaliveUrl, headersRaw, refererPath, fastPreview = BATCH_FAST_PREVIEW }) {
  const run = createBackgroundBatchRun({
    rootCgId,
    targetCgSelector,
    baseFolderInput,
    selectedProviders,
    keepaliveUrl
  });
  const now = Date.now();

  run.type = 'preview';
  run.fastPreview = Boolean(fastPreview);
  run.status = 'running';
  run.parentContainerName = String(parentContainerName || '').trim() || null;
  run.previewLoopStarted = false;
  run.previewRetryCount = 0;
  run.maxPreviewRetries = clampPositiveInt(BATCH_PREVIEW_RETRY_LIMIT, 2);
  run.lastProgressAt = now;
  run.lastSuccessfulOffset = 0;
  run.previewItems = [];
  run.previewItemsOverflow = 0;
  run.previewItemsByKey = new Map();
  run.newItems = [];
  run.newItemsOverflow = 0;
  run.skippedPreviewItems = [];
  run.skippedPreviewItemsOverflow = 0;
  run.retryItems = [];
  run.retryItemsOverflow = 0;
  run.finalizeItems = [];
  run.finalizeItemsOverflow = 0;
  run.previewStats = {
    download: 0,
    retry: 0,
    finalize: 0,
    skip: 0,
    unknown: 0
  };
  run.lastError = null;
  run.sessionContext = {
    rootCgId,
    targetCgSelector: String(targetCgSelector || '').trim() || null,
    parentContainerName: String(parentContainerName || '').trim() || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
    baseFolderInput: String(baseFolderInput || '').trim() || '',
    selectedProviders: Array.isArray(selectedProviders) ? [...selectedProviders] : null,
    fastPreview: Boolean(fastPreview),
    headersRaw: String(headersRaw || ''),
    refererPath: String(refererPath || '').trim() || ''
  };
  run.chainPreview = null;
  return run;
}

function touchBackgroundBatchRun(run) {
  const now = Date.now();
  run.updatedAt = now;
  run.expiresAt = now + clampPositiveInt(BACKGROUND_BATCH_RUN_TTL_MS, 21600000);
}

function safeArrayClone(value) {
  return Array.isArray(value) ? [...value] : [];
}

function pushPreviewSample(target, item, overflowKey, owner, maxItems = PREVIEW_ACTION_ITEMS_SAMPLE_LIMIT) {
  pushCappedItem(target, item, Math.max(1, Number(maxItems) || PREVIEW_ACTION_ITEMS_SAMPLE_LIMIT), overflowKey, owner);
}

function createPreviewItemSnapshot(item = {}) {
  return {
    planKey: item.planKey || null,
    urlShortId: item.urlShortId || null,
    instanceName: item.instance?.name || item.instanceName || null,
    containerUrlShortId: item.containerUrlShortId || null,
    containerName: item.containerName || null,
    outputName: item.outputName || null,
    folderInput: item.folderInput || 'root',
    folderId: item.folderId || null,
    path: item.path || '',
    action: item.action || 'unknown',
    reason: item.reason || null,
    fileId: item.fileId || null,
    existingStatus: item.existingStatus || null,
    pendingProviders: safeArrayClone(item.pendingProviders),
    selectedProviders: safeArrayClone(item.selectedProviders),
    availableProviders: safeArrayClone(item.availableProviders),
    refererPath: item.refererPath || null
  };
}

function resetPreviewRunSamples(run) {
  run.previewItems = [];
  run.previewItemsOverflow = 0;
  run.previewItemsByKey = new Map();
  run.newItems = [];
  run.newItemsOverflow = 0;
  run.skippedPreviewItems = [];
  run.skippedPreviewItemsOverflow = 0;
  run.retryItems = [];
  run.retryItemsOverflow = 0;
  run.finalizeItems = [];
  run.finalizeItemsOverflow = 0;
  run.previewStats = {
    download: 0,
    retry: 0,
    finalize: 0,
    skip: 0,
    unknown: 0
  };
}

function rebuildPreviewRunSamples(run) {
  resetPreviewRunSamples(run);

  const plannedItems = Array.isArray(run.chainPreview?.plannedItems) ? run.chainPreview.plannedItems : [];
  for (const plannedItem of plannedItems) {
    const snapshot = createPreviewItemSnapshot(plannedItem);
    const key = snapshot.planKey || [snapshot.containerUrlShortId, snapshot.urlShortId, snapshot.outputName, snapshot.folderId].join('::');
    if (key && run.previewItemsByKey.has(key)) {
      continue;
    }

    if (key) {
      run.previewItemsByKey.set(key, snapshot);
    }
    pushPreviewSample(run.previewItems, snapshot, 'previewItemsOverflow', run, PREVIEW_ITEMS_SAMPLE_LIMIT);

    const action = String(snapshot.action || 'unknown').trim().toLowerCase() || 'unknown';
    run.previewStats[action] = Number(run.previewStats[action] || 0) + 1;

    if (action === 'skip') {
      pushPreviewSample(run.skippedPreviewItems, snapshot, 'skippedPreviewItemsOverflow', run);
    } else if (action === 'retry') {
      pushPreviewSample(run.retryItems, snapshot, 'retryItemsOverflow', run);
    } else if (action === 'finalize') {
      pushPreviewSample(run.finalizeItems, snapshot, 'finalizeItemsOverflow', run);
    } else {
      pushPreviewSample(run.newItems, snapshot, 'newItemsOverflow', run);
    }
  }
}

function buildPreviewItemsSummary(run) {
  return {
    totalTracked: run.previewItemsByKey instanceof Map ? run.previewItemsByKey.size : Number(run.previewItems?.length || 0),
    sampled: Array.isArray(run.previewItems) ? run.previewItems.length : 0,
    overflow: Number(run.previewItemsOverflow || 0),
    byAction: {
      download: Number(run.previewStats?.download || 0),
      retry: Number(run.previewStats?.retry || 0),
      finalize: Number(run.previewStats?.finalize || 0),
      skip: Number(run.previewStats?.skip || 0),
      unknown: Number(run.previewStats?.unknown || 0)
    },
    newItemsTracked: Array.isArray(run.newItems) ? run.newItems.length : 0,
    newItemsOverflow: Number(run.newItemsOverflow || 0),
    skippedItemsTracked: Array.isArray(run.skippedPreviewItems) ? run.skippedPreviewItems.length : 0,
    skippedItemsOverflow: Number(run.skippedPreviewItemsOverflow || 0),
    retryItemsTracked: Array.isArray(run.retryItems) ? run.retryItems.length : 0,
    retryItemsOverflow: Number(run.retryItemsOverflow || 0),
    finalizeItemsTracked: Array.isArray(run.finalizeItems) ? run.finalizeItems.length : 0,
    finalizeItemsOverflow: Number(run.finalizeItemsOverflow || 0)
  };
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
  const previewSummary = summarizePreviewChain(run.chainPreview || null);
  const previewItemsSummary = buildPreviewItemsSummary(run);
  const scannedContainerCount = Number(run.scannedContainerCount || run.processedContainers || 0);
  const totalContainers = Number(run.totalContainers || 0);
  const discoveredVideoCount = Number(run.discoveredVideoCount || 0);

  return {
    id: run.id,
    type: run.type || 'download',
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
    lastSuccessfulOffset: Number(run.lastSuccessfulOffset || 0),
    previewRetryCount: Number(run.previewRetryCount || 0),
    maxPreviewRetries: Number(run.maxPreviewRetries || 0),
    lastError: run.lastError || run.error || null,
    lastProgressAt: run.lastProgressAt ? new Date(run.lastProgressAt).toISOString() : null,
    previewSummary,
    previewItemsSummary,
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

function serializeBackgroundBatchRun(run, { includePreview = false } = {}) {
  const summary = summarizeBackgroundBatchRun(run);
  if (includePreview) {
    summary.chainPreview = run?.chainPreview || null;
    summary.previewItems = Array.isArray(run?.previewItems) ? run.previewItems : [];
    summary.newItems = Array.isArray(run?.newItems) ? run.newItems : [];
    summary.skippedPreviewItems = Array.isArray(run?.skippedPreviewItems) ? run.skippedPreviewItems : [];
    summary.retryItems = Array.isArray(run?.retryItems) ? run.retryItems : [];
    summary.finalizeItems = Array.isArray(run?.finalizeItems) ? run.finalizeItems : [];
  }
  return summary;
}

function serializeBatchPreviewSession(session) {
  if (!session) {
    return null;
  }

  return {
    sessionId: session.id,
    type: session.type || null,
    fastPreview: Boolean(session.fastPreview),
    rootCgId: session.rootCgId,
    rootCgName: session.rootCgName,
    targetCgSelector: session.targetCgSelector,
    targetCgId: session.targetCgId,
    parentContainerName: resolveBatchParentContainerName(session),
    discoveryDone: Boolean(session.discoveryDone),
    leafCgIds: Array.isArray(session.leafCgIds) ? [...session.leafCgIds] : [],
    traversal: Array.isArray(session.traversal) ? [...session.traversal] : [],
    totalContainers: Number(session.containerByShortId?.size || 0),
    planReady: Boolean(session.planReady),
    planContextKey: session.planContextKey,
    baseFolderInput: session.planBaseFolderInput || '',
    selectedProviders: Array.isArray(session.planSelectedProviders) ? [...session.planSelectedProviders] : null,
    plannedItemCount: Array.isArray(session.plannedItems) ? session.plannedItems.length : 0,
    plannedItems: Array.isArray(session.plannedItems) ? [...session.plannedItems] : [],
    containerDetails: session.containerDetailsByShortId instanceof Map
      ? Array.from(session.containerDetailsByShortId.values())
      : [],
    errors: Array.isArray(session.errors) ? [...session.errors] : []
  };
}

function prepareBatchPlanForExecution(session) {
  if (!session || !Array.isArray(session.plannedItems)) {
    return;
  }

  for (const plannedItem of session.plannedItems) {
    if (plannedItem?.fastPreviewProviderValidation) {
      plannedItem.action = 'pending-provider-validation';
      plannedItem.reason = 'Provider validation deferred until execution';
    }
  }
}

function serializeBatchChainSessionState(session, overrides = {}) {
  if (!session) {
    return null;
  }

  const mergedContainers = Array.from(session.containerByShortId.values());
  const totalContainers = mergedContainers.length;
  const containerDetails = session.containerDetailsByShortId instanceof Map
    ? Array.from(session.containerDetailsByShortId.values())
    : [];
  const plannedItems = Array.isArray(session.plannedItems) ? [...session.plannedItems] : [];
  const normalizedOffset = Math.min(normalizeChunkOffset(overrides.containerOffset), totalContainers);
  const normalizedLimit = normalizeChunkLimit(overrides.containerLimit) || clampPositiveInt(DEFAULT_BATCH_CHAIN_CHUNK_SIZE, 8);
  const nextContainerOffset = Number.isFinite(Number(overrides.nextContainerOffset))
    ? Number(overrides.nextContainerOffset)
    : (session.discoveryDone && session.planCursor >= totalContainers ? null : session.planCursor);
  const hasMoreContainers = typeof overrides.hasMoreContainers === 'boolean'
    ? overrides.hasMoreContainers
    : Boolean(nextContainerOffset !== null || !session.discoveryDone);

  return {
    sessionId: session.id,
    type: session.type || null,
    fastPreview: Boolean(session.fastPreview),
    rootCgId: session.rootCgId,
    rootCgName: session.rootCgName,
    targetCgSelector: session.targetCgSelector,
    targetCgId: session.targetCgId,
    parentContainerName: resolveBatchParentContainerName(session),
    leafCgId: session.leafCgIds.length === 1 ? session.leafCgIds[0] : null,
    leafCgIds: Array.isArray(session.leafCgIds) ? [...session.leafCgIds] : [],
    traversal: Array.isArray(session.traversal) ? [...session.traversal] : [],
    discoveredLeafCount: session.leafCgIds.length,
    discoveryQueueRemaining: session.queueCgIds.length,
    discoveryDone: session.discoveryDone,
    totalContainers,
    containerOffset: normalizedOffset,
    containerLimit: normalizedLimit,
    processedContainerCount: Number(overrides.processedContainerCount || 0),
    hasMoreContainers,
    nextContainerOffset,
    planReady: Boolean(session.planReady),
    plannedItemCount: plannedItems.length,
    containerList: {
      urlShortId: session.leafCgIds.length === 1 ? session.leafCgIds[0] : null,
      urlShortIds: Array.isArray(session.leafCgIds) ? [...session.leafCgIds] : [],
      totalContainers,
      items: mergedContainers.slice(normalizedOffset, Math.min(totalContainers, normalizedOffset + normalizedLimit))
    },
    containerDetails,
    plannedItems,
    skipped: Array.isArray(overrides.skipped) ? overrides.skipped : [],
    prefetch: overrides.prefetch || {
      folderCount: 0,
      missingFolderCount: 0,
      providerFileCount: 0
    },
    errors: Array.isArray(session.errors) ? [...session.errors] : [],
    timeBudgetMs: null
  };
}

function summarizePreviewChain(chainPreview = null) {
  const containerDetails = Array.isArray(chainPreview?.containerDetails) ? chainPreview.containerDetails : [];
  const plannedItems = Array.isArray(chainPreview?.plannedItems) ? chainPreview.plannedItems : [];
  const videoCount = containerDetails.reduce((sum, container) => sum + Number(container?.videoInstances?.length || 0), 0);

  return {
    previewContainerCount: containerDetails.length,
    previewVideoCount: videoCount,
    plannedItemCount: plannedItems.length,
    discoveredContainerCount: Number(chainPreview?.containerList?.totalContainers || 0),
    leafCgIds: Array.isArray(chainPreview?.leafCgIds) ? chainPreview.leafCgIds : []
  };
}

async function persistPreviewRunSnapshot(run) {
  if (!run?.id) {
    return null;
  }

  const previewSummary = summarizePreviewChain(run.chainPreview || null);
  const sessionData = {
    type: 'preview',
    sessionId: run.sessionId || null,
    rootCgId: run.rootCgId,
    rootCgName: run.rootCgName,
    targetCgSelector: run.targetCgSelector,
    parentContainerName: run.parentContainerName,
    baseFolderInput: run.baseFolderInput,
    selectedProviders: Array.isArray(run.selectedProviders) ? run.selectedProviders : null,
    discoveredVideoCount: Number(run.discoveredVideoCount || 0),
    scannedContainerCount: Number(run.scannedContainerCount || 0),
    processedContainers: Number(run.processedContainers || 0),
    nextContainerOffset: Number(run.nextContainerOffset || 0),
    hasMoreContainers: run.hasMoreContainers !== false,
    previewSummary,
    previewItemsSummary: buildPreviewItemsSummary(run),
    chainPreview: run.chainPreview || null,
    previewItems: Array.isArray(run.chainPreview?.plannedItems) ? run.chainPreview.plannedItems : [],
    previewContainers: Array.isArray(run.chainPreview?.containerDetails) ? run.chainPreview.containerDetails : [],
    newItems: Array.isArray(run.newItems) ? run.newItems : [],
    skippedPreviewItems: Array.isArray(run.skippedPreviewItems) ? run.skippedPreviewItems : [],
    retryItems: Array.isArray(run.retryItems) ? run.retryItems : [],
    finalizeItems: Array.isArray(run.finalizeItems) ? run.finalizeItems : [],
    previewRetryCount: Number(run.previewRetryCount || 0),
    lastError: run.lastError || run.error || null,
    lastProgressAt: run.lastProgressAt || null,
    lastSuccessfulOffset: Number(run.lastSuccessfulOffset || 0),
    itemErrors: Array.isArray(run.itemErrors) ? run.itemErrors : [],
    itemErrorsOverflow: Number(run._itemErrorsOverflow || 0)
  };

  return withDbRetry(
    () => db.updateBatchSession(run.id, {
      runId: run.id,
      status: run.status,
      rootCgName: run.rootCgName,
      parentContainerName: run.parentContainerName,
      totalContainers: Number(run.totalContainers || previewSummary.discoveredContainerCount || 0),
      processedContainers: Number(run.processedContainers || 0),
      queuedCount: Number(run.queuedCount || 0),
      skippedCount: Number(run.skippedCount || 0),
      nextContainerOffset: Number(run.nextContainerOffset || 0),
      hasMore: run.hasMoreContainers !== false,
      error: run.error || null,
      queuedItems: Array.isArray(run.queued) ? run.queued : [],
      skippedItems: Array.isArray(run.skipped) ? run.skipped : [],
      chainErrors: Array.isArray(run.chainErrors) ? run.chainErrors : [],
      sessionData
    }),
    `preview-session-update[${run.id}]`
  );
}

function summarizePersistedPreviewRun(dbSession) {
  const sessionData = dbSession?.sessionData || {};
  const chainPreview = sessionData.chainPreview || null;
  const previewSummary = sessionData.previewSummary || summarizePreviewChain(chainPreview);
  const previewItemsSummary = sessionData.previewItemsSummary || null;
  const scannedContainerCount = Number(sessionData.scannedContainerCount || dbSession?.processedContainers || 0);
  const totalContainers = Number(dbSession?.totalContainers || previewSummary.discoveredContainerCount || 0);
  const discoveredVideoCount = Number(sessionData.discoveredVideoCount || previewSummary.previewVideoCount || 0);

  return {
    id: dbSession.id,
    type: 'preview',
    status: dbSession.status,
    error: dbSession.error || null,
    rootCgId: dbSession.rootCgId,
    targetCgSelector: dbSession.targetCgSelector,
    baseFolderInput: sessionData.baseFolderInput || '',
    selectedProviders: Array.isArray(sessionData.selectedProviders) ? sessionData.selectedProviders : null,
    sessionId: sessionData.sessionId || dbSession.id,
    rootCgName: dbSession.rootCgName,
    parentContainerName: dbSession.parentContainerName,
    totalContainers,
    scannedContainerCount,
    processedContainers: scannedContainerCount,
    discoveredVideoCount,
    queuedCount: Number(dbSession.queuedCount || 0),
    skippedCount: Number(dbSession.skippedCount || 0),
    queuedItemsTracked: Array.isArray(dbSession.queuedItems) ? dbSession.queuedItems.length : 0,
    skippedItemsTracked: Array.isArray(dbSession.skippedItems) ? dbSession.skippedItems.length : 0,
    queuedItemsOverflow: 0,
    skippedItemsOverflow: 0,
    itemErrorsTracked: Array.isArray(sessionData.itemErrors) ? sessionData.itemErrors.length : 0,
    itemErrorsOverflow: Number(sessionData.itemErrorsOverflow || 0),
    downloadCompletedCount: 0,
    downloadFailedCount: 0,
    hasMoreContainers: dbSession.hasMore !== false,
    nextContainerOffset: Number(sessionData.nextContainerOffset || dbSession.nextContainerOffset || 0),
    lastSuccessfulOffset: Number(sessionData.lastSuccessfulOffset || 0),
    previewRetryCount: Number(sessionData.previewRetryCount || 0),
    maxPreviewRetries: clampPositiveInt(BATCH_PREVIEW_RETRY_LIMIT, 2),
    lastError: sessionData.lastError || dbSession.error || null,
    lastProgressAt: sessionData.lastProgressAt || dbSession.updatedAt,
    containerProgress: {
      processed: scannedContainerCount,
      total: totalContainers,
      percent: totalContainers > 0 ? Math.round((scannedContainerCount / totalContainers) * 100) : 0
    },
    videoProgress: {
      discovered: discoveredVideoCount,
      queued: 0,
      skipped: 0,
      completed: 0,
      failed: 0,
      pending: 0
    },
    chainErrors: Array.isArray(dbSession.chainErrors) ? dbSession.chainErrors : [],
    previewSummary,
    previewItemsSummary,
    startedAt: dbSession.createdAt,
    finishedAt: dbSession.finishedAt,
    updatedAt: dbSession.updatedAt
  };
}

function serializePersistedPreviewRun(dbSession, { includePreview = false } = {}) {
  const summary = summarizePersistedPreviewRun(dbSession);
  if (includePreview) {
    summary.chainPreview = dbSession?.sessionData?.chainPreview || null;
    summary.previewItems = Array.isArray(dbSession?.sessionData?.previewItems) ? dbSession.sessionData.previewItems : [];
    summary.newItems = Array.isArray(dbSession?.sessionData?.newItems) ? dbSession.sessionData.newItems : [];
    summary.skippedPreviewItems = Array.isArray(dbSession?.sessionData?.skippedPreviewItems) ? dbSession.sessionData.skippedPreviewItems : [];
    summary.retryItems = Array.isArray(dbSession?.sessionData?.retryItems) ? dbSession.sessionData.retryItems : [];
    summary.finalizeItems = Array.isArray(dbSession?.sessionData?.finalizeItems) ? dbSession.sessionData.finalizeItems : [];
  }
  return summary;
}

function hydrateBatchChainSessionFromPersistedPreview(dbSession, requestedPlanSessionId = null) {
  const sessionData = dbSession?.sessionData || {};
  const chainPreview = sessionData.chainPreview || null;
  const planSessionId = normalizeSessionId(chainPreview?.sessionId || sessionData.sessionId || requestedPlanSessionId);

  if (!planSessionId || !chainPreview?.planReady || !Array.isArray(chainPreview.plannedItems)) {
    return null;
  }

  const session = createBatchChainSession({
    rootCgId: normalizeCgId(chainPreview.rootCgId || sessionData.rootCgId || dbSession.rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID),
    targetCgSelector: chainPreview.targetCgSelector || sessionData.targetCgSelector || dbSession.targetCgSelector,
    parentContainerName: chainPreview.parentContainerName || sessionData.parentContainerName || dbSession.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME
  });

  session.id = planSessionId;
  session.type = sessionData.type || chainPreview.type || null;
  session.fastPreview = Boolean(sessionData.fastPreview || chainPreview.fastPreview);
  session.rootCgName = chainPreview.rootCgName || sessionData.rootCgName || dbSession.rootCgName || null;
  session.discoveryInitialized = true;
  session.discoveryDone = true;
  session.leafCgIds = Array.isArray(chainPreview.leafCgIds) ? [...chainPreview.leafCgIds] : [];
  session.leafCgIdSet = new Set(session.leafCgIds);
  session.traversal = Array.isArray(chainPreview.traversal) ? [...chainPreview.traversal] : [];
  session.queueCgIds = [];
  session.leafCursor = session.leafCgIds.length;

  session.containerByShortId = new Map();
  session.containerDetailsByShortId = new Map();
  const containerDetails = Array.isArray(chainPreview.containerDetails) ? chainPreview.containerDetails : [];
  for (const containerDetail of containerDetails) {
    const containerShortId = normalizeNumericShortId(containerDetail?.containerUrlShortId);
    if (!containerShortId) continue;

    session.containerByShortId.set(containerShortId, {
      'url-short-id': containerShortId,
      name: containerDetail.containerName || containerShortId,
      type: containerDetail.containerType || null,
      'path-url': containerDetail.containerPathUrl || null,
      sourceLeafCgId: containerDetail.sourceLeafCgId || null,
      parentPathSegments: []
    });
    session.containerDetailsByShortId.set(containerShortId, containerDetail);
    if (Array.isArray(containerDetail.videoInstances)) {
      session.containerVideoInstancesByShortId.set(containerShortId, containerDetail.videoInstances);
    }
  }

  const planningContext = normalizePlanningContext(sessionData.baseFolderInput || chainPreview.baseFolderInput || '', sessionData.selectedProviders || chainPreview.selectedProviders || null);
  session.planContextKey = chainPreview.planContextKey || planningContext.key;
  session.planBaseFolderInput = planningContext.baseFolderInput;
  session.planSelectedProviders = planningContext.selectedProviders;
  session.plannedItems = [...chainPreview.plannedItems];
  session.plannedItemByKey = new Map();
  for (const plannedItem of session.plannedItems) {
    if (plannedItem?.planKey && !session.plannedItemByKey.has(plannedItem.planKey)) {
      session.plannedItemByKey.set(plannedItem.planKey, plannedItem);
    }
  }
  session.plannedContainers = containerDetails;
  session.planCursor = session.plannedItems.length;
  session.planReady = true;
  session.errors = Array.isArray(chainPreview.errors)
    ? [...chainPreview.errors]
    : (Array.isArray(dbSession.chainErrors) ? [...dbSession.chainErrors] : []);

  touchBatchChainSession(session);
  batchChainSessions.set(session.id, session);
  return session;
}

async function hydrateBatchChainSessionForDownload({ sessionId, previewRunId }) {
  const normalizedSessionId = normalizeSessionId(sessionId);
  const normalizedPreviewRunId = normalizeSessionId(previewRunId);

  if (normalizedSessionId && batchChainSessions.has(normalizedSessionId)) {
    return batchChainSessions.get(normalizedSessionId);
  }

  const candidates = [];
  if (normalizedPreviewRunId) candidates.push(normalizedPreviewRunId);
  if (normalizedSessionId) candidates.push(normalizedSessionId);

  for (const candidateId of unique(candidates)) {
    const dbSession = await db.getBatchSession(candidateId).catch(() => null);
    if (dbSession?.sessionData?.type !== 'preview') continue;

    const hydratedSession = hydrateBatchChainSessionFromPersistedPreview(dbSession, normalizedSessionId);
    if (hydratedSession) {
      return hydratedSession;
    }
  }

  return null;
}

function shouldContinuePreviewAfterError(run, error) {
  const maxRetries = Math.max(0, Number(run?.maxPreviewRetries || 0));
  if (Number(run?.previewRetryCount || 0) >= maxRetries) {
    return false;
  }

  const message = String(error?.message || '').toLowerCase();
  if (!message) {
    return true;
  }

  const fatalPatterns = ['cgroup id is required', 'must be numeric', 'preview plan is not ready'];
  return !fatalPatterns.some((pattern) => message.includes(pattern));
}

async function sleepPreviewRetry(attemptNumber) {
  const delayMs = Math.max(100, clampPositiveInt(BATCH_PREVIEW_RETRY_DELAY_MS, 750) * Math.max(1, attemptNumber));
  await sleep(delayMs);
}

async function runBackgroundBatchPreviewLoop(run, { requestContext, refererPath }) {
  if (!run || run.previewLoopStarted) {
    return;
  }

  run.previewLoopStarted = true;
  touchBackgroundBatchRun(run);

  try {
    let iteration = 0;
    while (run.status === 'running') {
      iteration += 1;
      if (iteration > BACKGROUND_BATCH_MAX_ITERATIONS) {
        throw new Error(`Background preview iteration limit reached (${BACKGROUND_BATCH_MAX_ITERATIONS}). Increase ZENIUS_BACKGROUND_BATCH_MAX_ITERATIONS if needed.`);
      }

      try {
        await continueBackgroundBatchPreviewRun(run, { requestContext, refererPath });
        if (run.status !== 'running' || run.finishedAt) {
          break;
        }
        run.previewRetryCount = 0;
        run.lastError = null;
      } catch (error) {
        if (run.status !== 'running' || run.finishedAt) {
          break;
        }
        run.lastError = error.message;
        const nextRetryCount = Number(run.previewRetryCount || 0) + 1;
        console.warn(
          `[Zenius][Preview][Retry] run=${run.id} session=${run.sessionId || '-'} attempt=${nextRetryCount}/${run.maxPreviewRetries || BATCH_PREVIEW_RETRY_LIMIT} `
          + `offset=${run.nextContainerOffset || 0}/${run.totalContainers || '?'} processed=${run.processedContainers || 0} `
          + `plannedVideos=${run.discoveredVideoCount || 0} error=${error.message}`
        );
        if (!shouldContinuePreviewAfterError(run, error)) {
          run.status = 'failed';
          run.error = error.message;
          run.finishedAt = new Date().toISOString();
          console.error(
            `[Zenius][Preview][Failed] run=${run.id} session=${run.sessionId || '-'} retries=${run.previewRetryCount || 0}/${run.maxPreviewRetries || BATCH_PREVIEW_RETRY_LIMIT} `
            + `offset=${run.nextContainerOffset || 0}/${run.totalContainers || '?'} processed=${run.processedContainers || 0} `
            + `plannedVideos=${run.discoveredVideoCount || 0} error=${error.message}`
          );
          await persistPreviewRunSnapshot(run);
          touchBackgroundBatchRun(run);
          break;
        }

        run.previewRetryCount = nextRetryCount;
        run.chainErrors = Array.isArray(run.chainErrors) ? [...run.chainErrors, error.message] : [error.message];
        await persistPreviewRunSnapshot(run);
        touchBackgroundBatchRun(run);
        await sleepPreviewRetry(run.previewRetryCount);
      }

      if (run.status !== 'running' || run.finishedAt) {
        break;
      }

      if (BATCH_PREVIEW_LOOP_DELAY_MS > 0) {
        await sleep(clampPositiveInt(BATCH_PREVIEW_LOOP_DELAY_MS, 40));
      }
    }
  } finally {
    run.previewLoopStarted = false;
    touchBackgroundBatchRun(run);
  }
}

async function continueBackgroundBatchPreviewRun(run, { requestContext, refererPath }) {
  if (!run || run.status !== 'running' || run.finishedAt) {
    return;
  }

  run.status = 'running';
  touchBackgroundBatchRun(run);

  const boundSessionId = normalizeSessionId(run.sessionId);

    const previewRequestContext = run.sessionContext?.headersRaw
      ? buildRequestContext({ headersRaw: run.sessionContext.headersRaw }, null)
      : requestContext;
    const previewRefererPath = run.sessionContext?.refererPath || refererPath || '';

  const startedAt = Date.now();
  const stepsPerPoll = clampPositiveInt(BATCH_PREVIEW_STEPS_PER_POLL, 12);
  const containerLimit = clampPositiveInt(BATCH_PREVIEW_CONTAINER_LIMIT, clampPositiveInt(DEFAULT_BATCH_CHAIN_CHUNK_SIZE, 32));
  let nextOffset = Number.isFinite(Number(run.nextContainerOffset)) ? Number(run.nextContainerOffset) : 0;
  let hasMore = true;
  let totalProcessedThisPoll = 0;
  let latestDiscoveredVideos = Number(run.discoveredVideoCount || 0);

  for (let step = 0; step < stepsPerPoll && hasMore; step += 1) {
    if (run.status !== 'running' || run.finishedAt) {
      break;
    }

    const stepStartedAt = Date.now();
    const stepOffset = nextOffset;
    const chain = await buildBatchChain({
      rootCgId: run.sessionContext?.rootCgId || run.rootCgId,
      targetCgSelector: run.sessionContext?.targetCgSelector || run.targetCgSelector,
      parentContainerName: run.sessionContext?.parentContainerName || run.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
      requestContext: previewRequestContext,
      refererPath: previewRefererPath,
      baseFolderInput: run.sessionContext?.baseFolderInput || run.baseFolderInput,
      selectedProviders: run.sessionContext?.selectedProviders || run.selectedProviders,
      sessionId: boundSessionId,
      containerOffset: nextOffset,
      containerLimit,
      timeBudgetMs: null,
      allowSessionReuse: Boolean(boundSessionId)
    });

    if (boundSessionId && chain.sessionId !== boundSessionId) {
      throw new Error(`Preview session mismatch: expected ${boundSessionId}, got ${chain.sessionId}`);
    }

    const stepElapsedMs = Math.max(1, Date.now() - stepStartedAt);
    const stepProcessed = Number(chain.processedContainerCount || 0);
    const stepThroughput = (stepProcessed / (stepElapsedMs / 1000)).toFixed(2);
    console.log(
      `[Zenius][Preview][Step] run=${run.id} session=${chain.sessionId} step=${step + 1}/${stepsPerPoll} `
      + `offset=${stepOffset}->${chain.nextContainerOffset ?? 'done'} processed=${stepProcessed} `
      + `plannedVideos=${Number(chain.plannedItemCount || 0)} previewContainers=${Number(chain.previewSummary?.previewContainerCount || chain.containerDetails?.length || 0)} `
      + `discoveredContainers=${Number(chain.totalContainers || chain.containerList?.totalContainers || 0)} hasMore=${Boolean(chain.hasMoreContainers)} `
      + `durationMs=${stepElapsedMs} throughput=${stepThroughput}/s errors=${Array.isArray(chain.errors) ? chain.errors.length : 0}`
    );

    totalProcessedThisPoll += Number(chain.processedContainerCount || 0);
    latestDiscoveredVideos = Number(chain.plannedItemCount || latestDiscoveredVideos || 0);

    run.sessionId = chain.sessionId || run.sessionId || null;
    run.rootCgName = chain.rootCgName || run.rootCgName;
    run.parentContainerName = chain.parentContainerName || run.parentContainerName;
    run.totalContainers = Number(chain.totalContainers || 0);
    run.scannedContainerCount = Number(chain.nextContainerOffset ?? chain.totalContainers ?? 0);
    run.processedContainers = run.scannedContainerCount;
    run.discoveredVideoCount = Number(chain.plannedItemCount || run.discoveredVideoCount || 0);
    run.hasMoreContainers = Boolean(chain.hasMoreContainers);
    run.nextContainerOffset = Number.isFinite(Number(chain.nextContainerOffset)) ? Number(chain.nextContainerOffset) : 0;
    run.lastSuccessfulOffset = run.scannedContainerCount;
    run.lastProgressAt = Date.now();
    run.chainErrors = Array.isArray(chain.errors) ? [...chain.errors] : [];
    const activeSession = run.sessionId ? batchChainSessions.get(run.sessionId) || null : null;
    run.chainPreview = serializeBatchChainSessionState(activeSession, {
      containerOffset: chain.containerOffset,
      containerLimit: chain.containerLimit,
      processedContainerCount: chain.processedContainerCount,
      hasMoreContainers: chain.hasMoreContainers,
      nextContainerOffset: chain.nextContainerOffset,
      skipped: chain.skipped,
      prefetch: chain.prefetch
    }) || chain;
    rebuildPreviewRunSamples(run);

    hasMore = Boolean(chain.hasMoreContainers);
    nextOffset = Number.isFinite(Number(chain.nextContainerOffset)) ? Number(chain.nextContainerOffset) : nextOffset;
    if (run.status !== 'running' || run.finishedAt) {
      break;
    }

    if (!hasMore) {
      break;
    }

    if (BATCH_PREVIEW_LOOP_DELAY_MS > 0) {
      await sleep(clampPositiveInt(BATCH_PREVIEW_LOOP_DELAY_MS, 40));
    }
  }

  const elapsedMs = Math.max(1, Date.now() - startedAt);
  const throughput = (totalProcessedThisPoll / (elapsedMs / 1000)).toFixed(2);
  console.log(`[Zenius][Preview] processed=${totalProcessedThisPoll} discoveredVideos=${latestDiscoveredVideos} offset=${run.nextContainerOffset}/${run.totalContainers} hasMore=${hasMore} throughput=${throughput}/s elapsed=${elapsedMs}ms`);

  if (run.status !== 'running' || run.finishedAt) {
    await persistPreviewRunSnapshot(run);
    touchBackgroundBatchRun(run);
    return;
  }

  if (!hasMore) {
    run.status = 'completed';
    run.finishedAt = new Date().toISOString();
  }

  await persistPreviewRunSnapshot(run);

  touchBackgroundBatchRun(run);
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
    .map((run) => serializeBackgroundBatchRun(run));
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

async function cancelBackgroundBatchRun(runId, reason = 'Cancelled by user') {
  const normalizedRunId = normalizeSessionId(runId);
  if (!normalizedRunId) {
    return null;
  }

  let resolvedRunId = normalizedRunId;
  let run = backgroundBatchRuns.get(normalizedRunId) || null;
  if (!run) {
    for (const [candidateRunId, candidateRun] of backgroundBatchRuns.entries()) {
      if (!candidateRun) continue;
      if (candidateRun.sessionId === normalizedRunId) {
        resolvedRunId = candidateRunId;
        run = candidateRun;
        break;
      }
    }
  }

  if (!run) {
    const existingSession = await db.getBatchSession(normalizedRunId).catch(() => null);
    if (!existingSession) {
      return null;
    }

    await withDbRetry(
      () => db.updateBatchSession(normalizedRunId, {
        status: 'cancelled',
        error: reason,
        hasMore: false
      }),
      `batch-session-cancel-db-only[${normalizedRunId}]`
    );

    return {
      id: normalizedRunId,
      type: existingSession.sessionData?.type || 'download',
      status: 'cancelled',
      error: reason,
      sessionId: existingSession.sessionData?.sessionId || normalizedRunId,
      rootCgId: existingSession.rootCgId,
      rootCgName: existingSession.rootCgName,
      parentContainerName: existingSession.parentContainerName,
      totalContainers: existingSession.totalContainers,
      processedContainers: existingSession.processedContainers,
      scannedContainerCount: existingSession.processedContainers,
      queuedCount: existingSession.queuedCount,
      skippedCount: existingSession.skippedCount,
      downloadCompletedCount: 0,
      downloadFailedCount: 0,
      discoveredVideoCount: Number(existingSession.sessionData?.discoveredVideoCount || 0),
      hasMoreContainers: false,
      nextContainerOffset: Number(existingSession.nextContainerOffset || 0),
      chainErrors: Array.isArray(existingSession.chainErrors) ? existingSession.chainErrors : [],
      chainPreview: existingSession.sessionData?.chainPreview || null,
      previewItems: Array.isArray(existingSession.sessionData?.previewItems) ? existingSession.sessionData.previewItems : [],
      newItems: Array.isArray(existingSession.sessionData?.newItems) ? existingSession.sessionData.newItems : [],
      skippedPreviewItems: Array.isArray(existingSession.sessionData?.skippedPreviewItems) ? existingSession.sessionData.skippedPreviewItems : [],
      retryItems: Array.isArray(existingSession.sessionData?.retryItems) ? existingSession.sessionData.retryItems : [],
      finalizeItems: Array.isArray(existingSession.sessionData?.finalizeItems) ? existingSession.sessionData.finalizeItems : [],
      previewItemsSummary: existingSession.sessionData?.previewItemsSummary || null,
      previewSummary: existingSession.sessionData?.previewSummary || null,
      lastError: reason,
      lastProgressAt: existingSession.updatedAt,
      startedAt: existingSession.createdAt,
      finishedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
  }

  if (run.status === 'running') {
    run.status = 'cancelled';
    run.error = reason;
    run.lastError = reason;
    run.finishedAt = new Date().toISOString();
    touchBackgroundBatchRun(run);
  }

  const existingSession = await db.getBatchSession(resolvedRunId).catch(() => null);
  if (existingSession) {
    await withDbRetry(
      () => db.updateBatchSession(resolvedRunId, {
        status: run.status,
        error: run.error,
        rootCgName: run.rootCgName,
        parentContainerName: run.parentContainerName,
        totalContainers: Number(run.totalContainers || 0),
        processedContainers: Number(run.processedContainers || 0),
        queuedCount: Number(run.queuedCount || 0),
        skippedCount: Number(run.skippedCount || 0),
        nextContainerOffset: Number(run.nextContainerOffset || 0),
        hasMore: false,
        queuedItems: Array.isArray(run.queued) ? run.queued : [],
        skippedItems: Array.isArray(run.skipped) ? run.skipped : [],
        chainErrors: Array.isArray(run.chainErrors) ? run.chainErrors : []
      }),
      `batch-session-cancel[${resolvedRunId}]`
    );
  }

  return run;
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

async function getEnabledProviderIds() {
  const providerCatalog = await uploaderService.getProviderCatalog({ includeDisabled: false });
  return providerCatalog
    .filter((item) => item.enabled !== false)
    .map((item) => item.id);
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

async function advanceBatchChainBranchDiscovery(session, options, deadlineAt) {
  while (session.queueCgIds.length > 0 && !shouldStopForDeadline(deadlineAt, 2000)) {
    const currentCgId = session.queueCgIds.shift();
    if (!currentCgId || session.visitedCgIds.has(currentCgId)) {
      continue;
    }
    session.visitedCgIds.add(currentCgId);

    let payload;
    try {
      payload = await getCgByShortId(currentCgId, options);
    } catch (error) {
      pushBatchError(session.errors, {
        stage: 'cgroup-traversal',
        urlShortId: currentCgId,
        message: error.message
      });
      addLeafCgId(session, currentCgId);
      continue;
    }

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

  while (session.leafCursor < session.leafCgIds.length && !shouldStopForDeadline(deadlineAt, 2000)) {
    const leafCgId = session.leafCgIds[session.leafCursor];
    session.leafCursor += 1;
    if (!leafCgId) {
      continue;
    }

    try {
      const list = await getContainerListWithDetails(leafCgId, options);
      const leafPathSegments = sanitizePathSegments(session.cgPathById.get(leafCgId) || []);
      for (const item of list.items) {
        addDiscoveredContainer(session, item?.['url-short-id'], item, leafCgId, leafPathSegments);
      }
    } catch (error) {
      pushBatchError(session.errors, {
        stage: 'container-list',
        urlShortId: leafCgId,
        message: error.message
      });
    }
  }

  session.discoveryDone = session.queueCgIds.length === 0 && session.leafCursor >= session.leafCgIds.length;
}

async function advanceBatchChainSessionDiscovery(session, options, deadlineAt) {
  await initializeBatchChainSessionDiscovery(session, options);
  await advanceBatchChainBranchDiscovery(session, options, deadlineAt);
}

async function buildBatchContainerDetail({ container, options, session }) {
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

function chunkArray(values, chunkSize) {
  const size = Math.max(1, Number(chunkSize) || 1);
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function normalizeFolderInputKey(folderInput) {
  const normalized = String(folderInput || '')
    .replace(/\\/g, '/')
    .trim();

  if (!normalized || normalized === '/' || normalized.toLowerCase() === 'root') {
    return 'root';
  }

  const cleaned = normalized
    .replace(/^\/+/, '')
    .replace(/\/+$/g, '')
    .split('/')
    .map((segment) => String(segment || '').trim())
    .filter(Boolean)
    .join('/');

  return cleaned || 'root';
}

function toFolderInputKeyFromDbPath(dbPath) {
  const normalized = String(dbPath || '')
    .replace(/\\/g, '/')
    .trim();

  if (!normalized || normalized === '/') {
    return 'root';
  }

  const cleaned = normalized
    .replace(/^\/+/, '')
    .replace(/\/+$/g, '')
    .split('/')
    .map((segment) => String(segment || '').trim())
    .filter(Boolean)
    .join('/');

  return cleaned || 'root';
}

async function runDbQuery(sql, params = []) {
  await db._ready();
  const [rows] = await db.pool.query(sql, params);
  return rows;
}

async function loadExistingFolderMap(session = null) {
  if (session?.planExistingFolderMap instanceof Map) {
    return session.planExistingFolderMap;
  }

  const rows = await runDbQuery('SELECT id, path FROM folders');
  const folderIdByInputKey = new Map();

  for (const row of rows || []) {
    const id = String(row?.id || '').trim();
    const key = toFolderInputKeyFromDbPath(row?.path);
    if (!id || !key || folderIdByInputKey.has(key)) {
      continue;
    }
    folderIdByInputKey.set(key, id);
  }

  if (!folderIdByInputKey.has('root')) {
    folderIdByInputKey.set('root', 'root');
  }

  if (session) {
    session.planExistingFolderMap = folderIdByInputKey;
  }

  return folderIdByInputKey;
}

async function buildExistingFileLookup(containers, baseFolderInput, chunkSize = BATCH_FOLDER_PREFETCH_CHUNK_SIZE, session = null) {
  const plannedByFolderInput = new Map();

  for (const container of containers || []) {
    for (const instance of container.videoInstances || []) {
      const urlShortId = normalizeNumericShortId(instance.urlShortId);
      if (!urlShortId) continue;

      const outputBaseName = sanitizeOutputName(instance.outputName || instance.name || `zenius-${urlShortId}`, `zenius-${urlShortId}`);
      const outputFileName = `${outputBaseName}.mp4`;
      const chainPath = String(instance.path || container.path || '').trim();
      const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';

      if (!plannedByFolderInput.has(finalFolderInput)) {
        plannedByFolderInput.set(finalFolderInput, new Set());
      }
      plannedByFolderInput.get(finalFolderInput).add(outputFileName);
    }
  }

  const folderInputs = Array.from(plannedByFolderInput.keys());
  const existingFolderMap = await loadExistingFolderMap(session);
  const folderIdByInput = new Map();
  let missingFolderCount = 0;
  for (const folderInput of folderInputs) {
    const folderKey = normalizeFolderInputKey(folderInput);
    const folderId = existingFolderMap.get(folderKey) || null;
    folderIdByInput.set(folderInput, folderId);
    if (!folderId) {
      missingFolderCount += 1;
    }
  }

  const existingByFolderId = new Map();
  const plannedNamesByFolderId = new Map();
  for (const [folderInput, plannedNames] of plannedByFolderInput.entries()) {
    const folderId = folderIdByInput.get(folderInput);
    if (!folderId) {
      continue;
    }

    if (!plannedNamesByFolderId.has(folderId)) {
      plannedNamesByFolderId.set(folderId, new Set());
    }

    const merged = plannedNamesByFolderId.get(folderId);
    for (const plannedName of plannedNames) {
      merged.add(plannedName);
    }
  }

  const resolvableFolderIds = Array.from(plannedNamesByFolderId.keys());
  const folderIdChunks = chunkArray(resolvableFolderIds, chunkSize);

  for (const folderIdChunk of folderIdChunks) {
    if (folderIdChunk.length === 0) {
      continue;
    }

    const placeholders = folderIdChunk.map(() => '?').join(', ');
    const rows = await runDbQuery(
      `SELECT id, folder_id, name, status, local_path
         FROM files
        WHERE folder_id IN (${placeholders})
        ORDER BY created_at DESC`,
      folderIdChunk
    );

    for (const row of rows || []) {
      const folderId = String(row?.folder_id || '').trim();
      const nameKey = String(row?.name || '').trim();
      if (!folderId || !nameKey) {
        continue;
      }

      const plannedNames = plannedNamesByFolderId.get(folderId);
      if (!plannedNames || !plannedNames.has(nameKey)) {
        continue;
      }

      if (!existingByFolderId.has(folderId)) {
        existingByFolderId.set(folderId, new Map());
      }

      const byName = existingByFolderId.get(folderId);
      if (byName.has(nameKey)) {
        continue;
      }

      byName.set(nameKey, {
        id: row.id,
        name: row.name,
        folder_id: row.folder_id,
        status: row.status,
        localPath: row.local_path || null
      });
    }
  }

  return {
    folderIdByInput,
    existingByFolderId,
    plannedFolderCount: folderInputs.length,
    missingFolderCount
  };
}

function collectPlannedNamesByFolderInput(containers, baseFolderInput) {
  const plannedByFolderInput = new Map();

  for (const container of containers || []) {
    for (const instance of container.videoInstances || []) {
      const urlShortId = normalizeNumericShortId(instance.urlShortId);
      if (!urlShortId) continue;

      const outputBaseName = sanitizeOutputName(instance.outputName || instance.name || `zenius-${urlShortId}`, `zenius-${urlShortId}`);
      const outputFileName = `${outputBaseName}.mp4`;
      const chainPath = String(instance.path || container.path || '').trim();
      const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';

      if (!plannedByFolderInput.has(finalFolderInput)) {
        plannedByFolderInput.set(finalFolderInput, new Set());
      }
      plannedByFolderInput.get(finalFolderInput).add(outputFileName);
    }
  }

  return plannedByFolderInput;
}

function ensureBatchPlanPrefetchContext(session, planningContext) {
  if (!session) {
    return null;
  }

  if (session.planPrefetchContext && session.planPrefetchContext.key === planningContext.key) {
    return session.planPrefetchContext;
  }

  session.planPrefetchContext = {
    key: planningContext.key,
    folderIdByInput: new Map(),
    existingByFolderId: new Map(),
    providerStatusByFileId: new Map(),
    loadedFileIds: new Set(),
    selectedProviders: null,
    stats: {
      plannedFolderCount: 0,
      missingFolderCount: 0,
      providerFileCount: 0
    }
  };

  return session.planPrefetchContext;
}

function collectExistingFileIds(existingByFolderId) {
  const fileIds = [];
  for (const byName of existingByFolderId.values()) {
    for (const file of byName.values()) {
      const fileId = String(file?.id || '').trim();
      if (fileId) {
        fileIds.push(fileId);
      }
    }
  }
  return unique(fileIds);
}

async function buildProviderStatusLookup(existingByFolderId, chunkSize = BATCH_PROVIDER_PREFETCH_CHUNK_SIZE, selectedProviders = null) {
  const fileIds = collectExistingFileIds(existingByFolderId);
  const providerStatusByFileId = new Map();
  const providerFilter = Array.isArray(selectedProviders) && selectedProviders.length > 0
    ? unique(selectedProviders.map((provider) => String(provider || '').trim()).filter(Boolean))
    : [];

  if (fileIds.length === 0) {
    return { providerStatusByFileId, fileCount: 0, providerRowCount: 0 };
  }

  let providerRowCount = 0;
  const chunks = chunkArray(fileIds, chunkSize);
  for (const fileIdChunk of chunks) {
    const placeholders = fileIdChunk.map(() => '?').join(', ');
    const providerWhereSql = providerFilter.length > 0
      ? ` AND provider IN (${providerFilter.map(() => '?').join(', ')})`
      : '';
    const rows = await runDbQuery(
      `SELECT file_id, provider, status
         FROM file_providers
        WHERE file_id IN (${placeholders})${providerWhereSql}`,
      providerFilter.length > 0 ? [...fileIdChunk, ...providerFilter] : fileIdChunk
    );
    providerRowCount += Array.isArray(rows) ? rows.length : 0;

    for (const row of rows || []) {
      const fileId = String(row?.file_id || '').trim();
      const provider = String(row?.provider || '').trim();
      const status = String(row?.status || '').trim().toLowerCase() || 'pending';
      if (!fileId || !provider) {
        continue;
      }

      if (!providerStatusByFileId.has(fileId)) {
        providerStatusByFileId.set(fileId, new Map());
      }
      providerStatusByFileId.get(fileId).set(provider, status);
    }
  }

  return { providerStatusByFileId, fileCount: fileIds.length, providerRowCount };
}

async function buildPlannedBatchItem({
  container,
  instance,
  baseFolderInput,
  selectedProviders,
  folderCache,
  folderIdByInput,
  existingByFolderId,
  providerStatusByFileId,
  includeLocalSourceCheck = false,
  fastPreview = false
}) {
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
  const outputFileName = `${outputName}.mp4`;
  const chainPath = String(instance.path || container.path || '').trim();
  const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';
  const prefetchedFolderId = folderIdByInput?.get(finalFolderInput) || null;
  const folderId = prefetchedFolderId || await resolveFolderIdWithCache(folderCache, finalFolderInput);
  const existingFile = (folderId && existingByFolderId?.get(folderId)?.get(outputFileName)) || null;
  const planKey = createPlannedItemKey(container.containerUrlShortId, urlShortId, outputFileName, folderId);

  const plannedItem = {
    planKey,
    urlShortId,
    containerUrlShortId: container.containerUrlShortId,
    containerName: container.containerName || null,
    folderId,
    folderInput: finalFolderInput,
    outputName: outputFileName,
    path: chainPath,
    refererPath: container.containerPathUrl || null,
    selectedProviders: Array.isArray(selectedProviders) ? [...selectedProviders] : null,
    action: 'download',
    reason: 'Ready to download',
    fileId: existingFile?.id || null,
    existingStatus: existingFile?.status || null,
    pendingProviders: [],
    uploadQueue: null,
    instance: {
      ...instance,
      urlShortId,
      outputName
    }
  };

  if (!existingFile) {
    return {
      counted: true,
      planned: plannedItem
    };
  }

  const existingStatus = String(existingFile.status || '').trim().toLowerCase();
  plannedItem.fileId = existingFile.id;
  plannedItem.existingStatus = existingStatus || null;

  if (existingStatus === 'failed') {
    plannedItem.action = 'retry';
    plannedItem.reason = 'Retrying failed file';
    return {
      counted: true,
      planned: plannedItem
    };
  }

  if (existingStatus === 'processing' || existingStatus === 'uploading') {
    plannedItem.action = 'skip';
    plannedItem.reason = 'File is already being processed';
    return {
      counted: true,
      planned: plannedItem,
      skipped: {
        urlShortId,
        reason: plannedItem.reason,
        path: chainPath,
        fileId: existingFile.id,
        outputName: outputFileName,
        uploadQueue: null,
        pendingProviders: []
      }
    };
  }

  if (fastPreview) {
    plannedItem.action = 'skip';
    plannedItem.reason = 'File exists; provider validation deferred until execution';
    plannedItem.pendingProviders = [];
    plannedItem.fastPreviewProviderValidation = true;
    return {
      counted: true,
      planned: plannedItem,
      skipped: {
        urlShortId,
        reason: plannedItem.reason,
        path: chainPath,
        fileId: existingFile.id,
        outputName: outputFileName,
        uploadQueue: null,
        pendingProviders: [],
        fastPreviewProviderValidation: true
      }
    };
  }

  const targetProviders = Array.isArray(selectedProviders) ? selectedProviders : [];
  const fileProviderStatus = providerStatusByFileId?.get(existingFile.id) || new Map();
  const pendingProviders = targetProviders.filter((provider) => {
    const status = String(fileProviderStatus.get(provider) || 'pending').trim().toLowerCase();
    return status !== 'completed';
  });

  plannedItem.pendingProviders = pendingProviders;

  if (pendingProviders.length === 0) {
    plannedItem.action = 'skip';
    plannedItem.reason = 'File already exists on selected providers';
    return {
      counted: true,
      planned: plannedItem,
      skipped: {
        urlShortId,
        reason: plannedItem.reason,
        path: chainPath,
        fileId: existingFile.id,
        outputName: outputFileName,
        uploadQueue: null,
        pendingProviders: []
      }
    };
  }

  const hasLocalSource = includeLocalSourceCheck
    ? Boolean(existingFile.localPath && await fs.pathExists(existingFile.localPath))
    : true;
  if (hasLocalSource) {
    plannedItem.action = 'finalize';
    plannedItem.reason = 'File already exists locally; queued missing providers only';
    return {
      counted: true,
      planned: plannedItem
    };
  }

  plannedItem.action = 'download';
  plannedItem.reason = `Existing file ${existingFile.id} is missing local source; re-downloading missing providers`;
  return {
    counted: true,
    planned: plannedItem
  };
}

async function buildBatchPlanForContainer({
  containerDetail,
  baseFolderInput,
  selectedProviders,
  folderCache,
  folderIdByInput,
  existingByFolderId,
  providerStatusByFileId,
  includeLocalSourceCheck = false,
  fastPreview = false
}) {
  const plannedItems = [];
  const skipped = [];

  for (const instance of containerDetail.videoInstances || []) {
    const result = await buildPlannedBatchItem({
      container: containerDetail,
      instance,
      baseFolderInput,
      selectedProviders,
      folderCache,
      folderIdByInput,
      existingByFolderId,
      providerStatusByFileId,
      includeLocalSourceCheck,
      fastPreview
    });

    if (result?.planned) {
      plannedItems.push(result.planned);
    }

    if (result?.skipped) {
      skipped.push(result.skipped);
    }
  }

  return {
    containerDetail,
    plannedItems,
    skipped
  };
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
        pendingProviderInfo = await uploaderService.getPendingUploadProviders(existingFile.id, selectedProviders, {
          verifyRemote: false
        });

        if (!pendingProviderInfo.hasPendingProviders) {
          skipReason = 'File already exists on selected providers';
        } else {
          const hasLocalSource = Boolean(existingFile.localPath && await fs.pathExists(existingFile.localPath));

          if (hasLocalSource) {
            const existingPipeline = await finalizeExistingFilePipeline(existingFile.id, folderId, selectedProviders, {
              waitForUpload: true
            });
            uploadQueueResult = existingPipeline.uploadQueue;
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
  baseFolderInput = '',
  selectedProviders = null,
  sessionId,
  containerOffset,
  containerLimit,
  timeBudgetMs,
  allowSessionReuse = true
}) {
  cleanupExpiredBatchChainSessions();

  const normalizedRootCgId = normalizeCgId(rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID);
  const normalizedParentContainerName = sanitizePathSegment(stripWrappingQuotes(parentContainerName), 'unknown-parent');
  const normalizedTargetSelector = String(targetCgSelector || '').trim() || null;

  let session = null;
  const normalizedSessionId = normalizeSessionId(sessionId);
  if (allowSessionReuse && normalizedSessionId && batchChainSessions.has(normalizedSessionId)) {
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
      id: allowSessionReuse ? normalizedSessionId : null,
      rootCgId: normalizedRootCgId,
      targetCgSelector: normalizedTargetSelector,
      parentContainerName: normalizedParentContainerName
    });
    batchChainSessions.set(session.id, session);
  }

  touchBatchChainSession(session);

  // Preview/build requests are resumable already, so avoid a global deadline that can
  // cancel later upstream fetches before they even start. Keep per-request timeouts.
  const deadlineAt = null;
  const options = {
    requestContext,
    refererPath,
    deadlineAt,
    timeoutMs: clampPositiveInt(BATCH_UPSTREAM_TIMEOUT_MS, 7000),
    maxRetries: clampPositiveInt(BATCH_UPSTREAM_MAX_RETRIES, 1)
  };

  const planningContext = normalizePlanningContext(baseFolderInput, selectedProviders);
  ensureBatchPlanContext(session, planningContext);

  await advanceBatchChainSessionDiscovery(session, options, deadlineAt);

  touchBatchChainSession(session);

  const mergedContainers = Array.from(session.containerByShortId.values());
  const totalContainers = mergedContainers.length;
  const normalizedOffset = Math.min(normalizeChunkOffset(containerOffset), totalContainers);
  const normalizedLimit = normalizeChunkLimit(containerLimit) || clampPositiveInt(DEFAULT_BATCH_CHAIN_CHUNK_SIZE, 8);
  const details = [];
  const plannedItems = [];
  const skipped = [];
  const prefetch = {
    folderCount: 0,
    missingFolderCount: 0,
    providerFileCount: 0
  };
  const pendingContainers = [];
  let cursor = normalizedOffset;
  while (cursor < totalContainers && pendingContainers.length < normalizedLimit) {
    pendingContainers.push(mergedContainers[cursor]);
    cursor += 1;
  }

  if (pendingContainers.length > 0) {
    const detailLimit = pLimit(clampPositiveInt(BATCH_CG_FETCH_CONCURRENCY, 8));
    const detailResults = await Promise.all(
      pendingContainers.map((container) => detailLimit(async () => {
        const containerDetail = await buildBatchContainerDetail({
          container,
          options,
          session
        });

        return containerDetail || null;
      }))
    );

    for (const containerDetail of detailResults) {
      if (!containerDetail) {
        continue;
      }

      const containerShortId = String(containerDetail.containerUrlShortId || '').trim();
      if (containerShortId) {
        session.containerDetailsByShortId.set(containerShortId, containerDetail);
      }
      details.push(containerDetail);
    }

    const prefetchContext = await prefetchBatchPlanContext({
      containerDetails: details,
      baseFolderInput: planningContext.baseFolderInput,
      selectedProviders: planningContext.selectedProviders,
      session
    });
    prefetch.folderCount = Number(prefetchContext.prefetchFolderCount || 0);
    prefetch.missingFolderCount = Number(prefetchContext.prefetchMissingFolderCount || 0);
    prefetch.providerFileCount = Number(prefetchContext.prefetchProviderFileCount || 0);

    const planLimit = pLimit(clampPositiveInt(BATCH_INSTANCE_METADATA_CONCURRENCY, 12));
    const planResults = await Promise.all(
      details.map((containerDetail) => planLimit(async () => buildBatchPlanForContainer({
        containerDetail,
        baseFolderInput: planningContext.baseFolderInput,
        selectedProviders: prefetchContext.selectedProviders,
        folderCache: session.planFolderCache,
        folderIdByInput: prefetchContext.folderIdByInput,
        existingByFolderId: prefetchContext.existingByFolderId,
        providerStatusByFileId: prefetchContext.providerStatusByFileId,
        includeLocalSourceCheck: false,
        fastPreview: Boolean(prefetchContext.fastPreview)
      })))
    );

    for (const resolved of planResults) {
      if (!resolved?.containerDetail) {
        continue;
      }
      plannedItems.push(...(resolved.plannedItems || []));
      skipped.push(...(resolved.skipped || []));
    }
  }

  if (normalizedOffset === session.planCursor) {
    for (const containerDetail of details) {
      session.plannedContainers.push(containerDetail);
    }

    for (const plannedItem of plannedItems) {
      if (!plannedItem?.planKey || session.plannedItemByKey.has(plannedItem.planKey)) {
        continue;
      }

      session.plannedItemByKey.set(plannedItem.planKey, plannedItem);
      session.plannedItems.push(plannedItem);
    }

    session.planCursor = cursor;
    if (cursor >= totalContainers && session.discoveryDone) {
      session.planReady = true;
    }
  }

  const hasMoreKnownContainers = cursor < totalContainers;
  const hasMoreContainers = hasMoreKnownContainers || !session.discoveryDone;
  const nextContainerOffset = hasMoreContainers ? cursor : null;

  return serializeBatchChainSessionState(session, {
    containerOffset: normalizedOffset,
    containerLimit: normalizedLimit,
    processedContainerCount: details.length,
    hasMoreContainers,
    nextContainerOffset,
    skipped,
    prefetch
  });
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
  return downloadQueue.activeCount < downloadQueue.maxConcurrent && downloadQueue.queuedCount === 0;
}

function getQueueStatus() {
  const status = downloadQueue.getStatus();
  return {
    ...status,
    availableSlots: Math.max(0, status.max - status.active),
    canAcceptImmediately: status.active < status.max && status.queued === 0,
    workerState: status.isProcessing ? 'draining' : (status.active > 0 ? 'running' : 'idle')
  };
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
  let details;
  try {
    details = await getInstanceDetails({
      urlShortId,
      refererPath: task.refererPath,
      fallbackRefererPath: task.fallbackRefererPath,
      requestContext
    });
  } catch (error) {
    console.warn(`[Zenius] Skipping ${urlShortId}: failed to fetch instance details after retries (${error.message})`);
    return null;
  }

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

async function prefetchBatchPlanContext({ containerDetails, baseFolderInput, selectedProviders, session = null }) {
  const fastPreview = Boolean(session?.fastPreview);
  const activeProviders = Array.isArray(selectedProviders) && selectedProviders.length > 0
    ? selectedProviders
    : await getEnabledProviderIds();

  if (!session) {
    const existingLookup = await buildExistingFileLookup(
      containerDetails,
      baseFolderInput,
      clampPositiveInt(BATCH_FOLDER_PREFETCH_CHUNK_SIZE, 80),
      session
    );

    const providerLookup = fastPreview
      ? { providerStatusByFileId: new Map(), fileCount: 0, providerRowCount: 0 }
      : await buildProviderStatusLookup(
        existingLookup.existingByFolderId,
        clampPositiveInt(BATCH_PROVIDER_PREFETCH_CHUNK_SIZE, 200),
        activeProviders
      );

    return {
      folderIdByInput: existingLookup.folderIdByInput,
      existingByFolderId: existingLookup.existingByFolderId,
      providerStatusByFileId: providerLookup.providerStatusByFileId,
      selectedProviders: activeProviders,
      prefetchFolderCount: existingLookup.plannedFolderCount,
      prefetchMissingFolderCount: existingLookup.missingFolderCount,
      prefetchProviderFileCount: providerLookup.fileCount,
      prefetchProviderRowCount: providerLookup.providerRowCount,
      fastPreview
    };
  }

  const planningContext = normalizePlanningContext(baseFolderInput, selectedProviders);
  const cache = ensureBatchPlanPrefetchContext(session, planningContext);
  cache.selectedProviders = activeProviders;
  const plannedByFolderInput = collectPlannedNamesByFolderInput(containerDetails, baseFolderInput);
  const existingFolderMap = await loadExistingFolderMap(session);
  const startedAt = Date.now();
  let missingFolderCount = Number(cache.stats.missingFolderCount || 0);
  let folderQueryCount = 0;
  let fileRowCount = 0;

  for (const [folderInput, plannedNames] of plannedByFolderInput.entries()) {
    const folderKey = normalizeFolderInputKey(folderInput);
    const folderId = existingFolderMap.get(folderKey) || null;

    if (!cache.folderIdByInput.has(folderInput)) {
      cache.folderIdByInput.set(folderInput, folderId);
      cache.stats.plannedFolderCount = Number(cache.stats.plannedFolderCount || 0) + 1;
      if (!folderId) {
        missingFolderCount += 1;
      }
    }

    if (!folderId) {
      continue;
    }

    if (!cache.existingByFolderId.has(folderId)) {
      cache.existingByFolderId.set(folderId, new Map());
    }

    const knownByName = cache.existingByFolderId.get(folderId);
    const missingNames = [];
    for (const plannedName of plannedNames) {
      if (!knownByName.has(plannedName)) {
        missingNames.push(plannedName);
      }
    }

    if (missingNames.length === 0) {
      continue;
    }

    const placeholders = missingNames.map(() => '?').join(', ');
    const rows = await runDbQuery(
      `SELECT id, folder_id, name, status, local_path
         FROM files
        WHERE folder_id = ?
          AND name IN (${placeholders})
        ORDER BY created_at DESC`,
      [folderId, ...missingNames]
    );
    folderQueryCount += 1;
    fileRowCount += Array.isArray(rows) ? rows.length : 0;

    for (const row of rows || []) {
      const nameKey = String(row?.name || '').trim();
      if (!nameKey || knownByName.has(nameKey)) {
        continue;
      }

      knownByName.set(nameKey, {
        id: row.id,
        name: row.name,
        folder_id: row.folder_id,
        status: row.status,
        localPath: row.local_path || null
      });
    }
  }

  cache.stats.missingFolderCount = missingFolderCount;

  const newFileIds = [];
  for (const byName of cache.existingByFolderId.values()) {
    for (const file of byName.values()) {
      const fileId = String(file?.id || '').trim();
      if (!fileId || cache.loadedFileIds.has(fileId)) {
        continue;
      }
      cache.loadedFileIds.add(fileId);
      newFileIds.push(fileId);
    }
  }

  if (!fastPreview) {
    for (const fileIdChunk of chunkArray(newFileIds, clampPositiveInt(BATCH_PROVIDER_PREFETCH_CHUNK_SIZE, 200))) {
      if (fileIdChunk.length === 0) {
        continue;
      }

      const placeholders = fileIdChunk.map(() => '?').join(', ');
      const rows = await runDbQuery(
        `SELECT file_id, provider, status
           FROM file_providers
          WHERE file_id IN (${placeholders})${cache.selectedProviders?.length ? ` AND provider IN (${cache.selectedProviders.map(() => '?').join(', ')})` : ''}`,
        cache.selectedProviders?.length ? [...fileIdChunk, ...cache.selectedProviders] : fileIdChunk
      );
      cache.stats.providerRowCount = Number(cache.stats.providerRowCount || 0) + (Array.isArray(rows) ? rows.length : 0);

      for (const row of rows || []) {
        const fileId = String(row?.file_id || '').trim();
        const provider = String(row?.provider || '').trim();
        const status = String(row?.status || '').trim().toLowerCase() || 'pending';
        if (!fileId || !provider) {
          continue;
        }

        if (!cache.providerStatusByFileId.has(fileId)) {
          cache.providerStatusByFileId.set(fileId, new Map());
        }
        cache.providerStatusByFileId.get(fileId).set(provider, status);
      }
    }
  }

  cache.stats.providerFileCount = cache.loadedFileIds.size;
  console.log(
    `[Zenius][Preview][DupeCheck] containers=${containerDetails.length} folders=${plannedByFolderInput.size} `
    + `folderQueries=${folderQueryCount} fileRows=${fileRowCount} newProviderFiles=${newFileIds.length} `
    + `providerRows=${Number(cache.stats.providerRowCount || 0)} selectedProviders=${Array.isArray(activeProviders) ? activeProviders.join(',') : 'all'} `
    + `fastPreview=${fastPreview} durationMs=${Date.now() - startedAt}`
  );

  return {
    folderIdByInput: cache.folderIdByInput,
    existingByFolderId: cache.existingByFolderId,
    providerStatusByFileId: cache.providerStatusByFileId,
    selectedProviders: activeProviders,
    prefetchFolderCount: Number(cache.stats.plannedFolderCount || 0),
    prefetchMissingFolderCount: Number(cache.stats.missingFolderCount || 0),
    prefetchProviderFileCount: Number(cache.stats.providerFileCount || 0),
    prefetchProviderRowCount: Number(cache.stats.providerRowCount || 0),
    fastPreview
  };
}

async function queueBatchDownloadChunk({ chain, requestContext, refererPath, baseFolderInput, selectedProviders, runId = null, cancelled = () => false }) {
  const queued = [];
  const skipped = [];
  let totalInstances = 0;
  const session = chain.sessionId ? batchChainSessions.get(chain.sessionId) || null : null;
  const planningContext = normalizePlanningContext(baseFolderInput, selectedProviders);
  if (!session || session.planContextKey !== planningContext.key || !Array.isArray(session.plannedItems)) {
    throw new Error('Preview plan is not available for the requested folder/providers context');
  }

  const normalizedOffset = Math.min(normalizeChunkOffset(chain.containerOffset), session.plannedItems.length);
  const normalizedLimit = normalizeChunkLimit(chain.containerLimit) || clampPositiveInt(BACKGROUND_BATCH_CHUNK_SIZE, 6);
  const planChunk = session.plannedItems.slice(normalizedOffset, normalizedOffset + normalizedLimit);

  for (const plannedItem of planChunk) {
    if (cancelled()) {
      break;
    }

    totalInstances += 1;

    if (plannedItem.fastPreviewProviderValidation && plannedItem.fileId) {
      const existingFile = await db.getFile(plannedItem.fileId).catch(() => null);
      const existingStatus = String(existingFile?.status || plannedItem.existingStatus || '').trim().toLowerCase();

      if (existingStatus === 'processing' || existingStatus === 'uploading') {
        skipped.push({
          urlShortId: plannedItem.urlShortId,
          reason: 'File is already being processed',
          path: plannedItem.path,
          fileId: plannedItem.fileId,
          outputName: plannedItem.outputName,
          uploadQueue: null,
          pendingProviders: []
        });
        continue;
      }

      if (existingStatus !== 'failed') {
        const pendingProviderInfo = await uploaderService.getPendingUploadProviders(plannedItem.fileId, plannedItem.selectedProviders, {
          verifyRemote: false
        });

        if (!pendingProviderInfo.hasPendingProviders) {
          skipped.push({
            urlShortId: plannedItem.urlShortId,
            reason: 'File already exists on selected providers',
            path: plannedItem.path,
            fileId: plannedItem.fileId,
            outputName: plannedItem.outputName,
            uploadQueue: null,
            pendingProviders: []
          });
          continue;
        }

        plannedItem.action = 'finalize';
        plannedItem.reason = 'File already exists locally; queued missing providers only';
        plannedItem.pendingProviders = pendingProviderInfo.pendingProviders || [];
        if (!existingFile?.localPath || !await fs.pathExists(existingFile.localPath)) {
          plannedItem.action = 'download';
          plannedItem.reason = `Existing file ${plannedItem.fileId} is missing local source; re-downloading missing providers`;
        }
      } else {
        plannedItem.action = 'retry';
        plannedItem.reason = 'Retrying failed file';
      }
    }

    if (plannedItem.action === 'skip') {
      skipped.push({
        urlShortId: plannedItem.urlShortId,
        reason: plannedItem.reason,
        path: plannedItem.path,
        fileId: plannedItem.fileId,
        outputName: plannedItem.outputName,
        uploadQueue: null,
        pendingProviders: plannedItem.pendingProviders || []
      });
      continue;
    }

    if (plannedItem.action === 'finalize') {
      const finalizeProviders = Array.isArray(plannedItem.pendingProviders) && plannedItem.pendingProviders.length > 0
        ? plannedItem.pendingProviders
        : plannedItem.selectedProviders;

      const existingFile = plannedItem.fileId ? await db.getFile(plannedItem.fileId).catch(() => null) : null;
      const hasLocalSource = Boolean(existingFile?.localPath && await fs.pathExists(existingFile.localPath));
      if (!hasLocalSource) {
        await waitForDownloadQueueCapacity({
          cancelled,
          maxQueued: getBatchDownloadMaxQueued()
        });
        if (cancelled()) {
          break;
        }

        const requestedFilename = String(plannedItem.outputName || '').replace(/\.mp4$/i, '');
        const queuedDownloadPromise = queueDownload({
          urlShortId: plannedItem.urlShortId,
          folderId: plannedItem.folderId,
          outputName: requestedFilename,
          selectedProviders: finalizeProviders,
          requestContext,
          refererPath: plannedItem.refererPath || refererPath,
          fallbackRefererPath: plannedItem.refererPath || '',
          requestedFilename,
          skipIfExists: false,
          plannedFromPreview: true,
          previewPlan: {
            sessionId: session.id,
            planKey: plannedItem.planKey,
            action: 'download',
            reason: `Existing file ${plannedItem.fileId || 'unknown'} missing local source at execution time; switching finalize -> download`,
            fileId: plannedItem.fileId || null,
            existingStatus: plannedItem.existingStatus || null,
            pendingProviders: plannedItem.pendingProviders || []
          }
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
            console.log(`[Zenius] Batch download ${plannedItem.urlShortId} was cancelled`);
          } else {
            console.log(`[Zenius] Batch download ${plannedItem.urlShortId} completed successfully`);
            if (runId && backgroundBatchRuns.has(runId)) {
              const run = backgroundBatchRuns.get(runId);
              run.downloadCompletedCount = (run.downloadCompletedCount || 0) + 1;
              touchBackgroundBatchRun(run);
            }
          }
        }).catch((error) => {
          console.error(`[Zenius] Batch download pipeline failed for ${plannedItem.urlShortId}:`, error.message);
          if (runId && backgroundBatchRuns.has(runId)) {
            const run = backgroundBatchRuns.get(runId);
            run.downloadFailedCount = (run.downloadFailedCount || 0) + 1;
            touchBackgroundBatchRun(run);
          }
        });

        queued.push({
          urlShortId: plannedItem.urlShortId,
          name: plannedItem.instance?.name || null,
          outputName: plannedItem.outputName,
          path: plannedItem.path,
          folderInput: plannedItem.folderInput,
          folderId: plannedItem.folderId,
          action: 'download',
          reason: `Execution fallback: missing local source for finalize item ${plannedItem.fileId || 'unknown'}`,
          pendingProviders: plannedItem.pendingProviders || [],
          existingFileId: plannedItem.fileId || null,
          status: 'queued'
        });
        continue;
      }

      const existingPipeline = await finalizeExistingFilePipeline(plannedItem.fileId, plannedItem.folderId, finalizeProviders, {
        waitForUpload: true
      });
      skipped.push({
        urlShortId: plannedItem.urlShortId,
        reason: plannedItem.reason,
        path: plannedItem.path,
        fileId: plannedItem.fileId,
        outputName: plannedItem.outputName,
        uploadQueue: existingPipeline?.uploadQueue || null,
        pendingProviders: plannedItem.pendingProviders || []
      });
      continue;
    }

    const requestedFilename = String(plannedItem.outputName || '').replace(/\.mp4$/i, '');
    const uploadProviders = Array.isArray(plannedItem.pendingProviders) && plannedItem.pendingProviders.length > 0
      ? plannedItem.pendingProviders
      : plannedItem.selectedProviders;
    await waitForDownloadQueueCapacity({
      cancelled,
      maxQueued: getBatchDownloadMaxQueued()
    });
    if (cancelled()) {
      break;
    }

    const queuedDownloadPromise = queueDownload({
      urlShortId: plannedItem.urlShortId,
      folderId: plannedItem.folderId,
      outputName: requestedFilename,
      selectedProviders: uploadProviders,
      requestContext,
      refererPath: plannedItem.refererPath || refererPath,
      fallbackRefererPath: plannedItem.refererPath || '',
      requestedFilename,
      skipIfExists: false,
      plannedFromPreview: true,
      previewPlan: {
        sessionId: session.id,
        planKey: plannedItem.planKey,
        action: plannedItem.action,
        reason: plannedItem.reason,
        fileId: plannedItem.fileId || null,
        existingStatus: plannedItem.existingStatus || null,
        pendingProviders: plannedItem.pendingProviders || []
      }
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
        console.log(`[Zenius] Batch download ${plannedItem.urlShortId} was cancelled`);
      } else {
        console.log(`[Zenius] Batch download ${plannedItem.urlShortId} completed successfully`);
        if (runId && backgroundBatchRuns.has(runId)) {
          const run = backgroundBatchRuns.get(runId);
          run.downloadCompletedCount = (run.downloadCompletedCount || 0) + 1;
          touchBackgroundBatchRun(run);
        }
      }
    }).catch((error) => {
      console.error(`[Zenius] Batch download pipeline failed for ${plannedItem.urlShortId}:`, error.message);
      if (runId && backgroundBatchRuns.has(runId)) {
        const run = backgroundBatchRuns.get(runId);
        run.downloadFailedCount = (run.downloadFailedCount || 0) + 1;
        touchBackgroundBatchRun(run);
      }
    });

    queued.push({
      urlShortId: plannedItem.urlShortId,
      name: plannedItem.instance?.name || null,
      outputName: plannedItem.outputName,
      path: plannedItem.path,
      folderInput: plannedItem.folderInput,
      folderId: plannedItem.folderId,
      action: plannedItem.action,
      reason: plannedItem.reason,
      pendingProviders: plannedItem.pendingProviders || [],
      existingFileId: plannedItem.fileId || null,
      status: plannedItem.action === 'retry' ? 'retry-queued' : 'queued'
    });
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

    const planningContext = normalizePlanningContext(payload.baseFolderInput, payload.selectedProviders);

    if (!currentSessionId || !batchChainSessions.has(currentSessionId)) {
      throw new Error('Preview plan session is missing');
    }

    const session = batchChainSessions.get(currentSessionId);
    if (!session || session.planContextKey !== planningContext.key || !session.planReady) {
      throw new Error('Preview plan is not ready for the requested folder/providers context');
    }

    run.rootCgName = session.rootCgName || run.rootCgName;
    run.parentContainerName = resolveBatchParentContainerName(session) || run.parentContainerName;
    run.totalContainers = Number(session.plannedItems?.length || 0);
    run.hasMoreContainers = run.totalContainers > 0;
    run.nextContainerOffset = 0;
    run.chainErrors = Array.isArray(session.errors) ? [...session.errors] : [];

    while (hasMore) {
      if (run.status !== 'running') {
        break;
      }

      iteration += 1;
      if (iteration > BACKGROUND_BATCH_MAX_ITERATIONS) {
        throw new Error(`Background batch iteration limit reached (${BACKGROUND_BATCH_MAX_ITERATIONS}). Increase ZENIUS_BACKGROUND_BATCH_MAX_ITERATIONS if needed.`);
      }

      const planSession = batchChainSessions.get(currentSessionId);
      if (!planSession || planSession.planContextKey !== planningContext.key || !planSession.planReady) {
        throw new Error('Preview plan session is no longer available');
      }

      const totalPlannedItems = Number(planSession.plannedItems?.length || 0);
      const chunkLimit = normalizeChunkLimit(payload.containerLimit) || clampPositiveInt(BACKGROUND_BATCH_CHUNK_SIZE, 6);
      const chunkOffset = Math.min(nextOffset, totalPlannedItems);
      const nextChunkOffset = Math.min(chunkOffset + chunkLimit, totalPlannedItems);
      const chain = {
        sessionId: currentSessionId,
        containerOffset: chunkOffset,
        containerLimit: chunkLimit,
        totalContainers: totalPlannedItems,
        nextContainerOffset: nextChunkOffset >= totalPlannedItems ? null : nextChunkOffset,
        hasMoreContainers: nextChunkOffset < totalPlannedItems,
        errors: Array.isArray(planSession.errors) ? [...planSession.errors] : []
      };

      run.sessionId = currentSessionId;
      run.rootCgName = planSession.rootCgName || run.rootCgName;
      run.parentContainerName = resolveBatchParentContainerName(planSession) || run.parentContainerName;
      run.totalContainers = totalPlannedItems;
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

      if (run.status === 'completed' && Number(run.downloadFailedCount || 0) === 0) {
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
      } else {
        console.log(`[Webhook] Skipped batch notification for run ${run.id}: status=${run.status}, failed=${run.downloadFailedCount || 0}`);
      }
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
        if (result.skipped) {
          console.warn(`[Zenius] Download ${urlShortId} skipped: ${result.reason || 'unknown'}`);
        } else if (result.cancelled) {
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
        baseFolderInput: stripWrappingQuotes(req.body?.folderId || ''),
        selectedProviders: await normalizeProviders(req.body?.providers),
        sessionId: req.body?.sessionId,
        containerOffset: req.body?.containerOffset,
        containerLimit: req.body?.containerLimit,
        timeBudgetMs: req.body?.timeBudgetMs,
        allowSessionReuse: true
      });

      res.json({
        success: true,
        data: chain
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async startBatchChainBuild(req, res) {
    try {
      cleanupExpiredBatchChainSessions();
      cleanupExpiredBackgroundBatchRuns();

      const selectedProviders = await normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');
      const fastPreview = parseBoolean(req.body?.fastPreview) === true;
      const keepaliveUrl = resolveBackgroundKeepaliveUrl(req);

      const run = createBackgroundBatchPreviewRun({
        rootCgId: normalizeCgId(req.body?.rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID),
        targetCgSelector: req.body?.targetCgSelector,
        parentContainerName: req.body?.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
        baseFolderInput,
        selectedProviders,
        keepaliveUrl,
        headersRaw: req.body?.headersRaw || '',
        refererPath: req.body?.refererPath || '',
        fastPreview
      });

      const previewSession = createBatchChainSession({
        rootCgId: run.rootCgId,
        targetCgSelector: run.targetCgSelector,
        parentContainerName: run.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME
      });
      previewSession.type = 'preview';
      previewSession.fastPreview = fastPreview;
      batchChainSessions.set(previewSession.id, previewSession);
      run.sessionId = previewSession.id;

      backgroundBatchRuns.set(run.id, run);
      ensureBackgroundBatchKeepalive(keepaliveUrl);

      await withDbRetry(
        () => db.createBatchSession({
          id: run.id,
          runId: run.id,
          rootCgId: run.rootCgId,
          rootCgName: run.rootCgName,
          targetCgSelector: run.targetCgSelector,
          parentContainerName: run.parentContainerName,
          status: run.status,
          totalContainers: 0,
          processedContainers: 0,
          queuedCount: 0,
          skippedCount: 0,
          nextContainerOffset: 0,
          hasMore: true,
          sessionData: {
            type: 'preview',
            sessionId: run.sessionId,
            fastPreview: run.fastPreview,
            baseFolderInput: baseFolderInput || '',
            selectedProviders,
            chainPreview: null,
            previewSummary: summarizePreviewChain(null)
          },
          queuedItems: [],
          skippedItems: [],
          chainErrors: [],
          expiresAt: run.expiresAt
        }),
        `preview-session-create[${run.id}]`
      );

      res.status(202).json({
        success: true,
        message: 'Zenius batch chain session started',
        data: {
          sessionId: run.id,
          previewRunId: run.id,
          planSessionId: run.sessionId,
          status: serializeBackgroundBatchRun(run)
        }
      });

      const requestContext = buildRequestContext(req.body || {}, req);
      void runBackgroundBatchPreviewLoop(run, {
        requestContext,
        refererPath: req.body?.refererPath || ''
      }).catch((error) => {
        console.error(`[Zenius] Background preview run ${run.id} crashed:`, error.message);
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getBatchChainBuildStatus(req, res) {
    try {
      res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      res.set('Pragma', 'no-cache');
      res.set('Expires', '0');
      const runId = normalizeSessionId(req.params.id);
      const run = runId ? backgroundBatchRuns.get(runId) : null;

      if (!run || run.type !== 'preview') {
        const dbSession = runId ? await db.getBatchSession(runId) : null;
        if (dbSession?.sessionData?.type === 'preview') {
          return res.json({
            success: true,
            data: serializePersistedPreviewRun(dbSession, { includePreview: true })
          });
        }

        return res.status(404).json({ success: false, error: 'Batch preview build not found' });
      }

      res.json({
        success: true,
        data: serializeBackgroundBatchRun(run, { includePreview: true })
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async downloadBatch(req, res) {
    try {
      const sessionId = normalizeSessionId(req.body?.sessionId);
      const previewRunId = normalizeSessionId(req.body?.previewRunId);
      const session = await hydrateBatchChainSessionForDownload({ sessionId, previewRunId });
      if (!session) {
        return res.status(400).json({
          success: false,
          error: 'Preview chain required before batch download. Call /batch-chain first to build a preview session, then pass the plan sessionId here.',
          details: {
            receivedSessionId: sessionId,
            receivedPreviewRunId: previewRunId,
            sessionInMemory: sessionId ? batchChainSessions.has(sessionId) : false
          }
        });
      }

      // Touch the session BEFORE cleanup to ensure it won't be expired
      touchBatchChainSession(session);

      cleanupExpiredBatchChainSessions();
      cleanupExpiredBackgroundBatchRuns();

      const requestContext = buildRequestContext(req.body || {}, req);
      const selectedProviders = await normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');
      const planningContext = normalizePlanningContext(baseFolderInput, selectedProviders);

      if (!session || session.planContextKey !== planningContext.key || !session.planReady) {
        return res.status(400).json({
          success: false,
          error: 'Preview plan is not ready for this folder/providers context. Call /batch-chain until hasMoreContainers=false and planReady=true, then start batch download.',
          details: {
            receivedSessionId: sessionId,
            actualSessionId: session?.id || null,
            planReady: Boolean(session?.planReady),
            expectedContextKey: session?.planContextKey || null,
            requestedContextKey: planningContext.key,
            requestedFolder: baseFolderInput,
            requestedProviders: selectedProviders
          }
        });
      }

      const previewResult = serializeBatchPreviewSession(session);
      prepareBatchPlanForExecution(session);

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

      run.sessionId = session.id;

      backgroundBatchRuns.set(run.id, run);
      ensureBackgroundBatchKeepalive(keepaliveUrl);

      eventEmitter.emit('zenius:batch:started', {
        batchRunId: run.id,
        sessionId: session.id,
        rootCgId: run.rootCgId,
        targetCgSelector: run.targetCgSelector,
        baseFolderInput,
        providers: selectedProviders,
        preview: previewResult,
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
          status: serializeBackgroundBatchRun(run, { includePreview: true }),
          providers: selectedProviders,
          preview: previewResult,
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

  async cancelBatchRun(req, res) {
    try {
      const runId = normalizeSessionId(req.params.id);
      if (!runId) {
        return res.status(400).json({ success: false, error: 'Batch run id is required' });
      }

      const run = await cancelBackgroundBatchRun(runId, 'Cancelled by user');
      if (!run) {
        return res.status(404).json({ success: false, error: 'Background batch run not found' });
      }

      res.json({
        success: true,
        message: 'Background batch cancelled',
        data: serializeBackgroundBatchRun(run, { includePreview: true })
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

  async getQueueStatus(req, res) {
    res.json({
      success: true,
      data: getZeniusStatusSnapshot()
    });
  },

  async setMaxConcurrent(req, res) {
    const value = req.body?.maxConcurrent;
    if (typeof value !== 'number' || value < 1 || value > 50) {
      throw new AppError('maxConcurrent must be a number between 1 and 50', {
        statusCode: 400,
        code: 'INVALID_MAX_CONCURRENT'
      });
    }
    const actual = downloadQueue.setMaxConcurrent(value);
    console.log(`[Zenius] Max concurrent pipelines set to ${actual}`);
    res.json({
      success: true,
      data: { maxConcurrent: actual, ...getZeniusStatusSnapshot() }
    });
  },

  async getUploadConcurrency(req, res) {
    res.json({
      success: true,
      data: uploaderService.getConcurrencyConfig()
    });
  },

  async setUploadConcurrency(req, res) {
    const result = {};
    if (req.body?.maxConcurrentUploads !== undefined) {
      const v = req.body.maxConcurrentUploads;
      if (typeof v !== 'number' || v < 1 || v > 20) {
        throw new AppError('maxConcurrentUploads must be a number between 1 and 20', {
          statusCode: 400,
          code: 'INVALID_MAX_CONCURRENT_UPLOADS'
        });
      }
      result.maxConcurrentUploads = uploaderService.setMaxConcurrentUploads(v);
      console.log(`[Zenius] Max concurrent uploads set to ${result.maxConcurrentUploads}`);
    }
    if (req.body?.maxConcurrentProviders !== undefined) {
      const v = req.body.maxConcurrentProviders;
      if (typeof v !== 'number' || v < 1 || v > 20) {
        throw new AppError('maxConcurrentProviders must be a number between 1 and 20', {
          statusCode: 400,
          code: 'INVALID_MAX_CONCURRENT_PROVIDERS'
        });
      }
      result.maxConcurrentProviders = uploaderService.setMaxConcurrentProviders(v);
      console.log(`[Zenius] Max concurrent providers set to ${result.maxConcurrentProviders}`);
    }
    res.json({
      success: true,
      data: { ...result, ...uploaderService.getConcurrencyConfig() }
    });
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
          inMemoryRun: inMemoryRun ? serializeBackgroundBatchRun(inMemoryRun, { includePreview: true }) : null
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
if (process.env.NODE_ENV === 'test') {
  module.exports.__test = {
    batchChainSessions,
    hydrateBatchChainSessionFromPersistedPreview,
    hydrateBatchChainSessionForDownload,
    buildBatchChain,
    buildProviderStatusLookup,
    getBatchDownloadMaxQueued,
    normalizePlanningContext,
    serializeBatchPreviewSession,
    constants: {
      BACKGROUND_BATCH_CHUNK_SIZE,
      BATCH_DOWNLOAD_MAX_QUEUED_MULTIPLIER,
      DEFAULT_MAX_CONCURRENT_DOWNLOADS,
      BATCH_CG_FETCH_CONCURRENCY,
      BATCH_INSTANCE_METADATA_CONCURRENCY,
      BATCH_PREVIEW_STEPS_PER_POLL,
      BATCH_PREVIEW_CONTAINER_LIMIT,
      BATCH_PREVIEW_RETRY_LIMIT,
      BATCH_PREVIEW_RETRY_DELAY_MS
    }
  };
}
