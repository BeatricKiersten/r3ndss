const axios = require('axios');
const { randomUUID } = require('crypto');
const path = require('path');
const pLimit = require('p-limit');

const config = require('../config');
const { db, videoProcessor, uploaderService } = require('../services/runtime');
const webhookService = require('../services/webhookService');

const ZENIUS_BASE_URL = 'https://www.zenius.net';
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0';
const DEFAULT_BATCH_ROOT_CGROUP_ID = '34';
const DEFAULT_BATCH_PARENT_CONTAINER_NAME = '';
const RETRYABLE_UPSTREAM_STATUS = new Set([429, 500, 502, 503, 504]);
const UPSTREAM_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_UPSTREAM_TIMEOUT_MS || '12000', 10);
const UPSTREAM_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_UPSTREAM_MAX_RETRIES || '3', 10);
const UPSTREAM_RETRY_BASE_DELAY_MS = Number.parseInt(process.env.ZENIUS_UPSTREAM_RETRY_BASE_DELAY_MS || '500', 10);
const BATCH_CG_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_CG_FETCH_CONCURRENCY || '4', 10);
const BATCH_CONTAINER_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_CONTAINER_FETCH_CONCURRENCY || '4', 10);
const MAX_BATCH_ERRORS = Number.parseInt(process.env.ZENIUS_MAX_BATCH_ERRORS || '100', 10);
const DEFAULT_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_CHUNK_SIZE || '8', 10);
const MAX_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_MAX_CHUNK_SIZE || '20', 10);
const DEFAULT_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_BUDGET_MS || '24000', 10);
const MAX_BATCH_REQUEST_BUDGET_MS = Number.parseInt(process.env.ZENIUS_BATCH_REQUEST_MAX_BUDGET_MS || '28000', 10);
const BATCH_SESSION_TTL_MS = Number.parseInt(process.env.ZENIUS_BATCH_SESSION_TTL_MS || '900000', 10);
const BATCH_UPSTREAM_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_TIMEOUT_MS || '7000', 10);
const BATCH_UPSTREAM_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BATCH_UPSTREAM_MAX_RETRIES || '1', 10);
const BATCH_DEADLINE_GUARD_MS = Number.parseInt(process.env.ZENIUS_BATCH_DEADLINE_GUARD_MS || '1200', 10);
const BACKGROUND_BATCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_CHUNK_SIZE || '6', 10);
const BACKGROUND_BATCH_RUN_TTL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_RUN_TTL_MS || '21600000', 10);
const BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_INTERVAL_MS || '20000', 10);
const BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BACKGROUND_BATCH_KEEPALIVE_TIMEOUT_MS || '10000', 10);
const batchChainSessions = new Map();
const backgroundBatchRuns = new Map();
let backgroundBatchKeepaliveTimer = null;
let backgroundBatchKeepaliveUrl = String(process.env.ZENIUS_BATCH_KEEPALIVE_URL || '').trim();

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
  get activeTasks() { return Array.from(this._active.values()).map((t) => ({ id: t.id, urlShortId: t.urlShortId, outputName: t.outputName, startTime: t.startTime })); }
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
    const { urlShortId, videoUrl, folderId, outputName, ffmpegHeaders, selectedProviders } = task;

    console.log(`[DownloadQueue] Starting #${task.id} ${urlShortId} (active: ${this._active.size}/${this._maxConcurrent}, queued: ${this._queue.length})`);

    let fileId = null;

    try {
      const result = await videoProcessor.processHls(videoUrl, {
        folderId,
        outputName,
        outputDir: config.uploadDir,
        headers: ffmpegHeaders,
        skipIfExists: true
      });

      fileId = result.fileId;

      if (result.skipped) {
        let uploadQueueResult = null;
        if (result.reason === 'File already exists') {
          uploadQueueResult = await uploaderService.queueFileUpload(result.fileId, result.outputPath, folderId, selectedProviders);
        }
        console.log(`[DownloadQueue] #${task.id} ${urlShortId} skipped: ${result.reason}`);
        return { success: true, id: task.id, fileId, urlShortId, skipped: true, reason: result.reason, uploadQueue: uploadQueueResult };
      }

      console.log(`[DownloadQueue] #${task.id} ${urlShortId} downloaded, fileId=${fileId}, queuing upload`);

      await uploaderService.queueFileUpload(result.fileId, result.outputPath, folderId, selectedProviders);

      return { success: true, id: task.id, fileId, urlShortId };
    } catch (error) {
      console.error(`[DownloadQueue] #${task.id} ${urlShortId} failed:`, error.message);
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

const KNOWN_REQUEST_HEADERS = new Set([
  'accept',
  'accept-language',
  'accept-encoding',
  'referer',
  'user-agent',
  'sentry-trace',
  'baggage',
  'connection',
  'sec-fetch-dest',
  'sec-fetch-mode',
  'sec-fetch-site',
  'priority',
  'te',
  'pragma',
  'cache-control',
  'cookie',
  'host'
]);

const PASSTHROUGH_HEADER_MAP = {
  accept: 'Accept',
  'accept-language': 'Accept-Language',
  'accept-encoding': 'Accept-Encoding',
  'sec-fetch-dest': 'Sec-Fetch-Dest',
  'sec-fetch-mode': 'Sec-Fetch-Mode',
  'sec-fetch-site': 'Sec-Fetch-Site',
  priority: 'Priority',
  te: 'TE',
  pragma: 'Pragma',
  'cache-control': 'Cache-Control',
  connection: 'Connection'
};

function normalizeShortId(rawValue) {
  const value = String(rawValue || '').trim();
  if (!value) {
    throw new Error('urlShortId is required');
  }

  if (!/^[0-9]+$/.test(value)) {
    throw new Error('urlShortId must be numeric');
  }

  return value;
}

function resolveReferer(urlShortId, customRefererPath = '', fallbackPath = '') {
  const raw = String(customRefererPath || '').trim();
  if (raw) {
    if (raw.startsWith('http://') || raw.startsWith('https://')) {
      try {
        const parsed = new URL(raw);
        if (parsed.hostname === 'www.zenius.net' || parsed.hostname === 'zenius.net') {
          return parsed.toString();
        }
      } catch {
        // Ignore and fall through to path-based referer.
      }
    }

    if (raw.startsWith('/')) {
      return `${ZENIUS_BASE_URL}${raw}`;
    }
  }

  if (fallbackPath && String(fallbackPath).startsWith('/')) {
    return `${ZENIUS_BASE_URL}${fallbackPath}`;
  }

  return `${ZENIUS_BASE_URL}/ci/${urlShortId}`;
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
    cgPathById: new Map([[rootCgId, []]]),
    errors: []
  };
}

function touchBatchChainSession(session) {
  const now = Date.now();
  session.updatedAt = now;
  session.expiresAt = now + clampPositiveInt(BATCH_SESSION_TTL_MS, 900000);
}

function cleanupExpiredBatchChainSessions() {
  const now = Date.now();
  for (const [sessionId, session] of batchChainSessions.entries()) {
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
    processedContainers: 0,
    queuedCount: 0,
    skippedCount: 0,
    queued: [],
    skipped: [],
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

function summarizeBackgroundBatchRun(run) {
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
    totalContainers: run.totalContainers,
    processedContainers: run.processedContainers,
    queuedCount: run.queuedCount,
    skippedCount: run.skippedCount,
    hasMoreContainers: run.hasMoreContainers,
    nextContainerOffset: run.nextContainerOffset,
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
  const value = String(rawValue || '').trim();
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

function stripWrappingQuotes(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';

  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith('\'') && trimmed.endsWith('\''))) {
    return trimmed.slice(1, -1).trim();
  }

  return trimmed;
}

function sanitizeHeaderValue(value) {
  const raw = String(value || '').replace(/[\r\n]+/g, ' ').trim();
  if (!raw) return '';

  let sanitized = '';
  for (const char of raw) {
    const code = char.charCodeAt(0);
    if (code >= 32 && code <= 255) {
      sanitized += char;
    }
  }

  return sanitized.trim();
}

function sanitizeCookiePair(pair) {
  const normalized = sanitizeHeaderValue(pair);
  if (!normalized) return null;

  const cleaned = normalized.replace(/^;+|;+$/g, '').trim();
  if (!cleaned) return null;

  const eqIndex = cleaned.indexOf('=');
  if (eqIndex <= 0) return null;

  const key = cleaned.slice(0, eqIndex).trim();
  const value = cleaned.slice(eqIndex + 1).trim();
  if (!key) return null;

  return `${key}=${value}`;
}

function parseCookieHeaderValue(cookieHeaderValue) {
  const normalized = sanitizeHeaderValue(stripWrappingQuotes(cookieHeaderValue));
  if (!normalized) return [];

  return normalized
    .split(';')
    .map((part) => sanitizeCookiePair(part))
    .filter(Boolean);
}

function parseHeaderLine(line) {
  const raw = String(line || '').trim();
  if (!raw) return null;

  const tabMatch = raw.match(/^([^\t]+)\t+(.+)$/);
  if (tabMatch) {
    return { key: tabMatch[1].trim(), value: tabMatch[2].trim() };
  }

  const colonIdx = raw.indexOf(':');
  if (colonIdx > 0) {
    const key = raw.slice(0, colonIdx).trim();
    const value = raw.slice(colonIdx + 1).trim();
    if (key && value) {
      return { key, value };
    }
  }

  const spaceMatch = raw.match(/^([A-Za-z0-9._-]+)\s+(.+)$/);
  if (spaceMatch) {
    return { key: spaceMatch[1].trim(), value: spaceMatch[2].trim() };
  }

  return null;
}

function parseHeadersRaw(rawHeaders) {
  if (!rawHeaders) {
    return { headers: {}, cookiePairs: [] };
  }

  const lines = String(rawHeaders)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const headers = {};
  const cookiePairs = [];

  for (const line of lines) {
    const parsed = parseHeaderLine(line);
    if (!parsed) continue;

    const key = parsed.key;
    const value = sanitizeHeaderValue(stripWrappingQuotes(parsed.value));
    if (!value) continue;

    const keyLower = key.toLowerCase();

    if (keyLower === 'cookie') {
      cookiePairs.push(...parseCookieHeaderValue(value));
      continue;
    }

    if (KNOWN_REQUEST_HEADERS.has(keyLower)) {
      headers[keyLower] = value;
      continue;
    }

    cookiePairs.push(`${key}=${value}`);
  }

  return { headers, cookiePairs };
}

function mergeCookiePairs(...cookieSources) {
  const merged = [];
  const latestByKey = new Map();

  for (const source of cookieSources) {
    const pairs = Array.isArray(source)
      ? source
      : parseCookieHeaderValue(source);

    for (const pair of pairs) {
      const sanitized = sanitizeCookiePair(pair);
      if (!sanitized) continue;

      const eqIndex = sanitized.indexOf('=');
      const key = sanitized.slice(0, eqIndex).trim();
      if (!key) continue;

      latestByKey.set(key, sanitized);
    }
  }

  for (const pair of latestByKey.values()) {
    merged.push(pair);
  }

  return merged;
}

function buildRequestContext(payload = {}) {
  const parsedRaw = parseHeadersRaw(payload.headersRaw || payload.rawHeaders || '');

  const userAgent = String(
    payload.userAgent
    || parsedRaw.headers['user-agent']
    || DEFAULT_USER_AGENT
  );

  const sentryTrace = payload.sentryTrace || parsedRaw.headers['sentry-trace'] || '';
  const baggage = payload.baggage || parsedRaw.headers.baggage || '';

  const cookiePairs = mergeCookiePairs(
    parsedRaw.cookiePairs,
    parsedRaw.headers.cookie
  );

  return {
    parsedHeaders: parsedRaw.headers,
    userAgent,
    sentryTrace,
    baggage,
    cookieHeader: cookiePairs.length > 0 ? cookiePairs.join('; ') : ''
  };
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

function buildUpstreamHeaders({ requestContext, referer }) {
  const headers = {
    Host: 'www.zenius.net',
    'User-Agent': requestContext.userAgent,
    Accept: '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    Referer: referer,
    Connection: 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    Pragma: 'no-cache',
    'Cache-Control': 'no-cache'
  };

  for (const [key, outputHeaderName] of Object.entries(PASSTHROUGH_HEADER_MAP)) {
    if (requestContext.parsedHeaders[key]) {
      headers[outputHeaderName] = requestContext.parsedHeaders[key];
    }
  }

  if (requestContext.cookieHeader) headers.Cookie = requestContext.cookieHeader;
  if (requestContext.sentryTrace) headers['sentry-trace'] = String(requestContext.sentryTrace);
  if (requestContext.baggage) headers.baggage = String(requestContext.baggage);

  return headers;
}

async function getJson(endpoint, {
  requestContext,
  urlShortId,
  refererPath,
  fallbackRefererPath = '',
  timeoutMs,
  maxRetries,
  deadlineAt
}) {
  const referer = resolveReferer(
    urlShortId,
    refererPath || requestContext.parsedHeaders.referer,
    fallbackRefererPath
  );
  const headers = buildUpstreamHeaders({ requestContext, referer });

  const requestTimeoutMs = clampPositiveInt(timeoutMs, clampPositiveInt(UPSTREAM_TIMEOUT_MS, 12000));
  const requestMaxRetries = clampPositiveInt(maxRetries, clampPositiveInt(UPSTREAM_MAX_RETRIES, 3));
  const retryBaseDelay = clampPositiveInt(UPSTREAM_RETRY_BASE_DELAY_MS, 500);

  for (let attempt = 0; attempt <= requestMaxRetries; attempt += 1) {
    try {
      const remainingMs = deadlineAt ? deadlineAt - Date.now() : null;
      if (remainingMs !== null && remainingMs <= clampPositiveInt(BATCH_DEADLINE_GUARD_MS, 1200)) {
        throw new Error(`Batch request time budget reached before fetching ${endpoint}`);
      }

      const effectiveTimeoutMs = remainingMs !== null
        ? Math.max(1000, Math.min(requestTimeoutMs, remainingMs - 500))
        : requestTimeoutMs;

      const response = await axios.get(endpoint, {
        headers,
        timeout: effectiveTimeoutMs,
        validateStatus: () => true
      });

      if (response.status >= 200 && response.status < 300) {
        const payload = response.data;
        const isObjectPayload = payload && typeof payload === 'object' && !Array.isArray(payload);
        const payloadOk = isObjectPayload && payload.ok !== false;

        if (payloadOk) {
          return payload;
        }

        const payloadErrorText = isObjectPayload ? String(payload.error || '').toLowerCase() : '';
        const looksTransientPayload = payload === ''
          || payload === null
          || payload === undefined
          || typeof payload === 'string'
          || (isObjectPayload
            && payload.ok === false
            && (payloadErrorText.includes('503')
              || payloadErrorText.includes('tempor')
              || payloadErrorText.includes('service unavailable')));

        if (looksTransientPayload && attempt < requestMaxRetries && !shouldStopForDeadline(deadlineAt, 1500)) {
          const delayMs = retryBaseDelay * (2 ** attempt) + Math.floor(Math.random() * 250);
          await sleep(delayMs);
          continue;
        }

        throw new Error(`API returned non-ok payload for ${endpoint}: ${JSON.stringify(payload)}`);
      }

      const isRetryable = RETRYABLE_UPSTREAM_STATUS.has(response.status);
      if (isRetryable && attempt < requestMaxRetries) {
        const delayMs = retryBaseDelay * (2 ** attempt) + Math.floor(Math.random() * 250);
        await sleep(delayMs);
        continue;
      }

      throw new Error(`Request failed (${response.status}) for ${endpoint}. Body: ${bodyPreview(response.data)}`);
    } catch (error) {
      const status = error?.response?.status;
      const isRetryableStatus = typeof status === 'number' && RETRYABLE_UPSTREAM_STATUS.has(status);
      const isRetryableNetwork = !status && (
        error?.code === 'ECONNABORTED'
        || error?.code === 'ETIMEDOUT'
        || error?.code === 'ECONNRESET'
        || error?.code === 'EAI_AGAIN'
      );

      if ((isRetryableStatus || isRetryableNetwork) && attempt < requestMaxRetries && !shouldStopForDeadline(deadlineAt, 1500)) {
        const delayMs = retryBaseDelay * (2 ** attempt) + Math.floor(Math.random() * 250);
        await sleep(delayMs);
        continue;
      }

      throw error;
    }
  }

  throw new Error(`Request failed after retries for ${endpoint}`);
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
      urlShortId: String(item?.['url-short-id'] || '').trim(),
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

  let videoDetails = [];
  try {
    videoDetails = await getVideoInstanceDetails(container['url-short-id'], options);
  } catch (error) {
    pushBatchError(session.errors, {
      stage: 'container-details',
      urlShortId: container['url-short-id'],
      message: error.message
    });
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
  let cursor = normalizedOffset;
  while (
    cursor < totalContainers
    && details.length < normalizedLimit
    && !shouldStopForDeadline(deadlineAt, 1600)
  ) {
    const container = mergedContainers[cursor];
    const detail = await buildBatchContainerDetail({
      container,
      options,
      session,
      deadlineAt
    });
    details.push(detail);
    cursor += 1;
  }

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

async function getInstanceDetails({ urlShortId, refererPath, fallbackRefererPath, requestContext }) {
  const endpoint = `${ZENIUS_BASE_URL}/api/instance-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const referer = resolveReferer(
    urlShortId,
    refererPath || requestContext.parsedHeaders.referer,
    fallbackRefererPath
  );
  const headers = buildUpstreamHeaders({ requestContext, referer });

  const response = await axios.get(endpoint, {
    headers,
    timeout: 30000,
    validateStatus: () => true
  });

  return {
    endpoint,
    referer,
    status: response.status,
    headers: response.headers,
    body: response.data
  };
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

function queueDownload(task) {
  return downloadQueue.add(task);
}

async function queueBatchDownloadChunk({ chain, requestContext, refererPath, baseFolderInput, selectedProviders, cancelled = () => false }) {
  const queued = [];
  const skipped = [];

  for (const container of chain.containerDetails || []) {
    if (cancelled()) {
      break;
    }

    for (const instance of container.videoInstances || []) {
      if (cancelled()) {
        break;
      }

      let metadata = instance.metadata && typeof instance.metadata === 'object'
        ? { ...instance.metadata }
        : {};
      const rawUrlShortId = instance.urlShortId || metadata.urlShortId;

      if (!rawUrlShortId || !/^\d+$/.test(String(rawUrlShortId).trim())) {
        skipped.push({
          urlShortId: rawUrlShortId || null,
          reason: 'Invalid or missing urlShortId',
          path: instance.path || container.path || ''
        });
        continue;
      }

      const urlShortId = normalizeShortId(rawUrlShortId);
      let metadataRetryError = '';
      let videoUrl = String(metadata['video-url'] || '').trim();

      if (!videoUrl) {
        try {
          metadata = await fetchInstanceMetadata(urlShortId, {
            requestContext,
            refererPath,
            timeoutMs: clampPositiveInt(UPSTREAM_TIMEOUT_MS, 12000),
            maxRetries: clampPositiveInt(UPSTREAM_MAX_RETRIES, 3)
          });
          videoUrl = String(metadata['video-url'] || '').trim();
        } catch (error) {
          metadataRetryError = error.message;
        }
      }

      if (!videoUrl) {
        skipped.push({
          urlShortId,
          reason: metadataRetryError || instance.metadataError || 'Missing video-url in instance-details',
          path: instance.path || container.path || ''
        });
        continue;
      }

      const outputName = sanitizeOutputName(
        instance.outputName || instance.name || metadata.name || `zenius-${urlShortId}`,
        `zenius-${urlShortId}`
      );

      const chainPath = String(instance.path || container.path || '').trim();
      const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';
      const folderId = await resolveFolderId(finalFolderInput);

      const ffmpegHeaders = {
        'User-Agent': requestContext.userAgent,
        Referer: resolveReferer(
          urlShortId,
          refererPath,
          metadata['path-url'] || container.containerPathUrl || ''
        )
      };

      if (requestContext.cookieHeader) {
        ffmpegHeaders.Cookie = requestContext.cookieHeader;
      }

      queueDownload({
        urlShortId,
        videoUrl,
        folderId,
        outputName,
        ffmpegHeaders,
        selectedProviders
      }).then((result) => {
        if (result.cancelled) {
          console.log(`[Zenius] Batch download ${urlShortId} was cancelled`);
        } else {
          console.log(`[Zenius] Batch download ${urlShortId} completed successfully`);
        }
      }).catch((error) => {
        console.error(`[Zenius] Batch download pipeline failed for ${urlShortId}:`, error.message);
      });

      queued.push({
        urlShortId,
        name: metadata.name || instance.name || null,
        outputName: `${outputName}.mp4`,
        path: chainPath,
        folderInput: finalFolderInput,
        folderId,
        videoUrl
      });
    }
  }

  return { queued, skipped };
}

async function processBackgroundBatchRun(run, payload) {
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
      if (iteration > 120) {
        throw new Error('Background batch iteration limit reached');
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
        cancelled: () => run.status !== 'running'
      });

      run.queuedCount += chunkResult.queued.length;
      run.skippedCount += chunkResult.skipped.length;
      run.queued.push(...chunkResult.queued);
      run.skipped.push(...chunkResult.skipped);
      run.processedContainers = Number.isFinite(Number(chain.nextContainerOffset))
        ? Number(chain.nextContainerOffset)
        : Number(chain.totalContainers || run.processedContainers || 0);

      touchBackgroundBatchRun(run);

      hasMore = Boolean(chain.hasMoreContainers);
      nextOffset = Number.isFinite(Number(chain.nextContainerOffset)) ? Number(chain.nextContainerOffset) : 0;
    }

    if (run.status === 'running') {
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
    if (run.status === 'completed' || run.status === 'failed') {
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
        error: run.error,
        chainErrors: run.chainErrors,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt
      }).catch((e) => console.error('[Webhook] Notification error:', e.message));
    }
    if (!hasActiveBackgroundBatchRuns()) {
      stopBackgroundBatchKeepalive();
    }
  }
}

// ==================== CANCEL & RESET FUNCTIONS ====================

async function cancelAllZeniusDownloads() {
  const cancelled = {
    downloads: [],
    uploads: [],
    errors: []
  };
  
  // 1. Cancel queued downloads via DownloadQueue
  const queueCancelled = downloadQueue.cancelAll();
  cancelled.downloads.push(...queueCancelled);
  
  // 2. Cancel active FFmpeg processes from downloadQueue
  for (const taskInfo of downloadQueue.activeTasks) {
    try {
      const jobs = await db.getJobsByFile(taskInfo.id);
      for (const job of jobs) {
        if (job.type === 'process') {
          await videoProcessor.cancelJob(job.id);
          await db.updateJob(job.id, { status: 'cancelled', error: 'Cancelled by user (cancel all)' });
        }
      }
      cancelled.downloads.push({ id: taskInfo.id, urlShortId: taskInfo.urlShortId, fromActive: true });
    } catch (error) {
      cancelled.errors.push({ id: taskInfo.id, error: error.message });
    }
  }
  
  // 3. Cancel pending uploads
  try {
    const allFiles = await db.listFiles();
    const zeniusFiles = allFiles.filter(f => 
      f.originalUrl?.includes('zenius.net') || 
      f.name?.startsWith('zenius-') ||
      f.name?.toLowerCase().includes('zenius')
    );
    
    for (const file of zeniusFiles) {
      const jobs = await db.getJobsByFile(file.id);
      for (const job of jobs) {
        if (job.status === 'pending' || job.status === 'processing') {
          await uploaderService.cancelJob(job.id);
          cancelled.uploads.push({ fileId: file.id, jobId: job.id });
        }
      }
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
      const requestContext = buildRequestContext(req.body || {});
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
      const requestContext = buildRequestContext(req.body || {});

      const details = await getInstanceDetails({
        urlShortId,
        refererPath: req.body?.refererPath,
        requestContext
      });

      const isSuccessStatus = details.status >= 200 && details.status < 300;
      if (!isSuccessStatus || !details.body?.ok || !details.body?.value) {
        return res.status(400).json({
          success: false,
          error: `Failed to fetch instance details (HTTP ${details.status})`,
          data: {
            upstreamStatus: details.status,
            body: details.body
          }
        });
      }

      const instanceValue = details.body.value;
      const videoUrl = String(instanceValue['video-url'] || '').trim();
      if (!videoUrl) {
        return res.status(400).json({
          success: false,
          error: 'Instance does not contain video-url'
        });
      }

      const outputName = sanitizeOutputName(
        req.body?.filename || instanceValue.name || instanceValue['canonical-name'] || `zenius-${urlShortId}`,
        `zenius-${urlShortId}`
      );

      const ffmpegHeaders = {
        'User-Agent': requestContext.userAgent,
        Referer: resolveReferer(urlShortId, req.body?.refererPath, instanceValue['path-url'])
      };

      if (requestContext.cookieHeader) {
        ffmpegHeaders.Cookie = requestContext.cookieHeader;
      }

      // Queue download with concurrency control
      const queueStatus = getQueueStatus();
      const willQueue = !canStartNewDownload();
      
      queueDownload({
        urlShortId,
        videoUrl,
        folderId,
        outputName,
        ffmpegHeaders,
        selectedProviders
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
        message: willQueue ? 'Zenius download queued (waiting for slot)' : 'Zenius download started',
        data: {
          urlShortId,
          name: instanceValue.name || null,
          videoUrl,
          outputName: `${outputName}.mp4`,
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
      const requestContext = buildRequestContext(req.body || {});
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
      cleanupExpiredBatchChainSessions();
      cleanupExpiredBackgroundBatchRuns();

      const requestContext = buildRequestContext(req.body || {});
      const selectedProviders = await normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');
      const containerLimit = normalizeChunkLimit(req.body?.containerLimit)
        || clampPositiveInt(BACKGROUND_BATCH_CHUNK_SIZE, 6);
      const timeBudgetMs = normalizeBatchRequestBudgetMs(req.body?.timeBudgetMs);
      const keepaliveUrl = resolveBackgroundKeepaliveUrl(req);

      const sessionId = normalizeSessionId(req.body?.sessionId);
      if (!sessionId || !batchChainSessions.has(sessionId)) {
        return res.status(400).json({
          success: false,
          error: 'Preview chain required before batch download. Call /batch-chain first to build a preview session, then pass the sessionId here.'
        });
      }

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
        message: 'All Zenius downloads and uploads cancelled',
        data: {
          cancelledDownloads: result.downloads.length,
          cancelledUploads: result.uploads.length,
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
      console.log(`[Zenius] Max concurrent downloads set to ${actual}`);
      res.json({
        success: true,
        data: { maxConcurrent: actual, ...getZeniusStatusSnapshot() }
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
  }
};

module.exports = zeniusController;
