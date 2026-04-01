const axios = require('axios');
const { randomUUID } = require('crypto');
const path = require('path');
const pLimit = require('p-limit');

const config = require('../config');
const { db, videoProcessor, uploaderService } = require('../services/runtime');

const ZENIUS_BASE_URL = 'https://www.zenius.net';
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0';
const DEFAULT_BATCH_ROOT_CGROUP_ID = '34';
const DEFAULT_BATCH_PARENT_CONTAINER_NAME = 'Pengantar Keterampilan Matematika Dasar';
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
const batchChainSessions = new Map();
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

function sanitizePathSegment(value, fallback = 'unknown') {
  const normalized = String(value || '')
    .trim()
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || fallback;
}

function buildContainerPath(parentName, containerName) {
  const parent = sanitizePathSegment(parentName, 'unknown-parent');
  const container = sanitizePathSegment(containerName, 'unknown-container');
  return `${parent}/${container}/`;
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

function normalizeTargetCgId(rawSelector) {
  const raw = String(rawSelector || '').trim();
  if (!raw) return null;

  if (/^\d+$/.test(raw)) {
    return raw;
  }

  return extractCgId(raw);
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
      if (isRetryable && attempt < maxRetries) {
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

async function getVideoInstanceDetails(urlShortId, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/container-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint, {
    ...options,
    urlShortId,
    fallbackRefererPath: `/cg/${urlShortId}`
  });
  const instances = Array.isArray(payload?.value?.['content-instances'])
    ? payload.value['content-instances']
    : [];

  return instances
    .filter((item) => {
      const type = String(item?.type || '').trim().toLowerCase();
      return type === 'vidio' || type === 'video';
    })
    .map((item) => ({
      urlShortId: String(item?.['url-short-id'] || '').trim(),
      name: String(item?.name || item?.['canonical-name'] || '').trim() || null,
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

  const rootContainers = Array.isArray(rootPayload?.value?.['cgs-containers'])
    ? rootPayload.value['cgs-containers']
    : [];

  const matchedRootContainers = rootContainers.filter((item) => {
    const itemCgId = extractCgId(item?.['path-url']);
    if (!itemCgId) return false;
    if (!session.targetCgId) return true;
    return itemCgId === session.targetCgId;
  });

  if (matchedRootContainers.length === 0) {
    const availableCgIds = unique(
      rootContainers
        .map((item) => extractCgId(item?.['path-url']))
        .filter(Boolean)
    );

    if (session.targetCgId) {
      pushBatchError(session.errors, {
        stage: 'cgroup-root-match',
        urlShortId: session.targetCgId,
        message: `targetCgId=${session.targetCgId} not present under root ${session.rootCgId}, using target as direct leaf candidate`
      });
      addLeafCgId(session, session.targetCgId);
      session.traversal.push({ source: session.rootCgId, next: session.targetCgId, isParent: null });
      session.discoveryInitialized = true;
      return;
    }

    throw new Error(
      `No cgs-containers matching /cg/:ID for root id ${session.rootCgId}. `
      + `targetCgId=${session.targetCgId || 'auto'} available=[${availableCgIds.join(', ')}]`
    );
  }

  const startCgIds = unique(
    matchedRootContainers
      .map((item) => extractCgId(item?.['path-url']))
      .filter(Boolean)
  );

  for (const item of matchedRootContainers) {
    const next = extractCgId(item?.['path-url']);
    if (!next) continue;
    session.traversal.push({
      source: session.rootCgId,
      next,
      isParent: parseBoolean(item?.['is-parent'])
    });
  }

  for (const startCgId of startCgIds) {
    if (!session.visitedCgIds.has(startCgId)) {
      session.queueCgIds.push(startCgId);
    }
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
      const containers = Array.isArray(value['cgs-containers']) ? value['cgs-containers'] : [];
      const nextItems = unique(
        containers
          .map((item) => {
            const nextId = extractCgId(item?.['path-url']);
            if (nextId) {
              session.traversal.push({
                source: value['url-short-id'] || currentCgId,
                next: nextId,
                isParent: parseBoolean(item?.['is-parent'])
              });
            }
            return nextId;
          })
          .filter(Boolean)
      );

      if (nextItems.length === 0 || valueIsParent === false) {
        addLeafCgId(session, currentCgId);
        continue;
      }

      for (const nextId of nextItems) {
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

      for (const item of result.value.list.items) {
        const key = String(item?.['url-short-id'] || '').trim();
        if (!key || session.containerByShortId.has(key)) {
          continue;
        }

        session.containerByShortId.set(key, {
          ...item,
          sourceLeafCgId: leafCgId
        });
      }
    }

    session.leafCursor += leafBatch.length;
  }

  session.discoveryDone = session.queueCgIds.length === 0 && session.leafCursor >= session.leafCgIds.length;
}

async function buildBatchContainerDetail({ container, pathParentName, options, session, deadlineAt }) {
  const containerName = container.name || container['url-short-id'];
  const containerPath = buildContainerPath(pathParentName, containerName);

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
    if (shouldStopForDeadline(deadlineAt, 1500)) {
      break;
    }

    try {
      const metadata = await fetchInstanceMetadata(video.urlShortId, options);
      const outputName = sanitizeOutputName(
        metadata.name || video.name || `zenius-${video.urlShortId}`,
        `zenius-${video.urlShortId}`
      );

      instancesWithMetadata.push({
        ...video,
        path: containerPath,
        outputName,
        metadata
      });
    } catch (error) {
      pushBatchError(session.errors, {
        stage: 'instance-details',
        urlShortId: video.urlShortId,
        message: error.message
      });

      const outputName = sanitizeOutputName(
        video.name || `zenius-${video.urlShortId}`,
        `zenius-${video.urlShortId}`
      );

      instancesWithMetadata.push({
        ...video,
        path: containerPath,
        outputName,
        metadata: null,
        metadataError: error.message
      });
    }
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
  const normalizedParentContainerName = sanitizePathSegment(parentContainerName, 'unknown-parent');
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
  const pathParentName = session.parentContainerName;

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
      pathParentName,
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

      videoProcessor.processHls(videoUrl, {
        folderId,
        outputName,
        outputDir: config.uploadDir,
        headers: ffmpegHeaders
      }).then(async (result) => {
        await uploaderService.queueFileUpload(
          result.fileId,
          result.outputPath,
          folderId,
          selectedProviders
        );
      }).catch((error) => {
        console.error('[Zenius] Download pipeline failed:', error.message);
      });

      res.status(202).json({
        success: true,
        message: 'Zenius download queued',
        data: {
          urlShortId,
          name: instanceValue.name || null,
          videoUrl,
          outputName: `${outputName}.mp4`,
          cleanupLocalFile: true,
          folderInput: stripWrappingQuotes(requestedFolder),
          folderId,
          providers: selectedProviders
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
      const requestContext = buildRequestContext(req.body || {});
      const selectedProviders = await normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');

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

      const queued = [];
      const skipped = [];

      for (const container of chain.containerDetails || []) {
        for (const instance of container.videoInstances || []) {
          const metadata = instance.metadata || {};
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
          const videoUrl = String(metadata['video-url'] || '').trim();

          if (!videoUrl) {
            skipped.push({
              urlShortId,
              reason: 'Missing video-url in instance-details',
              path: instance.path || container.path || ''
            });
            continue;
          }

          const outputName = sanitizeOutputName(
            instance.outputName || metadata.name || instance.name || `zenius-${urlShortId}`,
            `zenius-${urlShortId}`
          );

          const chainPath = String(instance.path || container.path || '').trim();
          const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';
          const folderId = await resolveFolderId(finalFolderInput);

          const ffmpegHeaders = {
            'User-Agent': requestContext.userAgent,
            Referer: resolveReferer(
              urlShortId,
              req.body?.refererPath,
              metadata['path-url'] || container.containerPathUrl || ''
            )
          };

          if (requestContext.cookieHeader) {
            ffmpegHeaders.Cookie = requestContext.cookieHeader;
          }

          videoProcessor.processHls(videoUrl, {
            folderId,
            outputName,
            outputDir: config.uploadDir,
            headers: ffmpegHeaders
          }).then(async (result) => {
            await uploaderService.queueFileUpload(
              result.fileId,
              result.outputPath,
              folderId,
              selectedProviders
            );
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

      res.status(202).json({
        success: true,
        message: 'Zenius batch download queued',
        data: {
          sessionId: chain.sessionId,
          rootCgId: chain.rootCgId,
          targetCgSelector: chain.targetCgSelector,
          targetCgId: chain.targetCgId,
          leafCgId: chain.leafCgId,
          leafCgIds: chain.leafCgIds,
          traversal: chain.traversal,
          discoveredLeafCount: chain.discoveredLeafCount,
          discoveryQueueRemaining: chain.discoveryQueueRemaining,
          discoveryDone: chain.discoveryDone,
          totalContainers: chain.totalContainers,
          containerOffset: chain.containerOffset,
          containerLimit: chain.containerLimit,
          processedContainerCount: chain.processedContainerCount,
          hasMoreContainers: chain.hasMoreContainers,
          nextContainerOffset: chain.nextContainerOffset,
          parentContainerName: chain.parentContainerName,
          queuedCount: queued.length,
          skippedCount: skipped.length,
          queued,
          skipped,
          chainErrors: Array.isArray(chain.errors) ? chain.errors : [],
          providers: selectedProviders,
          cleanupLocalFile: true,
          baseFolderInput
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  }
};

module.exports = zeniusController;
