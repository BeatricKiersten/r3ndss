const axios = require('axios');
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
const BATCH_METADATA_FETCH_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BATCH_METADATA_FETCH_CONCURRENCY || '8', 10);
const MAX_BATCH_ERRORS = Number.parseInt(process.env.ZENIUS_MAX_BATCH_ERRORS || '100', 10);
const DEFAULT_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_CHUNK_SIZE || '8', 10);
const MAX_BATCH_CHAIN_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BATCH_CHAIN_MAX_CHUNK_SIZE || '20', 10);
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

function normalizeCgIdList(rawValue) {
  if (!Array.isArray(rawValue)) {
    return [];
  }

  return unique(
    rawValue
      .map((value) => String(value || '').trim())
      .filter((value) => /^\d+$/.test(value))
  );
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

function normalizeProviders(rawProviders) {
  if (!Array.isArray(rawProviders) || rawProviders.length === 0) {
    return null;
  }

  const unique = new Set();
  for (const provider of rawProviders) {
    if (config.supportedProviders.includes(provider)) {
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
  fallbackRefererPath = ''
}) {
  const referer = resolveReferer(
    urlShortId,
    refererPath || requestContext.parsedHeaders.referer,
    fallbackRefererPath
  );
  const headers = buildUpstreamHeaders({ requestContext, referer });

  const timeoutMs = clampPositiveInt(UPSTREAM_TIMEOUT_MS, 12000);
  const maxRetries = clampPositiveInt(UPSTREAM_MAX_RETRIES, 3);
  const retryBaseDelay = clampPositiveInt(UPSTREAM_RETRY_BASE_DELAY_MS, 500);

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const response = await axios.get(endpoint, {
        headers,
        timeout: timeoutMs,
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

        if (looksTransientPayload && attempt < maxRetries) {
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

      if ((isRetryableStatus || isRetryableNetwork) && attempt < maxRetries) {
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

async function resolveLeafCgIds({ rootCgId, targetCgSelector, options }) {
  const targetCgId = normalizeTargetCgId(targetCgSelector);
  const errors = [];
  let rootPayload;

  try {
    rootPayload = await getCgByShortId(rootCgId, options);
  } catch (error) {
    pushBatchError(errors, {
      stage: 'cgroup-root',
      urlShortId: rootCgId,
      message: error.message
    });

    if (targetCgId) {
      return {
        targetCgId,
        leafCgIds: [targetCgId],
        traversal: [{ source: rootCgId, next: targetCgId, isParent: null }],
        errors
      };
    }

    throw error;
  }

  const rootContainers = Array.isArray(rootPayload?.value?.['cgs-containers'])
    ? rootPayload.value['cgs-containers']
    : [];

  const matchedRootContainers = rootContainers.filter((item) => {
    const itemCgId = extractCgId(item?.['path-url']);
    if (!itemCgId) return false;
    if (!targetCgId) return true;
    return itemCgId === targetCgId;
  });

  if (matchedRootContainers.length === 0) {
    const availableCgIds = unique(
      rootContainers
        .map((item) => extractCgId(item?.['path-url']))
        .filter(Boolean)
    );

    if (targetCgId) {
      pushBatchError(errors, {
        stage: 'cgroup-root-match',
        urlShortId: targetCgId,
        message: `targetCgId=${targetCgId} not present under root ${rootCgId}, using target as direct leaf candidate`
      });

      return {
        targetCgId,
        leafCgIds: [targetCgId],
        traversal: [{ source: rootCgId, next: targetCgId, isParent: null }],
        errors
      };
    }

    throw new Error(
      `No cgs-containers matching /cg/:ID for root id ${rootCgId}. `
      + `targetCgId=${targetCgId || 'auto'} available=[${availableCgIds.join(', ')}]`
    );
  }

  const startCgIds = unique(
    matchedRootContainers
      .map((item) => extractCgId(item?.['path-url']))
      .filter(Boolean)
  );

  const traversal = matchedRootContainers.map((item) => ({
    source: rootCgId,
    next: extractCgId(item?.['path-url']),
    isParent: parseBoolean(item?.['is-parent'])
  })).filter((item) => item.next);

  const visited = new Set([rootCgId]);
  const queue = [...startCgIds];
  const leafCgIds = [];
  const cgFetchLimit = pLimit(clampPositiveInt(BATCH_CG_FETCH_CONCURRENCY, 4));

  while (queue.length > 0) {
    const batchIds = [];
    while (queue.length > 0 && batchIds.length < clampPositiveInt(BATCH_CG_FETCH_CONCURRENCY, 4)) {
      const currentCgId = queue.shift();
      if (!currentCgId || visited.has(currentCgId)) {
        continue;
      }
      visited.add(currentCgId);
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
        pushBatchError(errors, {
          stage: 'cgroup-traversal',
          urlShortId: currentCgId,
          message: result.reason?.message || String(result.reason)
        });
        leafCgIds.push(currentCgId);
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
              traversal.push({
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
        leafCgIds.push(currentCgId);
        continue;
      }

      for (const nextId of nextItems) {
        if (!visited.has(nextId)) {
          queue.push(nextId);
        }
      }
    }
  }

  if (leafCgIds.length === 0) {
    throw new Error(`No leaf cgroup found for root id ${rootCgId}`);
  }

  return {
    targetCgId,
    leafCgIds: unique(leafCgIds),
    traversal,
    errors
  };
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

async function buildBatchChain({
  rootCgId,
  targetCgSelector,
  parentContainerName,
  requestContext,
  refererPath,
  leafCgIds,
  containerOffset,
  containerLimit
}) {
  const normalizedRootCgId = normalizeCgId(rootCgId, DEFAULT_BATCH_ROOT_CGROUP_ID);
  const options = { requestContext, refererPath };
  const errors = [];
  const preResolvedLeafCgIds = normalizeCgIdList(leafCgIds);
  let leaf;

  if (preResolvedLeafCgIds.length > 0) {
    leaf = {
      targetCgId: normalizeTargetCgId(targetCgSelector),
      leafCgIds: preResolvedLeafCgIds,
      traversal: [],
      errors: []
    };
  } else {
    leaf = await resolveLeafCgIds({
      rootCgId: normalizedRootCgId,
      targetCgSelector,
      options
    });
  }

  if (Array.isArray(leaf.errors) && leaf.errors.length > 0) {
    for (const errorInfo of leaf.errors) {
      pushBatchError(errors, errorInfo);
    }
  }

  const containerFetchLimit = pLimit(clampPositiveInt(BATCH_CONTAINER_FETCH_CONCURRENCY, 4));
  const metadataFetchLimit = pLimit(clampPositiveInt(BATCH_METADATA_FETCH_CONCURRENCY, 8));

  const containerItems = [];
  const containerListResults = await Promise.allSettled(
    leaf.leafCgIds.map((leafCgId) => containerFetchLimit(async () => ({
      leafCgId,
      list: await getContainerListWithDetails(leafCgId, options)
    })))
  );

  containerListResults.forEach((result, index) => {
    const leafCgId = leaf.leafCgIds[index];
    if (result.status !== 'fulfilled') {
      pushBatchError(errors, {
        stage: 'container-list',
        urlShortId: leafCgId,
        message: result.reason?.message || String(result.reason)
      });
      return;
    }

    for (const item of result.value.list.items) {
      containerItems.push({
        ...item,
        sourceLeafCgId: leafCgId
      });
    }
  });

  const containerByShortId = new Map();
  for (const item of containerItems) {
    const key = String(item['url-short-id'] || '').trim();
    if (!key || containerByShortId.has(key)) continue;
    containerByShortId.set(key, item);
  }

  const mergedContainers = Array.from(containerByShortId.values());
  const pathParentName = sanitizePathSegment(parentContainerName, 'unknown-parent');
  const totalContainers = mergedContainers.length;
  const normalizedOffset = normalizeChunkOffset(containerOffset);
  const normalizedLimit = normalizeChunkLimit(containerLimit);
  const chunkEnd = normalizedLimit === null
    ? totalContainers
    : Math.min(totalContainers, normalizedOffset + normalizedLimit);
  const containersToProcess = mergedContainers.slice(normalizedOffset, chunkEnd);

  const details = await Promise.all(
    containersToProcess.map((container) => containerFetchLimit(async () => {
      const containerName = container.name || container['url-short-id'];
      const containerPath = buildContainerPath(pathParentName, containerName);

      let videoDetails = [];
      try {
        videoDetails = await getVideoInstanceDetails(container['url-short-id'], options);
      } catch (error) {
        pushBatchError(errors, {
          stage: 'container-details',
          urlShortId: container['url-short-id'],
          message: error.message
        });
      }

      const instancesWithMetadata = await Promise.all(
        videoDetails.map((video) => metadataFetchLimit(async () => {
          try {
            const metadata = await fetchInstanceMetadata(video.urlShortId, options);
            const outputName = sanitizeOutputName(
              metadata.name || video.name || `zenius-${video.urlShortId}`,
              `zenius-${video.urlShortId}`
            );

            return {
              ...video,
              path: containerPath,
              outputName,
              metadata
            };
          } catch (error) {
            pushBatchError(errors, {
              stage: 'instance-details',
              urlShortId: video.urlShortId,
              message: error.message
            });

            const outputName = sanitizeOutputName(
              video.name || `zenius-${video.urlShortId}`,
              `zenius-${video.urlShortId}`
            );

            return {
              ...video,
              path: containerPath,
              outputName,
              metadata: null,
              metadataError: error.message
            };
          }
        }))
      );

      return {
        containerUrlShortId: container['url-short-id'],
        containerName: container.name,
        containerType: container.type,
        containerPathUrl: container['path-url'],
        sourceLeafCgId: container.sourceLeafCgId || null,
        path: containerPath,
        videoInstances: instancesWithMetadata
      };
    }))
  );

  return {
    rootCgId: normalizedRootCgId,
    targetCgSelector: targetCgSelector || null,
    targetCgId: leaf.targetCgId,
    parentContainerName: pathParentName,
    leafCgId: leaf.leafCgIds[0] || null,
    leafCgIds: leaf.leafCgIds,
    traversal: leaf.traversal,
    totalContainers,
    containerOffset: normalizedOffset,
    containerLimit: normalizedLimit,
    processedContainerCount: details.length,
    hasMoreContainers: chunkEnd < totalContainers,
    nextContainerOffset: chunkEnd < totalContainers ? chunkEnd : null,
    containerList: {
      urlShortId: leaf.leafCgIds[0] || null,
      urlShortIds: leaf.leafCgIds,
      totalContainers,
      items: normalizedLimit === null ? mergedContainers : containersToProcess
    },
    containerDetails: details,
    errors
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
      const selectedProviders = normalizeProviders(req.body?.providers);
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
        leafCgIds: req.body?.leafCgIds,
        containerOffset: req.body?.containerOffset,
        containerLimit: req.body?.containerLimit
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
      const selectedProviders = normalizeProviders(req.body?.providers);
      const baseFolderInput = stripWrappingQuotes(req.body?.folderId || '');

      const chain = await buildBatchChain({
        rootCgId: req.body?.rootCgId,
        targetCgSelector: req.body?.targetCgSelector,
        parentContainerName: req.body?.parentContainerName || DEFAULT_BATCH_PARENT_CONTAINER_NAME,
        requestContext,
        refererPath: req.body?.refererPath,
        leafCgIds: req.body?.leafCgIds,
        containerOffset: req.body?.containerOffset,
        containerLimit: req.body?.containerLimit
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
          rootCgId: chain.rootCgId,
          targetCgSelector: chain.targetCgSelector,
          targetCgId: chain.targetCgId,
          leafCgId: chain.leafCgId,
          leafCgIds: chain.leafCgIds,
          traversal: chain.traversal,
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
