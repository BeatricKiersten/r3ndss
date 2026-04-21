const axios = require('axios');
const { PASSTHROUGH_HEADER_MAP } = require('./requestContext');

const ZENIUS_BASE_URL = 'https://www.zenius.net';
const RETRYABLE_UPSTREAM_STATUS = new Set([202, 429, 500, 502, 503, 504]);

const INSTANCE_DETAIL_RETRY_DELAY_MS = 2000;
const INSTANCE_DETAIL_MAX_RETRIES = 5;

function clampPositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function bodyPreview(data) {
  if (data === null || data === undefined) return '';
  if (typeof data === 'string') return data.slice(0, 400);
  try {
    return JSON.stringify(data).slice(0, 400);
  } catch {
    return String(data).slice(0, 400);
  }
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

function buildUpstreamHeaders({ requestContext, referer }) {
  let refererOrigin = 'https://www.zenius.net';
  try {
    refererOrigin = new URL(referer).origin;
  } catch {
    refererOrigin = 'https://www.zenius.net';
  }

  const headers = {
    Host: 'www.zenius.net',
    'User-Agent': requestContext.userAgent,
    Accept: '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    Referer: referer,
    Origin: refererOrigin,
    Connection: 'keep-alive',
    'X-Requested-With': 'XMLHttpRequest',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"Linux"',
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
  if (requestContext.requestId) headers['X-Request-Id'] = String(requestContext.requestId);

  return headers;
}

function looksLikeWafChallenge(response) {
  const contentType = String(response?.headers?.['content-type'] || '').toLowerCase();
  const isHtml = contentType.includes('text/html') || contentType.includes('application/xhtml+xml');
  if (!isHtml) {
    return false;
  }

  const body = String(response?.data || '').toLowerCase();
  return body.includes('human verification')
    || body.includes('awswafcookiedomainlist')
    || body.includes('gokuprops')
    || body.includes('waf')
    || body.includes('captcha');
}

function shouldStopForDeadline(deadlineAt, guardMs) {
  return Number.isFinite(deadlineAt) && (deadlineAt - Date.now()) <= guardMs;
}

async function getJson(endpoint, {
  requestContext,
  urlShortId,
  refererPath,
  fallbackRefererPath = '',
  timeoutMs = 12000,
  maxRetries = 3,
  deadlineAt = null,
  retryBaseDelayMs = 500,
  deadlineGuardMs = 1200
}) {
  const referer = resolveReferer(
    urlShortId,
    refererPath || requestContext.parsedHeaders.referer,
    fallbackRefererPath
  );
  const headers = buildUpstreamHeaders({ requestContext, referer });

  const requestTimeoutMs = clampPositiveInt(timeoutMs, 12000);
  const requestMaxRetries = clampPositiveInt(maxRetries, 3);
  const retryBaseDelay = clampPositiveInt(retryBaseDelayMs, 500);
  const guardMs = clampPositiveInt(deadlineGuardMs, 1200);

  for (let attempt = 0; attempt <= requestMaxRetries; attempt += 1) {
    try {
      const remainingMs = deadlineAt ? deadlineAt - Date.now() : null;
      if (remainingMs !== null && remainingMs <= guardMs) {
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

      const wafChallenge = looksLikeWafChallenge(response);

      if (wafChallenge && attempt < requestMaxRetries && !shouldStopForDeadline(deadlineAt, 1500)) {
        const delayMs = retryBaseDelay * (2 ** attempt) + Math.floor(Math.random() * 250);
        await sleep(delayMs);
        continue;
      }

      if (wafChallenge) {
        throw new Error(`Upstream blocked by Human Verification/WAF for ${endpoint}. Refresh headersRaw + cookie from an active browser session and retry.`);
      }

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
      const response = error?.response || null;
      const isRetryableStatus = typeof status === 'number' && RETRYABLE_UPSTREAM_STATUS.has(status);
      const isRetryableWaf = looksLikeWafChallenge(response);
      const isRetryableNetwork = !status && (
        error?.code === 'ECONNABORTED'
        || error?.code === 'ETIMEDOUT'
        || error?.code === 'ECONNRESET'
        || error?.code === 'EAI_AGAIN'
      );

      if ((isRetryableStatus || isRetryableNetwork || isRetryableWaf) && attempt < requestMaxRetries && !shouldStopForDeadline(deadlineAt, 1500)) {
        const delayMs = retryBaseDelay * (2 ** attempt) + Math.floor(Math.random() * 250);
        await sleep(delayMs);
        continue;
      }

      throw error;
    }
  }

  throw new Error(`Request failed after retries for ${endpoint}`);
}

async function getInstanceDetails({ urlShortId, refererPath, fallbackRefererPath, requestContext, timeoutMs = 30000, maxRetries = INSTANCE_DETAIL_MAX_RETRIES }) {
  const endpoint = `${ZENIUS_BASE_URL}/api/instance-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const referer = resolveReferer(
    urlShortId,
    refererPath || requestContext.parsedHeaders.referer,
    fallbackRefererPath
  );
  const headers = buildUpstreamHeaders({ requestContext, referer });

  const effectiveTimeout = clampPositiveInt(timeoutMs, 30000);
  const maxAttempts = clampPositiveInt(maxRetries, INSTANCE_DETAIL_MAX_RETRIES);

  for (let attempt = 0; attempt <= maxAttempts; attempt += 1) {
    const response = await axios.get(endpoint, {
      headers,
      timeout: effectiveTimeout,
      validateStatus: () => true
    });

    if (response.status >= 200 && response.status < 300) {
      const payload = response.data;
      const isObjectPayload = payload && typeof payload === 'object' && !Array.isArray(payload);

      if (isObjectPayload && payload.ok !== false && payload.value) {
        return {
          endpoint,
          referer,
          status: response.status,
          headers: response.headers,
          body: payload
        };
      }
    }

    if (response.status === 202) {
      if (attempt < maxAttempts) {
        const delayMs = INSTANCE_DETAIL_RETRY_DELAY_MS * (attempt + 1);
        console.log(`[ZeniusUpstream] Instance details returned 202 for ${urlShortId}, retrying in ${delayMs}ms (attempt ${attempt + 1}/${maxAttempts})`);
        await sleep(delayMs);
        continue;
      }

      console.warn(`[ZeniusUpstream] Instance details 202 after ${maxAttempts} retries for ${urlShortId}, giving up`);
    }

    throw new Error(`Instance details request failed (HTTP ${response.status}) for ${endpoint}. Body: ${bodyPreview(response.data)}`);
  }

  throw new Error(`Instance details request failed after ${maxAttempts} retries for ${endpoint}`);
}

module.exports = {
  ZENIUS_BASE_URL,
  RETRYABLE_UPSTREAM_STATUS,
  clampPositiveInt,
  sleep,
  bodyPreview,
  resolveReferer,
  buildUpstreamHeaders,
  shouldStopForDeadline,
  getJson,
  getInstanceDetails
};
