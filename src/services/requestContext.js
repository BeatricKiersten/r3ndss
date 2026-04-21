const DEFAULT_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0';

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
  'host',
  'origin',
  'x-requested-with',
  'sec-ch-ua',
  'sec-ch-ua-mobile',
  'sec-ch-ua-platform'
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
  connection: 'Connection',
  origin: 'Origin',
  'x-requested-with': 'X-Requested-With',
  'sec-ch-ua': 'Sec-CH-UA',
  'sec-ch-ua-mobile': 'Sec-CH-UA-Mobile',
  'sec-ch-ua-platform': 'Sec-CH-UA-Platform'
};

function stripWrappingQuotes(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';

  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
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

function decodeCurlQuotedValue(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';

  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
}

function extractCurlArguments(rawHeaders) {
  const raw = String(rawHeaders || '').trim();
  if (!raw || !/^curl\s/i.test(raw)) {
    return null;
  }

  const normalized = raw.replace(/\\\r?\n/g, ' ');
  const args = [];
  const pattern = /('(?:\\'|[^'])*'|"(?:\\"|[^"])*"|\S+)/g;
  let match;

  while ((match = pattern.exec(normalized)) !== null) {
    args.push(match[0]);
  }

  return args;
}

function parseCurlCommand(rawHeaders) {
  const args = extractCurlArguments(rawHeaders);
  if (!args || args.length === 0) {
    return null;
  }

  const lines = [];

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    const next = args[index + 1];

    if ((token === '-H' || token === '--header') && next) {
      const value = decodeCurlQuotedValue(next);
      if (value) lines.push(value);
      index += 1;
      continue;
    }

    if ((token === '-b' || token === '--cookie') && next) {
      const value = decodeCurlQuotedValue(next);
      if (value) lines.push(`Cookie: ${value}`);
      index += 1;
      continue;
    }

    if ((token === '-A' || token === '--user-agent') && next) {
      const value = decodeCurlQuotedValue(next);
      if (value) lines.push(`User-Agent: ${value}`);
      index += 1;
    }
  }

  return lines;
}

function parseHeadersRaw(rawHeaders) {
  if (!rawHeaders) {
    return { headers: {}, cookiePairs: [] };
  }

  const curlLines = parseCurlCommand(rawHeaders);
  const lines = (curlLines || String(rawHeaders)
    .split(/\r?\n/))
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

function buildRequestContext(payload = {}, req = null) {
  const parsedRaw = parseHeadersRaw(payload.headersRaw || payload.rawHeaders || '');

  const userAgent = String(
    payload.userAgent
    || req?.headers?.['user-agent']
    || parsedRaw.headers['user-agent']
    || DEFAULT_USER_AGENT
  );

  const sentryTrace = payload.sentryTrace || parsedRaw.headers['sentry-trace'] || '';
  const baggage = payload.baggage || parsedRaw.headers.baggage || '';

  const cookiePairs = mergeCookiePairs(
    parsedRaw.cookiePairs,
    parsedRaw.headers.cookie,
    req?.headers?.cookie
  );

  return {
    requestId: req?.id || payload.requestId || null,
    parsedHeaders: parsedRaw.headers,
    userAgent,
    sentryTrace,
    baggage,
    cookieHeader: cookiePairs.length > 0 ? cookiePairs.join('; ') : ''
  };
}

module.exports = {
  DEFAULT_USER_AGENT,
  PASSTHROUGH_HEADER_MAP,
  stripWrappingQuotes,
  sanitizeHeaderValue,
  sanitizeCookiePair,
  parseCookieHeaderValue,
  parseHeadersRaw,
  mergeCookiePairs,
  buildRequestContext
};
