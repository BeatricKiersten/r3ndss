const axios = require('axios');

const BASE_URL = 'https://www.zenius.net';
const ROOT_CGROUP_ID = process.argv[2] || '34';
const TARGET_CG_SELECTOR = process.argv[3] || '';
const PARENT_CONTAINER_NAME = process.argv[4] || 'Pengantar Keterampilan Matematika Dasar';

const headers = {
  Host: 'www.zenius.net',
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0',
  Accept: '*/*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br, zstd',
  Referer: 'https://www.zenius.net/ci/79496/review-eksponen',
  'sentry-trace': '11e7fc28b31a44af8d43aff0aa68dd79-b6c27c60d753b172-0',
  baggage: 'sentry-environment=production,sentry-release=znet-fe%40a48d217a0372be8ca403c7cb4b6819425773cc98,sentry-public_key=26c5862b38d13f2148a0a45292d43ea5,sentry-trace_id=11e7fc28b31a44af8d43aff0aa68dd79,sentry-org_id=4510932896055296,sentry-transaction=%2F%3Aslug*%3F,sentry-sampled=false,sentry-sample_rand=0.7490837173266175,sentry-sample_rate=0.1',
  Connection: 'keep-alive',
  Cookie: process.env.ZENIUS_COOKIE || '',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  Priority: 'u=4',
  TE: 'trailers',
  Pragma: 'no-cache',
  'Cache-Control': 'no-cache'
};

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

async function getJson(endpoint) {
  const response = await axios.get(endpoint, {
    headers,
    timeout: 30000,
    validateStatus: () => true
  });

  console.log(`[DEBUG] ${response.status} ${endpoint}`);

  if (response.status < 200 || response.status >= 300) {
    const bodyPreview = typeof response.data === 'string'
      ? response.data.slice(0, 500)
      : JSON.stringify(response.data).slice(0, 500);
    throw new Error(`Request failed (${response.status}) for ${endpoint}. Body: ${bodyPreview}`);
  }

  if (!response.data || response.data.ok === false) {
    throw new Error(`API returned non-ok payload for ${endpoint}: ${JSON.stringify(response.data)}`);
  }

  return response.data;
}

async function getCgByShortId(id) {
  const endpoint = `${BASE_URL}/api/cgroup/get-cg-by-short-id?id=${encodeURIComponent(id)}`;
  return getJson(endpoint);
}

async function resolveLeafCgId() {
  const rootPayload = await getCgByShortId(ROOT_CGROUP_ID);
  const rootContainers = Array.isArray(rootPayload?.value?.['cgs-containers'])
    ? rootPayload.value['cgs-containers']
    : [];
  const targetCgId = normalizeTargetCgId(TARGET_CG_SELECTOR);

  const firstMatch = rootContainers.find((item) => {
    const itemCgId = extractCgId(item?.['path-url']);
    if (!itemCgId) return false;
    if (!targetCgId) return true;
    return itemCgId === targetCgId;
  });

  if (!firstMatch) {
    const availableCgIds = unique(
      rootContainers
        .map((item) => extractCgId(item?.['path-url']))
        .filter(Boolean)
    );
    throw new Error(
      `No cgs-containers matching /cg/:ID for root id ${ROOT_CGROUP_ID}. `
      + `targetCgId=${targetCgId || 'auto'} available=[${availableCgIds.join(', ')}]`
    );
  }

  const firstId = extractCgId(firstMatch['path-url']);
  if (!firstId) {
    throw new Error(`Unable to extract cg id from path-url: ${firstMatch['path-url']}`);
  }

  let currentCgId = firstId;
  let currentIsParent = parseBoolean(firstMatch['is-parent']);
  const visited = new Set([ROOT_CGROUP_ID]);
  const traversal = [{ source: ROOT_CGROUP_ID, next: firstId, isParent: currentIsParent }];

  while (currentIsParent !== false) {
    if (visited.has(currentCgId)) {
      throw new Error(`Detected cgroup loop at id ${currentCgId}`);
    }
    visited.add(currentCgId);

    const payload = await getCgByShortId(currentCgId);
    const value = payload?.value || {};
    const valueIsParent = parseBoolean(value['is-parent']);
    if (valueIsParent === false) {
      break;
    }

    const containers = Array.isArray(value['cgs-containers']) ? value['cgs-containers'] : [];
    const nextContainer = containers.find((item) => extractCgId(item?.['path-url'])) || null;

    if (!nextContainer) {
      if (valueIsParent === true) {
        throw new Error(`id ${currentCgId} is-parent=true but cgs-containers is empty`);
      }
      break;
    }

    const nextId = extractCgId(nextContainer['path-url']);
    if (!nextId) {
      throw new Error(`Unable to extract next cg id from path-url: ${nextContainer['path-url']}`);
    }

    currentCgId = nextId;
    currentIsParent = parseBoolean(nextContainer['is-parent']);
    traversal.push({ source: value['url-short-id'] || 'unknown', next: nextId, isParent: currentIsParent });
  }

  return {
    targetCgId,
    leafCgId: currentCgId,
    traversal
  };
}

async function getContainerListWithDetails(urlShortId) {
  const endpoint = `${BASE_URL}/api/container-list?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint);
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

async function getVideoInstanceDetails(urlShortId) {
  const endpoint = `${BASE_URL}/api/container-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint);
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

async function fetchInstanceMetadata(urlShortId) {
  const endpoint = `${BASE_URL}/api/instance-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint);
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

async function run() {
  if (!process.env.ZENIUS_COOKIE) {
    console.warn('[WARN] ZENIUS_COOKIE kosong. CloudFront/authorization bisa gagal.');
  }

  const leaf = await resolveLeafCgId();
  const containerList = await getContainerListWithDetails(leaf.leafCgId);
  const pathParentName = sanitizePathSegment(PARENT_CONTAINER_NAME, 'unknown-parent');

  const details = [];
  for (const container of containerList.items) {
    const containerName = container.name || container['url-short-id'];
    const containerPath = buildContainerPath(pathParentName, containerName);
    const videoDetails = await getVideoInstanceDetails(container['url-short-id']);
    
    const instancesWithMetadata = [];
    for (const video of videoDetails) {
      const metadata = await fetchInstanceMetadata(video.urlShortId);
      instancesWithMetadata.push({
        ...video,
        path: containerPath,
        metadata
      });
    }
    
    details.push({
      containerUrlShortId: container['url-short-id'],
      containerName: container.name,
      containerType: container.type,
      containerPathUrl: container['path-url'],
      path: containerPath,
      videoInstances: instancesWithMetadata
    });
  }

  const result = {
    rootCgId: ROOT_CGROUP_ID,
    targetCgSelector: TARGET_CG_SELECTOR || null,
    targetCgId: leaf.targetCgId,
    parentContainerName: pathParentName,
    leafCgId: leaf.leafCgId,
    traversal: leaf.traversal,
    containerList: {
      urlShortId: leaf.leafCgId,
      totalContainers: containerList.items.length,
      items: containerList.items
    },
    containerDetails: details
  };

  console.log(JSON.stringify(result, null, 2));
}

run().catch((error) => {
  console.error('[ERROR]', error.message);
  process.exitCode = 1;
});
