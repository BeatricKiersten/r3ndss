const pLimit = require('p-limit');
const fs = require('fs-extra');

const { getInstance } = require('../src/db/handler');
const { buildRequestContext } = require('../src/services/requestContext');
const { uploaderService } = require('../src/services/runtime');
const {
  ZENIUS_BASE_URL,
  clampPositiveInt,
  getJson
} = require('../src/services/zeniusUpstreamService');

const DEFAULT_ROOT_CGROUP_ID = '23';
const DEFAULT_TIMEOUT_MS = Number.parseInt(process.env.ZENIUS_BENCH_TIMEOUT_MS || '10000', 10);
const DEFAULT_MAX_RETRIES = Number.parseInt(process.env.ZENIUS_BENCH_MAX_RETRIES || '1', 10);
const DEFAULT_CONTAINER_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BENCH_CONTAINER_CONCURRENCY || '8', 10);
const DEFAULT_METADATA_CONCURRENCY = Number.parseInt(process.env.ZENIUS_BENCH_METADATA_CONCURRENCY || '12', 10);
const DEFAULT_FOLDER_PREFETCH_CHUNK_SIZE = Number.parseInt(process.env.ZENIUS_BENCH_FOLDER_PREFETCH_CHUNK_SIZE || '50', 10);
const DEFAULT_PROGRESS_LOG_INTERVAL_MS = Number.parseInt(process.env.ZENIUS_BENCH_PROGRESS_LOG_INTERVAL_MS || '1000', 10);

function db() {
  return getInstance();
}

function parseBoolean(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
  }
  return fallback;
}

function parseArgs(argv) {
  const positional = [];
  const flags = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || '');
    if (!token.startsWith('--')) {
      positional.push(token);
      continue;
    }

    const flag = token.slice(2);
    const eqIndex = flag.indexOf('=');
    if (eqIndex >= 0) {
      flags[flag.slice(0, eqIndex)] = flag.slice(eqIndex + 1);
      continue;
    }

    const nextToken = argv[index + 1];
    if (nextToken && !String(nextToken).startsWith('--')) {
      flags[flag] = nextToken;
      index += 1;
      continue;
    }

    flags[flag] = 'true';
  }

  return { positional, flags };
}

function extractCgId(pathUrl) {
  const match = String(pathUrl || '').match(/\/cg\/(\d+)(?:\/|$)/);
  return match ? match[1] : null;
}

function extractContainerShortIdFromPath(pathUrl) {
  const match = String(pathUrl || '').match(/\/cgc\/(\d+)(?:\/|$)/);
  return match ? match[1] : null;
}

function normalizeTargetCgId(rawSelector) {
  const raw = String(rawSelector || '').trim();
  if (!raw) return null;
  if (/^\d+$/.test(raw)) return raw;
  return extractCgId(raw) || extractContainerShortIdFromPath(raw);
}

function normalizeNumericShortId(value) {
  const normalized = String(value || '').trim();
  return /^\d+$/.test(normalized) ? normalized : null;
}

function resolveContainerShortId(item) {
  const byField = normalizeNumericShortId(item?.['url-short-id']);
  if (byField && !extractCgId(item?.['path-url'])) {
    return byField;
  }

  return extractContainerShortIdFromPath(item?.['path-url']);
}

let progressState = {
  total: 0,
  completed: 0,
  labelCounts: {},
  checkedInstances: 0,
  startedAt: 0,
  lastProgressLogAt: 0,
  progressLogIntervalMs: DEFAULT_PROGRESS_LOG_INTERVAL_MS,
  isProviderLabelRun: false,
  duplicateDetailLog: false
};

function formatDuration(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes > 0) {
    return `${minutes}m${String(remainingSeconds).padStart(2, '0')}s`;
  }
  return `${seconds}s`;
}

function incrementLabelCount(label) {
  const key = String(label || 'unknown');
  progressState.labelCounts[key] = Number(progressState.labelCounts[key] || 0) + 1;
}

function maybeLogProviderProgress(force = false) {
  if (!progressState.isProviderLabelRun) {
    return;
  }

  const now = Date.now();
  const intervalMs = Math.max(100, Number(progressState.progressLogIntervalMs) || DEFAULT_PROGRESS_LOG_INTERVAL_MS);
  const shouldLog = force || progressState.lastProgressLogAt === 0 || (now - progressState.lastProgressLogAt) >= intervalMs;
  if (!shouldLog) {
    return;
  }

  progressState.lastProgressLogAt = now;
  const elapsedMs = Math.max(1, now - progressState.startedAt);
  const checks = Number(progressState.checkedInstances || 0);
  const existingCount = Number(progressState.labelCounts.existing || 0);
  const newCount = Number(progressState.labelCounts.new || 0);
  const dupRate = checks > 0 ? ((existingCount / checks) * 100).toFixed(1) : '0.0';
  const throughput = (checks / (elapsedMs / 1000)).toFixed(2);

  console.log(`[PROGRESS-PROVIDERS] containers:${progressState.completed}/${progressState.total} checks:${checks} existing:${existingCount} new:${newCount} dupRate:${dupRate}% throughput:${throughput}/s elapsed:${formatDuration(elapsedMs)} labels:${formatLabelCounts(progressState.labelCounts)}`);
}

function recordDuplicateCheck(result, context = {}) {
  const label = String(result?.label || 'unknown');
  progressState.checkedInstances += 1;
  incrementLabelCount(label);

  if (progressState.duplicateDetailLog) {
    const sequence = progressState.checkedInstances;
    const containerId = String(context?.container?.['url-short-id'] || 'unknown');
    const containerName = String(context?.container?.name || '').trim().replace(/\s+/g, ' ').slice(0, 50) || '-';
    const pendingProviders = Array.isArray(result?.pendingProviders) && result.pendingProviders.length > 0
      ? result.pendingProviders.join(',')
      : '-';
    const activeProviders = Array.isArray(result?.availableProviders) && result.availableProviders.length > 0
      ? result.availableProviders.join(',')
      : '-';

    console.log(`[DUP-CHECK] #${sequence} container=${containerId} name="${containerName}" ci=${result?.urlShortId || 'unknown'} label=${label} folderId=${result?.folderId || '-'} file="${result?.outputName || '-'}" existingFileId=${result?.existingFileId || '-'} status=${result?.existingStatus || '-'} pendingProviders=${pendingProviders} activeProviders=${activeProviders}`);
  }

  maybeLogProviderProgress(false);
}

function logProgress() {
  progressState.completed += 1;
  maybeLogProviderProgress(false);
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

function buildContainerPath(parentSegments, containerName) {
  const parts = [...(parentSegments || []).map((part) => sanitizePathSegment(part)).filter(Boolean)];
  parts.push(sanitizePathSegment(containerName, 'unknown-container'));
  return parts.join('/');
}

function createOptions(requestContext, refererPath, overrides = {}) {
  return {
    requestContext,
    refererPath,
    timeoutMs: clampPositiveInt(overrides.timeoutMs, DEFAULT_TIMEOUT_MS),
    maxRetries: clampPositiveInt(overrides.maxRetries, DEFAULT_MAX_RETRIES),
    deadlineAt: null
  };
}

async function runDbQuery(sql, params = []) {
  const database = db();
  await database._ready();
  const [rows] = await database.pool.query(sql, params);
  return rows;
}

function firstNonEmptyString(...values) {
  for (const value of values) {
    const normalized = String(value || '').trim();
    if (normalized) return normalized;
  }
  return '';
}

async function loadHeadersFromDb(flags) {
  const explicitQuery = String(flags.dbQuery || process.env.ZENIUS_BENCH_DB_QUERY || '').trim();
  const explicitSessionId = String(flags.dbSessionId || process.env.ZENIUS_BENCH_DB_SESSION_ID || '').trim();

  if (explicitQuery) {
    const rows = await runDbQuery(explicitQuery);
    const row = rows[0] || null;
    if (!row) {
      throw new Error('DB query returned no rows for benchmark headers');
    }

    return {
      headersRaw: firstNonEmptyString(row.headers_raw, row.headersRaw, row.raw_headers, row.rawHeaders, row.curl, row.cookie),
      refererPath: firstNonEmptyString(row.referer_path, row.refererPath, row.path_url, row.pathUrl, row.referer),
      source: 'dbQuery'
    };
  }

  if (explicitSessionId) {
    const rows = await runDbQuery(
      'SELECT session_data FROM batch_sessions WHERE id = ? LIMIT 1',
      [explicitSessionId]
    );
    const sessionData = rows[0]?.session_data || {};
    const parsed = typeof sessionData === 'string' ? JSON.parse(sessionData) : sessionData;

    return {
      headersRaw: firstNonEmptyString(parsed?.headersRaw, parsed?.rawHeaders, parsed?.curl),
      refererPath: firstNonEmptyString(parsed?.refererPath, parsed?.pathUrl),
      source: 'batchSessionById'
    };
  }

  const rows = await runDbQuery(
    `SELECT session_data
       FROM batch_sessions
      WHERE session_data IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 20`
  );

  for (const row of rows) {
    const sessionData = row?.session_data || {};
    const parsed = typeof sessionData === 'string' ? JSON.parse(sessionData) : sessionData;
    const headersRaw = firstNonEmptyString(parsed?.headersRaw, parsed?.rawHeaders, parsed?.curl);
    const refererPath = firstNonEmptyString(parsed?.refererPath, parsed?.pathUrl);
    if (headersRaw) {
      return { headersRaw, refererPath, source: 'recentBatchSessions' };
    }
  }

  return { headersRaw: '', refererPath: '', source: 'database-empty' };
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
      'url-short-id': normalizeNumericShortId(item?.['url-short-id']),
      name: String(item?.name || item?.title || '').trim() || null,
      type: String(item?.type || '').trim(),
      'path-url': String(item?.['path-url'] || '').trim() || null
    })).filter((item) => item['url-short-id'])
  };
}

async function getVideoInstanceDetails(urlShortId, options) {
  const endpoint = `${ZENIUS_BASE_URL}/api/container-details?url-short-id=${encodeURIComponent(urlShortId)}`;
  const payload = await getJson(endpoint, {
    ...options,
    urlShortId,
    fallbackRefererPath: `/cgc/${urlShortId}`
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
      urlShortId: normalizeNumericShortId(item?.['url-short-id']),
      name: String(item?.name || item?.title || item?.['canonical-name'] || '').trim() || null,
      duration: item?.duration || item?.['duration-seconds'] || item?.['video-duration'] || null,
      type: String(item?.type || '').trim()
    }))
    .filter((item) => item.urlShortId);
}

async function getInstanceMetadata(urlShortId, options) {
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
    pathUrl: value['path-url'] || null,
    videoUrlPresent: Boolean(value['video-url'])
  };
}

async function discoverContainers({ rootCgId, targetCgSelector, parentContainerName, options }) {
  const normalizedParentName = sanitizePathSegment(parentContainerName, 'unknown-parent');
  const targetCgId = normalizeTargetCgId(targetCgSelector);
  const rootPayload = await getCgByShortId(rootCgId, options);
  const rootContainers = Array.isArray(rootPayload?.value?.['cgs-containers'])
    ? rootPayload.value['cgs-containers']
    : [];

  const firstMatch = rootContainers.find((item) => {
    const itemCgId = extractCgId(item?.['path-url']);
    if (!itemCgId) return false;
    return !targetCgId || itemCgId === targetCgId;
  });

  if (!firstMatch) {
    const availableCgIds = unique(rootContainers.map((item) => extractCgId(item?.['path-url'])).filter(Boolean));
    throw new Error(`No matching cgroup for root ${rootCgId}. target=${targetCgId || 'auto'} available=[${availableCgIds.join(', ')}]`);
  }

  const initialCgId = extractCgId(firstMatch['path-url']);
  if (!initialCgId) {
    throw new Error(`Unable to extract cg id from ${firstMatch['path-url']}`);
  }

  const session = {
    rootCgId,
    rootCgName: normalizedParentName,
    visitedCgIds: new Set([rootCgId]),
    queueCgIds: [initialCgId],
    cgPathById: new Map([[rootCgId, []], [initialCgId, [normalizedParentName]]]),
    leafCgIds: [],
    leafCgIdSet: new Set(),
    traversal: [],
    containerByShortId: new Map(),
    errors: []
  };

  const addLeafCgId = (cgId) => {
    const normalized = normalizeNumericShortId(cgId);
    if (!normalized || session.leafCgIdSet.has(normalized)) {
      return;
    }

    session.leafCgIdSet.add(normalized);
    session.leafCgIds.push(normalized);
  };

  while (session.queueCgIds.length > 0) {
    const currentCgId = session.queueCgIds.shift();
    if (!currentCgId || session.visitedCgIds.has(currentCgId)) {
      continue;
    }

    session.visitedCgIds.add(currentCgId);
    const payload = await getCgByShortId(currentCgId, options);
    const value = payload?.value || {};
    const valueIsParent = parseBoolean(value['is-parent'], false);
    const containers = Array.isArray(value['cgs-containers']) ? value['cgs-containers'] : [];
    const currentSegments = session.cgPathById.get(currentCgId) || [normalizedParentName];
    const directContainers = [];
    const nextCgIds = [];

    for (const item of containers) {
      const itemCgId = extractCgId(item?.['path-url']);
      const itemIsParent = parseBoolean(item?.['is-parent'], false);
      const containerShortId = resolveContainerShortId(item);
      const name = String(item?.name || item?.title || '').trim() || null;

      if (itemCgId) {
        nextCgIds.push(itemCgId);
        const nextSegments = [...currentSegments];
        if (name) nextSegments.push(name);
        if (!session.cgPathById.has(itemCgId)) {
          session.cgPathById.set(itemCgId, nextSegments);
        }
        continue;
      }

      if (itemIsParent === false && containerShortId) {
        const path = buildContainerPath(currentSegments, name || containerShortId);
        directContainers.push({
          'url-short-id': containerShortId,
          name,
          type: String(item?.type || '').trim(),
          'path-url': String(item?.['path-url'] || '').trim() || null,
          sourceLeafCgId: currentCgId,
          path,
          parentSegments: [...currentSegments]
        });
      }
    }

    for (const container of directContainers) {
      session.containerByShortId.set(container['url-short-id'], container);
    }

    if ((nextCgIds.length === 0 && directContainers.length === 0) || (valueIsParent === false && directContainers.length === 0)) {
      addLeafCgId(currentCgId);
    }

    for (const nextCgId of unique(nextCgIds)) {
      if (!session.visitedCgIds.has(nextCgId)) {
        session.queueCgIds.push(nextCgId);
      }
    }
  }

  for (const leafCgId of session.leafCgIds) {
    try {
      const list = await getContainerListWithDetails(leafCgId, options);
      const leafSegments = session.cgPathById.get(leafCgId) || [normalizedParentName];
      for (const item of list.items) {
        session.containerByShortId.set(item['url-short-id'], {
          ...item,
          sourceLeafCgId: leafCgId,
          path: buildContainerPath(leafSegments, item.name || item['url-short-id']),
          parentSegments: [...leafSegments]
        });
      }
    } catch (error) {
      session.errors.push({ stage: 'container-list', urlShortId: leafCgId, message: error.message });
    }
  }

  return {
    rootCgId,
    targetCgId,
    leafCgIds: session.leafCgIds,
    traversalCount: session.visitedCgIds.size,
    containerCount: session.containerByShortId.size,
    containers: Array.from(session.containerByShortId.values()),
    errors: session.errors
  };
}

async function resolveFolderId(folderInput) {
  const normalized = String(folderInput || '').trim();
  if (!normalized || normalized === 'root' || normalized === '/') {
    return 'root';
  }

  return db().ensureFolderPath(normalized).then((folder) => folder.id);
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

async function buildExistingFileLookup(containers, baseFolderInput, folderCache, chunkSize = DEFAULT_FOLDER_PREFETCH_CHUNK_SIZE) {
  const plannedByFolderInput = new Map();

  for (const container of containers) {
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
  const folderIdByInput = new Map();
  for (const folderInput of folderInputs) {
    const folderId = await resolveFolderIdWithCache(folderCache, folderInput);
    folderIdByInput.set(folderInput, folderId);
  }

  const existingByFolderId = new Map();
  for (const folderInputChunk of chunkArray(folderInputs, chunkSize)) {
    await Promise.all(folderInputChunk.map(async (folderInput) => {
      const folderId = folderIdByInput.get(folderInput);
      if (!folderId) return;

      const plannedNames = plannedByFolderInput.get(folderInput) || new Set();
      const files = await db().listFiles(folderId);
      const byName = new Map();
      for (const file of files || []) {
        const nameKey = String(file?.name || '').trim();
        if (!nameKey || !plannedNames.has(nameKey) || byName.has(nameKey)) {
          continue;
        }
        byName.set(nameKey, file);
      }
      existingByFolderId.set(folderId, byName);
    }));
  }

  return {
    folderIdByInput,
    existingByFolderId,
    plannedFolderCount: folderInputs.length
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

function sanitizeOutputName(value, fallback) {
  const normalized = sanitizePathSegment(String(value || '').replace(/\.mp4$/i, ''), fallback);
  return normalized || fallback;
}

async function getActiveProviderIds(selectedProviders = null) {
  if (Array.isArray(selectedProviders) && selectedProviders.length > 0) {
    return [...new Set(selectedProviders.map((item) => String(item || '').trim()).filter(Boolean))];
  }

  const providerCatalog = await uploaderService.getProviderCatalog({ includeDisabled: false });
  return providerCatalog.filter((item) => item.enabled !== false).map((item) => item.id);
}

async function buildProviderLabelForInstance({
  container,
  instance,
  baseFolderInput,
  selectedProviders,
  folderCache,
  includeProviderLabels,
  folderIdByInput,
  existingByFolderId
}) {
  const urlShortId = normalizeNumericShortId(instance.urlShortId);
  const outputBaseName = sanitizeOutputName(instance.outputName || instance.name || `zenius-${urlShortId}`, `zenius-${urlShortId}`);
  const outputFileName = `${outputBaseName}.mp4`;
  const chainPath = String(instance.path || container.path || '').trim();
  const finalFolderInput = joinFolderPaths(baseFolderInput, chainPath) || 'root';
  const folderId = folderIdByInput?.get(finalFolderInput)
    || await resolveFolderIdWithCache(folderCache, finalFolderInput);
  const existingFile = existingByFolderId?.get(folderId)?.get(outputFileName)
    || await db().findFileByNameInFolder(folderId, outputFileName);

  const result = {
    urlShortId,
    outputName: outputFileName,
    folderId,
    folderInput: finalFolderInput,
    existingFileId: existingFile?.id || null,
    existingStatus: existingFile?.status || null,
    label: existingFile ? 'existing' : 'new',
    providerSummary: null,
    pendingProviders: [],
    availableProviders: []
  };

  if (!existingFile || !includeProviderLabels) {
    recordDuplicateCheck(result, { container, instance });
    return result;
  }

  const pendingProviderInfo = await uploaderService.getPendingUploadProviders(existingFile.id, selectedProviders, {
    verifyRemote: false
  });
  const targetProviders = Array.isArray(pendingProviderInfo?.targetProviders)
    ? pendingProviderInfo.targetProviders
    : [];
  const pendingProviders = Array.isArray(pendingProviderInfo?.pendingProviders)
    ? pendingProviderInfo.pendingProviders
    : [];
  const completedProviders = targetProviders.filter((provider) => !pendingProviders.includes(provider));
  const hasLocalSource = Boolean(existingFile.localPath && await fs.pathExists(existingFile.localPath));

  result.pendingProviders = pendingProviders;
  result.availableProviders = targetProviders;
  result.providerSummary = `${completedProviders.length}/${targetProviders.length} active providers`;

  if (pendingProviders.length === 0) {
    result.label = 'complete-on-active-providers';
    recordDuplicateCheck(result, { container, instance });
    return result;
  }

  if (hasLocalSource) {
    result.label = `missing-active-providers:${pendingProviders.join(',')}`;
    recordDuplicateCheck(result, { container, instance });
    return result;
  }

  result.label = `missing-local-and-providers:${pendingProviders.join(',')}`;
  recordDuplicateCheck(result, { container, instance });
  return result;
}

async function enrichContainer(container, options, includeMetadata, metadataConcurrency, planning = {}) {
  const startedAt = Date.now();
  const instances = await getVideoInstanceDetails(container['url-short-id'], options);
  let enrichedInstances = instances;

  if (includeMetadata && instances.length > 0) {
    const metadataLimit = pLimit(metadataConcurrency);
    const metadataResults = await Promise.all(instances.map((item) => metadataLimit(async () => ({
      ...item,
      metadata: await getInstanceMetadata(item.urlShortId, options)
    }))));
    enrichedInstances = metadataResults;
  }

  if (planning.includeProviderLabels) {
    const providerLimit = pLimit(planning.providerCheckConcurrency || 8);
    enrichedInstances = await Promise.all(enrichedInstances.map((item) => providerLimit(async () => {
      const providerLabel = await buildProviderLabelForInstance({
        container,
        instance: item,
        baseFolderInput: planning.baseFolderInput,
        selectedProviders: planning.selectedProviders,
        folderCache: planning.folderCache,
        includeProviderLabels: planning.includeProviderLabels,
        folderIdByInput: planning.folderIdByInput,
        existingByFolderId: planning.existingByFolderId
      });

      return {
        ...item,
        benchmarkLabel: providerLabel.label,
        providerSummary: providerLabel.providerSummary,
        pendingProviders: providerLabel.pendingProviders,
        activeProviders: providerLabel.availableProviders,
        existingFileId: providerLabel.existingFileId,
        existingStatus: providerLabel.existingStatus,
        outputName: providerLabel.outputName,
        folderId: providerLabel.folderId,
        folderInput: providerLabel.folderInput
      };
    })));
  }

  const labels = summarizeInstanceLabels(enrichedInstances);

  logProgress();

  return {
    containerUrlShortId: container['url-short-id'],
    containerName: container.name,
    containerType: container.type,
    containerPathUrl: container['path-url'],
    sourceLeafCgId: container.sourceLeafCgId || null,
    path: container.path,
    videoInstances: enrichedInstances,
    labels,
    durationMs: Date.now() - startedAt
  };
}

function summarizeInstanceLabels(instances = []) {
  const counts = {};
  for (const instance of instances) {
    const key = String(instance?.benchmarkLabel || 'unlabeled');
    counts[key] = Number(counts[key] || 0) + 1;
  }
  return counts;
}

function formatLabelCounts(labelCounts = {}) {
  const entries = Object.entries(labelCounts)
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((left, right) => Number(right[1] || 0) - Number(left[1] || 0));

  if (entries.length === 0) {
    return 'none';
  }

  return entries.map(([label, count]) => `${label}:${count}`).join(', ');
}

async function runLimitedParallel(containers, options, includeMetadata, containerConcurrency, metadataConcurrency, planning) {
  const isProviderLabelRun = planning.includeProviderLabels;
  progressState.total = containers.length;
  progressState.completed = 0;
  progressState.labelCounts = {};
  progressState.checkedInstances = 0;
  progressState.startedAt = Date.now();
  progressState.lastProgressLogAt = 0;
  progressState.progressLogIntervalMs = clampPositiveInt(planning.progressLogIntervalMs, DEFAULT_PROGRESS_LOG_INTERVAL_MS);
  progressState.isProviderLabelRun = isProviderLabelRun;
  progressState.duplicateDetailLog = Boolean(isProviderLabelRun && planning.duplicateDetailLog);

  console.log(`[START${isProviderLabelRun ? '-PROVIDERS' : ''}] ${containers.length} containers (concurrency=${containerConcurrency}, providerLabels=${isProviderLabelRun})`);
  if (isProviderLabelRun) {
    console.log(`[DUP-CHECK] realtime=true detail=${progressState.duplicateDetailLog} intervalMs=${progressState.progressLogIntervalMs}`);
  }

  const limit = pLimit(containerConcurrency);
  const results = await Promise.all(containers.map((container) => limit(() => enrichContainer(
    container,
    options,
    includeMetadata,
    metadataConcurrency,
    planning
  ))));

  maybeLogProviderProgress(true);

  if (isProviderLabelRun) {
    const existingCount = Number(progressState.labelCounts.existing || 0);
    const newCount = Number(progressState.labelCounts.new || 0);
    const checks = Number(progressState.checkedInstances || 0);
    const dupRate = checks > 0 ? ((existingCount / checks) * 100).toFixed(1) : '0.0';
    console.log(`[DONE-PROVIDERS] containers:${progressState.completed}/${progressState.total} checks:${checks} existing:${existingCount} new:${newCount} dupRate:${dupRate}% labels:${formatLabelCounts(progressState.labelCounts)}`);
  } else {
    console.log(`[DONE] processed:${progressState.completed}/${progressState.total}`);
  }

  return results;
}

function summarizeResult(mode, startedAt, discovery, details) {
  const totalVideos = details.reduce((sum, container) => sum + (container.videoInstances?.length || 0), 0);
  const labelSummary = {};
  for (const detail of details) {
    for (const [label, count] of Object.entries(detail.labels || {})) {
      labelSummary[label] = Number(labelSummary[label] || 0) + Number(count || 0);
    }
  }
  const slowestContainers = [...details]
    .sort((left, right) => right.durationMs - left.durationMs)
    .slice(0, 10)
    .map((item) => ({
      containerUrlShortId: item.containerUrlShortId,
      containerName: item.containerName,
      durationMs: item.durationMs,
      videoCount: item.videoInstances?.length || 0,
      labels: item.labels,
      path: item.path
    }));

  return {
    mode,
    elapsedMs: Date.now() - startedAt,
    leafCgCount: discovery.leafCgIds.length,
    containerCount: details.length,
    totalVideos,
    labelSummary,
    traversalCount: discovery.traversalCount,
    discoveryErrors: discovery.errors,
    slowestContainers
  };
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const rootCgId = positional[0] || flags.rootCgId || DEFAULT_ROOT_CGROUP_ID;
  const targetCgSelector = positional[1] || flags.targetCgSelector || '';
  const parentContainerName = positional[2] || flags.parentContainerName || 'Pengantar Keterampilan Matematika Dasar';
  const mode = String(flags.mode || 'parallel').trim().toLowerCase();
  const includeMetadata = parseBoolean(flags.metadata || process.env.ZENIUS_BENCH_INCLUDE_METADATA, false);
  const containerLimit = clampPositiveInt(flags.containerLimit, 0);
  const timeoutMs = clampPositiveInt(flags.timeoutMs, DEFAULT_TIMEOUT_MS);
  const maxRetries = clampPositiveInt(flags.maxRetries, DEFAULT_MAX_RETRIES);
  const containerConcurrency = clampPositiveInt(flags.containerConcurrency, DEFAULT_CONTAINER_CONCURRENCY);
  const metadataConcurrency = clampPositiveInt(flags.metadataConcurrency, DEFAULT_METADATA_CONCURRENCY);
  const providerCheckConcurrency = clampPositiveInt(flags.providerCheckConcurrency || process.env.ZENIUS_BENCH_PROVIDER_CHECK_CONCURRENCY, 8);
  const includeProviderLabels = parseBoolean(flags.providerLabels ?? process.env.ZENIUS_BENCH_PROVIDER_LABELS, true);
  const duplicateDetailLog = parseBoolean(flags.duplicateDetailLog ?? process.env.ZENIUS_BENCH_DUPLICATE_DETAIL_LOG, true);
  const progressLogIntervalMs = clampPositiveInt(flags.progressLogIntervalMs || process.env.ZENIUS_BENCH_PROGRESS_LOG_INTERVAL_MS, DEFAULT_PROGRESS_LOG_INTERVAL_MS);
  const baseFolderInput = String(flags.baseFolderInput || process.env.ZENIUS_BENCH_BASE_FOLDER || '').trim();
  const folderPrefetchChunkSize = clampPositiveInt(flags.folderPrefetchChunkSize || process.env.ZENIUS_BENCH_FOLDER_PREFETCH_CHUNK_SIZE, DEFAULT_FOLDER_PREFETCH_CHUNK_SIZE);
  const selectedProviders = await getActiveProviderIds(
    String(flags.providers || '').trim()
      ? String(flags.providers).split(',').map((item) => item.trim()).filter(Boolean)
      : null
  );
  const folderCache = new Map();

  let dbHeaders = { headersRaw: '', refererPath: '', source: 'disabled' };
  const shouldLoadFromDb = parseBoolean(flags.fromDb || process.env.ZENIUS_BENCH_FROM_DB, false);
  if (shouldLoadFromDb) {
    dbHeaders = await loadHeadersFromDb(flags);
  }

  const rawHeaders = firstNonEmptyString(
    flags.headersRaw,
    process.env.ZENIUS_HEADERS_RAW,
    dbHeaders.headersRaw
  );
  const effectiveRefererPath = firstNonEmptyString(
    flags.refererPath,
    process.env.ZENIUS_REFERER_PATH,
    dbHeaders.refererPath
  );

  if (!process.env.ZENIUS_COOKIE && !rawHeaders) {
    const reasons = [];
    reasons.push('ZENIUS_COOKIE env kosong');
    reasons.push('headersRaw flag/env kosong');
    if (shouldLoadFromDb) {
      reasons.push(`lookup DB tidak menemukan auth (${dbHeaders.source || 'unknown'})`);
    }
    console.warn(`[WARN] Auth Zenius tidak ditemukan: ${reasons.join(', ')}.`);
  }

  const requestContext = buildRequestContext({
    headersRaw: rawHeaders,
    userAgent: flags.userAgent || process.env.ZENIUS_USER_AGENT || undefined,
    baggage: flags.baggage || process.env.ZENIUS_BAGGAGE || undefined,
    sentryTrace: flags.sentryTrace || process.env.ZENIUS_SENTRY_TRACE || undefined
  }, {
    headers: {
      cookie: process.env.ZENIUS_COOKIE || ''
    }
  });

  const options = createOptions(requestContext, effectiveRefererPath, { timeoutMs, maxRetries });

  console.log(`[DISCOVERY] Starting container discovery for rootCgId=${rootCgId}, parent="${parentContainerName}"`);
  const discoveryStartedAt = Date.now();
  const discovery = await discoverContainers({
    rootCgId,
    targetCgSelector,
    parentContainerName,
    options
  });
  const discoveryElapsedMs = Date.now() - discoveryStartedAt;
  console.log(`[DISCOVERY] Found ${discovery.containerCount} containers in ${discovery.leafCgIds.length} leaf CGs (${formatDuration(discoveryElapsedMs)})`);

  let containers = discovery.containers;
  if (containerLimit > 0) {
    containers = containers.slice(0, containerLimit);
    console.log(`[LIMIT] Container limit applied: ${containers.length} containers`);
  }

  let previewSeedContainers;
  
  if (!includeProviderLabels) {
    console.log(`[PREVIEW] Fetching container details for ${containers.length} containers...`);
    previewSeedContainers = await runLimitedParallel(containers, options, false, containerConcurrency, metadataConcurrency, {
      includeProviderLabels: false,
      baseFolderInput,
      selectedProviders,
      folderCache
    });
  } else {
    // When provider labels are enabled, we fetch container instances first without preview run
    const instanceLimit = pLimit(metadataConcurrency);
    previewSeedContainers = await Promise.all(containers.map(async container => {
      const instances = await getVideoInstanceDetails(container['url-short-id'], options);
      return { ...container, videoInstances: instances };
    }));
  }

  const existingFileLookup = includeProviderLabels
    ? await buildExistingFileLookup(previewSeedContainers, baseFolderInput, folderCache, folderPrefetchChunkSize)
    : { folderIdByInput: new Map(), existingByFolderId: new Map(), plannedFolderCount: 0 };

  console.log(JSON.stringify({
    phase: 'discovery',
    rootCgId,
    targetCgSelector: targetCgSelector || null,
    parentContainerName,
    discoveryElapsedMs,
    discoveredLeafCgIds: discovery.leafCgIds,
    discoveredContainerCount: discovery.containerCount,
    effectiveContainerCount: containers.length,
    includeMetadata,
    containerConcurrency,
    metadataConcurrency,
    mode,
    includeProviderLabels,
    duplicateDetailLog,
    progressLogIntervalMs,
    providerCheckConcurrency,
    folderPrefetchChunkSize,
    prefetchFolderCount: existingFileLookup.plannedFolderCount,
    baseFolderInput: baseFolderInput || null,
    selectedProviders,
    authSource: shouldLoadFromDb ? 'database' : 'env-or-flags',
    refererPath: effectiveRefererPath || null,
    cookiePresent: Boolean(requestContext.cookieHeader),
    rawHeadersPresent: Boolean(rawHeaders)
  }, null, 2));

  const planning = {
    includeProviderLabels,
    duplicateDetailLog,
    progressLogIntervalMs,
    providerCheckConcurrency,
    baseFolderInput,
    selectedProviders,
    folderCache,
    folderIdByInput: existingFileLookup.folderIdByInput,
    existingByFolderId: existingFileLookup.existingByFolderId
  };

  console.log(`[PLANNING] includeProviderLabels=${includeProviderLabels}, prefetch folders=${existingFileLookup.plannedFolderCount}`);

  const runners = {
    parallel: () => runLimitedParallel(containers, options, includeMetadata, containerConcurrency, metadataConcurrency, planning)
  };

  const modes = [mode];

  console.log(`[RUN] modes=${JSON.stringify(modes)}, includeMetadata=${includeMetadata}`);

  for (const currentMode of modes) {
    if (!runners[currentMode]) {
      throw new Error(`Unsupported mode: ${currentMode}`);
    }

    console.log(`[RUN] Starting mode=${currentMode}`);
    const startedAt = Date.now();
    const details = await runners[currentMode]();
    const summary = summarizeResult(currentMode, startedAt, discovery, details);
    console.log(JSON.stringify(summary, null, 2));
  }
}

main().catch((error) => {
  console.error('[benchmark-batch-preview] failed:', error.message);
  process.exitCode = 1;
});
