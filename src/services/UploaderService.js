/**
 * Uploader Service - Parallel Upload Queue & Workers
 * 
 * Handles queue management and parallel uploads to 4 storage providers.
 * Implements stream uploading, automatic retries, and progress tracking.
 */

const { EventEmitter } = require('events');
const { spawn } = require('child_process');
const pLimit = require('p-limit');
const fs = require('fs-extra');
const path = require('path');
const axios = require('axios');

const config = require('../config');

// Provider adapters
const { RcloneAdapter } = require('../providers/rclone');
const { VoeSxAdapter } = require('../providers/voesx');
const { CatboxAdapter } = require('../providers/catbox');
const { SeekStreamingAdapter } = require('../providers/seekstreaming');
const {
  LEGACY_RCLONE_PROVIDER_ID,
  buildRcloneProfileProviderId
} = require('./providerRegistry');

const { maxConcurrentUploads, maxConcurrentProviders, retryAttempts, retryDelay, timeout, probeTimeout, downloadTimeout } = config.upload;
const MAX_CONCURRENT_UPLOADS = maxConcurrentUploads;
const MAX_CONCURRENT_PROVIDERS = maxConcurrentProviders;
const UPLOAD_RETRY_ATTEMPTS = retryAttempts;
const UPLOAD_RETRY_DELAY = retryDelay;
const UPLOAD_TIMEOUT = timeout;
const SOURCE_PROBE_TIMEOUT = probeTimeout;
const SOURCE_DOWNLOAD_TIMEOUT = downloadTimeout;
const UPLOAD_DIR = config.uploadDir;
const DEBUG_UPLOAD_CLEANUP = String(process.env.DEBUG_UPLOAD_CLEANUP || 'false').toLowerCase() === 'true';
const LOCAL_FILE_CLEANUP_DELAY_MS = Number(process.env.UPLOAD_LOCAL_CLEANUP_DELAY_MS || 300000);
const ALLOWED_TRANSFER_SOURCE_HOSTS = new Set([
  'catbox.moe',
  'files.catbox.moe',
  'litterbox.catbox.moe'
]);

class UploaderService extends EventEmitter {
  constructor(dbHandler) {
    super();
    this.db = dbHandler;
    this.isRunning = false;
    this.activeUploads = new Map(); // Track active upload streams
    this.processingJobIds = new Set();
    this.localCleanupTimers = new Map();
    this._currentMaxConcurrentUploads = MAX_CONCURRENT_UPLOADS;
    this._currentMaxConcurrentProviders = MAX_CONCURRENT_PROVIDERS;
    this.providerLimit = pLimit(MAX_CONCURRENT_PROVIDERS);

    this.staticAdapters = {
      voesx: new VoeSxAdapter(),
      catbox: new CatboxAdapter(),
      seekstreaming: new SeekStreamingAdapter()
    };

    this.adapters = { ...this.staticAdapters };
    this.providerCatalogById = new Map();
    this.providerNames = Object.keys(this.adapters);
    this.uploadLimits = {};
    for (const provider of this.providerNames) {
      this.uploadLimits[provider] = pLimit(MAX_CONCURRENT_UPLOADS);
    }
  }

  _ensureUploadLimit(provider) {
    if (!this.uploadLimits[provider]) {
      this.uploadLimits[provider] = pLimit(MAX_CONCURRENT_UPLOADS);
    }
    return this.uploadLimits[provider];
  }

  async _refreshProviderRuntime() {
    const providerCatalog = await this.db.getProviderCatalog();
    const nextAdapters = { ...this.staticAdapters };
    const nextCatalogById = new Map(providerCatalog.map((item) => [item.id, item]));

    for (const provider of providerCatalog) {
      if (provider.kind !== 'rclone') {
        continue;
      }

      nextAdapters[provider.id] = new RcloneAdapter({
        db: this.db,
        providerName: provider.id,
        profileId: provider.profileId || null
      });
    }

    this.adapters = nextAdapters;
    this.providerCatalogById = nextCatalogById;
    this.providerNames = Object.keys(nextAdapters);

    for (const provider of this.providerNames) {
      this._ensureUploadLimit(provider);
    }

    return providerCatalog;
  }

  async _resolveLegacyRcloneProvider() {
    const rclone = await this.db.getRcloneConfig();
    const candidates = [];

    if (rclone?.defaultProfileId) {
      candidates.push(buildRcloneProfileProviderId(rclone.defaultProfileId));
    }

    for (const profile of rclone?.syncProfiles || []) {
      if (profile?.id) {
        candidates.push(buildRcloneProfileProviderId(profile.id));
      }
    }

    for (const candidate of candidates) {
      if (this.adapters[candidate]) {
        return candidate;
      }
    }

    return null;
  }

  async _resolveProviderId(provider, options = {}) {
    const allowDisabled = options.allowDisabled !== false;

    await this._refreshProviderRuntime();

    const normalized = String(provider || '').trim();
    if (!normalized) {
      throw new Error('Provider is required');
    }

    let resolved = normalized;

    if (!this.adapters[resolved] && resolved === LEGACY_RCLONE_PROVIDER_ID) {
      const legacyMapped = await this._resolveLegacyRcloneProvider();
      if (legacyMapped) {
        resolved = legacyMapped;
      }
    }

    const adapter = this.adapters[resolved];
    if (!adapter) {
      throw new Error(`Provider '${normalized}' is not supported`);
    }

    const providerMeta = this.providerCatalogById.get(resolved) || null;
    if (!allowDisabled && providerMeta && providerMeta.enabled === false) {
      throw new Error(`Provider ${resolved} is disabled`);
    }

    return resolved;
  }

  /**
   * Start the uploader service
   */
  async start() {
    if (this.isRunning) return;

    await this._refreshProviderRuntime();
    
    this.isRunning = true;
    console.log('[Uploader] Service started');

    await this._recoverStaleJobs();
    
    this.processInterval = setInterval(() => {
      this._processQueue();
    }, 2000);

    this.staleJobCheckInterval = setInterval(() => {
      this._recoverStaleJobs();
    }, 60000);

    this._processQueue();
  }

  /**
   * Stop the uploader service
   */
  async stop() {
    this.isRunning = false;
    if (this.processInterval) {
      clearInterval(this.processInterval);
    }
    if (this.staleJobCheckInterval) {
      clearInterval(this.staleJobCheckInterval);
    }
    
    for (const [key, controller] of this.activeUploads.entries()) {
      try {
        controller.abort();
      } catch (e) {
        console.error(`[Uploader] Failed to abort upload ${key}:`, e);
      }
    }
    
    this.activeUploads.clear();

    if (this._jobHeartbeats) {
      for (const timer of this._jobHeartbeats.values()) {
        clearInterval(timer);
      }
      this._jobHeartbeats.clear();
    }

    for (const timer of this.localCleanupTimers.values()) {
      clearTimeout(timer);
    }
    this.localCleanupTimers.clear();

    this.processingJobIds.clear();
    console.log('[Uploader] Service stopped');
  }

  /**
   * Queue a file for upload to all providers
   */
  async queueFileUpload(fileId, filePath, folderId = 'root', selectedProviders = null) {
    console.log(`[Uploader] Queueing file ${fileId} for upload`);

    const file = await this.db.getFile(fileId);
    const effectiveFolderId = file.folderId || folderId || 'root';
    const effectiveFilePath = filePath || file.localPath || null;
    const effectiveFileName = this._normalizeUploadFileName(file.name, effectiveFilePath || file.name || '');

    const providerCatalog = await this._refreshProviderRuntime();
    const enabledProviders = providerCatalog
      .filter((item) => item.enabled !== false)
      .map((item) => item.id);
    
    // Use selected providers if provided, otherwise use all enabled providers
    let providers;
    if (selectedProviders && selectedProviders.length > 0) {
      const resolved = [];
      for (const provider of selectedProviders) {
        try {
          const providerId = await this._resolveProviderId(provider, { allowDisabled: false });
          if (!resolved.includes(providerId)) {
            resolved.push(providerId);
          }
        } catch (_) {
          // Ignore invalid providers in payload.
        }
      }
      providers = resolved;
    } else {
      providers = enabledProviders;
    }

    if (providers.length === 0) {
      throw new Error('No providers are enabled. Enable at least one provider first.');
    }

    const jobsByFile = await this.db.getJobsByFile(fileId);
    const activeUploadProviders = new Set();
    for (const job of jobsByFile) {
      if (job.type !== 'upload' || !['pending', 'processing'].includes(job.status)) {
        continue;
      }

      const rawProvider = String(job.metadata?.provider || '').trim();
      if (!rawProvider) {
        continue;
      }

      try {
        const resolvedProvider = await this._resolveProviderId(rawProvider, { allowDisabled: true });
        activeUploadProviders.add(resolvedProvider);
      } catch {
        activeUploadProviders.add(rawProvider);
      }
    }

    const providersToQueue = [];
    const skippedProviders = [];

    for (const provider of providers) {
      const providerStatus = file.providers?.[provider] || null;

      if (providerStatus?.status === 'completed') {
        skippedProviders.push({ provider, reason: 'already-uploaded' });
        continue;
      }

      if (activeUploadProviders.has(provider)) {
        skippedProviders.push({ provider, reason: 'already-queued' });
        continue;
      }

      providersToQueue.push(provider);
    }

    if (providersToQueue.length === 0) {
      return { fileId, jobs: [], enabledProviders: [], skippedProviders };
    }

    await this.db.updateFileProgress(fileId, 'upload', 0);

    const jobs = [];
    for (const provider of providersToQueue) {
      const job = await this.db.createJob({
        type: 'upload',
        fileId,
        maxAttempts: UPLOAD_RETRY_ATTEMPTS,
        metadata: {
          provider,
          filePath: effectiveFilePath,
          folderId: effectiveFolderId,
          fileName: effectiveFileName,
          removeWhenUnused: false
        }
      });

      jobs.push({ provider, jobId: job.id });
    }

    this.emit('upload:queued', { fileId, jobs, enabledProviders: providersToQueue });

    this._processQueue();

    return {
      fileId,
      jobs,
      enabledProviders: providersToQueue,
      skippedProviders
    };
  }

  async getPendingUploadProviders(fileId, selectedProviders = null) {
    const file = await this.db.getFile(fileId);
    const providerCatalog = await this._refreshProviderRuntime();
    const enabledProviders = providerCatalog
      .filter((item) => item.enabled !== false)
      .map((item) => item.id);

    let targetProviders;
    if (Array.isArray(selectedProviders) && selectedProviders.length > 0) {
      const resolved = [];
      for (const provider of selectedProviders) {
        try {
          const providerId = await this._resolveProviderId(provider, { allowDisabled: false });
          if (!resolved.includes(providerId)) {
            resolved.push(providerId);
          }
        } catch (_) {
          // Ignore invalid providers in payload.
        }
      }
      targetProviders = resolved;
    } else {
      targetProviders = enabledProviders;
    }

    const jobsByFile = await this.db.getJobsByFile(fileId);
    const activeUploadProviders = new Set();
    for (const job of jobsByFile) {
      if (job.type !== 'upload' || !['pending', 'processing'].includes(job.status)) {
        continue;
      }

      const rawProvider = String(job.metadata?.provider || '').trim();
      if (!rawProvider) {
        continue;
      }

      try {
        const resolvedProvider = await this._resolveProviderId(rawProvider, { allowDisabled: true });
        activeUploadProviders.add(resolvedProvider);
      } catch {
        activeUploadProviders.add(rawProvider);
      }
    }

    const pendingProviders = [];
    const skippedProviders = [];

    for (const provider of targetProviders) {
      const providerStatus = file.providers?.[provider] || null;

      if (providerStatus?.status === 'completed') {
        skippedProviders.push({ provider, reason: 'already-uploaded' });
        continue;
      }

      if (activeUploadProviders.has(provider)) {
        skippedProviders.push({ provider, reason: 'already-queued' });
        continue;
      }

      pendingProviders.push(provider);
    }

    return {
      file,
      providerCatalog,
      targetProviders,
      pendingProviders,
      skippedProviders,
      hasPendingProviders: pendingProviders.length > 0
    };
  }

  async clearFileProviderLink(fileId, provider, reason = 'Provider link removed by user') {
    let resolvedProvider = String(provider || '').trim();
    if (!resolvedProvider) {
      throw new Error('Provider is required');
    }

    try {
      resolvedProvider = await this._resolveProviderId(resolvedProvider, { allowDisabled: true });
    } catch (_) {
      const file = await this.db.getFile(fileId);
      if (!file.providers?.[resolvedProvider]) {
        throw new Error(`Provider '${resolvedProvider}' not found for file ${fileId}`);
      }
    }

    return this.db.clearFileProviderLink(fileId, resolvedProvider, reason);
  }

  async clearMissingProviderLinks(provider, options = {}) {
    const resolvedProvider = await this._resolveProviderId(provider, { allowDisabled: true });
    const reason = String(options.reason || '').trim() || `Provider link removed after integrity check for ${resolvedProvider}`;

    const files = await this.db.listFiles();
    const results = {
      provider: resolvedProvider,
      checkedAt: new Date().toISOString(),
      totalFiles: files.length,
      checked: 0,
      cleared: [],
      skipped: [],
      failed: []
    };

    for (const file of files) {
      const statusResult = await this.checkFileProviderStatus(file.id, resolvedProvider);
      const status = statusResult[resolvedProvider];
      results.checked += 1;

      if (status.status !== 'completed') {
        continue;
      }

      if (status.remoteExists) {
        results.skipped.push({
          fileId: file.id,
          fileName: file.name,
          provider: resolvedProvider,
          reason: 'remote-still-exists'
        });
        continue;
      }

      try {
        await this.db.clearFileProviderLink(file.id, resolvedProvider, reason);
        results.cleared.push({
          fileId: file.id,
          fileName: file.name,
          provider: resolvedProvider
        });
      } catch (error) {
        results.failed.push({
          fileId: file.id,
          fileName: file.name,
          provider: resolvedProvider,
          error: error.message
        });
      }
    }

    return results;
  }

  async _getPrimaryProvider() {
    await this._refreshProviderRuntime();

    const provider = await this.db.getPrimaryProvider();
    if (this.providerNames.includes(provider)) {
      return provider;
    }

    return this.providerNames.includes('catbox')
      ? 'catbox'
      : this.providerNames[0] || 'catbox';
  }

  _getProviderDownloadUrl(provider, status) {
    if (!status || status.status !== 'completed') {
      return null;
    }

    const iframeProviders = ['seekstreaming', 'voesx', 'streamtape', 'mixdrop'];

    if (provider === 'seekstreaming') {
      return this.adapters.seekstreaming.getDownloadUrl(status);
    }

    if (iframeProviders.includes(provider)) {
      return null;
    }

    const url = status.url || null;
    if (!url) return null;

    return url;
  }

  _getSeekstreamingDownloadUrls(status) {
    const adapter = this.adapters.seekstreaming;
    if (!adapter) {
      return [];
    }

    if (typeof adapter.getDownloadUrlCandidates === 'function') {
      return adapter.getDownloadUrlCandidates(status).filter(Boolean);
    }

    const singleUrl = adapter.getDownloadUrl(status);
    return singleUrl ? [singleUrl] : [];
  }

  _getSourceHeaders(sourceUrl) {
    if (sourceUrl.includes('emergingtechhubonline.store') || sourceUrl.includes('technologyevolution.space')) {
      return this.adapters.seekstreaming.getDownloadHeaders();
    }

    return {};
  }

  async _downloadHlsSourceWithFfmpeg(sourceUrl, destinationPath) {
    await fs.ensureDir(path.dirname(destinationPath));

    const headers = this._getSourceHeaders(sourceUrl);
    const userAgent = String(headers['User-Agent'] || '').trim();
    const referer = String(headers.Referer || '').trim();
    const origin = String(headers.Origin || '').trim();
    const cookie = String(headers.Cookie || '').trim();

    const args = [
      '-y',
      '-hide_banner',
      '-loglevel', 'warning',
      '-stats',
      '-fflags', '+discardcorrupt',
      '-reconnect', '1',
      '-reconnect_at_eof', '1',
      '-reconnect_streamed', '1',
      '-reconnect_delay_max', '5',
      '-thread_queue_size', '4096',
      '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
      '-allowed_extensions', 'ALL',
      '-allowed_segment_extensions', 'ALL',
      '-extension_picky', '0'
    ];

    if (userAgent) {
      args.push('-user_agent', userAgent);
    }

    if (referer) {
      args.push('-referer', referer);
    }

    const extraHeaders = {};
    if (origin) {
      extraHeaders.Origin = origin;
    }
    if (cookie) {
      extraHeaders.Cookie = cookie;
    }

    if (Object.keys(extraHeaders).length > 0) {
      const headerString = Object.entries(extraHeaders)
        .filter(([_, value]) => value !== undefined && value !== null && String(value).trim().length > 0)
        .map(([key, value]) => `${key}: ${value}`)
        .join('\r\n');

      if (headerString) {
        args.push('-headers', `${headerString}\r\n`);
      }
    }

    args.push(
      '-i', sourceUrl,
      '-c', 'copy',
      '-bsf:a', 'aac_adtstoasc',
      '-movflags', '+faststart',
      destinationPath
    );

    await new Promise((resolve, reject) => {
      const proc = spawn('ffmpeg', args, {
        stdio: ['ignore', 'ignore', 'pipe']
      });

      let stderr = '';
      proc.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      proc.on('close', (code) => {
        if (code === 0) {
          resolve();
          return;
        }

        reject(new Error(`ffmpeg failed with code ${code}${stderr ? `: ${stderr.trim()}` : ''}`));
      });

      proc.on('error', (error) => {
        reject(new Error(`Failed to start ffmpeg: ${error.message}`));
      });
    });

    const stat = await fs.stat(destinationPath);
    if (!stat.size) {
      throw new Error('Downloaded HLS file is empty');
    }

    return {
      size: stat.size,
      contentType: 'video/mp4'
    };
  }

  async _resolveRemoteSource(file, options = {}) {
    const { excludeProvider = null, preferPrimary = true, specificProvider = null } = options;
    await this._refreshProviderRuntime();
    const primaryProvider = await this._getPrimaryProvider();
    const candidates = [];

    if (specificProvider) {
      candidates.push(specificProvider);
    } else if (preferPrimary && primaryProvider !== excludeProvider) {
      candidates.push(primaryProvider);
    }

    const discoveredProviders = new Set([
      ...this.providerNames,
      ...Object.keys(file.providers || {})
    ]);

    for (const provider of discoveredProviders) {
      if (provider === excludeProvider || candidates.includes(provider)) {
        continue;
      }
      candidates.push(provider);
    }

    for (const provider of candidates) {
      const status = file.providers?.[provider];
      let sourceUrl = this._getProviderDownloadUrl(provider, status);
      let fallbackUrls = [];
      const adapter = this.adapters[provider] || null;
      const canDownloadByFileId = Boolean(adapter && typeof adapter.download === 'function' && status?.fileId);

      if (provider === 'seekstreaming') {
        const candidateUrls = this._getSeekstreamingDownloadUrls(status);
        if (candidateUrls.length > 0) {
          sourceUrl = candidateUrls[0];
          fallbackUrls = candidateUrls.slice(1);
        }
      }

      if (sourceUrl || canDownloadByFileId) {
        return {
          provider,
          sourceUrl,
          fallbackUrls,
          isPrimary: provider === primaryProvider
        };
      }
    }

    return null;
  }

  async _downloadRemoteSourceToTemp(file, options = {}) {
    const remoteSource = await this._resolveRemoteSource(file, options);

    if (!remoteSource) {
      throw new Error('No remote source available for download');
    }

    await fs.ensureDir(UPLOAD_DIR);
    const tempPath = path.join(UPLOAD_DIR, `reupload-${file.id}-${Date.now()}-${file.name}`);

    const sourceStatus = file.providers?.[remoteSource.provider] || {};
    const sourceAdapter = this.adapters[remoteSource.provider] || null;
    const canDownloadByFileId = Boolean(sourceAdapter && typeof sourceAdapter.download === 'function' && sourceStatus.fileId);

    if (canDownloadByFileId) {
      await sourceAdapter.download(sourceStatus.fileId, tempPath);
    } else if (remoteSource.sourceUrl) {
      await this._downloadSourceFile(remoteSource.sourceUrl, tempPath, {
        fallbackUrls: remoteSource.fallbackUrls
      });
    } else {
      throw new Error(`No downloadable source for provider '${remoteSource.provider}'`);
    }

    return {
      ...remoteSource,
      filePath: tempPath,
      needsCleanup: true
    };
  }

  async _removeManagedLocalFile(fileId, filePath) {
    if (!filePath) return;

    this._cancelScheduledCleanup(filePath);

    const resolvedUploadDir = path.resolve(UPLOAD_DIR);
    const resolvedFilePath = path.resolve(filePath);

    if (resolvedFilePath.startsWith(resolvedUploadDir)) {
      if (DEBUG_UPLOAD_CLEANUP) {
        console.log(`[Uploader] Cleanup removing local file for ${fileId}: ${resolvedFilePath}`);
      }
      await fs.remove(resolvedFilePath).catch(() => {});
    }

    const file = await this.db.getFile(fileId).catch(() => null);
    if (file?.localPath && path.resolve(file.localPath) === resolvedFilePath) {
      await this.db.updateFileLocalPath(fileId, null).catch(() => {});
    }
  }

  _safeResolvePath(filePath) {
    if (!filePath || typeof filePath !== 'string') return null;
    return path.resolve(filePath);
  }

  _cancelScheduledCleanup(filePath) {
    if (!filePath) return;

    const key = this._safeResolvePath(filePath);
    if (!key) return;

    const timer = this.localCleanupTimers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.localCleanupTimers.delete(key);
    }
  }

  async _scheduleManagedLocalFileCleanup(fileId, filePath) {
    if (!filePath) return;

    const key = this._safeResolvePath(filePath);
    if (!key) return;

    this._cancelScheduledCleanup(key);

    const delayMs = Number.isFinite(LOCAL_FILE_CLEANUP_DELAY_MS)
      ? Math.max(0, LOCAL_FILE_CLEANUP_DELAY_MS)
      : 300000;

    if (delayMs === 0) {
      await this._finalizeManagedLocalFileCleanup(fileId, key);
      return;
    }

    if (DEBUG_UPLOAD_CLEANUP) {
      console.log(`[Uploader] Cleanup scheduled in ${delayMs}ms for ${fileId}: ${key}`);
    }

    const timer = setTimeout(async () => {
      this.localCleanupTimers.delete(key);
      await this._finalizeManagedLocalFileCleanup(fileId, key);
    }, delayMs);

    this.localCleanupTimers.set(key, timer);
  }

  async _finalizeManagedLocalFileCleanup(fileId, filePath) {
    if (!filePath) return;

    const resolvedTargetPath = this._safeResolvePath(filePath);
    if (!resolvedTargetPath) return;

    const jobs = await this.db.getJobsByFile(fileId);
    const relatedJobs = jobs.filter((item) => this._safeResolvePath(item.metadata?.filePath) === resolvedTargetPath);

    if (relatedJobs.length === 0) {
      return;
    }

    const hasNonFinalJob = relatedJobs.some((item) => (
      item.status === 'pending' || item.status === 'processing' || item.status === 'uploading'
    ));

    if (hasNonFinalJob) {
      return;
    }

    await this._removeManagedLocalFile(fileId, resolvedTargetPath);
  }

  async _cleanupFilePathWhenUnused(fileId, filePath) {
    if (!filePath) return;

    const resolvedTargetPath = this._safeResolvePath(filePath);
    if (!resolvedTargetPath) return;

    this._cancelScheduledCleanup(resolvedTargetPath);

    const jobs = await this.db.getJobsByFile(fileId);
    const relatedJobs = jobs.filter((item) => this._safeResolvePath(item.metadata?.filePath) === resolvedTargetPath);

    if (relatedJobs.length === 0) {
      return;
    }

    if (DEBUG_UPLOAD_CLEANUP) {
      console.log(`[Uploader] Cleanup check for ${fileId}: ${relatedJobs.map((item) => `${item.id}:${item.status}`).join(', ')}`);
    }

    const hasNonFinalJob = relatedJobs.some((item) => (
      item.status === 'pending' || item.status === 'processing'
    ));

    // A job stuck in 'uploading' should NOT block cleanup indefinitely.
    // If the upload heartbeat is old (> 10 min), treat it as stale and allow cleanup.
    const UPLOADING_STALE_MS = 10 * 60 * 1000;
    const hasRunningUploadState = relatedJobs.some((item) => {
      if (item.status !== 'uploading') return false;
      const updatedAt = item.updatedAt ? new Date(item.updatedAt).getTime() : 0;
      const isStale = updatedAt && (Date.now() - updatedAt) > UPLOADING_STALE_MS;
      return !isStale; // Only block if the upload is recent (not stale)
    });

    const allJobsFinished = relatedJobs.every((item) => (
      item.status === 'completed' || item.status === 'failed' || item.status === 'cancelled'
    ));

    if (!hasNonFinalJob && !hasRunningUploadState && allJobsFinished) {
      await this._scheduleManagedLocalFileCleanup(fileId, filePath);
    }
  }

  /**
   * Re-upload to a specific provider
   */
  async reuploadToProvider(fileId, provider, source) {
    console.log(`[Uploader] Re-uploading file ${fileId} to ${provider} from source ${source}`);

    const targetProviderId = await this._resolveProviderId(provider, { allowDisabled: false });
    const file = await this.db.getFile(fileId);

    if (!source) {
      throw new Error('Source is required for reupload');
    }

    let sourceProviderId = String(source).trim();
    if (!file.providers?.[sourceProviderId]) {
      try {
        sourceProviderId = await this._resolveProviderId(sourceProviderId, { allowDisabled: true });
      } catch (_) {
        // Keep original source identifier for compatibility lookup.
      }
    }

    if (sourceProviderId === targetProviderId) {
      throw new Error('Source cannot be the same as target provider');
    }

    const sourceStatus = file.providers?.[sourceProviderId];
    if (!sourceStatus || sourceStatus.status !== 'completed') {
      throw new Error(`Source provider '${sourceProviderId}' is not available or not completed`);
    }

    const hasLocalSource = Boolean(file.localPath && await fs.pathExists(file.localPath));
    const sourcePath = hasLocalSource ? file.localPath : null;
    const cleanupAfterUpload = false;

    // Reset provider status
    await this.db.updateProviderStatus(fileId, targetProviderId, 'pending', null, null, null, { embedUrl: null });

    // Create new job
    const job = await this.db.createJob({
      type: 'upload',
      fileId,
      maxAttempts: UPLOAD_RETRY_ATTEMPTS,
      metadata: {
        provider: targetProviderId,
        filePath: sourcePath,
        folderId: file.folderId,
        fileName: file.name,
        sourceProvider: sourceProviderId,
        cleanupAfterUpload,
        removeWhenUnused: cleanupAfterUpload
      }
    });

    this.emit('upload:queued', {
      fileId,
      jobs: [{ provider: targetProviderId, jobId: job.id }],
      enabledProviders: [targetProviderId]
    });
    this._processQueue();

    return { fileId, provider: targetProviderId, source: sourceProviderId, jobId: job.id };
  }

  _assertTransferSourceUrl(sourceUrl) {
    let parsed;
    try {
      parsed = new URL(String(sourceUrl || ''));
    } catch {
      throw new Error('Invalid sourceUrl');
    }

    if (parsed.protocol !== 'https:') {
      throw new Error('sourceUrl must use https');
    }

    if (!ALLOWED_TRANSFER_SOURCE_HOSTS.has(parsed.hostname)) {
      throw new Error(`Unsupported source host '${parsed.hostname}'. Only Catbox URLs are allowed`);
    }

    return parsed;
  }

  async _checkSourceAvailability(sourceUrl) {
    try {
      const head = await axios.head(sourceUrl, {
        timeout: SOURCE_PROBE_TIMEOUT,
        validateStatus: () => true
      });

      if (head.status >= 200 && head.status < 400) {
        return {
          available: true,
          status: head.status,
          size: Number(head.headers['content-length'] || 0) || null,
          contentType: head.headers['content-type'] || null,
          method: 'HEAD'
        };
      }
    } catch (_) {
      // Fallback to GET with range for providers that don't support HEAD reliably.
    }

    const get = await axios.get(sourceUrl, {
      timeout: SOURCE_PROBE_TIMEOUT,
      headers: { Range: 'bytes=0-1' },
      responseType: 'stream',
      validateStatus: () => true
    });

    if (get.data?.destroy) {
      get.data.destroy();
    }

    const ok = get.status === 206 || (get.status >= 200 && get.status < 400);
    return {
      available: ok,
      status: get.status,
      size: Number(get.headers['content-length'] || 0) || null,
      contentType: get.headers['content-type'] || null,
      method: 'GET_RANGE'
    };
  }

  _buildTransferFileName(filename, sourceUrl) {
    if (filename && String(filename).trim()) {
      return String(filename).trim();
    }

    const parsed = new URL(sourceUrl);
    const fromPath = path.basename(parsed.pathname || '');
    if (fromPath && fromPath !== '/' && fromPath !== '.') {
      return fromPath;
    }

    return `transfer_${Date.now()}.mp4`;
  }

  _normalizeUploadFileName(fileName, filePath = '') {
    const fallbackExtension = path.extname(filePath || '') || '.mp4';
    const safeExtension = fallbackExtension || '.mp4';
    const trimmed = String(fileName || '').trim();

    if (!trimmed) {
      return `upload_${Date.now()}${safeExtension}`;
    }

    const hasExtension = Boolean(path.extname(trimmed));
    const normalizedBase = path.basename(trimmed).replace(/[<>:"/\\|?*\x00-\x1F]/g, '_');

    if (hasExtension) {
      return normalizedBase;
    }

    return `${normalizedBase}${safeExtension}`;
  }

  async queueTransferJob({ sourceUrl, targetProvider = 'seekstreaming', folderId = 'root', filename = null }) {
    this._assertTransferSourceUrl(sourceUrl);
    const targetProviderId = await this._resolveProviderId(targetProvider, { allowDisabled: false });

    const sourceCheck = await this._checkSourceAvailability(sourceUrl);
    if (!sourceCheck.available) {
      throw new Error(`Source is not available (HTTP ${sourceCheck.status})`);
    }

    await fs.ensureDir(UPLOAD_DIR);
    const fileName = this._buildTransferFileName(filename, sourceUrl);
    const localPath = path.join(UPLOAD_DIR, `${Date.now()}-${fileName}`);

    const file = await this.db.createFile({
      folderId,
      name: fileName,
      originalUrl: sourceUrl,
      localPath
    });

    const job = await this.db.createJob({
      type: 'transfer',
      fileId: file.id,
      maxAttempts: UPLOAD_RETRY_ATTEMPTS,
      metadata: {
        sourceProvider: 'catbox',
        sourceUrl,
        sourceCheck,
        targetProvider: targetProviderId,
        filePath: localPath,
        folderId,
        fileName
      }
    });

    this.emit('transfer:queued', {
      fileId: file.id,
      jobId: job.id,
      sourceProvider: 'catbox',
      sourceUrl,
      targetProvider: targetProviderId
    });

    this._processQueue();

    return {
      fileId: file.id,
      jobId: job.id,
      sourceProvider: 'catbox',
      sourceUrl,
      targetProvider: targetProviderId,
      fileName
    };
  }

  /**
   * Check all providers status
   */
  async checkProvidersStatus() {
    await this._refreshProviderRuntime();

    const results = {};
    const providerConfigs = await this.db.getProviderConfigs();
    
    for (const [name, adapter] of Object.entries(this.adapters)) {
      try {
        const checkResult = await adapter.checkStatus();
        const enriched = {
          ...checkResult,
          enabled: providerConfigs?.[name]?.enabled !== false
        };

        results[name] = enriched;
        await this.db.setProviderCheckStatus(name, {
          ...enriched,
          source: 'live'
        });
      } catch (error) {
        const failed = {
          name,
          configured: false,
          authenticated: false,
          enabled: providerConfigs?.[name]?.enabled !== false,
          message: `Error: ${error.message}`
        };
        results[name] = failed;
        await this.db.setProviderCheckStatus(name, {
          ...failed,
          source: 'live'
        });
      }
    }

    return results;
  }

  /**
   * Check single provider status and persist snapshot
   */
  async checkSingleProviderStatus(provider) {
    const resolvedProvider = await this._resolveProviderId(provider, { allowDisabled: true });
    const adapter = this.adapters[resolvedProvider];

    const providerConfigs = await this.db.getProviderConfigs();

    try {
      const status = await adapter.checkStatus();
      const enriched = {
        ...status,
        enabled: providerConfigs?.[resolvedProvider]?.enabled !== false
      };

      await this.db.setProviderCheckStatus(resolvedProvider, {
        ...enriched,
        source: 'manual'
      });

      return enriched;
    } catch (error) {
      const failed = {
        name: resolvedProvider,
        configured: false,
        authenticated: false,
        enabled: providerConfigs?.[resolvedProvider]?.enabled !== false,
        message: `Error: ${error.message}`
      };

      await this.db.setProviderCheckStatus(resolvedProvider, {
        ...failed,
        source: 'manual'
      });

      return failed;
    }
  }

  /**
   * Check one provider status (compatibility helper)
   */
  async checkProviderStatus(provider) {
    return this.checkSingleProviderStatus(provider);
  }

  /**
   * Get persisted provider check snapshots
   */
  async getProviderCheckSnapshots() {
    return this.db.getProviderCheckStatuses();
  }

  async _checkProviderFileStatus(provider, status) {
    if (status.status === 'completed' && (status.url || status.fileId)) {
      const adapter = this.adapters[provider];

      if (!adapter) {
        return { ...status, remoteExists: false, error: `Provider adapter unavailable: ${provider}` };
      }

      try {
        if (provider === 'catbox' && status.url) {
          const exists = await adapter.checkFile(status.url);
          return { ...status, remoteExists: exists };
        }

        if (status.fileId) {
          const check = await adapter.checkFile(status.fileId);
          return { ...status, remoteExists: !!check.exists, remoteInfo: check.info || null };
        }

        return { ...status, remoteExists: false, error: 'No file ID or URL' };
      } catch (error) {
        return { ...status, remoteExists: false, error: error.message };
      }
    }

    return { ...status, remoteExists: false, note: 'Not uploaded' };
  }

  /**
   * Check one file status on one provider
   */
  async checkFileProviderStatus(fileId, provider) {
    const resolvedProvider = await this._resolveProviderId(provider, { allowDisabled: true });
    const file = await this.db.getFile(fileId);
    const status = file.providers?.[resolvedProvider] || {
      status: 'pending',
      url: null,
      fileId: null,
      embedUrl: null,
      error: null
    };

    const checkedStatus = await this._checkProviderFileStatus(resolvedProvider, status);
    return { [resolvedProvider]: checkedStatus };
  }

  /**
   * Check one provider integrity across all files
   */
  async checkProviderIntegrity(provider, options = {}) {
    const { autoReuploadMissing = false } = options;
    const resolvedProvider = await this._resolveProviderId(provider, { allowDisabled: true });

    const files = await this.db.listFiles();
    const providerStatus = await this.checkProviderStatus(resolvedProvider);
    const results = {
      provider: resolvedProvider,
      checkedAt: new Date().toISOString(),
      totalFiles: files.length,
      checked: 0,
      issues: [],
      reuploadsQueued: [],
      providerStatus
    };

    for (const file of files) {
      const statusResult = await this.checkFileProviderStatus(file.id, resolvedProvider);
      const status = statusResult[resolvedProvider];
      results.checked++;

      if (status.status === 'completed' && !status.remoteExists) {
        const issue = {
          fileId: file.id,
          fileName: file.name,
          provider: resolvedProvider,
          issue: 'File missing on provider'
        };

        results.issues.push(issue);

        if (autoReuploadMissing) {
          try {
            const reupload = await this.reuploadToProvider(file.id, resolvedProvider);
            results.reuploadsQueued.push({
              fileId: file.id,
              fileName: file.name,
              provider: resolvedProvider,
              jobId: reupload.jobId
            });
          } catch (error) {
            results.reuploadsQueued.push({
              fileId: file.id,
              fileName: file.name,
              provider: resolvedProvider,
              error: error.message
            });
          }
        }
      }
    }

    return results;
  }

  /**
   * Check file status on each provider
   */
  async checkFileProvidersStatus(fileId) {
    await this._refreshProviderRuntime();
    const file = await this.db.getFile(fileId);
    const results = {};

    const providerIds = new Set([
      ...this.providerNames,
      ...Object.keys(file.providers || {})
    ]);

    for (const provider of providerIds) {
      const status = file.providers?.[provider] || {
        status: 'pending',
        url: null,
        fileId: null,
        embedUrl: null,
        error: null
      };
      results[provider] = await this._checkProviderFileStatus(provider, status);
    }

    return results;
  }

  /**
   * Process upload queue
   */
  async _recoverStaleJobs() {
    try {
      const result = await this.db.resetStaleProcessingJobs(10);
      if (result.resetCount > 0) {
        console.log(`[Uploader] Recovered ${result.resetCount} stale processing jobs back to pending`);
        this.processingJobIds.clear();
      }
    } catch (error) {
      console.error('[Uploader] Stale job recovery failed:', error.message);
    }
  }

  async _processQueue() {
    if (!this.isRunning) return;

    let uploadJobs = [];
    let transferJobs = [];

    try {
      await this._refreshProviderRuntime();

      // Get pending upload jobs
      const pendingJobs = await this.db.getPendingJobs(20);
      uploadJobs = this._claimJobs(pendingJobs.filter(j => j.type === 'upload'));
      transferJobs = this._claimJobs(pendingJobs.filter(j => j.type === 'transfer'));

      if (uploadJobs.length === 0 && transferJobs.length === 0) return;

      console.log(`[Uploader] Processing ${uploadJobs.length} upload jobs and ${transferJobs.length} transfer jobs`);

      // Group jobs by file for parallel processing
      const jobsByFile = this._groupJobsByFile(uploadJobs);

      // Process each file's uploads in parallel
      const uploadPromise = Promise.all(
        Object.entries(jobsByFile).map(([fileId, jobs]) =>
          this._processFileUploads(fileId, jobs)
        )
      );

      const transferPromise = Promise.all(
        transferJobs.map((job) => this.providerLimit(async () => {
          try {
            await this._processTransferJob(job);
          } finally {
            this._releaseJobClaim(job.id);
          }
        }))
      );

      await Promise.all([uploadPromise, transferPromise]);

    } catch (error) {
      for (const job of uploadJobs) {
        this._releaseJobClaim(job.id);
      }
      for (const job of transferJobs) {
        this._releaseJobClaim(job.id);
      }
      console.error('[Uploader] Queue processing error:', error);
    }
  }

  _isHlsSourceUrl(sourceUrl) {
    try {
      const parsed = new URL(String(sourceUrl || ''));
      const pathname = parsed.pathname.toLowerCase();
      return pathname.endsWith('.m3u8') || pathname.endsWith('.txt');
    } catch {
      const normalized = String(sourceUrl || '').toLowerCase();
      return normalized.includes('.m3u8') || normalized.includes('.txt');
    }
  }

  _isNotFoundDownloadError(error) {
    const message = String(error?.message || '').toLowerCase();
    return (
      message.includes('http 404')
      || message.includes('status 404')
      || message.includes('404 not found')
      || message.includes('server returned 404')
    );
  }

  async _downloadSourceFile(sourceUrl, destinationPath, options = {}) {
    const fallbackUrls = Array.isArray(options.fallbackUrls) ? options.fallbackUrls : [];
    const candidates = [sourceUrl, ...fallbackUrls].filter(Boolean);
    const uniqueCandidates = [...new Set(candidates)];

    if (uniqueCandidates.length === 0) {
      throw new Error('No source URL available for download');
    }

    let lastError = null;

    for (let index = 0; index < uniqueCandidates.length; index += 1) {
      const candidateUrl = uniqueCandidates[index];
      const isLastCandidate = index === uniqueCandidates.length - 1;

      try {
        await fs.remove(destinationPath).catch(() => {});

        if (this._isHlsSourceUrl(candidateUrl)) {
          const result = await this._downloadHlsSourceWithFfmpeg(candidateUrl, destinationPath);
          return result;
        }

        const response = await axios.get(candidateUrl, {
          responseType: 'stream',
          timeout: SOURCE_DOWNLOAD_TIMEOUT,
          headers: this._getSourceHeaders(candidateUrl),
          validateStatus: () => true
        });

        if (response.status < 200 || response.status >= 400) {
          if (response.data?.destroy) response.data.destroy();
          throw new Error(`Source download failed with HTTP ${response.status}`);
        }

        await fs.ensureDir(path.dirname(destinationPath));
        const writer = fs.createWriteStream(destinationPath);

        await new Promise((resolve, reject) => {
          response.data.pipe(writer);
          writer.on('finish', resolve);
          writer.on('error', reject);
          response.data.on('error', reject);
        });

        const stat = await fs.stat(destinationPath);
        if (!stat.size) {
          throw new Error('Downloaded file is empty');
        }

        return {
          size: stat.size,
          contentType: response.headers['content-type'] || null
        };
      } catch (error) {
        lastError = error;

        if (!this._isNotFoundDownloadError(error) || isLastCandidate) {
          throw error;
        }

        const nextCandidateUrl = uniqueCandidates[index + 1];
        console.warn(`[Uploader] Source 404 on ${candidateUrl}, retrying with ${nextCandidateUrl}`);
      }
    }

    throw lastError || new Error('Source download failed');
  }

  async _processTransferJob(job) {
    const { sourceUrl, targetProvider, filePath, fileName } = job.metadata || {};
    const resolvedTargetProvider = await this._resolveProviderId(targetProvider, { allowDisabled: true });
    const currentAttempt = job.attempts + 1;
    const isLastAttempt = currentAttempt >= job.maxAttempts;

    if (!sourceUrl || !targetProvider || !filePath || !fileName) {
      throw new Error('Invalid transfer job metadata');
    }

    await this.db.updateJob(job.id, {
      status: 'processing',
      attempts: currentAttempt
    });

    await this.db.updateProviderStatus(job.fileId, resolvedTargetProvider, 'uploading');
    this.emit('transfer:started', {
      jobId: job.id,
      fileId: job.fileId,
      sourceProvider: 'catbox',
      sourceUrl,
      targetProvider: resolvedTargetProvider,
      attempt: currentAttempt
    });

    try {
      this._assertTransferSourceUrl(sourceUrl);

      const sourceCheck = await this._checkSourceAvailability(sourceUrl);
      if (!sourceCheck.available) {
        throw new Error(`Source unavailable (HTTP ${sourceCheck.status})`);
      }

      await this.db.updateJob(job.id, { progress: 10 });
      const downloaded = await this._downloadSourceFile(sourceUrl, filePath);
      await this.db.updateJob(job.id, { progress: 50 });

      const result = await this._uploadWithRetry(
        resolvedTargetProvider,
        filePath,
        fileName,
        job.fileId,
        job.id
      );

      const verifyResult = await this._verifyUploadedResult(resolvedTargetProvider, result);
      if (!verifyResult.exists) {
        throw new Error(`Target verification failed on ${resolvedTargetProvider}`);
      }

      await this.db.updateProviderStatus(
        job.fileId,
        resolvedTargetProvider,
        'completed',
        result.url,
        result.fileId,
        null,
        { embedUrl: result.embedUrl || null }
      );

      await this.db.updateJob(job.id, {
        status: 'completed',
        progress: 100,
        metadata: {
          ...job.metadata,
          sourceCheck,
          downloadedSize: downloaded.size,
          targetResult: result,
          targetVerify: verifyResult
        }
      });

      this.emit('transfer:completed', {
        jobId: job.id,
        fileId: job.fileId,
        sourceProvider: 'catbox',
        sourceUrl,
        targetProvider: resolvedTargetProvider,
        url: result.url,
        fileIdOnTarget: result.fileId
      });

      await this._cleanupFilePathWhenUnused(job.fileId, filePath);
    } catch (error) {
      const finalStatus = isLastAttempt ? 'failed' : 'pending';

      await this.db.updateProviderStatus(
        job.fileId,
        resolvedTargetProvider,
        finalStatus,
        null,
        null,
        error.message,
        { embedUrl: null }
      );

      await this.db.updateJob(job.id, {
        status: finalStatus,
        error: error.message
      });

      this.emit('transfer:failed', {
        jobId: job.id,
        fileId: job.fileId,
        sourceProvider: 'catbox',
        sourceUrl,
        targetProvider: resolvedTargetProvider,
        error: error.message,
        attempt: currentAttempt,
        maxAttempts: job.maxAttempts,
        willRetry: !isLastAttempt
      });
    }
  }

  async _verifyUploadedResult(provider, result) {
    const adapter = this.adapters[provider];
    if (!adapter || typeof adapter.checkFile !== 'function') {
      return { exists: true, reason: 'no-check-method' };
    }

    const probeValue = provider === 'catbox'
      ? (result.url || result.fileId)
      : result.fileId;

    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        const raw = await adapter.checkFile(probeValue);
        const exists = typeof raw === 'boolean' ? raw : Boolean(raw?.exists);

        if (exists) {
          return { exists: true, attempt, raw };
        }
      } catch (error) {
        if (attempt === 5) {
          return { exists: false, attempt, error: error.message };
        }
      }

      await this._sleep(2000 * attempt);
    }

    return { exists: false, attempt: 5 };
  }

  /**
   * Group jobs by file ID
   */
  _groupJobsByFile(jobs) {
    return jobs.reduce((acc, job) => {
      if (!acc[job.fileId]) {
        acc[job.fileId] = [];
      }
      acc[job.fileId].push(job);
      return acc;
    }, {});
  }

  _claimJobs(jobs) {
    const claimed = [];

    for (const job of jobs) {
      if (this.processingJobIds.has(job.id)) {
        continue;
      }
      this.processingJobIds.add(job.id);
      claimed.push(job);
    }

    return claimed;
  }

  _releaseJobClaim(jobId) {
    this.processingJobIds.delete(jobId);
  }

  async _isJobStillRunnable(jobId) {
    try {
      const latest = await this.db.getJob(jobId);
      return latest.status === 'pending';
    } catch (_) {
      return false;
    }
  }

  /**
   * Process all provider uploads for a single file
   */
  async _processFileUploads(fileId, jobs) {
    const promises = jobs.map(job =>
      this.providerLimit(async () => {
        try {
          const runnable = await this._isJobStillRunnable(job.id);
          if (!runnable) {
            console.log(`[Uploader] Skipping job ${job.id} for file ${fileId}: status changed before execution`);
            return;
          }
          await this._uploadToProvider(job);
        } finally {
          this._releaseJobClaim(job.id);
        }
      })
    );

    await Promise.allSettled(promises);

    const sourcePath = jobs[0]?.metadata?.filePath;
    if (sourcePath) {
      await this._cleanupFilePathWhenUnused(fileId, sourcePath);
    }
  }

  async _ensureUploadSourceForJob(job) {
    const currentPath = job.metadata?.filePath || null;

    if (currentPath && await fs.pathExists(currentPath)) {
      this._cancelScheduledCleanup(currentPath);
      return { filePath: currentPath, metadata: job.metadata };
    }

    const file = await this.db.getFile(job.fileId);

    let preferredSourceProvider = String(job.metadata?.sourceProvider || '').trim() || null;
    if (preferredSourceProvider) {
      try {
        preferredSourceProvider = await this._resolveProviderId(preferredSourceProvider, { allowDisabled: true });
      } catch (_) {
        preferredSourceProvider = null;
      }
    }

    const recovered = await this._downloadRemoteSourceToTemp(file, {
      excludeProvider: job.metadata?.provider,
      specificProvider: preferredSourceProvider,
      preferPrimary: preferredSourceProvider ? false : true
    });

    const nextMetadata = {
      ...job.metadata,
      filePath: recovered.filePath,
      cleanupAfterUpload: true,
      removeWhenUnused: true,
      sourceRecovered: true,
      sourceRecoveredAt: new Date().toISOString()
    };

    await this.db.updateJob(job.id, { metadata: nextMetadata });
    job.metadata = nextMetadata;

    return { filePath: recovered.filePath, metadata: nextMetadata };
  }

  /**
   * Upload to a specific provider
   */
  async _uploadToProvider(job) {
    const requestedProvider = job.metadata?.provider;
    const provider = await this._resolveProviderId(requestedProvider, { allowDisabled: true });

    if (provider !== requestedProvider) {
      const nextMetadata = { ...(job.metadata || {}), provider };
      await this.db.updateJob(job.id, { metadata: nextMetadata });
      job.metadata = nextMetadata;
    }

    const providerLimit = this._ensureUploadLimit(provider);

    return providerLimit(async () => {
      const { filePath, metadata } = await this._ensureUploadSourceForJob(job);
      const normalizedFileName = this._normalizeUploadFileName(metadata?.fileName, filePath);
      console.log(`[Uploader] Starting upload to ${provider}: ${normalizedFileName}`);

      const currentAttempt = job.attempts + 1;
      const isLastAttempt = currentAttempt >= job.maxAttempts;

      // Update job status
      await this.db.updateJob(job.id, {
        status: 'processing',
        attempts: currentAttempt
      });

      this._startJobHeartbeat(job.id);

      // Update provider status to uploading
      await this.db.updateProviderStatus(job.fileId, provider, 'uploading');
      this.emit('upload:started', { jobId: job.id, fileId: job.fileId, provider, attempt: currentAttempt });

      try {
        // Execute upload with retry logic
        const result = await this._uploadWithRetry(
          provider,
          filePath,
          normalizedFileName,
          job.fileId,
          job.id
        );

        const verifyResult = await this._verifyUploadedResult(provider, result);
        if (!verifyResult.exists) {
          throw new Error(`Verification failed on ${provider}`);
        }

        // Update success status
        await this.db.updateProviderStatus(
          job.fileId,
          provider,
          'completed',
          result.url,
          result.fileId,
          null,
          { embedUrl: result.embedUrl || null }
        );

        await this.db.updateJob(job.id, {
          status: 'completed',
          progress: 100,
          metadata: { ...job.metadata, normalizedFileName, result }
        });

        this._stopJobHeartbeat(job.id);

        this.emit('upload:completed', {
          jobId: job.id,
          fileId: job.fileId,
          provider,
          url: result.url
        });

        console.log(`[Uploader] Upload to ${provider} completed: ${result.url}`);

        if (job.metadata?.removeWhenUnused && filePath) {
          await this._cleanupFilePathWhenUnused(job.fileId, filePath);
        }

      } catch (error) {
        console.error(`[Uploader] Upload to ${provider} failed (attempt ${currentAttempt}/${job.maxAttempts}):`, error.message);

        this._stopJobHeartbeat(job.id);

        const finalStatus = isLastAttempt ? 'failed' : 'pending';

        // Update failure status - mark as failed after max attempts
        await this.db.updateProviderStatus(
          job.fileId,
          provider,
          finalStatus,
          null,
          null,
          error.message,
          { embedUrl: null }
        );

        await this.db.updateJob(job.id, {
          status: finalStatus,
          error: error.message
        });

        this.emit('upload:failed', {
          jobId: job.id,
          fileId: job.fileId,
          provider,
          error: error.message,
          attempt: currentAttempt,
          maxAttempts: job.maxAttempts,
          willRetry: !isLastAttempt
        });

        // Send notification if all attempts exhausted
        if (isLastAttempt) {
          this.emit('upload:failed:final', {
            jobId: job.id,
            fileId: job.fileId,
            provider,
            error: error.message
          });
          console.log(`[Uploader] ${provider} upload failed permanently after ${job.maxAttempts} attempts. Manual retry required.`);

          if (job.metadata?.removeWhenUnused && filePath) {
            await this._cleanupFilePathWhenUnused(job.fileId, filePath);
          }
        }
      }
    });
  }

  /**
   * Upload with retry logic
   */
  async _uploadWithRetry(provider, filePath, fileName, fileId, jobId) {
    let lastError;
    const adapter = this.adapters[provider];

    if (!adapter || typeof adapter.upload !== 'function') {
      throw new Error(`Provider adapter unavailable for '${provider}'`);
    }

    for (let attempt = 1; attempt <= UPLOAD_RETRY_ATTEMPTS; attempt++) {
      try {
        // Create abort controller for timeout handling
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
          controller.abort();
        }, UPLOAD_TIMEOUT);

        this.activeUploads.set(`${jobId}-${provider}`, controller);

        try {
          const result = await adapter.upload(
            filePath,
            fileName,
            (progress) => this._handleProgress(fileId, jobId, provider, progress),
            controller.signal
          );

          clearTimeout(timeoutId);
          return result;

        } finally {
          this.activeUploads.delete(`${jobId}-${provider}`);
          clearTimeout(timeoutId);
        }

      } catch (error) {
        lastError = error;
        console.log(`[Uploader] Attempt ${attempt} failed for ${provider}: ${error.message}`);

        if (attempt < UPLOAD_RETRY_ATTEMPTS) {
          await this._sleep(UPLOAD_RETRY_DELAY * attempt);
        }
      }
    }

    throw lastError;
  }

  _startJobHeartbeat(jobId) {
    this._stopJobHeartbeat(jobId);
    const timer = setInterval(async () => {
      try {
        await this.db.updateJobHeartbeat(jobId);
      } catch (_) {}
    }, 30000);
    if (!this._jobHeartbeats) this._jobHeartbeats = new Map();
    this._jobHeartbeats.set(jobId, timer);
  }

  _stopJobHeartbeat(jobId) {
    if (!this._jobHeartbeats) return;
    const timer = this._jobHeartbeats.get(jobId);
    if (timer) {
      clearInterval(timer);
      this._jobHeartbeats.delete(jobId);
    }
  }

  /**
   * Handle upload progress
   */
  async _handleProgress(fileId, jobId, provider, progress) {
    try {
      await this.db.updateJob(jobId, { progress });
      
      this.emit('upload:progress', {
        fileId,
        jobId,
        provider,
        progress
      });
    } catch (error) {
      console.error('[Uploader] Failed to update progress:', error);
    }
  }

  /**
   * Cancel upload
   */
  async cancelUpload(jobId, provider) {
    const keys = [`${jobId}-${provider}`];

    if (provider === LEGACY_RCLONE_PROVIDER_ID) {
      const mapped = await this._resolveLegacyRcloneProvider();
      if (mapped) {
        keys.push(`${jobId}-${mapped}`);
      }
    }

    for (const key of keys) {
      const controller = this.activeUploads.get(key);
      if (!controller) {
        continue;
      }

      controller.abort();
      this.activeUploads.delete(key);
      return true;
    }

    return false;
  }

  async cancelJob(jobId) {
    const job = await this.db.getJob(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
      return { cancelled: false, reason: `Job already ${job.status}`, job };
    }

    const provider = job.metadata?.provider || job.metadata?.targetProvider;
    if (provider) {
      await this.cancelUpload(job.id, provider);
    }

    await this.db.updateJob(job.id, {
      status: 'cancelled',
      error: 'Cancelled by user'
    });

    console.log(`[Uploader] Cancelled job ${job.id} (${job.type}) for file ${job.fileId || 'n/a'}${provider ? ` on ${provider}` : ''}`);

    if (provider && job.fileId) {
      await this.db.updateProviderStatus(job.fileId, provider, 'failed', null, null, 'Cancelled by user', { embedUrl: null });
    }

    if (job.metadata?.filePath && job.type === 'transfer') {
      await fs.remove(job.metadata.filePath).catch(() => {});
    }

    this.emit('job:cancelled', {
      jobId: job.id,
      fileId: job.fileId,
      type: job.type,
      provider
    });

    return { cancelled: true, jobId: job.id };
  }

  async cancelJobs(jobIds = []) {
    const uniqueJobIds = [...new Set((Array.isArray(jobIds) ? jobIds : []).filter(Boolean))];
    const results = {
      cancelled: [],
      skipped: [],
      failed: []
    };

    for (const jobId of uniqueJobIds) {
      try {
        const result = await this.cancelJob(jobId);
        if (result.cancelled) {
          results.cancelled.push({ jobId });
        } else {
          results.skipped.push({ jobId, reason: result.reason || 'not-cancelled' });
        }
      } catch (error) {
        results.failed.push({ jobId, error: error.message });
      }
    }

    return results;
  }

  async listJobs(filters = {}) {
    return this.db.listJobs(filters);
  }

  async deleteJob(jobId) {
    const job = await this.db.getJob(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    if (job.status === 'pending' || job.status === 'processing') {
      throw new Error('Cannot delete active job. Cancel it first.');
    }

    if (job.metadata?.filePath && job.type === 'transfer') {
      await fs.remove(job.metadata.filePath).catch(() => {});
    }

    const deleted = await this.db.deleteJob(jobId);
    this.emit('job:deleted', { jobId, fileId: deleted.fileId, type: deleted.type });
    return { deleted: true, jobId };
  }

  async deleteFileResources(fileId) {
    await this._refreshProviderRuntime();

    const file = await this.db.getFile(fileId);
    const jobs = await this.db.getJobsByFile(fileId);

    let cancelledUploads = 0;
    for (const job of jobs) {
      const provider = job.metadata?.provider;
      if (provider && (job.status === 'pending' || job.status === 'processing')) {
        const cancelled = await this.cancelUpload(job.id, provider);
        if (cancelled) cancelledUploads += 1;
      }
    }

    await this.db.cancelJobsForFile(fileId);

    const providerDeleteResults = {};
    for (const [provider, status] of Object.entries(file.providers || {})) {
      if (status.status === 'completed' && status.fileId) {
        try {
          const adapter = this.adapters[provider];
          if (!adapter || typeof adapter.delete !== 'function') {
            throw new Error(`Provider adapter unavailable: ${provider}`);
          }
          await adapter.delete(status.fileId);
          providerDeleteResults[provider] = { success: true, deletedRemote: true };
        } catch (error) {
          providerDeleteResults[provider] = {
            success: false,
            deletedRemote: false,
            error: error.message
          };
        }
      } else if (status.status === 'pending' || status.status === 'uploading' || status.status === 'processing') {
        providerDeleteResults[provider] = {
          success: true,
          deletedRemote: false,
          note: 'Upload was not completed; related jobs were cancelled.'
        };
      } else {
        providerDeleteResults[provider] = {
          success: true,
          deletedRemote: false,
          note: `No remote delete needed (status: ${status.status}).`
        };
      }
    }

    return {
      fileId,
      cancelledUploads,
      providerDeleteResults
    };
  }

  /**
   * Get upload status for a file
   */
  async getUploadStatus(fileId) {
    const file = await this.db.getFile(fileId);
    const jobs = await this.db.getJobsByFile(fileId);
    
    return {
      fileId,
      syncStatus: file.syncStatus,
      canDelete: file.canDelete,
      providers: file.providers,
      jobs: jobs.map(j => ({
        id: j.id,
        type: j.type,
        provider: j.metadata?.provider,
        status: j.status,
        progress: j.progress,
        attempts: j.attempts
      }))
    };
  }

  /**
   * Retry failed uploads for a file
   */
  async retryFailedUploads(fileId) {
    await this._refreshProviderRuntime();

    const file = await this.db.getFile(fileId);
    const providerConfigs = await this.db.getProviderConfigs();

    const failedProviders = [];
    for (const [provider, status] of Object.entries(file.providers || {})) {
      if (status.status !== 'failed') {
        continue;
      }

      try {
        const resolvedProvider = await this._resolveProviderId(provider, { allowDisabled: false });
        if (providerConfigs?.[resolvedProvider]?.enabled !== false && !failedProviders.includes(resolvedProvider)) {
          failedProviders.push(resolvedProvider);
        }
      } catch (_) {
        // Ignore providers that are no longer available.
      }
    }

    if (failedProviders.length === 0) {
      return { message: 'No failed uploads to retry for enabled providers' };
    }

    let sourcePath = null;
    let sourceProvider = null;
    const cleanupAfterUpload = false;

    if (file.localPath && await fs.pathExists(file.localPath)) {
      sourcePath = file.localPath;
    } else {
      const remoteSource = await this._resolveRemoteSource(file, { preferPrimary: true });
      if (!remoteSource) {
        throw new Error('No source available for retry: local file not found and no completed provider source');
      }
      sourceProvider = remoteSource.provider;
    }

    const jobs = [];
    for (const provider of failedProviders) {
      await this.db.updateProviderStatus(fileId, provider, 'pending', null, null, null, { embedUrl: null });

      const job = await this.db.createJob({
        type: 'upload',
        fileId,
        maxAttempts: UPLOAD_RETRY_ATTEMPTS,
        metadata: {
          provider,
          filePath: sourcePath,
          folderId: file.folderId,
          fileName: file.name,
          sourceProvider,
          cleanupAfterUpload,
          removeWhenUnused: cleanupAfterUpload
        }
      });

      jobs.push({ provider, jobId: job.id });
    }

    this._processQueue();

    return { fileId, retried: failedProviders, jobs };
  }

  /**
   * Delete file from all providers
   */
  async deleteFromProviders(fileId) {
    await this._refreshProviderRuntime();

    const file = await this.db.getFile(fileId);
    const results = {};

    for (const [provider, status] of Object.entries(file.providers)) {
      if (status.status === 'completed' && status.fileId) {
        try {
          const adapter = this.adapters[provider];
          if (!adapter || typeof adapter.delete !== 'function') {
            throw new Error(`Provider adapter unavailable: ${provider}`);
          }
          await adapter.delete(status.fileId);
          results[provider] = { success: true };
          
          await this.db.updateProviderStatus(fileId, provider, 'deleted', null, null, null, { embedUrl: null });
        } catch (error) {
          results[provider] = { success: false, error: error.message };
        }
      } else {
        results[provider] = { success: false, reason: 'Not uploaded or no file ID' };
      }
    }

    return results;
  }

  /**
   * Get service statistics
   */
  async getProviderCatalog(options = {}) {
    await this._refreshProviderRuntime();
    return this.db.getProviderCatalog(options);
  }

  getStats() {
    return {
      isRunning: this.isRunning,
      activeUploads: this.activeUploads.size,
      adapters: Object.keys(this.adapters),
      maxConcurrentUploads: this._currentMaxConcurrentUploads,
      maxConcurrentProviders: this._currentMaxConcurrentProviders
    };
  }

  getConcurrencyConfig() {
    return {
      maxConcurrentUploads: this._currentMaxConcurrentUploads,
      maxConcurrentProviders: this._currentMaxConcurrentProviders
    };
  }

  setMaxConcurrentUploads(value) {
    const n = Math.max(1, Math.min(20, Number(value) || MAX_CONCURRENT_UPLOADS));
    this._currentMaxConcurrentUploads = n;
    for (const provider of this.providerNames) {
      this.uploadLimits[provider] = pLimit(n);
    }
    this._processQueue();
    return n;
  }

  setMaxConcurrentProviders(value) {
    const n = Math.max(1, Math.min(20, Number(value) || MAX_CONCURRENT_PROVIDERS));
    this._currentMaxConcurrentProviders = n;
    this.providerLimit = pLimit(n);
    this._processQueue();
    return n;
  }

  /**
   * Copy file from one provider to another
   * Uses local file if available, otherwise downloads from source provider
   */
  async copyToProvider(fileId, targetProvider) {
    const targetProviderId = await this._resolveProviderId(targetProvider, { allowDisabled: false });
    const file = await this.db.getFile(fileId);

    // Check if target already has completed upload
    const targetStatus = file.providers?.[targetProviderId];
    if (targetStatus?.status === 'completed') {
      return { fileId, provider: targetProviderId, message: 'Already uploaded to this provider', url: targetStatus.url };
    }

    let sourcePath = null;
    let sourceProvider = null;

    if (file.localPath && await fs.pathExists(file.localPath)) {
      sourcePath = file.localPath;
    } else {
      const remoteSource = await this._resolveRemoteSource(file, { excludeProvider: targetProviderId, preferPrimary: true });
      if (!remoteSource) {
        throw new Error('No source available: local file not found and no provider has completed upload');
      }
      sourceProvider = remoteSource.provider;
    }

    // Create job for the copy
    const job = await this.db.createJob({
      type: 'upload',
      fileId,
      maxAttempts: UPLOAD_RETRY_ATTEMPTS,
      metadata: {
        provider: targetProviderId,
        filePath: sourcePath,
        folderId: file.folderId,
        fileName: file.name,
        sourceProvider,
        isCopy: true,
        cleanupAfterUpload: false,
        removeWhenUnused: false
      }
    });

    // Update provider status to pending
    await this.db.updateProviderStatus(fileId, targetProviderId, 'pending', null, null, null, { embedUrl: null });

    this.emit('upload:queued', { fileId, jobs: [{ provider: targetProviderId, jobId: job.id }], enabledProviders: [targetProviderId] });

    // Process queue
    this._processQueue();

    return { fileId, provider: targetProviderId, jobId: job.id, message: 'Copy queued for upload', sourceProvider };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = { UploaderService };
