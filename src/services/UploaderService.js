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
const { BackblazeAdapter } = require('../providers/backblaze');
const { VoeSxAdapter } = require('../providers/voesx');
const { CatboxAdapter } = require('../providers/catbox');
const { SeekStreamingAdapter } = require('../providers/seekstreaming');

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
const ALLOWED_TRANSFER_SOURCE_HOSTS = new Set([
  'catbox.moe',
  'files.catbox.moe',
  'litterbox.catbox.moe'
]);
const ALLOWED_REUPLOAD_SOURCES = ['catbox', 'backblaze', 'seekstreaming'];

class UploaderService extends EventEmitter {
  constructor(dbHandler) {
    super();
    this.db = dbHandler;
    this.isRunning = false;
    this.activeUploads = new Map(); // Track active upload streams
    this.providerLimit = pLimit(MAX_CONCURRENT_PROVIDERS);
    
    // Initialize provider adapters
    this.adapters = {
      backblaze: new BackblazeAdapter({ db: this.db }),
      voesx: new VoeSxAdapter(),
      catbox: new CatboxAdapter(),
      seekstreaming: new SeekStreamingAdapter()
    };
    this.providerNames = Object.keys(this.adapters);

    // Provider-specific rate limiters
    this.uploadLimits = {
      backblaze: pLimit(MAX_CONCURRENT_UPLOADS),
      voesx: pLimit(MAX_CONCURRENT_UPLOADS),
      catbox: pLimit(MAX_CONCURRENT_UPLOADS),
      seekstreaming: pLimit(MAX_CONCURRENT_UPLOADS)
    };
  }

  _ensureProviderSupported(provider) {
    if (!this.providerNames.includes(provider)) {
      throw new Error(`Provider '${provider}' is not supported`);
    }
  }

  /**
   * Start the uploader service
   */
  async start() {
    if (this.isRunning) return;
    
    this.isRunning = true;
    console.log('[Uploader] Service started');
    
    // Process queue periodically
    this.processInterval = setInterval(() => {
      this._processQueue();
    }, 2000);

    // Initial processing
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
    
    // Cancel all active uploads
    for (const [key, controller] of this.activeUploads.entries()) {
      try {
        controller.abort();
      } catch (e) {
        console.error(`[Uploader] Failed to abort upload ${key}:`, e);
      }
    }
    
    this.activeUploads.clear();
    console.log('[Uploader] Service stopped');
  }

  /**
   * Queue a file for upload to all providers
   */
  async queueFileUpload(fileId, filePath, folderId = 'root', selectedProviders = null) {
    console.log(`[Uploader] Queueing file ${fileId} for upload`);

    await this.db.updateFileProgress(fileId, 'upload', 0);

    const providerConfigs = await this.db.getProviderConfigs();
    const allProviders = ['backblaze', 'voesx', 'catbox', 'seekstreaming'];
    
    // Use selected providers if provided, otherwise use all enabled providers
    let providers;
    if (selectedProviders && selectedProviders.length > 0) {
      providers = selectedProviders.filter(p => providerConfigs?.[p]?.enabled !== false);
    } else {
      providers = allProviders.filter((provider) => providerConfigs?.[provider]?.enabled !== false);
    }

    if (providers.length === 0) {
      throw new Error('No providers are enabled. Enable at least one provider first.');
    }

    const jobs = [];
    for (const provider of providers) {
      const job = await this.db.createJob({
        type: 'upload',
        fileId,
        maxAttempts: UPLOAD_RETRY_ATTEMPTS,
        metadata: {
          provider,
          filePath,
          folderId,
          fileName: path.basename(filePath),
          removeWhenUnused: false
        }
      });

      jobs.push({ provider, jobId: job.id });
    }

    this.emit('upload:queued', { fileId, jobs, enabledProviders: providers });

    this._processQueue();

    return { fileId, jobs, enabledProviders: providers };
  }

  async _getPrimaryProvider() {
    const provider = await this.db.getPrimaryProvider();
    return this.providerNames.includes(provider) ? provider : 'catbox';
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

  _getSourceHeaders(sourceUrl) {
    if (sourceUrl.includes('emergingtechhubonline.store')) {
      return this.adapters.seekstreaming.getDownloadHeaders();
    }

    return {};
  }

  async _downloadHlsSourceWithYtdlp(sourceUrl, destinationPath) {
    await fs.ensureDir(path.dirname(destinationPath));

    const headers = this._getSourceHeaders(sourceUrl);
    const args = ['-o', destinationPath];

    for (const [key, value] of Object.entries(headers)) {
      args.push('--add-header', `${key}:${value}`);
    }

    args.push(sourceUrl);

    await new Promise((resolve, reject) => {
      const proc = spawn('yt-dlp', args, {
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

        reject(new Error(`yt-dlp failed with code ${code}${stderr ? `: ${stderr.trim()}` : ''}`));
      });

      proc.on('error', (error) => {
        reject(new Error(`Failed to start yt-dlp: ${error.message}`));
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
    const primaryProvider = await this._getPrimaryProvider();
    const candidates = [];

    if (specificProvider) {
      candidates.push(specificProvider);
    } else if (preferPrimary && primaryProvider !== excludeProvider) {
      candidates.push(primaryProvider);
    }

    for (const provider of this.providerNames) {
      if (provider === excludeProvider || candidates.includes(provider)) {
        continue;
      }
      candidates.push(provider);
    }

    for (const provider of candidates) {
      const status = file.providers?.[provider];
      const sourceUrl = this._getProviderDownloadUrl(provider, status);

      if (sourceUrl) {
        return {
          provider,
          sourceUrl,
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
    await this._downloadSourceFile(remoteSource.sourceUrl, tempPath);

    return {
      ...remoteSource,
      filePath: tempPath,
      needsCleanup: true
    };
  }

  async _removeManagedLocalFile(fileId, filePath) {
    if (!filePath) return;

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

  async _cleanupFilePathWhenUnused(fileId, filePath) {
    if (!filePath) return;

    const jobs = await this.db.getJobsByFile(fileId);
    const relatedJobs = jobs.filter((item) => item.metadata?.filePath === filePath);

    if (relatedJobs.length === 0) {
      return;
    }

    if (DEBUG_UPLOAD_CLEANUP) {
      console.log(`[Uploader] Cleanup check for ${fileId}: ${relatedJobs.map((item) => `${item.id}:${item.status}`).join(', ')}`);
    }

    const hasNonFinalJob = relatedJobs.some((item) => (
      item.status === 'pending' || item.status === 'processing'
    ));

    const hasRunningUploadState = relatedJobs.some((item) => item.status === 'uploading');

    const allJobsFinished = relatedJobs.every((item) => (
      item.status === 'completed' || item.status === 'failed' || item.status === 'cancelled'
    ));

    if (!hasNonFinalJob && !hasRunningUploadState && allJobsFinished) {
      await this._removeManagedLocalFile(fileId, filePath);
    }
  }

  /**
   * Re-upload to a specific provider
   */
  async reuploadToProvider(fileId, provider, source) {
    console.log(`[Uploader] Re-uploading file ${fileId} to ${provider} from source ${source}`);

    const file = await this.db.getFile(fileId);
    const providerConfigs = await this.db.getProviderConfigs();

    if (providerConfigs?.[provider]?.enabled === false) {
      throw new Error(`Provider ${provider} is disabled`);
    }

    if (!source) {
      throw new Error('Source is required for reupload');
    }

    if (!ALLOWED_REUPLOAD_SOURCES.includes(source)) {
      throw new Error(`Source must be one of: ${ALLOWED_REUPLOAD_SOURCES.join(', ')}`);
    }

    if (source === provider) {
      throw new Error('Source cannot be the same as target provider');
    }

    const sourceStatus = file.providers?.[source];
    if (!sourceStatus || sourceStatus.status !== 'completed') {
      throw new Error(`Source provider '${source}' is not available or not completed`);
    }

    let sourcePath = null;
    let cleanupAfterUpload = false;

    if (file.localPath && await fs.pathExists(file.localPath)) {
      sourcePath = file.localPath;
    } else {
      const downloadedSource = await this._downloadRemoteSourceToTemp(file, { excludeProvider: provider, specificProvider: source });
      sourcePath = downloadedSource.filePath;
      cleanupAfterUpload = downloadedSource.needsCleanup;
    }

    // Reset provider status
    await this.db.updateProviderStatus(fileId, provider, 'pending', null, null, null, { embedUrl: null });

    // Create new job
    const job = await this.db.createJob({
      type: 'upload',
      fileId,
      maxAttempts: UPLOAD_RETRY_ATTEMPTS,
      metadata: {
        provider,
        filePath: sourcePath,
        folderId: file.folderId,
        fileName: file.name,
        cleanupAfterUpload,
        removeWhenUnused: true
      }
    });

    this.emit('upload:queued', { fileId, jobs: [{ provider, jobId: job.id }], enabledProviders: [provider] });
    this._processQueue();

    return { fileId, provider, jobId: job.id };
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
    this._ensureProviderSupported(targetProvider);

    const providerConfigs = await this.db.getProviderConfigs();
    if (providerConfigs?.[targetProvider]?.enabled === false) {
      throw new Error(`Target provider ${targetProvider} is disabled`);
    }

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
        targetProvider,
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
      targetProvider
    });

    this._processQueue();

    return {
      fileId: file.id,
      jobId: job.id,
      sourceProvider: 'catbox',
      sourceUrl,
      targetProvider,
      fileName
    };
  }

  /**
   * Check all providers status
   */
  async checkProvidersStatus() {
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
    const adapter = this.adapters[provider];
    if (!adapter) {
      throw new Error(`Provider '${provider}' not found`);
    }

    const providerConfigs = await this.db.getProviderConfigs();

    try {
      const status = await adapter.checkStatus();
      const enriched = {
        ...status,
        enabled: providerConfigs?.[provider]?.enabled !== false
      };

      await this.db.setProviderCheckStatus(provider, {
        ...enriched,
        source: 'manual'
      });

      return enriched;
    } catch (error) {
      const failed = {
        name: provider,
        configured: false,
        authenticated: false,
        enabled: providerConfigs?.[provider]?.enabled !== false,
        message: `Error: ${error.message}`
      };

      await this.db.setProviderCheckStatus(provider, {
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
    if (status.status === 'completed' && status.url) {
      const adapter = this.adapters[provider];

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
    this._ensureProviderSupported(provider);
    const file = await this.db.getFile(fileId);
    const status = file.providers?.[provider] || {
      status: 'pending',
      url: null,
      fileId: null,
      embedUrl: null,
      error: null
    };

    const checkedStatus = await this._checkProviderFileStatus(provider, status);
    return { [provider]: checkedStatus };
  }

  /**
   * Check one provider integrity across all files
   */
  async checkProviderIntegrity(provider, options = {}) {
    const { autoReuploadMissing = false } = options;
    this._ensureProviderSupported(provider);

    const files = await this.db.listFiles();
    const providerStatus = await this.checkProviderStatus(provider);
    const results = {
      provider,
      checkedAt: new Date().toISOString(),
      totalFiles: files.length,
      checked: 0,
      issues: [],
      reuploadsQueued: [],
      providerStatus
    };

    for (const file of files) {
      const statusResult = await this.checkFileProviderStatus(file.id, provider);
      const status = statusResult[provider];
      results.checked++;

      if (status.status === 'completed' && !status.remoteExists) {
        const issue = {
          fileId: file.id,
          fileName: file.name,
          provider,
          issue: 'File missing on provider'
        };

        results.issues.push(issue);

        if (autoReuploadMissing) {
          try {
            const reupload = await this.reuploadToProvider(file.id, provider);
            results.reuploadsQueued.push({
              fileId: file.id,
              fileName: file.name,
              provider,
              jobId: reupload.jobId
            });
          } catch (error) {
            results.reuploadsQueued.push({
              fileId: file.id,
              fileName: file.name,
              provider,
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
    const file = await this.db.getFile(fileId);
    const results = {};

    for (const [provider, status] of Object.entries(file.providers)) {
      results[provider] = await this._checkProviderFileStatus(provider, status);
    }

    return results;
  }

  /**
   * Process upload queue
   */
  async _processQueue() {
    if (!this.isRunning) return;

    try {
      // Get pending upload jobs
      const pendingJobs = await this.db.getPendingJobs(20);
      const uploadJobs = pendingJobs.filter(j => j.type === 'upload');
      const transferJobs = pendingJobs.filter(j => j.type === 'transfer');

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
        transferJobs.map((job) => this.providerLimit(() => this._processTransferJob(job)))
      );

      await Promise.all([uploadPromise, transferPromise]);

    } catch (error) {
      console.error('[Uploader] Queue processing error:', error);
    }
  }

  async _downloadSourceFile(sourceUrl, destinationPath) {
    if (sourceUrl.includes('emergingtechhubonline.store') && (sourceUrl.includes('.txt') || sourceUrl.includes('.m3u8'))) {
      return this._downloadHlsSourceWithYtdlp(sourceUrl, destinationPath);
    }

    const response = await axios.get(sourceUrl, {
      responseType: 'stream',
      timeout: SOURCE_DOWNLOAD_TIMEOUT,
      headers: this._getSourceHeaders(sourceUrl),
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
  }

  async _processTransferJob(job) {
    const { sourceUrl, targetProvider, filePath, fileName } = job.metadata || {};
    const currentAttempt = job.attempts + 1;
    const isLastAttempt = currentAttempt >= job.maxAttempts;

    if (!sourceUrl || !targetProvider || !filePath || !fileName) {
      throw new Error('Invalid transfer job metadata');
    }

    await this.db.updateJob(job.id, {
      status: 'processing',
      attempts: currentAttempt
    });

    await this.db.updateProviderStatus(job.fileId, targetProvider, 'uploading');
    this.emit('transfer:started', {
      jobId: job.id,
      fileId: job.fileId,
      sourceProvider: 'catbox',
      sourceUrl,
      targetProvider,
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
        targetProvider,
        filePath,
        fileName,
        job.fileId,
        job.id
      );

      const verifyResult = await this._verifyUploadedResult(targetProvider, result);
      if (!verifyResult.exists) {
        throw new Error(`Target verification failed on ${targetProvider}`);
      }

      await this.db.updateProviderStatus(
        job.fileId,
        targetProvider,
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
        targetProvider,
        url: result.url,
        fileIdOnTarget: result.fileId
      });

      await this._cleanupFilePathWhenUnused(job.fileId, filePath);
    } catch (error) {
      const finalStatus = isLastAttempt ? 'failed' : 'pending';

      await this.db.updateProviderStatus(
        job.fileId,
        targetProvider,
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
        targetProvider,
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

  /**
   * Process all provider uploads for a single file
   */
  async _processFileUploads(fileId, jobs) {
    const promises = jobs.map(job => 
      this.providerLimit(() => this._uploadToProvider(job))
    );

    await Promise.allSettled(promises);

    const sourcePath = jobs[0]?.metadata?.filePath;
    if (sourcePath) {
      await this._cleanupFilePathWhenUnused(fileId, sourcePath);
    }
  }

  /**
   * Upload to a specific provider
   */
  async _uploadToProvider(job) {
    const { provider, filePath, fileName } = job.metadata;
    const providerLimit = this.uploadLimits[provider];

    return providerLimit(async () => {
      const normalizedFileName = this._normalizeUploadFileName(fileName, filePath);
      console.log(`[Uploader] Starting upload to ${provider}: ${normalizedFileName}`);

      const currentAttempt = job.attempts + 1;
      const isLastAttempt = currentAttempt >= job.maxAttempts;

      // Update job status
      await this.db.updateJob(job.id, {
        status: 'processing',
        attempts: currentAttempt
      });

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
    const controller = this.activeUploads.get(`${jobId}-${provider}`);
    if (controller) {
      controller.abort();
      this.activeUploads.delete(`${jobId}-${provider}`);
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
    const file = await this.db.getFile(fileId);
    const providerConfigs = await this.db.getProviderConfigs();

    const failedProviders = Object.entries(file.providers)
      .filter(([provider, status]) => status.status === 'failed' && providerConfigs?.[provider]?.enabled !== false)
      .map(([provider]) => provider);

    if (failedProviders.length === 0) {
      return { message: 'No failed uploads to retry for enabled providers' };
    }

    let sourcePath = null;
    let cleanupAfterUpload = false;

    if (file.localPath && await fs.pathExists(file.localPath)) {
      sourcePath = file.localPath;
    } else {
      const downloadedSource = await this._downloadRemoteSourceToTemp(file, { preferPrimary: true });
      sourcePath = downloadedSource.filePath;
      cleanupAfterUpload = downloadedSource.needsCleanup;
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
          cleanupAfterUpload,
          removeWhenUnused: true
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
    const file = await this.db.getFile(fileId);
    const results = {};

    for (const [provider, status] of Object.entries(file.providers)) {
      if (status.status === 'completed' && status.fileId) {
        try {
          const adapter = this.adapters[provider];
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
  getStats() {
    return {
      isRunning: this.isRunning,
      activeUploads: this.activeUploads.size,
      adapters: Object.keys(this.adapters)
    };
  }

  /**
   * Copy file from one provider to another
   * Uses local file if available, otherwise downloads from source provider
   */
  async copyToProvider(fileId, targetProvider) {
    this._ensureProviderSupported(targetProvider);

    const file = await this.db.getFile(fileId);
    const providerConfigs = await this.db.getProviderConfigs();

    if (providerConfigs?.[targetProvider]?.enabled === false) {
      throw new Error(`Target provider ${targetProvider} is disabled`);
    }

    // Check if target already has completed upload
    const targetStatus = file.providers?.[targetProvider];
    if (targetStatus?.status === 'completed') {
      return { fileId, provider: targetProvider, message: 'Already uploaded to this provider', url: targetStatus.url };
    }

    const primaryProvider = await this._getPrimaryProvider();

    // Determine source: prefer primary provider, else local file, else any completed provider
    let sourcePath = null;
    let needsCleanup = false;

    const preferredRemoteSource = await this._resolveRemoteSource(file, { excludeProvider: targetProvider, preferPrimary: true });

    if (preferredRemoteSource?.provider === primaryProvider) {
      const downloadedSource = await this._downloadRemoteSourceToTemp(file, { excludeProvider: targetProvider, preferPrimary: true });
      sourcePath = downloadedSource.filePath;
      needsCleanup = downloadedSource.needsCleanup;
    } else if (file.localPath && await fs.pathExists(file.localPath)) {
      sourcePath = file.localPath;
    } else {
      const remoteSource = preferredRemoteSource || await this._resolveRemoteSource(file, { excludeProvider: targetProvider, preferPrimary: false });

      if (!remoteSource) {
        throw new Error('No source available: local file not found and no provider has completed upload');
      }

      console.log(`[Uploader] Downloading from ${remoteSource.provider} to copy to ${targetProvider}`);
      const downloadedSource = await this._downloadRemoteSourceToTemp(file, { excludeProvider: targetProvider, preferPrimary: false });
      sourcePath = downloadedSource.filePath;
      needsCleanup = downloadedSource.needsCleanup;
    }

    try {
      // Create job for the copy
      const job = await this.db.createJob({
        type: 'upload',
        fileId,
        maxAttempts: UPLOAD_RETRY_ATTEMPTS,
        metadata: {
          provider: targetProvider,
          filePath: sourcePath,
          folderId: file.folderId,
          fileName: file.name,
          isCopy: true,
          cleanupAfterUpload: needsCleanup,
          removeWhenUnused: true
        }
      });

      // Update provider status to pending
      await this.db.updateProviderStatus(fileId, targetProvider, 'pending', null, null, null, { embedUrl: null });

      this.emit('upload:queued', { fileId, jobs: [{ provider: targetProvider, jobId: job.id }], enabledProviders: [targetProvider] });

      // Process queue
      this._processQueue();

      return { fileId, provider: targetProvider, jobId: job.id, message: 'Copy queued for upload' };
    } catch (error) {
      // Clean up temp file if we created one
      if (needsCleanup && sourcePath) {
        await fs.remove(sourcePath).catch(() => {});
      }
      throw error;
    }
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = { UploaderService };
