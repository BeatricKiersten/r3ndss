/**
 * Video Processor - yt-dlp HLS Processing
 * 
 * Handles downloading, decrypting, and merging HLS streams into MP4.
 * Uses spawn for streaming processing to prevent memory exhaustion.
 */

const { spawn } = require('child_process');
const fs = require('fs-extra');
const path = require('path');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');

const config = require('../config');
const UPLOAD_DIR = config.uploadDir;
const MAX_RETRIES = config.ffmpeg.maxRetries;
const RETRY_DELAY = config.ffmpeg.retryDelay;
const DEBUG_YTDLP_FULL = String(process.env.DEBUG_YTDLP_FULL || 'true').toLowerCase() === 'true';

// Download error codes and their meanings
const DOWNLOAD_ERRORS = {
  'No space left on device': 'Not enough disk space on the server',
  'Connection refused': 'Source server refused connection',
  '404 Not Found': 'Source playlist or segment not found',
  '403 Forbidden': 'Access denied to source stream',
  'Invalid data': 'Corrupted or invalid download data',
  'Protocol not found': 'Protocol not supported',
  'Encryption': 'Failed to decrypt stream'
};

const FFMPEG_IGNORED_WARNING_PATTERNS = [
  /Will reconnect at .*error=End of file\.?/i,
  /error=End of file\.?/i
];

class VideoProcessor {
  constructor(dbHandler, eventEmitter) {
    this.db = dbHandler;
    this.eventEmitter = eventEmitter;
    this.activeProcesses = new Map();
    this._jobHeartbeats = new Map();
  }

  _normalizeProcessingError(error, context = {}) {
    const fallbackMessage = context.fallbackMessage || 'Video processing failed';
    const code = String(error?.code || 'VIDEO_PROCESSING_FAILED').trim() || 'VIDEO_PROCESSING_FAILED';
    const message = String(error?.message || fallbackMessage).trim() || fallbackMessage;

    return {
      code,
      message,
      details: {
        signal: error?.signal || null,
        outputPath: error?.outputPath || context.outputPath || null,
        hlsUrl: error?.hlsUrl || context.hlsUrl || null
      }
    };
  }

  /**
   * Main entry point: Process HLS URL to MP4
   * Checks for duplicate files before creating new ones
   */
  async processHls(hlsUrl, options = {}) {
    const {
      folderId = 'root',
      outputName = null,
      outputDir = null,
      decryptionKey = null,
      headers = {},
      cookies = null,
      skipIfExists = true
    } = options;

    const processId = uuidv4();
    let createdJobId = null;
    
    try {
      const normalizedUrl = String(hlsUrl || '').trim().toLowerCase();
      if (normalizedUrl.includes('youtube.com')) {
        console.log('[VideoProcessor] Skipping unsupported YouTube URL', { hlsUrl });
        return {
          success: true,
          skipped: true,
          reason: 'YouTube URLs are skipped',
          status: 'skipped'
        };
      }

      const resolvedOutputDir = this._resolveOutputDir(outputDir);
      await fs.ensureDir(resolvedOutputDir);
      
      const timestamp = Date.now();
      const baseName = this._sanitizeFileBaseName(outputName) || `video_${timestamp}`;
      const outputFileName = `${baseName}.mp4`;
      const outputPath = path.join(resolvedOutputDir, outputFileName);
      if (skipIfExists) {
        const existingFile = await this.db.findFileByNameInFolder(folderId, outputFileName);
        if (existingFile) {
          console.log(`[VideoProcessor] File "${outputFileName}" already exists in folder ${folderId}, skipping`);
          
          if (existingFile.status === 'completed') {
            return {
              success: true,
              fileId: existingFile.id,
              outputPath: existingFile.localPath,
              size: existingFile.size,
              duration: existingFile.duration,
              skipped: true,
              reason: 'File already exists'
            };
          }
          
          if (existingFile.status === 'processing') {
            return {
              success: true,
              fileId: existingFile.id,
              outputPath: existingFile.localPath,
              skipped: true,
              reason: 'File is already being processed',
              status: existingFile.status
            };
          }
          
          console.log(`[VideoProcessor] Existing file failed before, will retry`);
        }
      }

      const file = await this.db.createFile({
        folderId,
        name: outputFileName,
        originalUrl: hlsUrl,
        localPath: outputPath
      });

      const job = await this.db.createJob({
        type: 'process',
        fileId: file.id,
        maxAttempts: MAX_RETRIES,
        metadata: {
          hlsUrl,
          outputPath
        }
      });
      createdJobId = job.id;

      this.eventEmitter.emit('job:started', { jobId: job.id, fileId: file.id, type: 'process' });

      let lastError = null;
      let succeeded = false;

      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          console.log(`[VideoProcessor] Attempt ${attempt}/${MAX_RETRIES} for ${outputFileName}`);

          await this.db.updateJob(job.id, {
            status: 'processing',
            attempts: attempt,
            progress: 0
          });

          await this.db.updateFileProgress(file.id, 'processing', 0);

          this._startJobHeartbeat(job.id);

          await this._executeYtDlp(job.id, file.id, hlsUrl, outputPath, {
            headers,
            cookies
          });

          this._stopJobHeartbeat(job.id);

          const stats = await fs.stat(outputPath);
          if (stats.size === 0) {
            throw new Error('Output file is empty');
          }

          const duration = await this._getVideoDuration(outputPath);
          await this.db.updateFileProgress(file.id, 'processing', 100);
          
          await this.db.updateJob(job.id, {
            status: 'completed',
            progress: 100,
            metadata: {
              ...(job.metadata || {}),
              fileSize: stats.size,
              duration,
              completedAt: new Date().toISOString()
            }
          });

          await this.db.updateFileMetadata(file.id, {
            size: stats.size,
            duration
          });

          this.eventEmitter.emit('job:completed', { jobId: job.id, fileId: file.id });
          this.eventEmitter.emit('file:ready', { fileId: file.id, filePath: outputPath });

          succeeded = true;
          lastError = null;
          break;
        } catch (error) {
          this._stopJobHeartbeat(job.id);
          lastError = this._normalizeProcessingError(error, {
            outputPath,
            hlsUrl,
            fallbackMessage: 'Video processing attempt failed'
          });
          console.error(`[VideoProcessor] Attempt ${attempt}/${MAX_RETRIES} failed for ${outputFileName}:`, lastError.message);

          if (attempt < MAX_RETRIES) {
            await this.db.updateJob(job.id, {
              status: 'pending',
              progress: 0,
              error: `Attempt ${attempt} failed: ${lastError.message}`,
              metadata: {
                ...(job.metadata || {}),
                lastError
              }
            });
            await this.db.updateFileProgress(file.id, 'processing', 0);

            try {
              if (await fs.pathExists(outputPath)) {
                await fs.remove(outputPath);
              }
            } catch (_) {}

            const delay = RETRY_DELAY * attempt;
            console.log(`[VideoProcessor] Retrying in ${delay}ms...`);
            await this._sleep(delay);
          }
        }
      }

      if (!succeeded) {
        await this.db.updateJob(createdJobId, {
          status: 'failed',
          error: lastError?.message || 'All retry attempts exhausted',
          metadata: {
            ...(job.metadata || {}),
            lastError,
            terminalError: lastError
          }
        });

        await this.db.markFileProcessingCancelled(file.id).catch(() => {});

        this.eventEmitter.emit('job:failed', { jobId: createdJobId, fileId: file.id, error: lastError?.message, details: lastError });
        const terminalError = new Error(lastError?.message || 'Processing failed after all retries');
        terminalError.code = lastError?.code || 'VIDEO_PROCESSING_FAILED';
        terminalError.details = lastError?.details || null;
        throw terminalError;
      }

      const finalFile = await this.db.getFile(file.id);
      return {
        success: true,
        fileId: file.id,
        jobId: job.id,
        outputPath,
        size: finalFile.size,
        duration: finalFile.duration
      };

    } catch (error) {
      const normalizedError = this._normalizeProcessingError(error, { hlsUrl: hlsUrl, fallbackMessage: 'Video processing failed' });
      console.error('Video processing failed:', normalizedError);
      
      if (createdJobId) {
        this._stopJobHeartbeat(createdJobId);
        await this.db.updateJob(createdJobId, {
          status: 'failed',
          error: normalizedError.message,
          metadata: {
            lastError: normalizedError,
            terminalError: normalizedError
          }
        });
        this.eventEmitter.emit('job:failed', { jobId: createdJobId, error: normalizedError.message, details: normalizedError });
      }

      if (createdJobId) {
        const failedJob = await this.db.getJob(createdJobId).catch(() => null);
        if (failedJob?.fileId) {
          await this.db.markFileProcessingCancelled(failedJob.fileId).catch(() => {});
        }
      }

      const surfacedError = new Error(normalizedError.message);
      surfacedError.code = normalizedError.code;
      surfacedError.details = normalizedError.details;
      throw surfacedError;
    }
  }

  /**
   * Execute yt-dlp with streaming and progress tracking
   */
  async _executeYtDlp(jobId, fileId, hlsUrl, outputPath, options) {
    return new Promise((resolve, reject) => {
      const { headers, cookies } = options;
      const args = this._buildYtDlpArgs(hlsUrl, outputPath);

      console.log(`[yt-dlp] Starting job ${jobId} for ${hlsUrl}`);
      if (DEBUG_YTDLP_FULL) {
        console.log('[yt-dlp] Spawn context', {
          jobId,
          fileId,
          hlsUrl,
          outputPath,
          headers,
          cookies,
          args
        });
      }

      const ytDlp = spawn('yt-dlp', args, {
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.activeProcesses.set(jobId, ytDlp);

      let stdoutBuffer = '';
      let stderrBuffer = '';
      let lastProgress = 0;

      ytDlp.stdout.on('data', (data) => {
        const output = data.toString();
        stdoutBuffer += output;

        if (DEBUG_YTDLP_FULL) {
          const lines = output.split(/\r?\n/).filter(Boolean);
          for (const line of lines) {
            console.log(`[yt-dlp][stdout][job:${jobId}] ${line}`);
          }
        }
      });

      ytDlp.stderr.on('data', (data) => {
        const output = data.toString();
        stderrBuffer += output;

        if (DEBUG_YTDLP_FULL) {
          const lines = output.split(/\r?\n/).filter(Boolean);
          for (const line of lines) {
            console.error(`[yt-dlp][stderr][job:${jobId}] ${line}`);
          }
        }

        const progressMatch = output.match(/\[download\]\s+(\d{1,3}(?:\.\d+)?)%/);
        if (progressMatch) {
          const progress = Math.min(Math.round(parseFloat(progressMatch[1])), 99);
          if (!Number.isNaN(progress) && progress !== lastProgress) {
            lastProgress = progress;
            this._updateProgress(jobId, fileId, progress);
          }
        }

        Object.keys(DOWNLOAD_ERRORS).forEach(errorKey => {
          if (output.includes(errorKey)) {
            console.error(`[yt-dlp] Error detected: ${DOWNLOAD_ERRORS[errorKey]}`);
          }
        });
      });

      ytDlp.on('close', async (code, signal) => {
        this.activeProcesses.delete(jobId);

        if (code === 0) {
          console.log(`[yt-dlp] Process completed successfully`, { jobId, code, signal });
          resolve();
        } else {
          console.error('[yt-dlp] Process failed', {
            jobId,
            fileId,
            code,
            signal,
            hlsUrl,
            outputPath,
            args,
            stdout: stdoutBuffer.trim() || null,
            stderr: stderrBuffer.trim() || null
          });
          if (stderrBuffer.trim()) {
            console.error(`[yt-dlp] stderr for failed job ${jobId}:\n${stderrBuffer.trim()}`);
          }
          if (stdoutBuffer.trim()) {
            console.error(`[yt-dlp] stdout for failed job ${jobId}:\n${stdoutBuffer.trim()}`);
          }
          const errorMessage = this._parseDownloadError(stderrBuffer, code);
          const error = new Error(errorMessage);
          error.code = code;
          error.signal = signal;
          error.stdout = stdoutBuffer;
          error.stderr = stderrBuffer;
          error.args = args;
          error.hlsUrl = hlsUrl;
          error.outputPath = outputPath;
          reject(error);
        }
      });

      ytDlp.on('error', (error) => {
        this.activeProcesses.delete(jobId);
        console.error('[yt-dlp] Failed to start process', {
          jobId,
          fileId,
          hlsUrl,
          outputPath,
          args,
          error
        });
        const spawnError = new Error(`Failed to start yt-dlp: ${error.message}`);
        spawnError.cause = error;
        spawnError.args = args;
        spawnError.hlsUrl = hlsUrl;
        spawnError.outputPath = outputPath;
        reject(spawnError);
      });

      const timeout = setTimeout(() => {
        ytDlp.kill('SIGTERM');
        reject(new Error('yt-dlp process timed out after 2 hours'));
      }, 2 * 60 * 60 * 1000); // 2 hours timeout

      ytDlp.on('close', () => {
        clearTimeout(timeout);
      });
    });
  }

  /**
   * Build yt-dlp arguments
   */
  _buildYtDlpArgs(hlsUrl, outputPath) {
    const args = [
      '--no-playlist',
      '--newline',
      '--no-warnings',
      '--downloader', 'ffmpeg',
      '--hls-use-mpegts',
      '-f', 'bestvideo*+bestaudio/best',
      '--merge-output-format', 'mp4',
      '-o', outputPath
    ];

    args.push(hlsUrl);
    return args;
  }

  _resolveOutputDir(outputDir) {
    if (!outputDir || !String(outputDir).trim()) {
      return path.resolve(UPLOAD_DIR);
    }

    const rawPath = String(outputDir).trim();
    return path.isAbsolute(rawPath)
      ? path.normalize(rawPath)
      : path.resolve(process.cwd(), rawPath);
  }

  _sanitizeFileBaseName(name) {
    if (!name || !String(name).trim()) {
      return null;
    }

    const withoutExtension = path.parse(String(name).trim()).name;
    const sanitized = withoutExtension
      .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 180);

    return sanitized || null;
  }

  /**
   * Update progress in database and emit events
   */
  async _updateProgress(jobId, fileId, progress) {
    try {
      await this.db.updateJob(jobId, { progress });
      await this.db.updateFileProgress(fileId, 'processing', progress);
      
      this.eventEmitter.emit('progress', {
        jobId,
        fileId,
        type: 'processing',
        progress
      });
    } catch (error) {
      console.error('Failed to update progress:', error);
    }
  }

  /**
   * Get video duration using ffprobe
   */
  async _getVideoDuration(filePath) {
    return new Promise((resolve, reject) => {
      const ffprobe = spawn('ffprobe', [
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        filePath
      ]);

      let output = '';
      ffprobe.stdout.on('data', (data) => {
        output += data.toString();
      });

      ffprobe.on('close', (code) => {
        if (code === 0) {
          const duration = parseFloat(output.trim());
          resolve(isNaN(duration) ? 0 : duration);
        } else {
          resolve(0);
        }
      });

      ffprobe.on('error', () => {
        resolve(0);
      });
    });
  }

  /**
   * Parse download error output
   */
  _parseDownloadError(stderr, code) {
    for (const [key, value] of Object.entries(DOWNLOAD_ERRORS)) {
      if (stderr.includes(key)) {
        return `yt-dlp Error (${code}): ${value}`;
      }
    }

    const lines = stderr.split('\n').map((line) => line.trim()).filter(Boolean);
    const meaningfulError = [...lines].reverse().find((line) => {
      const normalized = String(line || '').trim();
      if (!normalized) return false;
      if (!/(Error|error|Invalid|Failed|failed|forbidden|not found|timed out)/.test(normalized)) {
        return false;
      }
      return !FFMPEG_IGNORED_WARNING_PATTERNS.some((pattern) => pattern.test(normalized));
    });

    if (meaningfulError) {
      return meaningfulError;
    }

    return `yt-dlp process exited with code ${code}`;
  }

  _startJobHeartbeat(jobId) {
    this._stopJobHeartbeat(jobId);
    const timer = setInterval(async () => {
      try {
        await this.db.updateJobHeartbeat(jobId);
      } catch (_) {}
    }, 15000);
    this._jobHeartbeats.set(jobId, timer);
  }

  _stopJobHeartbeat(jobId) {
    const timer = this._jobHeartbeats.get(jobId);
    if (timer) {
      clearInterval(timer);
      this._jobHeartbeats.delete(jobId);
    }
  }

  /**
   * Cancel active download process
   */
  async cancelJob(jobId) {
    let job = null;
    try {
      job = await this.db.getJob(jobId);
    } catch (_) {}

    this._stopJobHeartbeat(jobId);
    const childProcess = this.activeProcesses.get(jobId);
    if (childProcess) {
      childProcess.kill('SIGTERM');
      this.activeProcesses.delete(jobId);
      
      // Wait a bit, then force kill if still running
      setTimeout(() => {
        try {
          childProcess.kill('SIGKILL');
        } catch (e) {
          // Process already terminated
        }
      }, 5000);
    }

    if (job?.metadata?.outputPath) {
      await fs.remove(job.metadata.outputPath).catch(() => {});
    }

    if (job?.fileId) {
      await this.db.markFileProcessingCancelled(job.fileId).catch(() => {});
    }

    return Boolean(job || childProcess);
  }

  /**
   * Cleanup temporary files
   */
  async _cleanup(...paths) {
    for (const filePath of paths) {
      try {
        if (await fs.pathExists(filePath)) {
          await fs.remove(filePath);
        }
      } catch (error) {
        console.error(`Failed to cleanup ${filePath}:`, error);
      }
    }
  }

  /**
   * Retry wrapper for operations
   */
  async _retry(operation, maxRetries = MAX_RETRIES) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        console.log(`Attempt ${attempt} failed: ${error.message}`);
        
        if (attempt < maxRetries) {
          await this._sleep(RETRY_DELAY * attempt);
        }
      }
    }
    
    throw lastError;
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get active processes status
   */
  getActiveProcesses() {
    return Array.from(this.activeProcesses.entries()).map(([jobId, process]) => ({
      jobId,
      pid: process.pid,
      killed: process.killed
    }));
  }
}

module.exports = { VideoProcessor };
