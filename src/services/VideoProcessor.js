/**
 * Video Processor - Secure FFmpeg HLS Processing
 * 
 * Handles downloading, decrypting, and merging HLS streams into MP4.
 * Uses spawn for streaming processing to prevent memory exhaustion.
 */

const { spawn } = require('child_process');
const fs = require('fs-extra');
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');

const config = require('../config');
const UPLOAD_DIR = config.uploadDir;
const MAX_RETRIES = config.ffmpeg.maxRetries;
const RETRY_DELAY = config.ffmpeg.retryDelay;

// FFmpeg error codes and their meanings
const FFMPEG_ERRORS = {
  'Connection refused': 'Source server refused connection',
  '404 Not Found': 'HLS playlist or segment not found',
  '403 Forbidden': 'Access denied to HLS stream',
  'Invalid data': 'Corrupted or invalid HLS data',
  'Protocol not found': 'Protocol not supported',
  'Encryption': 'Failed to decrypt stream'
};

class VideoProcessor {
  constructor(dbHandler, eventEmitter) {
    this.db = dbHandler;
    this.eventEmitter = eventEmitter;
    this.activeProcesses = new Map();
    this._jobHeartbeats = new Map();
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
      const resolvedOutputDir = this._resolveOutputDir(outputDir);
      await fs.ensureDir(resolvedOutputDir);
      
      const timestamp = Date.now();
      const baseName = this._sanitizeFileBaseName(outputName) || `video_${timestamp}`;
      const outputFileName = `${baseName}.mp4`;
      const outputPath = path.join(resolvedOutputDir, outputFileName);
      const tempPlaylistPath = path.join(resolvedOutputDir, `playlist_${processId}.m3u8`);

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
          outputPath,
          tempPlaylistPath,
          hasDecryption: !!decryptionKey
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

          await this._executeFfmpeg(job.id, file.id, hlsUrl, outputPath, {
            decryptionKey,
            headers,
            cookies,
            tempPlaylistPath
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

          await this._cleanup(tempPlaylistPath);

          this.eventEmitter.emit('job:completed', { jobId: job.id, fileId: file.id });
          this.eventEmitter.emit('file:ready', { fileId: file.id, filePath: outputPath });

          succeeded = true;
          lastError = null;
          break;
        } catch (error) {
          this._stopJobHeartbeat(job.id);
          lastError = error;
          console.error(`[VideoProcessor] Attempt ${attempt}/${MAX_RETRIES} failed for ${outputFileName}:`, error.message);

          if (attempt < MAX_RETRIES) {
            await this.db.updateJob(job.id, {
              status: 'pending',
              progress: 0,
              error: `Attempt ${attempt} failed: ${error.message}`
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
          error: lastError?.message || 'All retry attempts exhausted'
        });

        this.eventEmitter.emit('job:failed', { jobId: createdJobId, error: lastError?.message });
        throw lastError || new Error('Processing failed after all retries');
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
      console.error('Video processing failed:', error);
      
      if (createdJobId) {
        this._stopJobHeartbeat(createdJobId);
        await this.db.updateJob(createdJobId, {
          status: 'failed',
          error: error.message
        });
        this.eventEmitter.emit('job:failed', { jobId: createdJobId, error: error.message });
      }

      throw error;
    }
  }

  /**
   * Execute FFmpeg with streaming and progress tracking
   */
  async _executeFfmpeg(jobId, fileId, hlsUrl, outputPath, options) {
    return new Promise((resolve, reject) => {
      const { decryptionKey, headers, cookies, tempPlaylistPath } = options;
      
      // Build FFmpeg arguments
      const args = this._buildFfmpegArgs(hlsUrl, outputPath, {
        decryptionKey,
        headers,
        cookies
      });

      console.log(`[FFmpeg] Starting: ffmpeg ${args.join(' ')}`);

      // Spawn FFmpeg process
      const ffmpeg = spawn('ffmpeg', args, {
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.activeProcesses.set(jobId, ffmpeg);

      let stderrBuffer = '';
      let duration = 0;
      let lastProgress = 0;

      // Parse progress from stderr
      ffmpeg.stderr.on('data', (data) => {
        const output = data.toString();
        stderrBuffer += output;

        // Parse duration if not yet known
        if (duration === 0) {
          const durationMatch = output.match(/Duration: (\d{2}):(\d{2}):(\d{2}\.\d{2})/);
          if (durationMatch) {
            const hours = parseInt(durationMatch[1]);
            const minutes = parseInt(durationMatch[2]);
            const seconds = parseFloat(durationMatch[3]);
            duration = hours * 3600 + minutes * 60 + seconds;
          }
        }

        // Parse current time for progress
        const timeMatch = output.match(/time=(\d{2}):(\d{2}):(\d{2}\.\d{2})/);
        if (timeMatch && duration > 0) {
          const hours = parseInt(timeMatch[1]);
          const minutes = parseInt(timeMatch[2]);
          const seconds = parseFloat(timeMatch[3]);
          const currentTime = hours * 3600 + minutes * 60 + seconds;
          const progress = Math.min(Math.round((currentTime / duration) * 100), 99);

          if (progress !== lastProgress) {
            lastProgress = progress;
            this._updateProgress(jobId, fileId, progress);
          }
        }

        // Check for errors in output
        Object.keys(FFMPEG_ERRORS).forEach(errorKey => {
          if (output.includes(errorKey)) {
            console.error(`[FFmpeg] Error detected: ${FFMPEG_ERRORS[errorKey]}`);
          }
        });
      });

      // Handle process completion
      ffmpeg.on('close', async (code) => {
        this.activeProcesses.delete(jobId);

        if (code === 0) {
          console.log(`[FFmpeg] Process completed successfully`);
          resolve();
        } else {
          const errorMessage = this._parseFfmpegError(stderrBuffer, code);
          reject(new Error(errorMessage));
        }
      });

      // Handle process errors
      ffmpeg.on('error', (error) => {
        this.activeProcesses.delete(jobId);
        reject(new Error(`Failed to start FFmpeg: ${error.message}`));
      });

      // Set timeout
      const timeout = setTimeout(() => {
        ffmpeg.kill('SIGTERM');
        reject(new Error('FFmpeg process timed out after 2 hours'));
      }, 2 * 60 * 60 * 1000); // 2 hours timeout

      ffmpeg.on('close', () => {
        clearTimeout(timeout);
      });
    });
  }

  /**
   * Build FFmpeg arguments
   */
  _buildFfmpegArgs(hlsUrl, outputPath, options) {
    const { decryptionKey, headers, cookies } = options;
    const args = [
      '-y', // Overwrite output
      '-hide_banner',
      '-loglevel', 'warning', // Reduce verbosity but show warnings
      '-stats',
      '-fflags', '+discardcorrupt', // Discard corrupt packets
      '-reconnect', '1', // Reconnect on network issues
      '-reconnect_at_eof', '1',
      '-reconnect_streamed', '1',
      '-reconnect_delay_max', '5',
      '-thread_queue_size', '4096'
    ];

    // Add headers if provided
    if (headers && Object.keys(headers).length > 0) {
      const headerString = Object.entries(headers)
        .filter(([_, value]) => value !== undefined && value !== null && String(value).length > 0)
        .map(([key, value]) => `${key}: ${value}`)
        .join('\r\n');

      if (headerString) {
        args.push('-headers', `${headerString}\r\n`);
      }
    }

    // Handle decryption
    if (decryptionKey) {
      // If key is provided as URL or data, handle accordingly
      if (decryptionKey.startsWith('http')) {
        args.push('-decryption_key', decryptionKey);
      } else {
        // Key is a hex string
        args.push('-decryption_key', decryptionKey);
      }
    }

    // Add cookies if provided
    if (cookies) {
      args.push('-cookies', cookies);
    }

    args.push('-i', hlsUrl);

    // Output settings for MP4
    args.push(
      '-c', 'copy', // Copy streams without re-encoding
      '-bsf:a', 'aac_adtstoasc', // Fix AAC audio
      '-movflags', '+faststart', // Web-optimized
      '-metadata', 'processed_by=HLS-MP4-Backup-Platform',
      outputPath
    );

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
   * Parse FFmpeg error output
   */
  _parseFfmpegError(stderr, code) {
    for (const [key, value] of Object.entries(FFMPEG_ERRORS)) {
      if (stderr.includes(key)) {
        return `FFmpeg Error (${code}): ${value}`;
      }
    }
    
    // Try to extract the last error line
    const lines = stderr.split('\n').filter(l => l.trim());
    const lastError = lines.find(l => 
      l.includes('Error') || l.includes('error')
    );
    
    return lastError || `FFmpeg process exited with code ${code}`;
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
   * Cancel active FFmpeg process
   */
  async cancelJob(jobId) {
    this._stopJobHeartbeat(jobId);
    const process = this.activeProcesses.get(jobId);
    if (process) {
      process.kill('SIGTERM');
      this.activeProcesses.delete(jobId);
      
      // Wait a bit, then force kill if still running
      setTimeout(() => {
        try {
          process.kill('SIGKILL');
        } catch (e) {
          // Process already terminated
        }
      }, 5000);
      
      return true;
    }
    return false;
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
