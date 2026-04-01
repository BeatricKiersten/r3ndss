/**
 * SeekStreaming Adapter
 * 
 * Adapter for SeekStreaming video hosting
 * Docs: https://seekstreaming.com/api-document/index.html
 * 
 * SeekStreaming uses TUS protocol for uploads (chunkSize: 52,428,800 bytes)
 * Auth header: api-token (not Bearer)
 */

const fs = require('fs');
const axios = require('axios');

class SeekStreamingAdapter {
  constructor() {
    this.apiKey = process.env.SEEK_API_KEY;
    this.baseUrl = process.env.SEEK_API_BASE_URL || 'https://seekstreaming.com';
    this.chunkSize = 52428800; // 52,428,800 bytes (~50MB) as per API docs
  }

  _getApiHeaders() {
    return {
      'api-token': this.apiKey
    };
  }

  _normalizeUploadEndpoint(tusUrl) {
    if (!tusUrl) {
      throw new Error('SeekStreaming did not return a tusUrl');
    }

    return tusUrl.endsWith('/') ? tusUrl : `${tusUrl}/`;
  }

  _buildTusMetadata(fileName, fileType, accessToken, extraMetadata = {}) {
    return {
      accessToken,
      filename: fileName,
      filetype: fileType,
      ...extraMetadata
    };
  }

  _encodeTusMetadata(metadata) {
    return Object.entries(metadata)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
      .map(([key, value]) => `${key} ${Buffer.from(String(value), 'utf-8').toString('base64')}`)
      .join(',');
  }

  _getTusHeaders(metadata, additionalHeaders = {}) {
    const encodedMetadata = this._encodeTusMetadata(metadata);

    return {
      'Tus-Resumable': '1.0.0',
      metadata: encodedMetadata,
      'Upload-Metadata': encodedMetadata,
      ...additionalHeaders
    };
  }

  /**
   * Construct playable/embed URLs from video ID
   * SeekStreaming uses ID-based URLs for playback
   */
  _buildVideoUrls(videoId) {
    if (!videoId) return { playerUrl: null, embedUrl: null };
    
    // SeekStreaming embed/player URL patterns
    // Observed embed host format: https://seekstream.embedseek.com/#<id>
    return {
      playerUrl: `https://seekstreaming.com/v/${videoId}`,
      embedUrl: `https://seekstream.embedseek.com/#${videoId}`,
      iframeUrl: `https://seekstream.embedseek.com/#${videoId}`
    };
  }

  _extractVideoId(input) {
    if (!input) return null;

    if (typeof input === 'string') {
      const trimmed = input.trim();
      if (!trimmed) return null;

      if (!trimmed.includes('http')) {
        return trimmed;
      }

      try {
        const parsed = new URL(trimmed);
        if (parsed.hash && parsed.hash.length > 1) {
          return parsed.hash.slice(1);
        }

        const pathMatch = parsed.pathname.match(/\/(?:v|embed)\/([^/?#]+)/);
        if (pathMatch) {
          return pathMatch[1];
        }
      } catch (_) {
        return null;
      }
    }

    return null;
  }

  getDownloadVideoId(status = {}) {
    return this._extractVideoId(status.fileId)
      || this._extractVideoId(status.embedUrl)
      || this._extractVideoId(status.url)
      || null;
  }

  getDownloadUrlCandidates(status = {}) {
    const videoId = this.getDownloadVideoId(status);
    if (!videoId) return [];

    const primaryBaseUrl = process.env.SEEK_DOWNLOAD_BASE_URL || 'https://suo.emergingtechhubonline.store';
    const primaryVariantPath = process.env.SEEK_DOWNLOAD_VARIANT_PATH || 'v4/k5';
    const playlistName = process.env.SEEK_DOWNLOAD_PLAYLIST || 'index-f1-v1-a1.txt';

    const fallbackBaseUrls = String(
      process.env.SEEK_DOWNLOAD_FALLBACK_BASE_URLS || 'https://s9m.technologyevolution.space'
    )
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean);

    const fallbackVariantPaths = String(
      process.env.SEEK_DOWNLOAD_FALLBACK_VARIANT_PATHS || 'v4/us'
    )
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean);

    const baseUrls = [primaryBaseUrl, ...fallbackBaseUrls]
      .map((value) => value.replace(/\/+$/, ''))
      .filter(Boolean);

    const variantPaths = [primaryVariantPath, ...fallbackVariantPaths]
      .map((value) => value.replace(/^\/+/, '').replace(/\/+$/, ''))
      .filter(Boolean);

    const candidates = [];
    for (const baseUrl of baseUrls) {
      for (const variantPath of variantPaths) {
        candidates.push(`${baseUrl}/${variantPath}/${videoId}/${playlistName}`);
      }
    }

    return [...new Set(candidates)];
  }

  getDownloadUrl(status = {}) {
    return this.getDownloadUrlCandidates(status)[0] || null;
  }

  getDownloadHeaders() {
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      Referer: 'https://seekstream.embedseek.com/',
      Origin: 'https://seekstream.embedseek.com'
    };

    const cookie = String(process.env.SEEK_DOWNLOAD_COOKIE || '').trim();
    if (cookie) {
      headers.Cookie = cookie;
    }

    return headers;
  }

  _mapVideoInfo(fileInfo, fallbackName = null, fallbackFileId = null) {
    const resolvedFileId = fileInfo.id || fallbackFileId;
    const urls = this._buildVideoUrls(resolvedFileId);
    
    // Try explicit URL fields first, then fall back to constructed URLs
    const resolvedUrl = fileInfo.player_url || fileInfo.play_url || fileInfo.video_url || urls.playerUrl;
    const resolvedEmbedUrl = fileInfo.iframeApi || fileInfo.embed_url || fileInfo.iframe_url || fileInfo.embed || urls.embedUrl;

    console.log(`[SeekStreaming] _buildVideoUrls returned:`, urls);
    console.log(`[SeekStreaming] Mapped video info: id=${resolvedFileId}, url=${resolvedUrl}, embedUrl=${resolvedEmbedUrl}`);

    return {
      url: resolvedUrl,
      fileId: resolvedFileId,
      provider: 'seekstreaming',
      size: fileInfo.size || null,
      embedUrl: resolvedEmbedUrl,
      downloadUrl: fileInfo.download_url || null,
      thumbnail: fileInfo.thumbnail_url || fileInfo.poster || null,
      title: fileInfo.name || fileInfo.title || fallbackName,
      status: fileInfo.status || null,
      raw: fileInfo
    };
  }

  /**
   * Upload file to SeekStreaming
   * Uses TUS protocol for resumable uploads
   */
  async upload(filePath, fileName, onProgress, signal) {
    if (!this.apiKey) {
      throw new Error('SeekStreaming API key not configured');
    }

    const fileSize = fs.statSync(filePath).size;
    const maxFileSize = 20 * 1024 * 1024 * 1024; // 20GB

    if (fileSize > maxFileSize) {
      throw new Error(`File size ${fileSize} bytes exceeds SeekStreaming limit of ${maxFileSize} bytes (20GB)`);
    }

    try {
      // Step 1: Get upload endpoints from SeekStreaming
      const uploadInfo = await this._getUploadEndpoints(fileName, 'video/mp4', signal);

      // Step 2: Upload using TUS protocol
      const uploadResult = await this._uploadViaTus(
        uploadInfo.tusUrl,
        uploadInfo.accessToken,
        filePath,
        fileName,
        'video/mp4',
        onProgress,
        signal
      );

      // Step 3: Get file info (by looking up filename in recent uploads)
      const fileInfo = await this._waitForUploadedVideo(uploadResult.fileId, fileName, signal);

      if (!fileInfo) {
        throw new Error(`File ${fileName} not found after upload. The file may still be processing by SeekStreaming.`);
      }

      return {
        ...this._mapVideoInfo(fileInfo, fileName, uploadResult.fileId),
        size: fileSize
      };
    } catch (error) {
      if (signal?.aborted) {
        throw new Error('Upload cancelled');
      }
      if (error.code === 'ENOTFOUND') {
        throw new Error(`SeekStreaming host not found. Check SEEK_API_BASE_URL (current: ${this.baseUrl})`);
      }
      throw error;
    }
  }

  /**
   * Get TUS upload endpoints from SeekStreaming
   */
  async _getUploadEndpoints(fileName, fileType, signal) {
    const response = await axios.get(
      `${this.baseUrl}/api/v1/video/upload`,
      {
        headers: this._getApiHeaders(),
        signal
      }
    );

    if (!response.data?.tusUrl || !response.data?.accessToken) {
      throw new Error('Failed to get upload endpoints from SeekStreaming');
    }

    return response.data;
  }

  /**
   * Upload file using TUS protocol
   * TUS Protocol: https://tus.io/protocols/resumable-upload.html
   * 
   * Flow:
   * 1. POST to tusUrl to create upload -> returns Location header with upload URL
   * 2. PATCH to upload URL with chunks
   * 3. HEAD to check upload status (for resume)
   */
  async _uploadViaTus(tusUrl, accessToken, filePath, fileName, fileType, onProgress, signal) {
    const fileSize = fs.statSync(filePath).size;
    const uploadMetadata = this._buildTusMetadata(fileName, fileType, accessToken);
    const tusCollectionUrl = this._normalizeUploadEndpoint(tusUrl);

    let uploadUrl;

    // Step 1: Create new upload via POST
    // TUS spec: POST creates a new upload resource
    console.log(`[SeekStreaming] Creating TUS upload for ${fileName} (${fileSize} bytes)`);

    const createResponse = await axios.post(tusCollectionUrl, null, {
      headers: this._getTusHeaders(uploadMetadata, {
        'Upload-Length': String(fileSize)
      }),
      validateStatus: (status) => status >= 200 && status < 300 || status === 201,
      maxRedirects: 0,
      signal
    });

    // Get upload URL from Location header
    uploadUrl = createResponse.headers['location'];
    
    if (!uploadUrl) {
      // If no Location header, the upload URL might be the response data
      throw new Error('TUS server did not return a Location header with upload URL');
    }

    // Handle relative URLs
    if (uploadUrl.startsWith('/')) {
      const baseUrlParsed = new URL(tusCollectionUrl);
      uploadUrl = `${baseUrlParsed.origin}${uploadUrl}`;
    }

    console.log(`[SeekStreaming] TUS upload URL: ${uploadUrl}`);

    // Step 2: Upload file in chunks using PATCH
    const totalChunks = Math.ceil(fileSize / this.chunkSize);

    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      const chunkStart = chunkIndex * this.chunkSize;
      const chunkEnd = Math.min(chunkStart + this.chunkSize - 1, fileSize - 1);
      const chunkSize = chunkEnd - chunkStart + 1;

      // Read only the chunk we need
      const fd = fs.openSync(filePath, 'r');
      const chunkData = Buffer.allocUnsafe(chunkSize);
      fs.readSync(fd, chunkData, 0, chunkSize, chunkStart);
      fs.closeSync(fd);

      await axios.patch(uploadUrl, chunkData, {
        headers: this._getTusHeaders(uploadMetadata, {
          'Content-Type': 'application/offset+octet-stream',
          'Content-Length': String(chunkSize),
          'Upload-Offset': String(chunkStart),
        }),
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        signal
      });

      const progress = Math.round(((chunkEnd + 1) / fileSize) * 100);
      if (onProgress) {
        onProgress(progress);
      }

      console.log(`[SeekStreaming] Uploaded chunk ${chunkIndex + 1}/${totalChunks} (${progress}%)`);
    }

    console.log(`[SeekStreaming] Upload completed for ${fileName}`);
    
    // Extract file ID from upload URL (format varies by server)
    // Common patterns: /files/{id}, /uploads/{id}, etc.
    const fileIdMatch = uploadUrl.match(/\/([a-f0-9-]{36}|[a-zA-Z0-9_-]{20,})\/?$/);
    return {
      fileId: fileIdMatch ? fileIdMatch[1] : null,
      uploadUrl
    };
  }

  /**
   * Get file information
   */
  async _getFileInfo(fileId) {
    const response = await axios.get(
      `${this.baseUrl}/api/v1/video/manage/${fileId}`,
      {
        headers: this._getApiHeaders()
      }
    );

    return response.data.data || response.data;
  }

  /**
   * Delete file from SeekStreaming
   */
  async delete(fileId) {
    if (!this.apiKey) {
      throw new Error('SeekStreaming API key not configured');
    }

    await axios.delete(
      `${this.baseUrl}/api/v1/video/manage/${fileId}`,
      {
        headers: this._getApiHeaders()
      }
    );

    return { deleted: true };
  }

  /**
   * Get file info
   */
  async getFileInfo(fileId) {
    if (!this.apiKey) {
      throw new Error('SeekStreaming API key not configured');
    }

    const response = await axios.get(
      `${this.baseUrl}/api/v1/video/manage/${fileId}`,
      {
        headers: this._getApiHeaders()
      }
    );

    return response.data.data || response.data;
  }

  /**
   * List all files
   */
  async listFiles(page = 1, limit = 20, signal) {
    if (!this.apiKey) {
      throw new Error('SeekStreaming API key not configured');
    }

    const response = await axios.get(
      `${this.baseUrl}/api/v1/video/manage`,
      {
        headers: this._getApiHeaders(),
        params: { page, perPage: limit },
        signal
      }
    );

    return response.data.data || response.data;
  }

  /**
   * Check if file exists
   */
  async checkFile(fileId) {
    try {
      const info = await this.getFileInfo(fileId);
      return { exists: true, info };
    } catch (error) {
      if (error.response?.status === 404) {
        return { exists: false, error: error.message };
      }
      return { exists: false, error: error.message };
    }
  }

  /**
   * Find file by filename (after upload)
   * Since TUS doesn't return file ID, we need to look it up
   */
  async _findFileByFilename(fileName, signal, maxRetries = 10, retryDelay = 2500) {
    // Strip extension for more flexible matching
    const baseNameLower = fileName.replace(/\.[^/.]+$/, '').toLowerCase();
    const fileNameLower = fileName.toLowerCase();

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const files = await this.listFiles(1, 100, signal);
        
        if (!Array.isArray(files)) {
          console.log(`[SeekStreaming] Unexpected listFiles response:`, typeof files);
          continue;
        }

        // Find file matching filename (exact match, base name match, or contains match)
        const fileInfo = files.find(f => {
          const serverName = (f.name || f.title || '').toLowerCase();
          const serverBaseName = serverName.replace(/\.[^/.]+$/, '');
          
          // Exact match
          if (serverName === fileNameLower) return true;
          // Base name exact match (without extension)
          if (serverBaseName === baseNameLower) return true;
          // Contains match (server name contains our filename or vice versa)
          if (serverName.includes(baseNameLower) || baseNameLower.includes(serverBaseName)) return true;
          
          return false;
        });

        if (fileInfo) {
          console.log(`[SeekStreaming] Found video: id=${fileInfo.id}, name=${fileInfo.name || fileInfo.title}`);
          return fileInfo;
        }

        if (attempt < maxRetries - 1) {
          const waitTime = retryDelay * Math.min(attempt + 1, 4); // Cap multiplier at 4
          console.log(`[SeekStreaming] Video not yet found, retrying in ${waitTime}ms... (attempt ${attempt + 1}/${maxRetries})`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }
      } catch (error) {
        if (error.code === 'ABORT_ERR' || error.code === 'ECONNABORTED' || signal?.aborted) {
          throw error;
        }
        console.log(`[SeekStreaming] Error listing files (attempt ${attempt + 1}):`, error.message);
        // Continue retries on other errors
      }
    }

    throw new Error(`Timeout: Could not find file "${fileName}" after ${maxRetries} retries. The file may still be processing by SeekStreaming.`);
  }

  /**
   * Wait for uploaded video to become available
   * Note: TUS upload ID is NOT the same as SeekStreaming video ID.
   * The TUS upload ID (from Location header) is temporary; we must search by filename.
   */
  async _waitForUploadedVideo(fileId, fileName, signal, maxRetries = 10, retryDelay = 2500) {
    // TUS upload IDs are typically hex strings (32 chars) - these are NOT valid video IDs
    // SeekStreaming video IDs have a different format, so we skip direct lookup
    // and always search by filename in the video list
    console.log(`[SeekStreaming] Waiting for video "${fileName}" to appear in account...`);
    
    return this._findFileByFilename(fileName, signal, maxRetries, retryDelay);
  }

  /**
   * Check provider status and configuration
   */
  async checkStatus() {
    const status = {
      name: 'seekstreaming',
      configured: false,
      authenticated: false,
      message: ''
    };

    if (!this.apiKey) {
      status.message = 'Missing API key (SEEK_API_KEY)';
      return status;
    }

    status.configured = true;

    try {
      // Test API connection by trying to get upload endpoints
      const response = await axios.get(
        `${this.baseUrl}/api/v1/video/upload`,
        {
          headers: this._getApiHeaders(),
          timeout: 10000
        }
      );

      status.authenticated = response.status === 200;
      status.message = response.data?.tusUrl
        ? 'Connected successfully'
        : 'Connected, but upload endpoint returned unexpected payload';
    } catch (error) {
      status.authenticated = false;
      if (error.code === 'ENOTFOUND') {
        status.message = `Host not found. Check SEEK_API_BASE_URL (current: ${this.baseUrl})`;
      } else if (error.response?.status === 401) {
        status.message = 'Invalid API key';
      } else {
        status.message = `Connection error: ${error.response?.data?.message || error.message}`;
      }
    }

    return status;
  }
}

module.exports = { SeekStreamingAdapter };
