/**
 * Voe.sx Adapter
 * 
 * Adapter for Voe.sx video hosting
 * Docs: https://voesxapi.docs.apiary.io/
 */

const fs = require('fs');
const path = require('path');
const FormData = require('form-data');
const axios = require('axios');
const { attachHttpLogger } = require('../utils/httpLogger');

const http = attachHttpLogger(axios.create({ timeout: 30000 }), 'voesx');

class VoeSxAdapter {
  constructor() {
    this.apiKey = process.env.VOE_API_KEY;
    this.baseUrl = 'https://voe.sx/api';
  }

  _isSuccessResponse(payload) {
    return payload?.success === true || payload?.status === 'success' || Number(payload?.status) === 200;
  }

  _extractMessage(payload, fallback) {
    return payload?.msg || payload?.message || fallback;
  }

  _normalizeFileName(fileName, filePath) {
    const extension = path.extname(fileName || '') || path.extname(filePath || '') || '.mp4';
    const baseName = path.basename(fileName || `upload${extension}`, path.extname(fileName || ''))
      .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
      .trim() || 'upload';

    return `${baseName}${extension}`;
  }

  _buildFileUrls(fileCode) {
    if (!fileCode) {
      return { url: null, embedUrl: null };
    }

    const embedUrl = `https://voe.sx/e/${fileCode}`;
    return {
      url: embedUrl,
      embedUrl
    };
  }

  /**
   * Upload file to Voe.sx
   */
  async upload(filePath, fileName, onProgress, signal) {
    if (!this.apiKey) {
      throw new Error('Voe.sx API key not configured');
    }

    const fileSize = fs.statSync(filePath).size;
    const normalizedFileName = this._normalizeFileName(fileName, filePath);
    const form = new FormData();
    
    form.append('key', this.apiKey);
    form.append('file', fs.createReadStream(filePath), {
      filename: normalizedFileName,
      contentType: 'video/mp4'
    });

    // Get upload server
    const serverResponse = await http.get(`${this.baseUrl}/upload/server`, {
      params: { key: this.apiKey },
      signal
    });

    if (!this._isSuccessResponse(serverResponse.data) || !serverResponse.data?.result) {
      throw new Error(this._extractMessage(serverResponse.data, 'Failed to get Voe.sx upload server'));
    }

    const uploadUrl = serverResponse.data.result;

    // Upload file
    const uploadResponse = await http.post(uploadUrl, form, {
      headers: {
        ...form.getHeaders(),
        Accept: 'application/json'
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      onUploadProgress: (progressEvent) => {
        const total = progressEvent.total || fileSize;
        const progress = total > 0 ? Math.round((progressEvent.loaded * 100) / total) : 0;
        if (onProgress) onProgress(progress);
      },
      signal
    });

    if (!this._isSuccessResponse(uploadResponse.data)) {
      throw new Error(`Voe.sx upload failed: ${this._extractMessage(uploadResponse.data, 'Unknown error')} (filename: ${normalizedFileName})`);
    }

    const uploadedFile = uploadResponse.data?.file || uploadResponse.data?.result?.file || null;
    const fileCode = uploadedFile?.file_code || uploadResponse.data?.result?.filecode || null;

    if (!fileCode) {
      throw new Error(`Voe.sx upload succeeded but file_code is missing (filename: ${normalizedFileName})`);
    }

    const urls = this._buildFileUrls(fileCode);
    
    return {
      url: urls.url,
      fileId: fileCode,
      provider: 'voesx',
      size: fileSize,
      embedUrl: urls.embedUrl,
      title: uploadedFile?.file_title || normalizedFileName,
      remoteId: uploadedFile?.id || null,
      encodingNecessary: uploadedFile?.encoding_necessary ?? null
    };
  }

  /**
   * Delete file from Voe.sx
   */
  async delete(fileCode) {
    if (!this.apiKey) {
      throw new Error('Voe.sx API key not configured');
    }

    const response = await http.get(`${this.baseUrl}/file/delete`, {
      params: {
        key: this.apiKey,
        del_code: fileCode
      }
    });

    if (!this._isSuccessResponse(response.data)) {
      throw new Error(`Voe.sx delete failed: ${this._extractMessage(response.data, 'Unknown error')}`);
    }

    return { deleted: true };
  }

  /**
   * Get file info
   */
  async getFileInfo(fileCode) {
    if (!this.apiKey) {
      throw new Error('Voe.sx API key not configured');
    }

    const response = await http.get(`${this.baseUrl}/file/info`, {
      params: {
        key: this.apiKey,
        file_code: fileCode
      }
    });

    return response.data;
  }

  /**
   * Check if file exists
   */
  async checkFile(fileCode) {
    try {
      const info = await this.getFileInfo(fileCode);
      return { exists: this._isSuccessResponse(info), info: info.result };
    } catch (error) {
      return { exists: false, error: error.message };
    }
  }

  /**
   * Check provider status and configuration
   */
  async checkStatus() {
    const status = {
      name: 'voesx',
      configured: false,
      authenticated: false,
      message: ''
    };

    if (!this.apiKey) {
      status.message = 'Missing API key (VOE_API_KEY)';
      return status;
    }

    status.configured = true;

    try {
      // Test API connection by getting account info
      const response = await http.get(`${this.baseUrl}/account/info`, {
        params: { key: this.apiKey },
        timeout: 10000
      });

      if (this._isSuccessResponse(response.data)) {
        status.authenticated = true;
        const account = response.data.result;
        status.message = `Connected as ${account.email || 'authenticated user'}`;
        status.accountInfo = {
          email: account.email,
          balance: account.balance,
          storageUsed: account.storage_used
        };
      } else {
        status.message = `API error: ${this._extractMessage(response.data, 'Unknown error')}`;
      }
    } catch (error) {
      status.message = `Connection error: ${error.response?.data?.msg || error.message}`;
    }

    return status;
  }
}

module.exports = { VoeSxAdapter };
