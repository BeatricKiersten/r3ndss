/**
 * Catbox.moe Adapter
 *
 * Anonymous upload only — no user hash.
 * Docs: https://catbox.moe/tools.php
 */

const fs = require('fs');
const { spawn } = require('child_process');
const axios = require('axios');

class CatboxAdapter {
  constructor() {
    this.apiUrl = 'https://catbox.moe/user/api.php';
    this.maxAvailabilityChecks = Number(process.env.CATBOX_MAX_AVAILABILITY_CHECKS || 8);
    this.availabilityBaseDelayMs = Number(process.env.CATBOX_AVAILABILITY_BASE_DELAY_MS || 2000);
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  _normalizeCatboxUrl(fileUrlOrId) {
    const value = String(fileUrlOrId || '').trim();
    if (!value) return null;

    if (value.startsWith('http://') || value.startsWith('https://')) {
      return value;
    }

    return `https://files.catbox.moe/${value.replace(/^\/+/, '')}`;
  }

  _looksLikeMediaResponse(status, headers = {}) {
    if (!(status === 200 || status === 206)) return false;

    const contentType = String(headers['content-type'] || '').toLowerCase();
    if (!contentType) return true;

    if (contentType.includes('text/html')) return false;
    return true;
  }

  async _probeAvailability(fileUrlOrId) {
    const normalizedUrl = this._normalizeCatboxUrl(fileUrlOrId);
    if (!normalizedUrl || !normalizedUrl.startsWith('https://files.catbox.moe/')) {
      return { exists: false, status: 0, reason: 'invalid-url' };
    }

    try {
      const response = await axios.head(normalizedUrl, {
        timeout: 10000,
        validateStatus: () => true,
        headers: {
          'user-agent': 'Mozilla/5.0',
          referer: 'https://catbox.moe/'
        }
      });

      if (this._looksLikeMediaResponse(response.status, response.headers)) {
        return { exists: true, status: response.status, url: normalizedUrl };
      }
    } catch (_) {
      // Fallback to GET range probe.
    }

    try {
      const response = await axios.get(normalizedUrl, {
        timeout: 10000,
        headers: {
          Range: 'bytes=0-1',
          'user-agent': 'Mozilla/5.0',
          referer: 'https://catbox.moe/'
        },
        responseType: 'stream',
        validateStatus: () => true
      });

      if (response.data?.destroy) {
        response.data.destroy();
      }

      const exists = this._looksLikeMediaResponse(response.status, response.headers);
      return { exists, status: response.status, url: normalizedUrl };
    } catch (error) {
      return {
        exists: false,
        status: 0,
        url: normalizedUrl,
        reason: error.message
      };
    }
  }

  async _waitUntilAvailable(fileUrlOrId) {
    for (let attempt = 1; attempt <= this.maxAvailabilityChecks; attempt++) {
      const probe = await this._probeAvailability(fileUrlOrId);
      if (probe.exists) {
        return probe;
      }

      if (attempt < this.maxAvailabilityChecks) {
        await this._sleep(this.availabilityBaseDelayMs * attempt);
      }
    }

    return { exists: false };
  }

  async _uploadViaCurl(filePath, signal) {
    const args = [
      '-sS', '-X', 'POST',
      '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
      '-F', 'reqtype=fileupload',
      '-F', `fileToUpload=@${filePath}`,
      this.apiUrl
    ];

    console.log('[Catbox] Upload args:', args.filter(arg => !arg.includes('User-Agent'))); // Don't log user agent for security

    return new Promise((resolve, reject) => {
      const proc = spawn('curl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
      let stdout = '';
      let stderr = '';

      const onAbort = () => { try { proc.kill('SIGTERM'); } catch (_) {} };
      if (signal) signal.addEventListener('abort', onAbort, { once: true });

      proc.stdout.on('data', (d) => { stdout += d.toString(); });
      proc.stderr.on('data', (d) => { stderr += d.toString(); });

      proc.on('error', reject);

      proc.on('close', (code) => {
        if (signal) signal.removeEventListener('abort', onAbort);

        console.log(`[Catbox] Curl exit code: ${code}`);
        console.log(`[Catbox] Curl stdout: ${stdout}`);
        console.log(`[Catbox] Curl stderr: ${stderr}`);

        if (code !== 0) {
          return reject(new Error((stderr || `curl exited with code ${code}`).trim()));
        }

        const fileUrl = stdout.trim();

        // Check for common error responses
        if (fileUrl.includes('error') || fileUrl.includes('Error') || fileUrl.includes('403') || fileUrl.includes('502')) {
          return reject(new Error(`Catbox upload failed: ${fileUrl}`));
        }

        if (!fileUrl.startsWith('http')) {
          return reject(new Error(`Catbox upload failed: ${fileUrl || 'empty response'}`));
        }

        resolve(fileUrl);
      });
    });
  }

  async upload(filePath, fileName, onProgress, signal) {
    const fileSize = fs.statSync(filePath).size;

    if (onProgress) onProgress(5);

    try {
      const fileUrl = await this._uploadViaCurl(filePath, signal);

      if (onProgress) onProgress(90);

      const availability = await this._waitUntilAvailable(fileUrl);
      if (!availability.exists) {
        throw new Error('Catbox URL not yet available after upload propagation checks');
      }

      if (onProgress) onProgress(100);

      return {
        url: fileUrl,
        fileId: fileUrl.split('/').pop(),
        provider: 'catbox',
        size: fileSize
      };
    } catch (error) {
      const msg = String(error.message || '');
      if (msg.includes('412') || msg.includes('Precondition')) {
        throw new Error('Catbox rejected upload (412). Possible WAF/rate-limit/IP block.');
      }
      throw error;
    }
  }

  async checkFile(fileUrl) {
    const probe = await this._probeAvailability(fileUrl);
    return probe.exists;
  }

  async delete(_fileId) {
    throw new Error('Catbox anonymous uploads cannot be deleted');
  }

  async checkStatus() {
    return {
      name: 'catbox',
      configured: true,
      authenticated: false,
      message: 'Anonymous mode'
    };
  }
}

module.exports = { CatboxAdapter };
