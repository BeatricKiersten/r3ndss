/**
 * Backblaze Adapter (Rclone-backed)
 *
 * Uses rclone as the core engine so users can point Backblaze uploads
 * (and other compatible remotes) via configurable remotes/sync profiles.
 */

const fs = require('fs');
const fsExtra = require('fs-extra');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

class BackblazeAdapter {
  constructor(options = {}) {
    this.db = options.db;
    this.providerName = 'backblaze';
    this.rcloneBin = process.env.RCLONE_BIN || 'rclone';
  }

  _safeIniValue(value) {
    return String(value ?? '').replace(/[\r\n]/g, ' ').trim();
  }

  _joinRemotePath(basePath, fileName) {
    const normalizedBase = String(basePath || '')
      .replace(/^\/+/, '')
      .replace(/\/+$/, '');
    const normalizedName = String(fileName || '').replace(/^\/+/, '');
    return normalizedBase ? `${normalizedBase}/${normalizedName}` : normalizedName;
  }

  _buildPublicUrl(profile, remotePath) {
    const base = String(profile.publicBaseUrl || '').trim();
    if (!base) {
      return `${profile.remoteName}:${remotePath}`;
    }

    const encodedPath = remotePath
      .split('/')
      .filter(Boolean)
      .map((segment) => encodeURIComponent(segment))
      .join('/');

    return `${base.replace(/\/+$/, '')}/${encodedPath}`;
  }

  async _getRcloneConfig() {
    if (!this.db || typeof this.db.getRcloneConfig !== 'function') {
      throw new Error('Database handler is required for rclone-backed Backblaze adapter');
    }

    const providerConfigs = await this.db.getProviderConfigs();
    const rclone = await this.db.getRcloneConfig();
    const remotes = Array.isArray(rclone.remotes) ? rclone.remotes : [];
    const profiles = Array.isArray(rclone.syncProfiles) ? rclone.syncProfiles : [];

    const providerProfileId = providerConfigs?.[this.providerName]?.config?.profileId || null;
    const selectedProfileId = providerProfileId || rclone.defaultProfileId || null;

    let profile = profiles.find((item) => item.id === selectedProfileId && item.enabled !== false);
    if (!profile) {
      profile = profiles.find((item) => item.provider === this.providerName && item.enabled !== false);
    }

    if (!profile) {
      throw new Error('No active rclone sync profile configured for backblaze provider');
    }

    const remote = remotes.find((item) => item.name === profile.remoteName);
    if (!remote) {
      throw new Error(`Remote '${profile.remoteName}' from selected profile is not defined`);
    }

    return { profile, remote, remotes, profiles };
  }

  async _withTempRcloneConfig(runFn) {
    const { remotes } = await this._getRcloneConfig();

    const configLines = [];
    for (const remote of remotes) {
      configLines.push(`[${remote.name}]`);
      configLines.push(`type = ${this._safeIniValue(remote.type)}`);

      const parameters = remote.parameters && typeof remote.parameters === 'object'
        ? remote.parameters
        : {};

      for (const [key, value] of Object.entries(parameters)) {
        if (value === undefined || value === null || value === '') continue;
        configLines.push(`${key} = ${this._safeIniValue(value)}`);
      }

      configLines.push('');
    }

    const tmpDir = await fsExtra.mkdtemp(path.join(os.tmpdir(), 'zenius-rclone-'));
    const configPath = path.join(tmpDir, 'rclone.conf');
    await fsExtra.writeFile(configPath, `${configLines.join('\n')}\n`, 'utf8');

    try {
      return await runFn(configPath);
    } finally {
      await fsExtra.remove(tmpDir).catch(() => {});
    }
  }

  _runRclone(args, { signal } = {}) {
    return new Promise((resolve, reject) => {
      const proc = spawn(this.rcloneBin, args, {
        stdio: ['ignore', 'pipe', 'pipe']
      });

      let stdout = '';
      let stderr = '';

      const onAbort = () => {
        try {
          proc.kill('SIGTERM');
        } catch (_) {
          // noop
        }
      };

      if (signal) {
        signal.addEventListener('abort', onAbort, { once: true });
      }

      proc.stdout.on('data', (chunk) => {
        stdout += chunk.toString();
      });

      proc.stderr.on('data', (chunk) => {
        stderr += chunk.toString();
      });

      proc.on('error', (error) => {
        if (signal) signal.removeEventListener('abort', onAbort);
        reject(error);
      });

      proc.on('close', (code) => {
        if (signal) signal.removeEventListener('abort', onAbort);

        if (code !== 0) {
          reject(new Error((stderr || stdout || `rclone exited with code ${code}`).trim()));
          return;
        }

        resolve({ stdout, stderr });
      });
    });
  }

  async upload(filePath, fileName, onProgress, signal) {
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }

    const fileSize = fs.statSync(filePath).size;
    const { profile } = await this._getRcloneConfig();

    if (onProgress) onProgress(10);

    return this._withTempRcloneConfig(async (configPath) => {
      const remotePath = this._joinRemotePath(profile.destinationPath, fileName);
      const destination = `${profile.remoteName}:${remotePath}`;

      await this._runRclone([
        'copyto',
        filePath,
        destination,
        '--config',
        configPath,
        '--stats=1s',
        '--stats-one-line',
        '--retries=2'
      ], { signal });

      if (onProgress) onProgress(100);

      const url = this._buildPublicUrl(profile, remotePath);

      return {
        url,
        fileId: destination,
        provider: this.providerName,
        size: fileSize,
        remotePath,
        profileId: profile.id
      };
    });
  }

  async delete(fileId) {
    if (!fileId || !String(fileId).includes(':')) {
      throw new Error('Invalid remote file identifier');
    }

    return this._withTempRcloneConfig(async (configPath) => {
      await this._runRclone(['deletefile', String(fileId), '--config', configPath]);
      return { deleted: true };
    });
  }

  async checkFile(fileId) {
    if (!fileId || !String(fileId).includes(':')) {
      return { exists: false, error: 'Invalid remote file identifier' };
    }

    try {
      return await this._withTempRcloneConfig(async (configPath) => {
        const result = await this._runRclone(['lsf', String(fileId), '--config', configPath]);
        return { exists: result.stdout.trim().length > 0, info: { listing: result.stdout.trim() } };
      });
    } catch (error) {
      return { exists: false, error: error.message };
    }
  }

  async checkStatus() {
    const status = {
      name: this.providerName,
      configured: false,
      authenticated: false,
      message: ''
    };

    try {
      const { profile } = await this._getRcloneConfig();
      status.configured = true;

      await this._withTempRcloneConfig(async (configPath) => {
        await this._runRclone(['lsf', `${profile.remoteName}:`, '--max-depth', '0', '--config', configPath]);
      });

      status.authenticated = true;
      status.message = `Rclone connected using profile '${profile.name}' (${profile.remoteName})`;
      return status;
    } catch (error) {
      status.message = error.message;
      return status;
    }
  }
}

module.exports = { BackblazeAdapter };
