const { spawn } = require('child_process');
const fsExtra = require('fs-extra');
const os = require('os');
const path = require('path');

class RcloneServeService {
  constructor(db) {
    this.db = db;
    this.rcloneBin = process.env.RCLONE_BIN || 'rclone';
    this.bindHost = process.env.RCLONE_SERVE_BIND_HOST || '127.0.0.1';
    this.port = Number(process.env.RCLONE_SERVE_PORT || 8080);
    this.proc = null;
    this.tmpDir = null;
    this.configPath = null;
    this.currentRemoteName = null;
  }

  _safeIniValue(value) {
    return String(value ?? '').replace(/[\r\n]/g, ' ').trim();
  }

  async _buildTempConfig() {
    const rclone = await this.db.getRcloneConfig();
    const remotes = Array.isArray(rclone?.remotes) ? rclone.remotes : [];
    const lines = [];

    for (const remote of remotes) {
      lines.push(`[${remote.name}]`);
      lines.push(`type = ${this._safeIniValue(remote.type)}`);
      const parameters = remote.parameters && typeof remote.parameters === 'object' ? remote.parameters : {};
      for (const [key, value] of Object.entries(parameters)) {
        if (value === undefined || value === null || value === '') continue;
        lines.push(`${key} = ${this._safeIniValue(value)}`);
      }
      lines.push('');
    }

    this.tmpDir = await fsExtra.mkdtemp(path.join(os.tmpdir(), 'zenius-rclone-serve-'));
    this.configPath = path.join(this.tmpDir, 'rclone.conf');
    await fsExtra.writeFile(this.configPath, `${lines.join('\n')}\n`, 'utf8');
  }

  getLocalBaseUrl() {
    return `http://${this.bindHost}:${this.port}`;
  }

  async start(remoteName) {
    const normalizedRemoteName = String(remoteName || '').trim();
    if (!normalizedRemoteName) return null;

    if (this.proc && this.currentRemoteName === normalizedRemoteName) {
      return this.getLocalBaseUrl();
    }

    await this.stop();
    await this._buildTempConfig();

    this.proc = spawn(this.rcloneBin, [
      'serve',
      'http',
      `${normalizedRemoteName}:`,
      '--addr',
      `${this.bindHost}:${this.port}`,
      '--config',
      this.configPath,
      '--no-modtime'
    ], {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    this.currentRemoteName = normalizedRemoteName;

    this.proc.stdout.on('data', (chunk) => {
      const text = chunk.toString().trim();
      if (text) console.log(`[RcloneServe] ${text}`);
    });

    this.proc.stderr.on('data', (chunk) => {
      const text = chunk.toString().trim();
      if (text) console.log(`[RcloneServe] ${text}`);
    });

    this.proc.on('exit', async () => {
      this.proc = null;
      this.currentRemoteName = null;
      if (this.tmpDir) {
        await fsExtra.remove(this.tmpDir).catch(() => {});
        this.tmpDir = null;
        this.configPath = null;
      }
    });

    return this.getLocalBaseUrl();
  }

  async ensureDefaultGoogleDriveRemote() {
    const rclone = await this.db.getRcloneConfig();
    const profiles = Array.isArray(rclone?.syncProfiles) ? rclone.syncProfiles : [];
    const remotes = Array.isArray(rclone?.remotes) ? rclone.remotes : [];
    const defaultProfileId = String(rclone?.defaultProfileId || '').trim();
    const profile = profiles.find((item) => item.id === defaultProfileId && item.enabled !== false)
      || profiles.find((item) => item.enabled !== false)
      || null;

    if (!profile?.remoteName) {
      return null;
    }

    const remote = remotes.find((item) => item.name === profile.remoteName) || null;
    if (!remote || String(remote.type || '').trim().toLowerCase() !== 'drive') {
      return null;
    }

    return this.start(profile.remoteName);
  }

  async stop() {
    if (this.proc) {
      const proc = this.proc;
      this.proc = null;
      this.currentRemoteName = null;
      await new Promise((resolve) => {
        const timer = setTimeout(() => {
          try {
            proc.kill('SIGKILL');
          } catch (_) {}
          resolve();
        }, 5000);

        proc.once('exit', () => {
          clearTimeout(timer);
          resolve();
        });

        try {
          proc.kill('SIGTERM');
        } catch (_) {
          clearTimeout(timer);
          resolve();
        }
      });
    }

    if (this.tmpDir) {
      await fsExtra.remove(this.tmpDir).catch(() => {});
      this.tmpDir = null;
      this.configPath = null;
    }
  }
}

module.exports = { RcloneServeService };
