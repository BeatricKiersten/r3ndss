/**
 * MySQL Database Handler
 *
 * Persists critical application data in MySQL:
 * - folders, files, jobs
 * - provider states/configs/check snapshots
 * - rclone remotes/profiles/system state
 *
 * Non-critical logs are intentionally kept lightweight (console / in-memory),
 * not persisted in this database layer.
 */

const mysql = require('mysql2/promise');
const { v4: uuidv4 } = require('uuid');
const {
  STATIC_PROVIDER_DEFS,
  LEGACY_RCLONE_PROVIDER_ID,
  getStaticProviderIds,
  buildRcloneProfileProviderId,
  isKnownProviderId,
  isRcloneProfileProviderId,
  parseRcloneProfileId
} = require('../services/providerRegistry');

const STATIC_PROVIDER_IDS = getStaticProviderIds();

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBool(value) {
  return value === true || value === 1 || value === '1';
}

function parseJson(value, fallback) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }

  if (typeof value === 'object') {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function stringifyJson(value) {
  return JSON.stringify(value === undefined ? null : value);
}

function buildMysqlConfigFromEnv() {
  const config = {
    host: process.env.MYSQL_HOST || '127.0.0.1',
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'zenius',
    connectionLimit: Number(process.env.MYSQL_CONNECTION_LIMIT || 25),
    autoCreateDatabase: String(process.env.MYSQL_AUTO_CREATE_DATABASE || 'true').toLowerCase() !== 'false',
    ssl: String(process.env.MYSQL_SSL || 'false').toLowerCase() === 'true',
    sslRejectUnauthorized: String(process.env.MYSQL_SSL_REJECT_UNAUTHORIZED || 'true').toLowerCase() !== 'false'
  };

  const mysqlUrl = process.env.MYSQL_URL || process.env.DATABASE_URL;
  if (!mysqlUrl) {
    return config;
  }

  const parsed = new URL(mysqlUrl);
  if (!['mysql:', 'mysql2:'].includes(parsed.protocol)) {
    throw new Error('MYSQL_URL must use mysql:// or mysql2:// protocol');
  }

  config.host = parsed.hostname || config.host;
  config.port = Number(parsed.port || config.port);
  config.user = decodeURIComponent(parsed.username || config.user);
  config.password = decodeURIComponent(parsed.password || config.password);

  const dbName = decodeURIComponent((parsed.pathname || '').replace(/^\//, ''));
  if (dbName) {
    config.database = dbName;
  }

  const sslMode = String(
    parsed.searchParams.get('ssl-mode') ||
    parsed.searchParams.get('sslmode') ||
    parsed.searchParams.get('ssl_mode') ||
    ''
  ).toLowerCase();

  const urlForcesSsl = ['required', 'require', 'verify_ca', 'verify_identity'].includes(sslMode)
    || String(parsed.searchParams.get('ssl') || '').toLowerCase() === 'true';

  if (urlForcesSsl) {
    config.ssl = true;
  }

  if (parsed.searchParams.get('connectionLimit')) {
    config.connectionLimit = Number(parsed.searchParams.get('connectionLimit')) || config.connectionLimit;
  }

  return config;
}

class DatabaseHandler {
  constructor() {
    this.mysql = buildMysqlConfigFromEnv();

    this.pool = null;
    this.initPromise = this._initialize();
  }

  _now() {
    return new Date().toISOString();
  }

  _addDaysIso(iso, days) {
    const date = new Date(iso);
    date.setDate(date.getDate() + days);
    return date.toISOString();
  }

  _assertProviderSupported(provider) {
    if (!isKnownProviderId(provider)) {
      throw new Error(`Provider '${provider}' not found`);
    }
  }

  _getStaticProviderCatalog() {
    return STATIC_PROVIDER_IDS.map((providerId) => ({
      ...STATIC_PROVIDER_DEFS[providerId],
      enabled: true,
      source: 'static'
    }));
  }

  _buildRcloneProviderCatalog(rcloneConfig = null) {
    const config = rcloneConfig || {};
    const profiles = Array.isArray(config.syncProfiles) ? config.syncProfiles : [];
    const remotes = new Map((config.remotes || []).map((remote) => [remote.name, remote]));

    return profiles.map((profile) => {
      const providerId = buildRcloneProfileProviderId(profile.id);
      const remote = remotes.get(profile.remoteName) || null;

      return {
        id: providerId,
        name: profile.name || providerId,
        short: `R${String(profile.name || profile.id || '').trim().slice(0, 2).toUpperCase()}`,
        kind: 'rclone',
        source: 'rclone-profile',
        profileId: profile.id,
        remoteName: profile.remoteName,
        remoteType: remote?.type || null,
        destinationPath: profile.destinationPath || '',
        publicBaseUrl: profile.publicBaseUrl || '',
        enabled: profile.enabled !== false,
        supportsStream: Boolean(profile.publicBaseUrl),
        supportsReupload: true,
        supportsCopy: true
      };
    });
  }

  _buildProviderCatalog(configs = {}, rcloneConfig = null) {
    const catalog = [...this._getStaticProviderCatalog(), ...this._buildRcloneProviderCatalog(rcloneConfig)];
    return catalog.map((item) => {
      const configEntry = configs?.[item.id] || null;
      const enabledByConfig = configEntry?.enabled !== false;
      const enabled = item.enabled !== false && enabledByConfig;

      return {
        ...item,
        enabled,
        configured: item.kind === 'rclone' ? Boolean(item.remoteName) : true
      };
    });
  }

  _mapFolderRow(row) {
    return {
      id: row.id,
      name: row.name,
      parentId: row.parent_id,
      path: row.path,
      createdAt: row.created_at,
      updatedAt: row.updated_at
    };
  }

  _mapJobRow(row) {
    return {
      id: row.id,
      type: row.type,
      fileId: row.file_id,
      status: row.status,
      progress: toInt(row.progress),
      attempts: toInt(row.attempts),
      maxAttempts: toInt(row.max_attempts, 3),
      error: row.error,
      metadata: parseJson(row.metadata, {}) || {},
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      startedAt: row.started_at,
      completedAt: row.completed_at,
      heartbeatAt: row.heartbeat_at || null
    };
  }

  _buildProviders(providerRows = []) {
    const providers = {};

    for (const row of providerRows) {
      providers[row.provider] = {
        status: row.status || 'pending',
        url: row.url || null,
        fileId: row.remote_file_id || null,
        embedUrl: row.embed_url || null,
        error: row.error || null,
        urlHistory: parseJson(row.url_history, []) || [],
        updatedAt: row.updated_at || null
      };
    }

    return providers;
  }

  _mapFileRow(row, providerRows = []) {
    const extraProgress = parseJson(row.progress_extra, {}) || {};

    return {
      id: row.id,
      folderId: row.folder_id,
      name: row.name,
      originalUrl: row.original_url,
      localPath: row.local_path,
      size: toInt(row.size),
      duration: Number(row.duration) || 0,
      status: row.status,
      progress: {
        download: toInt(row.progress_download),
        processing: toInt(row.progress_processing),
        upload: toInt(row.progress_upload),
        ...extraProgress
      },
      providers: this._buildProviders(providerRows),
      syncStatus: toInt(row.sync_status),
      canDelete: toBool(row.can_delete),
      createdAt: row.created_at,
      updatedAt: row.updated_at
    };
  }

  async _initialize() {
    if (this.mysql.autoCreateDatabase) {
      await this._createDatabaseIfNeeded();
    }

    const poolConfig = {
      host: this.mysql.host,
      port: this.mysql.port,
      user: this.mysql.user,
      password: this.mysql.password,
      database: this.mysql.database,
      waitForConnections: true,
      connectionLimit: this.mysql.connectionLimit,
      queueLimit: 0,
      charset: 'utf8mb4'
    };

    if (this.mysql.ssl) {
      poolConfig.ssl = { rejectUnauthorized: this.mysql.sslRejectUnauthorized };
    }

    this.pool = mysql.createPool(poolConfig);
    await this._createSchema();
    await this._seedDefaults();
  }

  async _createDatabaseIfNeeded() {
    const bootstrapConfig = {
      host: this.mysql.host,
      port: this.mysql.port,
      user: this.mysql.user,
      password: this.mysql.password,
      charset: 'utf8mb4'
    };

    if (this.mysql.ssl) {
      bootstrapConfig.ssl = { rejectUnauthorized: this.mysql.sslRejectUnauthorized };
    }

    const connection = await mysql.createConnection(bootstrapConfig);
    try {
      const safeDbName = String(this.mysql.database).replace(/`/g, '``');
      await connection.query(
        `CREATE DATABASE IF NOT EXISTS \`${safeDbName}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
      );
    } finally {
      await connection.end();
    }
  }

  async _createSchema() {
    const statements = [
      `CREATE TABLE IF NOT EXISTS folders (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        parent_id VARCHAR(64) NULL,
        path TEXT NOT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        INDEX idx_folders_parent (parent_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS files (
        id VARCHAR(64) PRIMARY KEY,
        folder_id VARCHAR(64) NOT NULL,
        name VARCHAR(512) NOT NULL,
        original_url TEXT NULL,
        local_path TEXT NULL,
        size BIGINT NOT NULL DEFAULT 0,
        duration DOUBLE NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL,
        progress_download INT NOT NULL DEFAULT 0,
        progress_processing INT NOT NULL DEFAULT 0,
        progress_upload INT NOT NULL DEFAULT 0,
        progress_extra LONGTEXT NULL,
        sync_status INT NOT NULL DEFAULT 0,
        can_delete TINYINT(1) NOT NULL DEFAULT 0,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        INDEX idx_files_folder (folder_id),
        INDEX idx_files_status (status),
        INDEX idx_files_created (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS file_providers (
        file_id VARCHAR(64) NOT NULL,
        provider VARCHAR(64) NOT NULL,
        status VARCHAR(32) NOT NULL,
        url TEXT NULL,
        remote_file_id TEXT NULL,
        embed_url TEXT NULL,
        error TEXT NULL,
        url_history LONGTEXT NULL,
        updated_at VARCHAR(40) NOT NULL,
        PRIMARY KEY (file_id, provider),
        INDEX idx_file_providers_provider (provider),
        INDEX idx_file_providers_status (status)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS jobs (
        id VARCHAR(64) PRIMARY KEY,
        type VARCHAR(32) NOT NULL,
        file_id VARCHAR(64) NULL,
        status VARCHAR(32) NOT NULL,
        progress INT NOT NULL DEFAULT 0,
        attempts INT NOT NULL DEFAULT 0,
        max_attempts INT NOT NULL DEFAULT 3,
        error TEXT NULL,
        metadata LONGTEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        started_at VARCHAR(40) NULL,
        completed_at VARCHAR(40) NULL,
        heartbeat_at VARCHAR(40) NULL,
        INDEX idx_jobs_status (status),
        INDEX idx_jobs_file (file_id),
        INDEX idx_jobs_type (type),
        INDEX idx_jobs_created (created_at),
        INDEX idx_jobs_heartbeat (heartbeat_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS batch_sessions (
        id VARCHAR(64) PRIMARY KEY,
        run_id VARCHAR(64) NULL,
        root_cg_id VARCHAR(64) NOT NULL,
        root_cg_name VARCHAR(255) NULL,
        target_cg_selector VARCHAR(255) NULL,
        parent_container_name VARCHAR(255) NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'running',
        total_containers INT NOT NULL DEFAULT 0,
        processed_containers INT NOT NULL DEFAULT 0,
        queued_count INT NOT NULL DEFAULT 0,
        skipped_count INT NOT NULL DEFAULT 0,
        next_container_offset INT NOT NULL DEFAULT 0,
        has_more TINYINT(1) NOT NULL DEFAULT 1,
        error TEXT NULL,
        session_data LONGTEXT NULL,
        queued_items LONGTEXT NULL,
        skipped_items LONGTEXT NULL,
        chain_errors LONGTEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        expires_at VARCHAR(40) NULL,
        finished_at VARCHAR(40) NULL,
        INDEX idx_batch_status (status),
        INDEX idx_batch_run (run_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS provider_configs (
        provider VARCHAR(64) PRIMARY KEY,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        config LONGTEXT NULL,
        updated_at VARCHAR(40) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS system_state (
        id TINYINT PRIMARY KEY,
        last_check VARCHAR(40) NULL,
        next_scheduled_check VARCHAR(40) NULL,
        primary_provider VARCHAR(64) NOT NULL,
        updated_at VARCHAR(40) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS provider_checks (
        provider VARCHAR(64) PRIMARY KEY,
        payload LONGTEXT NULL,
        checked_at VARCHAR(40) NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS rclone_remotes (
        name VARCHAR(128) PRIMARY KEY,
        type VARCHAR(64) NOT NULL,
        parameters LONGTEXT NULL,
        updated_at VARCHAR(40) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS rclone_sync_profiles (
        id VARCHAR(128) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        provider VARCHAR(64) NOT NULL,
        remote_name VARCHAR(128) NOT NULL,
        destination_path TEXT NULL,
        public_base_url TEXT NULL,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        updated_at VARCHAR(40) NOT NULL,
        INDEX idx_rclone_profiles_provider (provider),
        INDEX idx_rclone_profiles_remote (remote_name)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
      `CREATE TABLE IF NOT EXISTS rclone_state (
        id TINYINT PRIMARY KEY,
        default_profile_id VARCHAR(128) NULL,
        last_validation LONGTEXT NULL,
        last_validated_at VARCHAR(40) NULL,
        updated_at VARCHAR(40) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`
    ];

    for (const statement of statements) {
      await this.pool.query(statement);
    }
  }

  async _seedDefaults() {
    const now = this._now();

    await this.pool.query(
      `INSERT INTO folders (id, name, parent_id, path, created_at, updated_at)
       VALUES ('root', 'Root', NULL, '/', ?, ?)
       ON DUPLICATE KEY UPDATE name = VALUES(name), path = VALUES(path), updated_at = VALUES(updated_at)`,
      [now, now]
    );

    for (const provider of STATIC_PROVIDER_IDS) {
      await this.pool.query(
        `INSERT INTO provider_configs (provider, enabled, config, updated_at)
         VALUES (?, 1, '{}', ?)
         ON DUPLICATE KEY UPDATE provider = provider`,
        [provider, now]
      );

      await this.pool.query(
        `INSERT INTO provider_checks (provider, payload, checked_at)
         VALUES (?, NULL, NULL)
         ON DUPLICATE KEY UPDATE provider = provider`,
        [provider]
      );
    }

    await this.pool.query(
      `INSERT INTO system_state (id, last_check, next_scheduled_check, primary_provider, updated_at)
       VALUES (1, NULL, NULL, 'catbox', ?)
       ON DUPLICATE KEY UPDATE id = id`,
      [now]
    );

    await this.pool.query(
      `INSERT INTO rclone_state (id, default_profile_id, last_validation, last_validated_at, updated_at)
       VALUES (1, NULL, NULL, NULL, ?)
       ON DUPLICATE KEY UPDATE id = id`,
      [now]
    );

    const [files] = await this.pool.query('SELECT id FROM files');
    for (const row of files) {
      await this._ensureFileProvidersForFile(this.pool, row.id);
    }

    await this._runMigrations();

    await this._migrateLegacyRcloneProviderRows();
  }

  async _runMigrations() {
    const migrations = [
      {
        name: 'add_heartbeat_at_to_jobs',
        check: async () => {
          const [columns] = await this.pool.query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jobs' AND COLUMN_NAME = 'heartbeat_at'"
          );
          return columns.length === 0;
        },
        sql: "ALTER TABLE jobs ADD COLUMN heartbeat_at VARCHAR(40) NULL, ADD INDEX idx_jobs_heartbeat (heartbeat_at)"
      }
    ];

    for (const migration of migrations) {
      try {
        const needsRun = await migration.check();
        if (needsRun) {
          console.log(`[DB Migration] Running: ${migration.name}`);
          await this.pool.query(migration.sql);
          console.log(`[DB Migration] Completed: ${migration.name}`);
        }
      } catch (error) {
        console.warn(`[DB Migration] Failed ${migration.name}: ${error.message}`);
      }
    }
  }

  async _ready() {
    await this.initPromise;
  }

  async _withTransaction(fn) {
    await this._ready();
    const connection = await this.pool.getConnection();

    try {
      await connection.beginTransaction();
      const result = await fn(connection);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async _ensureFileProvidersForFile(connection, fileId) {
    const now = this._now();

    for (const provider of STATIC_PROVIDER_IDS) {
      await connection.query(
        `INSERT INTO file_providers
          (file_id, provider, status, url, remote_file_id, embed_url, error, url_history, updated_at)
         VALUES (?, ?, 'pending', NULL, NULL, NULL, NULL, '[]', ?)
         ON DUPLICATE KEY UPDATE file_id = file_id`,
        [fileId, provider, now]
      );
    }
  }

  async _migrateLegacyRcloneProviderRows(connection = this.pool) {
    const [stateRows] = await connection.query('SELECT default_profile_id FROM rclone_state WHERE id = 1 LIMIT 1');
    const defaultProfileId = String(stateRows[0]?.default_profile_id || '').trim();
    if (!defaultProfileId) {
      return;
    }

    const targetProviderId = buildRcloneProfileProviderId(defaultProfileId);

    await connection.query(
      `UPDATE file_providers fp
       SET provider = ?
       WHERE fp.provider = ?
         AND NOT EXISTS (
           SELECT 1
           FROM file_providers existing
           WHERE existing.file_id = fp.file_id
             AND existing.provider = ?
         )`,
      [targetProviderId, LEGACY_RCLONE_PROVIDER_ID, targetProviderId]
    );

    await connection.query(
      'DELETE FROM file_providers WHERE provider = ?',
      [LEGACY_RCLONE_PROVIDER_ID]
    );

    await connection.query(
      `UPDATE provider_configs pc
       SET provider = ?
       WHERE pc.provider = ?
         AND NOT EXISTS (
           SELECT 1 FROM provider_configs existing WHERE existing.provider = ?
         )`,
      [targetProviderId, LEGACY_RCLONE_PROVIDER_ID, targetProviderId]
    );

    await connection.query(
      'DELETE FROM provider_configs WHERE provider = ?',
      [LEGACY_RCLONE_PROVIDER_ID]
    );

    await connection.query(
      `UPDATE provider_checks pc
       SET provider = ?
       WHERE pc.provider = ?
         AND NOT EXISTS (
           SELECT 1 FROM provider_checks existing WHERE existing.provider = ?
         )`,
      [targetProviderId, LEGACY_RCLONE_PROVIDER_ID, targetProviderId]
    );

    await connection.query(
      'DELETE FROM provider_checks WHERE provider = ?',
      [LEGACY_RCLONE_PROVIDER_ID]
    );

    await connection.query(
      'UPDATE system_state SET primary_provider = ?, updated_at = ? WHERE id = 1 AND primary_provider = ?',
      [targetProviderId, this._now(), LEGACY_RCLONE_PROVIDER_ID]
    );
  }

  async _getSystemState() {
    await this._ready();
    const [rows] = await this.pool.query('SELECT * FROM system_state WHERE id = 1 LIMIT 1');
    const state = rows[0] || null;

    if (!state) {
      const now = this._now();
      await this.pool.query(
        `INSERT INTO system_state (id, last_check, next_scheduled_check, primary_provider, updated_at)
         VALUES (1, NULL, NULL, 'catbox', ?)`,
        [now]
      );
      return {
        id: 1,
        last_check: null,
        next_scheduled_check: null,
        primary_provider: 'catbox',
        updated_at: now
      };
    }

    return state;
  }

  async _getProviderRowsByFileIds(fileIds) {
    if (!fileIds.length) {
      return new Map();
    }

    const placeholders = fileIds.map(() => '?').join(',');
    const [rows] = await this.pool.query(
      `SELECT * FROM file_providers WHERE file_id IN (${placeholders})`,
      fileIds
    );

    const grouped = new Map();
    for (const row of rows) {
      if (!grouped.has(row.file_id)) {
        grouped.set(row.file_id, []);
      }
      grouped.get(row.file_id).push(row);
    }

    return grouped;
  }

  async _hydrateFiles(fileRows) {
    const fileIds = fileRows.map((row) => row.id);
    const providerMap = await this._getProviderRowsByFileIds(fileIds);

    return fileRows.map((row) => this._mapFileRow(row, providerMap.get(row.id) || []));
  }

  async _getSystemPayload() {
    const state = await this._getSystemState();
    const rclone = await this.getRcloneConfig();
    const providerConfigs = await this.getProviderConfigs();
    const providerCatalog = this._buildProviderCatalog(providerConfigs, rclone);
    const providerChecks = await this.getProviderCheckStatuses();

    const hasPrimary = providerCatalog.some((item) => item.id === state.primary_provider);
    const fallbackPrimary = providerCatalog.find((item) => item.enabled)?.id
      || providerCatalog[0]?.id
      || 'catbox';

    return {
      lastCheck: state.last_check,
      nextScheduledCheck: state.next_scheduled_check,
      primaryProvider: hasPrimary ? state.primary_provider : fallbackPrimary,
      providerChecks,
      providerCatalog,
      rclone
    };
  }

  // ==================== FOLDER OPERATIONS ====================

  async createFolder(name, parentId = 'root') {
    await this._ready();

    const trimmedName = String(name || '').trim();
    if (!trimmedName) {
      throw new Error('Folder name is required');
    }

    const [parents] = await this.pool.query('SELECT * FROM folders WHERE id = ? LIMIT 1', [parentId]);
    const parent = parents[0];

    if (!parent) {
      throw new Error(`Parent folder ${parentId} not found`);
    }

    const [existing] = await this.pool.query(
      'SELECT id FROM folders WHERE parent_id <=> ? AND name = ? LIMIT 1',
      [parentId, trimmedName]
    );

    if (existing.length > 0) {
      throw new Error(`Folder '${trimmedName}' already exists in this location`);
    }

    const now = this._now();
    const folder = {
      id: uuidv4(),
      name: trimmedName,
      parentId,
      path: parent.path === '/' ? `/${trimmedName}` : `${parent.path}/${trimmedName}`,
      createdAt: now,
      updatedAt: now
    };

    await this.pool.query(
      `INSERT INTO folders (id, name, parent_id, path, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [folder.id, folder.name, folder.parentId, folder.path, folder.createdAt, folder.updatedAt]
    );

    return folder;
  }

  _normalizeFolderPath(folderPath = 'root') {
    const rawValue = String(folderPath || '').trim();
    if (!rawValue) {
      return [];
    }

    const unwrapped = (
      (rawValue.startsWith('"') && rawValue.endsWith('"'))
      || (rawValue.startsWith('\'') && rawValue.endsWith('\''))
    )
      ? rawValue.slice(1, -1).trim()
      : rawValue;

    const normalized = unwrapped.replace(/\\/g, '/');
    const segments = normalized
      .split('/')
      .map((segment) => String(segment || '').trim())
      .filter(Boolean);

    if (segments[0]?.toLowerCase() === 'root') {
      segments.shift();
    }

    return segments;
  }

  async ensureFolderPath(folderPath = 'root') {
    await this._ready();

    const segments = this._normalizeFolderPath(folderPath);

    return this._withTransaction(async (connection) => {
      const [rootRows] = await connection.query(
        'SELECT * FROM folders WHERE id = ? LIMIT 1 FOR UPDATE',
        ['root']
      );

      const root = rootRows[0];
      if (!root) {
        throw new Error('Root folder not found');
      }

      let current = root;

      for (const segment of segments) {
        const [existingRows] = await connection.query(
          'SELECT * FROM folders WHERE parent_id <=> ? AND name = ? LIMIT 1 FOR UPDATE',
          [current.id, segment]
        );

        if (existingRows[0]) {
          current = existingRows[0];
          continue;
        }

        const now = this._now();
        const nextFolder = {
          id: uuidv4(),
          name: segment,
          parent_id: current.id,
          path: current.path === '/' ? `/${segment}` : `${current.path}/${segment}`,
          created_at: now,
          updated_at: now
        };

        await connection.query(
          `INSERT INTO folders (id, name, parent_id, path, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            nextFolder.id,
            nextFolder.name,
            nextFolder.parent_id,
            nextFolder.path,
            nextFolder.created_at,
            nextFolder.updated_at
          ]
        );

        current = nextFolder;
      }

      return this._mapFolderRow(current);
    });
  }

  async getFolder(folderId = 'root') {
    await this._ready();

    const [folders] = await this.pool.query('SELECT * FROM folders WHERE id = ? LIMIT 1', [folderId]);
    const folder = folders[0];

    if (!folder) {
      throw new Error(`Folder ${folderId} not found`);
    }

    const [childrenRows] = await this.pool.query('SELECT * FROM folders WHERE parent_id = ? ORDER BY name ASC', [folderId]);
    const files = await this.listFiles(folderId);

    return {
      ...this._mapFolderRow(folder),
      children: childrenRows.map((row) => this._mapFolderRow(row)),
      files
    };
  }

  async getFolderTree() {
    await this._ready();

    const [folderRows] = await this.pool.query('SELECT * FROM folders ORDER BY created_at ASC');
    const [fileRows] = await this.pool.query('SELECT * FROM files ORDER BY created_at DESC');
    const files = await this._hydrateFiles(fileRows);

    const folders = folderRows.map((row) => this._mapFolderRow(row));
    const foldersByParent = new Map();
    const filesByFolder = new Map();

    for (const folder of folders) {
      const key = folder.parentId || '__root__';
      if (!foldersByParent.has(key)) foldersByParent.set(key, []);
      foldersByParent.get(key).push(folder);
    }

    for (const file of files) {
      if (!filesByFolder.has(file.folderId)) filesByFolder.set(file.folderId, []);
      filesByFolder.get(file.folderId).push(file);
    }

    const buildTree = (parentId) => {
      const childFolders = (foldersByParent.get(parentId) || []).map((folder) => ({
        ...folder,
        children: buildTree(folder.id)
      }));

      return {
        folders: childFolders,
        files: filesByFolder.get(parentId) || []
      };
    };

    return buildTree('root');
  }

  async moveFolder(folderId, newParentId) {
    await this._ready();

    const [folderRows] = await this.pool.query('SELECT * FROM folders WHERE id = ? LIMIT 1', [folderId]);
    const folder = folderRows[0];

    if (!folder) {
      throw new Error(`Folder ${folderId} not found`);
    }

    const [allRows] = await this.pool.query('SELECT id, parent_id FROM folders');
    const parentLookup = new Map(allRows.map((row) => [row.id, row.parent_id]));

    let currentId = newParentId;
    while (currentId) {
      if (currentId === folderId) {
        throw new Error('Cannot move folder into its own subtree');
      }
      currentId = parentLookup.get(currentId) || null;
    }

    const [parentRows] = await this.pool.query('SELECT * FROM folders WHERE id = ? LIMIT 1', [newParentId]);
    const newParent = parentRows[0];

    if (!newParent) {
      throw new Error(`Folder ${newParentId} not found`);
    }

    const now = this._now();
    const nextPath = newParent.path === '/'
      ? `/${folder.name}`
      : `${newParent.path}/${folder.name}`;

    await this.pool.query(
      'UPDATE folders SET parent_id = ?, path = ?, updated_at = ? WHERE id = ?',
      [newParentId, nextPath, now, folderId]
    );

    return {
      ...this._mapFolderRow({
        ...folder,
        parent_id: newParentId,
        path: nextPath,
        updated_at: now
      })
    };
  }

  async deleteFolder(folderId) {
    await this._ready();

    const normalizedId = String(folderId || '').trim();
    if (!normalizedId) {
      throw new Error('Folder id is required');
    }

    if (normalizedId === 'root') {
      throw new Error('Cannot delete root folder');
    }

    return this._withTransaction(async (connection) => {
      const [folderRows] = await connection.query(
        'SELECT * FROM folders WHERE id = ? LIMIT 1 FOR UPDATE',
        [normalizedId]
      );
      const folder = folderRows[0];

      if (!folder) {
        throw new Error(`Folder ${normalizedId} not found`);
      }

      const [childRows] = await connection.query(
        'SELECT id FROM folders WHERE parent_id = ? LIMIT 1 FOR UPDATE',
        [normalizedId]
      );
      if (childRows[0]) {
        throw new Error('Folder is not empty. Delete subfolders first');
      }

      const [fileRows] = await connection.query(
        'SELECT id FROM files WHERE folder_id = ? LIMIT 1 FOR UPDATE',
        [normalizedId]
      );
      if (fileRows[0]) {
        throw new Error('Folder contains files. Move or delete files first');
      }

      await connection.query('DELETE FROM folders WHERE id = ?', [normalizedId]);

      return this._mapFolderRow(folder);
    });
  }

  async purgeFolder(folderId) {
    await this._ready();

    const normalizedId = String(folderId || '').trim();
    if (!normalizedId) {
      throw new Error('Folder id is required');
    }

    if (normalizedId === 'root') {
      throw new Error('Cannot purge root folder');
    }

    return this._withTransaction(async (connection) => {
      const [targetRows] = await connection.query(
        'SELECT * FROM folders WHERE id = ? LIMIT 1 FOR UPDATE',
        [normalizedId]
      );
      const target = targetRows[0];

      if (!target) {
        throw new Error(`Folder ${normalizedId} not found`);
      }

      const [allFolderRows] = await connection.query(
        'SELECT id, parent_id, path FROM folders ORDER BY CHAR_LENGTH(path) DESC FOR UPDATE'
      );

      const childrenByParent = new Map();
      for (const row of allFolderRows) {
        const key = row.parent_id || '__root__';
        if (!childrenByParent.has(key)) childrenByParent.set(key, []);
        childrenByParent.get(key).push(row);
      }

      const folderIdsToDelete = [];
      const stack = [normalizedId];
      while (stack.length > 0) {
        const currentId = stack.pop();
        if (!currentId || folderIdsToDelete.includes(currentId)) {
          continue;
        }

        folderIdsToDelete.push(currentId);
        const children = childrenByParent.get(currentId) || [];
        for (const child of children) {
          stack.push(child.id);
        }
      }

      if (!folderIdsToDelete.length) {
        return {
          deleted: true,
          folderId: normalizedId,
          removedFolders: 0,
          removedFiles: 0,
          removedJobs: 0,
          removedProviders: 0
        };
      }

      const folderPlaceholders = folderIdsToDelete.map(() => '?').join(',');
      const [fileRows] = await connection.query(
        `SELECT id FROM files WHERE folder_id IN (${folderPlaceholders}) FOR UPDATE`,
        folderIdsToDelete
      );

      const fileIds = fileRows.map((row) => row.id);
      let removedProviders = 0;
      let removedJobs = 0;
      let removedFiles = 0;

      if (fileIds.length > 0) {
        const filePlaceholders = fileIds.map(() => '?').join(',');

        const [providerDelete] = await connection.query(
          `DELETE FROM file_providers WHERE file_id IN (${filePlaceholders})`,
          fileIds
        );
        removedProviders = providerDelete.affectedRows || 0;

        const [jobDelete] = await connection.query(
          `DELETE FROM jobs WHERE file_id IN (${filePlaceholders})`,
          fileIds
        );
        removedJobs = jobDelete.affectedRows || 0;

        const [fileDelete] = await connection.query(
          `DELETE FROM files WHERE id IN (${filePlaceholders})`,
          fileIds
        );
        removedFiles = fileDelete.affectedRows || 0;
      }

      const [folderDelete] = await connection.query(
        `DELETE FROM folders WHERE id IN (${folderPlaceholders})`,
        folderIdsToDelete
      );

      return {
        deleted: true,
        folderId: normalizedId,
        removedFolders: folderDelete.affectedRows || 0,
        removedFiles,
        removedJobs,
        removedProviders
      };
    });
  }

  async deleteAllFolders() {
    await this._ready();

    return this._withTransaction(async (connection) => {
      // Get all non-root folders ordered by path length DESC (children first)
      const [allFolderRows] = await connection.query(
        "SELECT id, parent_id, path FROM folders WHERE id != 'root' ORDER BY CHAR_LENGTH(path) DESC FOR UPDATE"
      );

      if (allFolderRows.length === 0) {
        return {
          deleted: true,
          removedFolders: 0,
          removedFiles: 0,
          removedJobs: 0,
          removedProviders: 0
        };
      }

      const folderIdsToDelete = allFolderRows.map((row) => row.id);
      const folderPlaceholders = folderIdsToDelete.map(() => '?').join(',');

      // Get all files in these folders
      const [fileRows] = await connection.query(
        `SELECT id FROM files WHERE folder_id IN (${folderPlaceholders})`,
        folderIdsToDelete
      );
      const fileIds = fileRows.map((row) => row.id);

      let removedFiles = 0;
      let removedJobs = 0;
      let removedProviders = 0;

      if (fileIds.length > 0) {
        const filePlaceholders = fileIds.map(() => '?').join(',');

        // Delete provider entries
        const [providerDelete] = await connection.query(
          `DELETE FROM file_providers WHERE file_id IN (${filePlaceholders})`,
          fileIds
        );
        removedProviders = providerDelete.affectedRows || 0;

        // Delete jobs
        const [jobDelete] = await connection.query(
          `DELETE FROM jobs WHERE file_id IN (${filePlaceholders})`,
          fileIds
        );
        removedJobs = jobDelete.affectedRows || 0;

        // Delete files
        const [fileDelete] = await connection.query(
          `DELETE FROM files WHERE id IN (${filePlaceholders})`,
          fileIds
        );
        removedFiles = fileDelete.affectedRows || 0;
      }

      // Delete all folders (except root)
      const [folderDelete] = await connection.query(
        `DELETE FROM folders WHERE id IN (${folderPlaceholders})`,
        folderIdsToDelete
      );

      return {
        deleted: true,
        removedFolders: folderDelete.affectedRows || 0,
        removedFiles,
        removedJobs,
        removedProviders,
        folderIds: folderIdsToDelete
      };
    });
  }

  // ==================== FILE OPERATIONS ====================

  async createFile(fileData) {
    const fileId = await this._withTransaction(async (connection) => {
      const folderId = fileData.folderId || 'root';
      const [folderRows] = await connection.query('SELECT id FROM folders WHERE id = ? LIMIT 1', [folderId]);
      if (!folderRows[0]) {
        throw new Error(`Folder ${folderId} not found`);
      }

      const now = this._now();
      const id = uuidv4();

      await connection.query(
        `INSERT INTO files (
          id, folder_id, name, original_url, local_path, size, duration, status,
          progress_download, progress_processing, progress_upload, progress_extra,
          sync_status, can_delete, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          id,
          folderId,
          fileData.name,
          fileData.originalUrl || null,
          fileData.localPath || null,
          toInt(fileData.size),
          Number(fileData.duration) || 0,
          'processing',
          0,
          0,
          0,
          '{}',
          0,
          0,
          now,
          now
        ]
      );

      await this._ensureFileProvidersForFile(connection, id);
      return id;
    });

    return this.getFile(fileId);
  }

  async findFileByNameInFolder(folderId, fileName) {
    await this._ready();

    const [rows] = await this.pool.query(
      'SELECT * FROM files WHERE folder_id = ? AND name = ? LIMIT 1',
      [folderId, fileName]
    );

    if (!rows[0]) {
      return null;
    }

    const fileId = rows[0].id;
    const providerMap = await this._getProviderRowsByFileIds([fileId]);
    return this._mapFileRow(rows[0], providerMap.get(fileId) || []);
  }

  async getFile(fileId) {
    await this._ready();

    const [rows] = await this.pool.query('SELECT * FROM files WHERE id = ? LIMIT 1', [fileId]);
    const fileRow = rows[0];

    if (!fileRow) {
      throw new Error(`File ${fileId} not found`);
    }

    const providerMap = await this._getProviderRowsByFileIds([fileId]);
    return this._mapFileRow(fileRow, providerMap.get(fileId) || []);
  }

  async updateProviderStatus(fileId, provider, status, url = null, fileId_provider = null, error = null, extra = {}) {
    this._assertProviderSupported(provider);

    await this._withTransaction(async (connection) => {
      const [fileRows] = await connection.query('SELECT id FROM files WHERE id = ? FOR UPDATE', [fileId]);
      if (!fileRows[0]) {
        throw new Error(`File ${fileId} not found`);
      }

      await this._ensureFileProvidersForFile(connection, fileId);

      const [currentRows] = await connection.query(
        'SELECT * FROM file_providers WHERE file_id = ? AND provider = ? FOR UPDATE',
        [fileId, provider]
      );

      const current = currentRows[0] || null;
      const history = parseJson(current?.url_history, []) || [];

      if (current?.url && current.url !== url) {
        history.push({
          url: current.url,
          fileId: current.remote_file_id || null,
          embedUrl: current.embed_url || null,
          archivedAt: this._now()
        });
      }

      const now = this._now();
      const embedUrl = extra.embedUrl !== undefined
        ? extra.embedUrl
        : (current?.embed_url || null);

      await connection.query(
        `INSERT INTO file_providers
          (file_id, provider, status, url, remote_file_id, embed_url, error, url_history, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
          status = VALUES(status),
          url = VALUES(url),
          remote_file_id = VALUES(remote_file_id),
          embed_url = VALUES(embed_url),
          error = VALUES(error),
          url_history = VALUES(url_history),
          updated_at = VALUES(updated_at)`,
        [
          fileId,
          provider,
          status,
          url,
          fileId_provider,
          embedUrl,
          error,
          stringifyJson(history),
          now
        ]
      );

      const [providerRows] = await connection.query('SELECT status FROM file_providers WHERE file_id = ?', [fileId]);
      const statuses = providerRows.map((row) => row.status);
      const providerCount = Math.max(1, statuses.length);
      const completed = statuses.filter((value) => value === 'completed').length;
      const failed = statuses.filter((value) => value === 'failed').length;

      let fileStatus = 'uploading';
      if (completed === providerCount) {
        fileStatus = 'completed';
      } else if (completed + failed === providerCount) {
        fileStatus = failed === providerCount ? 'failed' : 'partial';
      }

      const syncStatus = Math.round((completed / providerCount) * 100);
      const canDelete = completed === providerCount && failed === 0;

      await connection.query(
        'UPDATE files SET sync_status = ?, can_delete = ?, status = ?, updated_at = ? WHERE id = ?',
        [syncStatus, canDelete ? 1 : 0, fileStatus, now, fileId]
      );
    });

    return this.getFile(fileId);
  }

  async updateFileProgress(fileId, progressType, value) {
    await this._ready();

    const progressColumns = {
      download: 'progress_download',
      processing: 'progress_processing',
      upload: 'progress_upload'
    };

    const nextValue = toInt(value);
    const now = this._now();

    if (progressColumns[progressType]) {
      const [result] = await this.pool.query(
        `UPDATE files SET ${progressColumns[progressType]} = ?, updated_at = ? WHERE id = ?`,
        [nextValue, now, fileId]
      );

      if (result.affectedRows === 0) {
        throw new Error(`File ${fileId} not found`);
      }

      return this.getFile(fileId);
    }

    const [rows] = await this.pool.query('SELECT progress_extra FROM files WHERE id = ? LIMIT 1', [fileId]);
    if (!rows[0]) {
      throw new Error(`File ${fileId} not found`);
    }

    const extra = parseJson(rows[0].progress_extra, {}) || {};
    extra[progressType] = nextValue;

    await this.pool.query(
      'UPDATE files SET progress_extra = ?, updated_at = ? WHERE id = ?',
      [stringifyJson(extra), now, fileId]
    );

    return this.getFile(fileId);
  }

  async markFileDeleted(fileId) {
    return this._withTransaction(async (connection) => {
      const [rows] = await connection.query('SELECT id, can_delete FROM files WHERE id = ? FOR UPDATE', [fileId]);
      const row = rows[0];

      if (!row) {
        throw new Error(`File ${fileId} not found`);
      }

      if (!toBool(row.can_delete)) {
        throw new Error('File cannot be deleted: sync not 100% complete');
      }

      await connection.query('DELETE FROM file_providers WHERE file_id = ?', [fileId]);
      await connection.query('DELETE FROM files WHERE id = ?', [fileId]);

      return { deleted: true, fileId };
    });
  }

  async cancelJobsForFile(fileId) {
    await this._ready();

    const now = this._now();
    const [result] = await this.pool.query(
      `UPDATE jobs
       SET status = 'cancelled',
           error = 'Cancelled by user',
           updated_at = ?,
           completed_at = ?
       WHERE file_id = ?
         AND (status = 'pending' OR status = 'processing')`,
      [now, now, fileId]
    );

    return { fileId, cancelledJobs: result.affectedRows };
  }

  async purgeFileAndJobs(fileId) {
    return this._withTransaction(async (connection) => {
      const [fileRows] = await connection.query('SELECT id, name FROM files WHERE id = ? FOR UPDATE', [fileId]);
      const file = fileRows[0];

      if (!file) {
        throw new Error(`File ${fileId} not found`);
      }

      const [jobCountRows] = await connection.query('SELECT COUNT(*) AS count FROM jobs WHERE file_id = ?', [fileId]);
      const removedJobs = toInt(jobCountRows[0]?.count);

      await connection.query('DELETE FROM file_providers WHERE file_id = ?', [fileId]);
      await connection.query('DELETE FROM jobs WHERE file_id = ?', [fileId]);
      await connection.query('DELETE FROM files WHERE id = ?', [fileId]);

      return {
        deleted: true,
        fileId,
        removedJobs,
        fileName: file.name
      };
    });
  }

  async listFiles(folderId = null, status = null) {
    await this._ready();

    const where = [];
    const params = [];

    if (folderId) {
      where.push('folder_id = ?');
      params.push(folderId);
    }

    if (status) {
      where.push('status = ?');
      params.push(status);
    }

    let sql = 'SELECT * FROM files';
    if (where.length) {
      sql += ` WHERE ${where.join(' AND ')}`;
    }
    sql += ' ORDER BY created_at DESC';

    const [rows] = await this.pool.query(sql, params);
    return this._hydrateFiles(rows);
  }

  async moveFile(fileId, folderId) {
    await this._ready();

    const [fileRows] = await this.pool.query('SELECT * FROM files WHERE id = ? LIMIT 1', [fileId]);
    const file = fileRows[0];
    if (!file) {
      throw new Error(`File ${fileId} not found`);
    }

    const [folderRows] = await this.pool.query('SELECT id FROM folders WHERE id = ? LIMIT 1', [folderId]);
    if (!folderRows[0]) {
      throw new Error(`Folder ${folderId} not found`);
    }

    await this.pool.query(
      'UPDATE files SET folder_id = ?, updated_at = ? WHERE id = ?',
      [folderId, this._now(), fileId]
    );

    return this.getFile(fileId);
  }

  async updateFileLocalPath(fileId, localPath = null) {
    await this._ready();

    const [result] = await this.pool.query(
      'UPDATE files SET local_path = ?, updated_at = ? WHERE id = ?',
      [localPath, this._now(), fileId]
    );

    if (result.affectedRows === 0) {
      throw new Error(`File ${fileId} not found`);
    }

    return this.getFile(fileId);
  }

  async updateFileMetadata(fileId, metadata = {}) {
    await this._ready();

    const fields = [];
    const params = [];

    if (metadata.size !== undefined) {
      fields.push('size = ?');
      params.push(toInt(metadata.size));
    }

    if (metadata.duration !== undefined) {
      fields.push('duration = ?');
      params.push(Number(metadata.duration) || 0);
    }

    if (metadata.name !== undefined) {
      fields.push('name = ?');
      params.push(String(metadata.name));
    }

    if (fields.length === 0) {
      return this.getFile(fileId);
    }

    fields.push('updated_at = ?');
    params.push(this._now());
    params.push(fileId);

    const [result] = await this.pool.query(
      `UPDATE files SET ${fields.join(', ')} WHERE id = ?`,
      params
    );

    if (result.affectedRows === 0) {
      throw new Error(`File ${fileId} not found`);
    }

    return this.getFile(fileId);
  }

  // ==================== JOB OPERATIONS ====================

  async createJob(jobData) {
    await this._ready();

    const now = this._now();
    const id = uuidv4();

    await this.pool.query(
      `INSERT INTO jobs (
        id, type, file_id, status, progress, attempts, max_attempts,
        error, metadata, created_at, updated_at, started_at, completed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        jobData.type,
        jobData.fileId || null,
        'pending',
        0,
        0,
        toInt(jobData.maxAttempts, 3),
        null,
        stringifyJson(jobData.metadata || {}),
        now,
        now,
        null,
        null
      ]
    );

    return this.getJob(id);
  }

  async updateJob(jobId, updates) {
    await this._ready();

    const current = await this.getJob(jobId);
    const now = this._now();

    const next = {
      ...current,
      ...updates,
      metadata: updates.metadata !== undefined ? updates.metadata : current.metadata,
      updatedAt: now
    };

    if (updates.status === 'processing' && !current.startedAt) {
      next.startedAt = now;
    }

    if (['completed', 'failed', 'cancelled'].includes(updates.status)) {
      next.completedAt = now;
      next.heartbeatAt = null;
    }

    if (updates.status === 'processing') {
      next.heartbeatAt = now;
    }

    await this.pool.query(
      `UPDATE jobs
       SET type = ?,
           file_id = ?,
           status = ?,
           progress = ?,
           attempts = ?,
           max_attempts = ?,
           error = ?,
           metadata = ?,
           updated_at = ?,
           started_at = ?,
           completed_at = ?,
           heartbeat_at = ?
       WHERE id = ?`,
      [
        next.type,
        next.fileId || null,
        next.status,
        toInt(next.progress),
        toInt(next.attempts),
        toInt(next.maxAttempts, 3),
        next.error || null,
        stringifyJson(next.metadata || {}),
        next.updatedAt,
        next.startedAt || null,
        next.completedAt || null,
        next.heartbeatAt || null,
        jobId
      ]
    );

    return this.getJob(jobId);
  }

  async getJob(jobId) {
    await this._ready();
    const [rows] = await this.pool.query('SELECT * FROM jobs WHERE id = ? LIMIT 1', [jobId]);
    const row = rows[0];

    if (!row) {
      throw new Error(`Job ${jobId} not found`);
    }

    return this._mapJobRow(row);
  }

  async getPendingJobs(limit = 10) {
    await this._ready();

    const [rows] = await this.pool.query(
      `SELECT * FROM jobs
       WHERE status = 'pending' AND attempts < max_attempts
       ORDER BY created_at ASC
       LIMIT ?`,
      [Math.max(1, toInt(limit, 10))]
    );

    return rows.map((row) => this._mapJobRow(row));
  }

  async listJobs(filters = {}) {
    await this._ready();

    const { status = null, type = null, fileId = null, limit = 200 } = filters;
    const where = [];
    const params = [];

    if (status) {
      where.push('status = ?');
      params.push(status);
    }

    if (type) {
      where.push('type = ?');
      params.push(type);
    }

    if (fileId) {
      where.push('file_id = ?');
      params.push(fileId);
    }

    let sql = 'SELECT * FROM jobs';
    if (where.length) {
      sql += ` WHERE ${where.join(' AND ')}`;
    }
    sql += ' ORDER BY created_at DESC LIMIT ?';
    params.push(Math.max(1, toInt(limit, 200)));

    const [rows] = await this.pool.query(sql, params);
    return rows.map((row) => this._mapJobRow(row));
  }

  async getJobsByFile(fileId) {
    await this._ready();

    const [rows] = await this.pool.query(
      'SELECT * FROM jobs WHERE file_id = ? ORDER BY created_at DESC',
      [fileId]
    );

    return rows.map((row) => this._mapJobRow(row));
  }

  async deleteJob(jobId) {
    await this._ready();
    const job = await this.getJob(jobId);
    await this.pool.query('DELETE FROM jobs WHERE id = ?', [jobId]);
    return job;
  }

  async deleteCompletedJobs() {
    await this._ready();

    // Delete all non-active jobs (completed, failed, cancelled)
    const [result] = await this.pool.query(
      `DELETE FROM jobs WHERE status IN ('completed', 'failed', 'cancelled')`
    );

    return {
      deleted: true,
      deletedCount: result.affectedRows || 0
    };
  }

  // ==================== PROVIDER CONFIG OPERATIONS ====================

  async _getProviderConfigRowsMap() {
    const [rows] = await this.pool.query('SELECT * FROM provider_configs');
    const configs = {};

    for (const row of rows) {
      configs[row.provider] = {
        enabled: toBool(row.enabled),
        config: parseJson(row.config, {}) || {},
        updatedAt: row.updated_at || null
      };
    }

    return configs;
  }

  async updateProviderConfig(provider, config) {
    this._assertProviderSupported(provider);
    await this._ready();

    const providerCatalog = await this.getProviderCatalog({ includeDisabled: true });
    const existsInCatalog = providerCatalog.some((item) => item.id === provider);
    const isStatic = STATIC_PROVIDER_IDS.includes(provider);
    const isLegacy = provider === LEGACY_RCLONE_PROVIDER_ID;

    if (!existsInCatalog && !isStatic && !isLegacy) {
      throw new Error(`Provider '${provider}' not found`);
    }

    const configs = await this.getProviderConfigs();
    const current = configs[provider] || { enabled: true, config: {} };
    const { enabled, ...providerConfig } = config || {};

    const next = {
      enabled: enabled !== undefined ? Boolean(enabled) : current.enabled,
      config: {
        ...(current.config || {}),
        ...providerConfig
      },
      updatedAt: this._now()
    };

    await this.pool.query(
      `INSERT INTO provider_configs (provider, enabled, config, updated_at)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         enabled = VALUES(enabled),
         config = VALUES(config),
         updated_at = VALUES(updated_at)`,
      [provider, next.enabled ? 1 : 0, stringifyJson(next.config), next.updatedAt]
    );

    return {
      enabled: next.enabled,
      config: next.config,
      updatedAt: next.updatedAt
    };
  }

  async getProviderConfigs() {
    await this._ready();
    const rowConfigs = await this._getProviderConfigRowsMap();
    const rcloneConfig = await this.getRcloneConfig();
    const catalog = this._buildProviderCatalog(rowConfigs, rcloneConfig);

    const configs = {};
    for (const provider of catalog) {
      const row = rowConfigs[provider.id] || null;
      configs[provider.id] = {
        enabled: provider.enabled !== false,
        config: row?.config || {},
        updatedAt: row?.updatedAt || null,
        name: provider.name,
        kind: provider.kind,
        source: provider.source,
        profileId: provider.profileId || null,
        remoteName: provider.remoteName || null,
        remoteType: provider.remoteType || null,
        supportsStream: provider.supportsStream !== false,
        supportsReupload: provider.supportsReupload !== false,
        supportsCopy: provider.supportsCopy !== false,
        configured: provider.configured !== false
      };
    }

    for (const [providerId, row] of Object.entries(rowConfigs)) {
      if (configs[providerId]) continue;
      configs[providerId] = {
        enabled: row.enabled !== false,
        config: row.config || {},
        updatedAt: row.updatedAt || null,
        name: providerId,
        kind: isRcloneProfileProviderId(providerId) ? 'rclone' : 'unknown',
        source: 'config',
        profileId: parseRcloneProfileId(providerId),
        remoteName: null,
        remoteType: null,
        supportsStream: false,
        supportsReupload: true,
        supportsCopy: true,
        configured: true
      };
    }

    return configs;
  }

  async getProviderCatalog(options = {}) {
    await this._ready();

    const includeDisabled = options.includeDisabled !== false;
    const rowConfigs = await this._getProviderConfigRowsMap();
    const rcloneConfig = await this.getRcloneConfig();
    const catalog = this._buildProviderCatalog(rowConfigs, rcloneConfig);

    if (includeDisabled) {
      return catalog;
    }

    return catalog.filter((item) => item.enabled !== false);
  }

  async getEnabledProviderIds() {
    const catalog = await this.getProviderCatalog({ includeDisabled: false });
    return catalog.map((item) => item.id);
  }

  // ==================== STATS & HEALTH ====================

  async getStats() {
    await this._ready();

    const providerCatalog = await this.getProviderCatalog();

    const [fileTotals] = await this.pool.query('SELECT COUNT(*) AS count FROM files');
    const [jobTotals] = await this.pool.query('SELECT COUNT(*) AS count FROM jobs');
    const [fileStatusRows] = await this.pool.query('SELECT status, COUNT(*) AS count FROM files GROUP BY status');
    const [jobStatusRows] = await this.pool.query('SELECT status, COUNT(*) AS count FROM jobs GROUP BY status');
    const [providerRows] = await this.pool.query(
      'SELECT provider, status, COUNT(*) AS count FROM file_providers GROUP BY provider, status'
    );

    const providerStats = providerCatalog.reduce((acc, provider) => {
      acc[provider.id] = { pending: 0, completed: 0, failed: 0 };
      return acc;
    }, {});

    for (const row of providerRows) {
      if (!providerStats[row.provider]) {
        providerStats[row.provider] = { pending: 0, completed: 0, failed: 0 };
      }

      providerStats[row.provider][row.status] = toInt(row.count);
    }

    const fileByStatus = { processing: 0, uploading: 0, completed: 0, failed: 0, partial: 0 };
    for (const row of fileStatusRows) {
      fileByStatus[row.status] = toInt(row.count);
    }

    const jobByStatus = { pending: 0, processing: 0, completed: 0, failed: 0 };
    for (const row of jobStatusRows) {
      jobByStatus[row.status] = toInt(row.count);
    }

    return {
      folders: toInt((await this.pool.query('SELECT COUNT(*) AS count FROM folders'))[0][0]?.count),
      files: {
        total: toInt(fileTotals[0]?.count),
        byStatus: fileByStatus
      },
      jobs: {
        total: toInt(jobTotals[0]?.count),
        byStatus: jobByStatus
      },
      providers: providerStats,
      system: await this._getSystemPayload()
    };
  }

  async getDashboardData() {
    await this._ready();

    const [recentFileRows] = await this.pool.query(
      'SELECT * FROM files ORDER BY created_at DESC LIMIT 20'
    );
    const recentFiles = await this._hydrateFiles(recentFileRows);

    const [activeJobRows] = await this.pool.query(
      `SELECT id, type, file_id, progress, attempts
       FROM jobs
       WHERE status = 'processing'
       ORDER BY updated_at DESC`
    );

    const [pendingRows] = await this.pool.query("SELECT COUNT(*) AS count FROM jobs WHERE status = 'pending'");
    const [processingRows] = await this.pool.query("SELECT COUNT(*) AS count FROM jobs WHERE status = 'processing'");

    return {
      recentFiles: recentFiles.map((file) => ({
        id: file.id,
        name: file.name,
        status: file.status,
        syncStatus: file.syncStatus,
        progress: file.progress,
        providers: file.providers,
        updatedAt: file.updatedAt
      })),
      activeJobs: activeJobRows.map((row) => ({
        id: row.id,
        type: row.type,
        fileId: row.file_id,
        progress: toInt(row.progress),
        attempts: toInt(row.attempts)
      })),
      queueStats: {
        pending: toInt(pendingRows[0]?.count),
        processing: toInt(processingRows[0]?.count)
      },
      batchSessions: await this._getBatchSessionSummary(),
      system: await this._getSystemPayload()
    };
  }

  async _getBatchSessionSummary() {
    try {
      await this._ready();
      const [rows] = await this.pool.query(
        "SELECT id, status, root_cg_id, root_cg_name, total_containers, processed_containers, queued_count, skipped_count, has_more, created_at, updated_at FROM batch_sessions WHERE status IN ('running', 'pending') ORDER BY created_at DESC LIMIT 20"
      );
      return rows.map((row) => ({
        id: row.id,
        status: row.status,
        rootCgId: row.root_cg_id,
        rootCgName: row.root_cg_name,
        totalContainers: toInt(row.total_containers),
        processedContainers: toInt(row.processed_containers),
        queuedCount: toInt(row.queued_count),
        skippedCount: toInt(row.skipped_count),
        hasMore: toBool(row.has_more),
        createdAt: row.created_at,
        updatedAt: row.updated_at
      }));
    } catch {
      return [];
    }
  }

  // ==================== SYSTEM OPERATIONS ====================

  async setLastCheckTime(timestamp) {
    await this._ready();

    const nextCheck = this._addDaysIso(timestamp, 7);
    await this.pool.query(
      `UPDATE system_state
       SET last_check = ?, next_scheduled_check = ?, updated_at = ?
       WHERE id = 1`,
      [timestamp, nextCheck, this._now()]
    );

    return {
      lastCheck: timestamp,
      nextScheduledCheck: nextCheck
    };
  }

  async getLastCheckTime() {
    const state = await this._getSystemState();
    return state.last_check || null;
  }

  async getNextScheduledCheck() {
    const state = await this._getSystemState();
    return state.next_scheduled_check || null;
  }

  async getPrimaryProvider() {
    const state = await this._getSystemState();
    const providerCatalog = await this.getProviderCatalog();
    const hasPrimary = providerCatalog.some((item) => item.id === state.primary_provider);
    if (hasPrimary) {
      return state.primary_provider;
    }

    return providerCatalog.find((item) => item.enabled !== false)?.id
      || providerCatalog[0]?.id
      || 'catbox';
  }

  async setPrimaryProvider(provider) {
    this._assertProviderSupported(provider);
    await this._ready();

    const providerCatalog = await this.getProviderCatalog();
    const target = providerCatalog.find((item) => item.id === provider);
    if (!target) {
      throw new Error(`Provider '${provider}' not found`);
    }

    if (target.enabled === false) {
      throw new Error('Primary provider must be enabled');
    }

    await this.pool.query(
      'UPDATE system_state SET primary_provider = ?, updated_at = ? WHERE id = 1',
      [provider, this._now()]
    );

    return provider;
  }

  async getRcloneConfig() {
    await this._ready();

    const [remoteRows] = await this.pool.query('SELECT * FROM rclone_remotes ORDER BY name ASC');
    const [profileRows] = await this.pool.query('SELECT * FROM rclone_sync_profiles ORDER BY name ASC');
    const [stateRows] = await this.pool.query('SELECT * FROM rclone_state WHERE id = 1 LIMIT 1');
    const state = stateRows[0] || {};

    return {
      remotes: remoteRows.map((row) => ({
        name: row.name,
        type: row.type,
        parameters: parseJson(row.parameters, {}) || {}
      })),
      syncProfiles: profileRows.map((row) => ({
        providerId: buildRcloneProfileProviderId(row.id),
        id: row.id,
        name: row.name,
        provider: row.provider || 'rclone',
        remoteName: row.remote_name,
        destinationPath: row.destination_path || '',
        publicBaseUrl: row.public_base_url || '',
        enabled: toBool(row.enabled)
      })),
      defaultProfileId: state.default_profile_id || null,
      lastValidation: parseJson(state.last_validation, null),
      lastValidatedAt: state.last_validated_at || null
    };
  }

  async updateRcloneConfig(payload = {}) {
    await this._ready();

    const current = await this.getRcloneConfig();

    const normalizeRemote = (remote = {}) => ({
      name: String(remote.name || '').trim(),
      type: String(remote.type || '').trim(),
      parameters: remote.parameters && typeof remote.parameters === 'object' ? remote.parameters : {}
    });

    const normalizeProfile = (profile = {}) => ({
      id: String(profile.id || uuidv4()).trim().slice(0, 48),
      name: String(profile.name || '').trim(),
      remoteName: String(profile.remoteName || '').trim(),
      destinationPath: String(profile.destinationPath || '').trim(),
      publicBaseUrl: String(profile.publicBaseUrl || '').trim(),
      enabled: profile.enabled !== false
    });

    const remotes = Array.isArray(payload.remotes)
      ? payload.remotes
        .map(normalizeRemote)
        .filter((remote) => remote.name && remote.type)
      : current.remotes;

    const remoteNames = new Set(remotes.map((remote) => remote.name));

    const syncProfiles = Array.isArray(payload.syncProfiles)
      ? payload.syncProfiles
        .map(normalizeProfile)
        .filter((profile) => profile.id && profile.name && profile.remoteName && remoteNames.has(profile.remoteName))
      : current.syncProfiles;

    const incomingDefault = payload.defaultProfileId !== undefined
      ? (String(payload.defaultProfileId || '').trim() || null)
      : current.defaultProfileId;

    const profileIds = new Set(syncProfiles.map((profile) => profile.id));
    const defaultProfileId = incomingDefault && profileIds.has(incomingDefault)
      ? incomingDefault
      : null;

    const now = this._now();

    await this._withTransaction(async (connection) => {
      await connection.query('DELETE FROM rclone_sync_profiles');
      await connection.query('DELETE FROM rclone_remotes');

      for (const remote of remotes) {
        await connection.query(
          `INSERT INTO rclone_remotes (name, type, parameters, updated_at)
           VALUES (?, ?, ?, ?)` ,
          [remote.name, remote.type, stringifyJson(remote.parameters), now]
        );
      }

      for (const profile of syncProfiles) {
        const providerId = buildRcloneProfileProviderId(profile.id);

        await connection.query(
          `INSERT INTO rclone_sync_profiles
            (id, name, provider, remote_name, destination_path, public_base_url, enabled, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            profile.id,
            profile.name,
            'rclone',
            profile.remoteName,
            profile.destinationPath,
            profile.publicBaseUrl,
            profile.enabled ? 1 : 0,
            now
          ]
        );

        await connection.query(
          `INSERT INTO provider_configs (provider, enabled, config, updated_at)
           VALUES (?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             enabled = VALUES(enabled),
             updated_at = VALUES(updated_at)`,
          [providerId, profile.enabled ? 1 : 0, '{}', now]
        );

        await connection.query(
          `INSERT INTO provider_checks (provider, payload, checked_at)
           VALUES (?, NULL, NULL)
           ON DUPLICATE KEY UPDATE provider = provider`,
          [providerId]
        );
      }

      const activeRcloneProviderIds = syncProfiles.map((profile) => buildRcloneProfileProviderId(profile.id));
      if (activeRcloneProviderIds.length > 0) {
        const placeholders = activeRcloneProviderIds.map(() => '?').join(',');
        await connection.query(
          `DELETE FROM provider_configs
           WHERE provider LIKE 'rclone:%'
             AND provider NOT IN (${placeholders})`,
          activeRcloneProviderIds
        );

        await connection.query(
          `DELETE FROM provider_checks
           WHERE provider LIKE 'rclone:%'
             AND provider NOT IN (${placeholders})`,
          activeRcloneProviderIds
        );
      } else {
        await connection.query("DELETE FROM provider_configs WHERE provider LIKE 'rclone:%'");
        await connection.query("DELETE FROM provider_checks WHERE provider LIKE 'rclone:%'");
      }

      await connection.query(
        `INSERT INTO rclone_state (id, default_profile_id, last_validation, last_validated_at, updated_at)
         VALUES (1, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           default_profile_id = VALUES(default_profile_id),
           last_validation = VALUES(last_validation),
           last_validated_at = VALUES(last_validated_at),
           updated_at = VALUES(updated_at)`,
        [
          defaultProfileId,
          stringifyJson(current.lastValidation),
          current.lastValidatedAt,
          now
        ]
      );

      await this._migrateLegacyRcloneProviderRows(connection);
    });

    return this.getRcloneConfig();
  }

  async setRcloneValidationResult(result) {
    await this._ready();

    const now = this._now();
    await this.pool.query(
      `UPDATE rclone_state
       SET last_validation = ?, last_validated_at = ?, updated_at = ?
       WHERE id = 1`,
      [stringifyJson(result), now, now]
    );

    return this.getRcloneConfig();
  }

  async setProviderCheckStatus(provider, status) {
    this._assertProviderSupported(provider);
    await this._ready();

    const checkedAt = this._now();
    const payload = {
      ...(status || {}),
      checkedAt
    };

    await this.pool.query(
      `INSERT INTO provider_checks (provider, payload, checked_at)
       VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE
         payload = VALUES(payload),
         checked_at = VALUES(checked_at)`,
      [provider, stringifyJson(payload), checkedAt]
    );

    return payload;
  }

  async getProviderCheckStatuses() {
    await this._ready();
    const [rows] = await this.pool.query('SELECT * FROM provider_checks');
    const catalog = await this.getProviderCatalog();

    const checks = {};
    for (const provider of catalog) {
      checks[provider.id] = null;
    }

    for (const row of rows) {
      checks[row.provider] = parseJson(row.payload, null);
    }

    return checks;
  }

  async getWebhookConfig() {
    try {
      const [rows] = await this.pool.query('SELECT payload FROM provider_checks WHERE provider = ?', ['__webhook_config']);
      if (rows.length > 0 && rows[0].payload) {
        return JSON.parse(rows[0].payload);
      }
    } catch {}
    return { enabled: false, url: '', to: '' };
  }

  async setWebhookConfig(config) {
    const payload = JSON.stringify({
      enabled: Boolean(config.enabled),
      url: String(config.url || ''),
      to: String(config.to || '')
    });
    const now = this._now();
    await this.pool.query(
      'INSERT INTO provider_checks (provider, payload, checked_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE payload = ?, checked_at = ?',
      ['__webhook_config', payload, now, payload, now]
    );
    return config;
  }

  async updateJobHeartbeat(jobId) {
    await this._ready();
    const now = this._now();
    await this.pool.query(
      'UPDATE jobs SET heartbeat_at = ? WHERE id = ?',
      [now, jobId]
    );
  }

  async getStaleProcessingJobs(staleThresholdMinutes = 10) {
    await this._ready();
    const cutoffIso = new Date(Date.now() - staleThresholdMinutes * 60 * 1000).toISOString();
    const [rows] = await this.pool.query(
      `SELECT * FROM jobs
       WHERE status = 'processing'
         AND (heartbeat_at IS NULL OR heartbeat_at < ?)
         AND updated_at < ?`,
      [cutoffIso, cutoffIso]
    );
    return rows.map((row) => this._mapJobRow(row));
  }

  async resetStaleProcessingJobs(staleThresholdMinutes = 10) {
    await this._ready();
    const now = this._now();
    const cutoffIso = new Date(Date.now() - staleThresholdMinutes * 60 * 1000).toISOString();
    const [result] = await this.pool.query(
      `UPDATE jobs
       SET status = 'pending',
           heartbeat_at = NULL,
           updated_at = ?
       WHERE status = 'processing'
         AND (heartbeat_at IS NULL OR heartbeat_at < ?)
         AND updated_at < ?`,
      [now, cutoffIso, cutoffIso]
    );
    return { resetCount: result.affectedRows || 0 };
  }

  async createBatchSession(sessionData) {
    await this._ready();
    const now = this._now();
    const id = sessionData.id || uuidv4();
    const expiresAt = sessionData.expiresAt
      ? new Date(sessionData.expiresAt).toISOString()
      : new Date(Date.now() + 6 * 60 * 60 * 1000).toISOString();

    await this.pool.query(
      `INSERT INTO batch_sessions (
        id, run_id, root_cg_id, root_cg_name, target_cg_selector, parent_container_name,
        status, total_containers, processed_containers, queued_count, skipped_count,
        next_container_offset, has_more, error, session_data, queued_items, skipped_items,
        chain_errors, created_at, updated_at, expires_at, finished_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        sessionData.runId || null,
        sessionData.rootCgId || '',
        sessionData.rootCgName || null,
        sessionData.targetCgSelector || null,
        sessionData.parentContainerName || null,
        sessionData.status || 'running',
        toInt(sessionData.totalContainers, 0),
        toInt(sessionData.processedContainers, 0),
        toInt(sessionData.queuedCount, 0),
        toInt(sessionData.skippedCount, 0),
        toInt(sessionData.nextContainerOffset, 0),
        sessionData.hasMore !== false ? 1 : 0,
        sessionData.error || null,
        stringifyJson(sessionData.sessionData || {}),
        stringifyJson(sessionData.queuedItems || []),
        stringifyJson(sessionData.skippedItems || []),
        stringifyJson(sessionData.chainErrors || []),
        now,
        now,
        expiresAt,
        null
      ]
    );

    return this.getBatchSession(id);
  }

  async updateBatchSession(sessionId, updates) {
    await this._ready();
    const now = this._now();

    const fields = [];
    const params = [];

    const allowedFields = {
      status: 'status',
      rootCgName: 'root_cg_name',
      targetCgSelector: 'target_cg_selector',
      parentContainerName: 'parent_container_name',
      totalContainers: 'total_containers',
      processedContainers: 'processed_containers',
      queuedCount: 'queued_count',
      skippedCount: 'skipped_count',
      nextContainerOffset: 'next_container_offset',
      hasMore: 'has_more',
      error: 'error',
      sessionData: 'session_data',
      queuedItems: 'queued_items',
      skippedItems: 'skipped_items',
      chainErrors: 'chain_errors',
      runId: 'run_id'
    };

    for (const [key, column] of Object.entries(allowedFields)) {
      if (updates[key] !== undefined) {
        fields.push(`${column} = ?`);
        if (['sessionData', 'queuedItems', 'skippedItems', 'chainErrors'].includes(key)) {
          params.push(stringifyJson(updates[key]));
        } else if (key === 'hasMore') {
          params.push(updates[key] ? 1 : 0);
        } else if (['totalContainers', 'processedContainers', 'queuedCount', 'skippedCount', 'nextContainerOffset'].includes(key)) {
          params.push(toInt(updates[key]));
        } else {
          params.push(updates[key]);
        }
      }
    }

    if (updates.status && ['completed', 'failed', 'cancelled'].includes(updates.status)) {
      fields.push('finished_at = ?');
      params.push(now);
    }

    fields.push('updated_at = ?');
    params.push(now);
    params.push(sessionId);

    await this.pool.query(
      `UPDATE batch_sessions SET ${fields.join(', ')} WHERE id = ?`,
      params
    );

    return this.getBatchSession(sessionId);
  }

  async getBatchSession(sessionId) {
    await this._ready();
    const [rows] = await this.pool.query(
      'SELECT * FROM batch_sessions WHERE id = ? LIMIT 1',
      [sessionId]
    );
    if (!rows[0]) return null;
    return this._mapBatchSessionRow(rows[0]);
  }

  async getActiveBatchSessions() {
    await this._ready();
    const [rows] = await this.pool.query(
      "SELECT * FROM batch_sessions WHERE status IN ('running', 'pending') ORDER BY created_at DESC"
    );
    return rows.map((row) => this._mapBatchSessionRow(row));
  }

  async getBatchSessionsByRunId(runId) {
    await this._ready();
    const [rows] = await this.pool.query(
      'SELECT * FROM batch_sessions WHERE run_id = ? ORDER BY created_at DESC',
      [runId]
    );
    return rows.map((row) => this._mapBatchSessionRow(row));
  }

  async cleanupExpiredBatchSessions() {
    await this._ready();
    const now = this._now();
    const [result] = await this.pool.query(
      'DELETE FROM batch_sessions WHERE expires_at IS NOT NULL AND expires_at < ?',
      [now]
    );
    return { deletedCount: result.affectedRows || 0 };
  }

  _mapBatchSessionRow(row) {
    return {
      id: row.id,
      runId: row.run_id,
      rootCgId: row.root_cg_id,
      rootCgName: row.root_cg_name,
      targetCgSelector: row.target_cg_selector,
      parentContainerName: row.parent_container_name,
      status: row.status,
      totalContainers: toInt(row.total_containers),
      processedContainers: toInt(row.processed_containers),
      queuedCount: toInt(row.queued_count),
      skippedCount: toInt(row.skipped_count),
      nextContainerOffset: toInt(row.next_container_offset),
      hasMore: toBool(row.has_more),
      error: row.error,
      sessionData: parseJson(row.session_data, {}),
      queuedItems: parseJson(row.queued_items, []),
      skippedItems: parseJson(row.skipped_items, []),
      chainErrors: parseJson(row.chain_errors, []),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      expiresAt: row.expires_at,
      finishedAt: row.finished_at
    };
  }

  async close() {
    if (this.pool) {
      await this.pool.end();
    }
  }
}

// Singleton instance
let instance = null;

module.exports = {
  getInstance: () => {
    if (!instance) {
      instance = new DatabaseHandler();
    }
    return instance;
  },
  DatabaseHandler
};
