#!/usr/bin/env node

require('dotenv').config();

const mysql = require('mysql2/promise');
const { Client } = require('pg');

const DEFAULT_POSTGRES_URL = 'postgresql://neondb_owner:npg_k31pGoLTjyMu@ep-sparkling-brook-aoeqdq6s.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require';
const CONNECTION_RETRY_ATTEMPTS = Math.max(1, Number(process.env.DB_MIGRATION_RETRY_ATTEMPTS || 5));
const CONNECTION_RETRY_DELAY_MS = Math.max(1000, Number(process.env.DB_MIGRATION_RETRY_DELAY_MS || 5000));

const TABLES = [
  {
    name: 'folders',
    createSql: `CREATE TABLE IF NOT EXISTS folders (
      id VARCHAR(64) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      parent_id VARCHAR(64) NULL,
      path TEXT NOT NULL,
      created_at VARCHAR(40) NOT NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders (parent_id)'
    ],
    orderBy: 'id'
  },
  {
    name: 'files',
    createSql: `CREATE TABLE IF NOT EXISTS files (
      id VARCHAR(64) PRIMARY KEY,
      folder_id VARCHAR(64) NOT NULL,
      name VARCHAR(512) NOT NULL,
      original_url TEXT NULL,
      local_path TEXT NULL,
      size BIGINT NOT NULL DEFAULT 0,
      duration DOUBLE PRECISION NOT NULL DEFAULT 0,
      status VARCHAR(32) NOT NULL,
      progress_download INTEGER NOT NULL DEFAULT 0,
      progress_processing INTEGER NOT NULL DEFAULT 0,
      progress_upload INTEGER NOT NULL DEFAULT 0,
      progress_extra TEXT NULL,
      sync_status INTEGER NOT NULL DEFAULT 0,
      can_delete BOOLEAN NOT NULL DEFAULT FALSE,
      created_at VARCHAR(40) NOT NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_files_folder ON files (folder_id)',
      'CREATE INDEX IF NOT EXISTS idx_files_folder_name ON files (folder_id, name)',
      'CREATE INDEX IF NOT EXISTS idx_files_status ON files (status)',
      'CREATE INDEX IF NOT EXISTS idx_files_created ON files (created_at)'
    ],
    orderBy: 'id'
  },
  {
    name: 'file_providers',
    createSql: `CREATE TABLE IF NOT EXISTS file_providers (
      file_id VARCHAR(64) NOT NULL,
      provider VARCHAR(64) NOT NULL,
      status VARCHAR(32) NOT NULL,
      url TEXT NULL,
      remote_file_id TEXT NULL,
      public_file_id TEXT NULL,
      embed_url TEXT NULL,
      error TEXT NULL,
      url_history TEXT NULL,
      updated_at VARCHAR(40) NOT NULL,
      PRIMARY KEY (file_id, provider)
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_file_providers_provider ON file_providers (provider)',
      'CREATE INDEX IF NOT EXISTS idx_file_providers_status ON file_providers (status)'
    ],
    orderBy: 'file_id, provider'
  },
  {
    name: 'jobs',
    createSql: `CREATE TABLE IF NOT EXISTS jobs (
      id VARCHAR(64) PRIMARY KEY,
      type VARCHAR(32) NOT NULL,
      file_id VARCHAR(64) NULL,
      status VARCHAR(32) NOT NULL,
      progress INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      error TEXT NULL,
      metadata TEXT NULL,
      created_at VARCHAR(40) NOT NULL,
      updated_at VARCHAR(40) NOT NULL,
      started_at VARCHAR(40) NULL,
      completed_at VARCHAR(40) NULL,
      heartbeat_at VARCHAR(40) NULL
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)',
      'CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at)',
      'CREATE INDEX IF NOT EXISTS idx_jobs_file ON jobs (file_id)',
      'CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (type)',
      'CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at)',
      'CREATE INDEX IF NOT EXISTS idx_jobs_heartbeat ON jobs (heartbeat_at)'
    ],
    orderBy: 'id'
  },
  {
    name: 'batch_sessions',
    createSql: `CREATE TABLE IF NOT EXISTS batch_sessions (
      id VARCHAR(64) PRIMARY KEY,
      run_id VARCHAR(64) NULL,
      root_cg_id VARCHAR(64) NOT NULL,
      root_cg_name VARCHAR(255) NULL,
      target_cg_selector VARCHAR(255) NULL,
      parent_container_name VARCHAR(255) NULL,
      status VARCHAR(32) NOT NULL DEFAULT 'running',
      total_containers INTEGER NOT NULL DEFAULT 0,
      processed_containers INTEGER NOT NULL DEFAULT 0,
      queued_count INTEGER NOT NULL DEFAULT 0,
      skipped_count INTEGER NOT NULL DEFAULT 0,
      next_container_offset INTEGER NOT NULL DEFAULT 0,
      has_more BOOLEAN NOT NULL DEFAULT TRUE,
      error TEXT NULL,
      session_data TEXT NULL,
      queued_items TEXT NULL,
      skipped_items TEXT NULL,
      chain_errors TEXT NULL,
      created_at VARCHAR(40) NOT NULL,
      updated_at VARCHAR(40) NOT NULL,
      expires_at VARCHAR(40) NULL,
      finished_at VARCHAR(40) NULL
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_sessions (status)',
      'CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_sessions (run_id)'
    ],
    orderBy: 'id'
  },
  {
    name: 'provider_configs',
    createSql: `CREATE TABLE IF NOT EXISTS provider_configs (
      provider VARCHAR(64) PRIMARY KEY,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      config TEXT NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [],
    orderBy: 'provider'
  },
  {
    name: 'system_state',
    createSql: `CREATE TABLE IF NOT EXISTS system_state (
      id SMALLINT PRIMARY KEY,
      last_check VARCHAR(40) NULL,
      next_scheduled_check VARCHAR(40) NULL,
      primary_provider VARCHAR(64) NOT NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [],
    orderBy: 'id'
  },
  {
    name: 'provider_checks',
    createSql: `CREATE TABLE IF NOT EXISTS provider_checks (
      provider VARCHAR(64) PRIMARY KEY,
      payload TEXT NULL,
      checked_at VARCHAR(40) NULL
    )`,
    indexes: [],
    orderBy: 'provider'
  },
  {
    name: 'rclone_remotes',
    createSql: `CREATE TABLE IF NOT EXISTS rclone_remotes (
      name VARCHAR(128) PRIMARY KEY,
      type VARCHAR(64) NOT NULL,
      parameters TEXT NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [],
    orderBy: 'name'
  },
  {
    name: 'rclone_sync_profiles',
    createSql: `CREATE TABLE IF NOT EXISTS rclone_sync_profiles (
      id VARCHAR(128) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      provider VARCHAR(64) NOT NULL,
      remote_name VARCHAR(128) NOT NULL,
      destination_path TEXT NULL,
      public_base_url TEXT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [
      'CREATE INDEX IF NOT EXISTS idx_rclone_profiles_provider ON rclone_sync_profiles (provider)',
      'CREATE INDEX IF NOT EXISTS idx_rclone_profiles_remote ON rclone_sync_profiles (remote_name)'
    ],
    orderBy: 'id'
  },
  {
    name: 'rclone_state',
    createSql: `CREATE TABLE IF NOT EXISTS rclone_state (
      id SMALLINT PRIMARY KEY,
      default_profile_id VARCHAR(128) NULL,
      last_validation TEXT NULL,
      last_validated_at VARCHAR(40) NULL,
      updated_at VARCHAR(40) NOT NULL
    )`,
    indexes: [],
    orderBy: 'id'
  }
];

function quoteIdentifier(value) {
  return `"${String(value).replace(/"/g, '""')}"`;
}

function quoteMysqlIdentifier(value) {
  return `\`${String(value).replace(/`/g, '``')}\``;
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function isTooManyConnectionsError(error) {
  const message = String(error?.message || '').toLowerCase();
  const code = String(error?.code || '').toUpperCase();

  return code === 'ER_CON_COUNT_ERROR'
    || code === '53300'
    || message.includes('too many connections')
    || message.includes('remaining connection slots are reserved');
}

async function connectWithRetry(label, factory) {
  let lastError = null;

  for (let attempt = 1; attempt <= CONNECTION_RETRY_ATTEMPTS; attempt += 1) {
    try {
      return await factory();
    } catch (error) {
      lastError = error;
      if (!isTooManyConnectionsError(error) || attempt === CONNECTION_RETRY_ATTEMPTS) {
        break;
      }

      console.warn(`[Migration] ${label} connection attempt ${attempt}/${CONNECTION_RETRY_ATTEMPTS} failed: ${error.message}`);
      await sleep(CONNECTION_RETRY_DELAY_MS);
    }
  }

  throw new Error(`${label} connection failed: ${lastError.message}`);
}

function buildMysqlConfig() {
  const mysqlUrl = process.env.MYSQL_URL || process.env.DATABASE_URL;
  if (!mysqlUrl) {
    throw new Error('MYSQL_URL or DATABASE_URL is required');
  }

  const parsed = new URL(mysqlUrl);
  if (!['mysql:', 'mysql2:'].includes(parsed.protocol)) {
    throw new Error('MYSQL_URL must use mysql:// or mysql2:// protocol');
  }

  const sslMode = String(
    parsed.searchParams.get('ssl-mode') ||
    parsed.searchParams.get('sslmode') ||
    parsed.searchParams.get('ssl_mode') ||
    ''
  ).toLowerCase();

  const useSsl = ['required', 'require', 'verify_ca', 'verify_identity'].includes(sslMode)
    || String(parsed.searchParams.get('ssl') || '').toLowerCase() === 'true'
    || String(process.env.MYSQL_SSL || 'false').toLowerCase() === 'true';

  return {
    host: parsed.hostname,
    port: Number(parsed.port || 3306),
    user: decodeURIComponent(parsed.username || ''),
    password: decodeURIComponent(parsed.password || ''),
    database: decodeURIComponent((parsed.pathname || '').replace(/^\//, '')),
    ssl: useSsl ? { rejectUnauthorized: String(process.env.MYSQL_SSL_REJECT_UNAUTHORIZED || 'true').toLowerCase() !== 'false' } : undefined
  };
}

function buildPostgresConfig() {
  const connectionString = process.env.POSTGRES_MIGRATION_URL || process.env.POSTGRES_URL || DEFAULT_POSTGRES_URL;
  const parsed = new URL(connectionString);
  const sslmode = String(parsed.searchParams.get('sslmode') || '').toLowerCase();
  const needsSsl = ['require', 'verify-ca', 'verify-full'].includes(sslmode) || parsed.hostname.includes('neon.tech');

  return {
    connectionString,
    ssl: needsSsl ? { rejectUnauthorized: false } : false
  };
}

async function getMysqlColumns(connection, tableName) {
  const [rows] = await connection.query(`SHOW COLUMNS FROM ${quoteMysqlIdentifier(tableName)}`);
  return rows.map((row) => row.Field);
}

async function mysqlTableExists(connection, tableName) {
  const [rows] = await connection.query('SHOW TABLES LIKE ?', [tableName]);
  return rows.length > 0;
}

async function ensurePostgresSchema(client) {
  for (const table of TABLES) {
    await client.query(table.createSql);
    for (const indexSql of table.indexes) {
      await client.query(indexSql);
    }
  }
}

async function truncatePostgresTables(client) {
  const names = TABLES.map((table) => quoteIdentifier(table.name)).join(', ');
  await client.query(`TRUNCATE TABLE ${names}`);
}

function normalizeValue(columnName, value) {
  if (value === undefined || value === null) {
    return null;
  }

  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (columnName === 'can_delete' || columnName === 'enabled' || columnName === 'has_more') {
    return value === true || value === 1 || value === '1';
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return value;
}

async function copyTable(mysqlConnection, pgClient, table) {
  const tableExists = await mysqlTableExists(mysqlConnection, table.name);
  if (!tableExists) {
    console.log(`[Migration] ${table.name}: skipped, source table not found`);
    return 0;
  }

  const columns = await getMysqlColumns(mysqlConnection, table.name);
  const selectSql = `SELECT * FROM ${quoteMysqlIdentifier(table.name)} ORDER BY ${table.orderBy}`;
  const [rows] = await mysqlConnection.query(selectSql);

  if (!rows.length) {
    console.log(`[Migration] ${table.name}: 0 rows`);
    return 0;
  }

  const quotedColumns = columns.map(quoteIdentifier).join(', ');
  const batchSize = 200;
  let inserted = 0;

  for (const batch of chunkArray(rows, batchSize)) {
    const values = [];
    const placeholders = batch.map((row, rowIndex) => {
      const tuple = columns.map((column, columnIndex) => {
        values.push(normalizeValue(column, row[column]));
        return `$${rowIndex * columns.length + columnIndex + 1}`;
      });
      return `(${tuple.join(', ')})`;
    });

    const insertSql = `INSERT INTO ${quoteIdentifier(table.name)} (${quotedColumns}) VALUES ${placeholders.join(', ')}`;
    await pgClient.query(insertSql, values);
    inserted += batch.length;
  }

  console.log(`[Migration] ${table.name}: ${inserted} rows`);
  return inserted;
}

async function main() {
  const mysqlConnection = await connectWithRetry('MySQL', async () => mysql.createConnection(buildMysqlConfig()));
  const pgClient = await connectWithRetry('PostgreSQL', async () => {
    const client = new Client(buildPostgresConfig());
    await client.connect();
    return client;
  });

  try {
    console.log('[Migration] Preparing PostgreSQL schema...');
    await pgClient.query('BEGIN');
    await ensurePostgresSchema(pgClient);
    await truncatePostgresTables(pgClient);

    let totalRows = 0;
    for (const table of TABLES) {
      totalRows += await copyTable(mysqlConnection, pgClient, table);
    }

    await pgClient.query('COMMIT');
    console.log(`[Migration] Completed successfully. Total rows copied: ${totalRows}`);
  } catch (error) {
    await pgClient.query('ROLLBACK');
    throw error;
  } finally {
    await pgClient.end();
    await mysqlConnection.end();
  }
}

main().catch((error) => {
  console.error(`[Migration] Failed: ${error.message}`);
  process.exit(1);
});
