#!/usr/bin/env node

/**
 * One-time migration utility:
 * JSON database (legacy) -> MySQL
 *
 * Usage:
 *   node src/scripts/migrate-json-to-mysql.js [path-to-db.json]
 */

const fs = require('fs-extra');
const path = require('path');
const { getInstance } = require('../db/handler');

const SUPPORTED_PROVIDERS = ['voesx', 'catbox', 'seekstreaming', 'rclone'];

function toIso(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date.toISOString();
}

function ensureObject(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value;
  }
  return {};
}

async function run() {
  const sourceArg = process.argv[2];
  const sourcePath = path.resolve(sourceArg || process.env.JSON_DATABASE_PATH || './data/db.json');

  if (!(await fs.pathExists(sourcePath))) {
    throw new Error(`JSON source not found: ${sourcePath}`);
  }

  const legacy = await fs.readJson(sourcePath);
  const now = new Date().toISOString();

  const db = getInstance();
  await db.getProviderConfigs();

  const pool = db.pool;
  if (!pool) {
    throw new Error('MySQL pool is not initialized');
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    await connection.query('DELETE FROM file_providers');
    await connection.query('DELETE FROM jobs');
    await connection.query('DELETE FROM files');
    await connection.query('DELETE FROM folders');
    await connection.query('DELETE FROM provider_configs');
    await connection.query('DELETE FROM provider_checks');
    await connection.query('DELETE FROM rclone_sync_profiles');
    await connection.query('DELETE FROM rclone_remotes');
    await connection.query('DELETE FROM rclone_state');
    await connection.query('DELETE FROM system_state');

    const folders = Array.isArray(legacy.folders) ? legacy.folders : [];
    const rootFolder = folders.find((folder) => folder.id === 'root') || {
      id: 'root',
      name: 'Root',
      parentId: null,
      path: '/',
      createdAt: now,
      updatedAt: now
    };

    const normalizedFolders = [
      {
        id: String(rootFolder.id || 'root'),
        name: String(rootFolder.name || 'Root'),
        parentId: null,
        path: String(rootFolder.path || '/'),
        createdAt: toIso(rootFolder.createdAt, now),
        updatedAt: toIso(rootFolder.updatedAt, now)
      },
      ...folders
        .filter((folder) => folder.id && folder.id !== 'root')
        .map((folder) => ({
          id: String(folder.id),
          name: String(folder.name || 'Untitled'),
          parentId: folder.parentId ? String(folder.parentId) : 'root',
          path: String(folder.path || `/${folder.name || folder.id}`),
          createdAt: toIso(folder.createdAt, now),
          updatedAt: toIso(folder.updatedAt, now)
        }))
    ];

    for (const folder of normalizedFolders) {
      await connection.query(
        `INSERT INTO folders (id, name, parent_id, path, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [folder.id, folder.name, folder.parentId, folder.path, folder.createdAt, folder.updatedAt]
      );
    }

    const files = Array.isArray(legacy.files) ? legacy.files : [];
    for (const file of files) {
      const fileId = String(file.id || '');
      if (!fileId) continue;

      const progress = ensureObject(file.progress);
      const progressExtra = { ...progress };
      delete progressExtra.download;
      delete progressExtra.processing;
      delete progressExtra.upload;

      await connection.query(
        `INSERT INTO files (
          id, folder_id, name, original_url, local_path, size, duration, status,
          progress_download, progress_processing, progress_upload, progress_extra,
          sync_status, can_delete, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          fileId,
          String(file.folderId || 'root'),
          String(file.name || `file-${fileId}`),
          file.originalUrl || null,
          file.localPath || null,
          Number(file.size) || 0,
          Number(file.duration) || 0,
          String(file.status || 'processing'),
          Number(progress.download) || 0,
          Number(progress.processing) || 0,
          Number(progress.upload) || 0,
          JSON.stringify(progressExtra),
          Number(file.syncStatus) || 0,
          file.canDelete ? 1 : 0,
          toIso(file.createdAt, now),
          toIso(file.updatedAt, now)
        ]
      );

      const providerState = ensureObject(file.providers);
      for (const provider of SUPPORTED_PROVIDERS) {
        const state = ensureObject(providerState[provider]);

        await connection.query(
          `INSERT INTO file_providers
            (file_id, provider, status, url, remote_file_id, embed_url, error, url_history, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            fileId,
            provider,
            String(state.status || 'pending'),
            state.url || null,
            state.fileId || null,
            state.embedUrl || null,
            state.error || null,
            JSON.stringify(Array.isArray(state.urlHistory) ? state.urlHistory : []),
            toIso(state.updatedAt, toIso(file.updatedAt, now))
          ]
        );
      }
    }

    const jobs = Array.isArray(legacy.jobs) ? legacy.jobs : [];
    for (const job of jobs) {
      const id = String(job.id || '');
      if (!id) continue;

      await connection.query(
        `INSERT INTO jobs (
          id, type, file_id, status, progress, attempts, max_attempts,
          error, metadata, created_at, updated_at, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          id,
          String(job.type || 'upload'),
          job.fileId || null,
          String(job.status || 'pending'),
          Number(job.progress) || 0,
          Number(job.attempts) || 0,
          Number(job.maxAttempts) || 3,
          job.error || null,
          JSON.stringify(ensureObject(job.metadata)),
          toIso(job.createdAt, now),
          toIso(job.updatedAt, now),
          toIso(job.startedAt, null),
          toIso(job.completedAt, null)
        ]
      );
    }

    const providerConfigs = ensureObject(legacy.providers);
    for (const provider of SUPPORTED_PROVIDERS) {
      const current = ensureObject(providerConfigs[provider]);
      await connection.query(
        `INSERT INTO provider_configs (provider, enabled, config, updated_at)
         VALUES (?, ?, ?, ?)`,
        [
          provider,
          current.enabled === false ? 0 : 1,
          JSON.stringify(ensureObject(current.config)),
          toIso(current.updatedAt, now)
        ]
      );
    }

    const system = ensureObject(legacy.system);
    await connection.query(
      `INSERT INTO system_state (id, last_check, next_scheduled_check, primary_provider, updated_at)
       VALUES (1, ?, ?, ?, ?)`,
      [
        toIso(system.lastCheck, null),
        toIso(system.nextScheduledCheck, null),
        SUPPORTED_PROVIDERS.includes(system.primaryProvider) ? system.primaryProvider : 'catbox',
        now
      ]
    );

    const providerChecks = ensureObject(system.providerChecks);
    for (const provider of SUPPORTED_PROVIDERS) {
      const snapshot = providerChecks[provider] === null ? null : ensureObject(providerChecks[provider]);
      await connection.query(
        `INSERT INTO provider_checks (provider, payload, checked_at)
         VALUES (?, ?, ?)`,
        [
          provider,
          snapshot ? JSON.stringify(snapshot) : null,
          snapshot ? toIso(snapshot.checkedAt, now) : null
        ]
      );
    }

    const rclone = ensureObject(system.rclone);
    const remotes = Array.isArray(rclone.remotes) ? rclone.remotes : [];
    const profiles = Array.isArray(rclone.syncProfiles) ? rclone.syncProfiles : [];

    for (const remote of remotes) {
      if (!remote?.name || !remote?.type) continue;
      await connection.query(
        `INSERT INTO rclone_remotes (name, type, parameters, updated_at)
         VALUES (?, ?, ?, ?)`,
        [
          String(remote.name),
          String(remote.type),
          JSON.stringify(ensureObject(remote.parameters)),
          now
        ]
      );
    }

    for (const profile of profiles) {
      if (!profile?.id || !profile?.name || !profile?.remoteName) continue;
      await connection.query(
        `INSERT INTO rclone_sync_profiles
          (id, name, provider, remote_name, destination_path, public_base_url, enabled, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          String(profile.id),
          String(profile.name),
          String(profile.provider || 'rclone'),
          String(profile.remoteName),
          String(profile.destinationPath || ''),
          String(profile.publicBaseUrl || ''),
          profile.enabled === false ? 0 : 1,
          now
        ]
      );
    }

    await connection.query(
      `INSERT INTO rclone_state (id, default_profile_id, last_validation, last_validated_at, updated_at)
       VALUES (1, ?, ?, ?, ?)`,
      [
        rclone.defaultProfileId || null,
        rclone.lastValidation ? JSON.stringify(rclone.lastValidation) : null,
        toIso(rclone.lastValidatedAt, null),
        now
      ]
    );

    await connection.commit();

    console.log(`[Migration] Success. Imported ${normalizedFolders.length} folders, ${files.length} files, ${jobs.length} jobs.`);
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
    await db.close();
  }
}

run().catch((error) => {
  console.error(`[Migration] Failed: ${error.message}`);
  process.exit(1);
});
