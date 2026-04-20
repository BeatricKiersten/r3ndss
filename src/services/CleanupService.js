/**
 * CleanupService - Periodic cleanup scheduler
 *
 * Handles:
 * 1. Periodic scan of download dir for orphaned files (no DB record)
 * 2. Force-cleanup of stuck jobs (processing/uploading > threshold)
 * 3. Cleanup of expired batch sessions in DB
 * 4. Forced local file removal for files whose jobs are all finished
 * 5. Post-batch cleanup trigger
 */

const fs = require('fs-extra');
const path = require('path');

// How often the main cleanup cycle runs
const CLEANUP_INTERVAL_MS = Number(process.env.CLEANUP_INTERVAL_MS || 30 * 60 * 1000); // 30 min
// How long a job can be stuck in 'processing' before being force-reset
const STUCK_JOB_THRESHOLD_MINUTES = Number(process.env.STUCK_JOB_THRESHOLD_MINUTES || 60); // 60 min
// Grace period before deleting orphaned local files (files in upload dir with no DB record)
const ORPHAN_GRACE_PERIOD_MS = Number(process.env.ORPHAN_GRACE_PERIOD_MS || 2 * 60 * 60 * 1000); // 2 hours
// Maximum age of a completed/failed batch session before it gets purged from DB
const BATCH_SESSION_RETAIN_MS = Number(process.env.BATCH_SESSION_RETAIN_MS || 7 * 24 * 60 * 60 * 1000); // 7 days

class CleanupService {
  /**
   * @param {{ db: import('./runtime').db, uploadDir: string, uploaderService?: object }} options
   */
  constructor({ db, uploadDir, uploaderService = null }) {
    this.db = db;
    this.uploadDir = path.resolve(uploadDir);
    this.uploaderService = uploaderService;
    this._timer = null;
    this._running = false;
    this._cycleCount = 0;
  }

  start() {
    if (this._timer) return;
    console.log(`[Cleanup] Service started – interval=${CLEANUP_INTERVAL_MS / 1000}s`);
    // Run once shortly after startup, then repeat
    setTimeout(() => this._runCycle(), 60_000);
    this._timer = setInterval(() => this._runCycle(), CLEANUP_INTERVAL_MS);
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    console.log('[Cleanup] Service stopped');
  }

  /** Manually trigger a full cleanup cycle (e.g. post-batch) */
  async runNow(label = 'manual') {
    return this._runCycle(label);
  }

  // ─────────────────────────────────────────────
  // MAIN CYCLE
  // ─────────────────────────────────────────────

  async _runCycle(label = 'scheduled') {
    if (this._running) {
      console.log('[Cleanup] Skipping cycle – previous cycle still running');
      return;
    }

    this._running = true;
    this._cycleCount += 1;
    const cycleId = this._cycleCount;
    console.log(`[Cleanup] Cycle #${cycleId} (${label}) started`);
    const summary = { cycleId, label, startedAt: new Date().toISOString() };
    let filesSnapshot = [];

    try {
      filesSnapshot = await this.db.listFiles().catch(() => []);
      summary.stuckJobs = await this._fixStuckJobs();
      summary.expiredSessions = await this._cleanupExpiredBatchSessions();
      summary.orphanedFiles = await this._cleanupOrphanedLocalFiles(filesSnapshot);
      summary.localFileCleanup = await this._forceLocalFileCleanupForFinishedFiles(filesSnapshot);
    } catch (error) {
      console.error(`[Cleanup] Cycle #${cycleId} encountered an error: ${error.message}`);
      summary.error = error.message;
    } finally {
      this._running = false;
      summary.finishedAt = new Date().toISOString();
      const dur = new Date(summary.finishedAt) - new Date(summary.startedAt);
      console.log(`[Cleanup] Cycle #${cycleId} done in ${dur}ms –`, JSON.stringify(summary));
    }

    return summary;
  }

  // ─────────────────────────────────────────────
  // 1. FIX STUCK JOBS
  // ─────────────────────────────────────────────

  async _fixStuckJobs() {
    const result = { reset: 0, errors: [] };

    try {
      // Reset jobs stuck in 'processing' longer than the threshold
      const staleResult = await this.db.resetStaleProcessingJobs(STUCK_JOB_THRESHOLD_MINUTES);
      result.reset = staleResult?.resetCount || 0;

      if (result.reset > 0) {
        console.log(`[Cleanup] Reset ${result.reset} stuck processing jobs back to pending`);
      }
    } catch (error) {
      console.error('[Cleanup] Failed to reset stuck jobs:', error.message);
      result.errors.push(error.message);
    }

    // Additionally: find jobs stuck in 'uploading' state (status set by provider
    // during upload but never cleared after abort). These are different from
    // 'processing' (which resetStaleProcessingJobs handles). We reset them too.
    try {
      const stuckUploading = await this._resetStuckUploadingJobs();
      result.stuckUploading = stuckUploading;
    } catch (error) {
      console.error('[Cleanup] Failed to reset stuck uploading jobs:', error.message);
      result.errors.push(`uploading: ${error.message}`);
    }

    return result;
  }

  async _resetStuckUploadingJobs() {
    // Jobs stuck in 'uploading' for > threshold shouldn't exist – they're
    // a transient in-memory state. If they survived a restart they're orphaned.
    let resetCount = 0;
    try {
      if (typeof this.db.getStaleUploadingJobs === 'function') {
        const staleJobs = await this.db.getStaleUploadingJobs(STUCK_JOB_THRESHOLD_MINUTES);
        for (const job of staleJobs) {
          await this.db.updateJob(job.id, {
            status: 'pending',
            error: `Reset by cleanup: stuck in 'uploading' since ${job.updatedAt}`
          }).catch(() => {});
          resetCount += 1;
        }
      }
    } catch (_) {
      // Not critical – skip silently
    }

    return resetCount;
  }

  // ─────────────────────────────────────────────
  // 2. CLEANUP EXPIRED BATCH SESSIONS
  // ─────────────────────────────────────────────

  async _cleanupExpiredBatchSessions() {
    const result = { deleted: 0, errors: [] };

    try {
      const cleanupResult = await this.db.cleanupExpiredBatchSessions();
      result.deleted = cleanupResult?.deletedCount || 0;

      if (result.deleted > 0) {
        console.log(`[Cleanup] Deleted ${result.deleted} expired batch sessions`);
      }
    } catch (error) {
      console.error('[Cleanup] Failed to cleanup expired batch sessions:', error.message);
      result.errors.push(error.message);
    }

    return result;
  }

  // ─────────────────────────────────────────────
  // 3. CLEANUP ORPHANED LOCAL FILES
  // ─────────────────────────────────────────────

  /**
   * Scans UPLOAD_DIR for .mp4 files that have no DB record (or whose DB
   * record is deleted) and deletes them after the grace period.
   */
  async _cleanupOrphanedLocalFiles(filesSnapshot = null) {
    const result = { scanned: 0, deleted: 0, skipped: 0, errors: [] };

    try {
      const exists = await fs.pathExists(this.uploadDir);
      if (!exists) return result;

      // Build a set of all known local paths from DB
      const knownPaths = await this._getKnownLocalPaths(filesSnapshot);

      // Walk the upload dir (one level only – flat structure)
      const entries = await fs.readdir(this.uploadDir).catch(() => []);

      for (const entry of entries) {
        const fullPath = path.join(this.uploadDir, entry);
        result.scanned += 1;

        try {
          const stat = await fs.stat(fullPath);

          // Only target regular files (not directories)
          if (!stat.isFile()) {
            result.skipped += 1;
            continue;
          }

          const ext = path.extname(entry).toLowerCase();
          // Only clean up video/media files – never touch configs or other assets
          if (!['.mp4', '.mkv', '.webm', '.m3u8', '.ts'].includes(ext)) {
            result.skipped += 1;
            continue;
          }

          // Check if the file is known in DB
          const resolvedPath = path.resolve(fullPath);
          if (knownPaths.has(resolvedPath)) {
            result.skipped += 1;
            continue;
          }

          // Unknown file – check age
          const ageMs = Date.now() - stat.mtimeMs;
          if (ageMs < ORPHAN_GRACE_PERIOD_MS) {
            result.skipped += 1;
            continue;
          }

          // Old enough – delete it
          await fs.remove(fullPath);
          result.deleted += 1;
          console.log(`[Cleanup] Removed orphaned local file: ${entry} (age ${Math.round(ageMs / 60000)}min)`);
        } catch (fileError) {
          result.errors.push(`${entry}: ${fileError.message}`);
        }
      }
    } catch (error) {
      console.error('[Cleanup] Orphaned file scan failed:', error.message);
      result.errors.push(error.message);
    }

    return result;
  }

  async _getKnownLocalPaths(filesSnapshot = null) {
    const paths = new Set();

    try {
      const files = Array.isArray(filesSnapshot) ? filesSnapshot : await this.db.listFiles();
      for (const file of files) {
        if (file.localPath) {
          paths.add(path.resolve(file.localPath));
        }
      }
    } catch (_) {
      // Ignore – if we can't query DB, skip path filtering (conservative)
    }

    return paths;
  }

  // ─────────────────────────────────────────────
  // 4. FORCE LOCAL FILE CLEANUP FOR FINISHED FILES
  // ─────────────────────────────────────────────

  /**
   * Find files that still have a localPath set in DB but all their jobs are
   * completed/failed/cancelled. These files should have been deleted already
   * but weren't (e.g. the scheduled cleanup timer was lost on restart).
   */
  async _forceLocalFileCleanupForFinishedFiles(filesSnapshot = null) {
    const result = { checked: 0, cleaned: 0, skipped: 0, errors: [] };

    try {
      const files = Array.isArray(filesSnapshot) ? filesSnapshot : await this.db.listFiles();
      const filesWithLocalPath = files.filter((file) => file?.localPath);
      const jobActivityByFileId = await this.db.getJobActivityByFileIds(filesWithLocalPath.map((file) => file.id));

      for (const file of filesWithLocalPath) {

        result.checked += 1;

        try {
          // Check if local file actually exists
          const localExists = await fs.pathExists(file.localPath);
          if (!localExists) {
            // Path is stale in DB – clear it
            await this.db.updateFileLocalPath(file.id, null).catch(() => {});
            result.cleaned += 1;
            continue;
          }

          // Get all jobs for this file
          const jobActivity = jobActivityByFileId.get(String(file.id));
          if (!jobActivity || jobActivity.totalCount === 0) {
            result.skipped += 1;
            continue;
          }

          if (jobActivity.activeCount > 0) {
            result.skipped += 1;
            continue;
          }

          // All jobs are in final state – safe to delete local file
          const resolvedPath = path.resolve(file.localPath);
          const resolvedUploadDir = path.resolve(this.uploadDir);

          if (!resolvedPath.startsWith(resolvedUploadDir)) {
            // Safety: never delete files outside UPLOAD_DIR
            result.skipped += 1;
            continue;
          }

          await fs.remove(resolvedPath).catch(() => {});
          await this.db.updateFileLocalPath(file.id, null).catch(() => {});
          result.cleaned += 1;

          console.log(`[Cleanup] Removed finished-file local copy: ${file.name} (${path.basename(resolvedPath)})`);
        } catch (fileError) {
          result.errors.push(`${file.id}: ${fileError.message}`);
        }
      }
    } catch (error) {
      console.error('[Cleanup] Force local file cleanup failed:', error.message);
      result.errors.push(error.message);
    }

    return result;
  }
}

module.exports = { CleanupService };
