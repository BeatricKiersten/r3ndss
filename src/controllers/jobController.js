const { db, uploaderService, videoProcessor } = require('../services/runtime');
const AppError = require('../errors/AppError');
const { success } = require('../utils/apiResponse');

const jobController = {
  async list(req, res) {
    const { status, type, fileId, limit } = req.query;
    const jobs = await uploaderService.listJobs({
      status: status || null,
      type: type || null,
      fileId: fileId || null,
      limit: limit ? Number(limit) : 200
    });
    res.json(success(jobs));
  },

  async cancel(req, res) {
    let job;

    try {
      job = await db.getJob(req.params.id);
    } catch (error) {
      throw new AppError('Job not found', { statusCode: 404, code: 'JOB_NOT_FOUND' });
    }

    if (job.type === 'process') {
      const cancelled = await videoProcessor.cancelJob(req.params.id);
      await db.updateJob(req.params.id, {
        status: 'cancelled',
        error: cancelled ? 'Cancelled by user' : 'Cancellation requested'
      });
      return res.json(success({ cancelled }));
    }

    const result = await uploaderService.cancelJob(req.params.id);
    res.json(success(result));
  },

  async delete(req, res) {
    const result = await uploaderService.deleteJob(req.params.id);
    res.json(success(result));
  },

  async cancelAll(req, res) {
    const activeJobs = await uploaderService.listJobs({
      status: null,
      limit: 1000
    });

    const jobsToCancel = activeJobs.filter((job) => (
      job.status === 'pending' || job.status === 'processing'
    ));

    const results = {
      cancelled: 0,
      failed: 0,
      details: []
    };

    for (const job of jobsToCancel) {
      try {
        if (job.type === 'process') {
          await videoProcessor.cancelJob(job.id);
          await db.updateJob(job.id, {
            status: 'cancelled',
            error: 'Cancelled by user (bulk cancel)'
          });
        } else {
          await uploaderService.cancelJob(job.id);
        }
        results.cancelled += 1;
        results.details.push({ jobId: job.id, status: 'cancelled' });
      } catch (err) {
        results.failed += 1;
        results.details.push({ jobId: job.id, status: 'error', error: err.message });
      }
    }

    res.json(success(results, {
      message: `Cancelled ${results.cancelled} jobs`
    }));
  },

  async clearLogs(req, res) {
    const result = await db.deleteCompletedJobs();
    res.json(success(result, {
      message: `Cleared ${result.deletedCount} completed/failed/cancelled jobs`
    }));
  },

  async wipeAll(req, res) {
    const jobs = await db.getAllJobs();
    const results = {
      totalJobs: jobs.length,
      abortedProcesses: 0,
      abortedUploads: 0,
      deletedCount: 0,
      errors: []
    };

    for (const job of jobs) {
      try {
        if (job.status !== 'pending' && job.status !== 'processing') {
          continue;
        }

        if (job.type === 'process') {
          const aborted = await videoProcessor.cancelJob(job.id);
          if (aborted) {
            results.abortedProcesses += 1;
          }
          continue;
        }

        const provider = job.metadata?.provider || job.metadata?.targetProvider;
        if (provider) {
          const aborted = await uploaderService.cancelUpload(job.id, provider);
          if (aborted) {
            results.abortedUploads += 1;
          }
        }
      } catch (error) {
        results.errors.push({ jobId: job.id, error: error.message });
      }
    }

    const deleted = await db.deleteAllJobs();
    results.deletedCount = deleted.deletedCount;

    res.json(success(results, {
      message: `Deleted ${results.deletedCount} jobs and cleared all logs`
    }));
  }
};

module.exports = jobController;
