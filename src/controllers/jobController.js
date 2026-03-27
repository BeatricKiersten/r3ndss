const { db, uploaderService, videoProcessor } = require('../services/runtime');

const jobController = {
  async list(req, res) {
    try {
      const { status, type, fileId, limit } = req.query;
      const jobs = await uploaderService.listJobs({
        status: status || null,
        type: type || null,
        fileId: fileId || null,
        limit: limit ? Number(limit) : 200
      });
      res.json({ success: true, data: jobs });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async cancel(req, res) {
    try {
      const job = await db.getJob(req.params.id);
      if (!job) {
        return res.status(404).json({ success: false, error: 'Job not found' });
      }

      if (job.type === 'process') {
        const cancelled = await videoProcessor.cancelJob(req.params.id);
        await db.updateJob(req.params.id, {
          status: 'cancelled',
          error: cancelled ? 'Cancelled by user' : 'Cancellation requested'
        });
        return res.json({ success: true, data: { cancelled } });
      }

      const result = await uploaderService.cancelJob(req.params.id);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async delete(req, res) {
    try {
      const result = await uploaderService.deleteJob(req.params.id);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  }
};

module.exports = jobController;
