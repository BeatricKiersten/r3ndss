const { db, uploaderService, videoProcessor } = require('../services/runtime');

const dashboardController = {
  async getData(req, res) {
    try {
      const data = await db.getDashboardData();
      res.json({ success: true, data });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getStats(req, res) {
    try {
      const stats = await db.getStats();
      res.json({ success: true, data: stats });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getProcesses(req, res) {
    res.json({
      success: true,
      data: {
        ffmpeg: videoProcessor.getActiveProcesses(),
        uploads: uploaderService.getStats()
      }
    });
  }
};

module.exports = dashboardController;
