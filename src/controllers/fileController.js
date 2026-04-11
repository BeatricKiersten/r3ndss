const { db, uploaderService } = require('../services/runtime');

const fileController = {
  async list(req, res) {
    try {
      const { folderId, status } = req.query;
      const files = await db.listFiles(folderId, status);
      res.json({ success: true, data: files });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async get(req, res) {
    try {
      const file = await db.getFile(req.params.id);
      res.json({ success: true, data: file });
    } catch (error) {
      res.status(404).json({ success: false, error: error.message });
    }
  },

  async getStatus(req, res) {
    try {
      const status = await uploaderService.getUploadStatus(req.params.id);
      res.json({ success: true, data: status });
    } catch (error) {
      res.status(404).json({ success: false, error: error.message });
    }
  },

  async getProvidersStatus(req, res) {
    try {
      const status = await uploaderService.checkFileProvidersStatus(req.params.id);
      res.json({ success: true, data: status });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async move(req, res) {
    try {
      const { folderId } = req.body;
      const file = await db.moveFile(req.params.id, folderId);
      res.json({ success: true, data: file });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async delete(req, res) {
    try {
      const result = await db.markFileDeleted(req.params.id);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async deleteForce(req, res) {
    try {
      const fileId = req.params.id;
      const file = await db.getFile(fileId);
      const remote = await uploaderService.deleteFileResources(fileId);
      const purged = await db.purgeFileAndJobs(fileId);
      res.json({ success: true, data: { ...remote, ...purged } });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async retry(req, res) {
    try {
      const result = await uploaderService.retryFailedUploads(req.params.id);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async reupload(req, res) {
    try {
      const { source } = req.body;
      const result = await uploaderService.reuploadToProvider(req.params.id, req.params.provider, source);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async copy(req, res) {
    try {
      const result = await uploaderService.copyToProvider(req.params.id, req.params.targetProvider);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async deleteAllFailed(req, res) {
    try {
      const failedFiles = await db.listFiles(null, 'failed');
      const results = [];

      for (const file of failedFiles) {
        try {
          await uploaderService.deleteFileResources(file.id);
          await db.purgeFileAndJobs(file.id);
          results.push({ fileId: file.id, name: file.name, deleted: true });
        } catch (error) {
          results.push({ fileId: file.id, name: file.name, deleted: false, error: error.message });
        }
      }

      res.json({ success: true, data: { total: failedFiles.length, results } });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
};

module.exports = fileController;
