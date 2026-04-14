const pLimit = require('p-limit');
const { db, uploaderService } = require('../services/runtime');

const BULK_FILE_DELETE_CONCURRENCY = Math.max(1, Number(process.env.BULK_FILE_DELETE_CONCURRENCY || 4));

async function deleteFilesInBulk(files) {
  const limit = pLimit(BULK_FILE_DELETE_CONCURRENCY);
  const results = await Promise.all(files.map((file) => limit(async () => {
    try {
      await uploaderService.deleteFileResources(file.id);
      await db.purgeFileAndJobs(file.id);
      return { fileId: file.id, name: file.name, status: file.status, deleted: true };
    } catch (error) {
      return { fileId: file.id, name: file.name, status: file.status, deleted: false, error: error.message };
    }
  })));

  return {
    total: files.length,
    results
  };
}

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

  async clearProviderLink(req, res) {
    try {
      const reason = String(req.body?.reason || '').trim() || 'Provider link removed by user';
      const result = await uploaderService.clearFileProviderLink(req.params.id, req.params.provider, reason);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async refreshCompleteness(req, res) {
    try {
      const fileId = String(req.params.id || '').trim();
      if (fileId) {
        const result = await db.refreshFileCompleteness(fileId);
        res.json({ success: true, data: result });
        return;
      }

      const result = await db.refreshAllFilesCompleteness();
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async deleteAllFailed(req, res) {
    try {
      const failedFiles = await db.listFiles(null, 'failed');
      const result = await deleteFilesInBulk(failedFiles);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async deleteAllProblemFiles(req, res) {
    try {
      const files = await db.listFiles();
      const statuses = new Set(['processing', 'failed', 'cancelled']);
      const targetFiles = files.filter((file) => statuses.has(String(file.status || '').toLowerCase()));
      const result = await deleteFilesInBulk(targetFiles);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
};

module.exports = fileController;
