const path = require('path');
const { videoProcessor, uploaderService, db } = require('../services/runtime');

const uploadController = {
  async processHls(req, res) {
    try {
      const {
        url,
        folderId = 'root',
        filename,
        decryptionKey,
        headers,
        providers
      } = req.body;

      if (!url) {
        return res.status(400).json({ success: false, error: 'HLS URL is required' });
      }

      videoProcessor.processHls(url, {
        folderId,
        outputName: filename,
        decryptionKey,
        headers
      }).then(async (result) => {
        await uploaderService.queueFileUpload(
          result.fileId,
          result.outputPath,
          folderId,
          providers
        );
      }).catch(error => {
        console.error('[Process] Failed:', error);
      });

      res.status(202).json({
        success: true,
        message: 'HLS processing started',
        data: { url, folderId, providers }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async transfer(req, res) {
    try {
      const {
        sourceUrl,
        targetProvider = 'seekstreaming',
        folderId = 'root',
        filename = null
      } = req.body || {};

      if (!sourceUrl) {
        return res.status(400).json({ success: false, error: 'sourceUrl is required' });
      }

      const result = await uploaderService.queueTransferJob({
        sourceUrl,
        targetProvider,
        folderId,
        filename
      });

      res.status(202).json({
        success: true,
        message: 'Transfer job queued',
        data: result
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async uploadFile(req, res) {
    try {
      const uploadedFile = req.file;
      const folderId = req.body?.folderId || 'root';
      const requestedFilename = String(req.body?.filename || '').trim();
      let providers = null;

      if (req.body?.providers) {
        try {
          providers = JSON.parse(req.body.providers);
        } catch {
          return res.status(400).json({ success: false, error: 'Invalid providers payload' });
        }
      }

      if (!uploadedFile) {
        return res.status(400).json({ success: false, error: 'Video file is required' });
      }

      const extension = path.extname(uploadedFile.originalname || uploadedFile.filename || '') || '.mp4';
      const baseName = requestedFilename || path.basename(uploadedFile.originalname || uploadedFile.filename, extension);
      const normalizedName = path.extname(baseName) ? baseName : `${baseName}${extension}`;

      const file = await db.createFile({
        folderId,
        name: normalizedName,
        originalUrl: null,
        localPath: uploadedFile.path,
        size: uploadedFile.size || 0,
        duration: 0
      });

      await uploaderService.queueFileUpload(
        file.id,
        uploadedFile.path,
        folderId,
        Array.isArray(providers) ? providers : null
      );

      res.status(202).json({
        success: true,
        message: 'Video upload queued for backup and sync',
        data: {
          fileId: file.id,
          name: normalizedName,
          size: uploadedFile.size || 0,
          providers: Array.isArray(providers) ? providers : null
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  }
};

module.exports = uploadController;
