const path = require('path');
const { runProcessUploadPipeline, uploaderService, db, normalizePipelineError } = require('../services/runtime');
const AppError = require('../errors/AppError');
const { success } = require('../utils/apiResponse');

const uploadController = {
  async processHls(req, res) {
    const {
      url,
      folderId = 'root',
      filename,
      decryptionKey,
      headers,
      providers
    } = req.body;

    if (!url) {
      throw new AppError('HLS URL is required', { statusCode: 400, code: 'HLS_URL_REQUIRED' });
    }

    void runProcessUploadPipeline(url, {
      folderId,
      outputName: filename,
      decryptionKey,
      headers,
      selectedProviders: providers,
      waitForUpload: false
    }).catch((error) => {
      const normalizedError = normalizePipelineError(error, 'PROCESS_UPLOAD_FAILED');
      console.error('[Process] Failed:', {
        url,
        folderId,
        message: normalizedError.message,
        code: normalizedError.code,
        stack: error.stack || null
      });
    });

    res.status(202).json(success({
      url,
      folderId,
      providers,
      state: 'queued'
    }, {
      message: 'HLS processing started'
    }));
  },

  async transfer(req, res) {
    const {
      sourceUrl,
      targetProvider = 'seekstreaming',
      folderId = 'root',
      filename = null
    } = req.body || {};

    if (!sourceUrl) {
      throw new AppError('sourceUrl is required', { statusCode: 400, code: 'SOURCE_URL_REQUIRED' });
    }

    const result = await uploaderService.queueTransferJob({
      sourceUrl,
      targetProvider,
      folderId,
      filename
    });

    res.status(202).json(success(result, {
      message: 'Transfer job queued'
    }));
  },

  async uploadFile(req, res) {
    const uploadedFile = req.file;
    const folderId = req.body?.folderId || 'root';
    const requestedFilename = String(req.body?.filename || '').trim();
    let providers = null;

    if (req.body?.providers) {
      try {
        providers = JSON.parse(req.body.providers);
      } catch {
        throw new AppError('Invalid providers payload', { statusCode: 400, code: 'INVALID_PROVIDERS_PAYLOAD' });
      }
    }

    if (!uploadedFile) {
      throw new AppError('Video file is required', { statusCode: 400, code: 'VIDEO_FILE_REQUIRED' });
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

    res.status(202).json(success({
      fileId: file.id,
      name: normalizedName,
      size: uploadedFile.size || 0,
      providers: Array.isArray(providers) ? providers : null
    }, {
      message: 'Video upload queued for backup and sync'
    }));
  }
};

module.exports = uploadController;
