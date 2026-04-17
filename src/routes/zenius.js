const express = require('express');
const router = express.Router();
const zeniusController = require('../controllers/zeniusController');
const asyncHandler = require('../middleware/asyncHandler');

router.post('/details', asyncHandler(zeniusController.getInstanceDetails));
router.post('/download', asyncHandler(zeniusController.download));
router.post('/batch-chain', asyncHandler(zeniusController.getBatchChain));
router.post('/batch-download', asyncHandler(zeniusController.downloadBatch));
router.get('/batch-sessions', asyncHandler(zeniusController.getBatchSessions));
router.get('/batch-sessions/:id', asyncHandler(zeniusController.getBatchSessionStatus));

// New management endpoints
router.post('/cancel-all', asyncHandler(zeniusController.cancelAll));
router.post('/reset-files', asyncHandler(zeniusController.resetFiles));
router.get('/queue-status', asyncHandler(zeniusController.getQueueStatus));
router.put('/max-concurrent', asyncHandler(zeniusController.setMaxConcurrent));
router.get('/upload-concurrency', asyncHandler(zeniusController.getUploadConcurrency));
router.put('/upload-concurrency', asyncHandler(zeniusController.setUploadConcurrency));
router.get('/webhook', asyncHandler(zeniusController.getWebhookConfig));
router.put('/webhook', asyncHandler(zeniusController.updateWebhookConfig));
router.post('/webhook/test', asyncHandler(zeniusController.testWebhook));

module.exports = router;
