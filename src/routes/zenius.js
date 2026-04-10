const express = require('express');
const router = express.Router();
const zeniusController = require('../controllers/zeniusController');

router.post('/details', zeniusController.getInstanceDetails);
router.post('/download', zeniusController.download);
router.post('/batch-chain', zeniusController.getBatchChain);
router.post('/batch-download', zeniusController.downloadBatch);
router.get('/batch-sessions', zeniusController.getBatchSessions);
router.get('/batch-sessions/:id', zeniusController.getBatchSessionStatus);

// New management endpoints
router.post('/cancel-all', zeniusController.cancelAll);
router.post('/reset-files', zeniusController.resetFiles);
router.get('/queue-status', zeniusController.getQueueStatus);
router.put('/max-concurrent', zeniusController.setMaxConcurrent);
router.get('/webhook', zeniusController.getWebhookConfig);
router.put('/webhook', zeniusController.updateWebhookConfig);
router.post('/webhook/test', zeniusController.testWebhook);

module.exports = router;
