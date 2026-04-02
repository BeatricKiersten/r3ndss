const express = require('express');
const router = express.Router();
const zeniusController = require('../controllers/zeniusController');

router.post('/details', zeniusController.getInstanceDetails);
router.post('/download', zeniusController.download);
router.post('/batch-chain', zeniusController.getBatchChain);
router.post('/batch-download', zeniusController.downloadBatch);

// New management endpoints
router.post('/cancel-all', zeniusController.cancelAll);
router.post('/reset-files', zeniusController.resetFiles);
router.get('/queue-status', zeniusController.getQueueStatus);

module.exports = router;
