const express = require('express');
const router = express.Router();
const jobController = require('../controllers/jobController');
const uploadController = require('../controllers/uploadController');
const asyncHandler = require('../middleware/asyncHandler');

router.get('/', asyncHandler(jobController.list));
router.post('/transfer', asyncHandler(uploadController.transfer));
router.post('/cancel-all', asyncHandler(jobController.cancelAll));
router.post('/clear-logs', asyncHandler(jobController.clearLogs));
router.post('/wipe-all', asyncHandler(jobController.wipeAll));
router.post('/:id/cancel', asyncHandler(jobController.cancel));
router.delete('/:id', asyncHandler(jobController.delete));

module.exports = router;
