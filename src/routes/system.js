const express = require('express');
const router = express.Router();
const systemController = require('../controllers/systemController');

router.get('/primary-provider', systemController.getPrimaryProvider);
router.put('/primary-provider', systemController.updatePrimaryProvider);
router.post('/check', systemController.runCheck);
router.get('/last-check', systemController.getLastCheck);
router.get('/rclone-config', systemController.getRcloneConfig);
router.put('/rclone-config', systemController.updateRcloneConfig);
router.post('/rclone-config/validate', systemController.validateRclone);

module.exports = router;
