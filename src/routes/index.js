const express = require('express');
const router = express.Router();
const dashboardController = require('../controllers/dashboardController');

router.use('/folders', require('./folders'));
router.use('/files', require('./files'));
router.use('/jobs', require('./jobs'));
router.use('/providers', require('./providers'));
router.use('/dashboard', require('./dashboard'));
router.use('/system', require('./system'));
router.use('/upload', require('./upload'));
router.use('/zenius', require('./zenius'));
router.get('/stats', dashboardController.getStats);
router.get('/processes', dashboardController.getProcesses);

module.exports = router;
