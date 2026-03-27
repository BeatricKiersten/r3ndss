const express = require('express');
const router = express.Router();
const dashboardController = require('../controllers/dashboardController');

router.get('/', dashboardController.getData);
router.get('/stats', dashboardController.getStats);
router.get('/processes', dashboardController.getProcesses);

module.exports = router;
