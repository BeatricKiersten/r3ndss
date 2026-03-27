const express = require('express');
const router = express.Router();
const jobController = require('../controllers/jobController');
const uploadController = require('../controllers/uploadController');

router.get('/', jobController.list);
router.post('/transfer', uploadController.transfer);
router.post('/:id/cancel', jobController.cancel);
router.delete('/:id', jobController.delete);

module.exports = router;
