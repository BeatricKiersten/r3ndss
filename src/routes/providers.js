const express = require('express');
const router = express.Router();
const providerController = require('../controllers/providerController');

router.get('/', providerController.list);
router.get('/status', providerController.getAllStatus);
router.get('/check-snapshots', providerController.getCheckSnapshots);
router.get('/:name/status', providerController.getStatus);
router.put('/:name', providerController.update);
router.post('/:provider/check', providerController.check);
router.post('/check', providerController.checkBulk);

module.exports = router;
