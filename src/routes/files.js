const express = require('express');
const router = express.Router();
const fileController = require('../controllers/fileController');

router.get('/', fileController.list);
router.post('/bulk/delete-failed', fileController.deleteAllFailed);
router.post('/bulk/delete-problem', fileController.deleteAllProblemFiles);
router.get('/:id', fileController.get);
router.get('/:id/status', fileController.getStatus);
router.get('/:id/providers/status', fileController.getProvidersStatus);
router.put('/:id/move', fileController.move);
router.delete('/:id', fileController.delete);
router.post('/:id/delete-force', fileController.deleteForce);
router.post('/:id/retry', fileController.retry);
router.post('/:id/reupload/:provider', fileController.reupload);
router.post('/:id/copy/:targetProvider', fileController.copy);
router.delete('/:id/providers/:provider', fileController.clearProviderLink);

module.exports = router;
