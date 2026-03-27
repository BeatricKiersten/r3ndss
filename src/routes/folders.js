const express = require('express');
const router = express.Router();
const folderController = require('../controllers/folderController');

router.get('/tree', folderController.getTree);
router.get('/:id?', folderController.getFolder);
router.post('/', folderController.create);
router.put('/:id/move', folderController.move);
router.delete('/:id/purge', folderController.purge);
router.delete('/:id', folderController.delete);

module.exports = router;
