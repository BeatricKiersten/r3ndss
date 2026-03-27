const express = require('express');
const path = require('path');
const multer = require('multer');
const router = express.Router();
const uploadController = require('../controllers/uploadController');
const config = require('../config');

const storage = multer.diskStorage({
  destination: (_, __, cb) => {
    cb(null, config.uploadDir);
  },
  filename: (_, file, cb) => {
    const extension = path.extname(file.originalname || '') || '.mp4';
    const baseName = path.basename(file.originalname || `upload${extension}`, extension)
      .replace(/[^a-zA-Z0-9-_\.]/g, '_')
      .slice(0, 120) || `upload_${Date.now()}`;

    cb(null, `${Date.now()}-${baseName}${extension}`);
  }
});

const upload = multer({
  storage,
  fileFilter: (_, file, cb) => {
    if (String(file.mimetype || '').startsWith('video/')) {
      cb(null, true);
      return;
    }

    cb(new Error('Only video uploads are allowed'));
  }
});

router.post('/hls', uploadController.processHls);
router.post('/file', upload.single('file'), uploadController.uploadFile);
router.post('/transfer', uploadController.transfer);

module.exports = router;
