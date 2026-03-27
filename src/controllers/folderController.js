const { getInstance: getDb } = require('../db/handler');

const db = getDb();

const folderController = {
  async getTree(req, res) {
    try {
      const tree = await db.getFolderTree();
      res.json({ success: true, data: tree });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getFolder(req, res) {
    try {
      const folderId = req.params.id || 'root';
      const folder = await db.getFolder(folderId);
      res.json({ success: true, data: folder });
    } catch (error) {
      res.status(404).json({ success: false, error: error.message });
    }
  },

  async create(req, res) {
    try {
      const { name, parentId } = req.body;
      const folder = await db.createFolder(name, parentId);
      res.status(201).json({ success: true, data: folder });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async move(req, res) {
    try {
      const { newParentId } = req.body;
      const folder = await db.moveFolder(req.params.id, newParentId);
      res.json({ success: true, data: folder });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async delete(req, res) {
    try {
      const folder = await db.deleteFolder(req.params.id);
      res.json({ success: true, data: folder });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async purge(req, res) {
    try {
      const result = await db.purgeFolder(req.params.id);
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  }
};

module.exports = folderController;
