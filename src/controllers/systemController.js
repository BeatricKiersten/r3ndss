const { db, uploaderService } = require('../services/runtime');
const config = require('../config');

const systemController = {
  async getRcloneConfig(req, res) {
    try {
      const config = await db.getRcloneConfig();
      res.json({ success: true, data: config });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async updateRcloneConfig(req, res) {
    try {
      const updated = await db.updateRcloneConfig(req.body || {});
      res.json({ success: true, data: updated });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async validateRclone(req, res) {
    try {
      const backblazeStatus = await uploaderService.checkSingleProviderStatus('backblaze');
      const persisted = await db.setRcloneValidationResult(backblazeStatus);
      res.json({
        success: true,
        data: {
          ...backblazeStatus,
          lastValidatedAt: persisted.lastValidatedAt || null
        }
      });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getPrimaryProvider(req, res) {
    try {
      const [primaryProvider, providerConfigs] = await Promise.all([
        db.getPrimaryProvider(),
        db.getProviderConfigs()
      ]);

      res.json({
        success: true,
        data: {
          primaryProvider,
          enabled: providerConfigs?.[primaryProvider]?.enabled !== false
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async updatePrimaryProvider(req, res) {
    try {
      const provider = String(req.body?.provider || '');

      if (!config.supportedProviders.includes(provider)) {
        return res.status(400).json({ success: false, error: 'Invalid provider' });
      }

      const providerConfigs = await db.getProviderConfigs();
      if (providerConfigs?.[provider]?.enabled === false) {
        return res.status(400).json({ success: false, error: 'Primary provider must be enabled' });
      }

      const primaryProvider = await db.setPrimaryProvider(provider);
      res.json({ success: true, data: { primaryProvider, enabled: true } });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async runCheck(req, res) {
    try {
      const { providers, autoReuploadMissing = false } = req.body || {};
      const selectedProviders = Array.isArray(providers) && providers.length > 0
        ? providers.filter((provider) => config.supportedProviders.includes(provider))
        : [...config.supportedProviders];

      if (selectedProviders.length === 0) {
        return res.status(400).json({ success: false, error: 'No valid providers selected' });
      }

      const providerResults = {};
      for (const provider of selectedProviders) {
        providerResults[provider] = await uploaderService.checkProviderIntegrity(provider, {
          autoReuploadMissing: Boolean(autoReuploadMissing)
        });
      }

      const results = {
        totalFiles: Object.values(providerResults).reduce((acc, item) => acc + item.totalFiles, 0),
        checked: Object.values(providerResults).reduce((acc, item) => acc + item.checked, 0),
        issues: Object.values(providerResults).flatMap((item) => item.issues || []),
        reuploadsQueued: Object.values(providerResults).flatMap((item) => item.reuploadsQueued || []),
        providers: providerResults,
        checkedProviders: selectedProviders,
        checkedAt: new Date().toISOString()
      };

      await db.setLastCheckTime(results.checkedAt);
      res.json({ success: true, data: results });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getLastCheck(req, res) {
    try {
      const [lastCheck, nextScheduledCheck] = await Promise.all([
        db.getLastCheckTime(),
        db.getNextScheduledCheck()
      ]);
      res.json({ success: true, data: { lastCheck, nextScheduledCheck } });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
};

module.exports = systemController;
