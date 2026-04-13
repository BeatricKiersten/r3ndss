const { db, uploaderService } = require('../services/runtime');

const providerController = {
  async list(req, res) {
    try {
      const configs = await db.getProviderConfigs();
      const sanitized = Object.entries(configs).reduce((acc, [key, value]) => {
        acc[key] = {
          enabled: value.enabled,
          configured: value.configured !== false,
          name: value.name || key,
          kind: value.kind || 'unknown',
          source: value.source || 'config',
          profileId: value.profileId || null,
          remoteName: value.remoteName || null,
          remoteType: value.remoteType || null,
          supportsStream: value.supportsStream !== false,
          supportsReupload: value.supportsReupload !== false,
          supportsCopy: value.supportsCopy !== false
        };
        return acc;
      }, {});
      res.json({ success: true, data: sanitized });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getAllStatus(req, res) {
    try {
      const status = await uploaderService.checkProvidersStatus();
      res.json({ success: true, data: status });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async getStatus(req, res) {
    try {
      const status = await uploaderService.checkSingleProviderStatus(req.params.name);
      res.json({ success: true, data: status });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async getCheckSnapshots(req, res) {
    try {
      const snapshots = await uploaderService.getProviderCheckSnapshots();
      res.json({ success: true, data: snapshots });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async update(req, res) {
    try {
      const config = await db.updateProviderConfig(req.params.name, req.body);
      res.json({ success: true, data: config });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async check(req, res) {
    try {
      const { provider } = req.params;
      const { autoReuploadMissing = false } = req.body || {};
      const result = await uploaderService.checkProviderIntegrity(provider, {
        autoReuploadMissing: Boolean(autoReuploadMissing)
      });
      await db.setLastCheckTime(new Date().toISOString());
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  },

  async checkBulk(req, res) {
    try {
      const { providers, autoReuploadMissing = false } = req.body || {};
      const providerCatalog = await uploaderService.getProviderCatalog({ includeDisabled: false });
      const availableProviderIds = new Set(providerCatalog.map((item) => item.id));

      const selectedProviders = Array.isArray(providers) && providers.length > 0
        ? providers.filter((provider) => availableProviderIds.has(provider))
        : providerCatalog.map((item) => item.id);

      if (selectedProviders.length === 0) {
        return res.status(400).json({ success: false, error: 'No valid providers selected' });
      }

      const providerResults = {};
      for (const provider of selectedProviders) {
        providerResults[provider] = await uploaderService.checkProviderIntegrity(provider, {
          autoReuploadMissing: Boolean(autoReuploadMissing)
        });
      }

      const summary = {
        providers: selectedProviders,
        totalFiles: Object.values(providerResults).reduce((acc, item) => acc + item.totalFiles, 0),
        checked: Object.values(providerResults).reduce((acc, item) => acc + item.checked, 0),
        issues: Object.values(providerResults).flatMap((item) => item.issues || []),
        reuploadsQueued: Object.values(providerResults).flatMap((item) => item.reuploadsQueued || []),
        providerResults,
        checkedAt: new Date().toISOString()
      };

      await db.setLastCheckTime(summary.checkedAt);
      res.json({ success: true, data: summary });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  },

  async clearMissingLinks(req, res) {
    try {
      const { provider } = req.params;
      const reason = String(req.body?.reason || '').trim() || `Provider link removed by user after missing-remote check for ${provider}`;
      const result = await uploaderService.clearMissingProviderLinks(provider, { reason });
      res.json({ success: true, data: result });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  }
};

module.exports = providerController;
