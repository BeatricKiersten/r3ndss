const axios = require('axios');

class WebhookService {
  constructor() {
    this._config = null;
  }

  async _ensureConfig(db) {
    if (!this._config) {
      await this.loadConfig(db);
    }
  }

  async loadConfig(db) {
    try {
      const row = await db.getWebhookConfig();
      this._config = {
        enabled: Boolean(row?.enabled),
        url: String(row?.url || ''),
        to: String(row?.to || '')
      };
    } catch (error) {
      console.error('[Webhook] Failed to load config:', error.message);
      this._config = { enabled: false, url: '', to: '' };
    }
    return this._config;
  }

  getConfig() {
    return this._config || { enabled: false, url: '', to: '' };
  }

  async updateConfig(db, updates) {
    await this._ensureConfig(db);

    this._config = {
      enabled: typeof updates.enabled === 'boolean' ? updates.enabled : this._config.enabled,
      url: typeof updates.url === 'string' ? updates.url : this._config.url,
      to: typeof updates.to === 'string' ? updates.to : this._config.to
    };

    await db.setWebhookConfig(this._config);
    console.log(`[Webhook] Config saved: enabled=${this._config.enabled}, url=${this._config.url ? '***' : '(empty)'}, to=${this._config.to || '(empty)'}`);
    return this._config;
  }

  async sendBatchComplete(db, batchData) {
    await this._ensureConfig(db);

    if (!this._config.enabled || !this._config.url) {
      console.log('[Webhook] Skipped: disabled or no URL configured');
      return null;
    }

    const text = this._formatBatchMessage(batchData);

    try {
      const response = await axios.post(this._config.url, {
        to: this._config.to,
        text
      }, {
        timeout: 15000,
        validateStatus: () => true
      });

      console.log(`[Webhook] Batch notification sent: ${response.status}`);
      return { success: true, status: response.status };
    } catch (error) {
      console.error('[Webhook] Batch notification failed:', error.message);
      return { success: false, error: error.message };
    }
  }

  async sendTest(db) {
    await this._ensureConfig(db);

    if (!this._config.url) throw new Error('Webhook URL not configured');
    if (!this._config.to) throw new Error('Recipient number not configured');

    const response = await axios.post(this._config.url, {
      to: this._config.to,
      text: '🔔 Test notification dari Zenius Batch Downloader\n\nWebhook berhasil terhubung!'
    }, {
      timeout: 15000,
      validateStatus: () => true
    });

    return { status: response.status, data: response.data };
  }

  _formatBatchMessage(data) {
    const {
      id,
      rootCgId,
      rootCgName,
      targetCgSelector,
      parentContainerName,
      status,
      totalContainers,
      processedContainers,
      scannedContainerCount,
      discoveredVideoCount,
      queuedCount,
      skippedCount,
      downloadCompletedCount,
      downloadFailedCount,
      itemErrors,
      error,
      chainErrors,
      sessionId,
      startedAt,
      finishedAt
    } = data;

    const duration = this._formatDuration(startedAt, finishedAt);
    const containerCount = Number(scannedContainerCount || processedContainers || 0);
    const totalVideoCount = Number(discoveredVideoCount || queuedCount || 0) + Number(skippedCount || 0);
    const failedCount = Number(downloadFailedCount || 0);
    const completedCount = Number(downloadCompletedCount || 0);
    const queueRate = totalVideoCount > 0 ? ((Number(queuedCount || 0) / totalVideoCount) * 100).toFixed(1) : '0.0';
    const statusIcon = status === 'completed' ? '✅' : status === 'failed' ? '❌' : '⚠️';

    const lines = [];

    lines.push(`${statusIcon} *BATCH ${status.toUpperCase()}*`);
    lines.push('');

    lines.push('📋 *Info:*');
    lines.push(`• Batch ID: \`${id?.slice(0, 12) || '-'}...\``);
    lines.push(`• Root CG: ${rootCgName || rootCgId || '-'}`);
    if (targetCgSelector) lines.push(`• Target: ${targetCgSelector}`);
    if (parentContainerName) lines.push(`• Parent: ${parentContainerName}`);
    if (sessionId) lines.push(`• Session: \`${sessionId.slice(0, 12)}...\``);
    lines.push('');

    lines.push('📊 *Statistik:*');
    lines.push(`• Containers Scanned: ${containerCount}/${totalContainers}`);
    lines.push(`• Videos Found: ${totalVideoCount}`);
    lines.push(`• Video Queued: ${queuedCount}`);
    lines.push(`• Video Skipped: ${skippedCount}`);
    lines.push(`• Download Completed: ${completedCount}`);
    if (failedCount > 0) lines.push(`• Video Failed: ${failedCount}`);
    lines.push(`• Queue Rate: ${queueRate}%`);
    lines.push('');

    lines.push('⏱️ *Waktu:*');
    lines.push(`• Durasi: ${duration}`);
    if (startedAt) lines.push(`• Mulai: ${new Date(startedAt).toLocaleString('id-ID')}`);
    if (finishedAt) lines.push(`• Selesai: ${new Date(finishedAt).toLocaleString('id-ID')}`);

    if (error) {
      lines.push('');
      lines.push(`❌ *Error:* ${String(error).slice(0, 500)}`);
    }

    if (Array.isArray(chainErrors) && chainErrors.length > 0) {
      lines.push('');
      lines.push(`⚠️ *Chain Errors:* ${chainErrors.length}`);
      chainErrors.slice(0, 5).forEach((e, i) => {
        lines.push(`  ${i + 1}. ${String(e.message || e.error || JSON.stringify(e)).slice(0, 120)}`);
      });
      if (chainErrors.length > 5) {
        lines.push(`  ... +${chainErrors.length - 5} lainnya`);
      }
    }

    if (Array.isArray(itemErrors) && itemErrors.length > 0) {
      lines.push('');
      lines.push(`⚠️ *Item Errors:* ${itemErrors.length}`);
      itemErrors.slice(0, 5).forEach((item, i) => {
        const containerLabel = item.containerName || item.containerUrlShortId || '-';
        const instanceLabel = item.instanceName || item.instanceUrlShortId || '-';
        const stageLabel = item.stage || '-';
        const errorLabel = String(item.error || 'Unknown error').slice(0, 140);
        lines.push(`  ${i + 1}. [${stageLabel}] ${containerLabel} > ${instanceLabel}: ${errorLabel}`);
      });
      if (itemErrors.length > 5) {
        lines.push(`  ... +${itemErrors.length - 5} lainnya`);
      }
    }

    return lines.join('\n');
  }

  _formatDuration(startedAt, finishedAt) {
    if (!startedAt || !finishedAt) return '-';
    const start = new Date(startedAt).getTime();
    const end = new Date(finishedAt).getTime();
    if (!Number.isFinite(start) || !Number.isFinite(end)) return '-';
    const diff = Math.max(0, Math.floor((end - start) / 1000));

    const hours = Math.floor(diff / 3600);
    const minutes = Math.floor((diff % 3600) / 60);
    const seconds = diff % 60;

    const parts = [];
    if (hours > 0) parts.push(`${hours} jam`);
    if (minutes > 0) parts.push(`${minutes} menit`);
    parts.push(`${seconds} detik`);

    return parts.join(' ');
  }
}

module.exports = new WebhookService();
