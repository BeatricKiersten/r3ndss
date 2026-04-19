const axios = require('axios');

class WebhookService {
  constructor() {
    this._config = null;
    this._crashBuffer = [];
    this._crashFlushTimer = null;
    this._crashFlushInFlight = null;
    this._crashFlushIntervalMs = 10 * 60 * 1000;
    this._maxBufferedCrashEvents = 200;
  }

  startCrashBuffering() {
    if (this._crashFlushTimer) {
      return;
    }

    this._crashFlushTimer = setInterval(() => {
      this.flushCrashAlerts().catch((error) => {
        console.error('[Webhook] Scheduled crash alert flush failed:', error.message);
      });
    }, this._crashFlushIntervalMs);

    if (typeof this._crashFlushTimer.unref === 'function') {
      this._crashFlushTimer.unref();
    }

    console.log(`[Webhook] Crash alert buffering enabled (${Math.floor(this._crashFlushIntervalMs / 60000)} minute window)`);
  }

  stopCrashBuffering() {
    if (!this._crashFlushTimer) {
      return;
    }

    clearInterval(this._crashFlushTimer);
    this._crashFlushTimer = null;
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

  async sendCrashAlert(db, payload = {}) {
    this.startCrashBuffering();
    this._crashBufferEvent(payload);

    return {
      success: true,
      buffered: true,
      queued: this._crashBuffer.length
    };
  }

  async flushCrashAlerts(db, { force = false } = {}) {
    await this._ensureConfig(db);

    if (!this._config.enabled || !this._config.url || !this._config.to) {
      if (force && this._crashBuffer.length > 0) {
        console.log('[Webhook] Crash alert flush skipped: disabled or incomplete config');
      }
      return null;
    }

    if (this._crashFlushInFlight) {
      return this._crashFlushInFlight;
    }

    if (this._crashBuffer.length === 0) {
      return { success: true, flushed: 0 };
    }

    const events = this._crashBuffer.splice(0, this._crashBuffer.length);
    const text = this._formatCrashSummaryMessage(events);

    this._crashFlushInFlight = this._postMessage(text, 'Crash alert summary', async () => {
      return { success: true, flushed: events.length };
    }, async (error) => {
      this._crashBuffer.unshift(...events.slice(0, this._maxBufferedCrashEvents));
      return { success: false, error: error.message, flushed: 0 };
    });

    try {
      return await this._crashFlushInFlight;
    } finally {
      this._crashFlushInFlight = null;
    }
  }

  _crashBufferEvent(payload) {
    if (this._crashBuffer.length >= this._maxBufferedCrashEvents) {
      this._crashBuffer.shift();
    }

    this._crashBuffer.push({
      type: String(payload?.type || 'process-error'),
      message: String(payload?.message || 'Unknown error'),
      stack: String(payload?.stack || ''),
      timestamp: payload?.timestamp || new Date().toISOString(),
      pid: payload?.pid,
      uptime: payload?.uptime,
      hostname: payload?.hostname,
      nodeEnv: payload?.nodeEnv,
      extra: payload?.extra && typeof payload.extra === 'object' ? payload.extra : {}
    });
  }

  async _postMessage(text, logLabel, onSuccess, onError) {
    try {
      const response = await axios.post(this._config.url, {
        to: this._config.to,
        text
      }, {
        timeout: 15000,
        validateStatus: () => true
      });

      console.log(`[Webhook] ${logLabel} sent: ${response.status}`);
      return onSuccess ? await onSuccess(response) : { success: true, status: response.status };
    } catch (error) {
      console.error(`[Webhook] ${logLabel} failed:`, error.message);
      return onError ? await onError(error) : { success: false, error: error.message };
    }
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
        lines.push(`  ${i + 1}. [${stageLabel}] ${containerLabel} > ${instanceLabel} | ${errorLabel}`);
      });
      if (itemErrors.length > 5) {
        lines.push(`  ... +${itemErrors.length - 5} lainnya`);
      }
    }

    return lines.join('\n');
  }

  _formatCrashMessage(data) {
    const {
      type = 'process-error',
      message = 'Unknown error',
      stack = '',
      timestamp,
      pid,
      uptime,
      hostname,
      nodeEnv,
      extra = {}
    } = data;

    const lines = [];
    lines.push('🚨 *SERVER CRASH ALERT*');
    lines.push('');
    lines.push(`• Type: ${String(type).slice(0, 80)}`);
    lines.push(`• Message: ${String(message).slice(0, 500)}`);
    if (timestamp) lines.push(`• Time: ${new Date(timestamp).toLocaleString('id-ID')}`);
    if (pid) lines.push(`• PID: ${pid}`);
    if (hostname) lines.push(`• Host: ${hostname}`);
    if (nodeEnv) lines.push(`• Env: ${nodeEnv}`);
    if (Number.isFinite(uptime)) lines.push(`• Uptime: ${Math.floor(uptime)}s`);

    const extraEntries = Object.entries(extra || {}).filter(([, value]) => value !== undefined && value !== null && value !== '');
    if (extraEntries.length > 0) {
      lines.push('');
      lines.push('📎 *Context:*');
      for (const [key, value] of extraEntries.slice(0, 8)) {
        lines.push(`• ${key}: ${String(value).slice(0, 200)}`);
      }
    }

    if (stack) {
      lines.push('');
      lines.push('```');
      lines.push(String(stack).slice(0, 1500));
      lines.push('```');
    }

    return lines.join('\n');
  }

  _formatCrashSummaryMessage(events) {
    const lines = [];
    const firstEvent = events[0] || {};
    const lastEvent = events[events.length - 1] || {};
    const countsByType = new Map();

    for (const event of events) {
      const type = String(event?.type || 'process-error');
      countsByType.set(type, Number(countsByType.get(type) || 0) + 1);
    }

    lines.push('🚨 *SERVER ERROR SUMMARY*');
    lines.push('');
    lines.push(`• Total Errors: ${events.length}`);
    if (firstEvent.timestamp) lines.push(`• First Error: ${new Date(firstEvent.timestamp).toLocaleString('id-ID')}`);
    if (lastEvent.timestamp) lines.push(`• Last Error: ${new Date(lastEvent.timestamp).toLocaleString('id-ID')}`);
    if (lastEvent.hostname) lines.push(`• Host: ${lastEvent.hostname}`);
    if (lastEvent.nodeEnv) lines.push(`• Env: ${lastEvent.nodeEnv}`);
    lines.push('');
    lines.push('📊 *By Type:*');

    for (const [type, count] of countsByType.entries()) {
      lines.push(`• ${type}: ${count}`);
    }

    lines.push('');
    lines.push('📋 *Recent Errors:*');

    events.slice(0, 10).forEach((event, index) => {
      const when = event?.timestamp ? new Date(event.timestamp).toLocaleTimeString('id-ID') : '-';
      lines.push(`${index + 1}. [${String(event?.type || 'process-error').slice(0, 40)}] ${when} | ${String(event?.message || 'Unknown error').slice(0, 180)}`);
    });

    if (events.length > 10) {
      lines.push(`... +${events.length - 10} error lainnya`);
    }

    const stackSource = events.find((event) => event?.stack);
    if (stackSource?.stack) {
      lines.push('');
      lines.push('```');
      lines.push(String(stackSource.stack).slice(0, 1500));
      lines.push('```');
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
