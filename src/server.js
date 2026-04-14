/**
 * Express API Server - Entry Point
 * 
 * Main server with REST API and WebSocket for real-time updates
 */

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
const axios = require('axios');
const { createServer } = require('http');
const { WebSocketServer } = require('ws');

const config = require('./config');
const routes = require('./routes');
const errorHandler = require('./middleware/errorHandler');
const websocketHandler = require('./websocket/events');
const mediaController = require('./controllers/mediaController');
const { db, eventEmitter, uploaderService, cleanupService, rcloneServeService } = require('./services/runtime');
// Lazy-import to avoid circular deps — zeniusController loads runtime which is already loaded
let _zeniusController = null;
function getZeniusController() {
  if (!_zeniusController) _zeniusController = require('./controllers/zeniusController');
  return _zeniusController;
}

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(cors({
  origin: config.frontendUrl,
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

if (process.env.NODE_ENV !== 'production') {
  app.use((req, res, next) => {
    const startedAt = Date.now();
    const verbose = String(process.env.DEBUG_HTTP_VERBOSE || 'false').toLowerCase() === 'true';

    if (verbose) {
      console.log(`[HTTP] -> ${req.method} ${req.originalUrl}`, {
        query: req.query,
        body: req.body,
        headers: {
          'content-type': req.headers['content-type'],
          'user-agent': req.headers['user-agent'],
          origin: req.headers.origin
        }
      });
    } else {
      console.log(`[HTTP] -> ${req.method} ${req.originalUrl}`);
    }

    res.on('finish', () => {
      console.log(`[HTTP] <- ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - startedAt}ms`);
    });

    next();
  });
}

fs.ensureDirSync(config.uploadDir);

wss.on('connection', (ws) => {
  console.log('[WebSocket] Client connected');
  websocketHandler.addClient(ws);

  ws.on('close', () => {
    websocketHandler.removeClient(ws);
    console.log('[WebSocket] Client disconnected');
  });

  ws.on('error', (error) => {
    console.error('[WebSocket] Error:', error);
    websocketHandler.removeClient(ws);
  });
});

websocketHandler.initialize(eventEmitter);

app.use('/api', routes);

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    services: {
      database: 'connected',
      uploader: uploaderService.getStats()
    }
  });
});

app.get('/api/proxy/video', async (req, res) => {
  try {
    const targetUrl = String(req.query.url || '');
    if (!targetUrl) {
      return res.status(400).json({ success: false, error: 'url query is required' });
    }

    let parsed;
    try {
      parsed = new URL(targetUrl);
    } catch {
      return res.status(400).json({ success: false, error: 'Invalid url' });
    }

    const allowedHosts = new Set([
      'catbox.moe',
      'files.catbox.moe',
      'litterbox.catbox.moe',
      'voe.sx',
      'www.voe.sx',
      'streamtape.com',
      'www.streamtape.com',
      'mixdrop.co',
      'www.mixdrop.co'
    ]);

    if (parsed.protocol !== 'https:' || !allowedHosts.has(parsed.hostname)) {
      return res.status(403).json({ success: false, error: 'Host is not allowed' });
    }

    const upstreamHeaders = {
      'user-agent': req.headers['user-agent'] || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      accept: req.headers.accept || '*/*',
      referer: `https://${parsed.hostname}/`,
      origin: `https://${parsed.hostname}`
    };
    if (req.headers.range) {
      upstreamHeaders.range = req.headers.range;
    }

    const upstream = await axios.get(parsed.toString(), {
      responseType: 'stream',
      validateStatus: () => true,
      headers: upstreamHeaders,
      timeout: 30000
    });

    res.status(upstream.status);

    if (upstream.status >= 400) {
      const chunks = [];
      upstream.data.on('data', (chunk) => chunks.push(chunk));
      upstream.data.on('end', () => {
        const message = Buffer.concat(chunks).toString('utf8').slice(0, 400);
        res.end(message || `Upstream returned ${upstream.status}`);
      });
      return;
    }

    const passthroughHeaders = [
      'content-type', 'content-length', 'content-range',
      'accept-ranges', 'cache-control', 'etag', 'last-modified'
    ];

    passthroughHeaders.forEach((name) => {
      if (upstream.headers[name]) {
        res.setHeader(name, upstream.headers[name]);
      }
    });

    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-methods', 'GET, HEAD, OPTIONS');
    res.setHeader('access-control-allow-headers', 'Range, Content-Type');
    res.setHeader('cross-origin-resource-policy', 'cross-origin');
    res.setHeader('cross-origin-embedder-policy', 'credentialless');

    if (req.method === 'OPTIONS') {
      return res.status(200).end();
    }

    if (!res.getHeader('accept-ranges')) {
      res.setHeader('accept-ranges', 'bytes');
    }

    upstream.data.on('error', () => {
      if (!res.headersSent) {
        res.status(502).end('Upstream stream error');
      } else {
        res.end();
      }
    });

    req.on('close', () => {
      if (upstream?.data?.destroy) {
        upstream.data.destroy();
      }
    });

    upstream.data.pipe(res);
  } catch (error) {
    res.status(502).json({ success: false, error: error.message || 'Proxy error' });
  }
});

app.get('/uploads/:filename', async (req, res) => {
  try {
    const filePath = path.join(config.uploadDir, req.params.filename);
    if (!filePath.startsWith(path.resolve(config.uploadDir))) {
      return res.status(403).send('Access denied');
    }
    if (!await fs.pathExists(filePath)) {
      return res.status(404).send('File not found');
    }
    res.sendFile(filePath);
  } catch (error) {
    res.status(500).send('Server error');
  }
});

app.options('/media/rclone/*', mediaController.proxyRclone);
app.get('/media/rclone/*', mediaController.proxyRclone);

const frontendDistPath = path.join(__dirname, 'frontend/dist');
if (fs.existsSync(frontendDistPath)) {
  app.use(express.static(frontendDistPath));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/uploads')) {
      return next();
    }
    res.sendFile(path.join(frontendDistPath, 'index.html'));
  });
}

app.use(errorHandler);

async function startServer() {
  try {
    // Bind server to port ASAP to satisfy Heroku's 60s startup requirement
    server.listen(config.port, () => {
      console.log(`========================================`);
      console.log(`HLS-to-MP4 Backup Platform Server`);
      console.log(`Running on port ${config.port}`);
      console.log(`Initializing services...`);
      console.log(`========================================`);
    });

    // Validate database initialization with timeout
    const DB_TIMEOUT_MS = Math.max(30000, Number(process.env.DB_INIT_TIMEOUT_MS || 60000)); // 60s default for cloud DBs
    const dbCheckPromise = db.getProviderConfigs();
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Database initialization timeout (30s)')), DB_TIMEOUT_MS)
    );
    
    try {
      await Promise.race([dbCheckPromise, timeoutPromise]);
      console.log('[Server] Database initialized successfully');
    } catch (dbError) {
      console.error('[Server] Database initialization failed:', dbError.message);
      console.error('[Server] Check MYSQL_URL, network/TLS access, or slow startup migrations/backfill');
      // Don't crash - let the server run but APIs will fail
    }

    await uploaderService.start();
    await rcloneServeService.ensureDefaultGoogleDriveRemote().catch((error) => {
      console.warn('[Server] Failed to start rclone serve:', error.message);
    });

    // Start periodic cleanup scheduler (orphaned files, stuck jobs, expired sessions)
    cleanupService.start();

    try {
      const staleResult = await db.resetStaleProcessingJobs(10);
      if (staleResult.resetCount > 0) {
        console.log(`[Server] Recovered ${staleResult.resetCount} stale jobs from previous session`);
      }
    } catch (_) {}

    try {
      const expiredSessions = await db.cleanupExpiredBatchSessions();
      if (expiredSessions.deletedCount > 0) {
        console.log(`[Server] Cleaned up ${expiredSessions.deletedCount} expired batch sessions`);
      }
    } catch (_) {}

    try {
      const activeSessions = await db.getActiveBatchSessions();
      for (const session of activeSessions) {
        if (session.status === 'running') {
          console.log(`[Server] Marking orphaned batch session ${session.id} as failed (server restarted)`);
          await db.updateBatchSession(session.id, {
            status: 'failed',
            error: 'Server restarted during batch processing',
            hasMore: false
          });
        }
      }
    } catch (_) {}

    startWeeklyChecker();
    
    console.log('[Server] All services initialized successfully');
    console.log('[Server] Server is fully ready');
  } catch (error) {
    console.error('Failed to start server:', error);
    // Keep server running even if some services fail
    console.log('[Server] Server running with degraded functionality');
  }
}

let weeklyCheckerInterval = null;

async function startWeeklyChecker() {
  weeklyCheckerInterval = setInterval(async () => {
    try {
      const lastCheck = await db.getLastCheckTime();
      const now = new Date();
      
      if (!lastCheck) {
        await db.setLastCheckTime(now.toISOString());
        return;
      }
      
      const lastCheckDate = new Date(lastCheck);
      const daysSinceLastCheck = (now - lastCheckDate) / (1000 * 60 * 60 * 24);
      
      if (daysSinceLastCheck >= config.checker.weeklyIntervalDays) {
        console.log('[WeeklyChecker] Running weekly system check...');
        await runSystemCheck();
      }
    } catch (error) {
      console.error('[WeeklyChecker] Error:', error);
    }
  }, config.checker.intervalHours * 60 * 60 * 1000);
  
  console.log('[WeeklyChecker] Started - will check every 7 days');
}

async function runSystemCheck() {
  try {
    const providerCatalog = await uploaderService.getProviderCatalog({ includeDisabled: true });
    const providersToCheck = providerCatalog.map((item) => item.id);
    const providerResults = {};
    for (const provider of providersToCheck) {
      providerResults[provider] = await uploaderService.checkProviderIntegrity(provider, {
        autoReuploadMissing: true
      });
    }

    const results = {
      totalFiles: Object.values(providerResults).reduce((acc, item) => acc + item.totalFiles, 0),
      checked: Object.values(providerResults).reduce((acc, item) => acc + item.checked, 0),
      issues: Object.values(providerResults).flatMap((item) => item.issues || []),
      reuploadsQueued: Object.values(providerResults).flatMap((item) => item.reuploadsQueued || []),
      providers: providerResults,
      checkedProviders: providersToCheck,
      checkedAt: new Date().toISOString()
    };

    await db.setLastCheckTime(results.checkedAt);
    websocketHandler.broadcast('system:checked', results);
    
    console.log(`[WeeklyChecker] Check complete. Found ${results.issues.length} issues and queued ${results.reuploadsQueued.length} re-uploads.`);
  } catch (error) {
    console.error('[WeeklyChecker] Check failed:', error);
  }
}

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  cleanupService.stop();
  // Persist any in-memory batch run state before exiting
  try {
    const ctrl = getZeniusController();
    if (ctrl?.persistAllBatchRunsToDb) {
      await ctrl.persistAllBatchRunsToDb();
    }
  } catch (e) {
    console.error('[Server] Failed to persist batch runs on shutdown:', e.message);
  }
  await rcloneServeService.stop().catch(() => {});
  await uploaderService.stop();
  if (weeklyCheckerInterval) clearInterval(weeklyCheckerInterval);
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  cleanupService.stop();
  try {
    const ctrl = getZeniusController();
    if (ctrl?.persistAllBatchRunsToDb) {
      await ctrl.persistAllBatchRunsToDb();
    }
  } catch (e) {
    console.error('[Server] Failed to persist batch runs on shutdown:', e.message);
  }
  await rcloneServeService.stop().catch(() => {});
  await uploaderService.stop();
  if (weeklyCheckerInterval) clearInterval(weeklyCheckerInterval);
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

startServer();

module.exports = { app, server, eventEmitter };
