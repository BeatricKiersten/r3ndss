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
const { db, eventEmitter, uploaderService } = require('./services/runtime');

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
    // Validate database connectivity before starting workers.
    await db.getProviderConfigs();

    await uploaderService.start();
    startWeeklyChecker();
    
    server.listen(config.port, () => {
      console.log(`========================================`);
      console.log(`HLS-to-MP4 Backup Platform Server`);
      console.log(`Running on port ${config.port}`);
      console.log(`WebSocket enabled for real-time updates`);
      console.log(`Weekly checker enabled`);
      console.log(`========================================`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
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
    const providerResults = {};
    for (const provider of config.supportedProviders) {
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
      checkedProviders: [...config.supportedProviders],
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
  await uploaderService.stop();
  if (weeklyCheckerInterval) clearInterval(weeklyCheckerInterval);
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  await uploaderService.stop();
  if (weeklyCheckerInterval) clearInterval(weeklyCheckerInterval);
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

startServer();

module.exports = { app, server, eventEmitter };
