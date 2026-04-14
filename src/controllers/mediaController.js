const axios = require('axios');
const { rcloneServeService } = require('../services/runtime');

const mediaController = {
  async proxyRclone(req, res) {
    try {
      const rawPath = Array.isArray(req.params[0]) ? req.params[0].join('/') : req.params[0];
      const remotePath = String(rawPath || '').replace(/^\/+/, '');
      if (!remotePath) {
        return res.status(400).json({ success: false, error: 'Remote path is required' });
      }

      const upstreamUrl = `${rcloneServeService.getLocalBaseUrl()}/${remotePath.split('/').map(encodeURIComponent).join('/')}`;
      const upstreamHeaders = {
        accept: req.headers.accept || '*/*'
      };

      if (req.headers.range) {
        upstreamHeaders.range = req.headers.range;
      }

      const upstream = await axios.get(upstreamUrl, {
        responseType: 'stream',
        validateStatus: () => true,
        headers: upstreamHeaders,
        timeout: 30000
      });

      res.status(upstream.status);

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

      if (!res.getHeader('accept-ranges')) {
        res.setHeader('accept-ranges', 'bytes');
      }

      if (req.method === 'OPTIONS') {
        return res.status(200).end();
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
  }
};

module.exports = mediaController;
