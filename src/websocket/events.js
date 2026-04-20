const { getInstance: getDb } = require('../db/handler');

let clients = new Set();
let eventEmitter = null;
let sendDashboardData = null;
let dashboardSnapshotPromise = null;
let dashboardRefreshTimer = null;
let dashboardSnapshotCache = null;
let dashboardSnapshotCacheAt = 0;
let dashboardBackoffUntil = 0;
const DASHBOARD_CACHE_TTL_MS = Math.max(1000, Number(process.env.DASHBOARD_CACHE_TTL_MS || 5000));
const DASHBOARD_PRESSURE_BACKOFF_MS = Math.max(5000, Number(process.env.DASHBOARD_PRESSURE_BACKOFF_MS || 15000));

function isDashboardDbPressureError(error) {
  const code = String(error?.code || '').trim().toUpperCase();
  return ['ECONNRESET', 'PROTOCOL_CONNECTION_LOST', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ER_CON_COUNT_ERROR'].includes(code);
}

function getDashboardSnapshot() {
  const now = Date.now();
  if (dashboardSnapshotCache && (now - dashboardSnapshotCacheAt) < DASHBOARD_CACHE_TTL_MS) {
    return Promise.resolve(dashboardSnapshotCache);
  }

  if (dashboardSnapshotCache && dashboardBackoffUntil > now) {
    return Promise.resolve(dashboardSnapshotCache);
  }

  if (!dashboardSnapshotCache && dashboardBackoffUntil > now) {
    return Promise.reject(Object.assign(new Error('Dashboard refresh backing off due to DB pressure'), {
      code: 'ER_CON_COUNT_ERROR'
    }));
  }

  if (!dashboardSnapshotPromise) {
    const db = getDb();
    dashboardSnapshotPromise = db.getDashboardData()
      .then((snapshot) => {
        dashboardSnapshotCache = snapshot;
        dashboardSnapshotCacheAt = Date.now();
        dashboardBackoffUntil = 0;
        return snapshot;
      })
      .catch((error) => {
        if (isDashboardDbPressureError(error)) {
          dashboardBackoffUntil = Date.now() + DASHBOARD_PRESSURE_BACKOFF_MS;
        }
        throw error;
      })
      .finally(() => {
        dashboardSnapshotPromise = null;
      });
  }

  return dashboardSnapshotPromise;
}

function scheduleDashboardBroadcast(delayMs = 500) {
  if (clients.size === 0) {
    return;
  }

  if (dashboardBackoffUntil > Date.now() && dashboardSnapshotCache) {
    clients.forEach(client => sendDashboardData(client));
    return;
  }

  if (dashboardRefreshTimer) {
    return;
  }

  dashboardRefreshTimer = setTimeout(() => {
    dashboardRefreshTimer = null;
    clients.forEach(client => sendDashboardData(client));
  }, delayMs);
}

const websocketHandler = {
  initialize(emitter) {
    eventEmitter = emitter;
    sendDashboardData = getDashboardData;
    this.setupEventForwarding(emitter);
  },

  addClient(ws) {
    clients.add(ws);
    sendDashboardData(ws);
  },

  removeClient(ws) {
    clients.delete(ws);
  },

  broadcast(event, data) {
    const message = JSON.stringify({ event, data });
    clients.forEach(client => {
      if (client.readyState === 1) {
        client.send(message);
      }
    });
  },

  setupEventForwarding(emitter) {
    const eventsToBroadcast = [
      'job:started',
      'job:completed',
      'job:failed',
      'job:cancelled',
      'job:deleted',
      'transfer:queued',
      'transfer:started',
      'transfer:completed',
      'transfer:failed',
      'file:ready',
      'progress',
      'upload:started',
      'upload:completed',
      'upload:failed',
      'upload:progress',
      'upload:queued',
      'download:progress',
      'download:completed',
      'download:failed',
      'batch:progress',
      'zenius:batch:started',
      'zenius:batch:progress',
      'zenius:batch:completed',
      'zenius:batch:failed',
      'provider:checked',
      'provider:checked:bulk',
      'system:checked'
    ];

    eventsToBroadcast.forEach(event => {
      emitter.on(event, (data) => {
        this.broadcast(event, data);

        if (['job:completed', 'job:failed', 'upload:completed', 'upload:failed', 'download:completed', 'download:failed'].includes(event)) {
          scheduleDashboardBroadcast(500);
        }
      });
    });

    setInterval(() => {
      if (clients.size === 0) {
        return;
      }
      scheduleDashboardBroadcast(0);
    }, 15000);
  },

  getClientCount() {
    return clients.size;
  }
};

async function getDashboardData(ws) {
  try {
    const data = await getDashboardSnapshot();
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({ event: 'dashboard:update', data }));
    }
  } catch (error) {
    const code = String(error?.code || '').trim().toUpperCase();
    if (isDashboardDbPressureError(error)) {
      console.warn(`Failed to send dashboard data: ${code || error.message}`);
      return;
    }

    console.error('Failed to send dashboard data:', error);
  }
}

module.exports = websocketHandler;
