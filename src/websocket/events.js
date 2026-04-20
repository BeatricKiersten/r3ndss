const { getInstance: getDb } = require('../db/handler');

let clients = new Set();
let eventEmitter = null;
let sendDashboardData = null;
let dashboardSnapshotPromise = null;
let dashboardRefreshTimer = null;

function getDashboardSnapshot() {
  if (!dashboardSnapshotPromise) {
    const db = getDb();
    dashboardSnapshotPromise = db.getDashboardData()
      .finally(() => {
        dashboardSnapshotPromise = null;
      });
  }

  return dashboardSnapshotPromise;
}

function scheduleDashboardBroadcast(delayMs = 500) {
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
    if (['ECONNRESET', 'PROTOCOL_CONNECTION_LOST', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ER_CON_COUNT_ERROR'].includes(code)) {
      console.warn(`Failed to send dashboard data: ${code || error.message}`);
      return;
    }

    console.error('Failed to send dashboard data:', error);
  }
}

module.exports = websocketHandler;
