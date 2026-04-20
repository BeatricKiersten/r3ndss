require('dotenv').config();

const DEFAULT_PROVIDERS = ['voesx', 'catbox', 'seekstreaming'];

module.exports = {
  port: Number(process.env.PORT || process.env.API_PORT || 3001),
  frontendUrl: process.env.FRONTEND_URL || 'http://localhost:3000',
  uploadDir: process.env.UPLOAD_DIR || './uploads',
  jobs: {
    maxListLimit: Math.max(1, Number(process.env.JOBS_MAX_LIST_LIMIT || 100))
  },
  mysql: {
    url: process.env.MYSQL_URL || process.env.DATABASE_URL || null,
    host: process.env.MYSQL_HOST || '127.0.0.1',
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'zenius',
    connectionLimit: Number(process.env.MYSQL_CONNECTION_LIMIT || 50),
    maxIdle: Number(process.env.MYSQL_MAX_IDLE || process.env.MYSQL_CONNECTION_LIMIT || 50),
    idleTimeout: Number(process.env.MYSQL_IDLE_TIMEOUT || 60000),
    queueLimit: Number(process.env.MYSQL_QUEUE_LIMIT || 0),
    connectTimeout: Number(process.env.MYSQL_CONNECT_TIMEOUT || 10000),
    enableKeepAlive: String(process.env.MYSQL_ENABLE_KEEP_ALIVE || 'true').toLowerCase() !== 'false',
    keepAliveInitialDelay: Number(process.env.MYSQL_KEEP_ALIVE_INITIAL_DELAY || 0),
    autoCreateDatabase: String(process.env.MYSQL_AUTO_CREATE_DATABASE || 'true').toLowerCase() !== 'false',
    ssl: String(process.env.MYSQL_SSL || 'false').toLowerCase() === 'true',
    sslRejectUnauthorized: String(process.env.MYSQL_SSL_REJECT_UNAUTHORIZED || 'true').toLowerCase() !== 'false'
  },
  supportedProviders: DEFAULT_PROVIDERS,
  upload: {
    maxConcurrentUploads: 2,
    maxConcurrentProviders: 4,
    retryAttempts: 2,
    retryDelay: 5000,
    timeout: 30 * 60 * 1000,
    probeTimeout: 15000,
    downloadTimeout: 30 * 60 * 1000
  },
  ffmpeg: {
    timeout: 2 * 60 * 60 * 1000,
    maxRetries: 3,
    retryDelay: 2000
  },
  checker: {
    intervalHours: 1,
    weeklyIntervalDays: 7
  },
  rcloneProviders: {
    rclone: {
      name: 'Rclone Storage',
      description: 'Rclone-backed remote storage (S3, B2, Drive, etc)',
      supportsMultipleRemotes: true
    }
  }
};
