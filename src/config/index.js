require('dotenv').config();

const DEFAULT_PROVIDERS = ['voesx', 'catbox', 'seekstreaming'];

module.exports = {
  databaseUrl: process.env.POSTGRES_URL
    || process.env.DATABASE_URL
    || ((/^postgres(ql)?:\/\//i.test(String(process.env.MYSQL_URL || ''))) ? process.env.MYSQL_URL : null),
  port: Number(process.env.PORT || process.env.API_PORT || 3001),
  frontendUrl: process.env.FRONTEND_URL || 'http://localhost:3000',
  uploadDir: process.env.UPLOAD_DIR || './uploads',
  jobs: {
    maxListLimit: Math.max(1, Number(process.env.JOBS_MAX_LIST_LIMIT || 100))
  },
  postgres: {
    url: process.env.POSTGRES_URL
      || process.env.DATABASE_URL
      || ((/^postgres(ql)?:\/\//i.test(String(process.env.MYSQL_URL || ''))) ? process.env.MYSQL_URL : null),
    host: process.env.POSTGRES_HOST || process.env.PGHOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT || process.env.PGPORT || 5432),
    user: process.env.POSTGRES_USER || process.env.PGUSER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || process.env.PGPASSWORD || '',
    database: process.env.POSTGRES_DATABASE || process.env.PGDATABASE || 'zenius',
    connectionLimit: Number(process.env.POSTGRES_CONNECTION_LIMIT || process.env.PGPOOLSIZE || 5),
    idleTimeout: Number(process.env.POSTGRES_IDLE_TIMEOUT || 60000),
    connectTimeout: Number(process.env.POSTGRES_CONNECT_TIMEOUT || 10000),
    keepAlive: String(process.env.POSTGRES_ENABLE_KEEP_ALIVE || 'true').toLowerCase() !== 'false',
    ssl: String(process.env.POSTGRES_SSL || 'false').toLowerCase() === 'true',
    sslRejectUnauthorized: String(process.env.POSTGRES_SSL_REJECT_UNAUTHORIZED || 'true').toLowerCase() !== 'false'
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
