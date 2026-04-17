class AppError extends Error {
  constructor(message, options = {}) {
    super(message || 'Application error');
    this.name = 'AppError';
    this.statusCode = Number.isInteger(options.statusCode) ? options.statusCode : 500;
    this.code = options.code || 'INTERNAL_ERROR';
    this.expose = options.expose !== false;
    this.details = options.details || null;
  }
}

module.exports = AppError;
