const { failure } = require('../utils/apiResponse');

function errorHandler(err, req, res, next) {
  const statusCode = Number.isInteger(err?.statusCode) ? err.statusCode : 500;
  const safeMessage = statusCode >= 500 && err?.expose === false
    ? 'Internal server error'
    : (err?.message || 'Internal server error');

  console.error('[ExpressError]', {
    requestId: req?.id || null,
    method: req?.method || null,
    path: req?.originalUrl || req?.url || null,
    statusCode,
    code: err?.code || null,
    message: err?.message || null,
    stack: err?.stack || null,
    details: err?.details || null
  });

  if (res.headersSent) {
    return next(err);
  }

  res.status(statusCode).json(failure(safeMessage, {
    code: err?.code || (statusCode >= 500 ? 'INTERNAL_ERROR' : 'REQUEST_FAILED'),
    requestId: req?.id || null
  }));
}

module.exports = errorHandler;
