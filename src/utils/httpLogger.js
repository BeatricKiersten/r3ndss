const util = require('util');

const isDevelopment = process.env.NODE_ENV !== 'production';
const isVerboseHttpLog = String(process.env.DEBUG_HTTP_VERBOSE || 'false').toLowerCase() === 'true';

function formatBody(body) {
  if (body === undefined || body === null) return body;

  if (typeof body === 'string') {
    return body.length > 1200 ? `${body.slice(0, 1200)}...<truncated>` : body;
  }

  return util.inspect(body, {
    depth: 4,
    maxArrayLength: 20,
    breakLength: 120
  });
}

function createHttpLogger(name) {
  return {
    request(config = {}) {
      if (!isDevelopment) return config;

      const method = String(config.method || 'GET').toUpperCase();
      const url = config.baseURL ? `${config.baseURL}${config.url || ''}` : (config.url || '');

      console.log(`[HTTP:${name}] Request ${method} ${url}`);

      if (isVerboseHttpLog) {
        const params = formatBody(config.params);
        const data = formatBody(config.data);

        if (params) console.log(`[HTTP:${name}] Params`, params);
        if (config.headers) console.log(`[HTTP:${name}] Headers`, formatBody(config.headers));
        if (data && config.responseType !== 'stream') console.log(`[HTTP:${name}] Body`, data);
      }

      return config;
    },

    response(response) {
      if (!isDevelopment) return response;

      console.log(`[HTTP:${name}] Response ${response.status} ${response.config?.url || ''}`);

      if (isVerboseHttpLog) {
        if (response.headers) console.log(`[HTTP:${name}] Response Headers`, formatBody(response.headers));
        if (response.config?.responseType !== 'stream') {
          console.log(`[HTTP:${name}] Response Body`, formatBody(response.data));
        }
      }

      return response;
    },

    error(error) {
      if (!isDevelopment) throw error;

      console.error(`[HTTP:${name}] Error`, error.message);
      if (isVerboseHttpLog && error.config) {
        console.error(`[HTTP:${name}] Failed Request`, {
          method: String(error.config.method || 'GET').toUpperCase(),
          url: error.config.baseURL ? `${error.config.baseURL}${error.config.url || ''}` : (error.config.url || ''),
          params: error.config.params,
          headers: error.config.headers
        });
      }

      if (isVerboseHttpLog && error.response) {
        console.error(`[HTTP:${name}] Error Status`, error.response.status);
        console.error(`[HTTP:${name}] Error Headers`, formatBody(error.response.headers));
        if (error.config?.responseType !== 'stream') {
          console.error(`[HTTP:${name}] Error Body`, formatBody(error.response.data));
        }
      }

      throw error;
    }
  };
}

function attachHttpLogger(instance, name) {
  const logger = createHttpLogger(name);
  instance.interceptors.request.use(logger.request, logger.error);
  instance.interceptors.response.use(logger.response, logger.error);
  return instance;
}

module.exports = {
  attachHttpLogger,
  isDevelopment
};
