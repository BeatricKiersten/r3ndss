function success(data = null, extra = {}) {
  return {
    success: true,
    data,
    error: null,
    ...extra
  };
}

function failure(message, extra = {}) {
  return {
    success: false,
    data: null,
    error: message,
    ...extra
  };
}

module.exports = {
  success,
  failure
};
