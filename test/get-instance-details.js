const axios = require('axios');

const instanceId = process.argv[2] || '79496';
const endpoint = `https://www.zenius.net/api/instance-details?url-short-id=${encodeURIComponent(instanceId)}`;

const headers = {
  Host: 'www.zenius.net',
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0',
  Accept: '*/*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br, zstd',
  Referer: `https://www.zenius.net/ci/${instanceId}/review-eksponen`,
  'sentry-trace': process.env.ZENIUS_SENTRY_TRACE || '',
  baggage: process.env.ZENIUS_BAGGAGE || '',
  Connection: 'keep-alive',
  Cookie: process.env.ZENIUS_COOKIE || '',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  Priority: 'u=4',
  TE: 'trailers',
  Pragma: 'no-cache',
  'Cache-Control': 'no-cache'
};

if (!process.env.ZENIUS_COOKIE) {
  console.warn('Warning: ZENIUS_COOKIE is empty. Endpoint may return unauthorized response.');
}

async function run() {
  try {
    const response = await axios.get(endpoint, {
      headers,
      timeout: 30000,
      validateStatus: () => true
    });

    console.log('Status:', response.status);
    console.log('URL:', endpoint);
    console.log('Response Headers:', response.headers);
    console.log('Response Body:', JSON.stringify(response.data, null, 2));
  } catch (error) {
    console.error('Request failed:', error.message);
    process.exitCode = 1;
  }
}

run();
