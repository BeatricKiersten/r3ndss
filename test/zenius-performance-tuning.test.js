process.env.NODE_ENV = 'test';

jest.mock('../src/services/runtime', () => ({
  db: { getBatchSession: jest.fn() },
  uploaderService: {},
  eventEmitter: { emit: jest.fn() },
  cleanupService: null,
  runProcessUploadPipeline: jest.fn(),
  finalizeExistingFilePipeline: jest.fn(),
  normalizePipelineError: jest.fn((error) => error)
}));

jest.mock('../src/services/webhookService', () => ({
  sendBatchComplete: jest.fn(),
  loadConfig: jest.fn(),
  getConfig: jest.fn(),
  updateConfig: jest.fn(),
  sendTest: jest.fn()
}));

jest.mock('../src/services/zeniusUpstreamService', () => ({
  resolveReferer: jest.fn(),
  getJson: jest.fn(),
  getInstanceDetails: jest.fn(),
  ZENIUS_BASE_URL: 'https://www.zenius.net'
}));

const controller = require('../src/controllers/zeniusController');

describe('Zenius performance tuning defaults', () => {
  test('uses higher safe defaults for batch chunking and download concurrency', () => {
    expect(controller.__test.constants.BACKGROUND_BATCH_CHUNK_SIZE).toBe(24);
    expect(controller.__test.constants.DEFAULT_MAX_CONCURRENT_DOWNLOADS).toBe(6);
    expect(controller.__test.constants.BATCH_DOWNLOAD_MAX_QUEUED_MULTIPLIER).toBe(4);
  });

  test('computes adaptive queued download backpressure from concurrency', () => {
    expect(controller.__test.getBatchDownloadMaxQueued()).toBe(24);
  });
});
