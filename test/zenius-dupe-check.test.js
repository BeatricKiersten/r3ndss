process.env.NODE_ENV = 'test';

const queries = [];

jest.mock('../src/services/runtime', () => ({
  db: {
    _ready: jest.fn().mockResolvedValue(undefined),
    pool: {
      query: jest.fn(async (sql, params) => {
        queries.push({ sql, params });
        return [[
          { file_id: 'file-1', provider: 'catbox', status: 'completed' },
          { file_id: 'file-2', provider: 'catbox', status: 'failed' }
        ]];
      })
    },
    getBatchSession: jest.fn()
  },
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

describe('Zenius preview duplicate check', () => {
  beforeEach(() => {
    queries.length = 0;
  });

  test('filters provider status lookup to selected providers', async () => {
    const existingByFolderId = new Map([
      ['folder-1', new Map([
        ['A.mp4', { id: 'file-1' }],
        ['B.mp4', { id: 'file-2' }]
      ])]
    ]);

    const result = await controller.__test.buildProviderStatusLookup(existingByFolderId, 200, ['catbox']);

    expect(queries).toHaveLength(1);
    expect(queries[0].sql).toContain('provider IN (?)');
    expect(queries[0].params).toEqual(['file-1', 'file-2', 'catbox']);
    expect(result.fileCount).toBe(2);
    expect(result.providerRowCount).toBe(2);
    expect(result.providerStatusByFileId.get('file-1').get('catbox')).toBe('completed');
  });
});
