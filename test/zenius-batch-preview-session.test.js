process.env.NODE_ENV = 'test';

const mockDb = {
  getBatchSession: jest.fn()
};

jest.mock('../src/services/runtime', () => ({
  db: mockDb,
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

const {
  batchChainSessions,
  hydrateBatchChainSessionFromPersistedPreview,
  hydrateBatchChainSessionForDownload,
  normalizePlanningContext,
  serializeBatchPreviewSession
} = controller.__test;

function buildPersistedPreviewSession(overrides = {}) {
  const selectedProviders = overrides.selectedProviders || ['catbox'];
  const baseFolderInput = overrides.baseFolderInput || 'kelas-10';
  const planningContext = normalizePlanningContext(baseFolderInput, selectedProviders);

  return {
    id: overrides.previewRunId || 'preview-run-1',
    rootCgId: '34',
    rootCgName: 'Root CG',
    targetCgSelector: '99',
    parentContainerName: 'Parent',
    status: 'completed',
    totalContainers: 1,
    processedContainers: 1,
    queuedCount: 0,
    skippedCount: 0,
    nextContainerOffset: 0,
    hasMore: false,
    error: null,
    queuedItems: [],
    skippedItems: [],
    chainErrors: [],
    createdAt: '2026-01-01T00:00:00.000Z',
    updatedAt: '2026-01-01T00:01:00.000Z',
    finishedAt: '2026-01-01T00:01:00.000Z',
    sessionData: {
      type: 'preview',
      sessionId: overrides.planSessionId || 'plan-session-1',
      baseFolderInput,
      selectedProviders,
      chainPreview: {
        sessionId: overrides.planSessionId || 'plan-session-1',
        rootCgId: '34',
        rootCgName: 'Root CG',
        targetCgSelector: '99',
        parentContainerName: 'Parent',
        leafCgIds: ['99'],
        traversal: [{ source: '34', next: '99', isParent: false }],
        totalContainers: 1,
        planReady: overrides.planReady !== false,
        planContextKey: planningContext.key,
        baseFolderInput,
        selectedProviders,
        plannedItemCount: 1,
        containerDetails: [
          {
            containerUrlShortId: '200',
            containerName: 'Container A',
            containerType: 'video',
            containerPathUrl: '/cgc/200',
            sourceLeafCgId: '99',
            path: 'Parent/Container A',
            videoInstances: [
              {
                urlShortId: '300',
                name: 'Video A',
                outputName: 'Video A'
              }
            ]
          }
        ],
        plannedItems: overrides.plannedItems || [
          {
            planKey: '200::300::Video A.mp4::folder-1',
            urlShortId: '300',
            containerUrlShortId: '200',
            containerName: 'Container A',
            folderId: 'folder-1',
            folderInput: 'kelas-10/Parent/Container A',
            outputName: 'Video A.mp4',
            path: 'Parent/Container A',
            selectedProviders,
            action: 'download',
            reason: 'Ready to download',
            pendingProviders: [],
            instance: {
              urlShortId: '300',
              name: 'Video A',
              outputName: 'Video A'
            }
          }
        ],
        errors: []
      }
    }
  };
}

describe('Zenius batch preview session contract', () => {
  beforeEach(() => {
    batchChainSessions.clear();
    mockDb.getBatchSession.mockReset();
  });

  test('hydrates a plan session from a persisted preview run snapshot', () => {
    const dbSession = buildPersistedPreviewSession();

    const session = hydrateBatchChainSessionFromPersistedPreview(dbSession, 'plan-session-1');
    const serialized = serializeBatchPreviewSession(session);

    expect(session.id).toBe('plan-session-1');
    expect(session.planReady).toBe(true);
    expect(session.plannedItems).toHaveLength(1);
    expect(serialized.sessionId).toBe('plan-session-1');
    expect(serialized.plannedItems[0].urlShortId).toBe('300');
    expect(batchChainSessions.get('plan-session-1')).toBe(session);
  });

  test('recovers a missing in-memory plan session by previewRunId', async () => {
    const dbSession = buildPersistedPreviewSession({ previewRunId: 'preview-run-1', planSessionId: 'plan-session-1' });
    mockDb.getBatchSession.mockResolvedValueOnce(dbSession);

    const session = await hydrateBatchChainSessionForDownload({
      sessionId: 'plan-session-1',
      previewRunId: 'preview-run-1'
    });

    expect(mockDb.getBatchSession).toHaveBeenCalledWith('preview-run-1');
    expect(session.id).toBe('plan-session-1');
    expect(session.planContextKey).toBe(normalizePlanningContext('kelas-10', ['catbox']).key);
    expect(session.plannedItems[0].action).toBe('download');
  });

  test('does not hydrate incomplete persisted previews', () => {
    const dbSession = buildPersistedPreviewSession({ planReady: false });

    const session = hydrateBatchChainSessionFromPersistedPreview(dbSession, 'plan-session-1');

    expect(session).toBeNull();
    expect(batchChainSessions.has('plan-session-1')).toBe(false);
  });
});
