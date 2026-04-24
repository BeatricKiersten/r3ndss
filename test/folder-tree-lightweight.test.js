const { DatabaseHandler } = require('../src/db/handler');

function createHandlerWithRows(responses) {
  const handler = Object.create(DatabaseHandler.prototype);
  handler._ready = jest.fn().mockResolvedValue(undefined);
  handler.pool = {
    query: jest.fn().mockImplementation(async () => responses.shift())
  };
  return handler;
}

describe('folder tree lightweight loading', () => {
  test('getFolderTree uses file counts instead of hydrating every file', async () => {
    const handler = createHandlerWithRows([
      [[
        { id: 'root', name: 'Root', parent_id: null, path: '/', created_at: '2026-01-01', updated_at: '2026-01-01' },
        { id: 'child-1', name: 'Child', parent_id: 'root', path: '/Child', created_at: '2026-01-01', updated_at: '2026-01-01' }
      ]],
      [[
        { folder_id: 'root', count: '1200' },
        { folder_id: 'child-1', count: '25' }
      ]]
    ]);
    handler._hydrateFiles = jest.fn();

    const tree = await handler.getFolderTree();

    expect(handler._hydrateFiles).not.toHaveBeenCalled();
    expect(handler.pool.query).toHaveBeenCalledTimes(2);
    expect(handler.pool.query.mock.calls[1][0]).toContain('COUNT(*) AS count');
    expect(tree.fileCount).toBe(1200);
    expect(tree.files).toEqual([]);
    expect(tree.folders[0].fileCount).toBe(25);
    expect(tree.folders[0].children.files).toEqual([]);
  });

  test('getFolder returns child folders and count without listing full files', async () => {
    const handler = createHandlerWithRows([
      [[{ id: 'root', name: 'Root', parent_id: null, path: '/', created_at: '2026-01-01', updated_at: '2026-01-01' }]],
      [[{ id: 'child-1', name: 'Child', parent_id: 'root', path: '/Child', created_at: '2026-01-01', updated_at: '2026-01-01' }]],
      [[{ count: '1200' }]]
    ]);
    handler.listFiles = jest.fn();

    const folder = await handler.getFolder('root');

    expect(handler.listFiles).not.toHaveBeenCalled();
    expect(handler.pool.query).toHaveBeenCalledTimes(3);
    expect(handler.pool.query.mock.calls[2][0]).toContain('COUNT(*) AS count');
    expect(folder.fileCount).toBe(1200);
    expect(folder.files).toEqual([]);
    expect(folder.children).toHaveLength(1);
  });
});
