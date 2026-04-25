import React, { useMemo, useState } from 'react';
import { ArrowRight, CheckCircle, Folder, FolderInput, Home as HomeIcon, RefreshCw, Search, X } from 'lucide-react';

export function flattenFolderTree(folderTree, options = {}) {
  const { excludeFolderId = null, includeRoot = true } = options;
  const result = includeRoot ? [{ id: 'root', name: 'Root', path: '/', depth: -1, hasSubfolders: true }] : [];

  const walk = (node, parentPath = '', depth = 0) => {
    (node?.folders || []).forEach((folder) => {
      if (folder.id === excludeFolderId) return;
      const currentPath = parentPath ? `${parentPath}/${folder.name}` : folder.name;
      const hasSubfolders = (folder.children?.folders || []).length > 0;

      result.push({
        id: folder.id,
        name: folder.name,
        path: `/${currentPath}`,
        depth,
        hasSubfolders,
        fileCount: Number(folder.fileCount || 0)
      });

      walk(folder.children, currentPath, depth + 1);
    });
  };

  walk(folderTree, '', 0);
  return result;
}

export default function FolderPickerModal({
  isOpen,
  title = 'Pilih Folder',
  description,
  confirmText = 'Pilih',
  sourceLabel,
  folderTree,
  currentFolderId,
  excludeFolderId,
  isSubmitting = false,
  onClose,
  onConfirm
}) {
  const [selectedId, setSelectedId] = useState('');
  const [search, setSearch] = useState('');

  const folders = useMemo(() => flattenFolderTree(folderTree, { excludeFolderId }), [folderTree, excludeFolderId]);
  const filteredFolders = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return folders;
    return folders.filter((folder) => folder.name.toLowerCase().includes(query) || folder.path.toLowerCase().includes(query));
  }, [folders, search]);
  const selectedFolder = folders.find((folder) => folder.id === selectedId);

  if (!isOpen) return null;

  const handleConfirm = () => {
    if (!selectedFolder || selectedFolder.id === currentFolderId) return;
    onConfirm(selectedFolder);
  };

  return (
    <div className="fixed inset-0 bg-black/75 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-5 max-w-xl w-full max-h-[88vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-4 mb-4">
          <div>
            <h3 className="text-lg font-medium text-white flex items-center gap-2">
              <FolderInput className="w-5 h-5 text-blue-400" />
              {title}
            </h3>
            {description && <p className="text-xs text-[#888] mt-1">{description}</p>}
          </div>
          <button onClick={onClose} disabled={isSubmitting} className="p-1.5 hover:bg-[#222] rounded disabled:opacity-50">
            <X className="w-5 h-5 text-[#666]" />
          </button>
        </div>

        <div className="relative mb-3">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-[#555]" />
          <input
            type="text"
            placeholder="Cari nama atau path folder..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-9 pr-4 py-2.5 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-sm text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#444]"
            autoFocus
          />
        </div>

        <div className="flex-1 overflow-y-auto border border-[#222] rounded-xl bg-[#0d0d0d] mb-4 min-h-[260px] max-h-[430px]">
          {filteredFolders.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12">
              <Folder className="w-9 h-9 text-[#333] mb-2" />
              <p className="text-xs text-[#666]">Tidak ada folder ditemukan</p>
            </div>
          ) : (
            <div className="py-1.5">
              {filteredFolders.map((folder) => {
                const isRoot = folder.id === 'root';
                const isSelected = selectedId === folder.id;
                const isCurrent = currentFolderId === folder.id;

                return (
                  <button
                    key={folder.id}
                    onClick={() => !isCurrent && setSelectedId(folder.id)}
                    disabled={isCurrent || isSubmitting}
                    className={`w-full flex items-center gap-2 px-3 py-2.5 text-left text-sm transition-colors border-l-2 disabled:cursor-not-allowed ${
                      isSelected
                        ? 'bg-blue-500/10 text-blue-300 border-blue-400'
                        : isCurrent
                        ? 'text-[#555] bg-[#111] border-transparent'
                        : 'text-[#aaa] hover:bg-[#151515] hover:text-white border-transparent'
                    }`}
                    style={{ paddingLeft: `${(folder.depth + 1) * 18 + 12}px` }}
                  >
                    {isRoot ? (
                      <HomeIcon className={`w-4 h-4 flex-shrink-0 ${isSelected ? 'text-blue-400' : 'text-[#666]'}`} />
                    ) : (
                      <Folder className={`w-4 h-4 flex-shrink-0 ${isSelected ? 'text-blue-400' : 'text-[#666]'}`} />
                    )}
                    <span className="truncate flex-1">{folder.path}</span>
                    {isCurrent && <span className="text-[10px] text-[#666]">current</span>}
                    {folder.fileCount > 0 && <span className="text-[10px] text-[#666]">{folder.fileCount} file</span>}
                    {isSelected && <CheckCircle className="w-4 h-4 text-blue-400 flex-shrink-0" />}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {selectedFolder && (
          <div className="mb-4 p-3 rounded-lg bg-[#151515] border border-[#2a2a2a] text-xs text-[#888]">
            {sourceLabel && <span className="text-white break-all">{sourceLabel}</span>}
            {sourceLabel && <ArrowRight className="w-3 h-3 inline mx-2 text-[#555]" />}
            <span className="text-blue-300 break-all">{selectedFolder.path}</span>
          </div>
        )}

        <div className="flex gap-3 justify-end">
          <button
            onClick={onClose}
            disabled={isSubmitting}
            className="px-4 py-2.5 text-sm text-[#888] hover:text-white rounded-lg hover:bg-[#222] transition-colors disabled:opacity-50"
          >
            Batal
          </button>
          <button
            onClick={handleConfirm}
            disabled={!selectedFolder || selectedFolder.id === currentFolderId || isSubmitting}
            className="px-5 py-2.5 text-sm rounded-lg font-medium bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 border border-blue-500/30 transition-colors disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {isSubmitting ? <RefreshCw className="w-4 h-4 animate-spin" /> : <FolderInput className="w-4 h-4" />}
            {isSubmitting ? 'Memproses...' : confirmText}
          </button>
        </div>
      </div>
    </div>
  );
}
