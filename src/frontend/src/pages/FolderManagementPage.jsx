import React, { useMemo, useState } from 'react';
import { AlertTriangle, ArrowLeft, ChevronDown, ChevronRight, Folder, Home as HomeIcon, RefreshCw, Search, Trash2, X } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useFolders, usePurgeFolder } from '../hooks/api';
import { toast } from '../store/toastStore';
import { PATHS } from '../config/routes.jsx';

function buildFolderRows(folderTree) {
  const result = [];

  const walk = (node, parentPath = '', depth = 0, parentId = null) => {
    return (node?.folders || []).map((folder) => {
      const path = parentPath ? `${parentPath}/${folder.name}` : `/${folder.name}`;
      const childRows = walk(folder.children, path, depth + 1, folder.id);
      const directFileCount = Number(folder.fileCount || 0);
      const descendantFolderCount = childRows.reduce((sum, child) => sum + child.descendantFolderCount + 1, 0);
      const recursiveFileCount = directFileCount + childRows.reduce((sum, child) => sum + child.recursiveFileCount, 0);
      const row = {
        ...folder,
        path,
        depth,
        parentId,
        childFolderCount: childRows.length,
        descendantFolderCount,
        directFileCount,
        recursiveFileCount,
        childIds: childRows.map((child) => child.id)
      };

      result.push(row);
      return row;
    });
  };

  walk(folderTree);
  return result;
}

function getVisibleFolders(folders, expandedIds, search) {
  const query = search.trim().toLowerCase();
  const matches = (folder) => folder.name.toLowerCase().includes(query) || folder.path.toLowerCase().includes(query);

  if (query) {
    return folders.filter(matches);
  }

  const hiddenFolderIds = new Set();

  return folders.filter((folder) => {
    if (folder.parentId && (!expandedIds.has(folder.parentId) || hiddenFolderIds.has(folder.parentId))) {
      hiddenFolderIds.add(folder.id);
      return false;
    }
    return true;
  });
}

function DeleteFolderModal({ folders, isDeleting, onClose, onConfirm }) {
  if (!folders.length) return null;

  const totalFolders = folders.reduce((sum, folder) => sum + folder.descendantFolderCount + 1, 0);
  const totalFiles = folders.reduce((sum, folder) => sum + folder.recursiveFileCount, 0);
  const title = folders.length === 1 ? 'Hapus folder permanen?' : `Hapus ${folders.length} folder terpilih?`;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4" onClick={onClose}>
      <div className="w-full max-w-lg bg-[#1a1a1a] rounded-xl border border-red-500/20 shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start gap-3 p-5 border-b border-[#222]">
          <div className="w-10 h-10 rounded-lg bg-red-500/10 flex items-center justify-center flex-shrink-0">
            <AlertTriangle className="w-5 h-5 text-red-400" />
          </div>
          <div className="min-w-0 flex-1">
            <h3 className="text-base font-medium text-white">{title}</h3>
            <p className="text-xs text-[#888] mt-1">Aksi ini tidak bisa dibatalkan.</p>
          </div>
          <button onClick={onClose} disabled={isDeleting} className="p-1 text-[#666] hover:text-white disabled:opacity-50">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          <div className="grid grid-cols-3 gap-2">
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{totalFolders}</p>
              <p className="text-[11px] text-[#666]">folder</p>
            </div>
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{totalFiles}</p>
              <p className="text-[11px] text-[#666]">file</p>
            </div>
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{folders.length}</p>
              <p className="text-[11px] text-[#666]">dipilih</p>
            </div>
          </div>

          <div className="max-h-40 overflow-y-auto rounded-lg bg-[#0d0d0d] border border-[#222] divide-y divide-[#1f1f1f]">
            {folders.map((folder) => (
              <div key={folder.id} className="px-3 py-2 text-xs text-[#aaa] break-all">
                {folder.path}
              </div>
            ))}
          </div>

          <div className="rounded-lg bg-red-500/10 border border-red-500/20 p-3 text-xs text-red-200">
            Aksi ini menghapus folder, subfolder, file, provider status, dan jobs dari database. Tidak bisa dibatalkan.
          </div>
        </div>

        <div className="flex items-center justify-end gap-2 p-5 border-t border-[#222]">
          <button
            onClick={onClose}
            disabled={isDeleting}
            className="px-4 py-2 text-sm rounded-lg bg-[#222] text-[#aaa] hover:bg-[#333] hover:text-white disabled:opacity-50"
          >
            Batal
          </button>
          <button
            onClick={onConfirm}
            disabled={isDeleting}
            className="px-4 py-2 text-sm rounded-lg bg-red-500/20 text-red-400 hover:bg-red-500/30 border border-red-500/30 disabled:opacity-50 flex items-center gap-2"
          >
            {isDeleting ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
            {isDeleting ? 'Menghapus...' : 'Hapus Permanen'}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function FolderManagementPage() {
  const navigate = useNavigate();
  const { data: folderTree, isLoading, isFetching } = useFolders();
  const purgeFolder = usePurgeFolder();
  const [search, setSearch] = useState('');
  const [deleteTargets, setDeleteTargets] = useState([]);
  const [selectedIds, setSelectedIds] = useState(() => new Set());
  const [expandedIds, setExpandedIds] = useState(() => new Set());

  const folders = useMemo(() => buildFolderRows(folderTree), [folderTree]);
  const folderById = useMemo(() => new Map(folders.map((folder) => [folder.id, folder])), [folders]);
  const visibleFolders = useMemo(() => getVisibleFolders(folders, expandedIds, search), [folders, expandedIds, search]);
  const selectedFolders = useMemo(() => [...selectedIds].map((id) => folderById.get(id)).filter(Boolean), [folderById, selectedIds]);
  const allVisibleSelected = visibleFolders.length > 0 && visibleFolders.every((folder) => selectedIds.has(folder.id));

  const toggleExpanded = (folderId) => {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (next.has(folderId)) next.delete(folderId);
      else next.add(folderId);
      return next;
    });
  };

  const toggleSelected = (folderId) => {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (next.has(folderId)) next.delete(folderId);
      else next.add(folderId);
      return next;
    });
  };

  const toggleVisibleSelection = () => {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (allVisibleSelected) {
        visibleFolders.forEach((folder) => next.delete(folder.id));
      } else {
        visibleFolders.forEach((folder) => next.add(folder.id));
      }
      return next;
    });
  };

  const handleDelete = async () => {
    if (!deleteTargets.length) return;

    try {
      const results = [];
      for (const folder of deleteTargets) {
        results.push(await purgeFolder.mutateAsync(folder.id));
      }
      const summary = results.reduce((acc, result) => ({
        removedFolders: acc.removedFolders + Number(result.removedFolders || 0),
        removedFiles: acc.removedFiles + Number(result.removedFiles || 0),
        removedJobs: acc.removedJobs + Number(result.removedJobs || 0)
      }), { removedFolders: 0, removedFiles: 0, removedJobs: 0 });

      toast.success(
        deleteTargets.length === 1 ? 'Folder dihapus' : 'Folder terpilih dihapus',
        `${summary.removedFolders} folder, ${summary.removedFiles} file, ${summary.removedJobs} jobs dihapus`
      );
      setSelectedIds(new Set());
      setDeleteTargets([]);
    } catch (error) {
      toast.error('Gagal menghapus folder', error?.response?.data?.error || error.message);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <button onClick={() => navigate(PATHS.files)} className="text-xs text-[#888] hover:text-white flex items-center gap-1 mb-2">
            <ArrowLeft className="w-3.5 h-3.5" /> Back to Files
          </button>
          <h2 className="text-xl font-semibold text-white">Folder Management</h2>
          <p className="text-sm text-[#888] mt-1">Pilih banyak folder, collapse parent, lalu hapus yang diperlukan.</p>
        </div>
        {isFetching && (
          <div className="flex items-center gap-2 text-xs text-[#888]">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" /> Refreshing
          </div>
        )}
      </div>

      <div className="card p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center mb-4">
          <div className="relative flex-1">
            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-[#555]" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Cari folder atau path..."
              className="w-full pl-9 pr-4 py-2.5 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-sm text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#444]"
            />
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={toggleVisibleSelection}
              disabled={visibleFolders.length === 0}
              className="px-3 py-2.5 text-xs rounded-lg bg-[#151515] border border-[#2a2a2a] text-[#aaa] hover:text-white hover:border-[#444] disabled:opacity-40"
            >
              {allVisibleSelected ? 'Unselect visible' : 'Select visible'}
            </button>
            <button
              onClick={() => setDeleteTargets(selectedFolders)}
              disabled={selectedFolders.length === 0}
              className="px-3 py-2.5 text-xs rounded-lg bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/20 disabled:opacity-40 flex items-center gap-1.5"
            >
              <Trash2 className="w-3.5 h-3.5" /> Delete {selectedFolders.length || ''}
            </button>
          </div>
        </div>

        <div className="flex items-center justify-between text-xs text-[#666] mb-2">
          <span>{visibleFolders.length} shown / {folders.length} total</span>
          <span>{selectedFolders.length} selected</span>
        </div>

        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((item) => <div key={item} className="h-16 rounded-lg bg-[#1a1a1a] animate-pulse" />)}
          </div>
        ) : visibleFolders.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16">
            <Folder className="w-10 h-10 text-[#333] mb-2" />
            <p className="text-sm text-[#777]">Tidak ada folder ditemukan</p>
          </div>
        ) : (
          <div className="overflow-hidden rounded-xl border border-[#222] bg-[#0d0d0d] divide-y divide-[#1f1f1f]">
            {visibleFolders.map((folder) => {
              const hasChildren = folder.childFolderCount > 0;
              const isExpanded = expandedIds.has(folder.id);
              const isSelected = selectedIds.has(folder.id);

              return (
                <div key={folder.id} className={`group flex items-center gap-2 px-3 py-2.5 hover:bg-[#151515] ${isSelected ? 'bg-blue-500/5' : ''}`}>
                  <input
                    type="checkbox"
                    checked={isSelected}
                    onChange={() => toggleSelected(folder.id)}
                    className="h-4 w-4 rounded border-[#333] bg-[#111] accent-blue-500 flex-shrink-0"
                    aria-label={`Pilih ${folder.path}`}
                  />

                  <button
                    onClick={() => hasChildren && toggleExpanded(folder.id)}
                    disabled={!hasChildren || search.trim() !== ''}
                    className="w-6 h-6 rounded flex items-center justify-center text-[#666] hover:text-white disabled:opacity-30 disabled:hover:text-[#666] flex-shrink-0"
                    aria-label={isExpanded ? 'Collapse folder' : 'Expand folder'}
                  >
                    {hasChildren ? (isExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />) : <span className="w-4" />}
                  </button>

                  <div className="flex items-center gap-2 min-w-0 flex-1" style={{ paddingLeft: search.trim() ? 0 : `${folder.depth * 16}px` }}>
                    {folder.depth === 0 ? <HomeIcon className="w-4 h-4 text-blue-400 flex-shrink-0" /> : <Folder className="w-4 h-4 text-yellow-500/80 flex-shrink-0" />}
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-[#ddd] truncate">{folder.name}</p>
                      <p className="text-[11px] text-[#666] truncate">{folder.path}</p>
                    </div>
                  </div>

                  <div className="hidden sm:flex items-center gap-2 text-[11px] text-[#777] flex-shrink-0">
                    <span>{folder.descendantFolderCount + 1} folder</span>
                    <span>{folder.recursiveFileCount} file</span>
                  </div>

                  <button
                    onClick={() => setDeleteTargets([folder])}
                    className="p-2 rounded-lg text-red-400 hover:bg-red-500/10 opacity-80 group-hover:opacity-100 flex-shrink-0"
                    aria-label={`Hapus ${folder.path}`}
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <DeleteFolderModal
        folders={deleteTargets}
        isDeleting={purgeFolder.isLoading}
        onClose={() => setDeleteTargets([])}
        onConfirm={handleDelete}
      />
    </div>
  );
}
