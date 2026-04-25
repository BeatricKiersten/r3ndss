import React, { useMemo, useState } from 'react';
import { AlertTriangle, ArrowLeft, Folder, Home as HomeIcon, RefreshCw, Search, Trash2, X } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useFolders, usePurgeFolder } from '../hooks/api';
import { toast } from '../store/toastStore';
import { PATHS } from '../config/routes.jsx';

function collectFolders(folderTree) {
  const result = [];

  const walk = (node, parentPath = '', depth = 0) => {
    (node?.folders || []).forEach((folder) => {
      const path = parentPath ? `${parentPath}/${folder.name}` : `/${folder.name}`;
      const children = folder.children?.folders || [];
      const directFileCount = Number(folder.fileCount || 0);
      const nested = collectFolders(folder.children);
      const recursiveFileCount = directFileCount + nested.reduce((sum, child) => sum + child.directFileCount, 0);

      result.push({
        ...folder,
        path,
        depth,
        childFolderCount: children.length,
        descendantFolderCount: nested.length,
        directFileCount,
        recursiveFileCount
      });

      walk(folder.children, path, depth + 1);
    });
  };

  walk(folderTree);
  return result;
}

function DeleteFolderModal({ folder, isDeleting, onClose, onConfirm }) {
  if (!folder) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4" onClick={onClose}>
      <div className="w-full max-w-lg bg-[#1a1a1a] rounded-xl border border-red-500/20 shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start gap-3 p-5 border-b border-[#222]">
          <div className="w-10 h-10 rounded-lg bg-red-500/10 flex items-center justify-center flex-shrink-0">
            <AlertTriangle className="w-5 h-5 text-red-400" />
          </div>
          <div className="min-w-0 flex-1">
            <h3 className="text-base font-medium text-white">Hapus folder permanen?</h3>
            <p className="text-xs text-[#888] mt-1 break-all">{folder.path}</p>
          </div>
          <button onClick={onClose} disabled={isDeleting} className="p-1 text-[#666] hover:text-white disabled:opacity-50">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          <div className="grid grid-cols-3 gap-2">
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{folder.descendantFolderCount + 1}</p>
              <p className="text-[11px] text-[#666]">folder</p>
            </div>
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{folder.recursiveFileCount}</p>
              <p className="text-[11px] text-[#666]">file</p>
            </div>
            <div className="rounded-lg bg-[#0d0d0d] border border-[#222] p-3">
              <p className="text-lg font-semibold text-white">{folder.childFolderCount}</p>
              <p className="text-[11px] text-[#666]">child</p>
            </div>
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
  const [deleteTarget, setDeleteTarget] = useState(null);

  const folders = useMemo(() => collectFolders(folderTree), [folderTree]);
  const filteredFolders = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return folders;
    return folders.filter((folder) => folder.name.toLowerCase().includes(query) || folder.path.toLowerCase().includes(query));
  }, [folders, search]);

  const handleDelete = async () => {
    if (!deleteTarget) return;

    try {
      const result = await purgeFolder.mutateAsync(deleteTarget.id);
      toast.success(
        'Folder dihapus',
        `${result.removedFolders} folder, ${result.removedFiles} file, ${result.removedJobs} jobs dihapus`
      );
      setDeleteTarget(null);
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
          <p className="text-sm text-[#888] mt-1">Cari, review, dan hapus folder beserta seluruh isinya dengan lebih aman.</p>
        </div>
        {isFetching && (
          <div className="flex items-center gap-2 text-xs text-[#888]">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" /> Refreshing
          </div>
        )}
      </div>

      <div className="card p-4">
        <div className="relative mb-4">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-[#555]" />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Cari folder atau path..."
            className="w-full pl-9 pr-4 py-2.5 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-sm text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#444]"
          />
        </div>

        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((item) => <div key={item} className="h-16 rounded-lg bg-[#1a1a1a] animate-pulse" />)}
          </div>
        ) : filteredFolders.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16">
            <Folder className="w-10 h-10 text-[#333] mb-2" />
            <p className="text-sm text-[#777]">Tidak ada folder ditemukan</p>
          </div>
        ) : (
          <div className="space-y-2">
            {filteredFolders.map((folder) => (
              <div key={folder.id} className="group flex items-center gap-3 rounded-xl bg-[#141414] border border-[#222] p-3 hover:border-[#333] transition-colors">
                <div className="w-10 h-10 rounded-lg bg-[#222] flex items-center justify-center flex-shrink-0">
                  {folder.depth === 0 ? <HomeIcon className="w-4 h-4 text-blue-400" /> : <Folder className="w-4 h-4 text-yellow-500/80" />}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium text-[#ddd] truncate">{folder.name}</p>
                  <p className="text-xs text-[#666] truncate">{folder.path}</p>
                </div>
                <div className="hidden sm:flex items-center gap-2 text-xs text-[#777]">
                  <span className="px-2 py-1 rounded bg-[#0d0d0d] border border-[#222]">{folder.descendantFolderCount + 1} folder</span>
                  <span className="px-2 py-1 rounded bg-[#0d0d0d] border border-[#222]">{folder.recursiveFileCount} file</span>
                </div>
                <button
                  onClick={() => setDeleteTarget(folder)}
                  className="px-3 py-2 rounded-lg text-xs bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/20 flex items-center gap-1.5"
                >
                  <Trash2 className="w-3.5 h-3.5" /> Delete
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <DeleteFolderModal
        folder={deleteTarget}
        isDeleting={purgeFolder.isLoading}
        onClose={() => setDeleteTarget(null)}
        onConfirm={handleDelete}
      />
    </div>
  );
}
