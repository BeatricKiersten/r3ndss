import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  useFiles,
  useForceDeleteFile,
  useFolders,
  useFolder,
  useCreateFolder,
  useMoveFile,
  useMoveFolder,
  usePurgeFolder,
  useReuploadToProvider,
  useFileProvidersStatus,
  useProblemFiles,
  useDeleteAllProblemFiles,
  useClearFileProviderLink,
  useCheckAllFilesCompleteness,
  useCheckFileCompleteness
} from '../hooks/api';
import { usePlayerStore } from '../store/websocketStore';
import { useNavigate } from 'react-router-dom';
import {
  FileVideo,
  Play,
  Trash2,
  CheckCircle,
  AlertCircle,
  Clock,
  ExternalLink,
  Folder,
  FolderPlus,
  ChevronRight,
  ChevronDown,
  Home as HomeIcon,
  Plus,
  Upload,
  RefreshCw,
  ScanSearch,
  X,
  Cloud,
  Search,
  MoreVertical,
  Eye,
  Package,
  Film,
  HardDrive,
  Server,
  FolderInput,
  ArrowRight,
  GripVertical,
  AlertTriangle,
  ListX
} from 'lucide-react';
import { PROVIDERS, getProviderConfig, getStatusConfig } from '../config/providers';
import { toast } from '../store/toastStore';

const statusIconMap = {
  CheckCircle,
  AlertCircle,
  Clock
};

const providerIconMap = {
  Cloud,
  Play,
  Package,
  Film,
  HardDrive,
  Server
};

function formatSize(bytes) {
  if (!bytes) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDate(dateString) {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function parseRcloneFileId(fileId) {
  const raw = String(fileId || '');
  const separatorIndex = raw.indexOf(':');
  if (separatorIndex <= 0) {
    return { remoteName: null, remotePath: null };
  }

  return {
    remoteName: raw.slice(0, separatorIndex),
    remotePath: raw.slice(separatorIndex + 1)
  };
}

function flattenFolderTreeForMove(folderTree, excludeFolderId) {
  const result = [];

  const walk = (node, parentPath = '', depth = 0) => {
    (node?.folders || []).forEach((folder) => {
      if (folder.id === excludeFolderId) return;
      const currentPath = parentPath ? `${parentPath}/${folder.name}` : folder.name;
      const hasSubfolders = (folder.children?.folders || []).length > 0;
      const isDescendant = (() => {
        const check = (n) => {
          if (!n?.folders) return false;
          return n.folders.some((f) => f.id === excludeFolderId || check(f.children));
        };
        return check(folder.children);
      })();

      if (!isDescendant) {
        result.push({
          id: folder.id,
          name: folder.name,
          path: currentPath,
          depth,
          hasSubfolders
        });
      }

      walk(folder.children, currentPath, depth + 1);
    });
  };

  walk(folderTree, '', 0);
  return result;
}

function MoveFolderModal({ folder, folderTree, onClose, onMove }) {
  const [selectedParentId, setSelectedParentId] = useState(null);
  const [search, setSearch] = useState('');
  const moveFolder = useMoveFolder();

  const targetFolders = useMemo(() => {
    if (!folderTree) return [];
    const flat = flattenFolderTreeForMove(folderTree, folder?.id);
    flat.unshift({ id: 'root', name: 'Root', path: '/', depth: -1, hasSubfolders: true });
    return flat;
  }, [folderTree, folder?.id]);

  const filtered = useMemo(() => {
    if (!search.trim()) return targetFolders;
    const q = search.toLowerCase();
    return targetFolders.filter((f) => f.name.toLowerCase().includes(q) || f.path.toLowerCase().includes(q));
  }, [targetFolders, search]);

  const handleMove = async () => {
    if (!selectedParentId) return;
    try {
      await onMove({ folderId: folder.id, newParentId: selectedParentId });
      toast.success('Folder Moved', `"${folder.name}" moved successfully`);
      onClose();
    } catch (error) {
      toast.error('Move Failed', error?.response?.data?.error || error.message);
    }
  };

  const selectedTarget = targetFolders.find((f) => f.id === selectedParentId);

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-5 max-w-md w-full max-h-[85vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-lg font-medium text-white flex items-center gap-2">
              <FolderInput className="w-5 h-5 text-blue-400" />
              Move Folder
            </h3>
            <p className="text-xs text-[#888] mt-1">
              Pindahkan <strong className="text-white">{folder?.name}</strong> ke folder lain
            </p>
          </div>
          <button onClick={onClose} className="p-1.5 hover:bg-[#222] rounded">
            <X className="w-5 h-5 text-[#666]" />
          </button>
        </div>

        <div className="mb-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-[#555]" />
            <input
              type="text"
              placeholder="Cari folder..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full pl-9 pr-4 py-2.5 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-sm text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#444]"
            />
          </div>
        </div>

        <div className="flex-1 overflow-y-auto border border-[#222] rounded-lg bg-[#0d0d0d] mb-4 min-h-[200px] max-h-[400px]">
          {filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-10">
              <Folder className="w-8 h-8 text-[#333] mb-2" />
              <p className="text-xs text-[#666]">Tidak ada folder ditemukan</p>
            </div>
          ) : (
            <div className="py-1">
              {filtered.map((f) => {
                const isSelected = selectedParentId === f.id;
                const isRoot = f.id === 'root';
                return (
                  <button
                    key={f.id}
                    onClick={() => setSelectedParentId(f.id)}
                    className={`w-full flex items-center gap-2 px-3 py-2 text-left text-sm transition-colors ${
                      isSelected
                        ? 'bg-blue-500/10 text-blue-300 border-l-2 border-blue-400'
                        : 'text-[#aaa] hover:bg-[#141414] hover:text-white border-l-2 border-transparent'
                    }`}
                    style={{ paddingLeft: `${(f.depth + 1) * 16 + 12}px` }}
                  >
                    {isRoot ? (
                      <HomeIcon className="w-4 h-4 flex-shrink-0 text-[#666]" />
                    ) : (
                      <Folder className={`w-4 h-4 flex-shrink-0 ${isSelected ? 'text-blue-400' : 'text-[#555]'}`} />
                    )}
                    <span className="truncate flex-1">{f.name}</span>
                    {f.hasSubfolders && !isRoot && (
                      <span className="text-[10px] text-[#555] flex-shrink-0">📁</span>
                    )}
                    {isSelected && (
                      <CheckCircle className="w-4 h-4 text-blue-400 flex-shrink-0" />
                    )}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {selectedTarget && (
          <div className="mb-4 p-3 rounded-lg bg-[#1a1a1a] border border-[#2a2a2a]">
            <p className="text-xs text-[#888]">
              <strong className="text-white">{folder?.name}</strong>
              <ArrowRight className="w-3 h-3 inline mx-1.5 text-[#555]" />
              <strong className="text-white">{selectedTarget.name}</strong>
            </p>
          </div>
        )}

        <div className="flex gap-3 justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2.5 text-sm text-[#888] hover:text-white rounded-lg hover:bg-[#222] transition-colors"
          >
            Batal
          </button>
          <button
            onClick={handleMove}
            disabled={!selectedParentId || moveFolder.isLoading}
            className="px-5 py-2.5 text-sm rounded-lg font-medium bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 border border-blue-500/30 transition-colors disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {moveFolder.isLoading ? (
              <><RefreshCw className="w-4 h-4 animate-spin" /> Moving...</>
            ) : (
              <><FolderInput className="w-4 h-4" /> Pindahkan</>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

// File Detail Modal Component
function FileDetailModal({ file, onClose }) {
  const reuploadToProvider = useReuploadToProvider();
  const clearFileProviderLink = useClearFileProviderLink();
  const { data: providerStatus, refetch: refetchStatus, isFetching } = useFileProvidersStatus(file?.id);
  const [selectedSource, setSelectedSource] = useState('');
  const [selectedProvider, setSelectedProvider] = useState('');

  if (!file) return null;

  const showSourceSelect = (provider) => {
    setSelectedProvider(provider);
    setSelectedSource('');
  };

  const cancelSourceSelect = () => {
    setSelectedProvider('');
    setSelectedSource('');
  };

  const handleReupload = async (provider, source) => {
    if (!source) {
      throw new Error('Source is required for reupload');
    }
    const response = await reuploadToProvider.mutateAsync({ fileId: file.id, provider, source });
    setSelectedProvider('');
    setSelectedSource('');

    if (response?.jobId) {
      onClose?.();
      return;
    }

    refetchStatus();
  };

  const handleClearProviderLink = async (provider) => {
    const confirmed = window.confirm(`Remove stored link for ${getProviderConfig(provider)?.name || provider}? This will mark the provider as needing re-upload.`);
    if (!confirmed) return;
    await clearFileProviderLink.mutateAsync({
      fileId: file.id,
      provider,
      reason: 'Provider link removed by user from file detail'
    });
    refetchStatus();
  };

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
      <div className="card p-5 max-w-lg w-full max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-medium text-white">File Details</h3>
          <button onClick={onClose} className="p-1.5 hover:bg-[#222] rounded">
            <X className="w-5 h-5 text-[#666]" />
          </button>
        </div>

        <div className="space-y-4">
          <div>
            <p className="text-xs text-[#666] uppercase mb-1">Filename</p>
            <p className="text-sm text-white">{file.name}</p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs text-[#666] uppercase mb-1">Size</p>
              <p className="text-sm text-[#ccc]">{formatSize(file.size)}</p>
            </div>
            <div>
              <p className="text-xs text-[#666] uppercase mb-1">Status</p>
              <p className="text-sm text-[#ccc]">{file.status}</p>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="text-xs text-[#666] uppercase">Providers</p>
              <button
                onClick={() => refetchStatus()}
                disabled={isFetching}
                className="flex items-center gap-1 text-xs text-[#888] hover:text-white"
              >
                <RefreshCw className={`w-3 h-3 ${isFetching ? 'animate-spin' : ''}`} />
                Check Status
              </button>
            </div>

            <div className="space-y-2">
              {Object.entries(file.providers).map(([key, ps]) => {
                const config = getProviderConfig(key);
                const providerLabel = ps?.providerName || config.name;
                const ProviderIcon = providerIconMap[config?.icon] || Cloud;
                const remoteStatus = providerStatus?.[key];
                const availableSources = Object.entries(file.providers || {})
                  .filter(([_, sourcePs]) => sourcePs.status === 'completed')
                  .filter(([sourceKey]) => sourceKey !== key);
                const canClearLink = ps.status === 'completed' || Boolean(ps.url || ps.embedUrl || ps.fileId);
                const isClearingLink = clearFileProviderLink.isLoading
                  && clearFileProviderLink.variables?.fileId === file.id
                  && clearFileProviderLink.variables?.provider === key;
                
                return (
                  <div key={key} className={`p-3 rounded-lg ${config.bgColor} border border-[#222]`}>
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <ProviderIcon className={`w-4 h-4 ${config.color}`} />
                          <span className={`text-sm font-medium ${config.color}`}>{providerLabel}</span>
                      </div>
                      {ps.status === 'completed' ? (
                        <span className="flex items-center gap-1 text-xs text-green-400">
                          <CheckCircle className="w-3 h-3" />
                          Uploaded
                        </span>
                      ) : ps.status === 'failed' ? (
                        <span className="flex items-center gap-1 text-xs text-red-400">
                          <AlertCircle className="w-3 h-3" />
                          Failed
                        </span>
                      ) : ps.status === 'uploading' ? (
                        <span className="flex items-center gap-1 text-xs text-blue-400">
                          <RefreshCw className="w-3 h-3 animate-spin" />
                          Uploading
                        </span>
                      ) : (
                        <span className="flex items-center gap-1 text-xs text-[#666]">
                          <Clock className="w-3 h-3" />
                          Pending
                        </span>
                      )}
                    </div>

                    {remoteStatus && (
                      <div className="text-xs text-[#888] mb-2">
                        Remote: {remoteStatus.remoteExists ? (
                          <span className="text-green-400">Exists</span>
                        ) : ps.status === 'completed' ? (
                          <span className="text-red-400">Missing!</span>
                        ) : (
                          <span className="text-[#555]">N/A</span>
                        )}
                      </div>
                    )}

                    {(() => {
                      const displayUrl = ps.embedUrl || ps.url;
                      if (!displayUrl) return null;
                      return (
                        <a
                          href={displayUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs text-[#888] hover:text-white flex items-center gap-1 mb-2"
                        >
                          <ExternalLink className="w-3 h-3" />
                          View URL
                        </a>
                      );
                    })()}

                    {ps.error && (
                      <p className="text-xs text-red-400 mb-2">{ps.error}</p>
                    )}

                    {canClearLink && (
                      <button
                        onClick={() => handleClearProviderLink(key)}
                        disabled={isClearingLink || reuploadToProvider.isLoading}
                        className="flex items-center gap-1 text-xs text-red-300 hover:text-red-200 mb-2 disabled:opacity-50"
                      >
                        {isClearingLink ? (
                          <RefreshCw className="w-3 h-3 animate-spin" />
                        ) : (
                          <Trash2 className="w-3 h-3" />
                        )}
                        {isClearingLink ? 'Removing link...' : 'Remove stored link'}
                      </button>
                    )}

                    {selectedProvider === key ? (
                      <div className="space-y-2">
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-[#888]">Source:</span>
                          <select
                            value={selectedSource}
                            onChange={(e) => setSelectedSource(e.target.value)}
                            className="flex-1 text-xs bg-[#222] text-white rounded px-2 py-1 border border-[#333]"
                            disabled={reuploadToProvider.isLoading}
                          >
                            <option value="">Select source...</option>
                            {availableSources.map(([sourceKey, sourcePs]) => {
                              const config = getProviderConfig(sourceKey);
                              return (
                            <option key={sourceKey} value={sourceKey}>
                              {(sourcePs?.providerName || config.name)} ({sourceKey})
                            </option>
                          );
                        })}
                          </select>
                        </div>
                        <div className="flex gap-2">
                          <button
                            onClick={() => handleReupload(key, selectedSource)}
                            disabled={reuploadToProvider.isLoading || !selectedSource}
                            className="flex-1 flex items-center justify-center gap-1 text-xs bg-[#333] hover:bg-[#444] text-white rounded px-2 py-1"
                          >
                            {reuploadToProvider.isLoading ? (
                              <RefreshCw className="w-3 h-3 animate-spin" />
                            ) : (
                              <Upload className="w-3 h-3" />
                            )}
                            Re-upload from {selectedSource ? (file.providers?.[selectedSource]?.providerName || getProviderConfig(selectedSource)?.name) : 'source'}
                          </button>
                          <button
                            onClick={cancelSourceSelect}
                            disabled={reuploadToProvider.isLoading}
                            className="text-xs bg-[#222] hover:bg-[#333] text-[#888] rounded px-2 py-1"
                          >
                            Cancel
                          </button>
                        </div>
                      </div>
                    ) : (
                      <button
                        onClick={() => showSourceSelect(key)}
                        disabled={reuploadToProvider.isLoading}
                        className="flex items-center gap-1 text-xs text-[#888] hover:text-white"
                      >
                        <Upload className="w-3 h-3" />
                        Re-upload
                      </button>
                    )}
                  </div>
                );
              })}
            </div>

            {(() => {
              const rcloneEntries = Object.entries(file.providers || {})
                .filter(([providerKey]) => providerKey === 'rclone' || providerKey.startsWith('rclone:'));

              if (rcloneEntries.length === 0) return null;

              return rcloneEntries.map(([providerKey, rcloneStatus]) => {
                const parsed = parseRcloneFileId(rcloneStatus.fileId);
                const googleDriveFileId = rcloneStatus.publicId || null;

                return (
                  <div key={`rclone-info-${providerKey}`} className="mt-3 p-3 rounded-lg bg-red-400/10 border border-red-400/20">
                    <div className="flex items-center justify-between mb-2">
                      <p className="text-xs uppercase tracking-wide text-red-300">{rcloneStatus?.providerName || getProviderConfig(providerKey).name}</p>
                      <span className="text-[11px] text-red-200">{rcloneStatus.status || 'pending'}</span>
                    </div>
                    <div className="space-y-1 text-xs text-[#ddd]">
                      <p>Remote: <span className="text-white">{parsed.remoteName || '-'}</span></p>
                      <p>Path: <span className="text-white break-all">{parsed.remotePath || '-'}</span></p>
                      {googleDriveFileId ? <p>Google Drive File ID: <span className="text-white break-all">{googleDriveFileId}</span></p> : null}
                      <p>Public URL: <span className="text-white break-all">{rcloneStatus.url || '-'}</span></p>
                    </div>
                  </div>
                );
              });
            })()}
          </div>
        </div>
      </div>
    </div>
  );
}

function RemoveProblemFilesModal({ files, isLoading, isDeleting, onConfirm, onClose }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70" onClick={onClose}>
      <div className="w-full max-w-lg bg-[#1a1a1a] rounded-xl border border-[#333] shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center gap-3 p-4 border-b border-[#222]">
          <div className="w-9 h-9 rounded-lg bg-red-500/10 flex items-center justify-center">
            <AlertTriangle className="w-5 h-5 text-red-400" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-white">Remove Problem Files</h3>
            <p className="text-xs text-[#888]">{files.length} file akan dihapus secara permanen</p>
          </div>
          <button onClick={onClose} className="ml-auto p-1 text-[#666] hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="max-h-80 overflow-y-auto p-4 space-y-2">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="w-5 h-5 text-[#666] animate-spin" />
            </div>
          ) : (
            files.map((file) => (
              <div key={file.id} className="flex items-center gap-3 p-2.5 rounded-lg bg-[#0d0d0d] border border-[#222]">
                <div className={`w-7 h-7 rounded flex items-center justify-center flex-shrink-0 ${file.status === 'processing' ? 'bg-amber-500/10' : 'bg-red-500/10'}`}>
                  {file.status === 'processing' ? (
                    <Clock className="w-3.5 h-3.5 text-amber-400" />
                  ) : (
                    <AlertCircle className="w-3.5 h-3.5 text-red-400" />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm text-[#ccc] truncate">{file.name}</p>
                  <div className="flex items-center gap-2 text-xs text-[#666]">
                    <span>{formatSize(file.size)}</span>
                    <span className="text-[#333]">|</span>
                    <span>{formatDate(file.createdAt)}</span>
                  </div>
                </div>
                <span className={`text-[10px] px-1.5 py-0.5 rounded ${file.status === 'processing' ? 'bg-amber-400/10 text-amber-400' : 'bg-red-400/10 text-red-400'}`}>
                  {file.status}
                </span>
              </div>
            ))
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t border-[#222]">
          <button
            onClick={onClose}
            disabled={isDeleting}
            className="px-4 py-2 text-xs rounded-lg bg-[#222] text-[#aaa] hover:bg-[#333] hover:text-white transition-colors disabled:opacity-50"
          >
            Batal
          </button>
          <button
            onClick={onConfirm}
            disabled={isDeleting || files.length === 0}
            className="px-4 py-2 text-xs rounded-lg bg-red-500/20 text-red-400 hover:bg-red-500/30 border border-red-500/30 transition-colors disabled:opacity-50 flex items-center gap-1.5"
          >
            {isDeleting ? (
              <>
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                Menghapus...
              </>
            ) : (
              <>
                <Trash2 className="w-3.5 h-3.5" />
                Hapus {files.length} File
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function FileListPage() {
  const navigate = useNavigate();
  const setCurrentFile = usePlayerStore((state) => state.setCurrentFile);

  const [currentFolderId, setCurrentFolderId] = useState('root');
  const [newFolderName, setNewFolderName] = useState('');
  const [expandedFolders, setExpandedFolders] = useState({ root: true });
  const [showNewFolderInput, setShowNewFolderInput] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('date');
  const [sortOrder, setSortOrder] = useState('desc');
  const [fileMenuOpen, setFileMenuOpen] = useState(null);
  const [showRemoveFailedModal, setShowRemoveFailedModal] = useState(false);

  const {
    data: files,
    isLoading: isFilesLoading,
    isFetching: isFilesFetching
  } = useFiles(currentFolderId);
  const {
    data: currentFolder,
    isLoading: isFolderLoading,
    isFetching: isFolderFetching
  } = useFolder(currentFolderId);
  const { data: folderTree } = useFolders();
  const forceDeleteFile = useForceDeleteFile();
  const createFolder = useCreateFolder();
  const moveFile = useMoveFile();
  const moveFolder = useMoveFolder();
  const purgeFolder = usePurgeFolder();
  const problemFilesQuery = useProblemFiles();
  const deleteAllProblemFiles = useDeleteAllProblemFiles();
  const checkAllFilesCompleteness = useCheckAllFilesCompleteness();
  const checkFileCompleteness = useCheckFileCompleteness();
  const [moveFolderTarget, setMoveFolderTarget] = useState(null);

  const allFolders = useMemo(() => {
    const result = [{ id: 'root', name: 'Root', path: '/', depth: 0 }];

    const walk = (node, parentPath = '/', depth = 0) => {
      (node?.folders || []).forEach((folder) => {
        const fullPath = parentPath === '/' ? `/${folder.name}` : `${parentPath}/${folder.name}`;
        result.push({ id: folder.id, name: folder.name, path: fullPath, depth: depth + 1 });
        walk(folder.children, fullPath, depth + 1);
      });
    };

    if (folderTree) walk(folderTree, '/', 0);
    return result;
  }, [folderTree]);

  const folderStatsById = useMemo(() => {
    const map = new Map();

    const walk = (node) => {
      if (!node?.folders) return;

      node.folders.forEach((folder) => {
        const childNode = folder.children || { folders: [], files: [] };
        map.set(folder.id, {
          folderCount: (childNode.folders || []).length,
          fileCount: (childNode.files || []).length
        });
        walk(childNode);
      });
    };

    walk(folderTree);
    return map;
  }, [folderTree]);

  const isInitialLoading = (isFilesLoading || isFolderLoading) && !files && !currentFolder;
  const isFolderTransitioning = !isInitialLoading && (isFilesFetching || isFolderFetching);

  const filteredAndSortedFiles = useMemo(() => {
    if (!files) return [];
    
    let result = [...files];
    
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      result = result.filter(file => 
        file.name.toLowerCase().includes(query)
      );
    }
    
    result.sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case 'name':
          comparison = a.name.localeCompare(b.name);
          break;
        case 'size':
          comparison = (a.size || 0) - (b.size || 0);
          break;
        case 'status':
          comparison = a.status.localeCompare(b.status);
          break;
        case 'date':
        default:
          comparison = new Date(a.createdAt || 0) - new Date(b.createdAt || 0);
          break;
      }
      return sortOrder === 'asc' ? comparison : -comparison;
    });
    
    return result;
  }, [files, searchQuery, sortBy, sortOrder]);

  const currentFolderPath = useMemo(() => {
    if (currentFolderId === 'root') return '/';
    return currentFolder?.path || '/';
  }, [currentFolderId, currentFolder]);

  const toggleFolderExpand = (folderId) => {
    setExpandedFolders((prev) => ({ ...prev, [folderId]: !prev[folderId] }));
  };

  const breadcrumbPath = useMemo(() => {
    if (currentFolderId === 'root') return [{ id: 'root', name: 'Root' }];
    
    const segments = currentFolderPath.split('/').filter(Boolean);
    const result = [{ id: 'root', name: 'Root' }];
    
    let currentPath = '';
    segments.forEach((name) => {
      currentPath = currentPath === '' ? `/${name}` : `${currentPath}/${name}`;
      const folder = allFolders.find((f) => f.path === currentPath);
      if (folder) {
        result.push({ id: folder.id, name: folder.name });
      }
    });
    
    return result;
  }, [currentFolderId, currentFolderPath, allFolders]);

  useEffect(() => {
    if (currentFolderId === 'root') return;

    setExpandedFolders((prev) => {
      const next = { ...prev, root: true };
      breadcrumbPath.forEach((segment) => {
        next[segment.id] = true;
      });
      return next;
    });
  }, [currentFolderId, breadcrumbPath]);

  const handleFolderChange = (folderId) => {
    setCurrentFolderId(folderId);
    setFileMenuOpen(null);
  };

  const handlePlay = (file) => {
    setCurrentFile(file);
    navigate('/player');
  };

  const handleDelete = async (fileId, name) => {
    if (!window.confirm(`Delete "${name}"?`)) return;
    try {
      await forceDeleteFile.mutateAsync(fileId);
    } catch (error) {
      alert('Delete failed: ' + error.message);
    }
  };

  const handleOpenRemoveFailedModal = async () => {
    const result = await problemFilesQuery.refetch();
    if (!result.data || result.data.length === 0) {
      toast.info('Tidak ada file dengan status processing, failed, atau cancelled');
      return;
    }
    setShowRemoveFailedModal(true);
  };

  const handleConfirmRemoveFailed = async () => {
    try {
      const result = await deleteAllProblemFiles.mutateAsync();
      const deleted = result.results?.filter((r) => r.deleted).length || 0;
      const failed = result.results?.filter((r) => !r.deleted).length || 0;
      toast.success(`${deleted} file problem berhasil dihapus${failed > 0 ? `, ${failed} gagal dihapus` : ''}`);
      setShowRemoveFailedModal(false);
    } catch (error) {
      toast.error('Gagal menghapus file problem: ' + error.message);
    }
  };

  const handleCheckCompleteness = async (fileId = null) => {
    try {
      if (fileId) {
        const result = await checkFileCompleteness.mutateAsync(fileId);
        const { completeness } = result || {};
        const pendingCount = completeness?.pendingProviders?.length || 0;
        const completedCount = completeness?.completedCount || 0;
        const targetCount = completeness?.targetCount || 0;
        const pending = completeness?.pendingProviders || [];

        if (pendingCount === 0) {
          toast.success(
            'File sudah lengkap',
            `${result?.file?.name || 'File ini'}: ${completedCount}/${targetCount} provider aktif`
          );
        } else {
          toast.warning(
            `File belum lengkap (${pendingCount}/${targetCount})`,
            `${result?.file?.name || 'File ini'}: missing ${pending.join(', ')}`
          );
        }
        return;
      }

      const result = await checkAllFilesCompleteness.mutateAsync();
      const complete = result?.complete || 0;
      const incomplete = result?.incomplete || 0;
      const total = result?.total || 0;
      const incompleteFiles = (result?.results || []).filter((r) => (r.pendingProviders || []).length > 0);

      if (incomplete === 0) {
        toast.success(
          `Semua file lengkap (${total}/${total})`,
          `Seluruh ${total} file sudah lengkap di semua provider aktif`
        );
      } else {
        const sample = incompleteFiles.slice(0, 3);
        const details = sample.map((f) => `${f.name}: ${(f.pendingProviders || []).join(', ')}`).join('\n');
        const more = incompleteFiles.length > 3 ? `\n...dan ${incompleteFiles.length - 3} file lainnya` : '';
        toast.warning(
          `${incomplete} dari ${total} file belum lengkap`,
          `${details}${more}`
        );
      }
    } catch (error) {
      toast.error('Gagal cek kelengkapan', error?.response?.data?.error || error.message);
    }
  };

  const handleCreateFolder = async () => {
    const trimmed = newFolderName.trim();
    if (!trimmed) return;

    try {
      await createFolder.mutateAsync({ name: trimmed, parentId: currentFolderId });
      setNewFolderName('');
    } catch (error) {
      alert('Create folder failed: ' + error.message);
    }
  };

  const handleMoveFile = async (fileId, folderId) => {
    try {
      await moveFile.mutateAsync({ fileId, folderId });
      setFileMenuOpen(null);
    } catch (error) {
      alert('Move file failed: ' + error.message);
    }
  };

  const handleMoveFolder = async ({ folderId, newParentId }) => {
    await moveFolder.mutateAsync({ folderId, newParentId });
    if (currentFolderId === folderId) {
      setCurrentFolderId(newParentId);
    }
  };

  const handlePurgeCurrentFolder = async () => {
    if (currentFolderId === 'root') {
      return;
    }

    const folderName = currentFolder?.name || 'folder ini';
    const confirmMessage = `Hapus folder "${folderName}" beserta semua subfolder, file, provider status, dan jobs dari database? Aksi ini tidak bisa dibatalkan.`;
    if (!window.confirm(confirmMessage)) {
      return;
    }

    try {
      await purgeFolder.mutateAsync(currentFolderId);
      handleFolderChange('root');
    } catch (error) {
      alert('Hapus folder gagal: ' + error.message);
    }
  };

  const handleToggleSort = (newSortBy) => {
    if (sortBy === newSortBy) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(newSortBy);
      setSortOrder('desc');
    }
  };

  const [folderContextMenu, setFolderContextMenu] = useState(null);

  const FolderTree = ({ node, depth = 0 }) => {
    if (!node) return null;
    
    const folders = node.folders || [];
    
    return (
      <div className="space-y-0.5">
        {folders.map((folder) => {
          const hasChildren = (folder.children?.folders || []).length > 0;
          const isExpanded = expandedFolders[folder.id];
          const isCurrent = currentFolderId === folder.id;
          
          return (
            <div key={folder.id}>
              <div
                className={`flex items-center group/folder ${
                  isCurrent ? 'bg-[#333] text-white rounded' : 'text-[#aaa] hover:bg-[#222] hover:text-white rounded'
                }`}
                style={{ paddingLeft: `${depth * 12 + 8}px` }}
              >
                <button
                  onClick={() => {
                    handleFolderChange(folder.id);
                    if (!expandedFolders[folder.id]) {
                      toggleFolderExpand(folder.id);
                    }
                  }}
                  className="flex-1 flex items-center gap-1.5 px-2 py-1.5 text-left text-sm transition-colors min-w-0"
                >
                  {hasChildren ? (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleFolderExpand(folder.id);
                      }}
                      className="p-0.5 hover:bg-[#444] rounded flex-shrink-0"
                    >
                      {isExpanded ? (
                        <ChevronDown className="w-3 h-3" />
                      ) : (
                        <ChevronRight className="w-3 h-3" />
                      )}
                    </button>
                  ) : (
                    <span className="w-4" />
                  )}
                  <Folder className={`w-4 h-4 flex-shrink-0 ${isCurrent ? 'text-blue-400' : 'text-[#666]'}`} />
                  <span className="truncate">{folder.name}</span>
                </button>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    setMoveFolderTarget(folder);
                  }}
                  className="p-1 mr-1 rounded text-[#555] hover:text-blue-400 hover:bg-blue-500/10 opacity-0 group-hover/folder:opacity-100 transition-all flex-shrink-0"
                  title="Move folder"
                >
                  <FolderInput className="w-3 h-3" />
                </button>
              </div>
              {isExpanded && folder.children && (
                <FolderTree node={folder.children} depth={depth + 1} />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  if (isInitialLoading) {
    return (
      <div className="space-y-4">
        <div className="h-6 w-24 bg-[#222] rounded animate-pulse" />
        {[1, 2, 3].map((i) => (
          <div key={i} className="card p-4 h-16 animate-pulse bg-[#1a1a1a]" />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-xl font-semibold text-white">Files</h2>
        <div className="flex items-center gap-1 mt-1 text-sm text-[#888]">
          {breadcrumbPath.map((segment, index) => (
            <React.Fragment key={segment.id}>
              {index > 0 && <ChevronRight className="w-3 h-3" />}
              <button
                onClick={() => handleFolderChange(segment.id)}
                className={`hover:text-white transition-colors ${
                  index === breadcrumbPath.length - 1 ? 'text-white font-medium' : ''
                }`}
              >
                {segment.id === 'root' ? (
                  <span className="flex items-center gap-1">
                    <HomeIcon className="w-3.5 h-3.5" />
                    Root
                  </span>
                ) : (
                  segment.name
                )}
              </button>
            </React.Fragment>
          ))}
        </div>
      </div>

      <div className="card p-4">
        <div className="flex gap-4">
          <div className="w-52 flex-shrink-0 border-r border-[#222] pr-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-[#666] uppercase tracking-wide">Folders</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={handlePurgeCurrentFolder}
                  disabled={currentFolderId === 'root' || purgeFolder.isLoading}
                  className="p-1 hover:bg-[#333] rounded text-[#666] hover:text-red-400 disabled:opacity-40 disabled:cursor-not-allowed"
                  title="Delete folder with contents"
                >
                  <Trash2 className={`w-3.5 h-3.5 ${purgeFolder.isLoading ? 'animate-pulse' : ''}`} />
                </button>
                <button
                  onClick={() => setShowNewFolderInput(!showNewFolderInput)}
                  className="p-1 hover:bg-[#333] rounded text-[#666] hover:text-white"
                  title="New folder"
                >
                  <Plus className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>

            {currentFolderId !== 'root' && (
              <p className="text-[11px] text-[#666] mb-2">
                Tombol tempat sampah akan menghapus folder aktif beserta seluruh isi dari database.
              </p>
            )}
            
            {showNewFolderInput && (
              <div className="mb-2 space-y-2">
                <input
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value)}
                  placeholder="Folder name"
                  className="input !text-xs !py-1.5"
                  autoFocus
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      handleCreateFolder();
                      setShowNewFolderInput(false);
                    }
                    if (e.key === 'Escape') {
                      setShowNewFolderInput(false);
                      setNewFolderName('');
                    }
                  }}
                />
                <div className="flex gap-1">
                  <button
                    onClick={() => {
                      handleCreateFolder();
                      setShowNewFolderInput(false);
                    }}
                    className="btn btn-primary !py-1 !px-2 text-xs flex-1"
                  >
                    Create
                  </button>
                  <button
                    onClick={() => {
                      setShowNewFolderInput(false);
                      setNewFolderName('');
                    }}
                    className="btn !py-1 !px-2 text-xs"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            <div className="space-y-0.5 max-h-[calc(100vh-300px)] overflow-y-auto">
              <button
                onClick={() => handleFolderChange('root')}
                className={`w-full flex items-center gap-2 px-2 py-1.5 rounded text-left text-sm transition-colors ${
                  currentFolderId === 'root'
                    ? 'bg-[#333] text-white'
                    : 'text-[#aaa] hover:bg-[#222] hover:text-white'
                }`}
              >
                <HomeIcon className={`w-4 h-4 flex-shrink-0 ${currentFolderId === 'root' ? 'text-blue-400' : 'text-[#666]'}`} />
                <span>Root</span>
              </button>
              {folderTree && <FolderTree node={folderTree} />}
            </div>
          </div>

          <div className={`flex-1 min-w-0 relative transition-opacity duration-150 ${isFolderTransitioning ? 'opacity-90' : 'opacity-100'}`}>
            {isFolderTransitioning && (
              <div className="absolute right-0 -top-1 z-20 flex items-center gap-1 px-2 py-1 rounded-md bg-[#0d0d0d]/90 border border-[#2a2a2a] text-[11px] text-[#999]">
                <RefreshCw className="w-3 h-3 animate-spin" />
                Updating folder
              </div>
            )}

            {!(currentFolder?.children || []).length && !filteredAndSortedFiles?.length && !searchQuery && (
              <div className="flex flex-col items-center justify-center py-16 px-4">
                <div className="w-20 h-20 rounded-2xl bg-[#1a1a1a] flex items-center justify-center mb-4">
                  <Folder className="w-10 h-10 text-[#444]" />
                </div>
                <h3 className="text-lg font-medium text-[#aaa] mb-1">
                  This folder is empty
                </h3>
                <p className="text-sm text-[#666] mb-6 text-center max-w-sm">
                  Upload a video or create a subfolder to organize your files
                </p>
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => setShowNewFolderInput(true)}
                    className="btn flex items-center gap-2"
                  >
                    <FolderPlus className="w-4 h-4" />
                    New Folder
                  </button>
                  <button
                    onClick={() => navigate('/')}
                    className="btn btn-primary flex items-center gap-2"
                  >
                    <Upload className="w-4 h-4" />
                    Upload
                  </button>
                </div>
              </div>
            )}

            {(currentFolder?.children || []).length > 0 && (
              <div className="mb-4">
                <p className="text-xs font-medium text-[#666] uppercase tracking-wide mb-2">Subfolders</p>
                <div className="space-y-1.5">
                  {currentFolder.children.map((child) => {
                    const stats = folderStatsById.get(child.id) || { folderCount: 0, fileCount: 0 };

                    return (
                      <div
                        key={child.id}
                        className="group flex items-center gap-3 p-3 rounded-lg bg-[#1a1a1a] border border-[#222] hover:border-[#333] hover:bg-[#202020] transition-colors"
                      >
                        <button
                          onClick={() => handleFolderChange(child.id)}
                          className="flex items-center gap-3 flex-1 min-w-0 text-left"
                        >
                          <div className="w-9 h-9 rounded-lg bg-[#222] flex items-center justify-center flex-shrink-0">
                            <Folder className="w-4 h-4 text-yellow-500/80" />
                          </div>

                          <div className="min-w-0 flex-1">
                            <h3 className="text-sm font-medium text-[#ccc] truncate group-hover:text-white transition-colors">
                              {child.name}
                            </h3>
                            <p className="text-xs text-[#666] group-hover:text-[#888] transition-colors">
                              {stats.folderCount} folder | {stats.fileCount} file
                            </p>
                          </div>

                          <ChevronRight className="w-4 h-4 text-[#555] group-hover:text-[#888] group-hover:translate-x-0.5 transition-all" />
                        </button>

                        <button
                          onClick={() => setMoveFolderTarget(child)}
                          className="p-1.5 rounded text-[#555] hover:text-blue-400 hover:bg-blue-500/10 opacity-0 group-hover:opacity-100 transition-all"
                          title="Pindahkan folder"
                        >
                          <FolderInput className="w-4 h-4" />
                        </button>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {(currentFolder?.children || []).length > 0 || filteredAndSortedFiles?.length > 0 ? (
              <div className="mb-4">
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex-1 relative">
                    <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-[#555]" />
                    <input
                      type="text"
                      placeholder="Search files..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="w-full pl-9 pr-4 py-2 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-sm text-[#ccc] placeholder-[#555] focus:outline-none focus:border-[#444]"
                    />
                  </div>
                  <div className="flex items-center gap-1 bg-[#1a1a1a] rounded-lg p-1">
                    {['date', 'name', 'size', 'status'].map((sort) => (
                      <button
                        key={sort}
                        onClick={() => handleToggleSort(sort)}
                        className={`px-2.5 py-1 rounded text-xs capitalize transition-colors ${
                          sortBy === sort 
                            ? 'bg-[#333] text-white' 
                            : 'text-[#666] hover:text-[#aaa]'
                        }`}
                      >
                        {sort}
                        {sortBy === sort && (
                          <span className="ml-1">{sortOrder === 'asc' ? '↑' : '↓'}</span>
                        )}
                      </button>
                    ))}
                  </div>
                  <button
                    onClick={handleOpenRemoveFailedModal}
                    disabled={problemFilesQuery.isFetching}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/20 transition-colors disabled:opacity-50"
                    title="Hapus semua file processing, failed, atau cancelled"
                  >
                    {problemFilesQuery.isFetching ? (
                      <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                    ) : (
                      <ListX className="w-3.5 h-3.5" />
                    )}
                    Remove Problem Files
                  </button>
                  <button
                    onClick={() => handleCheckCompleteness()}
                    disabled={checkAllFilesCompleteness.isLoading}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs bg-blue-500/10 text-blue-400 hover:bg-blue-500/20 border border-blue-500/20 transition-colors disabled:opacity-50"
                    title="Cek apakah semua file sudah lengkap di provider aktif"
                  >
                    <ScanSearch className={`w-3.5 h-3.5 ${checkAllFilesCompleteness.isLoading ? 'animate-spin' : ''}`} />
                    {checkAllFilesCompleteness.isLoading ? 'Checking...' : 'Check Completeness'}
                  </button>
                </div>
              </div>
            ) : null}

            {!filteredAndSortedFiles?.length ? (
              <div className="flex flex-col items-center justify-center py-16 px-4">
                <div className="w-20 h-20 rounded-2xl bg-[#1a1a1a] flex items-center justify-center mb-4">
                  <FileVideo className="w-10 h-10 text-[#444]" />
                </div>
                <h3 className="text-lg font-medium text-[#aaa] mb-1">
                  {searchQuery ? 'No results found' : 'No files yet'}
                </h3>
                <p className="text-sm text-[#666] mb-6 text-center max-w-sm">
                  {searchQuery 
                    ? `No files match "${searchQuery}"`
                    : 'Upload your first video to get started'
                  }
                </p>
                {!searchQuery && (
                  <button
                    onClick={() => navigate('/')}
                    className="btn btn-primary flex items-center gap-2"
                  >
                    <Upload className="w-4 h-4" />
                    Upload Video
                  </button>
                )}
              </div>
            ) : (
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-[#666] uppercase tracking-wide mb-2">
                  Files ({filteredAndSortedFiles.length}{searchQuery ? ` of ${files.length}` : ''})
                </p>
                {filteredAndSortedFiles.map((file) => {
                  const status = getStatusConfig(file.status);
                  const StatusIcon = statusIconMap[status.icon] || Clock;

                  return (
                    <div key={file.id} className="group flex items-center gap-3 p-3 rounded-lg bg-[#1a1a1a] border border-[#222] hover:border-[#333] transition-colors">
                      <button
                        onClick={() => handlePlay(file)}
                        disabled={file.syncStatus === 0}
                        className="w-8 h-8 rounded bg-[#222] flex items-center justify-center flex-shrink-0 hover:bg-[#333] disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                      >
                        <Play className="w-4 h-4 text-[#888]" />
                      </button>

                      <button
                        onClick={() => handlePlay(file)}
                        disabled={file.syncStatus === 0}
                        className="min-w-0 flex-1 text-left disabled:opacity-30 disabled:cursor-not-allowed"
                      >
                        <h3 className="text-sm font-medium text-[#ccc] truncate hover:text-white transition-colors">{file.name}</h3>
                        <div className="flex items-center gap-2 text-xs text-[#666]">
                          <span>{formatSize(file.size)}</span>
                          <span className="text-[#333]">|</span>
                          <span>{formatDate(file.createdAt)}</span>
                        </div>
                      </button>

                      <div className="flex items-center gap-1">
                        {Object.entries(file.providers).slice(0, 4).map(([p, s]) => (
                          <span
                            key={p}
                            className={`w-5 h-5 rounded flex items-center justify-center text-[10px] font-medium cursor-pointer hover:scale-110 transition-transform ${
                              s.status === 'completed' ? 'bg-green-400/10 text-green-400' :
                              s.status === 'failed' ? 'bg-red-400/10 text-red-400' :
                              'bg-[#222] text-[#555]'
                            }`}
                            title={`${getProviderConfig(p)?.name || p}: ${s.status}`}
                            onClick={(e) => {
                              e.stopPropagation();
                              setSelectedFile(file);
                            }}
                          >
                            {(() => {
                              const config = getProviderConfig(p);
                              if (config.short === 'Voe') return 'V';
                              if (config.short === 'Cat') return 'C';
                              if (config.short === 'Seek') return 'S';
                              return config.short?.[0] || p[0].toUpperCase();
                            })()}
                          </span>
                        ))}
                        {Object.keys(file.providers || {}).length > 0 && (
                          <span
                            className={`w-5 h-5 rounded flex items-center justify-center text-[10px] font-medium ${
                              file.syncStatus === 100
                                ? 'bg-green-400/10 text-green-400'
                                : file.syncStatus > 0
                                ? 'bg-orange-400/10 text-orange-400'
                                : 'bg-[#222] text-[#555]'
                            }`}
                            title={`${file.syncStatus}% complete across ${Object.keys(file.providers || {}).length} providers`}
                          >
                            {file.syncStatus}%
                          </span>
                        )}
                      </div>

                      <div className={`flex items-center gap-1 px-2 py-0.5 rounded ${status.bg}`}>
                        <StatusIcon className={`w-3 h-3 ${status.color} ${file.status === 'uploading' || file.status === 'processing' ? 'animate-pulse' : ''}`} />
                        <span className={`text-xs ${status.color}`}>{file.status}</span>
                      </div>

                      {file.syncStatus !== 100 && (
                        <div className="w-20">
                          <div className="flex items-center justify-between text-xs mb-0.5">
                            <span className="text-[#666]">{file.syncStatus}%</span>
                          </div>
                          <div className="progress-bar !h-1">
                            <div className="progress-bar-fill" style={{ width: `${file.syncStatus}%` }} />
                          </div>
                        </div>
                      )}

                      <div className="relative">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setFileMenuOpen(fileMenuOpen === file.id ? null : file.id);
                          }}
                          className={`p-1.5 rounded ${fileMenuOpen === file.id ? 'bg-[#333] text-white' : 'text-[#666] hover:bg-[#222] hover:text-white'}`}
                        >
                          <MoreVertical className="w-4 h-4" />
                        </button>
                        
                        {fileMenuOpen === file.id && (
                          <>
                            <div 
                              className="fixed inset-0 z-10" 
                              onClick={() => setFileMenuOpen(null)}
                            />
                            <div className="absolute right-0 top-full mt-1 w-44 py-1 bg-[#1a1a1a] rounded-lg border border-[#333] shadow-xl z-20">
                              <button
                                onClick={() => { setSelectedFile(file); setFileMenuOpen(null); }}
                                className="w-full text-left px-3 py-2 text-xs text-[#aaa] hover:bg-[#222] hover:text-white flex items-center gap-2"
                              >
                                <Eye className="w-3.5 h-3.5" /> View Details
                              </button>
                              <button
                                onClick={() => {
                                  handleCheckCompleteness(file.id);
                                  setFileMenuOpen(null);
                                }}
                                className="w-full text-left px-3 py-2 text-xs text-[#aaa] hover:bg-[#222] hover:text-white flex items-center gap-2"
                              >
                                <ScanSearch className="w-3.5 h-3.5" /> Check Completeness
                              </button>
                               
                              <div className="border-t border-[#222] px-3 py-1.5 text-[10px] text-[#555] uppercase">Move to</div>
                              {allFolders
                                .filter((folder) => folder.id !== file.folderId)
                                .slice(0, 5)
                                .map((folder) => (
                                  <button
                                    key={folder.id}
                                    onClick={() => handleMoveFile(file.id, folder.id)}
                                    className="w-full text-left px-3 py-1.5 text-xs text-[#aaa] hover:bg-[#222] hover:text-white"
                                  >
                                    {folder.path}
                                  </button>
                                ))}
                              
                              {Object.values(file.providers).some((p) => p.url || p.embedUrl) && (
                                <>
                                  <div className="border-t border-[#222] px-3 py-1.5 text-[10px] text-[#555] uppercase">External Links</div>
                                  {Object.entries(file.providers)
                                    .filter(([_, p]) => p.url || p.embedUrl)
                                    .map(([name, p]) => (
                                      <a
                                        key={name}
                                        href={p.embedUrl || p.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="block px-3 py-1.5 text-xs text-[#aaa] hover:bg-[#222] hover:text-white"
                                      >
                                        {getProviderConfig(name)?.name || name}
                                      </a>
                                    ))}
                                </>
                              )}
                              
                              <div className="border-t border-[#222]" />
                              <button
                                onClick={() => { handleDelete(file.id, file.name); setFileMenuOpen(null); }}
                                className="w-full text-left px-3 py-2 text-xs text-red-400 hover:bg-red-500/10 flex items-center gap-2"
                              >
                                <Trash2 className="w-3.5 h-3.5" /> Delete
                              </button>
                            </div>
                          </>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>

      {selectedFile && (
        <FileDetailModal file={selectedFile} onClose={() => setSelectedFile(null)} />
      )}

      {moveFolderTarget && (
        <MoveFolderModal
          folder={moveFolderTarget}
          folderTree={folderTree}
          onClose={() => setMoveFolderTarget(null)}
          onMove={handleMoveFolder}
        />
      )}

      {showRemoveFailedModal && (
        <RemoveProblemFilesModal
          files={problemFilesQuery.data || []}
          isLoading={problemFilesQuery.isFetching}
          isDeleting={deleteAllProblemFiles.isLoading}
          onConfirm={handleConfirmRemoveFailed}
          onClose={() => setShowRemoveFailedModal(false)}
        />
      )}
    </div>
  );
}
