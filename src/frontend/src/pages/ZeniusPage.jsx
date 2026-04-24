import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Download,
  KeyRound,
  Search,
  Loader2,
  FolderOpen,
  Cloud,
  CheckCircle,
  AlertCircle,
  Trash2,
  Link as LinkIcon,
  Shield,
  XCircle,
  RotateCcw,
  Activity,
  Folder,
  Trash,
  ChevronDown,
  ChevronUp,
  ChevronRight,
  Copy,
  Eye,
  Zap,
  HelpCircle,
  ExternalLink,
  RefreshCw,
  Pause,
  Play,
  X,
  Bell,
  BellOff,
  Send,
  FolderTree,
  Home,
  Upload
} from 'lucide-react';
import {
  getZeniusBatchChainStatus,
  useDeleteAllFolders,
  useDeleteFolder,
  useFolders,
  useProviders,
  useSetMaxConcurrent,
  useTestWebhook,
  useUpdateWebhookConfig,
  useUploadConcurrency,
  useSetUploadConcurrency,
  useWebhookConfig,
  useZeniusBatchChain,
  useZeniusBatchDownload,
  useZeniusCancelBatchRun,
  useZeniusCancelAll,
  useZeniusDownload,
  useZeniusInstanceDetails,
  useZeniusQueueStatus,
  useZeniusResetFiles
} from '../hooks/api';
import { getProviderConfig } from '../config/providers';
import { ConfirmDialog } from '../components/ui/ConfirmDialog';
import { toast } from '../store/toastStore';
import { useWebSocketStore } from '../store/websocketStore';

const HEADERS_STORAGE_KEY = 'zenius-headers-raw';
const PROVIDERS_STORAGE_KEY = 'zenius-selected-providers';
const BATCH_PREVIEW_SESSION_STORAGE_KEY = 'zenius-batch-preview-session';
const BATCH_DOWNLOAD_CHUNK_SIZE = 24;
const BATCH_REQUEST_BUDGET_MS = 24000;
const BATCH_PREVIEW_POLL_INTERVAL_MS = 2000;
const BATCH_PREVIEW_ROW_RENDER_LIMIT = 300;

function loadSavedBatchPreviewSession() {
  try {
    const saved = localStorage.getItem(BATCH_PREVIEW_SESSION_STORAGE_KEY);
    if (!saved) return null;
    const parsed = JSON.parse(saved);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return '-';
  const totalSeconds = Math.floor(seconds);
  const hh = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
  const mm = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
  const ss = String(totalSeconds % 60).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function normalizeFolderInput(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return '';
  const unwrapped = ((raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'")))
    ? raw.slice(1, -1).trim()
    : raw;
  let normalized = unwrapped.replace(/\\/g, '/').replace(/^\/+/, '').replace(/\/+/g, '/');
  if (normalized.toLowerCase() === 'root') return '';
  if (normalized.toLowerCase().startsWith('root/')) normalized = normalized.slice(5);
  return normalized.trim();
}

function buildPreviewContextSignature({ rootCgId, targetCgSelector, parentContainerName, folderPrefix, providers, previewMode }) {
  return JSON.stringify({
    rootCgId: String(rootCgId || '').trim(),
    targetCgSelector: String(targetCgSelector || '').trim(),
    parentContainerName: String(parentContainerName || '').trim(),
    folderPrefix: normalizeFolderInput(folderPrefix),
    providers: Array.isArray(providers) ? [...providers].sort() : [],
    previewMode: previewMode || 'fast'
  });
}

function flattenFolderPaths(folderTree) {
  const result = [];
  const walk = (node, parentPath = '') => {
    (node?.folders || []).forEach((folder) => {
      const currentPath = parentPath ? `${parentPath}/${folder.name}` : folder.name;
      result.push({ id: folder.id, path: currentPath });
      walk(folder.children, currentPath);
    });
  };
  walk(folderTree, '');
  return result;
}

function loadSavedProviders() {
  try {
    const saved = localStorage.getItem(PROVIDERS_STORAGE_KEY);
    return saved ? JSON.parse(saved) : [];
  } catch {
    return [];
  }
}

function StatusBadge({ status }) {
  const config = {
    completed: { bg: 'bg-emerald-500/10', text: 'text-emerald-400', label: 'Done' },
    processing: { bg: 'bg-blue-500/10', text: 'text-blue-400', label: 'Processing' },
    uploading: { bg: 'bg-sky-500/10', text: 'text-sky-400', label: 'Uploading' },
    failed: { bg: 'bg-red-500/10', text: 'text-red-400', label: 'Failed' },
    pending: { bg: 'bg-zinc-500/10', text: 'text-zinc-400', label: 'Queued' },
    skipped: { bg: 'bg-amber-500/10', text: 'text-amber-400', label: 'Skipped' },
    ready: { bg: 'bg-emerald-500/10', text: 'text-emerald-300', label: 'Download' },
    retry: { bg: 'bg-orange-500/10', text: 'text-orange-300', label: 'Retry' },
    finalize: { bg: 'bg-cyan-500/10', text: 'text-cyan-300', label: 'Finalize' },
    mapped: { bg: 'bg-sky-500/10', text: 'text-sky-300', label: 'Mapped' }
  };
  const c = config[status] || config.pending;
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium ${c.bg} ${c.text}`}>
      {c.label}
    </span>
  );
}

function UploadConcurrencyCard() {
  const { data: uploadConcurrency } = useUploadConcurrency();
  const setUploadConcurrency = useSetUploadConcurrency();
  const [editingUploads, setEditingUploads] = useState(false);
  const [editingProviders, setEditingProviders] = useState(false);
  const [uploadsInput, setUploadsInput] = useState('');
  const [providersInput, setProvidersInput] = useState('');

  const currentUploads = uploadConcurrency?.maxConcurrentUploads || 2;
  const currentProviders = uploadConcurrency?.maxConcurrentProviders || 4;

  const submitUploads = () => {
    const v = parseInt(uploadsInput, 10);
    if (v >= 1 && v <= 20) {
      setUploadConcurrency.mutate({ maxConcurrentUploads: v });
    }
    setEditingUploads(false);
  };

  const submitProviders = () => {
    const v = parseInt(providersInput, 10);
    if (v >= 1 && v <= 20) {
      setUploadConcurrency.mutate({ maxConcurrentProviders: v });
    }
    setEditingProviders(false);
  };

  return (
    <div className="card p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Upload className="w-4 h-4 text-[#666]" />
          <span className="text-sm font-medium text-[#aaa]">Upload Concurrency</span>
        </div>
        {uploadConcurrency && (
          <div className="flex items-center gap-3 text-sm">
            <span className="text-[#666]">Upload: <span className="text-white">{currentUploads}</span></span>
            <span className="text-[#666]">Provider: <span className="text-white">{currentProviders}</span></span>
            <span className="text-[#666]">Aktif: <span className="text-white">{uploadConcurrency.activeUploads ?? '-'}</span></span>
          </div>
        )}
      </div>

      <div className="flex items-center gap-6">
        <div className="flex items-center gap-2">
          <span className="text-xs text-[#666]">Max Upload/Provider:</span>
          {editingUploads ? (
            <div className="flex items-center gap-1.5">
              <input
                type="number"
                min={1}
                max={20}
                value={uploadsInput}
                onChange={(e) => setUploadsInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') submitUploads();
                  if (e.key === 'Escape') setEditingUploads(false);
                }}
                className="w-14 px-2 py-1 bg-[#0d0d0d] border border-[#3a3a3a] rounded text-xs text-white text-center focus:outline-none focus:border-blue-500"
                autoFocus
              />
              <button onClick={submitUploads} disabled={setUploadConcurrency.isLoading} className="px-2 py-1 rounded text-xs bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 disabled:opacity-40">
                {setUploadConcurrency.isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Set'}
              </button>
              <button onClick={() => setEditingUploads(false)} className="px-2 py-1 rounded text-xs text-[#666] hover:text-white">
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => { setUploadsInput(String(currentUploads)); setEditingUploads(true); }}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-[#0d0d0d] border border-[#2a2a2a] text-xs text-white hover:border-[#3a3a3a] transition-colors"
            >
              <Upload className="w-3 h-3 text-blue-400" />
              {currentUploads}
              <span className="text-[#555]">threads</span>
            </button>
          )}
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs text-[#666]">Max Provider Paralel:</span>
          {editingProviders ? (
            <div className="flex items-center gap-1.5">
              <input
                type="number"
                min={1}
                max={20}
                value={providersInput}
                onChange={(e) => setProvidersInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') submitProviders();
                  if (e.key === 'Escape') setEditingProviders(false);
                }}
                className="w-14 px-2 py-1 bg-[#0d0d0d] border border-[#3a3a3a] rounded text-xs text-white text-center focus:outline-none focus:border-blue-500"
                autoFocus
              />
              <button onClick={submitProviders} disabled={setUploadConcurrency.isLoading} className="px-2 py-1 rounded text-xs bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 disabled:opacity-40">
                {setUploadConcurrency.isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Set'}
              </button>
              <button onClick={() => setEditingProviders(false)} className="px-2 py-1 rounded text-xs text-[#666] hover:text-white">
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => { setProvidersInput(String(currentProviders)); setEditingProviders(true); }}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-[#0d0d0d] border border-[#2a2a2a] text-xs text-white hover:border-[#3a3a3a] transition-colors"
            >
              <Cloud className="w-3 h-3 text-emerald-400" />
              {currentProviders}
              <span className="text-[#555]">providers</span>
            </button>
          )}
        </div>
      </div>

      {setUploadConcurrency.isSuccess && <p className="text-xs text-emerald-400">Upload concurrency updated.</p>}
      {setUploadConcurrency.isError && <p className="text-xs text-red-400">Failed: {setUploadConcurrency.error?.message}</p>}
    </div>
  );
}

function CollapsibleSection({ title, icon: Icon, defaultOpen = false, children, badge, rightElement }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="card overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between p-4 hover:bg-[#1f1f1f] transition-colors"
      >
        <div className="flex items-center gap-2">
          {Icon && <Icon className="w-4 h-4 text-[#666]" />}
          <span className="text-sm font-medium text-[#ccc]">{title}</span>
          {badge && <span className="text-[10px] px-1.5 py-0.5 rounded bg-[#2a2a2a] text-[#888]">{badge}</span>}
        </div>
        <div className="flex items-center gap-2">
          {rightElement}
          {open ? <ChevronUp className="w-4 h-4 text-[#666]" /> : <ChevronDown className="w-4 h-4 text-[#666]" />}
        </div>
      </button>
      {open && <div className="px-4 pb-4 border-t border-[#222]">{children}</div>}
    </div>
  );
}

function FolderPickerModal({ isOpen, onClose, onSelect, folderTree, currentValue, title, description }) {
  const [search, setSearch] = useState('');
  const [selectedId, setSelectedId] = useState(null);

  const folders = useMemo(() => {
    if (!folderTree) return [];
    const result = [{ id: 'root', name: 'Root', path: '/', depth: -1, hasSubfolders: true }];
    const walk = (node, parentPath = '', depth = 0) => {
      (node?.folders || []).forEach((folder) => {
        const currentPath = parentPath ? `${parentPath}/${folder.name}` : folder.name;
        const hasSubfolders = (folder.children?.folders || []).length > 0;
        result.push({ id: folder.id, name: folder.name, path: currentPath, depth, hasSubfolders });
        walk(folder.children, currentPath, depth + 1);
      });
    };
    walk(folderTree, '', 0);
    return result;
  }, [folderTree]);

  const filtered = useMemo(() => {
    if (!search.trim()) return folders;
    const q = search.toLowerCase();
    return folders.filter((f) => f.name.toLowerCase().includes(q) || f.path.toLowerCase().includes(q));
  }, [folders, search]);

  useEffect(() => {
    if (isOpen) {
      setSearch('');
      if (currentValue === '' || currentValue === 'root' || currentValue === '/') {
        setSelectedId('root');
      } else {
        const match = folders.find((f) => f.path.toLowerCase() === currentValue.toLowerCase() && f.id !== 'root');
        setSelectedId(match?.id || null);
      }
    }
  }, [isOpen, currentValue, folders]);

  if (!isOpen) return null;

  const handleSelect = () => {
    if (!selectedId) { onSelect(''); onClose(); return; }
    if (selectedId === 'root') { onSelect(''); onClose(); return; }
    const folder = folders.find((f) => f.id === selectedId);
    onSelect(folder?.path || '');
    onClose();
  };

  const selectedFolder = folders.find((f) => f.id === selectedId);

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-5 max-w-md w-full max-h-[85vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-lg font-medium text-white flex items-center gap-2">
              <FolderTree className="w-5 h-5 text-amber-400" />
              {title || 'Pilih Folder'}
            </h3>
            {description && <p className="text-xs text-[#888] mt-1">{description}</p>}
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
                const isSelected = selectedId === f.id;
                const isRoot = f.id === 'root';
                return (
                  <button
                    key={f.id}
                    onClick={() => setSelectedId(f.id)}
                    className={`w-full flex items-center gap-2 px-3 py-2 text-left text-sm transition-colors ${
                      isSelected
                        ? 'bg-amber-500/10 text-amber-300 border-l-2 border-amber-400'
                        : 'text-[#aaa] hover:bg-[#141414] hover:text-white border-l-2 border-transparent'
                    }`}
                    style={{ paddingLeft: `${(f.depth + 1) * 16 + 12}px` }}
                  >
                    {isRoot ? (
                      <Home className="w-4 h-4 flex-shrink-0 text-[#666]" />
                    ) : (
                      <Folder className={`w-4 h-4 flex-shrink-0 ${isSelected ? 'text-amber-400' : 'text-[#555]'}`} />
                    )}
                    <span className="truncate flex-1">{f.name}</span>
                    {isSelected && (
                      <CheckCircle className="w-4 h-4 text-amber-400 flex-shrink-0" />
                    )}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {selectedFolder && (
          <div className="mb-4 p-3 rounded-lg bg-[#1a1a1a] border border-[#2a2a2a]">
            <p className="text-xs text-[#888]">
              Folder prefix: <strong className="text-white">{selectedFolder.id === 'root' ? '(root - tanpa prefix)' : selectedFolder.path}</strong>
            </p>
            <p className="text-[10px] text-[#555] mt-1">
              Video akan disimpan di: <code className="text-[#8aa6d8]">root/{selectedFolder.id === 'root' ? '{chain path}' : `${selectedFolder.path}/{'{'}chain path{'}'}`}</code>
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
            onClick={handleSelect}
            className="px-5 py-2.5 text-sm rounded-lg font-medium bg-amber-500/20 text-amber-400 hover:bg-amber-500/30 border border-amber-500/30 transition-colors flex items-center gap-2"
          >
            <FolderTree className="w-4 h-4" />
            Pilih
          </button>
        </div>
      </div>
    </div>
  );
}

function ProviderSelector({ providers, selectedProviders, setSelectedProviders }) {
  const enabledProviders = useMemo(
    () => Object.entries(providers || {}).filter(([_, item]) => item.enabled).map(([key]) => key),
    [providers]
  );

  const toggleProvider = useCallback((provider) => {
    setSelectedProviders((prev) => {
      const next = prev.includes(provider)
        ? prev.filter((name) => name !== provider)
        : [...prev, provider];
      localStorage.setItem(PROVIDERS_STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  }, [setSelectedProviders]);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="flex items-center gap-2 text-sm text-[#aaa]">
          <Cloud className="w-4 h-4 text-[#666]" />
          Distribusi Storage
        </label>
        <div className="text-xs text-[#666]">
          {selectedProviders.length === 0 ? 'All enabled' : `${selectedProviders.length} selected`}
        </div>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        {Object.entries(providers || {}).map(([key, info]) => {
          const config = getProviderConfig(key);
          const isEnabled = info.enabled;
          const isSelected = selectedProviders.includes(key);
          return (
            <button
              key={key}
              type="button"
              disabled={!isEnabled}
              onClick={() => isEnabled && toggleProvider(key)}
              className={`px-3 py-2.5 rounded-lg text-sm border transition-all text-left ${
                !isEnabled
                  ? 'bg-[#111] border-[#1f1f1f] text-[#555] cursor-not-allowed opacity-50'
                  : isSelected
                    ? `${config?.bgColor || 'bg-[#2a2a2a]'} ${config?.color || 'text-white'} border-current ring-1 ring-current/20`
                    : 'bg-[#0d0d0d] border-[#2a2a2a] text-[#999] hover:bg-[#161616] hover:border-[#3a3a3a]'
              }`}
            >
              <span className="block text-xs font-medium truncate">{info?.name || config?.name || key}</span>
              {isSelected && <span className="block text-[10px] opacity-60 mt-0.5">Active</span>}
            </button>
          );
        })}
      </div>

      <p className="text-xs text-[#666]">
        {enabledProviders.length === 0
          ? 'Tidak ada provider aktif. Aktifkan provider di halaman Providers.'
          : selectedProviders.length === 0
            ? 'Semua provider aktif akan dipakai untuk distribusi.'
            : `Akan upload ke: ${selectedProviders.map((p) => providers?.[p]?.name || p).join(', ')}`}
      </p>
    </div>
  );
}

function BatchConfirmDialog({ isOpen, onClose, onConfirm, summary }) {
  if (!isOpen || !summary) return null;

  const totalVideos = summary.videoCount || 0;
  const totalContainers = summary.containerCount || 0;
  const providerNames = summary.providerNames || [];
  const folderPath = summary.folderPath || 'root';

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-6 max-w-md w-full" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start gap-4 mb-5">
          <div className="w-12 h-12 rounded-xl bg-emerald-500/10 flex items-center justify-center flex-shrink-0">
            <Download className="w-6 h-6 text-emerald-400" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-white mb-1">Start Batch Download</h3>
            <p className="text-sm text-[#888]">This will queue all videos for download and upload.</p>
          </div>
        </div>

        <div className="space-y-3 mb-5">
          <div className="bg-[#0d0d0d] rounded-lg p-3 space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-[#888]">Root CGroup</span>
              <span className="text-white font-mono">{summary.rootCgId}</span>
            </div>
            {summary.targetCgSelector && (
              <div className="flex justify-between text-sm">
                <span className="text-[#888]">Target</span>
                <span className="text-white font-mono">{summary.targetCgSelector}</span>
              </div>
            )}
            <div className="flex justify-between text-sm">
              <span className="text-[#888]">Estimated Videos</span>
              <span className="text-white font-semibold">{totalVideos}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-[#888]">Containers</span>
              <span className="text-white">{totalContainers}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-[#888]">Providers</span>
              <span className="text-white">{providerNames.length > 0 ? providerNames.join(', ') : 'All enabled'}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-[#888]">Folder</span>
              <span className="text-white font-mono">{folderPath}</span>
            </div>
          </div>

          <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
            <AlertCircle className="w-4 h-4 text-amber-400 mt-0.5 flex-shrink-0" />
            <p className="text-xs text-amber-300">
              This will queue <strong>{totalVideos} videos</strong> for download &amp; upload. The process runs in the background.
            </p>
          </div>
        </div>

        <div className="flex gap-3 justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2.5 text-sm text-[#888] hover:text-white rounded-lg hover:bg-[#222] transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="px-5 py-2.5 text-sm rounded-lg font-medium bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30 transition-colors flex items-center gap-2"
          >
            <Download className="w-4 h-4" />
            Start Batch
          </button>
        </div>
      </div>
    </div>
  );
}

function BatchProgressCard({ batchResult, batchQueueProgress, trackedBatchRun, batchSessionId, onCancelBatch }) {
  if (!batchResult && !trackedBatchRun) return null;

  const status = trackedBatchRun?.status || batchResult?.status || 'running';
  const queued = batchResult?.queuedCount || trackedBatchRun?.queuedCount || 0;
  const skipped = batchResult?.skippedCount || trackedBatchRun?.skippedCount || 0;
  const failed = trackedBatchRun?.downloadFailedCount || batchResult?.downloadFailedCount || 0;
  const completed = trackedBatchRun?.downloadCompletedCount || batchResult?.downloadCompletedCount || 0;
  const discovered = trackedBatchRun?.discoveredVideoCount || batchResult?.discoveredVideoCount || queued + skipped;
  const pending = Math.max(0, queued - completed - failed);
  const processed = batchQueueProgress?.containersProcessed || trackedBatchRun?.scannedContainerCount || trackedBatchRun?.processedContainers || 0;
  const total = batchQueueProgress?.containersTotal ?? trackedBatchRun?.totalContainers ?? null;
  const pct = total ? Math.round((processed / total) * 100) : 0;
  const isRunning = status === 'running';
  const runLabel = trackedBatchRun?.type === 'preview' ? 'Preview Build' : 'Batch Download';

  return (
    <div className={`card p-5 space-y-4 ${isRunning ? 'border-blue-500/30' : status === 'completed' ? 'border-emerald-500/30' : 'border-amber-500/30'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {isRunning ? (
            <div className="w-10 h-10 rounded-xl bg-blue-500/10 flex items-center justify-center">
              <Loader2 className="w-5 h-5 text-blue-400 animate-spin" />
            </div>
          ) : status === 'completed' ? (
            <div className="w-10 h-10 rounded-xl bg-emerald-500/10 flex items-center justify-center">
              <CheckCircle className="w-5 h-5 text-emerald-400" />
            </div>
          ) : (
            <div className="w-10 h-10 rounded-xl bg-amber-500/10 flex items-center justify-center">
              <AlertCircle className="w-5 h-5 text-amber-400" />
            </div>
          )}
          <div>
            <p className="text-sm font-semibold text-white">
              {isRunning ? `${runLabel} Running` : status === 'completed' ? `${runLabel} Completed` : `${runLabel} ${status}`}
            </p>
            <p className="text-xs text-[#888]">
              {isRunning ? 'Videos are being processed in the background' : `${runLabel} finished with status: ${status}`}
            </p>
          </div>
        </div>
        {isRunning && onCancelBatch && (
          <button
            onClick={onCancelBatch}
            className="px-3 py-1.5 text-xs rounded-lg border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>

      {total !== null && (
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-[#888]">Container Progress</span>
            <span className="text-white font-mono">{processed}/{total} ({pct}%)</span>
          </div>
          <div className="w-full bg-[#1a1a1a] rounded-full h-2.5 overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${pct >= 100 ? 'bg-gradient-to-r from-emerald-500 to-emerald-400' : 'bg-gradient-to-r from-blue-500 to-sky-400'}`}
              style={{ width: `${pct}%` }}
            />
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-[#777]">
            <span>Videos found: <span className="text-white">{discovered}</span></span>
            <span>Queued: <span className="text-blue-400">{queued}</span></span>
            <span>Pending: <span className="text-amber-400">{pending}</span></span>
            <span>Completed: <span className="text-emerald-400">{completed}</span></span>
            <span>Failed: <span className="text-red-400">{failed}</span></span>
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="bg-[#0d0d0d] rounded-lg p-3 text-center">
          <p className="text-lg font-bold text-blue-400">{queued}</p>
          <p className="text-[10px] text-[#666] uppercase tracking-wider">Queued</p>
        </div>
        <div className="bg-[#0d0d0d] rounded-lg p-3 text-center">
          <p className="text-lg font-bold text-amber-400">{skipped}</p>
          <p className="text-[10px] text-[#666] uppercase tracking-wider">Skipped</p>
        </div>
        <div className="bg-[#0d0d0d] rounded-lg p-3 text-center">
          <p className="text-lg font-bold text-red-400">{pending}</p>
          <p className="text-[10px] text-[#666] uppercase tracking-wider">Pending</p>
        </div>
        <div className="bg-[#0d0d0d] rounded-lg p-3 text-center">
          <p className="text-lg font-bold text-emerald-400">{completed}</p>
          <p className="text-[10px] text-[#666] uppercase tracking-wider">Completed</p>
        </div>
      </div>

      {batchSessionId && (
        <div className="flex items-center gap-2 text-xs text-[#666]">
          <span>Session:</span>
          <code className="text-[#888] bg-[#0d0d0d] px-2 py-0.5 rounded">{batchSessionId.slice(0, 16)}...</code>
          <button
            onClick={() => { navigator.clipboard.writeText(batchSessionId); toast.success('Copied', 'Session ID copied to clipboard'); }}
            className="p-1 hover:bg-[#222] rounded transition-colors"
          >
            <Copy className="w-3 h-3" />
          </button>
        </div>
      )}
    </div>
  );
}

export default function ZeniusPage() {
  const [urlShortId, setUrlShortId] = useState('');
  const [headersRaw, setHeadersRaw] = useState('');
  const [refererPath, setRefererPath] = useState('');
  const [folderId, setFolderId] = useState('');
  const [filename, setFilename] = useState('');
  const [selectedProviders, setSelectedProviders] = useState(loadSavedProviders);
  const [details, setDetails] = useState(null);
  const [batchRootCgId, setBatchRootCgId] = useState('34');
  const [batchTargetCgSelector, setBatchTargetCgSelector] = useState('');
  const [batchParentContainerName, setBatchParentContainerName] = useState('');
  const [batchFolderPrefix, setBatchFolderPrefix] = useState('');
  const [batchPreviewMode, setBatchPreviewMode] = useState('fast');
  const [batchChain, setBatchChain] = useState(null);
  const [batchResult, setBatchResult] = useState(null);
  const savedPreviewSession = useMemo(() => loadSavedBatchPreviewSession(), []);
  const [previewRunId, setPreviewRunId] = useState(savedPreviewSession?.previewRunId || null);
  const [downloadRunId, setDownloadRunId] = useState(null);
  const [batchSessionId, setBatchSessionId] = useState(savedPreviewSession?.batchSessionId || null);
  const [previewContextSignature, setPreviewContextSignature] = useState(savedPreviewSession?.contextSignature || null);
  const [batchBuildProgress, setBatchBuildProgress] = useState(null);
  const [batchQueueProgress, setBatchQueueProgress] = useState(null);
  const [previewPollErrorCount, setPreviewPollErrorCount] = useState(0);
  const [showHeadersHelp, setShowHeadersHelp] = useState(false);
  const [showBatchConfirm, setShowBatchConfirm] = useState(false);
  const [showFolderPrefixPicker, setShowFolderPrefixPicker] = useState(false);
  const [activeTab, setActiveTab] = useState('single');

  const detailsMutation = useZeniusInstanceDetails();
  const downloadMutation = useZeniusDownload();
  const batchChainMutation = useZeniusBatchChain();
  const batchDownloadMutation = useZeniusBatchDownload();
  const cancelBatchRunMutation = useZeniusCancelBatchRun();
  const deleteFolderMutation = useDeleteFolder();
  const cancelAllMutation = useZeniusCancelAll();
  const resetFilesMutation = useZeniusResetFiles();
  const setMaxConcurrentMutation = useSetMaxConcurrent();
  const { data: webhookConfig } = useWebhookConfig();
  const updateWebhookMutation = useUpdateWebhookConfig();
  const testWebhookMutation = useTestWebhook();
  const isWsConnected = useWebSocketStore((state) => state.isConnected);
  const { data: queueStatus } = useZeniusQueueStatus({
    enabled: true,
    refetchInterval: isWsConnected ? 20000 : 5000,
    refetchIntervalInBackground: !isWsConnected
  });
  const { data: providers } = useProviders();
  const { data: folderTree } = useFolders();

  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [showDeleteAllFoldersConfirm, setShowDeleteAllFoldersConfirm] = useState(false);
  const [editingMaxConcurrent, setEditingMaxConcurrent] = useState(false);
  const [maxConcurrentInput, setMaxConcurrentInput] = useState('');

  const deleteAllFolders = useDeleteAllFolders();
  const batchRootCgRef = useRef(null);
  const batchChainPollRef = useRef(null);
  const previewPollErrorCountRef = useRef(0);

  useEffect(() => {
    const savedHeaders = localStorage.getItem(HEADERS_STORAGE_KEY);
    if (savedHeaders) setHeadersRaw(savedHeaders);
  }, []);

  useEffect(() => {
    localStorage.setItem(HEADERS_STORAGE_KEY, headersRaw);
  }, [headersRaw]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (!previewRunId && !batchSessionId) {
      localStorage.removeItem(BATCH_PREVIEW_SESSION_STORAGE_KEY);
      return;
    }
    localStorage.setItem(BATCH_PREVIEW_SESSION_STORAGE_KEY, JSON.stringify({
      previewRunId,
      batchSessionId,
      contextSignature: previewContextSignature || null
    }));
  }, [previewRunId, batchSessionId, previewContextSignature]);

  useEffect(() => {
    previewPollErrorCountRef.current = previewPollErrorCount;
  }, [previewPollErrorCount]);

  const isBusy = detailsMutation.isLoading || downloadMutation.isLoading;
  const isBatchBusy = batchChainMutation.isLoading || batchDownloadMutation.isLoading;
  const instanceValue = details?.value || null;
  const normalizedFolderPath = useMemo(() => normalizeFolderInput(folderId), [folderId]);
  const normalizedBatchFolderPrefix = useMemo(() => normalizeFolderInput(batchFolderPrefix), [batchFolderPrefix]);
  const currentPreviewContextSignature = useMemo(() => buildPreviewContextSignature({
    rootCgId: batchRootCgId,
    targetCgSelector: batchTargetCgSelector,
    parentContainerName: batchParentContainerName,
    folderPrefix: normalizedBatchFolderPrefix,
    providers: selectedProviders,
    previewMode: batchPreviewMode
  }), [batchRootCgId, batchTargetCgSelector, batchParentContainerName, normalizedBatchFolderPrefix, selectedProviders, batchPreviewMode]);

  const trackedPreviewRun = useMemo(() => {
    const runs = Array.isArray(queueStatus?.backgroundBatches) ? queueStatus.backgroundBatches : [];
    if (!previewRunId) return null;
    return runs.find((item) => item.id === previewRunId && item.type === 'preview') || null;
  }, [queueStatus, previewRunId]);

  const trackedDownloadRun = useMemo(() => {
    const runs = Array.isArray(queueStatus?.backgroundBatches) ? queueStatus.backgroundBatches : [];
    if (!downloadRunId) return null;
    return runs.find((item) => item.id === downloadRunId && item.type !== 'preview') || null;
  }, [queueStatus, downloadRunId]);

  useEffect(() => {
    if (!trackedPreviewRun) return;
    setBatchSessionId((prev) => trackedPreviewRun.sessionId || prev || null);
    setPreviewRunId((prev) => trackedPreviewRun.id || prev || null);
    setPreviewPollErrorCount(0);

    setBatchBuildProgress(trackedPreviewRun.status === 'running'
      ? {
          processed: Number(trackedPreviewRun.scannedContainerCount || trackedPreviewRun.processedContainers || 0),
          total: Number.isFinite(Number(trackedPreviewRun.totalContainers)) ? Number(trackedPreviewRun.totalContainers) : null,
          discoveredVideos: Number(trackedPreviewRun.discoveredVideoCount || 0),
          previewContainers: Number(trackedPreviewRun.previewSummary?.previewContainerCount || 0),
          discoveredContainers: Number(trackedPreviewRun.previewSummary?.discoveredContainerCount || trackedPreviewRun.totalContainers || 0),
          hasMore: Boolean(trackedPreviewRun.hasMoreContainers)
        }
      : null);

    if (trackedPreviewRun.chainPreview) {
      setBatchChain(trackedPreviewRun.chainPreview);
    }

    if (trackedPreviewRun.status === 'completed' && trackedPreviewRun.chainPreview) {
      setBatchChain(trackedPreviewRun.chainPreview);
    }

    if (trackedPreviewRun.status === 'cancelled') {
      setBatchBuildProgress(null);
      if (batchChainPollRef.current) {
        window.clearInterval(batchChainPollRef.current);
        batchChainPollRef.current = null;
      }
    }
  }, [trackedPreviewRun]);

  useEffect(() => {
    if (trackedPreviewRun || !previewRunId || !Array.isArray(queueStatus?.backgroundBatches)) return;
    if (previewContextSignature && currentPreviewContextSignature && previewContextSignature !== currentPreviewContextSignature) {
      return;
    }
    const fallbackPreviewRun = queueStatus.backgroundBatches.find((item) => item.id === previewRunId) || null;
    if (!fallbackPreviewRun) return;
    setPreviewRunId(fallbackPreviewRun.id || previewRunId);
    setBatchSessionId((prev) => fallbackPreviewRun.sessionId || prev || null);
  }, [queueStatus, trackedPreviewRun, previewRunId, batchSessionId, previewContextSignature, currentPreviewContextSignature]);

  useEffect(() => {
    if (!trackedDownloadRun) return;
    setBatchQueueProgress({
      containersProcessed: Number(trackedDownloadRun.scannedContainerCount || trackedDownloadRun.processedContainers || 0),
      containersTotal: Number.isFinite(Number(trackedDownloadRun.totalContainers)) ? Number(trackedDownloadRun.totalContainers) : null
    });
    setBatchResult((prev) => ({
      ...(prev || {}),
      ...trackedDownloadRun,
      batchRunId: trackedDownloadRun.id,
      queuedCount: Number(trackedDownloadRun.queuedCount || 0),
      skippedCount: Number(trackedDownloadRun.skippedCount || 0),
      downloadCompletedCount: Number(trackedDownloadRun.downloadCompletedCount || 0),
      downloadFailedCount: Number(trackedDownloadRun.downloadFailedCount || 0),
      discoveredVideoCount: Number(trackedDownloadRun.discoveredVideoCount || 0)
    }));
  }, [trackedDownloadRun]);

  useEffect(() => {
    const hasActiveWork = Boolean(
      queueStatus?.active || queueStatus?.queued || queueStatus?.activeBackgroundBatchCount || trackedPreviewRun?.status === 'running' || trackedDownloadRun?.status === 'running'
    );
    if (!hasActiveWork || typeof window === 'undefined') return undefined;
    const keepalive = () => {
      fetch(`${window.location.origin}/zenius?keepalive=${Date.now()}`, { method: 'GET', cache: 'no-store', credentials: 'same-origin' }).catch(() => {});
    };
    const timer = window.setInterval(keepalive, 20000);
    return () => window.clearInterval(timer);
  }, [queueStatus, trackedPreviewRun, trackedDownloadRun]);

  const folderOptions = useMemo(() => flattenFolderPaths(folderTree), [folderTree]);
  const folderPathLookup = useMemo(() => new Set(folderOptions.map((item) => item.path.toLowerCase())), [folderOptions]);
  const folderMapByPath = useMemo(() => {
    const map = new Map();
    for (const item of folderOptions) map.set(item.path.toLowerCase(), item);
    return map;
  }, [folderOptions]);

  const exactFolderMatch = normalizedFolderPath ? folderMapByPath.get(normalizedFolderPath.toLowerCase()) || null : null;

  const folderSuggestions = useMemo(() => {
    const keyword = normalizedFolderPath.toLowerCase();
    const list = keyword ? folderOptions.filter((item) => item.path.toLowerCase().includes(keyword)) : folderOptions;
    return list.slice(0, 10);
  }, [folderOptions, normalizedFolderPath]);

  const folderExists = normalizedFolderPath ? folderPathLookup.has(normalizedFolderPath.toLowerCase()) : true;
  const resolvedFolderPathPreview = normalizedFolderPath ? `root/${normalizedFolderPath}` : 'root';

  const handleFolderInputChange = (value) => {
    setFolderId(value);
    if (deleteFolderMutation.isError || deleteFolderMutation.isSuccess) deleteFolderMutation.reset();
  };

  const handleDeleteFolder = async () => {
    if (!exactFolderMatch) return;
    const confirmed = window.confirm(`Hapus folder "root/${exactFolderMatch.path}"? Folder harus kosong.`);
    if (!confirmed) return;
    try {
      await deleteFolderMutation.mutateAsync(exactFolderMatch.id);
      setFolderId('');
    } catch (error) {
      console.error('Failed to delete folder:', error);
    }
  };

  const handleGetDetails = async (event) => {
    event.preventDefault();
    try {
      const result = await detailsMutation.mutateAsync({ urlShortId, headersRaw, refererPath });
      setDetails(result);
      if (!filename && result?.value?.name) setFilename(result.value.name);
      if (!refererPath && result?.value?.['path-url']) setRefererPath(result.value['path-url']);
      toast.success('Details Retrieved', `Found: ${result?.value?.name || urlShortId}`);
    } catch (error) {
      setDetails(null);
      console.error('Failed to fetch instance details:', error);
      toast.error('Failed', error?.response?.data?.error || error.message || 'Could not get details');
    }
  };

  const handleDownload = async (event) => {
    event.preventDefault();
    try {
      await downloadMutation.mutateAsync({
        urlShortId,
        headersRaw,
        refererPath,
        folderId: normalizedFolderPath || 'root',
        filename,
        providers: selectedProviders.length > 0 ? selectedProviders : null
      });
      toast.success('Download Queued', `Video ${urlShortId} added to processing queue`);
    } catch (error) {
      console.error('Failed to queue zenius download:', error);
      toast.error('Download Failed', error?.response?.data?.error || error.message);
    }
  };

  const handleGetBatchChain = async () => {
    try {
      const nextPreviewContextSignature = buildPreviewContextSignature({
        rootCgId: batchRootCgId,
        targetCgSelector: batchTargetCgSelector,
        parentContainerName: batchParentContainerName,
        folderPrefix: normalizedBatchFolderPrefix,
        providers: selectedProviders,
        previewMode: batchPreviewMode
      });
      if (batchChainPollRef.current) {
        window.clearInterval(batchChainPollRef.current);
        batchChainPollRef.current = null;
      }

      setBatchChain(null);
      setBatchResult(null);
      setDownloadRunId(null);
      setPreviewRunId(null);
      setBatchSessionId(null);
      setPreviewContextSignature(nextPreviewContextSignature);
      setBatchQueueProgress(null);
      setBatchBuildProgress({ processed: 0, total: null });
      setPreviewPollErrorCount(0);
      const started = await batchChainMutation.mutateAsync({
        rootCgId: batchRootCgId,
        targetCgSelector: batchTargetCgSelector,
        parentContainerName: batchParentContainerName || null,
        headersRaw,
        refererPath,
        timeBudgetMs: BATCH_REQUEST_BUDGET_MS,
        providers: selectedProviders.length > 0 ? selectedProviders : null,
        folderId: normalizedBatchFolderPrefix || null,
        fastPreview: batchPreviewMode !== 'full',
        ultraFastPreview: batchPreviewMode === 'ultra'
      });

      const startedPreviewRunId = started?.previewRunId || started?.sessionId || started?.status?.id || null;
      const startedPlanSessionId = started?.planSessionId || started?.status?.sessionId || null;
      setPreviewRunId(startedPreviewRunId);
      setBatchSessionId(startedPlanSessionId || null);

      const poll = async () => {
        if (!startedPreviewRunId) return;
        const status = await getZeniusBatchChainStatus(startedPreviewRunId);
        setPreviewPollErrorCount(0);
        setPreviewRunId(status.id || startedPreviewRunId);
        setBatchSessionId(status.chainPreview?.sessionId || status.sessionId || startedPlanSessionId || null);
        setBatchBuildProgress({
          processed: Number(status.containerProgress?.processed || status.processedContainers || 0),
          total: Number.isFinite(Number(status.containerProgress?.total)) ? Number(status.containerProgress.total) : null,
          discoveredVideos: Number(status.discoveredVideoCount || 0),
          previewContainers: Number(status.previewSummary?.previewContainerCount || 0),
          discoveredContainers: Number(status.previewSummary?.discoveredContainerCount || status.totalContainers || 0),
          hasMore: Boolean(status.hasMoreContainers)
        });

        if (status.chainPreview) {
          setBatchChain(status.chainPreview);
        }

        if (status.status === 'completed') {
          setBatchChain(status.chainPreview || null);
          setBatchBuildProgress(null);
          if (batchChainPollRef.current) {
            window.clearInterval(batchChainPollRef.current);
            batchChainPollRef.current = null;
          }
          const finalChain = status.chainPreview || null;
          const videoCount = (finalChain?.containerDetails || []).reduce((acc, c) => acc + (c.videoInstances?.length || 0), 0);
          const previewContainers = Number(finalChain?.containerDetails?.length || 0);
          const discoveredContainers = Number(finalChain?.containerList?.totalContainers || 0);
          toast.success('Chain Built', `Found ${videoCount} videos across ${previewContainers} preview containers (discovered: ${discoveredContainers})`);
          return;
        }

        if (status.status === 'cancelled') {
          setBatchBuildProgress(null);
          if (batchChainPollRef.current) {
            window.clearInterval(batchChainPollRef.current);
            batchChainPollRef.current = null;
          }
          return;
        }

        if (status.status === 'failed') {
          setBatchBuildProgress(null);
          if (batchChainPollRef.current) {
            window.clearInterval(batchChainPollRef.current);
            batchChainPollRef.current = null;
          }
          throw new Error(status.error || 'Batch preview build failed');
        }
      };

      await poll();
    } catch (error) {
      setBatchChain(null);
      setBatchBuildProgress(null);
      console.error('Failed to fetch zenius batch chain:', error);
      toast.error('Chain Failed', error?.response?.data?.error || error.message);
    }
  };

  useEffect(() => () => {
    if (batchChainPollRef.current) {
      window.clearInterval(batchChainPollRef.current);
      batchChainPollRef.current = null;
    }
  }, []);

  useEffect(() => {
    const hasFinalPreviewChain = Boolean(batchChain?.planReady);
    const previewEnded = trackedPreviewRun?.status === 'completed' || trackedPreviewRun?.status === 'failed' || trackedPreviewRun?.status === 'cancelled';
    if (!previewRunId || (previewEnded && hasFinalPreviewChain)) {
      if (batchChainPollRef.current) {
        window.clearInterval(batchChainPollRef.current);
        batchChainPollRef.current = null;
      }
      return undefined;
    }

    if (batchChainPollRef.current) {
      return undefined;
    }

    const poll = async () => {
      const status = await getZeniusBatchChainStatus(previewRunId);
      setPreviewPollErrorCount(0);
      setPreviewRunId(status.id || previewRunId);
      setBatchSessionId((prev) => status.chainPreview?.sessionId || status.sessionId || prev || null);
      if (status.chainPreview) {
        setBatchChain(status.chainPreview);
      }
      if (status.status === 'completed' && status.chainPreview?.planReady) {
        setBatchBuildProgress(null);
        if (batchChainPollRef.current) {
          window.clearInterval(batchChainPollRef.current);
          batchChainPollRef.current = null;
        }
        return;
      }
      if (status.status === 'cancelled') {
        setBatchBuildProgress(null);
        if (batchChainPollRef.current) {
          window.clearInterval(batchChainPollRef.current);
          batchChainPollRef.current = null;
        }
        setPreviewRunId(null);
        return;
      }
      setBatchBuildProgress(status.status === 'running'
        ? {
            processed: Number(status.containerProgress?.processed || status.processedContainers || 0),
            total: Number.isFinite(Number(status.containerProgress?.total)) ? Number(status.containerProgress.total) : null,
            discoveredVideos: Number(status.discoveredVideoCount || 0),
            previewContainers: Number(status.previewSummary?.previewContainerCount || 0),
            discoveredContainers: Number(status.previewSummary?.discoveredContainerCount || status.totalContainers || 0),
            hasMore: Boolean(status.hasMoreContainers)
          }
        : null);
    };

    batchChainPollRef.current = window.setInterval(() => {
      poll().catch((error) => {
        setPreviewPollErrorCount((count) => count + 1);
        if (previewPollErrorCountRef.current >= 2 && trackedPreviewRun?.status !== 'running' && batchChain?.planReady && batchChainPollRef.current) {
          window.clearInterval(batchChainPollRef.current);
          batchChainPollRef.current = null;
        }
        console.error('Failed to recover zenius batch chain status:', error);
      });
    }, BATCH_PREVIEW_POLL_INTERVAL_MS);

    return () => {
      if (batchChainPollRef.current) {
        window.clearInterval(batchChainPollRef.current);
        batchChainPollRef.current = null;
      }
    };
  }, [previewRunId, trackedPreviewRun, batchChain?.planReady]);

  const handleStartBatchDownload = () => {
    if (!isBatchChainReady) {
      toast.info('Preview belum selesai', 'Tunggu hingga chain preview selesai diproses sebelum start batch.');
      return;
    }

    const videoCount = (batchChain?.containerDetails || []).reduce((acc, c) => acc + (c.videoInstances?.length || 0), 0);
    const providerNames = selectedProviders.length > 0
      ? selectedProviders.map((p) => providers?.[p]?.name || p)
      : Object.entries(providers || {}).filter(([_, v]) => v.enabled).map(([k]) => providers?.[k]?.name || k);

    setShowBatchConfirm({
      rootCgId: batchRootCgId,
      targetCgSelector: batchTargetCgSelector || null,
      videoCount,
      containerCount: batchChain?.containerDetails?.length || 0,
      providerNames,
      folderPath: normalizedBatchFolderPrefix || 'root'
    });
  };

  const handleConfirmBatchDownload = async () => {
    setShowBatchConfirm(null);
    try {
      setBatchResult(null);
      setDownloadRunId(null);
      setBatchQueueProgress({ processed: 0, total: null });
      const result = await batchDownloadMutation.mutateAsync({
        rootCgId: batchRootCgId,
        targetCgSelector: batchTargetCgSelector,
        parentContainerName: batchParentContainerName || null,
        headersRaw,
        refererPath,
        folderId: normalizedBatchFolderPrefix || null,
        providers: selectedProviders.length > 0 ? selectedProviders : null,
        previewRunId,
        sessionId: batchChain?.sessionId || batchSessionId || null,
        containerLimit: BATCH_DOWNLOAD_CHUNK_SIZE,
        timeBudgetMs: BATCH_REQUEST_BUDGET_MS
      });

      const data = result?.data || {};
      const status = data.status || {};
      setDownloadRunId(data.batchRunId || null);
      setBatchSessionId(status.sessionId || batchChain?.sessionId || batchSessionId || null);
      setBatchResult({
        ...status,
        batchRunId: data.batchRunId || null,
        message: result?.message || 'Zenius batch download started in background',
        queuedCount: Number(status.queuedCount || 0),
        skippedCount: Number(status.skippedCount || 0)
      });
      toast.success('Batch Started', `${Number(status.queuedCount || 0)} videos queued, ${Number(status.skippedCount || 0)} skipped`);
    } catch (error) {
      setBatchResult(null);
      setDownloadRunId(null);
      setBatchQueueProgress(null);
      console.error('Failed to queue zenius batch download:', error);
      toast.error('Batch Failed', error?.response?.data?.error || error.message);
    }
  };

  const handleCancelTrackedRun = async (runId, type = 'batch') => {
    if (!runId) return;

    try {
      await cancelBatchRunMutation.mutateAsync(runId);
      if (type === 'preview') {
        if (batchChainPollRef.current) {
          window.clearInterval(batchChainPollRef.current);
          batchChainPollRef.current = null;
        }
        setBatchBuildProgress(null);
        setPreviewRunId(null);
      }
      toast.success('Cancelled', `${type === 'preview' ? 'Preview build' : 'Batch run'} cancelled`);
    } catch (error) {
      console.error('Failed to cancel background batch run:', error);
      toast.error('Cancel Failed', error?.response?.data?.error || error.message);
    }
  };

  useEffect(() => {
    const onKeyDown = (e) => {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable) return;
      if (e.ctrlKey && e.key === 'b') {
        e.preventDefault();
        batchRootCgRef.current?.focus();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  const batchVideoCount = useMemo(() => {
    const plannedCount = Number(batchChain?.plannedItems?.length || 0);
    if (plannedCount > 0) return plannedCount;
    return (batchChain?.containerDetails || []).reduce((acc, c) => acc + (c.videoInstances?.length || 0), 0);
  }, [batchChain]);
  const batchPreviewContainerCount = Number(batchChain?.containerDetails?.length || 0);
  const batchDiscoveredContainerCount = Number(batchChain?.containerList?.totalContainers || 0);
  const isBatchChainReady = Boolean(batchChain?.planReady || (
    batchChain?.discoveryDone
    && Number(batchChain?.containerList?.totalContainers || 0) > 0
    && Number(batchChain?.containerDetails?.length || 0) >= Number(batchChain?.containerList?.totalContainers || 0)
  ));
  const isPreviewRunning = Boolean(
    previewRunId && (
      batchChainMutation.isLoading
      || batchBuildProgress
      || trackedPreviewRun?.status === 'running'
    )
  );
  const previewCancelRunId = trackedPreviewRun?.id || previewRunId || batchSessionId || null;
  const previewItemsSummary = trackedPreviewRun?.previewItemsSummary || null;
  const previewStatusTone = trackedPreviewRun?.status === 'failed'
    ? 'text-red-300'
    : previewPollErrorCount > 0
      ? 'text-amber-300'
      : 'text-sky-300';
  const allBatchPreviewRows = useMemo(() => {
    const trackedPreviewItems = Array.isArray(trackedPreviewRun?.previewItems) ? trackedPreviewRun.previewItems : [];
    const plannedItems = trackedPreviewItems.length > 0
      ? trackedPreviewItems
      : (Array.isArray(batchChain?.plannedItems) ? batchChain.plannedItems : []);
    if (plannedItems.length > 0) {
      return plannedItems.map((item, index) => ({
        key: item.planKey || `${item.urlShortId || 'unknown'}-${index}`,
        index,
        urlShortId: item.urlShortId || '-',
        outputName: item.outputName || item.instance?.outputName || '-',
        folderInput: item.folderInput || 'root',
        chainPath: item.path || '',
        containerName: item.containerName || '-',
        reason: item.reason || '-',
        action: item.action || 'pending',
        providers: Array.isArray(item.pendingProviders) ? item.pendingProviders : [],
        selectedProviders: Array.isArray(item.selectedProviders) ? item.selectedProviders : [],
        instanceName: item.instance?.name || item.instanceName || '-'
      }));
    }

    return (batchChain?.containerDetails || []).flatMap((container, ci) =>
      (container.videoInstances || []).map((item, vi) => {
        const index = (ci * 1000) + vi;
        const outputName = item.outputName ? `${item.outputName}.mp4` : ((item.metadata?.name || item.name) ? `${item.metadata?.name || item.name}.mp4` : '-');
        const chainPath = item.path || container.path || '';
        const folderInput = normalizedBatchFolderPrefix
          ? `root/${normalizedBatchFolderPrefix}${chainPath ? `/${chainPath}` : ''}`
          : `root${chainPath ? `/${chainPath}` : ''}`;

        return {
          key: `${container.containerUrlShortId || 'container'}-${item.urlShortId || index}`,
          index,
          urlShortId: item.urlShortId || '-',
          outputName,
          folderInput,
          chainPath,
          containerName: container.containerName || '-',
          reason: 'Menunggu planning selesai',
          action: 'mapped',
          providers: [],
          selectedProviders: [],
          instanceName: item.metadata?.name || item.name || '-'
        };
      })
    );
  }, [batchChain, normalizedBatchFolderPrefix, trackedPreviewRun]);
  const batchPreviewRows = useMemo(() => allBatchPreviewRows.slice(0, BATCH_PREVIEW_ROW_RENDER_LIMIT), [allBatchPreviewRows]);
  const hiddenBatchPreviewRowCount = Math.max(0, allBatchPreviewRows.length - batchPreviewRows.length);
  const batchActionSummary = useMemo(() => {
    return allBatchPreviewRows.reduce((summary, item) => {
      const key = item.action || 'pending';
      summary[key] = (summary[key] || 0) + 1;
      return summary;
    }, {});
  }, [allBatchPreviewRows]);

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-white mb-1">Zenius Downloader</h2>
        <p className="text-sm text-[#888]">
          Download video dari Zenius, distribusikan ke storage, hapus file lokal otomatis.
        </p>
      </div>

      {/* Queue Status & Controls */}
      <div className="card p-4 space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-[#666]" />
            <span className="text-sm font-medium text-[#aaa]">Pipeline Queue</span>
          </div>
          <div className="flex items-center gap-3">
            {queueStatus && (
              <div className="flex items-center gap-3 text-sm">
                <span className="text-[#666]">Aktif: <span className="text-white">{queueStatus.active}/{queueStatus.max}</span></span>
                <span className="text-[#666]">Menunggu: <span className="text-white">{queueStatus.queued}</span></span>
                <span className="text-[#666]">Batch: <span className="text-white">{queueStatus.activeBackgroundBatchCount || 0}</span></span>
                {queueStatus.isProcessing && <span className="text-emerald-400 text-xs">Pipeline berjalan...</span>}
              </div>
            )}
          </div>
        </div>

        {queueStatus && (
          <div className="w-full bg-[#1a1a1a] rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all ${queueStatus.active >= queueStatus.max ? 'bg-gradient-to-r from-amber-500 to-amber-400' : 'bg-gradient-to-r from-blue-500 to-blue-400'}`}
              style={{ width: `${Math.min((queueStatus.active / queueStatus.max) * 100, 100)}%` }}
            />
          </div>
        )}

        {/* Pipeline Concurrency Control */}
        <div className="flex items-center gap-3">
          <span className="text-xs text-[#666]">Pipeline Concurrent:</span>
          {editingMaxConcurrent ? (
            <div className="flex items-center gap-1.5">
              <input
                type="number"
                min={1}
                max={50}
                value={maxConcurrentInput}
                onChange={(e) => setMaxConcurrentInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    const v = parseInt(maxConcurrentInput, 10);
                    if (v >= 1 && v <= 50) {
                      setMaxConcurrentMutation.mutate(v);
                    }
                    setEditingMaxConcurrent(false);
                  }
                  if (e.key === 'Escape') {
                    setEditingMaxConcurrent(false);
                  }
                }}
                className="w-14 px-2 py-1 bg-[#0d0d0d] border border-[#3a3a3a] rounded text-xs text-white text-center focus:outline-none focus:border-blue-500"
                autoFocus
              />
              <button
                onClick={() => {
                  const v = parseInt(maxConcurrentInput, 10);
                  if (v >= 1 && v <= 50) {
                    setMaxConcurrentMutation.mutate(v);
                  }
                  setEditingMaxConcurrent(false);
                }}
                disabled={setMaxConcurrentMutation.isLoading}
                className="px-2 py-1 rounded text-xs bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 disabled:opacity-40"
              >
                {setMaxConcurrentMutation.isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Set'}
              </button>
              <button
                onClick={() => setEditingMaxConcurrent(false)}
                className="px-2 py-1 rounded text-xs text-[#666] hover:text-white"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => {
                setMaxConcurrentInput(String(queueStatus?.max || 10));
                setEditingMaxConcurrent(true);
              }}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-[#0d0d0d] border border-[#2a2a2a] text-xs text-white hover:border-[#3a3a3a] transition-colors"
            >
              <Activity className="w-3 h-3 text-blue-400" />
              {queueStatus?.max || 10}
              <span className="text-[#555]">pipes</span>
            </button>
          )}
        </div>

        {/* Active Tasks */}
        {queueStatus?.activeTasks?.length > 0 && (
          <div className="space-y-1.5">
            <p className="text-[10px] text-[#555] uppercase tracking-wider">Active Pipelines</p>
            <div className="space-y-1">
              {queueStatus.activeTasks.map((task) => (
                <div key={task.id} className="flex items-center gap-2 px-2.5 py-1.5 rounded bg-[#0d0d0d] border border-[#1f1f1f]">
                  <div className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
                  <span className="text-xs text-[#aaa] font-mono">{task.urlShortId}</span>
                  <span className="text-[10px] text-[#555] truncate">{task.outputName}</span>
                  <span className="text-[10px] text-[#444] ml-auto">
                    {Math.round((Date.now() - new Date(task.startTime).getTime()) / 1000)}s
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Queued Tasks */}
        {queueStatus?.queuedTasks?.length > 0 && (
          <div className="space-y-1.5">
            <p className="text-[10px] text-[#555] uppercase tracking-wider">Queued Pipelines ({queueStatus.queuedTasks.length})</p>
            <div className="max-h-24 overflow-y-auto space-y-1">
              {queueStatus.queuedTasks.slice(0, 20).map((task, i) => (
                <div key={i} className="flex items-center gap-2 px-2.5 py-1 rounded bg-[#0d0d0d]">
                  <div className="w-1.5 h-1.5 rounded-full bg-zinc-600" />
                  <span className="text-[10px] text-[#666] font-mono">{task.urlShortId}</span>
                  <span className="text-[10px] text-[#444] truncate">{task.outputName}</span>
                </div>
              ))}
              {queueStatus.queuedTasks.length > 20 && (
                <p className="text-[10px] text-[#444] text-center">+{queueStatus.queuedTasks.length - 20} more</p>
              )}
            </div>
          </div>
        )}

        <div className="flex flex-wrap gap-2">
          <button type="button" onClick={() => setShowCancelConfirm(true)} disabled={cancelAllMutation.isLoading || (queueStatus?.active === 0 && queueStatus?.queued === 0)} className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-amber-500/40 text-amber-300 hover:bg-amber-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-xs">
            {cancelAllMutation.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
            Cancel All
          </button>
          <button type="button" onClick={() => setShowResetConfirm(true)} disabled={resetFilesMutation.isLoading} className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-red-500/40 text-red-300 hover:bg-red-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-xs">
            {resetFilesMutation.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RotateCcw className="w-3.5 h-3.5" />}
            Reset Files
          </button>
          <button type="button" onClick={() => setShowDeleteAllFoldersConfirm(true)} disabled={deleteAllFolders.isLoading} className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-rose-500/40 text-rose-300 hover:bg-rose-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-xs">
            {deleteAllFolders.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash className="w-3.5 h-3.5" />}
            Remove Folders
          </button>
        </div>

        {cancelAllMutation.isSuccess && <p className="text-xs text-emerald-400">Cancelled {cancelAllMutation.data?.data?.cancelledDownloads} downloads, {cancelAllMutation.data?.data?.cancelledUploads} uploads.</p>}
        {cancelAllMutation.isError && <p className="text-xs text-red-400">Failed: {cancelAllMutation.error?.message}</p>}
        {resetFilesMutation.isSuccess && <p className="text-xs text-emerald-400">Reset {resetFilesMutation.data?.data?.deletedCount} files.</p>}
        {resetFilesMutation.isError && <p className="text-xs text-red-400">Failed: {resetFilesMutation.error?.message}</p>}
        {deleteAllFolders.isSuccess && <p className="text-xs text-emerald-400">Deleted {deleteAllFolders.data?.data?.removedFolders} folders, {deleteAllFolders.data?.data?.removedFiles} files.</p>}
        {deleteAllFolders.isError && <p className="text-xs text-red-400">Failed: {deleteAllFolders.error?.message}</p>}
      </div>

      {/* Upload Concurrency Settings */}
      <UploadConcurrencyCard />

      {/* Batch Progress (shown when batch is running/completed) */}
      <BatchProgressCard
        batchResult={batchResult}
        batchQueueProgress={batchQueueProgress}
        trackedBatchRun={trackedDownloadRun}
        batchSessionId={batchSessionId}
        onCancelBatch={() => handleCancelTrackedRun(trackedDownloadRun?.id, 'batch')}
      />

      {trackedPreviewRun && (
        <BatchProgressCard
          batchResult={trackedPreviewRun}
          batchQueueProgress={{
            containersProcessed: Number(trackedPreviewRun.scannedContainerCount || trackedPreviewRun.processedContainers || 0),
            containersTotal: Number.isFinite(Number(trackedPreviewRun.totalContainers)) ? Number(trackedPreviewRun.totalContainers) : null
          }}
          trackedBatchRun={trackedPreviewRun}
          batchSessionId={trackedPreviewRun.sessionId || previewRunId}
          onCancelBatch={trackedPreviewRun.status === 'running'
            ? () => handleCancelTrackedRun(trackedPreviewRun.id, 'preview')
            : null}
        />
      )}

      {/* Shared Headers Section */}
      <CollapsibleSection title="Headers & Authentication" icon={KeyRound} defaultOpen={!headersRaw}>
        <div className="space-y-4 pt-3">
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm text-[#aaa]">Headers Zenius (raw)</label>
              <button
                type="button"
                onClick={() => setShowHeadersHelp(!showHeadersHelp)}
                className="flex items-center gap-1 text-xs text-[#666] hover:text-[#aaa] transition-colors"
              >
                <HelpCircle className="w-3.5 h-3.5" />
                {showHeadersHelp ? 'Hide Help' : 'How to get headers?'}
              </button>
            </div>
            <textarea
              value={headersRaw}
              onChange={(e) => setHeadersRaw(e.target.value)}
              rows={6}
              className="input font-mono text-xs"
              placeholder={`__anti-forgery-token\t"..."\n__Secure-next-auth.session-token\t"..."\nuser-agent\t"Mozilla/5.0 ..."`}
            />
            <p className="text-xs text-[#666] mt-1.5">
              Format: <code className="text-[#888]">key&#9;"value"</code>, <code className="text-[#888]">Key: value</code>, atau cookie string.
            </p>
          </div>

          {showHeadersHelp && (
            <div className="bg-[#0d0d0d] rounded-lg p-4 space-y-3 border border-[#1f1f1f]">
              <h4 className="text-sm font-medium text-white flex items-center gap-2">
                <HelpCircle className="w-4 h-4 text-amber-400" />
                How to get headers from Zenius
              </h4>
              <ol className="space-y-2 text-xs text-[#aaa] list-decimal list-inside">
                <li>Open DevTools (<kbd className="px-1 py-0.5 bg-[#222] rounded text-[#888]">F12</kbd>) &rarr; <strong>Network</strong> tab</li>
                <li>Login to <code className="text-[#8aa6d8]">zenius.net</code> and navigate to any video page</li>
                <li>Find a request to <code className="text-[#8aa6d8]">zenius.net/api/...</code></li>
                <li>Right-click the request &rarr; <strong>Copy</strong> &rarr; <strong>Copy as cURL</strong></li>
                <li>Paste below, or extract headers manually into the textarea above</li>
              </ol>
              <div className="border-t border-[#222] pt-3 space-y-1.5">
                <p className="text-xs font-medium text-[#ccc]">Required headers:</p>
                <div className="grid grid-cols-1 gap-1">
                  <code className="text-[10px] text-emerald-400 bg-emerald-500/5 px-2 py-1 rounded">__anti-forgery-token</code>
                  <code className="text-[10px] text-emerald-400 bg-emerald-500/5 px-2 py-1 rounded">__Secure-next-auth.session-token</code>
                  <code className="text-[10px] text-sky-400 bg-sky-500/5 px-2 py-1 rounded">user-agent</code>
                  <code className="text-[10px] text-zinc-400 bg-zinc-500/5 px-2 py-1 rounded">sentry-trace (optional)</code>
                  <code className="text-[10px] text-zinc-400 bg-zinc-500/5 px-2 py-1 rounded">baggage (optional)</code>
                </div>
              </div>
              <div className="border-t border-[#222] pt-3">
                <p className="text-xs text-[#888]">
                  Headers disimpan otomatis di browser. Tidak perlu input ulang setiap kali.
                </p>
              </div>
            </div>
          )}
        </div>
      </CollapsibleSection>

      {/* Webhook Notifications */}
      <CollapsibleSection title="Notifikasi Webhook" icon={webhookConfig?.enabled ? Bell : BellOff} defaultOpen={false} badge={webhookConfig?.enabled ? 'Aktif' : 'Mati'}>
        <div className="space-y-4 pt-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => updateWebhookMutation.mutate({ enabled: !webhookConfig?.enabled })}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${webhookConfig?.enabled ? 'bg-emerald-500' : 'bg-[#333]'}`}
              >
                <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${webhookConfig?.enabled ? 'translate-x-6' : 'translate-x-1'}`} />
              </button>
              <span className="text-sm text-[#aaa]">
                {webhookConfig?.enabled ? 'Notifikasi aktif' : 'Notifikasi mati'}
              </span>
            </div>
          </div>

          <div>
            <label className="text-sm text-[#aaa] block mb-2">Webhook URL</label>
            <input
              type="text"
              value={webhookConfig?.url || ''}
              onChange={(e) => updateWebhookMutation.mutate({ url: e.target.value })}
              placeholder="https://botwa-xxxx.herokuapp.com/send/text"
              className="input font-mono text-xs"
            />
            <p className="text-xs text-[#555] mt-1">Endpoint POST yang menerima <code className="text-[#888]">{'{ to, text }'}</code></p>
          </div>

          <div>
            <label className="text-sm text-[#aaa] block mb-2">Nomor WhatsApp</label>
            <input
              type="text"
              value={webhookConfig?.to || ''}
              onChange={(e) => updateWebhookMutation.mutate({ to: e.target.value })}
              placeholder="6281234567890"
              className="input font-mono text-xs"
            />
            <p className="text-xs text-[#555] mt-1">Format: kode negara + nomor (tanpa + atau spasi)</p>
          </div>

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => testWebhookMutation.mutate()}
              disabled={!webhookConfig?.url || !webhookConfig?.to || testWebhookMutation.isLoading}
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border border-blue-500/30 text-blue-400 hover:bg-blue-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-xs"
            >
              {testWebhookMutation.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Send className="w-3.5 h-3.5" />}
              Test Notifikasi
            </button>
            {testWebhookMutation.isSuccess && (
              <span className="text-xs text-emerald-400 flex items-center gap-1">
                <CheckCircle className="w-3.5 h-3.5" /> Terkirim
              </span>
            )}
            {testWebhookMutation.isError && (
              <span className="text-xs text-red-400 flex items-center gap-1">
                <AlertCircle className="w-3.5 h-3.5" /> Gagal
              </span>
            )}
          </div>

          <div className="p-3 rounded-lg bg-[#0d0d0d] border border-[#1f1f1f]">
            <p className="text-xs text-[#666]">
              Notifikasi akan dikirim otomatis setiap kali <strong className="text-[#aaa]">batch download selesai</strong> (berhasil atau gagal). Berisi: status batch, jumlah video, statistik, durasi, dan error (jika ada).
            </p>
          </div>
        </div>
      </CollapsibleSection>

      {/* Tab Navigation */}
      <div className="flex gap-1 p-1 bg-[#1a1a1a] rounded-lg border border-[#2a2a2a]">
        <button
          type="button"
          onClick={() => setActiveTab('single')}
          className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-md text-sm font-medium transition-all ${
            activeTab === 'single' ? 'bg-[#2a2a2a] text-white shadow-sm' : 'text-[#888] hover:text-[#ccc]'
          }`}
        >
          <Download className="w-4 h-4" />
          Single Download
        </button>
        <button
          type="button"
          onClick={() => setActiveTab('batch')}
          className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-md text-sm font-medium transition-all ${
            activeTab === 'batch' ? 'bg-[#2a2a2a] text-white shadow-sm' : 'text-[#888] hover:text-[#ccc]'
          }`}
        >
          <Zap className="w-4 h-4" />
          Batch Download
        </button>
      </div>

      {/* Single Download Tab */}
      {activeTab === 'single' && (
        <form onSubmit={handleGetDetails} className="card p-5 space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                <Search className="w-4 h-4 text-[#666]" />
                URL Short ID
              </label>
              <input
                type="text"
                value={urlShortId}
                onChange={(e) => setUrlShortId(e.target.value.replace(/[^0-9]/g, ''))}
                placeholder="79497"
                className="input"
                required
              />
            </div>

            <div>
              <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                <FolderOpen className="w-4 h-4 text-[#666]" />
                Folder Path <span className="text-[#555]">(optional)</span>
              </label>
              <input
                type="text"
                value={folderId}
                onChange={(e) => handleFolderInputChange(e.target.value)}
                placeholder="kelas10/MTK wajib/Persamaan"
                className="input"
                list="zenius-folder-suggestions"
              />
              <datalist id="zenius-folder-suggestions">
                {folderOptions.map((item) => <option key={item.id} value={item.path} />)}
              </datalist>

              <div className="mt-2 space-y-1.5 text-xs">
                <p className="text-[#666]">Preview: <span className="text-[#d0d0d0] font-mono">{resolvedFolderPathPreview}</span></p>
                <p className={folderExists ? 'text-emerald-400' : 'text-amber-400'}>
                  {folderExists ? 'Folder sudah ada.' : 'Folder belum ada, akan dibuat otomatis.'}
                </p>
                {folderSuggestions.length > 0 && (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {folderSuggestions.map((item) => (
                      <button key={item.id} type="button" onClick={() => handleFolderInputChange(item.path)} className="px-2 py-1 rounded bg-[#1c1c1c] hover:bg-[#262626] border border-[#2e2e2e] text-[#bfbfbf]">
                        {item.path}
                      </button>
                    ))}
                  </div>
                )}
                {exactFolderMatch && (
                  <button type="button" onClick={handleDeleteFolder} disabled={deleteFolderMutation.isLoading} className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded border border-red-500/40 text-red-300 hover:bg-red-500/10 disabled:opacity-40">
                    {deleteFolderMutation.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                    Hapus Folder
                  </button>
                )}
                {deleteFolderMutation.isSuccess && <p className="text-emerald-400">Folder berhasil dihapus.</p>}
                {deleteFolderMutation.isError && <p className="text-red-400">{deleteFolderMutation.error?.response?.data?.error || deleteFolderMutation.error?.message}</p>}
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-sm text-[#aaa] block mb-2">Filename Override <span className="text-[#555]">(optional)</span></label>
              <input type="text" value={filename} onChange={(e) => setFilename(e.target.value)} className="input" placeholder="fungsi-eksponen" />
              <p className="text-xs text-[#666] mt-1">Kosongkan untuk pakai nama dari response API.</p>
            </div>
            <div>
              <label className="text-sm text-[#aaa] block mb-2">Referer Path <span className="text-[#555]">(optional)</span></label>
              <input type="text" value={refererPath} onChange={(e) => setRefererPath(e.target.value)} className="input" placeholder="/ci/79497/fungsi-eksponen" />
              <p className="text-xs text-[#666] mt-1">Jika kosong, otomatis dari response.</p>
            </div>
          </div>

          <ProviderSelector providers={providers} selectedProviders={selectedProviders} setSelectedProviders={setSelectedProviders} />

          <div className="flex flex-col sm:flex-row gap-3">
            <button type="submit" disabled={!urlShortId || isBusy} className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto">
              {detailsMutation.isLoading ? <><Loader2 className="w-4 h-4 animate-spin" /> Getting Details...</> : <><Search className="w-4 h-4" /> Get Details</>}
            </button>
            <button type="button" onClick={handleDownload} disabled={!urlShortId || isBusy} className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto">
              {downloadMutation.isLoading ? <><Loader2 className="w-4 h-4 animate-spin" /> Queueing...</> : <><Download className="w-4 h-4" /> Download & Distribusi</>}
            </button>
          </div>
        </form>
      )}

      {/* Batch Download Tab */}
      {activeTab === 'batch' && (
        <div className="space-y-4">
          <div className="card p-5 space-y-4">
            <div>
              <h3 className="text-lg font-medium text-white flex items-center gap-2">
                <Zap className="w-5 h-5 text-amber-400" />
                Batch Downloader
              </h3>
              <p className="text-xs text-[#777] mt-1">
                Download semua video dari CGroup chain. Build chain dulu (preview), lalu start batch download.
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm text-[#aaa] block mb-2">Root CGroup ID</label>
                <input
                  ref={batchRootCgRef}
                  type="text"
                  value={batchRootCgId}
                  onChange={(e) => setBatchRootCgId(e.target.value.replace(/[^0-9]/g, ''))}
                  placeholder="34"
                  className="input"
                  required
                />
                <p className="text-xs text-[#555] mt-1">Shortcut: <kbd className="px-1 py-0.5 bg-[#222] rounded text-[#888]">Ctrl+B</kbd> to focus</p>
              </div>
              <div>
                <label className="text-sm text-[#aaa] block mb-2">Target CG Selector <span className="text-[#555]">(optional)</span></label>
                <input type="text" value={batchTargetCgSelector} onChange={(e) => setBatchTargetCgSelector(e.target.value)} placeholder="/cg/83067 atau 83067" className="input" />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm text-[#aaa] block mb-2">Parent Folder Name <span className="text-[#555]">(optional)</span></label>
                <input type="text" value={batchParentContainerName} onChange={(e) => setBatchParentContainerName(e.target.value)} placeholder="Otomatis dari nama CGroup" className="input" />
                <p className="text-xs text-[#666] mt-1">Override parent folder dari chain mapping.</p>
              </div>
              <div>
                <label className="text-sm text-[#aaa] block mb-2">Folder Prefix <span className="text-[#555]">(optional)</span></label>
                <button
                  type="button"
                  onClick={() => setShowFolderPrefixPicker(true)}
                  className="w-full flex items-center gap-3 px-4 py-2.5 bg-[#0d0d0d] border border-[#2a2a2a] rounded-lg text-left hover:border-[#3a3a3a] transition-colors"
                >
                  <FolderTree className="w-4 h-4 text-amber-400 flex-shrink-0" />
                  <span className={`text-sm flex-1 truncate ${normalizedBatchFolderPrefix ? 'text-white' : 'text-[#555]'}`}>
                    {normalizedBatchFolderPrefix || 'Pilih folder...'}
                  </span>
                  {normalizedBatchFolderPrefix && (
                    <button
                      type="button"
                      onClick={(e) => { e.stopPropagation(); setBatchFolderPrefix(''); }}
                      className="p-0.5 hover:bg-[#333] rounded text-[#666] hover:text-white"
                    >
                      <X className="w-3.5 h-3.5" />
                    </button>
                  )}
                  <ChevronRight className="w-4 h-4 text-[#555]" />
                </button>
                <p className="text-xs text-[#666] mt-1">
                  Video disimpan di: <code className="text-[#8aa6d8]">root/{normalizedBatchFolderPrefix ? `${normalizedBatchFolderPrefix}/` : ''}{'{'}chain path{'}'}</code>
                </p>
              </div>
            </div>

            <ProviderSelector providers={providers} selectedProviders={selectedProviders} setSelectedProviders={setSelectedProviders} />

            <div className="rounded-lg border border-[#242424] bg-[#0d0d0d] p-3 space-y-2">
              <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <div>
                  <p className="text-sm text-[#ddd]">Preview Mode</p>
                  <p className="text-xs text-[#666]">
                    {batchPreviewMode === 'ultra'
                      ? 'Ultra: preview tidak cek duplicate/provider; semua validasi dilakukan saat batch download.'
                      : batchPreviewMode === 'fast'
                        ? 'Fast: cek duplicate file efisien, provider dicek saat batch download.'
                        : 'Full: preview cek duplicate file dan status provider, summary paling presisi.'}
                  </p>
                </div>
                <div className="flex rounded-lg border border-[#333] bg-[#111] p-1 text-xs">
                  <button
                    type="button"
                    onClick={() => setBatchPreviewMode('ultra')}
                    className={`px-3 py-1.5 rounded-md transition-colors ${batchPreviewMode === 'ultra' ? 'bg-violet-500/20 text-violet-300' : 'text-[#777] hover:text-[#ddd]'}`}
                  >
                    Ultra
                  </button>
                  <button
                    type="button"
                    onClick={() => setBatchPreviewMode('fast')}
                    className={`px-3 py-1.5 rounded-md transition-colors ${batchPreviewMode === 'fast' ? 'bg-amber-500/20 text-amber-300' : 'text-[#777] hover:text-[#ddd]'}`}
                  >
                    Fast
                  </button>
                  <button
                    type="button"
                    onClick={() => setBatchPreviewMode('full')}
                    className={`px-3 py-1.5 rounded-md transition-colors ${batchPreviewMode === 'full' ? 'bg-emerald-500/20 text-emerald-300' : 'text-[#777] hover:text-[#ddd]'}`}
                  >
                    Full
                  </button>
                </div>
              </div>
            </div>

            {/* Action Buttons */}
            <div className="flex flex-col sm:flex-row gap-3">
              <button
                type="button"
                onClick={handleGetBatchChain}
                disabled={!batchRootCgId || isBatchBusy}
                className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto"
              >
                {batchChainMutation.isLoading ? (
                  <><Loader2 className="w-4 h-4 animate-spin" /> Building Chain...</>
                ) : batchChain ? (
                  <><RefreshCw className="w-4 h-4" /> Rebuild Chain</>
                ) : (
                  <><Eye className="w-4 h-4" /> Preview Chain</>
                )}
              </button>

              <button
                type="button"
                onClick={handleStartBatchDownload}
                disabled={!batchRootCgId || !batchChain || isBatchBusy}
                className={
                  !batchChain
                    ? 'btn flex items-center justify-center gap-2 sm:w-auto rounded-lg px-5 py-2.5 font-medium transition-colors bg-[#1a1a1a] text-[#555] border border-[#2a2a2a] cursor-not-allowed'
                    : 'btn flex items-center justify-center gap-2 sm:w-auto rounded-lg px-5 py-2.5 font-medium transition-colors bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30 border border-emerald-500/30'
                }
              >
                {batchDownloadMutation.isLoading ? (
                  <><Loader2 className="w-4 h-4 animate-spin" /> Starting Batch...</>
                ) : (
                  <><Zap className="w-4 h-4" /> Start Batch Download</>
                )}
              </button>

              <button
                type="button"
                onClick={() => handleCancelTrackedRun(previewCancelRunId, 'preview')}
                className="btn flex items-center justify-center gap-2 sm:w-auto rounded-lg px-5 py-2.5 font-medium transition-colors bg-red-500/15 text-red-300 hover:bg-red-500/25 border border-red-500/30"
              >
                {cancelBatchRunMutation.isLoading ? (
                  <><Loader2 className="w-4 h-4 animate-spin" /> Cancelling Preview...</>
                ) : (
                  <><XCircle className="w-4 h-4" /> Cancel Preview</>
                )}
              </button>
            </div>

            {!batchChain && !batchChainMutation.isLoading && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-[#0d0d0d] border border-[#1f1f1f]">
                <Eye className="w-4 h-4 text-[#555] mt-0.5 flex-shrink-0" />
                <p className="text-xs text-[#666]">
                  Klik <strong className="text-[#aaa]">Preview Chain</strong> terlebih dahulu untuk melihat daftar video yang akan di-download. Batch download hanya bisa dimulai setelah preview.
                </p>
              </div>
            )}

            {batchChain && !batchChainMutation.isLoading && isBatchChainReady && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-emerald-500/5 border border-emerald-500/20">
                <CheckCircle className="w-4 h-4 text-emerald-400 mt-0.5 flex-shrink-0" />
                <p className="text-xs text-emerald-300">
                  Chain ready — <strong>{batchVideoCount} video</strong> ditemukan di <strong>{batchPreviewContainerCount} container</strong>. Periksa preview di bawah, lalu klik <strong>Start Batch Download</strong>.
                </p>
              </div>
            )}

            {batchChain && !batchChainMutation.isLoading && !isBatchChainReady && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-sky-500/5 border border-sky-500/20">
                <Loader2 className="w-4 h-4 text-sky-400 mt-0.5 flex-shrink-0 animate-spin" />
                <p className="text-xs text-sky-300">
                  Preview masih diproses — saat ini <strong>{batchVideoCount} video</strong> dari <strong>{batchPreviewContainerCount} container</strong> (discovered: <strong>{batchDiscoveredContainerCount}</strong>). Tunggu hingga plan selesai sebelum start batch download.
                </p>
              </div>
            )}

            {(previewPollErrorCount > 0 || trackedPreviewRun?.lastError) && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
                <AlertCircle className="w-4 h-4 text-amber-300 mt-0.5 flex-shrink-0" />
                <div className="text-xs text-amber-200 space-y-1">
                  <p>
                    {previewPollErrorCount > 0
                      ? `Polling preview sempat gagal ${previewPollErrorCount}x. FE tetap mencoba reconnect tanpa menghapus session.`
                      : 'Preview worker melaporkan error terakhir, namun session tetap dipertahankan.'}
                  </p>
                  {trackedPreviewRun?.lastError && <p className="text-amber-300/90">Last error: {trackedPreviewRun.lastError}</p>}
                </div>
              </div>
            )}

            {/* Build Progress */}
            {batchBuildProgress && (
              <div className="space-y-2">
                <div className="flex items-center justify-between text-xs">
                  <span className={previewStatusTone}>
                    {previewPollErrorCount > 0 ? 'Building chain... reconnecting status' : 'Building chain...'}
                  </span>
                  <span className="text-[#888] font-mono">{batchBuildProgress.processed} / {batchBuildProgress.total ?? '?'}</span>
                </div>
                <div className="w-full bg-[#1a1a1a] rounded-full h-2 overflow-hidden">
                  <div className="bg-gradient-to-r from-sky-500 to-sky-400 h-full rounded-full transition-all duration-300" style={{ width: `${batchBuildProgress.total ? Math.round((batchBuildProgress.processed / batchBuildProgress.total) * 100) : 0}%` }} />
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[11px]">
                  <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                    <span className="text-[#666]">Discovered videos</span>
                    <p className="text-sky-200 font-mono">{Number(batchBuildProgress.discoveredVideos || 0)}</p>
                  </div>
                  <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                    <span className="text-[#666]">Preview containers</span>
                    <p className="text-sky-200 font-mono">{Number(batchBuildProgress.previewContainers || 0)}</p>
                  </div>
                  <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                    <span className="text-[#666]">Discovered containers</span>
                    <p className="text-sky-200 font-mono">{Number(batchBuildProgress.discoveredContainers || 0)}</p>
                  </div>
                  <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                    <span className="text-[#666]">Status</span>
                    <p className="text-sky-200 font-mono">{batchBuildProgress.hasMore ? 'running' : 'finalizing'}</p>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Chain Preview */}
          {batchChain && (
            <CollapsibleSection
              title="Chain Preview"
              icon={Eye}
              badge={`${batchVideoCount} videos, ${batchPreviewContainerCount} preview containers`}
              defaultOpen={true}
              rightElement={
                <button
                  type="button"
                  onClick={handleStartBatchDownload}
                  disabled={isBatchBusy || !isBatchChainReady}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30 border border-emerald-500/30 transition-colors disabled:opacity-40"
                >
                  {batchDownloadMutation.isLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Zap className="w-3.5 h-3.5" />}
                  Start Batch
                </button>
              }
            >
              <div className="space-y-3 pt-3">
                <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-sm">
                  <div className="bg-[#0d0d0d] p-3 rounded-lg">
                    <p className="text-[#666] mb-1 text-xs">Leaf CG IDs</p>
                    <p className="text-white font-mono break-all">
                      {Array.isArray(batchChain.leafCgIds) && batchChain.leafCgIds.length > 0
                        ? batchChain.leafCgIds.join(', ')
                        : (batchChain.leafCgId || '-')}
                    </p>
                  </div>
                  <div className="bg-[#0d0d0d] p-3 rounded-lg">
                    <p className="text-[#666] mb-1 text-xs">Preview Containers</p>
                    <p className="text-white">{batchPreviewContainerCount}</p>
                  </div>
                  <div className="bg-[#0d0d0d] p-3 rounded-lg">
                    <p className="text-[#666] mb-1 text-xs">Discovered Containers</p>
                    <p className="text-white">{batchDiscoveredContainerCount}</p>
                  </div>
                  <div className="bg-[#0d0d0d] p-3 rounded-lg">
                    <p className="text-[#666] mb-1 text-xs">Video Instances</p>
                    <p className="text-white">{batchVideoCount}</p>
                  </div>
                </div>

                {previewItemsSummary && (
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-sm">
                    <div className="bg-[#0d0d0d] p-3 rounded-lg">
                      <p className="text-[#666] mb-1 text-xs">Tracked Preview Items</p>
                      <p className="text-white">{Number(previewItemsSummary.totalTracked || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] p-3 rounded-lg">
                      <p className="text-[#666] mb-1 text-xs">New / Download</p>
                      <p className="text-emerald-300">{Number(previewItemsSummary.byAction?.download || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] p-3 rounded-lg">
                      <p className="text-[#666] mb-1 text-xs">Retry</p>
                      <p className="text-orange-300">{Number(previewItemsSummary.byAction?.retry || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] p-3 rounded-lg">
                      <p className="text-[#666] mb-1 text-xs">Finalize</p>
                      <p className="text-cyan-300">{Number(previewItemsSummary.byAction?.finalize || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] p-3 rounded-lg">
                      <p className="text-[#666] mb-1 text-xs">Skip</p>
                      <p className="text-amber-300">{Number(previewItemsSummary.byAction?.skip || 0)}</p>
                    </div>
                  </div>
                )}

                {Array.isArray(batchChain.errors) && batchChain.errors.length > 0 && (
                  <div className="p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
                    <p className="text-xs text-amber-300">
                      {batchChain.errors.length} error(s) during chain build. Data yang berhasil tetap ditampilkan.
                    </p>
                  </div>
                )}

                <div>
                  <div className="flex items-center justify-between gap-3 mb-2">
                    <p className="text-xs text-[#888]">Preview Mapping</p>
                    <p className="text-[11px] text-[#666]">
                      Menampilkan nama file final dan folder tujuan yang akan dipakai batch downloader.
                    </p>
                  </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mb-3 text-[11px]">
                    <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                      <span className="text-[#666]">Download</span>
                      <p className="text-emerald-300 font-mono">{Number(batchActionSummary.download || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                      <span className="text-[#666]">Retry</span>
                      <p className="text-orange-300 font-mono">{Number(batchActionSummary.retry || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                      <span className="text-[#666]">Finalize</span>
                      <p className="text-cyan-300 font-mono">{Number(batchActionSummary.finalize || 0)}</p>
                    </div>
                    <div className="bg-[#0d0d0d] rounded px-2 py-1 border border-[#1f1f1f]">
                      <span className="text-[#666]">Skip</span>
                      <p className="text-amber-300 font-mono">{Number(batchActionSummary.skip || 0)}</p>
                    </div>
                    </div>
                  {previewItemsSummary && (previewItemsSummary.overflow > 0 || previewItemsSummary.newItemsOverflow > 0 || previewItemsSummary.skippedItemsOverflow > 0 || previewItemsSummary.retryItemsOverflow > 0 || previewItemsSummary.finalizeItemsOverflow > 0) && (
                    <div className="mb-3 p-2 rounded border border-[#2a2a2a] bg-[#111] text-[11px] text-[#7f7f7f]">
                      Sample list dibatasi untuk menjaga memory FE ringan.
                      {previewItemsSummary.overflow > 0 ? ` Extra preview items: +${previewItemsSummary.overflow}.` : ''}
                      {previewItemsSummary.newItemsOverflow > 0 ? ` New overflow: +${previewItemsSummary.newItemsOverflow}.` : ''}
                      {previewItemsSummary.skippedItemsOverflow > 0 ? ` Skip overflow: +${previewItemsSummary.skippedItemsOverflow}.` : ''}
                      {previewItemsSummary.retryItemsOverflow > 0 ? ` Retry overflow: +${previewItemsSummary.retryItemsOverflow}.` : ''}
                      {previewItemsSummary.finalizeItemsOverflow > 0 ? ` Finalize overflow: +${previewItemsSummary.finalizeItemsOverflow}.` : ''}
                    </div>
                  )}
                  {hiddenBatchPreviewRowCount > 0 && (
                    <div className="mb-3 p-2 rounded border border-[#2a2a2a] bg-[#111] text-[11px] text-[#7f7f7f]">
                      Tabel hanya merender {BATCH_PREVIEW_ROW_RENDER_LIMIT} baris pertama agar UI tetap responsif. {hiddenBatchPreviewRowCount} baris lain tetap masuk plan batch download.
                    </div>
                  )}
                  <div className="max-h-64 overflow-auto rounded-lg border border-[#222] bg-[#0d0d0d]">
                    <table className="w-full text-xs">
                      <thead className="bg-[#141414] text-[#8a8a8a] sticky top-0">
                        <tr>
                          <th className="text-left px-3 py-2">#</th>
                          <th className="text-left px-3 py-2">Status</th>
                          <th className="text-left px-3 py-2">ID</th>
                          <th className="text-left px-3 py-2">Nama</th>
                          <th className="text-left px-3 py-2">Output File</th>
                          <th className="text-left px-3 py-2">Target Folder</th>
                          <th className="text-left px-3 py-2">Catatan</th>
                        </tr>
                      </thead>
                      <tbody>
                        {batchPreviewRows.map((item, index) => (
                          <tr key={item.key} className="border-t border-[#1e1e1e] hover:bg-[#141414] align-top">
                            <td className="px-3 py-2 text-[#555]">{index + 1}</td>
                            <td className="px-3 py-2 whitespace-nowrap">
                              <StatusBadge
                                status={item.action === 'download'
                                  ? 'ready'
                                  : item.action === 'skip'
                                    ? 'skipped'
                                    : item.action === 'retry'
                                      ? 'retry'
                                      : item.action === 'finalize'
                                        ? 'finalize'
                                        : 'mapped'}
                              />
                            </td>
                             <td className="px-3 py-2 text-[#c8c8c8] font-mono whitespace-nowrap">{item.urlShortId}</td>
                             <td className="px-3 py-2 text-[#cfd5df] min-w-[220px]">
                               <div className="break-words">{item.instanceName || '-'}</div>
                             </td>
                             <td className="px-3 py-2 text-[#e2e2e2] min-w-[240px]">
                               <div className="font-medium break-all">{item.outputName}</div>
                               <div className="text-[11px] text-[#666] mt-1">Container: {item.containerName}</div>
                            </td>
                            <td className="px-3 py-2 text-[#8aa6d8] font-mono min-w-[260px] break-all">{item.folderInput || 'root'}</td>
                            <td className="px-3 py-2 text-[#9aa3af] min-w-[260px]">
                              <div>{item.reason}</div>
                              {item.chainPath ? (
                                <div className="text-[11px] text-[#666] mt-1 break-all">Chain path: {item.chainPath}</div>
                              ) : null}
                              {item.providers.length > 0 ? (
                                <div className="text-[11px] text-[#666] mt-1 break-all">Pending providers: {item.providers.join(', ')}</div>
                              ) : null}
                              {item.selectedProviders.length > 0 ? (
                                <div className="text-[11px] text-[#666] mt-1 break-all">Selected providers: {item.selectedProviders.join(', ')}</div>
                              ) : null}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </CollapsibleSection>
          )}
        </div>
      )}

      {/* Instance Details */}
      {instanceValue && (
        <CollapsibleSection title="Instance Details" icon={Shield} defaultOpen badge={details?.ok ? 'ok' : 'error'}>
          <div className="space-y-4 pt-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
              <div className="bg-[#0d0d0d] p-3 rounded-lg">
                <p className="text-[#666] mb-1 text-xs">Judul</p>
                <p className="text-white">{instanceValue.name || '-'}</p>
              </div>
              <div className="bg-[#0d0d0d] p-3 rounded-lg">
                <p className="text-[#666] mb-1 text-xs">Durasi</p>
                <p className="text-white">{formatDuration(Number(instanceValue['duration-seconds']))}</p>
              </div>
              <div className="bg-[#0d0d0d] p-3 rounded-lg">
                <p className="text-[#666] mb-1 text-xs">Parent</p>
                <p className="text-white">{instanceValue.parent?.name || '-'}</p>
              </div>
              <div className="bg-[#0d0d0d] p-3 rounded-lg">
                <p className="text-[#666] mb-1 text-xs">Type / Hosting</p>
                <p className="text-white">{instanceValue.type || '-'} / {instanceValue['hosting-type'] || '-'}</p>
              </div>
            </div>

            <div>
              <p className="text-xs text-[#aaa] mb-1.5 flex items-center gap-2"><LinkIcon className="w-3.5 h-3.5 text-[#666]" /> Video URL</p>
              <div className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222]">
                <p className="text-xs text-[#8aa6d8] break-all">{instanceValue['video-url'] || '-'}</p>
              </div>
            </div>

            <CollapsibleSection title="Raw Response" icon={ChevronRight} defaultOpen={false}>
              <pre className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222] text-xs text-[#9da6b3] overflow-x-auto max-h-60">
                {JSON.stringify(details?.body, null, 2)}
              </pre>
            </CollapsibleSection>
          </div>
        </CollapsibleSection>
      )}

      {/* Download Success Toast-style Card */}
      {downloadMutation.isSuccess && !batchResult && (
        <div className="card p-4 flex items-center gap-3 border-emerald-500/30">
          <CheckCircle className="w-5 h-5 text-emerald-400" />
          <div>
            <p className="text-sm text-emerald-400 font-medium">Download queued</p>
            <p className="text-xs text-[#888]">Proses FFmpeg dan distribusi storage sudah masuk antrian. Pantau di halaman Jobs/Status.</p>
          </div>
        </div>
      )}

      {/* Error Card */}
      {(detailsMutation.isError || downloadMutation.isError || batchChainMutation.isError || batchDownloadMutation.isError) && (
        <div className="card p-4 flex items-center gap-3 border-red-500/30">
          <AlertCircle className="w-5 h-5 text-red-400" />
          <div>
            <p className="text-sm text-red-400 font-medium">Request failed</p>
            <p className="text-xs text-[#888] break-all">
              {detailsMutation.error?.response?.data?.error || downloadMutation.error?.response?.data?.error || batchChainMutation.error?.response?.data?.error || batchDownloadMutation.error?.response?.data?.error || detailsMutation.error?.message || downloadMutation.error?.message || batchChainMutation.error?.message || batchDownloadMutation.error?.message || 'Unknown error'}
            </p>
          </div>
        </div>
      )}

      {/* Batch Confirm Dialog */}
      <BatchConfirmDialog
        isOpen={Boolean(showBatchConfirm)}
        onClose={() => setShowBatchConfirm(null)}
        onConfirm={handleConfirmBatchDownload}
        summary={showBatchConfirm}
      />

      {/* Cancel All Confirmation */}
      <ConfirmDialog isOpen={showCancelConfirm} onClose={() => setShowCancelConfirm(false)} onConfirm={async () => { await cancelAllMutation.mutateAsync(); setShowCancelConfirm(false); }} title="Cancel All Downloads" message="This will cancel all active and queued Zenius downloads and uploads. Are you sure?" confirmText="Cancel All" variant="danger" />
      <ConfirmDialog isOpen={showResetConfirm} onClose={() => setShowResetConfirm(false)} onConfirm={async () => { await resetFilesMutation.mutateAsync(); setShowResetConfirm(false); }} title="Reset All Zenius Files" message="This will permanently delete all Zenius files and cancel all related jobs. This action cannot be undone." confirmText="Reset All" variant="danger" />
      <ConfirmDialog isOpen={showDeleteAllFoldersConfirm} onClose={() => setShowDeleteAllFoldersConfirm(false)} onConfirm={async () => { await deleteAllFolders.mutateAsync(); setShowDeleteAllFoldersConfirm(false); }} title="Remove All Folders" message="This will permanently delete ALL folders (except root), their files, and jobs. This action cannot be undone." confirmText="Delete All" variant="danger" />

      <FolderPickerModal
        isOpen={showFolderPrefixPicker}
        onClose={() => setShowFolderPrefixPicker(false)}
        onSelect={(path) => setBatchFolderPrefix(path)}
        folderTree={folderTree}
        currentValue={normalizedBatchFolderPrefix}
        title="Pilih Folder Prefix"
        description="Video akan disimpan di dalam folder ini. Pilih Root untuk tanpa prefix."
      />
    </div>
  );
}
