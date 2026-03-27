import React, { useEffect, useMemo, useState } from 'react';
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
  Shield
} from 'lucide-react';
import {
  useDeleteFolder,
  useFolders,
  useProviders,
  useZeniusBatchChain,
  useZeniusBatchDownload,
  useZeniusDownload,
  useZeniusInstanceDetails
} from '../hooks/api';
import { PROVIDERS } from '../config/providers';

const HEADERS_STORAGE_KEY = 'zenius-headers-raw';
const BATCH_CHAIN_CHUNK_SIZE = 8;
const BATCH_DOWNLOAD_CHUNK_SIZE = 6;
const BATCH_REQUEST_BUDGET_MS = 24000;

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return '-';
  }

  const totalSeconds = Math.floor(seconds);
  const hh = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
  const mm = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
  const ss = String(totalSeconds % 60).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function normalizeFolderInput(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) {
    return '';
  }

  const unwrapped = ((raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith('\'') && raw.endsWith('\'')))
    ? raw.slice(1, -1).trim()
    : raw;

  let normalized = unwrapped
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/+/g, '/');

  if (normalized.toLowerCase() === 'root') {
    return '';
  }

  if (normalized.toLowerCase().startsWith('root/')) {
    normalized = normalized.slice(5);
  }

  return normalized.trim();
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

function ProviderSelector({ providers, selectedProviders, setSelectedProviders }) {
  const enabledProviders = useMemo(
    () => Object.entries(providers || {}).filter(([_, item]) => item.enabled).map(([key]) => key),
    [providers]
  );

  const toggleProvider = (provider) => {
    setSelectedProviders((prev) => (
      prev.includes(provider)
        ? prev.filter((name) => name !== provider)
        : [...prev, provider]
    ));
  };

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

      <div className="grid grid-cols-2 gap-2">
        {Object.entries(providers || {}).map(([key, info]) => {
          const config = PROVIDERS[key];
          const isEnabled = info.enabled;
          const isSelected = selectedProviders.includes(key);

          return (
            <button
              key={key}
              type="button"
              disabled={!isEnabled}
              onClick={() => isEnabled && toggleProvider(key)}
              className={`px-3 py-2 rounded-lg text-sm border transition-colors text-left ${
                !isEnabled
                  ? 'bg-[#111] border-[#1f1f1f] text-[#555] cursor-not-allowed'
                  : isSelected
                    ? `${config?.bgColor || 'bg-[#2a2a2a]'} ${config?.color || 'text-white'} border-current`
                    : 'bg-[#0d0d0d] border-[#2a2a2a] text-[#999] hover:bg-[#161616]'
              }`}
            >
              {config?.name || key}
            </button>
          );
        })}
      </div>

      <p className="text-xs text-[#666]">
        {enabledProviders.length === 0
          ? 'Tidak ada provider aktif. Aktifkan provider di halaman Providers.'
          : selectedProviders.length === 0
            ? 'Semua provider aktif akan dipakai untuk distribusi.'
            : `Akan upload ke: ${selectedProviders.join(', ')}`}
      </p>
    </div>
  );
}

export default function ZeniusPage() {
  const [urlShortId, setUrlShortId] = useState('');
  const [headersRaw, setHeadersRaw] = useState('');
  const [refererPath, setRefererPath] = useState('');
  const [folderId, setFolderId] = useState('');
  const [filename, setFilename] = useState('');
  const [selectedProviders, setSelectedProviders] = useState([]);
  const [details, setDetails] = useState(null);
  const [batchRootCgId, setBatchRootCgId] = useState('34');
  const [batchTargetCgSelector, setBatchTargetCgSelector] = useState('');
  const [batchParentContainerName, setBatchParentContainerName] = useState('Pengantar Keterampilan Matematika Dasar');
  const [batchFolderPrefix, setBatchFolderPrefix] = useState('');
  const [batchChain, setBatchChain] = useState(null);
  const [batchResult, setBatchResult] = useState(null);
  const [batchSessionId, setBatchSessionId] = useState(null);
  const [batchBuildProgress, setBatchBuildProgress] = useState(null);
  const [batchQueueProgress, setBatchQueueProgress] = useState(null);

  const detailsMutation = useZeniusInstanceDetails();
  const downloadMutation = useZeniusDownload();
  const batchChainMutation = useZeniusBatchChain();
  const batchDownloadMutation = useZeniusBatchDownload();
  const deleteFolderMutation = useDeleteFolder();
  const { data: providers } = useProviders();
  const { data: folderTree } = useFolders();

  useEffect(() => {
    const savedHeaders = localStorage.getItem(HEADERS_STORAGE_KEY);

    if (savedHeaders) setHeadersRaw(savedHeaders);
  }, []);

  useEffect(() => {
    localStorage.setItem(HEADERS_STORAGE_KEY, headersRaw);
  }, [headersRaw]);

  const isBusy = detailsMutation.isLoading || downloadMutation.isLoading;
  const isBatchBusy = batchChainMutation.isLoading || batchDownloadMutation.isLoading;
  const instanceValue = details?.value || null;
  const normalizedFolderPath = useMemo(() => normalizeFolderInput(folderId), [folderId]);
  const normalizedBatchFolderPrefix = useMemo(() => normalizeFolderInput(batchFolderPrefix), [batchFolderPrefix]);

  const folderOptions = useMemo(() => flattenFolderPaths(folderTree), [folderTree]);

  const folderPathLookup = useMemo(() => {
    return new Set(folderOptions.map((item) => item.path.toLowerCase()));
  }, [folderOptions]);

  const folderMapByPath = useMemo(() => {
    const map = new Map();
    for (const item of folderOptions) {
      map.set(item.path.toLowerCase(), item);
    }
    return map;
  }, [folderOptions]);

  const exactFolderMatch = normalizedFolderPath
    ? folderMapByPath.get(normalizedFolderPath.toLowerCase()) || null
    : null;

  const folderSuggestions = useMemo(() => {
    const keyword = normalizedFolderPath.toLowerCase();
    const list = keyword
      ? folderOptions.filter((item) => item.path.toLowerCase().includes(keyword))
      : folderOptions;

    return list.slice(0, 6);
  }, [folderOptions, normalizedFolderPath]);

  const folderExists = normalizedFolderPath
    ? folderPathLookup.has(normalizedFolderPath.toLowerCase())
    : true;

  const resolvedFolderPathPreview = normalizedFolderPath ? `root/${normalizedFolderPath}` : 'root';

  const handleFolderInputChange = (value) => {
    setFolderId(value);
    if (deleteFolderMutation.isError || deleteFolderMutation.isSuccess) {
      deleteFolderMutation.reset();
    }
  };

  const handleDeleteFolder = async () => {
    if (!exactFolderMatch) {
      return;
    }

    const confirmed = window.confirm(`Hapus folder "root/${exactFolderMatch.path}"? Folder harus kosong.`);
    if (!confirmed) {
      return;
    }

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
      const result = await detailsMutation.mutateAsync({
        urlShortId,
        headersRaw,
        refererPath
      });

      setDetails(result);

      if (!filename && result?.value?.name) {
        setFilename(result.value.name);
      }

      if (!refererPath && result?.value?.['path-url']) {
        setRefererPath(result.value['path-url']);
      }
    } catch (error) {
      setDetails(null);
      console.error('Failed to fetch instance details:', error);
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
    } catch (error) {
      console.error('Failed to queue zenius download:', error);
    }
  };

  const handleGetBatchChain = async (event) => {
    event.preventDefault();

    let aggregate = null;
    let currentSessionId = null;
    let iteration = 0;

    try {
      setBatchChain(null);
      setBatchResult(null);
      setBatchSessionId(null);
      setBatchBuildProgress({ processed: 0, total: null });

      let nextOffset = 0;
      let hasMore = true;

      while (hasMore) {
        iteration += 1;
        if (iteration > 60) {
          break;
        }

        const result = await batchChainMutation.mutateAsync({
          rootCgId: batchRootCgId,
          targetCgSelector: batchTargetCgSelector,
          parentContainerName: batchParentContainerName,
          headersRaw,
          refererPath,
          sessionId: currentSessionId,
          containerOffset: nextOffset,
          containerLimit: currentSessionId ? BATCH_CHAIN_CHUNK_SIZE : Math.max(2, Math.floor(BATCH_CHAIN_CHUNK_SIZE / 2)),
          timeBudgetMs: BATCH_REQUEST_BUDGET_MS
        });

        currentSessionId = result.sessionId || currentSessionId;
        setBatchSessionId(currentSessionId);

        if (!aggregate) {
          aggregate = {
            ...result,
            containerDetails: [...(result.containerDetails || [])],
            errors: [...(result.errors || [])]
          };
        } else {
          aggregate = {
            ...aggregate,
            ...result,
            sessionId: currentSessionId || aggregate.sessionId || null,
            containerDetails: [
              ...(aggregate.containerDetails || []),
              ...(result.containerDetails || [])
            ],
            errors: [...(result.errors || aggregate.errors || [])]
          };
        }

        const total = Number.isFinite(Number(result.totalContainers))
          ? Number(result.totalContainers)
          : Number.isFinite(Number(aggregate?.totalContainers))
            ? Number(aggregate.totalContainers)
            : null;

        setBatchBuildProgress({
          processed: aggregate.containerDetails.length,
          total
        });
        setBatchChain(aggregate);

        hasMore = Boolean(result.hasMoreContainers);
        nextOffset = Number.isFinite(Number(result.nextContainerOffset))
          ? Number(result.nextContainerOffset)
          : 0;
      }

      setBatchBuildProgress(null);
    } catch (error) {
      if (!aggregate) {
        setBatchChain(null);
      }
      setBatchBuildProgress(null);
      console.error('Failed to fetch zenius batch chain:', error);
    }
  };

  const handleBatchDownload = async (event) => {
    event.preventDefault();

    let aggregateResult = null;
    let currentSessionId = batchSessionId || batchChain?.sessionId || null;
    let iteration = 0;

    try {
      setBatchResult(null);
      setBatchQueueProgress({ processed: 0, total: null });

      let nextOffset = 0;
      let hasMore = true;

      while (hasMore) {
        iteration += 1;
        if (iteration > 60) {
          break;
        }

        const result = await batchDownloadMutation.mutateAsync({
          rootCgId: batchRootCgId,
          targetCgSelector: batchTargetCgSelector,
          parentContainerName: batchParentContainerName,
          headersRaw,
          refererPath,
          folderId: normalizedBatchFolderPrefix || null,
          providers: selectedProviders.length > 0 ? selectedProviders : null,
          sessionId: currentSessionId,
          containerOffset: nextOffset,
          containerLimit: BATCH_DOWNLOAD_CHUNK_SIZE,
          timeBudgetMs: BATCH_REQUEST_BUDGET_MS
        });

        const data = result?.data || {};
        currentSessionId = data.sessionId || currentSessionId;
        setBatchSessionId(currentSessionId);

        if (!aggregateResult) {
          aggregateResult = {
            ...data,
            queued: [...(data.queued || [])],
            skipped: [...(data.skipped || [])],
            chainErrors: [...(data.chainErrors || [])],
            queuedCount: Number(data.queuedCount || 0),
            skippedCount: Number(data.skippedCount || 0)
          };
        } else {
          aggregateResult = {
            ...aggregateResult,
            ...data,
            sessionId: currentSessionId || aggregateResult.sessionId || null,
            queued: [
              ...(aggregateResult.queued || []),
              ...(data.queued || [])
            ],
            skipped: [
              ...(aggregateResult.skipped || []),
              ...(data.skipped || [])
            ],
            chainErrors: [...(data.chainErrors || aggregateResult.chainErrors || [])],
            queuedCount: Number(aggregateResult.queuedCount || 0) + Number(data.queuedCount || 0),
            skippedCount: Number(aggregateResult.skippedCount || 0) + Number(data.skippedCount || 0)
          };
        }

        const total = Number.isFinite(Number(data.totalContainers))
          ? Number(data.totalContainers)
          : Number.isFinite(Number(aggregateResult?.totalContainers))
            ? Number(aggregateResult.totalContainers)
            : null;
        const processedContainers = Number.isFinite(Number(data.nextContainerOffset))
          ? Number(data.nextContainerOffset)
          : (Number.isFinite(Number(data.totalContainers)) ? Number(data.totalContainers) : 0);

        setBatchQueueProgress({
          processed: processedContainers,
          total
        });
        setBatchResult(aggregateResult);

        hasMore = Boolean(data.hasMoreContainers);
        nextOffset = Number.isFinite(Number(data.nextContainerOffset))
          ? Number(data.nextContainerOffset)
          : 0;
      }

      setBatchQueueProgress(null);
      setBatchSessionId(currentSessionId);
      setBatchChain((prev) => prev || null);
    } catch (error) {
      if (!aggregateResult) {
        setBatchResult(null);
      }
      setBatchQueueProgress(null);
      console.error('Failed to queue zenius batch download:', error);
    }
  };

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-white mb-1">Zenius Downloader</h2>
        <p className="text-sm text-[#888]">
          Ambil instance details dari raw headers, download HLS dari `video-url` dengan FFmpeg, distribusikan ke storage, lalu hapus file lokal otomatis.
        </p>
      </div>

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
              onChange={(event) => setUrlShortId(event.target.value.replace(/[^0-9]/g, ''))}
              placeholder="79497"
              className="input"
              required
            />
          </div>

          <div>
            <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
              <FolderOpen className="w-4 h-4 text-[#666]" />
              Folder Path (optional)
            </label>
            <input
              type="text"
              value={folderId}
              onChange={(event) => handleFolderInputChange(event.target.value)}
              placeholder='kelas10/MTK wajib/Persamaan'
              className="input"
              list="zenius-folder-suggestions"
            />
            <datalist id="zenius-folder-suggestions">
              {folderOptions.map((item) => (
                <option key={item.id} value={item.path} />
              ))}
            </datalist>

            <div className="mt-2 space-y-1.5 text-xs">
              <p className="text-[#666]">
                Preview: <span className="text-[#d0d0d0] font-mono">{resolvedFolderPathPreview}</span>
              </p>
              <p className={folderExists ? 'text-emerald-400' : 'text-amber-400'}>
                {folderExists
                  ? 'Folder sudah ada.'
                  : 'Folder belum ada, akan dibuat otomatis saat download.'}
              </p>
              <p className="text-[#666]">Tidak perlu awalan `root/` karena default selalu di bawah root. Tanda kutip tetap didukung.</p>
              {folderSuggestions.length > 0 && (
                <div className="flex flex-wrap gap-1.5 pt-1">
                  {folderSuggestions.map((item) => (
                    <button
                      key={item.id}
                      type="button"
                      onClick={() => handleFolderInputChange(item.path)}
                      className="px-2 py-1 rounded bg-[#1c1c1c] hover:bg-[#262626] border border-[#2e2e2e] text-[#bfbfbf]"
                    >
                      {item.path}
                    </button>
                  ))}
                </div>
              )}

              <div className="pt-1">
                <button
                  type="button"
                  onClick={handleDeleteFolder}
                  disabled={!exactFolderMatch || deleteFolderMutation.isLoading}
                  className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded border border-red-500/40 text-red-300 hover:bg-red-500/10 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {deleteFolderMutation.isLoading ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Trash2 className="w-3.5 h-3.5" />
                  )}
                  Hapus Folder
                </button>
              </div>

              {deleteFolderMutation.isSuccess && (
                <p className="text-emerald-400">Folder berhasil dihapus.</p>
              )}

              {deleteFolderMutation.isError && (
                <p className="text-red-400">
                  {deleteFolderMutation.error?.response?.data?.error || deleteFolderMutation.error?.message || 'Gagal hapus folder'}
                </p>
              )}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="text-sm text-[#aaa] block mb-2">Filename Override (Optional)</label>
            <input
              type="text"
              value={filename}
              onChange={(event) => setFilename(event.target.value)}
              className="input"
              placeholder="fungsi-eksponen"
            />
            <p className="text-xs text-[#666] mt-1">Kosongkan untuk pakai nama dari response API.</p>
          </div>

          <div className="flex items-end">
            <p className="text-xs text-[#666]">
              File hasil download bersifat sementara dan akan dihapus setelah distribusi selesai.
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="text-sm text-[#aaa] block mb-2">Referer Path / URL (Optional)</label>
            <input
              type="text"
              value={refererPath}
              onChange={(event) => setRefererPath(event.target.value)}
              className="input"
              placeholder="/ci/79497/fungsi-eksponen"
            />
          </div>

          <div className="flex items-end">
            <p className="text-xs text-[#666]">
              Jika kosong, script akan pakai referer dari `path-url` response.
            </p>
          </div>
        </div>

        <div>
          <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
            <KeyRound className="w-4 h-4 text-[#666]" />
            Headers Zenius (raw)
          </label>
          <textarea
            value={headersRaw}
            onChange={(event) => setHeadersRaw(event.target.value)}
            rows={8}
            className="input font-mono text-xs"
            placeholder={`__anti-forgery-token\t"..."\n__Secure-next-auth.session-token\t"..."\nuser-agent\t"Mozilla/5.0 ..."\nsentry-trace\t"..."\nbaggage\t"..."`}
          />
          <p className="text-xs text-[#666] mt-1">
            Bisa paste format `key\t"value"`, `Key: value`, atau cookie string. Key non-header akan otomatis diparsing sebagai cookie.
          </p>
        </div>

        <ProviderSelector
          providers={providers}
          selectedProviders={selectedProviders}
          setSelectedProviders={setSelectedProviders}
        />

        <div className="flex flex-col sm:flex-row gap-3">
          <button
            type="submit"
            disabled={!urlShortId || isBusy}
            className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto"
          >
            {detailsMutation.isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Getting Details...
              </>
            ) : (
              <>
                <Search className="w-4 h-4" />
                Get Instance Details
              </>
            )}
          </button>

          <button
            type="button"
            onClick={handleDownload}
            disabled={!urlShortId || isBusy}
            className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto"
          >
            {downloadMutation.isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Queueing Download...
              </>
            ) : (
              <>
                <Download className="w-4 h-4" />
                Download & Distribusi
              </>
            )}
          </button>
        </div>
      </form>

      <form onSubmit={handleGetBatchChain} className="card p-5 space-y-4">
        <div>
          <h3 className="text-lg font-medium text-white">Batch Downloader (CGroup Chain)</h3>
          <p className="text-xs text-[#777] mt-1">
            Struktur ID, path, dan nama file mengikuti flow pada `test/get-cgroup-video-chain.js`.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="text-sm text-[#aaa] block mb-2">Root CGroup ID</label>
            <input
              type="text"
              value={batchRootCgId}
              onChange={(event) => setBatchRootCgId(event.target.value.replace(/[^0-9]/g, ''))}
              placeholder="34"
              className="input"
              required
            />
          </div>

          <div>
            <label className="text-sm text-[#aaa] block mb-2">Target CG Selector (Optional)</label>
            <input
              type="text"
              value={batchTargetCgSelector}
              onChange={(event) => setBatchTargetCgSelector(event.target.value)}
              placeholder="/cg/83067 atau 83067"
              className="input"
            />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="text-sm text-[#aaa] block mb-2">Parent Container Name</label>
            <input
              type="text"
              value={batchParentContainerName}
              onChange={(event) => setBatchParentContainerName(event.target.value)}
              placeholder="Pengantar Keterampilan Matematika Dasar"
              className="input"
              required
            />
          </div>

          <div>
            <label className="text-sm text-[#aaa] block mb-2">Folder Prefix (Optional)</label>
            <input
              type="text"
              value={batchFolderPrefix}
              onChange={(event) => setBatchFolderPrefix(event.target.value)}
              placeholder="kelas10"
              className="input"
            />
            <p className="text-xs text-[#666] mt-1">
              Jika diisi, path batch menjadi `prefix/{'{'}path dari chain{'}'}`.
            </p>
          </div>
        </div>

        <div className="flex flex-col sm:flex-row gap-3">
          <button
            type="submit"
            disabled={!batchRootCgId || !batchParentContainerName || isBatchBusy}
            className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto"
          >
            {batchChainMutation.isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Building Chain...
              </>
            ) : (
              <>
                <Search className="w-4 h-4" />
                Get Batch Chain
              </>
            )}
          </button>

          <button
            type="button"
            onClick={handleBatchDownload}
            disabled={!batchRootCgId || !batchParentContainerName || isBatchBusy}
            className="btn btn-primary flex items-center justify-center gap-2 sm:w-auto"
          >
            {batchDownloadMutation.isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Queueing Batch...
              </>
            ) : (
              <>
                <Download className="w-4 h-4" />
                Download Batch
              </>
            )}
          </button>
        </div>

        {batchBuildProgress && (
          <div className="card p-3 border-sky-500/30 bg-sky-500/5">
            <p className="text-xs text-sky-300">
              Build chain bertahap: {batchBuildProgress.processed} / {batchBuildProgress.total ?? '?'} container selesai diproses.
            </p>
          </div>
        )}

        {batchQueueProgress && (
          <div className="card p-3 border-emerald-500/30 bg-emerald-500/5">
            <p className="text-xs text-emerald-300">
              Queue batch bertahap: {batchQueueProgress.processed} / {batchQueueProgress.total ?? '?'} container sudah diproses.
            </p>
          </div>
        )}

        {batchChain && (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
              <div className="card p-3">
                <p className="text-[#666] mb-1">Leaf CG ID</p>
                <p className="text-white font-mono">{batchChain.leafCgId || '-'}</p>
              </div>
              <div className="card p-3">
                <p className="text-[#666] mb-1">Total Container</p>
                <p className="text-white">{batchChain.containerList?.totalContainers ?? 0}</p>
              </div>
              <div className="card p-3">
                <p className="text-[#666] mb-1">Total Video Instance</p>
                <p className="text-white">
                  {(batchChain.containerDetails || []).reduce((acc, item) => acc + (item.videoInstances?.length || 0), 0)}
                </p>
              </div>
            </div>

            {Array.isArray(batchChain.errors) && batchChain.errors.length > 0 && (
              <div className="card p-3 border-amber-500/30 bg-amber-500/5">
                <p className="text-xs text-amber-300">
                  Batch chain selesai dengan {batchChain.errors.length} error upstream. Data yang berhasil tetap ditampilkan.
                </p>
              </div>
            )}

            <div>
              <p className="text-sm text-[#aaa] mb-2">Preview Queue</p>
              <div className="max-h-72 overflow-auto rounded-lg border border-[#222] bg-[#0d0d0d]">
                <table className="w-full text-xs">
                  <thead className="bg-[#141414] text-[#8a8a8a] sticky top-0">
                    <tr>
                      <th className="text-left px-3 py-2">ID</th>
                      <th className="text-left px-3 py-2">Nama</th>
                      <th className="text-left px-3 py-2">Filename</th>
                      <th className="text-left px-3 py-2">Path</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(batchChain.containerDetails || []).flatMap((container) => (
                      (container.videoInstances || []).map((item) => (
                        <tr key={`${container.containerUrlShortId}-${item.urlShortId}`} className="border-t border-[#1e1e1e]">
                          <td className="px-3 py-2 text-[#c8c8c8] font-mono">{item.urlShortId}</td>
                          <td className="px-3 py-2 text-[#e2e2e2]">{item.metadata?.name || item.name || '-'}</td>
                          <td className="px-3 py-2 text-[#e2e2e2] font-mono">{item.outputName ? `${item.outputName}.mp4` : '-'}</td>
                          <td className="px-3 py-2 text-[#8aa6d8] font-mono">
                            {(normalizedBatchFolderPrefix
                              ? `${normalizedBatchFolderPrefix}/${item.path || ''}`
                              : (item.path || '')
                            ) || '-'}
                          </td>
                        </tr>
                      ))
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </form>

      {instanceValue && (
        <div className="card p-5 space-y-4">
          <div className="flex items-center gap-2">
            <Shield className="w-4 h-4 text-[#7d7d7d]" />
            <h3 className="text-lg font-medium text-white">Instance Details</h3>
            <span className={`text-xs px-2 py-0.5 rounded ${details?.ok ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'}`}>
              {details?.ok ? 'ok=true' : 'ok=false'}
            </span>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            <div className="card p-3">
              <p className="text-[#666] mb-1">Judul</p>
              <p className="text-white">{instanceValue.name || '-'}</p>
            </div>
            <div className="card p-3">
              <p className="text-[#666] mb-1">Durasi</p>
              <p className="text-white">{formatDuration(Number(instanceValue['duration-seconds']))}</p>
            </div>
            <div className="card p-3">
              <p className="text-[#666] mb-1">Parent</p>
              <p className="text-white">{instanceValue.parent?.name || '-'}</p>
            </div>
            <div className="card p-3">
              <p className="text-[#666] mb-1">Type / Hosting</p>
              <p className="text-white">{instanceValue.type || '-'} / {instanceValue['hosting-type'] || '-'}</p>
            </div>
          </div>

          <div>
            <p className="text-sm text-[#aaa] mb-2 flex items-center gap-2">
              <LinkIcon className="w-4 h-4 text-[#666]" />
              Video URL
            </p>
            <div className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222]">
              <p className="text-xs text-[#8aa6d8] break-all">{instanceValue['video-url'] || '-'}</p>
            </div>
          </div>

          <div>
            <p className="text-sm text-[#aaa] mb-2">Raw Response</p>
            <pre className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222] text-xs text-[#9da6b3] overflow-x-auto">
              {JSON.stringify(details?.body, null, 2)}
            </pre>
          </div>
        </div>
      )}

      {downloadMutation.isSuccess && (
        <div className="card p-4 flex items-center gap-3 border-emerald-500/30">
          <CheckCircle className="w-5 h-5 text-emerald-400" />
          <div>
            <p className="text-sm text-emerald-400 font-medium">Download queued</p>
            <p className="text-xs text-[#888]">Proses FFmpeg dan distribusi storage sudah masuk antrian. Pantau di halaman Jobs/Status.</p>
          </div>
        </div>
      )}

      {batchResult && (
        <div className="card p-4 flex items-center gap-3 border-emerald-500/30">
          <CheckCircle className="w-5 h-5 text-emerald-400" />
          <div>
            <p className="text-sm text-emerald-400 font-medium">Batch queued</p>
            <p className="text-xs text-[#888]">
              {batchResult.queuedCount || 0} video di-queue, {batchResult.skippedCount || 0} dilewati.
              Pantau progress di halaman Jobs/Status.
            </p>
          </div>
        </div>
      )}

      {(detailsMutation.isError || downloadMutation.isError || batchChainMutation.isError || batchDownloadMutation.isError) && (
        <div className="card p-4 flex items-center gap-3 border-red-500/30">
          <AlertCircle className="w-5 h-5 text-red-400" />
          <div>
            <p className="text-sm text-red-400 font-medium">Request failed</p>
            <p className="text-xs text-[#888] break-all">
              {detailsMutation.error?.response?.data?.error
                || downloadMutation.error?.response?.data?.error
                || batchChainMutation.error?.response?.data?.error
                || batchDownloadMutation.error?.response?.data?.error
                || detailsMutation.error?.message
                || downloadMutation.error?.message
                || batchChainMutation.error?.message
                || batchDownloadMutation.error?.message
                || 'Unknown error'}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
