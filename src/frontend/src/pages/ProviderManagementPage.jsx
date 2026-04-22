import React, { useState } from 'react';
import {
  useProviders,
  useProvidersStatus,
  useToggleProvider,
  useSystemCheck,
  useLastCheck,
  usePrimaryProvider,
  useUpdatePrimaryProvider,
  useFiles,
  useReuploadToProvider,
  useCheckSelectedProviders,
  useProviderCheckSnapshots,
  useClearMissingProviderLinks
} from '../hooks/api';
import { useWebSocketStore } from '../store/websocketStore';
import {
  Cloud,
  CheckCircle,
  XCircle,
  RefreshCw,
  Clock,
  Upload,
  ChevronDown,
  ChevronRight,
  Zap,
  Calendar,
  ShieldCheck,
  Sparkles,
  Copy,
  Trash2
} from 'lucide-react';
import { getProviderConfig } from '../config/providers';
import { toast } from '../store/toastStore';

export default function ProviderManagementPage() {
  const [expandedProvider, setExpandedProvider] = useState(null);
  const [showCheckModal, setShowCheckModal] = useState(false);
  const [selectedCheckProviders, setSelectedCheckProviders] = useState([]);
  const [autoReuploadMissing, setAutoReuploadMissing] = useState(false);

  const { data: providers, isLoading: providersLoading } = useProviders();
  const { data: providersStatus, isLoading: statusLoading, refetch: refetchStatus } = useProvidersStatus();
  const { data: providerSnapshots } = useProviderCheckSnapshots();
  const { data: filesResponse } = useFiles(undefined, undefined, { limit: 200, refetchInterval: 30000 });
  const files = filesResponse?.data || [];
  const { data: lastCheck } = useLastCheck();
  const { data: primaryProviderData } = usePrimaryProvider();

  const toggleProvider = useToggleProvider();
  const updatePrimaryProvider = useUpdatePrimaryProvider();
  const systemCheck = useSystemCheck();
  const checkSelectedProviders = useCheckSelectedProviders();
  const reuploadToProvider = useReuploadToProvider();
  const clearMissingProviderLinks = useClearMissingProviderLinks();

  const { isConnected } = useWebSocketStore();

  const enabledProviderKeys = Object.entries(providers || {})
    .filter(([_, info]) => info.enabled)
    .map(([key]) => key);

  const allProviderKeys = Object.keys(providers || {});

  const isRunningCheck = systemCheck.isLoading || checkSelectedProviders.isLoading;
  const latestCheckResult = checkSelectedProviders.data || systemCheck.data;
  const primaryProvider = primaryProviderData?.primaryProvider || 'catbox';

  const handleToggleProvider = async (name, enabled) => {
    await toggleProvider.mutateAsync({ name, enabled });

    if (!enabled) {
      setSelectedCheckProviders((prev) => prev.filter((item) => item !== name));
    }
  };

  const handleSetPrimaryProvider = async (provider) => {
    if (!(providers?.[provider]?.enabled)) {
      return;
    }

    await updatePrimaryProvider.mutateAsync(provider);
  };

  const toggleSelectedProvider = (provider) => {
    if (!(providers?.[provider]?.enabled)) {
      return;
    }

    setSelectedCheckProviders((prev) => (
      prev.includes(provider)
        ? prev.filter((item) => item !== provider)
        : [...prev, provider]
    ));
  };

  const selectAllCheckProviders = () => {
    setSelectedCheckProviders([...enabledProviderKeys]);
  };

  const clearSelectedCheckProviders = () => {
    setSelectedCheckProviders([]);
  };

  const handleCheckNow = async () => {
    setShowCheckModal(true);

    const payload = {
      autoReuploadMissing,
      providers: selectedCheckProviders.length > 0 ? selectedCheckProviders : undefined
    };

    try {
      if (selectedCheckProviders.length > 0) {
        await checkSelectedProviders.mutateAsync(payload);
      } else {
        await systemCheck.mutateAsync(payload);
      }
      await refetchStatus();
    } finally {
      setTimeout(() => setShowCheckModal(false), 3500);
    }
  };

  const handleReupload = async (fileId, provider) => {
    if (!window.confirm(`Re-upload this file to ${getProviderConfig(provider)?.name || provider}?`)) return;
    await reuploadToProvider.mutateAsync({ fileId, provider });
  };

  const handleClearMissingLinks = async (provider) => {
    const providerName = getProviderConfig(provider)?.name || provider;
    const confirmed = window.confirm(`Hapus semua link stale untuk ${providerName}? Sistem akan cek ulang semua file dan hanya menghapus record yang status-nya completed tapi remote-nya sudah tidak ada.`);
    if (!confirmed) return;

    try {
      const result = await clearMissingProviderLinks.mutateAsync({
        provider,
        reason: `Bulk stale link cleanup requested by user for ${provider}`
      });

      toast.success(
        'Cleanup completed',
        `${result.cleared?.length || 0} link dihapus, ${result.failed?.length || 0} gagal, ${result.skipped?.length || 0} dilewati`
      );
      await refetchStatus();
    } catch (error) {
      toast.error('Cleanup failed', error?.response?.data?.error || error.message);
    }
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'Never';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getProviderFiles = (providerKey) => {
    if (!files) return [];
    return files.filter((file) => {
      const providerStatus = file.providers?.[providerKey];
      return providerStatus && providerStatus.status !== 'pending';
    });
  };

  const getStatusBadge = (status) => {
    if (status?.authenticated) {
      return (
        <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-green-400/10 text-green-400 text-xs">
          <CheckCircle className="w-3 h-3" />
          Healthy
        </span>
      );
    }

    return (
      <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-red-400/10 text-red-400 text-xs">
        <XCircle className="w-3 h-3" />
        Not Configured
      </span>
    );
  };

  const isLoading = providersLoading || statusLoading;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-white">Provider Management</h2>
          <p className="text-sm text-[#888]">Monitor health, run targeted checks, and re-upload per provider.</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => refetchStatus()}
            disabled={isLoading}
            className="btn flex items-center gap-2"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <button
            onClick={handleCheckNow}
            disabled={isRunningCheck}
            className="btn btn-primary flex items-center gap-2"
          >
            {isRunningCheck ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                Checking...
              </>
            ) : (
              <>
                <Zap className="w-4 h-4" />
                Check Now
              </>
            )}
          </button>
        </div>
      </div>

      <div className="card p-4 border-[#2f2f2f] bg-gradient-to-br from-[#171717] to-[#121212]">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="flex items-center gap-3">
            <Calendar className="w-5 h-5 text-[#8b8b8b]" />
            <div>
              <p className="text-xs uppercase tracking-wide text-[#666]">Last System Check</p>
              <p className="text-sm font-medium text-white">{formatDate(lastCheck?.lastCheck)}</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <ShieldCheck className="w-5 h-5 text-[#8b8b8b]" />
            <div>
              <p className="text-xs uppercase tracking-wide text-[#666]">Next Weekly Check</p>
              <p className="text-sm font-medium text-white">{formatDate(lastCheck?.nextScheduledCheck)}</p>
            </div>
          </div>
          <div className="flex items-center justify-between lg:justify-end gap-3">
            <div className="flex items-center gap-2">
              <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-400' : 'bg-red-400'}`} />
              <span className="text-xs text-[#888]">{isConnected ? 'Realtime Connected' : 'Realtime Offline'}</span>
            </div>
          </div>
        </div>
      </div>

      <div className="card p-4 space-y-4">
        <div className="flex flex-col gap-3 border-b border-[#222] pb-4">
          <div>
            <h3 className="text-sm font-medium text-white flex items-center gap-2">
              <Copy className="w-4 h-4 text-[#999]" />
              Primary Reupload Source
            </h3>
            <p className="text-xs text-[#777]">Re-upload akan download dulu dari provider primary. Default `catbox`.</p>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {allProviderKeys.map((provider) => {
              const conf = {
                ...getProviderConfig(provider),
                name: providers?.[provider]?.name || getProviderConfig(provider).name
              };
              const enabled = providers?.[provider]?.enabled;
              const selected = primaryProvider === provider;

              return (
                <button
                  key={`primary-${provider}`}
                  type="button"
                  disabled={!enabled || updatePrimaryProvider.isLoading}
                  onClick={() => handleSetPrimaryProvider(provider)}
                  className={`p-2.5 rounded-lg border transition-colors text-left ${
                    !enabled
                      ? 'bg-[#141414] border-[#1f1f1f] text-[#444] cursor-not-allowed'
                      : selected
                        ? `${conf.bgColor} ${conf.borderColor} ${conf.color}`
                        : 'bg-[#131313] border-[#262626] text-[#9a9a9a] hover:bg-[#1a1a1a]'
                  }`}
                >
                  <p className="text-xs font-semibold flex items-center justify-between gap-2">
                    <span>{conf.name}</span>
                    {selected ? <span className="text-[10px] uppercase tracking-wide">Primary</span> : null}
                  </p>
                  <p className="text-[11px] opacity-80 mt-0.5">{enabled ? (selected ? 'Dipakai untuk source reupload' : 'Set as primary') : 'Disabled'}</p>
                </button>
              );
            })}
          </div>
        </div>

        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium text-white flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-[#999]" />
              Check Scope
            </h3>
            <p className="text-xs text-[#777]">Select providers for Check Now. Empty selection means all enabled providers.</p>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <button type="button" onClick={selectAllCheckProviders} className="text-[#aaa] hover:text-white">Select Enabled</button>
            <span className="text-[#333]">|</span>
            <button type="button" onClick={clearSelectedCheckProviders} className="text-[#aaa] hover:text-white">Clear</button>
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
          {allProviderKeys.map((provider) => {
            const conf = {
              ...getProviderConfig(provider),
              name: providers?.[provider]?.name || getProviderConfig(provider).name
            };
            const enabled = providers?.[provider]?.enabled;
            const selected = selectedCheckProviders.includes(provider);

            return (
              <button
                key={provider}
                type="button"
                disabled={!enabled}
                onClick={() => toggleSelectedProvider(provider)}
                className={`p-2.5 rounded-lg border transition-colors text-left ${
                  !enabled
                    ? 'bg-[#141414] border-[#1f1f1f] text-[#444] cursor-not-allowed'
                    : selected
                      ? `${conf.bgColor} ${conf.borderColor} ${conf.color}`
                      : 'bg-[#131313] border-[#262626] text-[#9a9a9a] hover:bg-[#1a1a1a]'
                }`}
              >
                <p className="text-xs font-semibold">{conf.name}</p>
                <p className="text-[11px] opacity-80 mt-0.5">{enabled ? (selected ? 'Selected' : 'Click to select') : 'Disabled'}</p>
              </button>
            );
          })}
        </div>

        <label className="flex items-center gap-2 text-xs text-[#888]">
          <input
            type="checkbox"
            checked={autoReuploadMissing}
            onChange={(e) => setAutoReuploadMissing(e.target.checked)}
            className="accent-[#666]"
          />
          Auto re-upload missing files during check
        </label>
      </div>

      <div className="space-y-3">
        {Object.entries(providers || {}).map(([key, info]) => {
          const config = {
            ...getProviderConfig(key),
            name: info?.name || getProviderConfig(key).name
          };
          const status = providersStatus?.[key] || {};
          const snapshot = providerSnapshots?.[key] || null;
          const providerFiles = getProviderFiles(key);
          const isExpanded = expandedProvider === key;
          const completedCount = providerFiles.filter((file) => file.providers?.[key]?.status === 'completed').length;

          return (
            <div key={key} className="card overflow-hidden border-[#2a2a2a]">
              <button
                onClick={() => setExpandedProvider(isExpanded ? null : key)}
                className="w-full p-4 flex items-center justify-between hover:bg-[#1a1a1a] transition-colors"
              >
                <div className="flex items-center gap-4">
                  <div className={`w-12 h-12 rounded-xl ${config.bgColor} flex items-center justify-center border ${config.borderColor}`}>
                    <Cloud className={`w-6 h-6 ${config.color}`} />
                  </div>
                  <div className="text-left">
                    <div className="flex items-center gap-2">
                      <h3 className="font-medium text-white">{config.name}</h3>
                      {getStatusBadge(status)}
                    </div>
                    <p className="text-xs text-[#666] mt-0.5">{config.description}</p>
                    <p className="text-[11px] text-[#6f6f6f] mt-1">
                      Last provider check: {formatDate(snapshot?.checkedAt)}
                    </p>
                  </div>
                </div>

                <div className="flex items-center gap-4">
                  <div className="text-right hidden sm:block">
                    <p className="text-sm font-medium text-white">{providerFiles.length} files</p>
                    <p className="text-xs text-[#666]">{completedCount} completed</p>
                  </div>

                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleToggleProvider(key, !info.enabled);
                    }}
                    className={`relative w-12 h-6 rounded-full transition-colors ${
                      info.enabled ? 'bg-green-500' : 'bg-[#333]'
                    }`}
                    title={info.enabled ? 'Disable provider' : 'Enable provider'}
                  >
                    <span
                      className={`absolute top-1 left-1 w-4 h-4 rounded-full bg-white transition-transform ${
                        info.enabled ? 'translate-x-6' : ''
                      }`}
                    />
                  </button>

                  {isExpanded ? (
                    <ChevronDown className="w-5 h-5 text-[#666]" />
                  ) : (
                    <ChevronRight className="w-5 h-5 text-[#666]" />
                  )}
                </div>
              </button>

              {isExpanded && (
                <div className="border-t border-[#222] p-4 bg-[#101010] space-y-4">
                  <div className="flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        setSelectedCheckProviders([key]);
                        setTimeout(() => handleCheckNow(), 0);
                      }}
                      disabled={isRunningCheck || !info.enabled}
                      className="btn btn-primary flex items-center gap-2"
                    >
                      <ShieldCheck className="w-4 h-4" />
                      Check This Provider
                    </button>
                    <button
                      type="button"
                      onClick={() => toggleSelectedProvider(key)}
                      className="btn flex items-center gap-2"
                      disabled={!info.enabled}
                    >
                      {selectedCheckProviders.includes(key) ? 'Remove from batch check' : 'Add to batch check'}
                    </button>
                    <button
                      type="button"
                      onClick={() => handleClearMissingLinks(key)}
                      className="btn flex items-center gap-2"
                      disabled={clearMissingProviderLinks.isLoading}
                    >
                      {clearMissingProviderLinks.isLoading && clearMissingProviderLinks.variables?.provider === key ? (
                        <RefreshCw className="w-4 h-4 animate-spin" />
                      ) : (
                        <Trash2 className="w-4 h-4" />
                      )}
                      Clear Missing Links
                    </button>
                  </div>

                  <div>
                    <h4 className="text-xs font-medium text-[#666] uppercase mb-3">Files ({providerFiles.length})</h4>
                    {providerFiles.length === 0 ? (
                      <p className="text-sm text-[#555] text-center py-4">No files uploaded to this provider</p>
                    ) : (
                      <div className="space-y-2 max-h-72 overflow-y-auto">
                        {providerFiles.map((file) => {
                          const ps = file.providers[key];
                          return (
                            <div key={file.id} className="flex items-center gap-3 p-2 rounded bg-[#1a1a1a] border border-[#242424]">
                              <div className="flex-1 min-w-0">
                                <p className="text-sm text-[#ccc] truncate">{file.name}</p>
                                <p className="text-xs text-[#666]">{formatDate(file.updatedAt)}</p>
                              </div>
                              <div className="flex items-center gap-2">
                                {ps.status === 'completed' ? (
                                  <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-green-400/10 text-green-400 text-xs">
                                    <CheckCircle className="w-3 h-3" />
                                    Done
                                  </span>
                                ) : ps.status === 'failed' ? (
                                  <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-red-400/10 text-red-400 text-xs">
                                    <XCircle className="w-3 h-3" />
                                    Failed
                                  </span>
                                ) : ps.status === 'uploading' ? (
                                  <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-blue-400/10 text-blue-400 text-xs">
                                    <RefreshCw className="w-3 h-3 animate-spin" />
                                    Uploading
                                  </span>
                                ) : (
                                  <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-[#222] text-[#666] text-xs">
                                    <Clock className="w-3 h-3" />
                                    Pending
                                  </span>
                                )}
                                {(ps.status === 'failed' || ps.status === 'completed') && (
                                  <button
                                    onClick={() => handleReupload(file.id, key)}
                                    disabled={reuploadToProvider.isLoading}
                                    className="p-1.5 rounded hover:bg-[#333] text-[#888] hover:text-white transition-colors"
                                    title="Re-upload"
                                  >
                                    <Upload className="w-3.5 h-3.5" />
                                  </button>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {showCheckModal && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
          <div className="card p-6 max-w-lg w-full mx-4">
            <div className="flex items-center gap-3 mb-4">
              {isRunningCheck ? (
                <RefreshCw className="w-6 h-6 text-blue-400 animate-spin" />
              ) : (
                <CheckCircle className="w-6 h-6 text-green-400" />
              )}
              <h3 className="text-lg font-medium text-white">
                {isRunningCheck ? 'Running Provider Check...' : 'Check Complete'}
              </h3>
            </div>

            {latestCheckResult && (
              <div className="space-y-2 text-sm">
                <p className="text-[#888]">
                  Checked <span className="text-white">{latestCheckResult.checked || 0}</span> items
                </p>
                <p className="text-[#888]">
                  Checked Providers:{' '}
                  <span className="text-white">
                    {latestCheckResult.checkedProviders?.length
                      ? latestCheckResult.checkedProviders.map((item) => getProviderConfig(item)?.short || item).join(', ')
                      : 'All enabled'}
                  </span>
                </p>
                <p className="text-[#888]">
                  Missing files found:{' '}
                  <span className={(latestCheckResult.issues?.length || 0) > 0 ? 'text-red-400' : 'text-green-400'}>
                    {latestCheckResult.issues?.length || 0}
                  </span>
                </p>
                {(latestCheckResult.reuploadsQueued?.length || 0) > 0 && (
                  <p className="text-[#888]">
                    Re-uploads queued: <span className="text-blue-400">{latestCheckResult.reuploadsQueued.length}</span>
                  </p>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
