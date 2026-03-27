import React, { useState } from 'react';
import { useDashboard, useStats, useProviders, useToggleProvider, useCopyToProvider } from '../hooks/api';
import { useWebSocketStore } from '../store/websocketStore';
import { Activity, Cloud, CheckCircle, Clock, Loader2, HardDrive, Server, Wifi, WifiOff, Copy, ChevronDown, ChevronUp, History, ExternalLink, ArrowRight } from 'lucide-react';
import { getProviderConfig } from '../config/providers';

export default function BackupStatusPage() {
  const { data: dashboard, isLoading } = useDashboard();
  const { data: stats } = useStats();
  const { data: providers } = useProviders();
  const toggleProvider = useToggleProvider();
  const copyToProvider = useCopyToProvider();
  const { events, isConnected } = useWebSocketStore();
  const [expandedFile, setExpandedFile] = useState(null);
  const [showCopyDropdown, setShowCopyDropdown] = useState(null);

  const recentEvents = events?.slice(0, 8) || [];

  function handleCopyToProvider(fileId, targetProvider) {
    copyToProvider.mutate({ fileId, targetProvider });
    setShowCopyDropdown(null);
  }

  function getEnabledProviders() {
    return Object.entries(providers || {})
      .filter(([_, info]) => info.enabled)
      .map(([name]) => name);
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-white">System Status</h2>
          <p className="text-sm text-[#888]">Real-time monitoring</p>
        </div>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-[#1a1a1a] border border-[#222]">
          {isConnected ? (
            <>
              <Wifi className="w-3.5 h-3.5 text-green-400" />
              <span className="text-xs text-green-400">Online</span>
            </>
          ) : (
            <>
              <WifiOff className="w-3.5 h-3.5 text-red-400" />
              <span className="text-xs text-red-400">Offline</span>
            </>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard label="Total Files" value={stats?.files?.total || 0} />
        <StatCard label="Processing" value={stats?.files?.byStatus?.processing || 0} />
        <StatCard label="Uploading" value={stats?.files?.byStatus?.uploading || 0} />
        <StatCard label="Completed" value={stats?.files?.byStatus?.completed || 0} />
      </div>

      <div className="grid lg:grid-cols-2 gap-5">
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-medium text-[#ccc]">Providers</h3>
            <span className="text-xs text-[#666]">Click to toggle</span>
          </div>
          <div className="grid grid-cols-2 gap-2">
            {Object.entries(providers || {}).map(([name, info]) => {
              const config = getProviderConfig(name);
              return (
                <button
                  key={name}
                  onClick={() => toggleProvider.mutate({ name, enabled: !info.enabled })}
                  className={`p-3 rounded-lg text-left transition-colors ${
                    info.enabled ? 'bg-[#333] text-white' : 'bg-[#1a1a1a] text-[#666] hover:bg-[#222]'
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-medium text-sm">{config?.short || name}</span>
                    <span className={`w-2 h-2 rounded-full ${info.enabled ? 'bg-green-400' : 'bg-[#444]'}`} />
                  </div>
                  <p className="text-xs opacity-70">{info.enabled ? 'Active' : 'Disabled'}</p>
                </button>
              );
            })}
          </div>
        </div>

        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-medium text-[#ccc]">Active Jobs</h3>
            <span className="text-xs text-[#666]">{dashboard?.activeJobs?.length || 0} running</span>
          </div>

          {isLoading ? (
            <div className="flex items-center justify-center py-6">
              <Loader2 className="w-5 h-5 animate-spin text-[#666]" />
            </div>
          ) : !dashboard?.activeJobs?.length ? (
            <div className="text-center py-6 text-[#666]">
              <Clock className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p className="text-sm">No active jobs</p>
            </div>
          ) : (
            <div className="space-y-2">
              {dashboard.activeJobs.map(job => (
                <div key={job.id} className="p-3 rounded-lg bg-[#0d0d0d]">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <div className="w-6 h-6 rounded bg-[#222] flex items-center justify-center">
                        {job.type === 'process' ? (
                          <Activity className="w-3 h-3 text-[#888]" />
                        ) : (
                          <Cloud className="w-3 h-3 text-[#888]" />
                        )}
                      </div>
                      <div>
                        <p className="text-xs text-[#ccc]">
                          {job.type === 'process' ? 'FFmpeg' : `Upload to ${job.provider}`}
                        </p>
                      </div>
                    </div>
                    <span className="text-xs font-medium text-[#888]">{job.progress || 0}%</span>
                  </div>
                  <div className="progress-bar">
                    <div className="progress-bar-fill" style={{ width: `${job.progress || 0}%` }} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="card p-5">
          <h3 className="text-sm font-medium text-[#ccc] mb-4">Recent Files</h3>

          {!dashboard?.recentFiles?.length ? (
            <div className="text-center py-6 text-[#666]">
              <HardDrive className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p className="text-sm">No files yet</p>
            </div>
          ) : (
            <div className="space-y-1">
              {dashboard.recentFiles.slice(0, 8).map(file => {
                const isExpanded = expandedFile === file.id;
                return (
                  <div key={file.id} className="rounded-lg overflow-hidden">
                    <button
                      onClick={() => setExpandedFile(isExpanded ? null : file.id)}
                      className="w-full flex items-center gap-3 p-2 rounded-lg hover:bg-[#0d0d0d] text-left"
                    >
                      <div className="w-8 h-8 rounded bg-[#222] flex items-center justify-center flex-shrink-0">
                        <HardDrive className="w-4 h-4 text-[#666]" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-xs text-[#ccc] truncate">{file.name}</p>
                        <div className="flex gap-1 mt-0.5">
                          {Object.entries(file.providers).map(([p, s]) => (
                            <span
                              key={p}
                              className={`w-4 h-4 rounded text-[8px] flex items-center justify-center ${
                                s.status === 'completed' ? 'bg-green-400/10 text-green-400' :
                                s.status === 'uploading' || s.status === 'processing' ? 'bg-yellow-400/10 text-yellow-400' :
                                s.status === 'failed' ? 'bg-red-400/10 text-red-400' :
                                'bg-[#222] text-[#666]'
                              }`}
                              title={`${getProviderConfig(p)?.name || p}: ${s.status}`}
                            >
                              {getProviderConfig(p)?.short?.[0] || p[0].toUpperCase()}
                            </span>
                          ))}
                        </div>
                      </div>
                      <div className="flex items-center gap-2 flex-shrink-0">
                        <span className={`text-xs font-medium ${file.syncStatus === 100 ? 'text-green-400' : 'text-[#888]'}`}>
                          {file.syncStatus}%
                        </span>
                        {isExpanded
                          ? <ChevronUp className="w-3 h-3 text-[#666]" />
                          : <ChevronDown className="w-3 h-3 text-[#666]" />
                        }
                      </div>
                    </button>

                    {isExpanded && (
                      <div className="mx-2 mb-2 p-3 rounded-lg bg-[#0d0d0d] space-y-2">
                        {Object.entries(file.providers).map(([p, s]) => {
                          const cfg = getProviderConfig(p);
                          const canCopy = s.status !== 'completed' && s.status !== 'uploading' && s.status !== 'processing';
                          const isCopying = copyToProvider.isLoading && copyToProvider.variables?.fileId === file.id && copyToProvider.variables?.targetProvider === p;
                          return (
                            <div key={p} className="space-y-1.5">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-medium text-[#ccc]">{cfg?.label || p}</span>
                                  <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                                    s.status === 'completed' ? 'bg-green-400/10 text-green-400' :
                                    s.status === 'uploading' || s.status === 'processing' ? 'bg-yellow-400/10 text-yellow-400' :
                                    s.status === 'failed' ? 'bg-red-400/10 text-red-400' :
                                    'bg-[#222] text-[#666]'
                                  }`}>
                                    {s.status}
                                  </span>
                                </div>
                                <div className="flex items-center gap-2">
                                  {s.url && (
                                    <a
                                      href={s.url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      onClick={e => e.stopPropagation()}
                                      className="text-[#666] hover:text-[#aaa]"
                                      title="Open current URL"
                                    >
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  )}
                                  {canCopy && (
                                    <button
                                      onClick={e => { e.stopPropagation(); handleCopyToProvider(file.id, p); }}
                                      disabled={isCopying}
                                      className="flex items-center gap-1 px-2 py-0.5 rounded text-[10px] bg-[#222] text-[#aaa] hover:bg-[#333] hover:text-white disabled:opacity-50 transition-colors"
                                      title={`Copy to ${cfg?.label || p}`}
                                    >
                                      {isCopying
                                        ? <Loader2 className="w-2.5 h-2.5 animate-spin" />
                                        : <Copy className="w-2.5 h-2.5" />
                                      }
                                      {isCopying ? 'Copying...' : 'Copy here'}
                                    </button>
                                  )}
                                </div>
                              </div>
                              {s.url && (
                                <p className="text-[10px] text-[#555] font-mono truncate">{s.url}</p>
                              )}
                              {s.urlHistory?.length > 0 && (
                                <div className="pl-2 border-l border-[#222] space-y-1">
                                  <div className="flex items-center gap-1 text-[10px] text-[#555]">
                                    <History className="w-2.5 h-2.5" />
                                    <span>Previous URLs</span>
                                  </div>
                                  {s.urlHistory.map((h, i) => (
                                    <div key={i} className="flex items-center gap-2">
                                      <p className="text-[10px] text-[#444] font-mono truncate flex-1">{h.url}</p>
                                      <a
                                        href={h.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        onClick={e => e.stopPropagation()}
                                        className="text-[#444] hover:text-[#666] flex-shrink-0"
                                      >
                                        <ExternalLink className="w-2.5 h-2.5" />
                                      </a>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          );
                        })}

                        {/* Copy to another provider section */}
                        <div className="pt-2 mt-2 border-t border-[#222]">
                          <div className="flex items-center gap-2">
                            <span className="text-[10px] text-[#666]">Copy to:</span>
                            <div className="flex gap-1">
                              {getEnabledProviders().map(targetProvider => {
                                const targetCfg = getProviderConfig(targetProvider);
                                const targetStatus = file.providers?.[targetProvider];
                                const alreadyCompleted = targetStatus?.status === 'completed';
                                const isCopying = copyToProvider.isLoading && copyToProvider.variables?.fileId === file.id && copyToProvider.variables?.targetProvider === targetProvider;

                                return (
                                  <button
                                    key={targetProvider}
                                    onClick={e => { e.stopPropagation(); handleCopyToProvider(file.id, targetProvider); }}
                                    disabled={isCopying || alreadyCompleted}
                                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-colors ${
                                      alreadyCompleted
                                        ? 'bg-green-400/10 text-green-400 cursor-not-allowed'
                                        : isCopying
                                          ? 'bg-[#333] text-[#888] cursor-wait'
                                          : 'bg-[#1a1a1a] text-[#888] hover:bg-[#333] hover:text-white'
                                    }`}
                                    title={alreadyCompleted ? 'Already uploaded' : `Copy to ${targetCfg?.label || targetProvider}`}
                                  >
                                    {isCopying ? (
                                      <Loader2 className="w-2.5 h-2.5 animate-spin" />
                                    ) : alreadyCompleted ? (
                                      <CheckCircle className="w-2.5 h-2.5" />
                                    ) : (
                                      <ArrowRight className="w-2.5 h-2.5" />
                                    )}
                                    {targetCfg?.short || targetProvider}
                                  </button>
                                );
                              })}
                            </div>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <div className="card p-5">
          <h3 className="text-sm font-medium text-[#ccc] mb-4">Activity Log</h3>
          <div className="max-h-48 overflow-y-auto space-y-1">
            {!recentEvents.length ? (
              <div className="text-center py-6 text-[#666]">
                <Activity className="w-8 h-8 mx-auto mb-2 opacity-50" />
                <p className="text-sm">No recent activity</p>
              </div>
            ) : (
              recentEvents.map((e, i) => (
                <div key={i} className="flex items-start gap-2 p-1.5 rounded hover:bg-[#0d0d0d]">
                  <span className="text-[10px] text-[#666] font-mono mt-0.5">
                    {new Date(e.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                  <span className={`text-xs px-1.5 py-0.5 rounded ${
                    e.event.includes('completed') ? 'bg-green-400/10 text-green-400' :
                    e.event.includes('failed') ? 'bg-red-400/10 text-red-400' :
                    'bg-[#222] text-[#888]'
                  }`}>
                    {e.event.replace('upload:', '').replace('job:', '')}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div className="card p-4">
      <p className="text-2xl font-semibold text-white">{value}</p>
      <p className="text-xs text-[#888]">{label}</p>
    </div>
  );
}
