import React, { useEffect, useMemo, useState } from 'react';
import { useCancelJob, useCancelAllJobs, useClearJobLogs, useDeleteJob, useJobs, useProviders, useQueueTransferJob, useWipeAllJobs } from '../hooks/api';
import { Trash2, Plus, Loader2, XCircle, CheckCircle2, Clock3, RefreshCw, AlertOctagon, Eraser, Bomb } from 'lucide-react';
import { ConfirmDialog } from '../components/ui/ConfirmDialog';

const JOB_STATUS_OPTIONS = ['', 'pending', 'processing', 'completed', 'failed', 'cancelled'];

function statusClass(status) {
  if (status === 'completed') return 'bg-green-400/10 text-green-400';
  if (status === 'failed') return 'bg-red-400/10 text-red-400';
  if (status === 'processing') return 'bg-blue-400/10 text-blue-400';
  if (status === 'cancelled') return 'bg-orange-400/10 text-orange-400';
  return 'bg-[#222] text-[#888]';
}

function formatDate(value) {
  if (!value) return '-';
  return new Date(value).toLocaleString();
}

export default function JobsPage() {
  const [sourceUrl, setSourceUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [targetProvider, setTargetProvider] = useState('seekstreaming');
  const [statusFilter, setStatusFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [showCancelAllConfirm, setShowCancelAllConfirm] = useState(false);
  const [showClearLogsConfirm, setShowClearLogsConfirm] = useState(false);
  const [showWipeAllConfirm, setShowWipeAllConfirm] = useState(false);

  const queueTransfer = useQueueTransferJob();
  const cancelJob = useCancelJob();
  const cancelAllJobs = useCancelAllJobs();
  const clearJobLogs = useClearJobLogs();
  const wipeAllJobs = useWipeAllJobs();
  const deleteJob = useDeleteJob();
  const { data: providers = {} } = useProviders();

  const enabledProviders = useMemo(
    () => Object.entries(providers).filter(([_, info]) => info.enabled).map(([id]) => id),
    [providers]
  );

  useEffect(() => {
    if (enabledProviders.length === 0) return;
    if (!enabledProviders.includes(targetProvider)) {
      setTargetProvider(enabledProviders[0]);
    }
  }, [enabledProviders, targetProvider]);

  const filters = useMemo(() => ({
    status: statusFilter || undefined,
    type: typeFilter || undefined,
    limit: 200
  }), [statusFilter, typeFilter]);

  const { data: jobs = [], isLoading, refetch, isFetching } = useJobs(filters);

  const handleSubmitTransfer = async (e) => {
    e.preventDefault();
    if (!sourceUrl.trim() || !targetProvider) return;

    await queueTransfer.mutateAsync({
      sourceUrl: sourceUrl.trim(),
      filename: filename.trim() || null,
      targetProvider
    });

    setSourceUrl('');
    setFilename('');
  };

  const handleCancel = async (job) => {
    if (!window.confirm(`Cancel job ${job.id}?`)) return;
    await cancelJob.mutateAsync(job.id);
  };

  const handleDelete = async (job) => {
    if (!window.confirm(`Delete job ${job.id}?`)) return;
    await deleteJob.mutateAsync(job.id);
  };

  const handleCancelAll = async () => {
    await cancelAllJobs.mutateAsync();
    setShowCancelAllConfirm(false);
  };

  const handleClearLogs = async () => {
    await clearJobLogs.mutateAsync();
    setShowClearLogsConfirm(false);
  };

  const handleWipeAll = async () => {
    await wipeAllJobs.mutateAsync();
    setShowWipeAllConfirm(false);
  };

  // Count active jobs
  const activeJobsCount = useMemo(() => {
    return jobs.filter(job => job.status === 'pending' || job.status === 'processing').length;
  }, [jobs]);

  return (
    <div className="space-y-5 max-w-6xl mx-auto">
      <div>
        <h2 className="text-xl font-semibold text-white mb-1">Jobs</h2>
        <p className="text-sm text-[#888]">Create transfer jobs and remove old jobs</p>
      </div>

      <div className="card p-5">
        <div className="flex items-center gap-2 mb-4">
          <Plus className="w-4 h-4 text-[#888]" />
          <h3 className="text-sm font-medium text-[#ccc]">Create Transfer Job</h3>
        </div>
        <form onSubmit={handleSubmitTransfer} className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <input
            type="url"
            value={sourceUrl}
            onChange={(e) => setSourceUrl(e.target.value)}
            placeholder="https://files.catbox.moe/xxxxxx.mp4"
            className="input md:col-span-2"
            required
          />
          <input
            type="text"
            value={filename}
            onChange={(e) => setFilename(e.target.value)}
            placeholder="filename optional"
            className="input"
          />
          <select
            value={targetProvider}
            onChange={(e) => setTargetProvider(e.target.value)}
            className="input"
          >
            {enabledProviders.map((providerId) => (
              <option key={providerId} value={providerId}>{providers?.[providerId]?.name || providerId}</option>
            ))}
          </select>
          <button
            type="submit"
            disabled={queueTransfer.isLoading || !sourceUrl.trim() || !targetProvider}
            className="btn btn-primary md:col-span-4 flex items-center justify-center gap-2"
          >
            {queueTransfer.isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
            Queue Transfer
          </button>
        </form>
      </div>

      <div className="card p-5">
        <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
          <h3 className="text-sm font-medium text-[#ccc]">Job List</h3>
          <div className="flex items-center gap-2">
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} className="input !py-1.5 !text-xs">
              {JOB_STATUS_OPTIONS.map((s) => (
                <option key={s || 'all'} value={s}>{s || 'all status'}</option>
              ))}
            </select>
            <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)} className="input !py-1.5 !text-xs">
              <option value="">all type</option>
              <option value="process">process</option>
              <option value="upload">upload</option>
              <option value="transfer">transfer</option>
            </select>
            <button onClick={() => refetch()} className="btn !py-1.5 !px-2" type="button">
              <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin' : ''}`} />
            </button>
            {activeJobsCount > 0 && (
              <button
                onClick={() => setShowCancelAllConfirm(true)}
                disabled={cancelAllJobs.isLoading}
                className="btn !py-1.5 !px-2 bg-red-500/20 text-red-400 hover:bg-red-500/30 border-red-500/40"
                type="button"
                title={`Cancel all ${activeJobsCount} running jobs`}
              >
                {cancelAllJobs.isLoading ? (
                  <Loader2 className="w-3.5 h-3.5 animate-spin" />
                ) : (
                  <AlertOctagon className="w-3.5 h-3.5" />
                )}
                <span className="ml-1 text-xs">Stop All ({activeJobsCount})</span>
              </button>
            )}
            <button
              onClick={() => setShowClearLogsConfirm(true)}
              disabled={clearJobLogs.isLoading}
              className="btn !py-1.5 !px-2 bg-orange-500/20 text-orange-400 hover:bg-orange-500/30 border-orange-500/40"
              type="button"
              title="Clear completed/failed/cancelled job logs"
            >
              {clearJobLogs.isLoading ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Eraser className="w-3.5 h-3.5" />
              )}
              <span className="ml-1 text-xs">Clear Logs</span>
            </button>
            <button
              onClick={() => setShowWipeAllConfirm(true)}
              disabled={wipeAllJobs.isLoading}
              className="btn !py-1.5 !px-2 bg-red-600/20 text-red-300 hover:bg-red-600/30 border-red-600/40"
              type="button"
              title="Delete every job and log entry"
            >
              {wipeAllJobs.isLoading ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Bomb className="w-3.5 h-3.5" />
              )}
              <span className="ml-1 text-xs">Wipe All Jobs</span>
            </button>
          </div>
        </div>

        {isLoading ? (
          <div className="py-8 text-center text-[#666]">
            <Loader2 className="w-5 h-5 animate-spin mx-auto mb-2" />
            Loading jobs...
          </div>
        ) : !jobs.length ? (
          <div className="py-8 text-center text-[#666]">No jobs found</div>
        ) : (
          <div className="space-y-2">
            {jobs.map((job) => {
              const isActive = job.status === 'pending' || job.status === 'processing';
              const deleting = deleteJob.isLoading && deleteJob.variables === job.id;
              const cancelling = cancelJob.isLoading && cancelJob.variables === job.id;

              return (
                <div key={job.id} className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222]">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="min-w-0">
                      <p className="text-xs text-[#aaa] font-mono truncate">{job.id}</p>
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-xs text-[#666]">{job.type}</span>
                        <span className={`text-[10px] px-1.5 py-0.5 rounded ${statusClass(job.status)}`}>
                          {job.status}
                        </span>
                        <span className="text-[10px] text-[#666]">{job.progress || 0}%</span>
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      {isActive ? (
                        <button
                          type="button"
                          onClick={() => handleCancel(job)}
                          disabled={cancelling}
                          className="btn !py-1 !px-2 text-xs flex items-center gap-1"
                        >
                          {cancelling ? <Loader2 className="w-3 h-3 animate-spin" /> : <XCircle className="w-3 h-3" />}
                          Cancel
                        </button>
                      ) : (
                        <button
                          type="button"
                          onClick={() => handleDelete(job)}
                          disabled={deleting}
                          className="btn !py-1 !px-2 text-xs flex items-center gap-1"
                        >
                          {deleting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                          Delete
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="mt-2 grid grid-cols-1 md:grid-cols-3 gap-2 text-[11px] text-[#666]">
                    <div>attempts: {job.attempts || 0}/{job.maxAttempts || 0}</div>
                    <div>created: {formatDate(job.createdAt)}</div>
                    <div>updated: {formatDate(job.updatedAt)}</div>
                  </div>

                  {job.error && (
                    <div className="mt-2 text-xs text-red-400">error: {job.error}</div>
                  )}

                  {job.metadata?.sourceUrl && (
                    <div className="mt-2 text-[11px] text-[#777] break-all">source: {job.metadata.sourceUrl}</div>
                  )}

                  {job.status === 'completed' ? (
                    <div className="mt-2 flex items-center gap-1 text-[11px] text-green-400">
                      <CheckCircle2 className="w-3 h-3" /> completed
                    </div>
                  ) : isActive ? (
                    <div className="mt-2 flex items-center gap-1 text-[11px] text-blue-400">
                      <Clock3 className="w-3 h-3" /> running
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Cancel All Confirmation Dialog */}
      <ConfirmDialog
        isOpen={showCancelAllConfirm}
        onClose={() => setShowCancelAllConfirm(false)}
        onConfirm={handleCancelAll}
        title="Cancel All Running Jobs"
        message={`This will cancel all ${activeJobsCount} running jobs (pending and processing). This action cannot be undone. Are you sure?`}
        confirmText="Cancel All"
        variant="danger"
      />

      {/* Success/Error Messages */}
      {cancelAllJobs.isSuccess && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-emerald-500/40 text-emerald-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Cancelled {cancelAllJobs.data?.data?.cancelled} jobs successfully
        </div>
      )}
      {cancelAllJobs.isError && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-red-500/40 text-red-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Failed to cancel jobs: {cancelAllJobs.error?.message}
        </div>
      )}

      {/* Clear Logs Confirmation Dialog */}
      <ConfirmDialog
        isOpen={showClearLogsConfirm}
        onClose={() => setShowClearLogsConfirm(false)}
        onConfirm={handleClearLogs}
        title="Clear Job Logs"
        message="This will permanently delete all completed, failed, and cancelled jobs from the database. Active jobs (pending/processing) will not be affected. Are you sure?"
        confirmText="Clear Logs"
        variant="warning"
      />

      <ConfirmDialog
        isOpen={showWipeAllConfirm}
        onClose={() => setShowWipeAllConfirm(false)}
        onConfirm={handleWipeAll}
        title="Wipe All Jobs"
        message="This will abort active work where possible and permanently delete every row in the jobs table, including pending, processing, completed, failed, and cancelled jobs. This action cannot be undone. Are you sure?"
        confirmText="Wipe All"
        variant="danger"
      />

      {/* Clear Logs Success/Error Messages */}
      {clearJobLogs.isSuccess && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-emerald-500/40 text-emerald-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Cleared {clearJobLogs.data?.data?.deletedCount} job logs successfully
        </div>
      )}
      {clearJobLogs.isError && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-red-500/40 text-red-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Failed to clear logs: {clearJobLogs.error?.message}
        </div>
      )}

      {wipeAllJobs.isSuccess && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-emerald-500/40 text-emerald-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Wiped {wipeAllJobs.data?.data?.deletedCount} jobs successfully
        </div>
      )}
      {wipeAllJobs.isError && (
        <div className="fixed bottom-4 right-4 bg-[#1a1a1a] border border-red-500/40 text-red-400 px-4 py-3 rounded-lg shadow-lg z-50">
          Failed to wipe jobs: {wipeAllJobs.error?.message}
        </div>
      )}
    </div>
  );
}
