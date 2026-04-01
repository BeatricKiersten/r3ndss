import React, { useMemo, useState } from 'react';
import { useCancelJob, useDeleteJob, useJobs, useQueueTransferJob } from '../hooks/api';
import { Trash2, Plus, Loader2, XCircle, CheckCircle2, Clock3, RefreshCw } from 'lucide-react';

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

  const queueTransfer = useQueueTransferJob();
  const cancelJob = useCancelJob();
  const deleteJob = useDeleteJob();

  const filters = useMemo(() => ({
    status: statusFilter || undefined,
    type: typeFilter || undefined,
    limit: 200
  }), [statusFilter, typeFilter]);

  const { data: jobs = [], isLoading, refetch, isFetching } = useJobs(filters);

  const handleSubmitTransfer = async (e) => {
    e.preventDefault();
    if (!sourceUrl.trim()) return;

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
            <option value="seekstreaming">seekstreaming</option>
            <option value="rclone">rclone-storage</option>
            <option value="voesx">voesx</option>
            <option value="catbox">catbox</option>
          </select>
          <button
            type="submit"
            disabled={queueTransfer.isLoading || !sourceUrl.trim()}
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
    </div>
  );
}
