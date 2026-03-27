import React, { useEffect, useMemo, useState } from 'react';
import { AlertCircle, CheckCircle, Plus, RefreshCw, Save, Server, Trash2 } from 'lucide-react';
import { useRcloneConfig, useUpdateRcloneConfig, useValidateRclone } from '../hooks/api';

function createRemote() {
  return {
    name: '',
    type: '',
    parametersText: '{\n  "account": "",\n  "key": ""\n}'
  };
}

function createProfile() {
  return {
    id: `profile-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    name: '',
    provider: 'backblaze',
    remoteName: '',
    destinationPath: '',
    publicBaseUrl: '',
    enabled: true
  };
}

function parseJson(text) {
  try {
    const parsed = JSON.parse(text || '{}');
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return { value: parsed, error: null };
    }
    return { value: null, error: 'Parameters must be a JSON object' };
  } catch (error) {
    return { value: null, error: error.message };
  }
}

export default function RcloneConfigPage() {
  const { data, isLoading, refetch, isFetching } = useRcloneConfig();
  const updateRcloneConfig = useUpdateRcloneConfig();
  const validateRclone = useValidateRclone();

  const [remotes, setRemotes] = useState([]);
  const [syncProfiles, setSyncProfiles] = useState([]);
  const [defaultProfileId, setDefaultProfileId] = useState('');

  useEffect(() => {
    const incomingRemotes = (data?.remotes || []).map((remote) => ({
      name: remote.name || '',
      type: remote.type || '',
      parametersText: JSON.stringify(remote.parameters || {}, null, 2)
    }));

    setRemotes(incomingRemotes);
    setSyncProfiles(data?.syncProfiles || []);
    setDefaultProfileId(data?.defaultProfileId || '');
  }, [data]);

  const remoteNameOptions = useMemo(
    () => remotes.map((remote) => remote.name).filter(Boolean),
    [remotes]
  );

  const validationSummary = data?.lastValidation || null;

  const updateRemoteField = (index, key, value) => {
    setRemotes((prev) => prev.map((remote, i) => (i === index ? { ...remote, [key]: value } : remote)));
  };

  const updateProfileField = (index, key, value) => {
    setSyncProfiles((prev) => prev.map((profile, i) => (i === index ? { ...profile, [key]: value } : profile)));
  };

  const handleSave = async () => {
    const normalizedRemotes = [];
    const remoteErrors = [];

    remotes.forEach((remote, index) => {
      const parsed = parseJson(remote.parametersText);
      if (parsed.error) {
        remoteErrors.push(`Remote #${index + 1} JSON error: ${parsed.error}`);
        return;
      }

      normalizedRemotes.push({
        name: String(remote.name || '').trim(),
        type: String(remote.type || '').trim(),
        parameters: parsed.value || {}
      });
    });

    const duplicateRemote = normalizedRemotes.find((remote, idx) => (
      normalizedRemotes.findIndex((item) => item.name === remote.name) !== idx
    ));

    if (remoteErrors.length > 0) {
      alert(remoteErrors.join('\n'));
      return;
    }

    if (duplicateRemote) {
      alert(`Duplicate remote name detected: ${duplicateRemote.name}`);
      return;
    }

    const normalizedProfiles = syncProfiles.map((profile) => ({
      ...profile,
      id: String(profile.id || '').trim(),
      name: String(profile.name || '').trim(),
      provider: String(profile.provider || 'backblaze').trim(),
      remoteName: String(profile.remoteName || '').trim(),
      destinationPath: String(profile.destinationPath || '').trim(),
      publicBaseUrl: String(profile.publicBaseUrl || '').trim(),
      enabled: profile.enabled !== false
    }));

    await updateRcloneConfig.mutateAsync({
      remotes: normalizedRemotes,
      syncProfiles: normalizedProfiles,
      defaultProfileId: defaultProfileId || null
    });
  };

  const handleValidate = async () => {
    await validateRclone.mutateAsync();
  };

  if (isLoading) {
    return (
      <div className="space-y-4">
        <div className="h-6 w-40 bg-[#222] rounded animate-pulse" />
        <div className="card p-6 h-40 animate-pulse bg-[#1a1a1a]" />
      </div>
    );
  }

  return (
    <div className="space-y-5 max-w-6xl mx-auto">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-white">Rclone Configuration</h2>
          <p className="text-sm text-[#888]">Kelola remote dan sync profile untuk backbone provider Backblaze.</p>
        </div>
        <div className="flex items-center gap-2">
          <button type="button" onClick={() => refetch()} className="btn flex items-center gap-2" disabled={isFetching}>
            <RefreshCw className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <button type="button" onClick={handleValidate} className="btn flex items-center gap-2" disabled={validateRclone.isLoading}>
            <Server className="w-4 h-4" />
            {validateRclone.isLoading ? 'Validating...' : 'Validate'}
          </button>
          <button type="button" onClick={handleSave} className="btn btn-primary flex items-center gap-2" disabled={updateRcloneConfig.isLoading}>
            <Save className="w-4 h-4" />
            {updateRcloneConfig.isLoading ? 'Saving...' : 'Save Config'}
          </button>
        </div>
      </div>

      <div className="card p-4">
        <h3 className="text-sm font-medium text-white mb-3">Validation Status</h3>
        {validationSummary ? (
          <div className="flex items-start gap-2 text-sm">
            {validationSummary.authenticated ? (
              <CheckCircle className="w-4 h-4 text-green-400 mt-0.5" />
            ) : (
              <AlertCircle className="w-4 h-4 text-red-400 mt-0.5" />
            )}
            <div>
              <p className={validationSummary.authenticated ? 'text-green-400' : 'text-red-400'}>{validationSummary.message}</p>
              <p className="text-xs text-[#666] mt-1">Last checked: {data?.lastValidatedAt ? new Date(data.lastValidatedAt).toLocaleString() : 'Never'}</p>
            </div>
          </div>
        ) : (
          <p className="text-sm text-[#777]">Belum ada validasi. Jalankan Validate setelah konfigurasi disimpan.</p>
        )}
      </div>

      <div className="card p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium text-white">Remotes</h3>
          <button type="button" className="btn !py-1.5 !px-2 text-xs flex items-center gap-1" onClick={() => setRemotes((prev) => [...prev, createRemote()])}>
            <Plus className="w-3.5 h-3.5" /> Add Remote
          </button>
        </div>

        {remotes.length === 0 ? (
          <p className="text-sm text-[#777]">Belum ada remote. Tambahkan minimal satu remote untuk digunakan profile sync.</p>
        ) : (
          <div className="space-y-3">
            {remotes.map((remote, index) => (
              <div key={`remote-${index}`} className="p-3 rounded-lg bg-[#0f0f0f] border border-[#222] space-y-2">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                  <input
                    value={remote.name}
                    onChange={(e) => updateRemoteField(index, 'name', e.target.value)}
                    className="input"
                    placeholder="remote name (contoh: b2-main)"
                  />
                  <input
                    value={remote.type}
                    onChange={(e) => updateRemoteField(index, 'type', e.target.value)}
                    className="input"
                    placeholder="remote type (contoh: b2, s3, drive)"
                  />
                  <button
                    type="button"
                    className="btn !py-2 flex items-center justify-center gap-1 text-xs"
                    onClick={() => setRemotes((prev) => prev.filter((_, i) => i !== index))}
                  >
                    <Trash2 className="w-3.5 h-3.5" /> Remove
                  </button>
                </div>
                <textarea
                  value={remote.parametersText}
                  onChange={(e) => updateRemoteField(index, 'parametersText', e.target.value)}
                  className="input min-h-[120px] font-mono text-xs"
                  placeholder="JSON object untuk parameter remote"
                />
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="card p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium text-white">Sync Profiles</h3>
          <button type="button" className="btn !py-1.5 !px-2 text-xs flex items-center gap-1" onClick={() => setSyncProfiles((prev) => [...prev, createProfile()])}>
            <Plus className="w-3.5 h-3.5" /> Add Profile
          </button>
        </div>

        <div>
          <label className="text-xs text-[#777] block mb-1">Default Profile</label>
          <select value={defaultProfileId} onChange={(e) => setDefaultProfileId(e.target.value)} className="input md:max-w-md">
            <option value="">No default</option>
            {syncProfiles.map((profile) => (
              <option key={profile.id} value={profile.id}>{profile.name || profile.id}</option>
            ))}
          </select>
        </div>

        {syncProfiles.length === 0 ? (
          <p className="text-sm text-[#777]">Belum ada profile. Tambahkan profile untuk mapping provider ke remote.</p>
        ) : (
          <div className="space-y-3">
            {syncProfiles.map((profile, index) => (
              <div key={profile.id || `profile-${index}`} className="p-3 rounded-lg bg-[#0f0f0f] border border-[#222]">
                <div className="grid grid-cols-1 md:grid-cols-6 gap-2">
                  <input value={profile.name || ''} onChange={(e) => updateProfileField(index, 'name', e.target.value)} className="input" placeholder="profile name" />
                  <select value={profile.provider || 'backblaze'} onChange={(e) => updateProfileField(index, 'provider', e.target.value)} className="input">
                    <option value="backblaze">backblaze</option>
                  </select>
                  <select value={profile.remoteName || ''} onChange={(e) => updateProfileField(index, 'remoteName', e.target.value)} className="input">
                    <option value="">Select remote</option>
                    {remoteNameOptions.map((name) => (
                      <option key={name} value={name}>{name}</option>
                    ))}
                  </select>
                  <input value={profile.destinationPath || ''} onChange={(e) => updateProfileField(index, 'destinationPath', e.target.value)} className="input" placeholder="destination path" />
                  <input value={profile.publicBaseUrl || ''} onChange={(e) => updateProfileField(index, 'publicBaseUrl', e.target.value)} className="input" placeholder="public base URL (opsional)" />
                  <div className="flex items-center gap-2">
                    <label className="text-xs text-[#777] flex items-center gap-1">
                      <input type="checkbox" checked={profile.enabled !== false} onChange={(e) => updateProfileField(index, 'enabled', e.target.checked)} className="accent-[#666]" /> Enabled
                    </label>
                    <button type="button" className="btn !py-1 !px-2 text-xs" onClick={() => setSyncProfiles((prev) => prev.filter((_, i) => i !== index))}>
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
