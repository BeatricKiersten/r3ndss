import React, { useMemo, useState } from 'react';
import { useUploadHls, useUploadFile, useProviders } from '../hooks/api';
import {
  Upload,
  Link,
  Key,
  CheckCircle,
  AlertCircle,
  Loader2,
  Cloud,
  ChevronDown,
  ChevronRight,
  Film,
  FileVideo
} from 'lucide-react';
import { getProviderConfig } from '../config/providers';

function ProviderSelector({
  providers,
  selectedProviders,
  setSelectedProviders,
  showProviderSelect,
  setShowProviderSelect
}) {
  const enabledProviders = Object.entries(providers || {})
    .filter(([_, info]) => info.enabled)
    .map(([key]) => key);

  const toggleProvider = (key) => {
    setSelectedProviders((prev) => (
      prev.includes(key)
        ? prev.filter((item) => item !== key)
        : [...prev, key]
    ));
  };

  const selectAllProviders = () => {
    setSelectedProviders([...enabledProviders]);
  };

  const deselectAllProviders = () => {
    setSelectedProviders([]);
  };

  return (
    <div>
      <button
        type="button"
        onClick={() => setShowProviderSelect(!showProviderSelect)}
        className="w-full flex items-center justify-between text-sm text-[#aaa] hover:text-white"
      >
        <div className="flex items-center gap-2">
          <Cloud className="w-4 h-4 text-[#666]" />
          <span>Upload Providers</span>
          {selectedProviders.length > 0 ? (
            <span className="px-2 py-0.5 rounded bg-[#333] text-xs text-white">
              {selectedProviders.length} selected
            </span>
          ) : (
            <span className="text-xs text-[#666]">(All enabled)</span>
          )}
        </div>
        {showProviderSelect ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
      </button>

      {showProviderSelect && (
        <div className="mt-3 p-3 rounded-lg bg-[#0d0d0d] border border-[#222]">
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs text-[#666]">Select providers to upload to:</span>
            <div className="flex gap-2">
              <button type="button" onClick={selectAllProviders} className="text-xs text-[#888] hover:text-white">Select All</button>
              <span className="text-[#333]">|</span>
              <button type="button" onClick={deselectAllProviders} className="text-xs text-[#888] hover:text-white">Deselect All</button>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-2">
            {Object.entries(providers || {}).map(([key, info]) => {
              const config = getProviderConfig(key);
              const isEnabled = info.enabled;
              const isSelected = selectedProviders.includes(key);

              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => isEnabled && toggleProvider(key)}
                  disabled={!isEnabled}
                  className={`p-2 rounded-lg text-left transition-colors ${
                    !isEnabled
                      ? 'bg-[#1a1a1a] text-[#444] cursor-not-allowed opacity-50'
                      : isSelected
                        ? `${config.bgColor} border border-current ${config.color}`
                        : 'bg-[#1a1a1a] text-[#888] hover:bg-[#222] border border-[#222]'
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <span className={`w-3 h-3 rounded ${isSelected ? config.bgColor : 'bg-[#333]'}`}>
                      {isSelected && <CheckCircle className="w-3 h-3" />}
                    </span>
                    <span className="text-xs font-medium">{info.name || config.short}</span>
                  </div>
                </button>
              );
            })}
          </div>
          <p className="text-xs text-[#555] mt-2">
            {selectedProviders.length === 0
              ? 'All enabled providers will be used'
              : `Will upload to: ${selectedProviders.map((provider) => providers?.[provider]?.name || getProviderConfig(provider)?.short).join(', ')}`}
          </p>
        </div>
      )}
    </div>
  );
}

export default function UploadPage() {
  const [url, setUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [decryptionKey, setDecryptionKey] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [fileFilename, setFileFilename] = useState('');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showHlsProviders, setShowHlsProviders] = useState(false);
  const [showFileProviders, setShowFileProviders] = useState(false);
  const [selectedHlsProviders, setSelectedHlsProviders] = useState([]);
  const [selectedFileProviders, setSelectedFileProviders] = useState([]);

  const uploadHlsMutation = useUploadHls();
  const uploadFileMutation = useUploadFile();
  const { data: providers } = useProviders();

  const isBusy = uploadHlsMutation.isLoading || uploadFileMutation.isLoading;

  const selectedFileSummary = useMemo(() => {
    if (!selectedFile) return null;
    const sizeInMb = (selectedFile.size / (1024 * 1024)).toFixed(2);
    return `${selectedFile.name} - ${sizeInMb} MB`;
  }, [selectedFile]);

  const handleHlsSubmit = async (e) => {
    e.preventDefault();
    if (!url) return;

    const providersToUse = selectedHlsProviders.length > 0 ? selectedHlsProviders : null;

    try {
      await uploadHlsMutation.mutateAsync({
        url,
        filename: filename || undefined,
        decryptionKey: decryptionKey || undefined,
        providers: providersToUse
      });
      setUrl('');
      setFilename('');
      setDecryptionKey('');
      setSelectedHlsProviders([]);
    } catch (error) {
      console.error('HLS upload failed:', error);
    }
  };

  const handleFileSubmit = async (e) => {
    e.preventDefault();
    if (!selectedFile) return;

    const providersToUse = selectedFileProviders.length > 0 ? selectedFileProviders : null;

    try {
      await uploadFileMutation.mutateAsync({
        file: selectedFile,
        filename: fileFilename || undefined,
        providers: providersToUse
      });
      setSelectedFile(null);
      setFileFilename('');
      setSelectedFileProviders([]);
      const input = document.getElementById('direct-video-upload-input');
      if (input) input.value = '';
    } catch (error) {
      console.error('Direct upload failed:', error);
    }
  };

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-white mb-1">Upload & Backup</h2>
        <p className="text-sm text-[#888]">Unggah video biasa atau proses HLS, lalu backup dan sync ke provider yang dipilih.</p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <div className="card p-5">
          <div className="flex items-center gap-3 mb-5">
            <div className="w-10 h-10 rounded-xl bg-blue-500/10 border border-blue-500/20 flex items-center justify-center">
              <Link className="w-5 h-5 text-blue-400" />
            </div>
            <div>
              <h3 className="text-lg font-medium text-white">HLS Stream</h3>
              <p className="text-sm text-[#888]">Download playlist `.m3u8`, convert ke MP4, lalu sync ke storage.</p>
            </div>
          </div>

          <form onSubmit={handleHlsSubmit} className="space-y-4">
            <div>
              <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                <Link className="w-4 h-4 text-[#666]" />
                HLS Stream URL
              </label>
              <input
                type="url"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://example.com/stream/playlist.m3u8"
                className="input"
                required
              />
            </div>

            <div>
              <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                <span className="text-xs text-[#666]">Aa</span>
                Filename <span className="text-[#666]">(optional)</span>
              </label>
              <input
                type="text"
                value={filename}
                onChange={(e) => setFilename(e.target.value)}
                placeholder="my-video"
                className="input"
              />
              <p className="text-xs text-[#666] mt-1">.mp4 will be added automatically</p>
            </div>

            <ProviderSelector
              providers={providers}
              selectedProviders={selectedHlsProviders}
              setSelectedProviders={setSelectedHlsProviders}
              showProviderSelect={showHlsProviders}
              setShowProviderSelect={setShowHlsProviders}
            />

            <button
              type="button"
              onClick={() => setShowAdvanced(!showAdvanced)}
              className="text-sm text-[#888] hover:text-[#aaa] flex items-center gap-1"
            >
              <Key className="w-3 h-3" />
              {showAdvanced ? 'Hide' : 'Show'} decryption options
            </button>

            {showAdvanced && (
              <div className="p-3 rounded-lg bg-[#0d0d0d] border border-[#222]">
                <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                  <Key className="w-4 h-4 text-[#666]" />
                  Decryption Key
                </label>
                <input
                  type="text"
                  value={decryptionKey}
                  onChange={(e) => setDecryptionKey(e.target.value)}
                  placeholder="hex string or key URL"
                  className="input"
                />
                <p className="text-xs text-[#666] mt-2">For encrypted HLS streams only</p>
              </div>
            )}

            <button
              type="submit"
              disabled={uploadHlsMutation.isLoading || !url || isBusy}
              className="btn btn-primary w-full flex items-center justify-center gap-2"
            >
              {uploadHlsMutation.isLoading ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Processing HLS...
                </>
              ) : (
                <>
                  <Upload className="w-4 h-4" />
                  Start HLS Backup
                </>
              )}
            </button>
          </form>
        </div>

        <div className="card p-5">
          <div className="flex items-center gap-3 mb-5">
            <div className="w-10 h-10 rounded-xl bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center">
              <Film className="w-5 h-5 text-emerald-400" />
            </div>
            <div>
              <h3 className="text-lg font-medium text-white">Direct Video Upload</h3>
              <p className="text-sm text-[#888]">Upload file video biasa lalu langsung backup dan sync ke provider.</p>
            </div>
          </div>

          <form onSubmit={handleFileSubmit} className="space-y-4">
            <label
              htmlFor="direct-video-upload-input"
              className="block rounded-2xl border border-dashed border-[#2d2d2d] bg-gradient-to-br from-[#151515] to-[#0f0f0f] p-6 text-center cursor-pointer hover:border-[#3f3f3f] transition-colors"
            >
              <FileVideo className="w-8 h-8 text-[#7a7a7a] mx-auto mb-3" />
              <p className="text-sm font-medium text-white">Choose a video file</p>
              <p className="text-xs text-[#777] mt-1">MP4, MKV, MOV, AVI, WEBM, dan format video lain yang dikenali browser</p>
              {selectedFileSummary ? (
                <p className="text-xs text-emerald-400 mt-3">{selectedFileSummary}</p>
              ) : (
                <p className="text-xs text-[#666] mt-3">No file selected</p>
              )}
            </label>
            <input
              id="direct-video-upload-input"
              type="file"
              accept="video/*"
              className="hidden"
              onChange={(e) => setSelectedFile(e.target.files?.[0] || null)}
            />

            <div>
              <label className="flex items-center gap-2 text-sm text-[#aaa] mb-2">
                <span className="text-xs text-[#666]">Aa</span>
                Rename file <span className="text-[#666]">(optional)</span>
              </label>
              <input
                type="text"
                value={fileFilename}
                onChange={(e) => setFileFilename(e.target.value)}
                placeholder="movie-backup"
                className="input"
              />
              <p className="text-xs text-[#666] mt-1">Jika tanpa ekstensi, sistem akan pakai ekstensi asli file video.</p>
            </div>

            <ProviderSelector
              providers={providers}
              selectedProviders={selectedFileProviders}
              setSelectedProviders={setSelectedFileProviders}
              showProviderSelect={showFileProviders}
              setShowProviderSelect={setShowFileProviders}
            />

            <button
              type="submit"
              disabled={uploadFileMutation.isLoading || !selectedFile || isBusy}
              className="btn btn-primary w-full flex items-center justify-center gap-2"
            >
              {uploadFileMutation.isLoading ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Uploading Video...
                </>
              ) : (
                <>
                  <Upload className="w-4 h-4" />
                  Start Direct Backup
                </>
              )}
            </button>
          </form>
        </div>
      </div>

      {(uploadHlsMutation.isSuccess || uploadFileMutation.isSuccess) && (
        <div className="card p-4 flex items-center gap-3 border-green-500/30">
          <CheckCircle className="w-5 h-5 text-green-400" />
          <div>
            <p className="text-sm text-green-400 font-medium">Upload queued</p>
            <p className="text-xs text-[#888]">File sudah masuk ke antrian backup dan sync. Cek Status page untuk progress.</p>
          </div>
        </div>
      )}

      {(uploadHlsMutation.isError || uploadFileMutation.isError) && (
        <div className="card p-4 flex items-center gap-3 border-red-500/30">
          <AlertCircle className="w-5 h-5 text-red-400" />
          <div>
            <p className="text-sm text-red-400 font-medium">Failed to queue</p>
            <p className="text-xs text-[#888]">
              {uploadHlsMutation.error?.message || uploadFileMutation.error?.message || 'Unknown error'}
            </p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
        <div className="card p-4">
          <h4 className="text-sm font-medium text-[#ccc] mb-1">Direct Video</h4>
          <p className="text-xs text-[#888]">Upload file video langsung dari device</p>
        </div>
        <div className="card p-4">
          <h4 className="text-sm font-medium text-[#ccc] mb-1">HLS URL</h4>
          <p className="text-xs text-[#888]">Paste playlist `.m3u8` saat perlu convert</p>
        </div>
        <div className="card p-4">
          <h4 className="text-sm font-medium text-[#ccc] mb-1">Backup Sync</h4>
          <p className="text-xs text-[#888]">Semua file masuk ke flow backup provider yang sama</p>
        </div>
        <div className="card p-4">
          <h4 className="text-sm font-medium text-[#ccc] mb-1">Flexible Source</h4>
          <p className="text-xs text-[#888]">Reupload tetap memakai primary provider sebagai source utama</p>
        </div>
      </div>
    </div>
  );
}
