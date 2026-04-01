import React, { useEffect, useRef, useState } from 'react';
import videojs from 'video.js';
import 'video.js/dist/video-js.css';
import { usePlayerStore, IFRAME_PROVIDERS } from '../store/websocketStore';
import { useFiles, useReuploadToProvider } from '../hooks/api';
import { Play, CheckCircle, AlertCircle, Clock, HardDrive, Cloud, ChevronLeft, ExternalLink, AlertTriangle, Package, Film, Upload, RefreshCw, X, Server } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { PROVIDERS, getProviderConfig } from '../config/providers';

const providerIconMap = {
  Cloud,
  Play,
  Package,
  Film,
  HardDrive,
  Server
};

function isHls(url) {
  if (!url) return false;
  return url.includes('.m3u8') || url.includes('playlist');
}

function requiresIframe(source) {
  if (!source) return false;
  if (IFRAME_PROVIDERS.includes(source.id)) return true;
  return false;
}

export default function VideoPlayerPage() {
  const containerRef = useRef(null);
  const playerRef = useRef(null);
  const [playerError, setPlayerError] = useState('');
  const [useNative, setUseNative] = useState(false);
  const [reuploadLoading, setReuploadLoading] = useState(null);
  const [reuploadSource, setReuploadSource] = useState('');
  const [reuploadProvider, setReuploadProvider] = useState('');
  const navigate = useNavigate();
  const { data: files } = useFiles();
  const reuploadToProvider = useReuploadToProvider();

  const { currentFile, setCurrentFile, currentSource, setCurrentSource, availableSources } = usePlayerStore();

  const handleReuploadSelect = (provider) => {
    setReuploadProvider(provider);
    setReuploadSource('');
  };

  const handleReuploadCancel = () => {
    setReuploadProvider('');
    setReuploadSource('');
  };

  const handleReupload = async (provider, source) => {
    if (!currentFile || !source) return;
    if (!window.confirm(`Re-upload to ${getProviderConfig(provider)?.name} from ${getProviderConfig(source)?.name}?`)) return;
    setReuploadLoading(provider);
    try {
      await reuploadToProvider.mutateAsync({ fileId: currentFile.id, provider, source });
    } catch (error) {
      alert('Re-upload failed: ' + error.message);
    } finally {
      setReuploadLoading(null);
      setReuploadProvider('');
      setReuploadSource('');
    }
  };

  useEffect(() => {
    if (!currentFile && files?.length > 0) {
      const completed = files.find(f => f.status === 'completed' || f.syncStatus > 0);
      if (completed) setCurrentFile(completed);
    }
  }, [files, currentFile, setCurrentFile]);

  useEffect(() => {
    // Skip video.js for iframe-based sources or when using native fallback
    if (!currentSource?.url || useNative || requiresIframe(currentSource)) return;

    const container = containerRef.current;
    if (!container) return;

    // Cleanup previous player
    if (playerRef.current) {
      try { playerRef.current.dispose(); } catch (_) {}
      playerRef.current = null;
    }
    container.innerHTML = '';

    const videoElement = document.createElement('video');
    videoElement.className = 'video-js vjs-big-play-centered w-full h-full';
    videoElement.controls = true;
    videoElement.preload = 'auto';
    container.appendChild(videoElement);

    const isHlsStream = isHls(currentSource.url);

    let player;
    try {
      player = videojs(videoElement, {
        controls: true,
        autoplay: false,
        preload: 'auto',
        fluid: true,
        techOrder: ['html5'],
        html5: {
          vhs: {
            overrideNative: !isHlsStream,
            limitRenditionByPlayerDimensions: true,
            useDevicePixelRatio: true
          }
        }
      });
    } catch (initErr) {
      console.error('[Player Init Error]', initErr);
      setPlayerError('Failed to initialize video player.');
      setUseNative(true);
      return;
    }

    player.src({
      src: currentSource.url,
      type: currentSource?.mimeType || 'video/mp4'
    });

    player.on('error', () => {
      const err = player.error();
      const code = err?.code;
      const msg = err?.message || 'Unknown player error';
      console.error('[Player Error]', code, msg);
      if (code === 4) {
        const maybeProxyHint = currentSource?.url?.includes('/api/proxy/video')
          ? ' Proxy endpoint tidak aktif (restart API server) atau URL ditolak.'
          : '';
        setPlayerError(`Video cannot be played (CORS or format issue).${maybeProxyHint}`);
        setUseNative(true);
      } else {
        setPlayerError(`Player error: ${msg}`);
      }
    });

    playerRef.current = player;

    return () => {
      if (playerRef.current) {
        try { playerRef.current.dispose(); } catch (_) {}
        playerRef.current = null;
      }
      if (container) container.innerHTML = '';
    };
  }, [currentSource, useNative]);

  // Native video fallback
  const renderNativePlayer = () => {
    if (!currentSource?.url) return null;
    return (
      <video
        controls
        preload="auto"
        className="w-full h-full"
        poster=""
        onError={(e) => {
          console.error('[Native Player Error]', e.target.error);
          setPlayerError('Native player also failed. The URL may require special headers or CORS is blocked.');
        }}
      >
        <source src={currentSource.url} type={currentSource?.mimeType || 'video/mp4'} />
        Your browser does not support the video tag.
      </video>
    );
  };

  // Iframe embed player (for SeekStreaming and other embed-only sources)
  const renderIframePlayer = () => {
    if (!currentSource?.url) return null;
    return (
      <iframe
        src={currentSource.url}
        className="w-full h-full"
        frameBorder="0"
        allowFullScreen
        allow="autoplay; fullscreen; picture-in-picture"
        title={currentFile?.name || 'Video Player'}
      />
    );
  };

  if (!currentFile) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[50vh] text-center p-6">
        <div className="w-16 h-16 rounded-full bg-[#1a1a1a] flex items-center justify-center mb-4">
          <Play className="w-8 h-8 text-[#444]" />
        </div>
        <h2 className="text-lg font-medium text-white mb-2">No Video Selected</h2>
        <p className="text-sm text-[#666] mb-4">Select a file from the library</p>
        <button onClick={() => navigate('/files')} className="btn btn-primary text-sm flex items-center gap-2">
          <ChevronLeft className="w-4 h-4" />
          Browse Files
        </button>
      </div>
    );
  }

  const isExternalSource = currentSource?.type === 'cloud';
  const isSeekStreamingSource = currentSource?.id === 'seekstreaming';

  return (
    <div className="space-y-5">
      <button onClick={() => navigate('/files')} className="flex items-center gap-1 text-[#888] hover:text-[#ccc] text-sm">
        <ChevronLeft className="w-4 h-4" />
        Back to Files
      </button>

      <div className="card overflow-hidden">
        <div className="aspect-video bg-black">
          {requiresIframe(currentSource) ? (
            renderIframePlayer()
          ) : useNative ? (
            renderNativePlayer()
          ) : (
            <div ref={containerRef} className="w-full h-full" />
          )}
        </div>
      </div>

      {playerError && (
        <div className="card p-4 border border-red-500/30">
          <div className="flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="text-sm text-red-400 mb-2">{playerError}</p>
              {isExternalSource && !isSeekStreamingSource && (
                <p className="text-xs text-[#888] mb-3">
                  External video hosts may block embedding due to CORS restrictions.
                  This is normal for many file hosting services.
                </p>
              )}
              <div className="flex items-center gap-3">
                <a
                  href={currentSource?.url || currentSource?.originalUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="btn btn-primary text-xs flex items-center gap-1.5"
                >
                  <ExternalLink className="w-3 h-3" />
                  Open Video
                </a>
                {useNative && (
                  <button
                    onClick={() => { setUseNative(false); setPlayerError(''); }}
                    className="text-xs text-[#888] hover:text-[#ccc]"
                  >
                    Retry Player
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="card p-5">
        <div className="flex flex-col lg:flex-row lg:items-start gap-5">
          <div className="flex-1">
            <h2 className="text-lg font-medium text-white mb-2">{currentFile.name}</h2>
            <div className="flex flex-wrap items-center gap-2 text-sm text-[#888]">
              <span className="px-2 py-1 rounded bg-[#222]">{(currentFile.size / 1024 / 1024).toFixed(1)} MB</span>
              <span className="px-2 py-1 rounded bg-[#222]">
                {Math.floor((currentFile.duration || 0) / 60)}:{String(Math.floor((currentFile.duration || 0) % 60)).padStart(2, '0')}
              </span>
              <span className={`px-2 py-1 rounded ${
                currentFile.status === 'completed' ? 'bg-green-400/10 text-green-400' : 'bg-yellow-400/10 text-yellow-400'
              }`}>
                {currentFile.status}
              </span>
            </div>
          </div>

          <div className="lg:w-56">
            <h3 className="text-xs font-medium text-[#888] mb-2">Source</h3>
            <div className="space-y-1">
              {availableSources.map((source) => {
                const config = getProviderConfig(source.id);
                const Icon = providerIconMap[config.icon] || HardDrive;
                const isActive = currentSource?.id === source.id;

                return (
                  <button
                    key={source.id}
                    onClick={() => setCurrentSource(source)}
                    className={`w-full flex items-center gap-2 p-2 rounded-lg text-left text-sm transition-colors ${
                      isActive ? 'bg-[#333] text-white' : 'bg-[#1a1a1a] text-[#888] hover:bg-[#222]'
                    }`}
                  >
                    <div className="w-7 h-7 rounded bg-[#222] flex items-center justify-center">
                      <Icon className="w-3.5 h-3.5 text-[#888]" />
                    </div>
                    <span className="flex-1">{config.name}</span>
                    {isActive && <span className="w-1.5 h-1.5 rounded-full bg-green-400" />}
                  </button>
                );
              })}
            </div>
          </div>
        </div>

        <div className="mt-4 pt-4 border-t border-[#222]">
          <h3 className="text-xs font-medium text-[#888] mb-2">Provider Status</h3>
          <div className="flex flex-wrap gap-2">
            {Object.entries(currentFile.providers || {}).map(([provider, status]) => (
              <div
                key={provider}
                className={`flex items-center gap-1.5 px-2 py-1 rounded text-xs ${
                  status.status === 'completed' ? 'bg-green-400/10 text-green-400' :
                  status.status === 'failed' ? 'bg-red-400/10 text-red-400' :
                  status.status === 'uploading' ? 'bg-blue-400/10 text-blue-400' :
                  'bg-[#222] text-[#888]'
                }`}
              >
                {status.status === 'completed' && <CheckCircle className="w-3 h-3" />}
                {status.status === 'failed' && <AlertCircle className="w-3 h-3" />}
                {status.status === 'uploading' && <Clock className="w-3 h-3 animate-pulse" />}
                {status.status === 'pending' && <Clock className="w-3 h-3" />}
                <span>{getProviderConfig(provider)?.name || provider}</span>
                {status.status === 'failed' && (
                  reuploadProvider === provider ? (
                    <div className="ml-1 flex items-center gap-1">
                      <select
                        value={reuploadSource}
                        onChange={(e) => setReuploadSource(e.target.value)}
                        className="text-xs bg-[#222] text-white rounded px-1 py-0.5 border border-[#333]"
                        disabled={reuploadLoading === provider}
                      >
                        <option value="">Select source...</option>
                        {(() => {
                          const availableSources = Object.entries(currentFile.providers || {})
                            .filter(([_, ps]) => ps.status === 'completed')
                            .filter(([key]) => key !== provider);
                          return availableSources.map(([key, sourcePs]) => (
                            <option key={key} value={key}>
                              {getProviderConfig(key)?.name}
                            </option>
                          ));
                        })()}
                      </select>
                      <button
                        onClick={() => handleReupload(provider, reuploadSource)}
                        disabled={reuploadLoading === provider || !reuploadSource}
                        className="p-0.5 rounded hover:bg-[#333] text-[#888] hover:text-white disabled:opacity-50"
                        title="Confirm reupload"
                      >
                        <CheckCircle className="w-3 h-3" />
                      </button>
                      <button
                        onClick={handleReuploadCancel}
                        disabled={reuploadLoading === provider}
                        className="p-0.5 rounded hover:bg-[#333] text-[#888] hover:text-white disabled:opacity-50"
                        title="Cancel"
                      >
                        <X className="w-3 h-3" />
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => handleReuploadSelect(provider)}
                      disabled={reuploadLoading}
                      className="ml-1 p-0.5 rounded hover:bg-[#333] text-[#888] hover:text-white disabled:opacity-50"
                      title="Re-upload"
                    >
                      <RefreshCw className="w-3 h-3" />
                    </button>
                  )
                )}
              </div>
            ))}
          </div>
          {currentSource?.url && (
            <div className="mt-3 flex items-center gap-2">
              <span className="text-[10px] text-[#666] uppercase">URL:</span>
              <a
                href={currentSource.url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-[#888] hover:text-white truncate max-w-md"
              >
                {currentSource.url}
              </a>
            </div>
          )}
        </div>
      </div>

      {files?.filter(f => f.id !== currentFile.id && f.syncStatus > 0).length > 0 && (
        <div className="card p-5">
          <h3 className="text-sm font-medium text-[#ccc] mb-3">More Videos</h3>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {files
              .filter(f => f.id !== currentFile.id && f.syncStatus > 0)
              .slice(0, 4)
              .map(file => (
                <button
                  key={file.id}
                  onClick={() => setCurrentFile(file)}
                  className="p-3 rounded-lg bg-[#1a1a1a] hover:bg-[#222] text-left"
                >
                  <div className="aspect-video rounded bg-[#222] flex items-center justify-center mb-2">
                    <Play className="w-6 h-6 text-[#444]" />
                  </div>
                  <p className="text-xs text-[#ccc] truncate">{file.name}</p>
                  <p className="text-xs text-[#666] mt-0.5">{(file.size / 1024 / 1024).toFixed(1)} MB</p>
                </button>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}
