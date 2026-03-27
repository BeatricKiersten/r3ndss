import { create } from 'zustand';
import { useQueryClient } from 'react-query';

const API_BASE = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '');

function toApiUrl(path) {
  if (!API_BASE) return path;
  return `${API_BASE}${path}`;
}

function getSafeLocalUrl(file) {
  if (!file?.localPath && !file?.name) return null;

  const rawName = file.localPath
    ? file.localPath.split(/[/\\]/).pop()
    : file.name;

  if (!rawName) return null;
  return toApiUrl(`/uploads/${encodeURIComponent(rawName)}`);
}

function inferMimeTypeFromUrl(url, fallback = 'video/mp4') {
  if (!url) return fallback;
  const clean = url.split('?')[0].toLowerCase();
  if (clean.endsWith('.m3u8')) return 'application/x-mpegURL';
  if (clean.endsWith('.webm')) return 'video/webm';
  if (clean.endsWith('.mov')) return 'video/quicktime';
  if (clean.endsWith('.mkv')) return 'video/x-matroska';
  if (clean.endsWith('.mp4')) return 'video/mp4';
  return fallback;
}

function getPlayableUrl(provider, url) {
  if (!url) return url;

  if (provider === 'seekstreaming') {
    if (url.includes('embedseek.com/#') || url.includes('/e/')) {
      return url;
    }

    const hashMatch = url.match(/#([^/?]+)/);
    if (hashMatch?.[1]) {
      return `https://seekstream.embedseek.com/#${hashMatch[1]}`;
    }

    const idMatch = url.match(/\/(?:v|e)\/([^/?#]+)/);
    if (idMatch?.[1]) {
      return `https://seekstream.embedseek.com/#${idMatch[1]}`;
    }
  }

  return url;
}

export const IFRAME_PROVIDERS = ['seekstreaming', 'voesx', 'streamtape', 'mixdrop', 'catbox', 'backblaze'];

export function requiresIframe(provider) {
  return IFRAME_PROVIDERS.includes(provider);
}

function getSeekStreamingUrl(fileId) {
  if (!fileId) return null;
  return `https://seekstream.embedseek.com/#${fileId}`;
}

const WS_URL =
  import.meta.env.VITE_WS_URL ||
  `ws://${window.location.hostname}:3001`;

export const useWebSocketStore = create((set, get) => ({
  socket: null,
  isConnected: false,
  lastMessage: null,
  dashboard: {
    recentFiles: [],
    activeJobs: [],
    queueStats: { pending: 0, processing: 0 }
  },
  events: [],
  progress: {},

  connect: () => {
    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      console.log('[WebSocket] Connected');
      set({ isConnected: true, socket: ws });
    };

    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        get().handleMessage(message);
      } catch (error) {
        console.error('[WebSocket] Parse error:', error);
      }
    };

    ws.onclose = () => {
      console.log('[WebSocket] Disconnected');
      set({ isConnected: false, socket: null });
      setTimeout(() => get().connect(), 5000);
    };

    ws.onerror = (error) => {
      console.error('[WebSocket] Error:', error);
    };

    set({ socket: ws });
  },

  disconnect: () => {
    const { socket } = get();
    if (socket) {
      socket.close();
    }
  },

  handleMessage: (message) => {
    const { event, data } = message;
    
    set((state) => ({
      lastMessage: message,
      events: [{ event, data, timestamp: Date.now() }, ...state.events].slice(0, 100)
    }));

    let queryClient = null;
    try {
      queryClient = useQueryClient();
    } catch {
      // React Query not available yet
    }

    switch (event) {
      case 'dashboard:update':
        set({ dashboard: data });
        break;
      
      case 'progress':
      case 'upload:progress':
        set((state) => ({
          progress: { ...state.progress, [data.fileId]: data }
        }));
        break;
      
      case 'job:started':
      case 'job:completed':
      case 'job:failed':
      case 'job:cancelled':
        if (queryClient) {
          queryClient.invalidateQueries('jobs');
          queryClient.invalidateQueries('dashboard');
          queryClient.invalidateQueries('files');
        }
        break;
      
      case 'upload:started':
      case 'upload:completed':
      case 'upload:failed':
        if (queryClient) {
          queryClient.invalidateQueries('files');
          queryClient.invalidateQueries('dashboard');
        }
        break;
      
      case 'transfer:queued':
      case 'transfer:started':
      case 'transfer:completed':
      case 'transfer:failed':
        if (queryClient) {
          queryClient.invalidateQueries('jobs');
          queryClient.invalidateQueries('dashboard');
        }
        break;
      
      case 'file:ready':
        if (queryClient) {
          queryClient.invalidateQueries('files');
        }
        break;
      
      case 'provider:checked':
      case 'provider:checked:bulk':
      case 'system:checked':
        if (queryClient) {
          queryClient.invalidateQueries('providers-status');
          queryClient.invalidateQueries('provider-check-snapshots');
          queryClient.invalidateQueries('files');
        }
        break;

      default:
        break;
    }
  },

  clearProgress: (fileId) => {
    set((state) => {
      const newProgress = { ...state.progress };
      if (fileId) {
        delete newProgress[fileId];
      }
      return { progress: newProgress };
    });
  }
}));

// File store for managing file list state
export const useFileStore = create((set, get) => ({
  files: [],
  folders: [],
  selectedFolder: 'root',
  selectedFile: null,
  isLoading: false,

  setFiles: (files) => set({ files }),
  setFolders: (folders) => set({ folders }),
  setSelectedFolder: (id) => set({ selectedFolder: id }),
  setSelectedFile: (file) => set({ selectedFile: file }),
  setLoading: (isLoading) => set({ isLoading }),

  updateFileProgress: (fileId, progress) => {
    set((state) => ({
      files: state.files.map(f =>
        f.id === fileId ? { ...f, progress: { ...f.progress, ...progress } } : f
      )
    }));
  },

  updateFileProviders: (fileId, provider, status) => {
    set((state) => ({
      files: state.files.map(f =>
        f.id === fileId
          ? { ...f, providers: { ...f.providers, [provider]: status } }
          : f
      )
    }));
  }
}));

// Player store for video player state
export const usePlayerStore = create((set, get) => ({
  currentFile: null,
  currentSource: null,
  availableSources: [],
  isPlaying: false,
  volume: 1,

  setCurrentFile: (file) => {
    const sources = get().getAvailableSources(file);
    const preferredSource = sources.find((source) => source.id === 'seekstreaming') || sources[0] || null;
    set({ 
      currentFile: file, 
      availableSources: sources,
      currentSource: preferredSource
    });
  },

  setCurrentSource: (source) => set({ currentSource: source }),
  setPlaying: (isPlaying) => set({ isPlaying }),
  setVolume: (volume) => set({ volume }),

  getAvailableSources: (file) => {
    if (!file) return [];
    
    const sources = [];
    
    // Provider sources
    const providerNames = {
      backblaze: 'Rclone Storage',
      voesx: 'Voe.sx',
      catbox: 'Catbox',
      seekstreaming: 'SeekStreaming'
    };

    Object.entries(file.providers || {}).forEach(([provider, info]) => {
      let providerUrl = info.url;
      
      if (provider === 'seekstreaming') {
        providerUrl = info.embedUrl || providerUrl;
        if (!providerUrl && info.fileId) {
          providerUrl = getSeekStreamingUrl(info.fileId);
        }
      }
      
      if (provider === 'voesx') {
        providerUrl = info.embedUrl || providerUrl;
      }

      if (info.status === 'completed' && providerUrl) {
        sources.push({
          id: provider,
          name: providerNames[provider],
          url: getPlayableUrl(provider, providerUrl),
          originalUrl: info.url || providerUrl,
          mimeType: inferMimeTypeFromUrl(providerUrl),
          type: 'cloud',
          status: info.status
        });
      }
    });

    return sources;
  }
}));
