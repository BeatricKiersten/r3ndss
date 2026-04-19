import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  BookOpen,
  ChevronDown,
  ChevronRight,
  Clock3,
  FileVideo,
  Folder,
  Home,
  Layers3,
  Play,
  Search,
  Sparkles,
  Menu,
  X
} from 'lucide-react';
import { useFiles, useFolder, useFolders } from '../hooks/api';
import { getProviderConfig } from '../config/providers';

const IFRAME_PROVIDERS = new Set(['seekstreaming', 'voesx', 'streamtape', 'mixdrop', 'catbox']);

function formatDuration(totalSeconds) {
  const seconds = Math.max(0, Math.floor(Number(totalSeconds) || 0));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remain = seconds % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(remain).padStart(2, '0')}`;
  }

  return `${minutes}:${String(remain).padStart(2, '0')}`;
}

function formatSize(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / (1024 ** index);
  return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function getPlayableUrl(provider, info) {
  if (!info) return null;

  if (provider === 'seekstreaming') {
    if (info.embedUrl) return info.embedUrl;
    if (info.fileId) return `https://seekstream.embedseek.com/#${info.fileId}`;
  }

  if (provider === 'voesx') {
    return info.embedUrl || info.url;
  }

  return info.embedUrl || info.url;
}

function getSources(file) {
  if (!file?.providers) return [];

  return Object.entries(file.providers)
    .filter(([, info]) => info?.status === 'completed')
    .map(([provider, info]) => ({
      id: provider,
      name: info?.providerName || getProviderConfig(provider).name,
      url: getPlayableUrl(provider, info),
      status: info.status
    }))
    .filter((source) => source.url);
}

function FolderBranch({ node, depth = 0, expandedFolders, onToggle, onSelect, currentFolderId }) {
  const folders = node?.folders || [];
  if (!folders.length) return null;

  return (
    <div className="space-y-1">
      {folders.map((folder) => {
        const isExpanded = Boolean(expandedFolders[folder.id]);
        const hasChildren = Boolean(folder.children?.folders?.length);
        const isCurrent = currentFolderId === folder.id;
        const hasPlayableVideoFiles = (folder.children?.files || []).some((file) => getSources(file).length > 0);
        const showVideoIcon = !hasChildren && hasPlayableVideoFiles;

        return (
          <div key={folder.id}>
            <button
              type="button"
              onClick={() => {
                onSelect(folder.id);
                if (hasChildren && !isExpanded) onToggle(folder.id);
              }}
              className={`public-folder-row ${isCurrent ? 'public-folder-row-active' : ''}`}
              style={{ paddingLeft: `${depth * 14 + 12}px` }}
            >
              {hasChildren ? (
                <span
                  className="public-folder-caret"
                  onClick={(event) => {
                    event.stopPropagation();
                    onToggle(folder.id);
                  }}
                >
                  {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                </span>
              ) : (
                <span className="public-folder-caret public-folder-caret-empty" />
              )}
              {showVideoIcon ? (
                <FileVideo className="h-4 w-4 shrink-0 text-yellow-300/90" />
              ) : (
                <Folder className="h-4 w-4 shrink-0" />
              )}
              <span className="truncate">{folder.name}</span>
            </button>
            {isExpanded && folder.children ? (
              <FolderBranch
                node={folder.children}
                depth={depth + 1}
                expandedFolders={expandedFolders}
                onToggle={onToggle}
                onSelect={onSelect}
                currentFolderId={currentFolderId}
              />
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

export default function PublicFilesPage() {
  const navigate = useNavigate();
  const { fileId } = useParams();
  const [currentFolderId, setCurrentFolderId] = useState('root');
  const [expandedFolders, setExpandedFolders] = useState({ root: true });
  const [activeSourceId, setActiveSourceId] = useState('');
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarFocused, setSidebarFocused] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');

  const { data: folderTree } = useFolders();
  const { data: currentFolder } = useFolder(currentFolderId);
  const { data: allFiles } = useFiles();
  const { data: folderFiles } = useFiles(currentFolderId);

  const playableFiles = useMemo(
    () => (allFiles || []).filter((file) => getSources(file).length > 0),
    [allFiles]
  );

  const selectedFile = useMemo(() => {
    if (fileId) {
      return playableFiles.find((file) => String(file.id) === String(fileId)) || null;
    }

    return playableFiles.find((file) => String(file.folderId || 'root') === String(currentFolderId)) || playableFiles[0] || null;
  }, [fileId, playableFiles, currentFolderId]);

  const selectedSources = useMemo(() => getSources(selectedFile), [selectedFile]);

  const activeSource = useMemo(() => {
    return selectedSources.find((source) => source.id === activeSourceId) || selectedSources[0] || null;
  }, [selectedSources, activeSourceId]);

  const currentPlaylist = useMemo(() => {
    const list = (folderFiles || []).filter((file) => getSources(file).length > 0);
    if (list.length) return list;
    return playableFiles;
  }, [folderFiles, playableFiles]);

  useEffect(() => {
    if (!selectedFile || currentFolderId === selectedFile.folderId) return;
    setCurrentFolderId(selectedFile.folderId || 'root');
  }, [selectedFile, currentFolderId]);

  useEffect(() => {
    if (!selectedSources.length) {
      setActiveSourceId('');
      return;
    }

    if (!selectedSources.some((source) => source.id === activeSourceId)) {
      setActiveSourceId(selectedSources[0].id);
    }
  }, [selectedSources, activeSourceId]);

  useEffect(() => {
    if (!fileId && selectedFile) {
      navigate(`/public/${selectedFile.id}`, { replace: true });
    }
  }, [fileId, selectedFile, navigate]);

  useEffect(() => {
    if (!selectedFile && playableFiles.length === 0) return;
    setExpandedFolders((prev) => ({
      ...prev,
      root: true,
      ...(selectedFile?.folderId ? { [selectedFile.folderId]: true } : {})
    }));
  }, [selectedFile, playableFiles.length]);

  const handleSelectFolder = (folderId) => {
    setCurrentFolderId(folderId);
    const nextFile = (allFiles || []).find(
      (file) => String(file.folderId || 'root') === String(folderId) && getSources(file).length > 0
    );

    if (nextFile) {
      navigate(`/public/${nextFile.id}`);
    }
  };

  const currentFolderName = currentFolderId === 'root' ? 'Semua Materi' : currentFolder?.name || 'Folder';

  const folderPathSegments = useMemo(() => {
    if (currentFolderId === 'root') return [];

    if (currentFolder?.path && currentFolder.path !== '/') {
      return currentFolder.path.split('/').filter(Boolean);
    }

    const findPathFromTree = (node, targetId, trail = []) => {
      const folders = node?.folders || [];
      for (const folder of folders) {
        const nextTrail = [...trail, folder.name];
        if (folder.id === targetId) {
          return nextTrail;
        }
        const found = findPathFromTree(folder.children, targetId, nextTrail);
        if (found) return found;
      }
      return null;
    };

    return findPathFromTree(folderTree, currentFolderId) || (currentFolderName ? [currentFolderName] : []);
  }, [currentFolderId, currentFolder?.path, currentFolderName, folderTree]);

  const breadcrumbSegments = useMemo(() => {
    const segments = [...folderPathSegments];
    if (selectedFile?.name) segments.push(selectedFile.name);
    return segments;
  }, [folderPathSegments, selectedFile?.name]);

  const filteredPlaylist = useMemo(() => {
    let list = currentPlaylist;
    if (searchQuery) {
      list = list.filter(file => 
        file.name.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }
    return list;
  }, [currentPlaylist, searchQuery]);

  return (
    <div className="public-shell min-h-screen text-white">
      <div className="public-noise" />
      
      {/* Top Navbar */}
      <nav className="sticky top-0 z-50 border-b border-white/5 bg-[#0f0f11]/95 backdrop-blur-md">
        <div className="mx-auto flex h-16 max-w-[1700px] items-center justify-between px-6">
          <div className="flex items-center gap-4">
            <button 
              type="button"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="rounded-lg p-2 hover:bg-white/5 lg:hidden"
            >
              {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </button>
            <div className="flex items-center gap-3">
              <span className="text-lg font-semibold text-white">Himel abdul rozak</span>
            </div>
            <div className="hidden items-center gap-6 md:flex">
              <a href="/public" className="text-sm font-medium text-white hover:text-yellow-400">Beranda</a>
              <a href="#" className="text-sm font-medium text-white/60 hover:text-white">Materi</a>
              <a href="#" className="text-sm font-medium text-white/60 hover:text-white">Tentang</a>
            </div>
          </div>
          
          <div className="flex items-center gap-4">
            <div className="relative hidden sm:block">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-white/40" />
              <input
                type="text"
                placeholder="Cari materi..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="h-9 w-64 rounded-full bg-white/5 pl-10 pr-4 text-sm text-white placeholder-white/40 border border-white/10 focus:border-yellow-400 focus:outline-none"
              />
            </div>
          </div>
        </div>
      </nav>

      <div className="relative mx-auto flex min-h-[calc(100vh-4rem)] w-full max-w-[1700px] flex-col gap-4 lg:gap-6 px-3 lg:px-6 py-4 sm:py-6">
        {/* Mobile sidebar overlay */}
        {sidebarOpen && (
          <div 
            className="fixed inset-0 top-16 z-30 bg-black/80 lg:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        <div className="public-layout mt-0 lg:mt-2 flex flex-1 flex-col gap-4 lg:gap-6 lg:flex-row">
          <aside 
            className={`fixed left-0 top-16 z-40 flex h-[calc(100vh-4rem)] shrink-0 flex-col gap-4 lg:gap-6 transform-gpu transition-all duration-300 ease-out bg-[#000000] border-r border-white/5 p-4 lg:sticky lg:top-[5.5rem] lg:bg-transparent lg:border-none lg:h-[calc(100vh-7rem)] lg:transform-none lg:p-0 ${sidebarOpen ? 'translate-x-0 w-[85vw] sm:w-80' : '-translate-x-full w-[85vw] sm:w-80'} ${sidebarFocused ? 'lg:w-[360px]' : 'lg:w-[280px]'}`}
            onMouseEnter={() => setSidebarFocused(true)}
            onMouseLeave={() => setSidebarFocused(false)}
            onFocus={() => setSidebarFocused(true)}
            onBlur={(e) => {
              if (!e.currentTarget.contains(e.relatedTarget)) setSidebarFocused(false);
            }}
          >
            <div className="public-panel public-side-panel flex min-h-0 flex-1 flex-col p-5">
              <div className="public-panel-header">
                <div>
                  <p className="public-overline">Materi</p>
                  <h2>Folder navigator</h2>
                </div>
              </div>

              <button
                type="button"
                onClick={() => {
                  handleSelectFolder('root');
                  setSidebarOpen(false);
                }}
                className={`public-folder-row ${currentFolderId === 'root' ? 'public-folder-row-active' : ''}`}
              >
                <span className="public-folder-caret public-folder-caret-empty" />
                <Home className="h-4 w-4 shrink-0" />
                <span>Semua Materi</span>
              </button>

              <div className="mt-2 flex-1 overflow-y-auto pr-1 no-scrollbar">
                <FolderBranch
                  node={folderTree}
                  expandedFolders={expandedFolders}
                  onToggle={(folderId) => setExpandedFolders((prev) => ({ ...prev, [folderId]: !prev[folderId] }))}
                  onSelect={handleSelectFolder}
                  currentFolderId={currentFolderId}
                />
              </div>
            </div>

            <div className="public-panel public-side-panel flex min-h-0 flex-1 flex-col p-5">
              <div className="public-panel-header">
                <div>
                  <p className="public-overline">Playlist</p>
                  <h2>{currentFolderName}</h2>
                </div>
              </div>

              <div className="mt-2 flex-1 space-y-2 overflow-y-auto pr-1 no-scrollbar">
                {filteredPlaylist.map((file) => {
                  const isActive = String(file.id) === String(selectedFile?.id);
                  const sources = getSources(file);

                  return (
                    <button
                      type="button"
                      key={file.id}
                      onClick={() => {
                        navigate(`/public/${file.id}`);
                        setSidebarOpen(false);
                      }}
                      className={`public-playlist-item ${isActive ? 'public-playlist-item-active' : ''}`}
                    >
                      <div className="public-playlist-index" aria-hidden="true">
                        <FileVideo className="h-3.5 w-3.5" />
                      </div>
                      <div className="min-w-0 flex-1 text-left">
                        <p className="truncate text-sm font-medium text-white">{file.name}</p>
                        <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-white/50">
                          <span className="inline-flex items-center gap-1">
                            <Clock3 className="h-3 w-3" />
                            {formatDuration(file.duration)}
                          </span>
                          <span>{sources.length} source</span>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          </aside>

          <main className="public-panel overflow-hidden flex-1 min-w-0">
            {selectedFile ? (
              <>
                <div className="public-breadcrumb">
                  {breadcrumbSegments.map((segment, index) => {
                    const isLast = index === breadcrumbSegments.length - 1;
                    return (
                      <React.Fragment key={`${segment}-${index}`}>
                        {index > 0 ? <ChevronRight className="h-3 w-3 shrink-0 text-white/35" /> : null}
                        <span className={isLast ? 'public-breadcrumb-current' : ''}>{segment}</span>
                      </React.Fragment>
                    );
                  })}
                </div>

                <div className="public-player-frame">
                  {activeSource && IFRAME_PROVIDERS.has(activeSource.id) ? (
                    <iframe
                      src={activeSource.url}
                      title={selectedFile.name}
                      className="h-full w-full"
                      allow="autoplay; fullscreen; picture-in-picture"
                      allowFullScreen
                    />
                  ) : activeSource ? (
                    <video controls className="h-full w-full" src={activeSource.url}>
                      Browser tidak mendukung video tag.
                    </video>
                  ) : (
                    <div className="flex h-full items-center justify-center text-sm text-white/60">Source tidak tersedia</div>
                  )}
                </div>

                <div className="mt-6 flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
                  <div className="min-w-0 flex-1">
                    <div className="inline-flex items-center gap-2 rounded-md border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] uppercase tracking-[0.2em] text-white/60">
                      <Sparkles className="h-3 w-3" />
                      Materi
                    </div>
                    <h2 className="mt-4 text-3xl font-bold tracking-tight text-white sm:text-4xl">{selectedFile.name}</h2>
                    <p className="mt-3 max-w-2xl text-base leading-relaxed text-white/70">
                      Karena Himmel sang pahlawan pasti akan melakukan hal yang sama.
                    </p>
                    <div className="mt-4 flex flex-wrap gap-2">
                      <span className="public-chip">{formatDuration(selectedFile.duration)}</span>
                      <span className="public-chip">{formatSize(selectedFile.size)}</span>
                      <span className="public-chip">{currentFolderName}</span>
                    </div>
                  </div>

                  <div className="public-source-list lg:w-[280px]">
                    {selectedSources.map((source) => {
                      const active = activeSource?.id === source.id;
                      return (
                        <button
                          type="button"
                          key={source.id}
                          onClick={() => setActiveSourceId(source.id)}
                          className={`public-source-button ${active ? 'public-source-button-active' : ''}`}
                        >
                          <div className="flex items-center gap-3">
                            <div className="public-source-icon">
                              <Play className="h-3.5 w-3.5" />
                            </div>
                            <div className="text-left">
                              <p className="text-sm font-medium text-white">{source.name}</p>
                              <p className="text-xs text-white/50">Ready to play</p>
                            </div>
                          </div>
                          <span className="public-source-pill">{source.id}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              </>
            ) : (
              <div className="flex min-h-[540px] flex-col items-center justify-center px-6 text-center">
                <div className="public-empty-icon">
                  <FileVideo className="h-10 w-10" />
                </div>
                <h2 className="mt-5 text-2xl font-semibold text-white">Belum ada file publik</h2>
                <p className="mt-2 max-w-md text-sm leading-6 text-white/60">
                  Upload file yang punya source playable agar halaman `/public/` langsung menampilkan player dan playlist.
                </p>
              </div>
            )}
          </main>
        </div>
      </div>
    </div>
  );
}
