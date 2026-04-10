import React, { useEffect, useState, useCallback } from 'react';
import { Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import { Upload, Folder, Play, Activity, Server, Menu, X, Cloud, ListChecks, Keyboard, BookOpen } from 'lucide-react';
import { useWebSocketStore } from './store/websocketStore';
import { NAV_ITEMS } from './config/providers';
import { ToastContainer } from './components/ui';
import UploadPage from './pages/UploadPage.jsx';
import FileListPage from './pages/FileListPage.jsx';
import VideoPlayerPage from './pages/VideoPlayerPage.jsx';
import BackupStatusPage from './pages/BackupStatusPage.jsx';
import ProviderManagementPage from './pages/ProviderManagementPage.jsx';
import JobsPage from './pages/JobsPage.jsx';
import RcloneConfigPage from './pages/RcloneConfigPage.jsx';
import ZeniusPage from './pages/ZeniusPage.jsx';
import PublicFilesPage from './pages/PublicFilesPage.jsx';

const iconMap = { Upload, Folder, Play, Activity, Server, Menu, X, Cloud, ListChecks, Keyboard, BookOpen };

const shortcuts = [
  { key: 'n', description: 'Go to Upload' },
  { key: 'f', description: 'Go to Files' },
  { key: 'j', description: 'Go to Jobs' },
  { key: 'p', description: 'Go to Providers' },
  { key: 'r', description: 'Go to Rclone' },
  { key: 's', description: 'Go to Status' },
  { key: 'z', description: 'Go to Zenius' },
  { key: 'Ctrl+B', description: 'Focus batch input (Zenius)' },
  { key: '1-8', description: 'Navigate by position' },
  { key: '?', description: 'Show shortcuts' },
  { key: 'Esc', description: 'Close modal/drawer' },
];

function ShortcutsModal({ onClose }) {
  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-6 max-w-sm w-full" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-medium text-white flex items-center gap-2">
            <Keyboard className="w-5 h-5" /> Keyboard Shortcuts
          </h3>
          <button onClick={onClose} className="p-1.5 hover:bg-[#222] rounded">
            <X className="w-5 h-5 text-[#666]" />
          </button>
        </div>
        <div className="space-y-2">
          {shortcuts.map((shortcut) => (
            <div key={shortcut.key} className="flex items-center justify-between py-1.5 border-b border-[#222] last:border-0">
              <span className="text-sm text-[#aaa]">{shortcut.description}</span>
              <kbd className="px-2 py-0.5 text-xs bg-[#222] text-[#888] rounded border border-[#333] font-mono">
                {shortcut.key}
              </kbd>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SidebarNav({ onClose }) {
  const location = useLocation();
  const navigate = useNavigate();

  const handleNavClick = (path) => {
    navigate(path);
    if (onClose) onClose();
  };

  return (
    <nav className="space-y-1">
      {NAV_ITEMS.map((item) => {
        const Icon = iconMap[item.icon] || Server;
        const isActive = location.pathname === item.path;
        return (
          <button
            key={item.path}
            onClick={() => handleNavClick(item.path)}
            className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-colors ${
              isActive ? 'bg-[#333] text-white' : 'text-[#888] hover:text-[#ccc] hover:bg-[#1a1a1a]'
            }`}
          >
            <Icon className="w-4 h-4" />
            <span className="text-sm flex-1 text-left">{item.label}</span>
            <kbd className="text-[10px] text-[#555] font-mono">{item.shortcut}</kbd>
          </button>
        );
      })}
    </nav>
  );
}

function SidebarFooter() {
  const { isConnected } = useWebSocketStore();
  return (
    <div className="pt-4 border-t border-[#222]">
      <div className="flex items-center gap-2 px-3 py-2">
        <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-400' : 'bg-red-400'}`} />
        <span className="text-xs text-[#888]">{isConnected ? 'Online' : 'Offline'}</span>
      </div>
    </div>
  );
}

function App() {
  const navigate = useNavigate();
  const location = useLocation();
  const { connect, disconnect, isConnected } = useWebSocketStore();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [showShortcuts, setShowShortcuts] = useState(false);

  const isPublicRoute = location.pathname === '/public' || location.pathname.startsWith('/public/');

  useEffect(() => {
    connect();
    return () => disconnect();
  }, [connect, disconnect]);

  const handleKeyDown = useCallback((e) => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable) return;
    
    switch (e.key) {
      case '?':
        setShowShortcuts(prev => !prev);
        break;
      case 'Escape':
        setSidebarOpen(false);
        setShowShortcuts(false);
        break;
      case 'n':
        if (!e.ctrlKey && !e.metaKey) navigate('/');
        break;
      case 'f':
        navigate('/files');
        break;
      case 'j':
        navigate('/jobs');
        break;
      case 'p':
        if (!e.ctrlKey && !e.metaKey) navigate('/providers');
        break;
      case 's':
        if (!e.ctrlKey && !e.metaKey) navigate('/status');
        break;
      case 'r':
        if (!e.ctrlKey && !e.metaKey) navigate('/rclone');
        break;
      case 'z':
        if (!e.ctrlKey && !e.metaKey) navigate('/zenius');
        break;
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
        const index = parseInt(e.key) - 1;
        if (NAV_ITEMS[index]) navigate(NAV_ITEMS[index].path);
        break;
      default:
        break;
    }
  }, [navigate]);

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  if (isPublicRoute) {
    return (
      <>
        <Routes>
          <Route path="/public" element={<PublicFilesPage />} />
          <Route path="/public/:fileId" element={<PublicFilesPage />} />
        </Routes>
        <ToastContainer />
      </>
    );
  }

  return (
    <div className="min-h-screen flex flex-col bg-[#111]">
      <header className="lg:hidden flex items-center justify-between px-4 py-3 border-b border-[#222] sticky top-0 bg-[#111] z-50">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-[#333] flex items-center justify-center">
            <Server className="w-4 h-4 text-[#888]" />
          </div>
          <span className="font-medium text-[#ccc]">HLS Backup</span>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => setShowShortcuts(true)} className="p-2 rounded-lg hover:bg-[#222]" title="Keyboard shortcuts">
            <Keyboard className="w-5 h-5 text-[#888]" />
          </button>
          <button onClick={() => setSidebarOpen(!sidebarOpen)} className="p-2 rounded-lg hover:bg-[#222]">
            {sidebarOpen ? <X className="w-5 h-5 text-[#888]" /> : <Menu className="w-5 h-5 text-[#888]" />}
          </button>
        </div>
      </header>

      <div className="flex-1 flex max-w-6xl mx-auto w-full">
        <aside className="hidden lg:flex flex-col w-48 py-6 px-3 sticky top-0 h-screen">
          <div className="flex items-center gap-2 px-2 mb-6">
            <div className="w-8 h-8 rounded-lg bg-[#333] flex items-center justify-center">
              <Server className="w-4 h-4 text-[#888]" />
            </div>
            <span className="font-medium text-[#ccc]">HLS Backup</span>
          </div>
          <div className="flex-1">
            <SidebarNav />
          </div>
          <SidebarFooter />
        </aside>

        {sidebarOpen && (
          <div className="fixed inset-0 bg-black/70 z-40 lg:hidden" onClick={() => setSidebarOpen(false)} />
        )}

        <aside className={`fixed inset-y-0 left-0 w-52 bg-[#111] border-r border-[#222] z-50 transform transition-transform lg:hidden ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}>
          <div className="p-4">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-[#333] flex items-center justify-center">
                  <Server className="w-4 h-4 text-[#888]" />
                </div>
                <span className="font-medium text-[#ccc]">HLS Backup</span>
              </div>
              <button onClick={() => setSidebarOpen(false)} className="p-2 hover:bg-[#222] rounded-lg">
                <X className="w-5 h-5 text-[#888]" />
              </button>
            </div>
            <SidebarNav onClose={() => setSidebarOpen(false)} />
            <div className="mt-4 pt-4 border-t border-[#222]">
              <SidebarFooter />
            </div>
          </div>
        </aside>

        <main className="flex-1 p-4 lg:p-6 overflow-x-hidden">
          <Routes>
            <Route path="/" element={<UploadPage />} />
            <Route path="/files" element={<FileListPage />} />
            <Route path="/jobs" element={<JobsPage />} />
            <Route path="/providers" element={<ProviderManagementPage />} />
            <Route path="/rclone" element={<RcloneConfigPage />} />
            <Route path="/player" element={<VideoPlayerPage />} />
            <Route path="/status" element={<BackupStatusPage />} />
            <Route path="/zenius" element={<ZeniusPage />} />
            <Route path="/public" element={<PublicFilesPage />} />
            <Route path="/public/:fileId" element={<PublicFilesPage />} />
          </Routes>
        </main>
      </div>

      {showShortcuts && <ShortcutsModal onClose={() => setShowShortcuts(false)} />}
      <ToastContainer />
    </div>
  );
}

export default App;
