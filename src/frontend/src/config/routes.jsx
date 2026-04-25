import React, { lazy } from 'react';

export const PATHS = {
  upload: '/',
  files: '/files',
  folderManagement: '/files/manage',
  jobs: '/jobs',
  providers: '/providers',
  rclone: '/rclone',
  player: '/player',
  status: '/status',
  zenius: '/zenius',
  public: '/public',
  publicFile: '/public/:fileId'
};

export const NAV_ITEMS = [
  { path: PATHS.upload, icon: 'Upload', label: 'Upload', shortcut: '1' },
  { path: PATHS.files, icon: 'Folder', label: 'Files', shortcut: '2' },
  { path: PATHS.jobs, icon: 'ListChecks', label: 'Jobs', shortcut: '3' },
  { path: PATHS.providers, icon: 'Cloud', label: 'Providers', shortcut: '4' },
  { path: PATHS.rclone, icon: 'Server', label: 'Rclone', shortcut: '5' },
  { path: PATHS.player, icon: 'Play', label: 'Player', shortcut: '6' },
  { path: PATHS.status, icon: 'Activity', label: 'Status', shortcut: '7' },
  { path: PATHS.zenius, icon: 'BookOpen', label: 'Zenius', shortcut: '8' }
];

const UploadPage = lazy(() => import('../pages/UploadPage.jsx'));
const FileListPage = lazy(() => import('../pages/FileListPage.jsx'));
const FolderManagementPage = lazy(() => import('../pages/FolderManagementPage.jsx'));
const JobsPage = lazy(() => import('../pages/JobsPage.jsx'));
const ProviderManagementPage = lazy(() => import('../pages/ProviderManagementPage.jsx'));
const RcloneConfigPage = lazy(() => import('../pages/RcloneConfigPage.jsx'));
const VideoPlayerPage = lazy(() => import('../pages/VideoPlayerPage.jsx'));
const BackupStatusPage = lazy(() => import('../pages/BackupStatusPage.jsx'));
const ZeniusPage = lazy(() => import('../pages/ZeniusPage.jsx'));
const PublicFilesPage = lazy(() => import('../pages/PublicFilesPage.jsx'));

export const APP_ROUTES = [
  { path: PATHS.upload, element: <UploadPage /> },
  { path: PATHS.files, element: <FileListPage /> },
  { path: PATHS.folderManagement, element: <FolderManagementPage /> },
  { path: PATHS.jobs, element: <JobsPage /> },
  { path: PATHS.providers, element: <ProviderManagementPage /> },
  { path: PATHS.rclone, element: <RcloneConfigPage /> },
  { path: PATHS.player, element: <VideoPlayerPage /> },
  { path: PATHS.status, element: <BackupStatusPage /> },
  { path: PATHS.zenius, element: <ZeniusPage /> }
];

export const PUBLIC_ROUTES = [
  { path: PATHS.public, element: <PublicFilesPage /> },
  { path: PATHS.publicFile, element: <PublicFilesPage /> }
];

export function isPublicPath(pathname) {
  return pathname === PATHS.public || pathname.startsWith(`${PATHS.public}/`);
}
