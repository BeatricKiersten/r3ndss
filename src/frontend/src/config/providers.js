export const PROVIDERS = {
  backblaze: {
    id: 'backblaze',
    name: 'Rclone Storage',
    short: 'R',
    color: 'text-red-400',
    bgColor: 'bg-red-400/10',
    borderColor: 'border-red-400/20',
    description: 'Rclone-backed remote storage (Backblaze and others)',
    icon: 'Server'
  },
  voesx: {
    id: 'voesx',
    name: 'Voe.sx',
    short: 'Voe',
    color: 'text-purple-400',
    bgColor: 'bg-purple-400/10',
    borderColor: 'border-purple-400/20',
    description: 'Video hosting with embed support',
    icon: 'Play'
  },
  catbox: {
    id: 'catbox',
    name: 'Catbox',
    short: 'Cat',
    color: 'text-blue-400',
    bgColor: 'bg-blue-400/10',
    borderColor: 'border-blue-400/20',
    description: 'Free file hosting service',
    icon: 'Package'
  },
  seekstreaming: {
    id: 'seekstreaming',
    name: 'SeekStreaming',
    short: 'Seek',
    color: 'text-green-400',
    bgColor: 'bg-green-400/10',
    borderColor: 'border-green-400/20',
    description: 'Video streaming platform',
    icon: 'Film'
  }
};

export const STATUS_CONFIG = {
  completed: { color: 'text-green-400', bg: 'bg-green-400/10', label: 'Completed', icon: 'CheckCircle' },
  processing: { color: 'text-yellow-400', bg: 'bg-yellow-400/10', label: 'Processing', icon: 'Clock' },
  uploading: { color: 'text-blue-400', bg: 'bg-blue-400/10', label: 'Uploading', icon: 'Clock' },
  failed: { color: 'text-red-400', bg: 'bg-red-400/10', label: 'Failed', icon: 'AlertCircle' },
  partial: { color: 'text-orange-400', bg: 'bg-orange-400/10', label: 'Partial', icon: 'AlertCircle' },
  pending: { color: 'text-gray-400', bg: 'bg-gray-400/10', label: 'Pending', icon: 'Clock' }
};

export const NAV_ITEMS = [
  { path: '/', icon: 'Upload', label: 'Upload', shortcut: '1' },
  { path: '/files', icon: 'Folder', label: 'Files', shortcut: '2' },
  { path: '/jobs', icon: 'ListChecks', label: 'Jobs', shortcut: '3' },
  { path: '/providers', icon: 'Cloud', label: 'Providers', shortcut: '4' },
  { path: '/rclone', icon: 'Server', label: 'Rclone', shortcut: '5' },
  { path: '/player', icon: 'Play', label: 'Player', shortcut: '6' },
  { path: '/status', icon: 'Activity', label: 'Status', shortcut: '7' },
  { path: '/zenius', icon: 'BookOpen', label: 'Zenius', shortcut: '8' }
];

export function getProviderConfig(providerKey) {
  return PROVIDERS[providerKey] || { name: providerKey, short: providerKey.slice(0, 3).toUpperCase(), color: 'text-gray-400', bgColor: 'bg-gray-400/10' };
}

export function getStatusConfig(status) {
  return STATUS_CONFIG[status] || STATUS_CONFIG.processing;
}
