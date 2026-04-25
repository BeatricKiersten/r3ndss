export const PROVIDERS = {
  rclone: {
    id: 'rclone',
    name: 'Rclone Storage',
    short: 'R',
    color: 'text-red-400',
    bgColor: 'bg-red-400/10',
    borderColor: 'border-red-400/20',
    description: 'Rclone-backed remote storage (S3, B2, Drive, etc)',
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

export function getProviderConfig(providerKey) {
  const key = String(providerKey || '');
  const isDynamicRclone = key.startsWith('rclone:');
  const base = isDynamicRclone ? PROVIDERS.rclone : PROVIDERS[key];

  if (base) {
    if (!isDynamicRclone) {
      return base;
    }

    const suffix = key.split(':')[1] || key;
    return {
      ...base,
      id: key,
      name: `Rclone ${suffix}`,
      short: `R-${suffix.slice(0, 3).toUpperCase()}`
    };
  }

  return {
    id: key,
    name: key,
    short: key.slice(0, 3).toUpperCase(),
    color: 'text-gray-400',
    bgColor: 'bg-gray-400/10',
    borderColor: 'border-gray-400/20',
    description: 'Provider',
    icon: 'Cloud'
  };
}

export function getStatusConfig(status) {
  return STATUS_CONFIG[status] || STATUS_CONFIG.processing;
}
