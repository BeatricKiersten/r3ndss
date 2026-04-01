const STATIC_PROVIDER_DEFS = {
  voesx: {
    id: 'voesx',
    name: 'Voe.sx',
    kind: 'native',
    supportsStream: true,
    supportsReupload: true,
    supportsCopy: true
  },
  catbox: {
    id: 'catbox',
    name: 'Catbox',
    kind: 'native',
    supportsStream: true,
    supportsReupload: true,
    supportsCopy: true
  },
  seekstreaming: {
    id: 'seekstreaming',
    name: 'SeekStreaming',
    kind: 'native',
    supportsStream: true,
    supportsReupload: true,
    supportsCopy: true
  }
};

const LEGACY_RCLONE_PROVIDER_ID = 'rclone';

function getStaticProviderIds() {
  return Object.keys(STATIC_PROVIDER_DEFS);
}

function buildRcloneProfileProviderId(profileId) {
  return `rclone:${String(profileId || '').trim()}`;
}

function isRcloneProfileProviderId(providerId) {
  return String(providerId || '').startsWith('rclone:');
}

function parseRcloneProfileId(providerId) {
  if (!isRcloneProfileProviderId(providerId)) return null;
  const profileId = String(providerId || '').slice('rclone:'.length).trim();
  return profileId || null;
}

function isKnownProviderId(providerId) {
  const normalized = String(providerId || '').trim();
  if (!normalized) return false;

  return (
    normalized === LEGACY_RCLONE_PROVIDER_ID
    || Object.prototype.hasOwnProperty.call(STATIC_PROVIDER_DEFS, normalized)
    || isRcloneProfileProviderId(normalized)
  );
}

module.exports = {
  STATIC_PROVIDER_DEFS,
  LEGACY_RCLONE_PROVIDER_ID,
  getStaticProviderIds,
  buildRcloneProfileProviderId,
  isRcloneProfileProviderId,
  parseRcloneProfileId,
  isKnownProviderId
};
