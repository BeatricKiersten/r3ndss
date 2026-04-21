const assert = require('assert/strict');
const path = require('path');

function sanitizePathSegment(value, fallback = 'unknown') {
  const normalized = String(value || '')
    .trim()
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || fallback;
}

function sanitizeOutputNameLegacy(value, fallback) {
  const normalized = sanitizePathSegment(String(value || '').replace(/\.mp4$/i, ''), fallback);
  return normalized || fallback;
}

function sanitizeOutputName(value, fallback) {
  const parsed = path.parse(String(value || '').trim()).name;
  const normalized = parsed
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 180);
  return normalized || fallback;
}

function reserveUniqueOutputName(rawName, fallback, urlShortId, usedNames) {
  const baseName = sanitizeOutputName(rawName, fallback);
  if (!usedNames) {
    return baseName;
  }

  const normalizedKey = baseName.toLowerCase();
  if (!usedNames.has(normalizedKey)) {
    usedNames.add(normalizedKey);
    return baseName;
  }

  const withId = sanitizeOutputName(`${baseName}-${urlShortId}`, fallback);
  const withIdKey = withId.toLowerCase();
  if (!usedNames.has(withIdKey)) {
    usedNames.add(withIdKey);
    return withId;
  }

  let counter = 2;
  while (true) {
    const candidate = sanitizeOutputName(`${baseName}-${urlShortId}-${counter}`, fallback);
    const candidateKey = candidate.toLowerCase();
    if (!usedNames.has(candidateKey)) {
      usedNames.add(candidateKey);
      return candidate;
    }
    counter += 1;
  }
}

function labelWithLegacyNaming(instances, existingNamesInFolder) {
  return instances.map((item) => {
    const outputBaseName = sanitizeOutputNameLegacy(item.name || `zenius-${item.urlShortId}`, `zenius-${item.urlShortId}`);
    const outputName = `${outputBaseName}.mp4`;
    return {
      urlShortId: item.urlShortId,
      outputName,
      label: existingNamesInFolder.has(outputName) ? 'existing' : 'new'
    };
  });
}

function labelWithCurrentNaming(instances, existingNamesInFolder) {
  const usedOutputNames = new Set();
  return instances.map((item) => {
    const outputBaseName = reserveUniqueOutputName(
      item.name || `zenius-${item.urlShortId}`,
      `zenius-${item.urlShortId}`,
      item.urlShortId,
      usedOutputNames
    );
    const outputName = `${outputBaseName}.mp4`;
    return {
      urlShortId: item.urlShortId,
      outputName,
      label: existingNamesInFolder.has(outputName) ? 'existing' : 'new'
    };
  });
}

function runScenario() {
  const instances = [
    { urlShortId: '111', name: 'Latihan Campuran' },
    { urlShortId: '222', name: 'Latihan Campuran' }
  ];

  // Simulasi kasus nyata: run lama sudah menyimpan item ke-2
  // sebagai nama unik (karena bentrok nama di folder yang sama).
  const existingNamesInFolder = new Set(['Latihan Campuran-222.mp4']);

  const legacy = labelWithLegacyNaming(instances, existingNamesInFolder);
  const current = labelWithCurrentNaming(instances, existingNamesInFolder);

  console.log('[SCENARIO] existing names:', Array.from(existingNamesInFolder));
  console.log('[LEGACY] ', legacy);
  console.log('[CURRENT]', current);

  assert.equal(legacy[1].label, 'new');
  assert.equal(current[1].label, 'existing');

  console.log('\n[PASS] Duplicate check dengan naming baru mendeteksi file existing di folder yang sama.');
}

runScenario();
