#!/usr/bin/env node

const os = require('os');
const path = require('path');
const fs = require('fs/promises');
const { spawn } = require('child_process');

function safeIniValue(value) {
  return String(value ?? '').replace(/[\r\n]/g, ' ').trim();
}

function joinRemotePath(basePath, fileName) {
  const normalizedBase = String(basePath || '').replace(/^\/+/, '').replace(/\/+$/, '');
  const normalizedName = String(fileName || '').replace(/^\/+/, '');
  return normalizedBase ? `${normalizedBase}/${normalizedName}` : normalizedName;
}

function runRclone(bin, args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (error) => reject(error));
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error((stderr || stdout || `rclone exited with code ${code}`).trim()));
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function main() {
  const rcloneBin = process.env.RCLONE_BIN || 'rclone';
  const remoteName = String(process.env.RCLONE_REMOTE_NAME || '').trim();
  const destinationPath = String(process.env.RCLONE_DESTINATION_PATH || '').trim();
  const keepTestFile = String(process.env.RCLONE_KEEP_TEST_FILE || 'false').toLowerCase() === 'true';
  const publicBaseUrl = String(process.env.RCLONE_PUBLIC_BASE_URL || '').trim();
  const useExistingRemote = String(process.env.RCLONE_USE_EXISTING_REMOTE || 'false').toLowerCase() === 'true';
  const uploadMode = String(process.env.RCLONE_UPLOAD_MODE || 'copy').toLowerCase();
  const testFileName = String(process.env.RCLONE_TEST_FILE_NAME || `.zenius-rclone-test-${Date.now()}.txt`).trim();

  if (!remoteName) {
    throw new Error('RCLONE_REMOTE_NAME is required');
  }

  if (!destinationPath) {
    throw new Error('RCLONE_DESTINATION_PATH is required (example: zenclone/bucket)');
  }

  if (uploadMode !== 'copy' && uploadMode !== 'copyto') {
    throw new Error('RCLONE_UPLOAD_MODE must be either copy or copyto');
  }

  const remoteType = String(process.env.RCLONE_REMOTE_TYPE || 's3').trim();
  const isS3Remote = remoteType.toLowerCase().includes('s3');

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'zenius-rclone-test-'));
  const configPath = path.join(tmpDir, 'rclone.conf');
  const localFilePath = path.join(tmpDir, testFileName);
  const remotePath = joinRemotePath(destinationPath, testFileName);
  const destinationFile = `${remoteName}:${remotePath}`;
  const destinationDir = `${remoteName}:${destinationPath}`;

  try {
    if (!useExistingRemote) {
      const paramsFilePath = String(process.env.RCLONE_PARAMETERS_FILE || '').trim();
      let rawParams = String(process.env.RCLONE_PARAMETERS_JSON || '{}');
      if (paramsFilePath) {
        rawParams = await fs.readFile(paramsFilePath, 'utf8');
      }

      let parameters;
      try {
        parameters = JSON.parse(rawParams);
      } catch (error) {
        throw new Error(`Invalid RCLONE_PARAMETERS_JSON: ${error.message}`);
      }

      if (!parameters || typeof parameters !== 'object' || Array.isArray(parameters)) {
        throw new Error('RCLONE_PARAMETERS_JSON must be a JSON object');
      }

      const lines = [`[${remoteName}]`, `type = ${safeIniValue(remoteType)}`];

      for (const [key, value] of Object.entries(parameters)) {
        if (value === undefined || value === null || value === '') continue;
        lines.push(`${key} = ${safeIniValue(value)}`);
      }

      lines.push('');
      await fs.writeFile(configPath, `${lines.join('\n')}\n`, 'utf8');
    }

    await fs.writeFile(localFilePath, `rclone test upload ${new Date().toISOString()}\n`, 'utf8');

    const configArgs = useExistingRemote ? [] : ['--config', configPath];
    const s3Args = isS3Remote
      ? ['--s3-no-check-bucket', '--s3-no-head', '--s3-no-head-object']
      : [];

    const uploadArgs = uploadMode === 'copy'
      ? ['copy', localFilePath, destinationDir, '--retries=1', '--no-check-dest', ...configArgs, ...s3Args]
      : ['copyto', localFilePath, destinationFile, '--retries=1', '--no-check-dest', ...configArgs, ...s3Args];

    console.log(`[test] Uploading test file to ${uploadMode === 'copy' ? destinationDir : destinationFile} (${uploadMode})`);
    await runRclone(rcloneBin, uploadArgs);
    console.log('[test] Upload OK');

    if (!keepTestFile) {
      const deleteArgs = [
        'deletefile',
        destinationFile,
        '--retries=1',
        ...configArgs
      ];

      await runRclone(rcloneBin, deleteArgs);
      console.log('[test] Cleanup OK (test file deleted)');
    } else {
      console.log('[test] Cleanup skipped (RCLONE_KEEP_TEST_FILE=true)');
    }

    if (publicBaseUrl) {
      const publicUrl = `${publicBaseUrl.replace(/\/+$/, '')}/${remotePath
        .split('/')
        .filter(Boolean)
        .map((segment) => encodeURIComponent(segment))
        .join('/')}`;
      console.log(`[test] Public URL: ${publicUrl}`);
    }

    console.log('[test] SUCCESS');
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
}

main().catch((error) => {
  console.error(`[test] FAILED: ${error.message}`);
  process.exit(1);
});
