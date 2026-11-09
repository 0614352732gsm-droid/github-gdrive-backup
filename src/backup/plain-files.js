// Copyright (c) 2026. Readable-file backup extension for the original project.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Commercial
// A working-tree snapshot is uploaded as ordinary Google Drive folders and files.
// Historical Git objects are not represented by the working-tree snapshot.
const fs = require('fs');
const fsp = fs.promises;
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { spawnSync } = require('child_process');
const GitHubClient = require('./github');
const GoogleDriveClient = require('./gdrive');

const PARALLEL_FILES = 3;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function retry(action, label) {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      return await action();
    } catch (err) {
      const status = err.code || err.status || (err.response && err.response.status);
      const reason = JSON.stringify(err.errors || (err.response && err.response.data) || {});
      const limited = status === 429 || status === 500 || status === 502 ||
        status === 503 || status === 504 ||
        (status === 403 && /rateLimitExceeded|userRateLimitExceeded|quotaExceeded/.test(reason));
      if (!limited || attempt === 7) {
        throw new Error(label + ': ' + err.message);
      }
      await sleep(Math.min(30000, 1000 * Math.pow(2, attempt)));
    }
  }
}

function safeId(id) {
  if (!/^[A-Za-z0-9_-]+$/.test(id)) throw new Error('Invalid Drive folder ID');
  return id;
}

async function children(drive, parentId) {
  const entries = [];
  let pageToken;
  do {
    const r = await retry(() => drive.files.list({
      q: "'" + safeId(parentId) + "' in parents and trashed=false",
      fields: 'nextPageToken,files(id,name,mimeType,size,md5Checksum)',
      pageSize: 1000,
      ...(pageToken ? { pageToken } : {}),
    }), 'list Drive folder');
    entries.push(...(r.data.files || []));
    pageToken = r.data.nextPageToken;
  } while (pageToken);
  return entries;
}

async function folder(drive, parentId, name, items) {
  const entries = items || await children(drive, parentId);
  const prev = entries.find(x => x.name === name &&
    x.mimeType === 'application/vnd.google-apps.folder');
  if (prev) return prev.id;
  const r = await retry(() => drive.files.create({
    requestBody: {
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [safeId(parentId)],
    },
    fields: 'id,name,mimeType',
  }), 'create Drive folder ' + name);
  entries.push(r.data);
  return r.data.id;
}

async function hashes(filePath) {
  return new Promise((resolve, reject) => {
    const md5 = crypto.createHash('md5');
    const sha256 = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', c => { md5.update(c); sha256.update(c); });
    stream.on('error', reject);
    stream.on('end', () => resolve({
      md5: md5.digest('hex'),
      sha256: sha256.digest('hex'),
    }));
  });
}

function contentType(name) {
  const extension = path.extname(name).toLowerCase();
  const types = {
    '.md': 'text/markdown', '.txt': 'text/plain',
    '.log': 'text/plain', '.json': 'application/json',
    '.jsonl': 'application/x-ndjson', '.js': 'text/javascript',
    '.mjs': 'text/javascript', '.cjs': 'text/javascript',
    '.ts': 'text/plain', '.py': 'text/x-python',
    '.html': 'text/html', '.htm': 'text/html',
    '.css': 'text/css', '.csv': 'text/csv', '.xml': 'application/xml',
    '.yaml': 'text/yaml', '.yml': 'text/yaml',
    '.svg': 'image/svg+xml', '.pdf': 'application/pdf',
    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.webp': 'image/webp', '.gif': 'image/gif', '.zip': 'application/zip',
    '.sh': 'text/plain', '.toml': 'text/plain', '.ini': 'text/plain',
    '.rst': 'text/plain', '.sql': 'text/plain',
  };
  return types[extension] || 'application/octet-stream';
}

async function uploadFile(drive, localPath, name, parentId, current) {
  const stat = await fsp.stat(localPath);
  const hash = await hashes(localPath);
  const old = current.find(x => x.name === name &&
    x.mimeType !== 'application/vnd.google-apps.folder');
  if (old && old.md5Checksum === hash.md5 && Number(old.size) === stat.size) {
    return { action: 'unchanged', bytes: stat.size, sha256: hash.sha256 };
  }
  const media = () => ({
    mimeType: contentType(name),
    body: fs.createReadStream(localPath),
  });
  let res;
  if (old) {
    res = await retry(() => drive.files.update({
      fileId: old.id,
      media: media(),
      fields: 'id,size,md5Checksum',
    }), 'update ' + name);
  } else {
    res = await retry(() => drive.files.create({
      requestBody: {
        name,
        mimeType: contentType(name),
        parents: [safeId(parentId)],
      },
      media: media(),
      fields: 'id,size,md5Checksum',
    }), 'upload ' + name);
  }
  let remote = res.data;
  if (!remote.md5Checksum || remote.size === undefined) {
    const check = await retry(() => drive.files.get({
      fileId: remote.id,
      fields: 'id,size,md5Checksum',
    }), 'verify ' + name);
    remote = check.data;
  }
  if (remote.md5Checksum !== hash.md5 || Number(remote.size) !== stat.size) {
    throw new Error('Drive verification mismatch for ' + name);
  }
  return {
    action: old ? 'updated' : 'uploaded',
    bytes: stat.size,
    sha256: hash.sha256,
  };
}

async function pool(tasks, concurrency) {
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, async () => {
    while (next < tasks.length) {
      const task = tasks[next++];
      await task();
    }
  });
  await Promise.all(workers);
}

async function uploadTree(drive, localDir, driveFolderId, stats, relative) {
  const entries = await fsp.readdir(localDir, { withFileTypes: true });
  const existing = await children(drive, driveFolderId);
  const dirs = entries.filter(e => e.isDirectory() && e.name !== '.git');
  const files = entries.filter(e => e.isFile());
  const unsupported = entries.filter(e => !e.isDirectory() && !e.isFile());
  for (const e of unsupported) {
    throw new Error('Unsupported symlink or special file: ' + path.join(relative, e.name));
  }
  for (const entry of dirs.sort((a, b) => a.name.localeCompare(b.name))) {
    const childId = await folder(drive, driveFolderId, entry.name, existing);
    await uploadTree(drive, path.join(localDir, entry.name), childId, stats,
      path.join(relative, entry.name));
  }
  await pool(files.map(entry => async () => {
    const p = path.join(localDir, entry.name);
    const result = await uploadFile(drive, p, entry.name, driveFolderId, existing);
    stats.files++;
    stats.bytes += result.bytes;
    stats[result.action]++;
    if (stats.files % 100 === 0) {
      console.log('Readable files: ' + stats.repo + ', ' + stats.files + ' verified');
    }
  }), PARALLEL_FILES);
}

function runGit(args, env) {
  const result = spawnSync('git', args, {
    env, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024,
  });
  if (result.error || result.status !== 0) {
    throw new Error('Git command failed (exit ' + result.status +
      '): ' + args[0] + ' (credentials omitted)');
  }
  return (result.stdout || '').trim();
}

async function oneRepository(drive, gh, repository, accountId, token) {
  const root = await folder(drive, accountId, repository.name);
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), 'hex-readable-'));
  const checkout = path.join(tmp, 'checkout');
  const authorization = 'Basic ' + Buffer.from('x-access-token:' + token).toString('base64');
  const env = {
    ...process.env,
    GIT_TERMINAL_PROMPT: '0',
    GIT_CONFIG_COUNT: '1',
    GIT_CONFIG_KEY_0: 'http.https://github.com/.extraheader',
    GIT_CONFIG_VALUE_0: 'AUTHORIZATION: ' + authorization,
  };
  const stats = { repo: repository.full_name, files: 0, bytes: 0,
    uploaded: 0, updated: 0, unchanged: 0 };
  try {
    runGit(['clone', '--depth', '1', '--single-branch', '--no-tags',
      '--', repository.clone_url, checkout], env);
    if (fs.existsSync(path.join(checkout, '.gitmodules'))) {
      runGit(['-C', checkout, 'submodule', 'update', '--init',
        '--recursive', '--depth', '1'], env);
    }
    const lfs = spawnSync('git', ['lfs', 'version'], { env, encoding: 'utf8' });
    if (lfs.status === 0) {
      runGit(['-C', checkout, 'lfs', 'pull'], env);
    } else {
      // Do not mistake Git LFS pointer files for downloaded originals.
      const attributes = path.join(checkout, '.gitattributes');
      if (fs.existsSync(attributes) &&
          /filter=lfs/.test(await fsp.readFile(attributes, 'utf8'))) {
        throw new Error('Git LFS unavailable: cannot certify actual file contents');
      }
    }
    const commit = runGit(['-C', checkout, 'rev-parse', 'HEAD'], env);
    await uploadTree(drive, checkout, root, stats, '');
    stats.commit = commit;
    stats.branch = repository.default_branch;
    console.log('READABLE_SUCCESS ' + repository.full_name + ' ' +
      stats.files + ' files (' + stats.uploaded + ' new, ' +
      stats.updated + ' updated, ' + stats.unchanged + ' unchanged)');
    return { ...stats, status: 'success' };
  } finally {
    await fsp.rm(tmp, { recursive: true, force: true });
  }
}

async function runReadableBackup(options = {}) {
  const owner = options.owner;
  const token = options.token;
  const rootId = process.env.GDRIVE_FOLDER_ID;
  if (!owner || !token || !rootId) throw new Error('Missing backup credentials');
  const gh = new GitHubClient(token);
  const auth = await GoogleDriveClient.createAuthClient(
    process.env.GOOGLE_CLIENT_SECRET_PATH || './credentials/google-client-secret.json',
    process.env.GOOGLE_TOKEN_PATH || './credentials/google-token.json'
  );
  const drive = new GoogleDriveClient(auth).drive;
  const accountId = await folder(drive, rootId, owner);
  const repositories = options.repos
    ? await Promise.all(options.repos.map(async fullName => {
      const parts = fullName.split('/');
      const result = await gh.octokit.repos.get({ owner: parts[0], repo: parts[1] });
      return result.data;
    }))
    : await gh.listRepos(owner);
  console.log('Readable backup of ' + owner + ': ' + repositories.length +
    ' repositories; target Drive folder ' + accountId);
  const results = [];
  for (const repository of repositories) {
    try {
      results.push(await oneRepository(drive, gh, repository, accountId, token));
    } catch (err) {
      console.error('READABLE_FAILED ' + repository.full_name + ': ' + err.message);
      results.push({ repo: repository.full_name, status: 'failed', error: err.message });
    }
  }
  const summary = {
    account: owner,
    format: 'normal Google Drive files, not ZIP',
    branch: 'default branch per repository',
    generated: new Date().toISOString(),
    total: repositories.length,
    success: results.filter(r => r.status === 'success').length,
    failed: results.filter(r => r.status === 'failed').length,
    files: results.reduce((n, r) => n + (r.files || 0), 0),
    results,
  };
  const reportName = 'hexREADABLE_BACKUP_REPORT_' +
    new Date().toISOString().replace(/[:.]/g, '-') + '.json';
  const reportBytes = Buffer.from(JSON.stringify(summary, null, 2), 'utf8');
  await retry(() => drive.files.create({
    requestBody: { name: reportName, parents: [accountId],
      mimeType: 'application/json' },
    media: { mimeType: 'application/json',
      body: require('stream').Readable.from([reportBytes]) },
    fields: 'id',
  }), 'write backup report');
  console.log('READABLE_ACCOUNT_SUMMARY ' + owner + ': ' +
    summary.success + '/' + summary.total + ' successful; ' +
    summary.files + ' ordinary files');
  return summary;
}

module.exports = { runReadableBackup };
