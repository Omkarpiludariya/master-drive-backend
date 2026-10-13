process.env.NODE_ENV = 'test';

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'http';
import crypto from 'crypto';
import { connectDB, disconnectDB } from '../src/config/db.js';
import { encrypt, decrypt } from '../src/services/cryptoService.js';
import VirtualItem from '../src/models/VirtualItem.js';
import DriveAccount from '../src/models/DriveAccount.js';

let app;
let server;
let baseUrl;
let authToken;
let userId;
let createdFolderId;
let uploadedFileId;
let testFileContent;
let testFileHash;

before(async () => {
  process.env.NODE_ENV = 'test';
  await connectDB();

  // Dynamically import app so NODE_ENV=test is already active
  const appModule = await import('../src/index.js');
  app = appModule.default;

  // Start app on ephemeral port
  server = http.createServer(app);
  await new Promise((resolve) => {
    server.listen(0, () => {
      const port = server.address().port;
      baseUrl = `http://localhost:${port}`;
      resolve();
    });
  });
});

after(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
  await disconnectDB();
});

test('1. Security: AES-256-GCM Crypto Service', () => {
  const secret = 'google_refresh_token_xyz_12345';
  const cipherText = encrypt(secret);

  assert.ok(cipherText.includes(':'), 'Encrypted token should contain iv:authTag:data format');
  assert.notEqual(cipherText, secret);

  const decrypted = decrypt(cipherText);
  assert.equal(decrypted, secret, 'Decrypted text should match original plaintext');

  // Verify tampering detection
  const tampered = cipherText.replace(/[0-9a-f]/, 'z');
  assert.throws(() => decrypt(tampered));

  // Verify auth tag corruption (flipping characters in ciphertext while maintaining valid even-length hex)
  const parts = cipherText.split(':');
  const lastTwo = parts[2].slice(-2);
  const flippedLastTwo = lastTwo === '00' ? 'ff' : '00';
  const corruptedCipher = parts[2].slice(0, -2) + flippedLastTwo;
  assert.throws(() => decrypt(`${parts[0]}:${parts[1]}:${corruptedCipher}`), /Unsupported state or unable to authenticate data/);
});

test('2. Auth API: User Registration & Login', async () => {
  const uniqueEmail = `test.user.${Date.now()}@example.com`;

  // Register
  const regRes = await fetch(`${baseUrl}/api/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: 'Test Explorer',
      email: uniqueEmail,
      password: 'securePassword123',
    }),
  });

  assert.equal(regRes.status, 201, 'Registration should return 201 Created');
  const regData = await regRes.json();
  assert.ok(regData.token, 'Should return JWT token');
  assert.equal(regData.user.email, uniqueEmail);
  authToken = regData.token;
  userId = regData.user.id;

  // Duplicate registration should fail
  const dupRes = await fetch(`${baseUrl}/api/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: 'Duplicate User',
      email: uniqueEmail,
      password: 'securePassword123',
    }),
  });
  assert.equal(dupRes.status, 400, 'Duplicate registration should return 400');

  // Login
  const loginRes = await fetch(`${baseUrl}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: uniqueEmail,
      password: 'securePassword123',
    }),
  });
  assert.equal(loginRes.status, 200, 'Login should return 200');
  const loginData = await loginRes.json();
  assert.ok(loginData.token);

  // Profile
  const profileRes = await fetch(`${baseUrl}/api/auth/me`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(profileRes.status, 200);
  const profileData = await profileRes.json();
  assert.equal(profileData.user.email, uniqueEmail);
});

test('3. Accounts API: Seed Mock Drive Accounts & Verify Pooled Storage', async () => {
  // Initially no accounts
  const emptyRes = await fetch(`${baseUrl}/api/accounts`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(emptyRes.status, 200);
  const emptyData = await emptyRes.json();
  assert.equal(emptyData.accounts.length, 0);

  // Seed 3 mock drives (3 x 15GB = 45GB pool)
  const seedRes = await fetch(`${baseUrl}/api/accounts/mock/seed`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(seedRes.status, 200);
  const seedData = await seedRes.json();
  assert.equal(seedData.createdCount, 3, 'Should seed 3 accounts');

  // Verify accounts in pool
  const accsRes = await fetch(`${baseUrl}/api/accounts`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  const accsData = await accsRes.json();
  assert.equal(accsData.accounts.length, 3);

  // Storage summary
  const summaryRes = await fetch(`${baseUrl}/api/files/storage/summary`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  const summaryData = await summaryRes.json();
  const expectedPoolBytes = 45 * 1024 * 1024 * 1024;
  assert.equal(summaryData.totalPooledLimit, expectedPoolBytes, 'Total pooled storage should be 45 GB');
  assert.equal(summaryData.totalPooledUsage, 0, 'Initial pooled usage should be 0');
  assert.equal(summaryData.totalAccounts, 3);
});

test('4. VFS: Create Virtual Folder and Validate Breadcrumbs', async () => {
  const folderRes = await fetch(`${baseUrl}/api/files/folders`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${authToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: 'Project Documents',
    }),
  });

  assert.equal(folderRes.status, 201);
  const folderData = await folderRes.json();
  assert.equal(folderData.folder.name, 'Project Documents');
  assert.equal(folderData.folder.type, 'folder');
  createdFolderId = folderData.folder._id;

  // Verify listing and breadcrumbs inside folder
  const listRes = await fetch(`${baseUrl}/api/files?parentId=${createdFolderId}`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  const listData = await listRes.json();
  assert.equal(listData.items.length, 0);
  assert.equal(listData.breadcrumbs.length, 2);
  assert.equal(listData.breadcrumbs[0].name, 'My Drive');
  assert.equal(listData.breadcrumbs[1].name, 'Project Documents');
});

test('5. Chunking Engine: Multipart Upload, Striping, and Manifest', async () => {
  // Generate random binary file content (64 KB)
  testFileContent = crypto.randomBytes(64 * 1024);
  testFileHash = crypto.createHash('sha256').update(testFileContent).digest('hex');

  const formData = new FormData();
  const blob = new Blob([testFileContent], { type: 'application/pdf' });
  formData.append('parentId', createdFolderId);
  formData.append('file', blob, 'MasterDrive_Architecture.pdf');

  const uploadRes = await fetch(`${baseUrl}/api/files/upload`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${authToken}`,
    },
    body: formData,
  });

  assert.equal(uploadRes.status, 201, 'Upload should succeed with 201');
  const uploadData = await uploadRes.json();
  assert.ok(uploadData.file);
  assert.equal(uploadData.file.name, 'MasterDrive_Architecture.pdf');
  assert.equal(uploadData.file.size, testFileContent.length);
  assert.equal(uploadData.file.status, 'ready');
  assert.equal(uploadData.file.parentId, createdFolderId);
  uploadedFileId = uploadData.file._id;

  // Inspect Chunk Manifest
  const manifestRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/manifest`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(manifestRes.status, 200);
  const manifestData = await manifestRes.json();
  assert.equal(manifestData.fileName, 'MasterDrive_Architecture.pdf');
  assert.equal(manifestData.totalSize, testFileContent.length);
  assert.equal(manifestData.checksum, testFileHash);
  assert.ok(manifestData.chunks.length >= 1);
  assert.ok(manifestData.chunks[0].driveAccount.email);
});

test('6. Streaming Reassembly: Full Download Integrity Verification', async () => {
  const downloadRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/download`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });

  assert.equal(downloadRes.status, 200);
  assert.equal(downloadRes.headers.get('content-type'), 'application/pdf');
  assert.equal(downloadRes.headers.get('content-length'), String(testFileContent.length));
  assert.ok(downloadRes.headers.get('content-disposition').includes('MasterDrive_Architecture.pdf'));

  const arrayBuffer = await downloadRes.arrayBuffer();
  const downloadedBuffer = Buffer.from(arrayBuffer);

  assert.equal(downloadedBuffer.length, testFileContent.length, 'Downloaded buffer length must match');
  const downloadedHash = crypto.createHash('sha256').update(downloadedBuffer).digest('hex');
  assert.equal(downloadedHash, testFileHash, 'Downloaded stream SHA-256 must match original file exactly');
});

test('7. Streaming Preview: HTTP 206 Partial Content Range Requests', async () => {
  const start = 100;
  const end = 499;
  const expectedLength = end - start + 1;

  const previewRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/preview`, {
    headers: {
      Authorization: `Bearer ${authToken}`,
      Range: `bytes=${start}-${end}`,
    },
  });

  assert.equal(previewRes.status, 206, 'Should return HTTP 206 Partial Content');
  assert.equal(previewRes.headers.get('content-length'), String(expectedLength));
  assert.equal(previewRes.headers.get('content-range'), `bytes ${start}-${end}/${testFileContent.length}`);

  const rangeBuffer = Buffer.from(await previewRes.arrayBuffer());
  assert.equal(rangeBuffer.length, expectedLength);

  const expectedSlice = testFileContent.subarray(start, end + 1);
  assert.ok(rangeBuffer.equals(expectedSlice), 'Returned byte slice must match exact original slice');
});

test('8. Token via Query Param: Streaming Download Auth via URL parameter', async () => {
  // Simulates browser <img> or <video> tag where headers cannot be set
  const queryAuthRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/download?token=${authToken}`);
  assert.equal(queryAuthRes.status, 200);
  const buf = Buffer.from(await queryAuthRes.arrayBuffer());
  assert.equal(buf.length, testFileContent.length);
});

test('9. File Lifecycle: Rename, Star, Trash, Restore, and Cascading Delete', async () => {
  // Rename & Star
  const patchRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}`, {
    method: 'PATCH',
    headers: {
      Authorization: `Bearer ${authToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: 'Renamed_Architecture.pdf',
      isStarred: true,
    }),
  });
  assert.equal(patchRes.status, 200);
  const patchData = await patchRes.json();
  assert.equal(patchData.item.name, 'Renamed_Architecture.pdf');
  assert.equal(patchData.item.isStarred, true);

  // Trash
  const trashRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/trash`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(trashRes.status, 200);

  // Verify file appears in trash
  const trashListRes = await fetch(`${baseUrl}/api/files?isTrash=true`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });
  const trashListData = await trashListRes.json();
  assert.ok(trashListData.items.some((i) => i._id === uploadedFileId));

  // Restore
  const restoreRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}/restore`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(restoreRes.status, 200);

  // Permanent Delete
  const delRes = await fetch(`${baseUrl}/api/files/${uploadedFileId}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${authToken}` },
  });
  assert.equal(delRes.status, 200);

  // Verify deleted from DB
  const checkDeleted = await VirtualItem.findById(uploadedFileId);
  assert.equal(checkDeleted, null, 'Virtual item must be removed from database');
});
