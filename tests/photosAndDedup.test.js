process.env.NODE_ENV = 'test';

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'http';
import { connectDB, disconnectDB } from '../src/config/db.js';
import VirtualItem from '../src/models/VirtualItem.js';
import User from '../src/models/User.js';
import { ensureFolderPath } from '../src/services/folderService.js';

let app;
let server;
let baseUrl;
let authToken;
let testUser;

before(async () => {
  process.env.NODE_ENV = 'test';
  await connectDB();

  const appModule = await import('../src/index.js');
  app = appModule.default;

  server = http.createServer(app);
  await new Promise((resolve) => {
    server.listen(0, () => {
      const port = server.address().port;
      baseUrl = `http://localhost:${port}`;
      resolve();
    });
  });

  // Create test user and obtain auth token
  const res = await fetch(`${baseUrl}/api/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: 'Media Test User',
      email: `mediatest_${Date.now()}@example.com`,
      password: 'Password123!',
    }),
  });
  const data = await res.json();
  authToken = data.token;
  testUser = data.user;
});

after(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
  await disconnectDB();
});

test('1. Folder Service: ensureFolderPath creates recursive folder tree idempotently', async () => {
  const userId = testUser.id;
  const targetPath = '/Photos & Videos/2026/10';

  // First call: should create all levels
  const folderId1 = await ensureFolderPath(userId, targetPath);
  assert.ok(folderId1, 'Should return target leaf folder ID');

  const leafFolder = await VirtualItem.findById(folderId1);
  assert.equal(leafFolder.name, '10');
  assert.equal(leafFolder.type, 'folder');

  const parentFolder = await VirtualItem.findById(leafFolder.parentId);
  assert.equal(parentFolder.name, '2026');

  const rootFolder = await VirtualItem.findById(parentFolder.parentId);
  assert.equal(rootFolder.name, 'Photos & Videos');
  assert.equal(rootFolder.parentId, null);

  // Second call: should be idempotent and return exact same ID without duplicating
  const folderId2 = await ensureFolderPath(userId, targetPath);
  assert.equal(folderId1.toString(), folderId2.toString(), 'Should return identical leaf folder ID');

  const folderCount = await VirtualItem.countDocuments({
    userId,
    type: 'folder',
    name: '10',
  });
  assert.equal(folderCount, 1, 'Should not create duplicate folders');
});

test('2. Deduplication API: checkMediaExists detects existing media items', async () => {
  const userId = testUser.id;
  const deviceId = 'test_phone_device_001';
  const assetId1 = 'camera_asset_001';
  const checksum1 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

  // Seed a backed-up item into DB
  const seeded = new VirtualItem({
    userId,
    name: 'IMG_20261012_001.jpg',
    type: 'file',
    size: 2048500,
    mimeType: 'image/jpeg',
    status: 'ready',
    checksum: checksum1,
    mediaMetadata: {
      takenAt: new Date('2026-10-12T10:00:00.000Z'),
      width: 4000,
      height: 3000,
      deviceId,
      deviceAssetId: assetId1,
    },
  });
  await seeded.save();

  // Query checkMediaExists with a list containing existing asset and a brand new asset
  const res = await fetch(`${baseUrl}/api/files/check-media-exists`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${authToken}`,
    },
    body: JSON.stringify({
      assets: [
        {
          deviceAssetId: assetId1,
          deviceId,
          checksum: checksum1,
          name: 'IMG_20261012_001.jpg',
          size: 2048500,
        },
        {
          deviceAssetId: 'camera_asset_999_new',
          deviceId,
          name: 'IMG_20261012_999.jpg',
          size: 3000000,
        },
      ],
    }),
  });

  assert.equal(res.status, 200);
  const data = await res.json();
  assert.ok(Array.isArray(data.backedUpAssetIds));
  assert.equal(data.backedUpAssetIds.length, 1);
  assert.equal(data.backedUpAssetIds[0], assetId1, 'Should flag assetId1 as backed up');
  assert.equal(data.existing.length, 1);
  assert.equal(data.existing[0].name, 'IMG_20261012_001.jpg');
});

test('3. Timeline API: getPhotoTimeline returns reverse-chronological media feed', async () => {
  const userId = testUser.id;
  // Seed two media files with different takenAt dates
  const olderDate = new Date('2026-09-01T12:00:00.000Z');
  const newerDate = new Date('2026-10-10T15:30:00.000Z');

  await VirtualItem.create([
    {
      userId,
      name: 'vacation_september.jpg',
      type: 'file',
      size: 1024,
      mimeType: 'image/jpeg',
      status: 'ready',
      mediaMetadata: { takenAt: olderDate },
    },
    {
      userId,
      name: 'birthday_october.mp4',
      type: 'file',
      size: 5048,
      mimeType: 'video/mp4',
      status: 'ready',
      mediaMetadata: { takenAt: newerDate, duration: 45 },
    },
  ]);

  const res = await fetch(`${baseUrl}/api/files/photos/timeline?limit=10`, {
    headers: { Authorization: `Bearer ${authToken}` },
  });

  assert.equal(res.status, 200);
  const data = await res.json();

  assert.ok(data.items.length >= 2, 'Should return at least the 2 seeded items');
  // Verify strict reverse chronological sorting
  for (let i = 0; i < data.items.length - 1; i++) {
    const current = new Date(data.items[i].mediaMetadata?.takenAt || data.items[i].createdAt).getTime();
    const next = new Date(data.items[i + 1].mediaMetadata?.takenAt || data.items[i + 1].createdAt).getTime();
    assert.ok(current >= next, 'Timeline items must be sorted in reverse chronological order');
  }
});
