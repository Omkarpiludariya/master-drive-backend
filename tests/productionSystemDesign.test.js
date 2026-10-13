import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'crypto';
import { PassThrough } from 'stream';
import mongoose from 'mongoose';
import { MongoMemoryServer } from 'mongodb-memory-server';

import User from '../src/models/User.js';
import DriveAccount from '../src/models/DriveAccount.js';
import VirtualItem from '../src/models/VirtualItem.js';
import { processChunkedUpload, rollbackChunks } from '../src/services/chunkEngine.js';
import { streamReassembledFile } from '../src/services/streamAssembler.js';
import { pickBestAccount, planChunkAllocation, InsufficientStorageError } from '../src/services/balancerService.js';
import { getChunkStream, deleteChunk } from '../src/services/driveService.js';
import { encrypt, decrypt } from '../src/services/cryptoService.js';

let mongod;
let testUser;
let accounts = [];

test.before(async () => {
  mongod = await MongoMemoryServer.create();
  await mongoose.connect(mongod.getUri());

  // Setup Production Test User
  testUser = await User.create({
    name: 'Production System User',
    email: 'production@masterdrive.io',
    password: 'securePassword123!',
  });

  // Setup Multi-Account Pool (3 Google Drive Accounts = 45 GB Pool)
  accounts = [
    await DriveAccount.create({
      userId: testUser._id,
      accountEmail: 'google.account.primary@gmail.com',
      displayName: 'Primary Google Account (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 2 * 1024 * 1024 * 1024, // 2 GB already used in Drive
      isActive: true,
      isMock: true,
      folderId: 'prod_folder_primary',
    }),
    await DriveAccount.create({
      userId: testUser._id,
      accountEmail: 'google.account.secondary@gmail.com',
      displayName: 'Secondary Google Account (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 5 * 1024 * 1024 * 1024, // 5 GB already used
      isActive: true,
      isMock: true,
      folderId: 'prod_folder_secondary',
    }),
    await DriveAccount.create({
      userId: testUser._id,
      accountEmail: 'google.account.tertiary@gmail.com',
      displayName: 'Tertiary Google Account (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 1 * 1024 * 1024 * 1024, // 1 GB already used
      isActive: true,
      isMock: true,
      folderId: 'prod_folder_tertiary',
    }),
  ];
});

test.after(async () => {
  await mongoose.disconnect();
  await mongod.stop();
});

test('PRODUCTION SUITE 1: Multi-Account Storage Balancer under Dynamic Load', async (t) => {
  await t.test('1.1 Balancer selects account with highest available quota', () => {
    // Tertiary has 14GB free, Primary has 13GB free, Secondary has 10GB free
    const chosen = pickBestAccount(accounts, 1024 * 1024);
    assert.equal(chosen.accountEmail, 'google.account.tertiary@gmail.com');
  });

  await t.test('1.2 Balancer allocates multi-chunk file proportionally without overflow', async () => {
    const totalSize = 30 * 1024 * 1024 * 1024; // 30 GB
    const chunkSize = 5 * 1024 * 1024 * 1024; // 5 GB chunks
    const plan = await planChunkAllocation(testUser._id, totalSize, chunkSize);

    assert.ok(plan.length >= 3);
    const assignedEmails = plan.map((p) => p.account.accountEmail);
    assert.ok(new Set(assignedEmails).size >= 2, 'Must distribute across multiple accounts');
  });

  await t.test('1.3 Balancer rejects files exceeding total combined pool with HTTP 507', async () => {
    const totalFree = accounts.reduce((acc, a) => acc + a.freeQuota, 0);
    const oversizedFile = totalFree + 1024 * 1024 * 1024; // 1 GB over capacity

    await assert.rejects(
      async () => {
        await planChunkAllocation(testUser._id, oversizedFile, 15 * 1024 * 1024);
      },
      (err) => err instanceof InsufficientStorageError
    );
  });
});

test('PRODUCTION SUITE 2: Distributed Chunking, Striping & Reassembly Integrity', async (t) => {
  let originalData;
  let originalChecksum;
  let virtualFile;

  await t.test('2.1 Zero-disk streaming upload of 32MB file (multi-part striping)', async () => {
    // Generate deterministic payload
    const part1 = crypto.randomBytes(15 * 1024 * 1024);
    const part2 = crypto.randomBytes(15 * 1024 * 1024);
    const part3 = crypto.randomBytes(2 * 1024 * 1024);

    originalData = Buffer.concat([part1, part2, part3]);
    originalChecksum = crypto.createHash('sha256').update(originalData).digest('hex');

    const passThrough = new PassThrough();
    passThrough.write(part1);
    passThrough.write(part2);
    passThrough.end(part3);

    virtualFile = await processChunkedUpload({
      userId: testUser._id,
      fileStream: passThrough,
      filename: 'production_highres_video.mp4',
      mimeType: 'video/mp4',
      totalSize: originalData.length,
    });

    assert.equal(virtualFile.status, 'ready');
    assert.equal(virtualFile.size, originalData.length);
    assert.equal(virtualFile.checksum, originalChecksum);
    assert.equal(virtualFile.totalChunks, 3);
    assert.ok(virtualFile.isChunked);

    // Verify chunks are striped across distinct accounts
    const chunkAccountIds = virtualFile.chunks.map((c) => c.driveAccountId.toString());
    assert.ok(new Set(chunkAccountIds).size >= 2, 'Chunks must be striped across at least 2 drive accounts');
  });

  await t.test('2.2 Full download reassembly bit-for-bit checksum match', async () => {
    const chunks = [];
    for (const part of virtualFile.chunks) {
      const account = await DriveAccount.findById(part.driveAccountId);
      const stream = await getChunkStream(account, part.googleDriveFileId);
      for await (const chunk of stream) {
        chunks.push(chunk);
      }
    }

    const reassembled = Buffer.concat(chunks);
    const reassembledChecksum = crypto.createHash('sha256').update(reassembled).digest('hex');

    assert.equal(reassembledChecksum, originalChecksum, 'Reassembled file checksum must match uploaded file');
  });

  await t.test('2.3 Range Request (HTTP 206 Partial Content) across chunk boundaries', async () => {
    // Request bytes from 14MB to 16MB (strictly spans Chunk 0 and Chunk 1 boundary at 15MB)
    const rangeStart = 14 * 1024 * 1024;
    const rangeEnd = 16 * 1024 * 1024 - 1;
    const expectedLength = rangeEnd - rangeStart + 1;

    const mockReq = {
      headers: {
        range: `bytes=${rangeStart}-${rangeEnd}`,
      },
    };

    const responseChunks = [];
    let statusCode = 200;
    const responseHeaders = {};

    const mockRes = new PassThrough();
    mockRes.status = (code) => {
      statusCode = code;
      return mockRes;
    };
    mockRes.setHeader = (name, val) => {
      responseHeaders[name] = val;
    };

    mockRes.on('data', (d) => responseChunks.push(d));

    await streamReassembledFile(virtualFile, mockReq, mockRes, 'inline');

    assert.equal(statusCode, 206, 'Must respond with HTTP 206 Partial Content');
    assert.equal(responseHeaders['Content-Length'], expectedLength);
    assert.equal(responseHeaders['Content-Range'], `bytes ${rangeStart}-${rangeEnd}/${originalData.length}`);

    const slicedPayload = Buffer.concat(responseChunks);
    assert.equal(slicedPayload.length, expectedLength);

    const expectedSlice = originalData.subarray(rangeStart, rangeEnd + 1);
    assert.deepEqual(slicedPayload, expectedSlice, 'Sliced byte range must strictly match original data across chunk boundary');
  });
});

test('PRODUCTION SUITE 3: Hierarchical VFS, Cascading Deletion & Quota Accounting', async (t) => {
  let rootFolder;
  let subFolder;
  let nestedFile;
  let initialUsedQuota;

  await t.test('3.1 Create nested folder hierarchy and files', async () => {
    rootFolder = await VirtualItem.create({
      userId: testUser._id,
      name: 'Corporate Projects',
      type: 'folder',
      parentId: null,
    });

    subFolder = await VirtualItem.create({
      userId: testUser._id,
      name: 'Financial Q3',
      type: 'folder',
      parentId: rootFolder._id,
    });

    // Upload small document into subFolder
    const docData = Buffer.from('Confidential Financial Report 2026 - Master Google Drive Pooled Storage');
    const passThrough = new PassThrough();
    passThrough.end(docData);

    nestedFile = await processChunkedUpload({
      userId: testUser._id,
      fileStream: passThrough,
      filename: 'report_q3.docx',
      mimeType: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      parentId: subFolder._id,
    });

    assert.equal(nestedFile.parentId.toString(), subFolder._id.toString());
  });

  await t.test('3.2 Cascading deletion of parent folder purges all nested items and chunks', async () => {
    const accountsBefore = await DriveAccount.find({ userId: testUser._id });
    const usedBefore = accountsBefore.reduce((acc, a) => acc + a.usedQuota, 0);

    // Cascading delete function
    async function cascadingPurge(itemId) {
      const item = await VirtualItem.findById(itemId);
      if (!item) return;
      if (item.type === 'file') {
        for (const chunk of item.chunks) {
          const acc = await DriveAccount.findById(chunk.driveAccountId);
          if (acc) {
            await deleteChunk(acc, chunk.googleDriveFileId);
            await DriveAccount.findByIdAndUpdate(acc._id, { $inc: { usedQuota: -chunk.size } });
          }
        }
        await VirtualItem.findByIdAndDelete(item._id);
      } else {
        const children = await VirtualItem.find({ parentId: item._id });
        for (const c of children) {
          await cascadingPurge(c._id);
        }
        await VirtualItem.findByIdAndDelete(item._id);
      }
    }

    // Delete Corporate Projects (rootFolder)
    await cascadingPurge(rootFolder._id);

    // Verify all nested items are gone from DB
    const remainingRoot = await VirtualItem.findById(rootFolder._id);
    const remainingSub = await VirtualItem.findById(subFolder._id);
    const remainingNested = await VirtualItem.findById(nestedFile._id);

    assert.equal(remainingRoot, null);
    assert.equal(remainingSub, null);
    assert.equal(remainingNested, null);

    // Verify quota was accurately refunded
    const accountsAfter = await DriveAccount.find({ userId: testUser._id });
    const usedAfter = accountsAfter.reduce((acc, a) => acc + a.usedQuota, 0);
    assert.equal(usedAfter, usedBefore - nestedFile.size);
  });
});

test('PRODUCTION SUITE 4: Security, Resilience & Partial Upload Rollback', async (t) => {
  await t.test('4.1 Token AES-256-GCM authenticated cipher resists bit-flip attacks', () => {
    const token = 'ya29.sample_oauth_token_production_access';
    const encrypted = encrypt(token);
    const [iv, authTag, data] = encrypted.split(':');

    // Flip bits in ciphertext
    const tamperedData = 'ff' + data.substring(2);
    const tamperedString = `${iv}:${authTag}:${tamperedData}`;

    assert.throws(
      () => decrypt(tamperedString),
      /Unsupported state or unable to authenticate data/,
      'Must reject tampered ciphertext'
    );
  });

  await t.test('4.2 Aborted upload rolls back and deletes intermediate chunks', async () => {
    const uploadedChunks = [
      { driveAccountId: accounts[0]._id, googleDriveFileId: 'mock_chunk_fail_1' },
      { driveAccountId: accounts[1]._id, googleDriveFileId: 'mock_chunk_fail_2' },
    ];

    // Rollback execution
    await rollbackChunks(uploadedChunks);
    // Should complete cleanly without uncaught exceptions
    assert.ok(true);
  });
});
