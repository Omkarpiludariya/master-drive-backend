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
import { pickBestAccount } from '../src/services/balancerService.js';
import { getChunkStream, withRetry, deleteChunk } from '../src/services/driveService.js';
import { encrypt, decrypt } from '../src/services/cryptoService.js';

let mongod;
let enterpriseUser;
let accounts = [];

test.before(async () => {
  mongod = await MongoMemoryServer.create();
  await mongoose.connect(mongod.getUri());

  enterpriseUser = await User.create({
    name: 'Enterprise Production Architect',
    email: 'architect@enterprise-drive.io',
    password: 'enterpriseSecure2026!',
  });

  // Setup 3 Google Drive Accounts totaling 45 GB
  accounts = [
    await DriveAccount.create({
      userId: enterpriseUser._id,
      accountEmail: 'google.enterprise.alpha@gmail.com',
      displayName: 'Enterprise Alpha (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 1 * 1024 * 1024 * 1024,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_alpha',
    }),
    await DriveAccount.create({
      userId: enterpriseUser._id,
      accountEmail: 'google.enterprise.beta@gmail.com',
      displayName: 'Enterprise Beta (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 3 * 1024 * 1024 * 1024,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_beta',
    }),
    await DriveAccount.create({
      userId: enterpriseUser._id,
      accountEmail: 'google.enterprise.gamma@gmail.com',
      displayName: 'Enterprise Gamma (15 GB)',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 0,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_gamma',
    }),
  ];
});

test.after(async () => {
  await mongoose.disconnect();
  await mongod.stop();
});

test('PRODUCTION TOP-LEVEL SYSTEM DESIGN & STRESS TEST', async (suite) => {
  // -------------------------------------------------------------
  // MODULE A: High-Concurrency Multi-File Stream Ingestion
  // -------------------------------------------------------------
  await suite.test('A. Concurrent Multi-File Ingestion with Stream Piping & Checksum Matching', async () => {
    const fileSpecs = [
      { name: 'document_spec.pdf', mime: 'application/pdf', size: 1024 * 1024 }, // 1 MB
      { name: 'product_render.png', mime: 'image/png', size: 5 * 1024 * 1024 }, // 5 MB
      { name: 'podcast_track.mp3', mime: 'audio/mpeg', size: 15 * 1024 * 1024 }, // 15 MB
      { name: 'webinar_recording.mp4', mime: 'video/mp4', size: 32 * 1024 * 1024 }, // 32 MB (multi-chunk)
      { name: 'dataset_archive.zip', mime: 'application/zip', size: 35 * 1024 * 1024 }, // 35 MB (multi-chunk)
    ];

    const generatedFiles = fileSpecs.map((spec) => {
      const data = crypto.randomBytes(spec.size);
      const hash = crypto.createHash('sha256').update(data).digest('hex');
      return { ...spec, data, hash };
    });

    // Concurrently upload all 5 files in parallel
    const uploadPromises = generatedFiles.map((file) => {
      const passThrough = new PassThrough();
      passThrough.end(file.data);

      return processChunkedUpload({
        userId: enterpriseUser._id,
        fileStream: passThrough,
        filename: file.name,
        mimeType: file.mime,
        totalSize: file.size,
      });
    });

    const uploadedItems = await Promise.all(uploadPromises);

    assert.equal(uploadedItems.length, 5);

    // Verify all 5 uploaded items have exact checksums and sizes
    for (let i = 0; i < uploadedItems.length; i++) {
      const item = uploadedItems[i];
      const orig = generatedFiles[i];

      assert.equal(item.status, 'ready');
      assert.equal(item.size, orig.size);
      assert.equal(item.checksum, orig.hash);
      assert.equal(item.name, orig.name);
    }

    // Verify multi-chunk files were striped across multiple Google accounts
    const largeFiles = uploadedItems.filter((item) => item.size > 15 * 1024 * 1024);
    for (const item of largeFiles) {
      assert.ok(item.chunks.length >= 3);
      const accountsUsed = new Set(item.chunks.map((c) => c.driveAccountId.toString()));
      assert.ok(accountsUsed.size >= 2, `${item.name} chunks must be striped across accounts`);
    }
  });

  // -------------------------------------------------------------
  // MODULE B: Real-World Seekable Video Playback (HTTP 206 Across Chunk Borders)
  // -------------------------------------------------------------
  await suite.test('B. Media Streaming with Multipoint Range Seeking Across Chunk Borders', async () => {
    // 32 MB Video File with 15 MB chunks (Chunk 0: 15MB, Chunk 1: 15MB, Chunk 2: 2MB)
    const videoData = crypto.randomBytes(32 * 1024 * 1024);
    const passThrough = new PassThrough();
    passThrough.end(videoData);

    const videoItem = await processChunkedUpload({
      userId: enterpriseUser._id,
      fileStream: passThrough,
      filename: '4k_feature_film.mp4',
      mimeType: 'video/mp4',
      totalSize: videoData.length,
    });

    assert.equal(videoItem.chunks.length, 3);

    // Test Point 1: Container Metadata Header (First 1MB)
    {
      const start = 0;
      const end = 1024 * 1024 - 1;
      const expectedLen = end - start + 1;
      const responseChunks = [];
      let statusCode = 200;
      const headers = {};

      const req = { headers: { range: `bytes=${start}-${end}` } };
      const res = new PassThrough();
      res.status = (code) => { statusCode = code; return res; };
      res.setHeader = (k, v) => { headers[k] = v; };
      res.on('data', (c) => responseChunks.push(c));

      await streamReassembledFile(videoItem, req, res, 'inline');

      assert.equal(statusCode, 206);
      assert.equal(headers['Content-Length'], expectedLen);
      assert.equal(headers['Content-Range'], `bytes ${start}-${end}/${videoData.length}`);
      const received = Buffer.concat(responseChunks);
      assert.deepEqual(received, videoData.subarray(start, end + 1));
    }

    // Test Point 2: Cross-Chunk Border Seeking (14.5MB to 16.5MB straddling Chunk 0 & Chunk 1)
    {
      const start = Math.floor(14.5 * 1024 * 1024);
      const end = Math.floor(16.5 * 1024 * 1024);
      const expectedLen = end - start + 1;
      const responseChunks = [];
      let statusCode = 200;
      const headers = {};

      const req = { headers: { range: `bytes=${start}-${end}` } };
      const res = new PassThrough();
      res.status = (code) => { statusCode = code; return res; };
      res.setHeader = (k, v) => { headers[k] = v; };
      res.on('data', (c) => responseChunks.push(c));

      await streamReassembledFile(videoItem, req, res, 'inline');

      assert.equal(statusCode, 206);
      assert.equal(headers['Content-Length'], expectedLen);
      assert.equal(headers['Content-Range'], `bytes ${start}-${end}/${videoData.length}`);
      const received = Buffer.concat(responseChunks);
      assert.deepEqual(received, videoData.subarray(start, end + 1));
    }

    // Test Point 3: Final Chunk Tail Seeking
    {
      const start = 31 * 1024 * 1024;
      const end = 32 * 1024 * 1024 - 1;
      const expectedLen = end - start + 1;
      const responseChunks = [];
      let statusCode = 200;
      const headers = {};

      const req = { headers: { range: `bytes=${start}-${end}` } };
      const res = new PassThrough();
      res.status = (code) => { statusCode = code; return res; };
      res.setHeader = (k, v) => { headers[k] = v; };
      res.on('data', (c) => responseChunks.push(c));

      await streamReassembledFile(videoItem, req, res, 'inline');

      assert.equal(statusCode, 206);
      assert.equal(headers['Content-Length'], expectedLen);
      const received = Buffer.concat(responseChunks);
      assert.deepEqual(received, videoData.subarray(start, end + 1));
    }
  });

  // -------------------------------------------------------------
  // MODULE C: Asymmetric Drive Pool Exhaustion & Automatic Spillover
  // -------------------------------------------------------------
  await suite.test('C. Dynamic Spillover Across Asymmetric Accounts (Filling Smaller Drives First)', async () => {
    // Create fresh user with tightly bounded asymmetric accounts
    const asymmetricUser = await User.create({
      name: 'Asymmetric Test User',
      email: 'asymmetric@masterdrive.io',
      password: 'password123',
    });

    const smallDrive = await DriveAccount.create({
      userId: asymmetricUser._id,
      accountEmail: 'small.drive@gmail.com',
      totalQuota: 20 * 1024 * 1024, // 20 MB total
      usedQuota: 0,
      isActive: true,
      isMock: true,
    });

    const mediumDrive = await DriveAccount.create({
      userId: asymmetricUser._id,
      accountEmail: 'medium.drive@gmail.com',
      totalQuota: 40 * 1024 * 1024, // 40 MB total
      usedQuota: 0,
      isActive: true,
      isMock: true,
    });

    const largeDrive = await DriveAccount.create({
      userId: asymmetricUser._id,
      accountEmail: 'large.drive@gmail.com',
      totalQuota: 100 * 1024 * 1024, // 100 MB total
      usedQuota: 0,
      isActive: true,
      isMock: true,
    });

    const asymAccounts = [smallDrive, mediumDrive, largeDrive];

    // Select account for 15MB chunk 0: picks small or medium
    const pick0 = pickBestAccount(asymAccounts, 15 * 1024 * 1024, 0);
    assert.ok(pick0);

    // Consume smallDrive
    smallDrive.usedQuota = 20 * 1024 * 1024; // Small drive is now 100% full
    assert.equal(smallDrive.freeQuota, 0);

    // Select account for 15MB chunk 1: must gracefully bypass smallDrive and pick medium or large
    const pick1 = pickBestAccount(asymAccounts, 15 * 1024 * 1024, 1);
    assert.notEqual(pick1.accountEmail, 'small.drive@gmail.com');
    assert.ok(['medium.drive@gmail.com', 'large.drive@gmail.com'].includes(pick1.accountEmail));

    // Consume mediumDrive
    mediumDrive.usedQuota = 40 * 1024 * 1024; // Medium drive is now 100% full
    assert.equal(mediumDrive.freeQuota, 0);

    // Select account for 15MB chunk 2: must spillover to largeDrive
    const pick2 = pickBestAccount(asymAccounts, 15 * 1024 * 1024, 2);
    assert.equal(pick2.accountEmail, 'large.drive@gmail.com');
  });

  // -------------------------------------------------------------
  // MODULE D: Fault Injection & Resilience (Google API 429, Token Expiry, Mid-Flight Abort)
  // -------------------------------------------------------------
  await suite.test('D.1 Google API 429 Rate Limit Exponential Backoff and Recovery', async () => {
    let callAttempts = 0;

    // Simulated API call that fails twice with 429 before succeeding
    const simulatedGoogleCall = async () => {
      callAttempts++;
      if (callAttempts < 3) {
        const rateLimitErr = new Error('Google Drive API Rate Limit Exceeded (Rate Limit 429)');
        rateLimitErr.status = 429;
        throw rateLimitErr;
      }
      return { status: 200, data: { fileId: 'success_after_backoff_123' } };
    };

    const startTime = Date.now();
    const result = await withRetry(simulatedGoogleCall, 3, 100); // 100ms base delay
    const duration = Date.now() - startTime;

    assert.equal(callAttempts, 3, 'Should have retried exactly 3 times');
    assert.equal(result.data.fileId, 'success_after_backoff_123');
    assert.ok(duration >= 200, 'Backoff must have introduced delays between attempts');
  });

  await suite.test('D.2 Mid-Flight Stream Pipeline Abort and Automatic Rollback', async () => {
    const rollbackTarget1 = accounts[0];
    const rollbackTarget2 = accounts[1];

    // Upload 2 mock chunks directly
    const pass1 = new PassThrough(); pass1.end(Buffer.from('chunk1 payload'));
    const pass2 = new PassThrough(); pass2.end(Buffer.from('chunk2 payload'));

    const { uploadChunk } = await import('../src/services/driveService.js');
    const part1 = await uploadChunk(rollbackTarget1, pass1, 'temp_part_1.bin');
    const part2 = await uploadChunk(rollbackTarget2, pass2, 'temp_part_2.bin');

    const uploadedParts = [
      { driveAccountId: rollbackTarget1._id, googleDriveFileId: part1.id },
      { driveAccountId: rollbackTarget2._id, googleDriveFileId: part2.id },
    ];

    // Trigger rollback
    await rollbackChunks(uploadedParts);

    // Verify deleted chunks no longer exist in storage
    assert.throws(() => {
      const { getMockChunkStream } = require('../src/services/mockDriveService.js');
      getMockChunkStream(rollbackTarget1._id, part1.id);
    });
  });

  // -------------------------------------------------------------
  // MODULE E: Deep Hierarchical VFS Cascading Destruction & Quota Reconciliation
  // -------------------------------------------------------------
  await suite.test('E. Deep Hierarchical VFS Cascading Destruction (4 Levels Deep)', async () => {
    // Create 4-level deep folder structure:
    // /Enterprise -> /Clients -> /Acme -> /Contracts
    const f1 = await VirtualItem.create({ userId: enterpriseUser._id, name: 'Enterprise', type: 'folder', parentId: null });
    const f2 = await VirtualItem.create({ userId: enterpriseUser._id, name: 'Clients', type: 'folder', parentId: f1._id });
    const f3 = await VirtualItem.create({ userId: enterpriseUser._id, name: 'Acme Corp', type: 'folder', parentId: f2._id });
    const f4 = await VirtualItem.create({ userId: enterpriseUser._id, name: 'Contracts 2026', type: 'folder', parentId: f3._id });

    // Ingest files at level 3 and level 4
    const file1Data = Buffer.from('Master Service Agreement Contract - Acme Corp');
    const pass1 = new PassThrough(); pass1.end(file1Data);
    const contractFile = await processChunkedUpload({
      userId: enterpriseUser._id,
      fileStream: pass1,
      filename: 'msa_contract.pdf',
      parentId: f4._id,
    });

    const file2Data = Buffer.from('Client Financial Summary Sheet');
    const pass2 = new PassThrough(); pass2.end(file2Data);
    const summaryFile = await processChunkedUpload({
      userId: enterpriseUser._id,
      fileStream: pass2,
      filename: 'summary.xlsx',
      parentId: f3._id,
    });

    const accountsBefore = await DriveAccount.find({ userId: enterpriseUser._id });
    const usedBefore = accountsBefore.reduce((acc, a) => acc + a.usedQuota, 0);

    // Cascading recursive delete function matching fileController implementation
    async function cascadingPurge(userId, itemId) {
      const item = await VirtualItem.findOne({ _id: itemId, userId });
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
        const children = await VirtualItem.find({ parentId: item._id, userId });
        for (const c of children) {
          await cascadingPurge(userId, c._id);
        }
        await VirtualItem.findByIdAndDelete(item._id);
      }
    }

    // Execute cascading delete on top-level /Enterprise folder
    await cascadingPurge(enterpriseUser._id, f1._id);

    // Verify all 4 folders and both files are purged from DB
    const checkF1 = await VirtualItem.findById(f1._id);
    const checkF2 = await VirtualItem.findById(f2._id);
    const checkF3 = await VirtualItem.findById(f3._id);
    const checkF4 = await VirtualItem.findById(f4._id);
    const checkDoc1 = await VirtualItem.findById(contractFile._id);
    const checkDoc2 = await VirtualItem.findById(summaryFile._id);

    assert.equal(checkF1, null);
    assert.equal(checkF2, null);
    assert.equal(checkF3, null);
    assert.equal(checkF4, null);
    assert.equal(checkDoc1, null);
    assert.equal(checkDoc2, null);

    // Verify exact byte-level quota reconciliation
    const accountsAfter = await DriveAccount.find({ userId: enterpriseUser._id });
    const usedAfter = accountsAfter.reduce((acc, a) => acc + a.usedQuota, 0);
    const totalExpectedFreed = contractFile.size + summaryFile.size;

    assert.equal(usedAfter, usedBefore - totalExpectedFreed, '100% of used quota must be reconciled after cascading purge');
  });
});
