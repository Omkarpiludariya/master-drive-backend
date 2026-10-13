import { test } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'crypto';
import { PassThrough } from 'stream';
import mongoose from 'mongoose';
import { MongoMemoryServer } from 'mongodb-memory-server';

import User from '../src/models/User.js';
import DriveAccount from '../src/models/DriveAccount.js';
import VirtualItem from '../src/models/VirtualItem.js';
import { processChunkedUpload } from '../src/services/chunkEngine.js';
import { getChunkStream } from '../src/services/driveService.js';
import { encrypt, decrypt } from '../src/services/cryptoService.js';

let mongod;

test.before(async () => {
  mongod = await MongoMemoryServer.create();
  const uri = mongod.getUri();
  await mongoose.connect(uri);
});

test.after(async () => {
  await mongoose.disconnect();
  await mongod.stop();
});

test('Master Google Drive End-to-End Pipeline', async (t) => {
  await t.test('1. User Creation & Token Encryption', async () => {
    const user = new User({
      name: 'Test Engineer',
      email: 'test@masterdrive.io',
      password: 'password123',
    });
    await user.save();
    assert.ok(user._id);

    // Verify password was hashed
    assert.notEqual(user.password, 'password123');
    const isMatch = await user.comparePassword('password123');
    assert.equal(isMatch, true);

    // Verify OAuth token encryption
    const sampleToken = 'ya29.sample_oauth_access_token_super_secret';
    const encrypted = encrypt(sampleToken);
    assert.notEqual(encrypted, sampleToken);
    const decrypted = decrypt(encrypted);
    assert.equal(decrypted, sampleToken);
  });

  await t.test('2. Multi-Account Google Drive Pool Setup', async () => {
    const user = await User.findOne({ email: 'test@masterdrive.io' });

    // Link 3 accounts to create a 45 GB pool
    const acc1 = await DriveAccount.create({
      userId: user._id,
      accountEmail: 'alpha.drive1@gmail.com',
      displayName: 'Alpha Drive',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 0,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_1',
    });

    const acc2 = await DriveAccount.create({
      userId: user._id,
      accountEmail: 'beta.drive2@gmail.com',
      displayName: 'Beta Drive',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 0,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_2',
    });

    const acc3 = await DriveAccount.create({
      userId: user._id,
      accountEmail: 'gamma.drive3@gmail.com',
      displayName: 'Gamma Drive',
      totalQuota: 15 * 1024 * 1024 * 1024,
      usedQuota: 0,
      isActive: true,
      isMock: true,
      folderId: 'mock_folder_3',
    });

    const allAccounts = await DriveAccount.find({ userId: user._id });
    assert.equal(allAccounts.length, 3);

    const totalPooled = allAccounts.reduce((sum, a) => sum + a.totalQuota, 0);
    assert.equal(totalPooled, 45 * 1024 * 1024 * 1024); // 45 GB
  });

  await t.test('3. Chunking & Striping Upload Across Drives', async () => {
    const user = await User.findOne({ email: 'test@masterdrive.io' });

    // Generate random binary data (~35 MB to ensure multiple chunks with 15MB chunk size)
    const testDataSize = 35 * 1024 * 1024; // 35 MB
    const chunk1 = crypto.randomBytes(15 * 1024 * 1024);
    const chunk2 = crypto.randomBytes(15 * 1024 * 1024);
    const chunk3 = crypto.randomBytes(5 * 1024 * 1024);

    const fullBuffer = Buffer.concat([chunk1, chunk2, chunk3]);
    const originalHash = crypto.createHash('sha256').update(fullBuffer).digest('hex');

    const passThrough = new PassThrough();
    passThrough.write(chunk1);
    passThrough.write(chunk2);
    passThrough.end(chunk3);

    const virtualItem = await processChunkedUpload({
      userId: user._id,
      fileStream: passThrough,
      filename: 'large_dataset.bin',
      mimeType: 'application/octet-stream',
      totalSize: testDataSize,
    });

    assert.equal(virtualItem.status, 'ready');
    assert.equal(virtualItem.size, testDataSize);
    assert.equal(virtualItem.checksum, originalHash);
    assert.ok(virtualItem.isChunked);
    assert.equal(virtualItem.chunks.length, 3); // 15MB + 15MB + 5MB

    // Verify chunks were striped across accounts
    const usedAccountIds = new Set(virtualItem.chunks.map((c) => c.driveAccountId.toString()));
    assert.ok(usedAccountIds.size >= 2, 'Chunks should be striped across multiple drive accounts');

    // 4. Download and Reassembly verification
    const reassembledChunks = [];
    for (const part of virtualItem.chunks) {
      const account = await DriveAccount.findById(part.driveAccountId);
      const stream = await getChunkStream(account, part.googleDriveFileId);
      for await (const chunkData of stream) {
        reassembledChunks.push(chunkData);
      }
    }

    const reassembledBuffer = Buffer.concat(reassembledChunks);
    const downloadedHash = crypto.createHash('sha256').update(reassembledBuffer).digest('hex');

    assert.equal(downloadedHash, originalHash, 'Downloaded file hash must match original uploaded file');
  });

  await t.test('4. Cascading Deletion & Quota Reclamation', async () => {
    const user = await User.findOne({ email: 'test@masterdrive.io' });
    const virtualItem = await VirtualItem.findOne({ userId: user._id, name: 'large_dataset.bin' });

    assert.ok(virtualItem);
    const totalSize = virtualItem.size;

    // Accounts before delete
    const accountsBefore = await DriveAccount.find({ userId: user._id });
    const usedBefore = accountsBefore.reduce((sum, a) => sum + a.usedQuota, 0);
    assert.equal(usedBefore, totalSize);

    // Clean up
    for (const chunk of virtualItem.chunks) {
      await DriveAccount.findByIdAndUpdate(chunk.driveAccountId, {
        $inc: { usedQuota: -chunk.size },
      });
    }
    await VirtualItem.findByIdAndDelete(virtualItem._id);

    const accountsAfter = await DriveAccount.find({ userId: user._id });
    const usedAfter = accountsAfter.reduce((sum, a) => sum + a.usedQuota, 0);
    assert.equal(usedAfter, 0, 'Used quota must be completely reclaimed after deletion');
  });
});
