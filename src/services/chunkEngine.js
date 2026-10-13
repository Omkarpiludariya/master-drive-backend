import { PassThrough } from 'stream';
import crypto from 'crypto';
import sharp from 'sharp';
import DriveAccount from '../models/DriveAccount.js';
import VirtualItem from '../models/VirtualItem.js';
import { uploadChunk, deleteChunk } from './driveService.js';
import { getActiveAccounts, pickBestAccount } from './balancerService.js';

const DEFAULT_CHUNK_SIZE = (parseInt(process.env.DEFAULT_CHUNK_SIZE_MB, 10) || 8) * 1024 * 1024;
const DEFAULT_CONCURRENCY = Math.max(1, parseInt(process.env.UPLOAD_CONCURRENCY, 10) || 1);

/**
 * Rolls back uploaded chunks upon failure to prevent orphaned cloud storage
 */
export async function rollbackChunks(uploadedChunks) {
  console.warn(`[ChunkEngine] Rolling back ${uploadedChunks.length} uploaded chunks...`);
  for (const part of uploadedChunks) {
    try {
      const account = await DriveAccount.findById(part.driveAccountId);
      if (account) {
        await deleteChunk(account, part.googleDriveFileId);
        // Fully reconcile usedQuota to prevent ghost quota leaks on failed uploads
        if (part.size) {
          await DriveAccount.findByIdAndUpdate(account._id, {
            $inc: { usedQuota: -part.size },
          });
        }
      }
    } catch (err) {
      console.error(`[ChunkEngine] Rollback failed for chunk ${part.googleDriveFileId}:`, err.message);
    }
  }
}

/**
 * Streams incoming file, splits it into chunks in memory with parallel sliding window striping,
 * validates end-to-end checksums, generates instant image thumbnails, and persists the VFS record.
 *
 * @param {object} params
 * @param {string} params.userId
 * @param {ReadableStream} params.fileStream - Stream from Busboy
 * @param {string} params.filename
 * @param {string} params.mimeType
 * @param {number} params.totalSize - Optional hinted size
 * @param {string|null} params.parentId - Virtual folder parent ID
 * @param {function} [params.getClientChecksum] - Function to retrieve expected client SHA-256 hash
 * @param {function} [params.onProgress] - Optional chunk upload callback
 * @returns {Promise<object>} Created VirtualItem
 */
export async function processChunkedUpload({
  userId,
  fileStream,
  filename,
  mimeType,
  totalSize,
  parentId = null,
  mediaMetadata = null,
  getClientChecksum = null,
  onProgress = null,
}) {
  const chunkSize = DEFAULT_CHUNK_SIZE;
  const concurrencyLimit = DEFAULT_CONCURRENCY;
  const accounts = await getActiveAccounts(userId);

  if (!accounts || accounts.length === 0) {
    throw new Error('No active Google Drive accounts connected. Please link an account first.');
  }

  // Pre-create VirtualItem with uploading status
  const virtualItem = new VirtualItem({
    userId,
    name: filename,
    type: 'file',
    parentId: parentId || null,
    mimeType: mimeType || 'application/octet-stream',
    status: 'uploading',
    isChunked: false,
    size: 0,
    chunks: [],
    thumbnail: null,
    mediaMetadata: mediaMetadata
      ? {
          takenAt: mediaMetadata.takenAt ? new Date(mediaMetadata.takenAt) : null,
          width: Number(mediaMetadata.width) || null,
          height: Number(mediaMetadata.height) || null,
          duration: Number(mediaMetadata.duration) || null,
          deviceId: mediaMetadata.deviceId || null,
          deviceAssetId: mediaMetadata.deviceAssetId || null,
        }
      : null,
  });
  await virtualItem.save();

  const uploadedParts = [];
  const inFlightUploads = new Set();
  const fileHash = crypto.createHash('sha256');

  let currentPartIndex = 0;
  let totalBytesProcessed = 0;
  let activeBuffer = [];
  let activeBufferLength = 0;

  // Buffer for generating image thumbnail if file is an image
  const isImage = (mimeType && mimeType.startsWith('image/') && !mimeType.includes('svg')) ||
    /\.(png|jpe?g|webp|bmp|gif)$/i.test(filename);
  let thumbnailBuffer = isImage ? [] : null;
  let thumbnailBytesAccumulated = 0;

  // Sliding window chunk uploader
  async function queueChunkUpload(buffer, partIndex) {
    const chunkHash = crypto.createHash('md5').update(buffer).digest('hex');
    const targetAccount = pickBestAccount(accounts, buffer.length, partIndex);
    const chunkName = `_chk_${virtualItem._id}_part_${partIndex}.bin`;

    // Immediately allocate quota locally for in-flight chunks to prevent race conditions
    targetAccount.usedQuota += buffer.length;

    const task = (async () => {
      const passThrough = new PassThrough();
      passThrough.end(buffer);

      const uploadResult = await uploadChunk(targetAccount, passThrough, chunkName);

      const partInfo = {
        partIndex,
        driveAccountId: targetAccount._id,
        googleDriveFileId: uploadResult.id,
        size: buffer.length,
        md5: uploadResult.md5 || chunkHash,
      };

      uploadedParts.push(partInfo);

      // Persist account quota increment to database
      await DriveAccount.findByIdAndUpdate(targetAccount._id, {
        $inc: { usedQuota: buffer.length },
      });

      if (onProgress) {
        onProgress({
          partIndex,
          uploadedBytes: totalBytesProcessed,
          chunkSize: buffer.length,
          accountEmail: targetAccount.accountEmail,
        });
      }
    })();

    inFlightUploads.add(task);
    task.finally(() => inFlightUploads.delete(task));

    // If window is saturated, apply backpressure by waiting for earliest task
    if (inFlightUploads.size >= concurrencyLimit) {
      await Promise.race(inFlightUploads);
    }
  }

  try {
    for await (const data of fileStream) {
      fileHash.update(data);
      totalBytesProcessed += data.length;

      activeBuffer.push(data);
      activeBufferLength += data.length;

      // Accumulate up to 4MB for thumbnail generation
      if (thumbnailBuffer && thumbnailBytesAccumulated < 4 * 1024 * 1024) {
        thumbnailBuffer.push(data);
        thumbnailBytesAccumulated += data.length;
      }

      // When accumulated data reaches chunk threshold, dispatch chunk upload
      while (activeBufferLength >= chunkSize) {
        const fullBuffer = Buffer.concat(activeBuffer);
        const chunkData = fullBuffer.subarray(0, chunkSize);
        const remainder = fullBuffer.subarray(chunkSize);

        await queueChunkUpload(chunkData, currentPartIndex);
        currentPartIndex++;

        activeBuffer = remainder.length > 0 ? [remainder] : [];
        activeBufferLength = remainder.length;
      }
    }

    // Flush any remaining data as final chunk
    if (activeBufferLength > 0 || (uploadedParts.length === 0 && inFlightUploads.size === 0)) {
      const remainingBuffer = Buffer.concat(activeBuffer);
      await queueChunkUpload(remainingBuffer, currentPartIndex);
      currentPartIndex++;
    }

    // Drain all in-flight chunk upload promises
    await Promise.all(inFlightUploads);

    // End-to-end Checksum Verification
    const computedChecksum = fileHash.digest('hex');
    const expectedChecksum = typeof getClientChecksum === 'function' ? getClientChecksum() : null;

    if (expectedChecksum) {
      const cleanExpected = String(expectedChecksum).toLowerCase().trim();
      if (cleanExpected !== computedChecksum.toLowerCase()) {
        const mismatchErr = new Error(
          `Integrity checksum validation failed: client SHA-256 (${cleanExpected}) does not match server computed SHA-256 (${computedChecksum})`
        );
        mismatchErr.status = 400;
        mismatchErr.code = 'CHECKSUM_MISMATCH';
        throw mismatchErr;
      }
    }

    // Generate instant WebP thumbnail if file is an image
    if (thumbnailBuffer && thumbnailBuffer.length > 0) {
      try {
        const fullImgBuffer = Buffer.concat(thumbnailBuffer);
        const thumbBuf = await sharp(fullImgBuffer)
          .resize(256, 256, { fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 75 })
          .toBuffer();
        virtualItem.thumbnail = `data:image/webp;base64,${thumbBuf.toString('base64')}`;
      } catch (thumbErr) {
        console.warn(`[ChunkEngine] Thumbnail generation skipped for ${filename}:`, thumbErr.message);
      }
    }

    // Sort parts strictly in ascending order by partIndex
    uploadedParts.sort((a, b) => a.partIndex - b.partIndex);

    // Finalize virtual item
    virtualItem.status = 'ready';
    virtualItem.size = totalBytesProcessed;
    virtualItem.isChunked = uploadedParts.length > 1;
    virtualItem.totalChunks = uploadedParts.length;
    virtualItem.checksum = computedChecksum;
    virtualItem.chunks = uploadedParts;
    await virtualItem.save();

    return virtualItem;
  } catch (uploadError) {
    console.error('[ChunkEngine] Upload failed, initiating cleanup:', uploadError);
    // Ensure all in-flight writes finish before initiating rollback
    await Promise.allSettled(inFlightUploads);
    // Rollback any uploaded chunks
    await rollbackChunks(uploadedParts);
    // Delete failed virtual item
    await VirtualItem.findByIdAndDelete(virtualItem._id);
    throw uploadError;
  }
}

export default {
  processChunkedUpload,
  rollbackChunks,
};
