import busboy from 'busboy';
import crypto from 'crypto';
import { PassThrough } from 'stream';
import VirtualItem from '../models/VirtualItem.js';
import DriveAccount from '../models/DriveAccount.js';
import { processChunkedUpload } from '../services/chunkEngine.js';
import { streamReassembledFile } from '../services/streamAssembler.js';
import { deleteChunk, uploadChunk } from '../services/driveService.js';
import { broadcaster } from '../services/eventBroadcaster.js';
import { ensureFolderPath } from '../services/folderService.js';
import { getActiveAccounts, pickBestAccount } from '../services/balancerService.js';

/**
 * Builds breadcrumbs hierarchy from current folder up to root
 */
async function getBreadcrumbs(userId, folderId) {
  const crumbs = [{ id: null, name: 'My Drive' }];
  if (!folderId) return crumbs;

  let currentId = folderId;
  const pathItems = [];

  while (currentId) {
    const item = await VirtualItem.findOne({ _id: currentId, userId, type: 'folder' });
    if (!item) break;
    pathItems.unshift({ id: item._id, name: item.name });
    currentId = item.parentId;
  }

  return [...crumbs, ...pathItems];
}

/**
 * Real-time Server-Sent Events (SSE) Stream
 * Subscribes user devices/tabs to live file and storage pool events
 */
export async function subscribeEvents(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  res.write(': connected\n\n');

  // Keep-alive heartbeat every 25 seconds to prevent proxy / NAT timeouts
  const heartbeat = setInterval(() => {
    try {
      res.write(': ping\n\n');
    } catch (e) {
      clearInterval(heartbeat);
    }
  }, 25000);

  broadcaster.addClient(req.user._id, res);

  req.on('close', () => {
    clearInterval(heartbeat);
  });
}

/**
 * List files and folders with filtering, search, and breadcrumbs
 */
export async function getFiles(req, res, next) {
  try {
    const { parentId, isTrash, isStarred, search } = req.query;

    const filter = {
      userId: req.user._id,
      isTrash: isTrash === 'true',
    };

    if (isStarred === 'true') {
      filter.isStarred = true;
    } else if (search) {
      filter.name = { $regex: search, $options: 'i' };
    } else if (isTrash !== 'true') {
      filter.parentId = parentId && parentId !== 'null' ? parentId : null;
    }

    const items = await VirtualItem.find(filter)
      .sort({ type: -1, name: 1 })
      .populate('chunks.driveAccountId', 'accountEmail displayName');

    const breadcrumbs = await getBreadcrumbs(
      req.user._id,
      parentId && parentId !== 'null' ? parentId : null
    );

    res.json({
      items,
      breadcrumbs,
      currentFolderId: parentId && parentId !== 'null' ? parentId : null,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Create a new virtual folder
 */
export async function createFolder(req, res, next) {
  try {
    const { name, parentId } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Folder name is required' });
    }

    const trimmedName = name.trim();

    // Check for duplicate folder name in same parent
    const existing = await VirtualItem.findOne({
      userId: req.user._id,
      parentId: parentId || null,
      name: trimmedName,
      type: 'folder',
      isTrash: false,
    });

    if (existing) {
      return res.status(400).json({ error: 'A folder with this name already exists in this location' });
    }

    const folder = new VirtualItem({
      userId: req.user._id,
      name: trimmedName,
      type: 'folder',
      parentId: parentId || null,
      size: 0,
      mimeType: 'application/vnd.google-apps.folder',
      status: 'ready',
    });

    await folder.save();

    // Broadcast real-time creation event
    broadcaster.broadcast(req.user._id, 'file:created', { folder });

    res.status(201).json({
      message: 'Folder created successfully',
      folder,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Streaming multipart upload handler with client-side checksum validation,
 * automatic nested path resolution, and camera roll media metadata.
 */
export async function uploadFile(req, res, next) {
  try {
    req.setTimeout(0);
    res.setTimeout(0);
    const bb = busboy({ headers: req.headers });

    let parentId = req.query.parentId && req.query.parentId !== 'null' ? req.query.parentId : null;
    let targetPath = req.query.targetPath || null;
    let mediaMetadata = null;
    let clientChecksum =
      req.headers['x-content-sha256'] ||
      req.headers['x-checksum-sha256'] ||
      req.headers['x-client-checksum'] ||
      req.query.checksum ||
      null;

    let uploadPromise = null;
    let fileUploaded = false;

    bb.on('field', async (name, val) => {
      if (name === 'parentId' && val && val !== 'null') {
        parentId = val;
      }
      if (name === 'targetPath' && val && val.trim()) {
        targetPath = val.trim();
      }
      if (name === 'mediaMetadata' && val) {
        try {
          mediaMetadata = typeof val === 'string' ? JSON.parse(val) : val;
        } catch (e) {
          // invalid metadata JSON, ignore
        }
      }
      if (['clientChecksum', 'checksum', 'sha256'].includes(name) && val) {
        clientChecksum = val.trim();
      }
    });

    bb.on('file', (name, fileStream, info) => {
      const { filename, mimeType } = info;
      fileUploaded = true;

      uploadPromise = (async () => {
        // If targetPath is specified and parentId is not set, resolve/create folder path
        if (targetPath && !parentId) {
          try {
            parentId = await ensureFolderPath(req.user._id, targetPath);
          } catch (folderErr) {
            console.warn('[uploadFile] Failed to resolve targetPath:', folderErr.message);
          }
        }

        return processChunkedUpload({
          userId: req.user._id,
          fileStream,
          filename,
          mimeType,
          parentId,
          mediaMetadata,
          getClientChecksum: () => clientChecksum,
        });
      })();
    });

    bb.on('error', (err) => {
      next(err);
    });

    bb.on('close', async () => {
      if (!fileUploaded || !uploadPromise) {
        return res.status(400).json({ error: 'No file provided for upload' });
      }

      try {
        const virtualItem = await uploadPromise;
        // If parentId was resolved or provided after file in multipart fields, ensure item parentId is synced
        if (parentId && String(virtualItem.parentId) !== String(parentId)) {
          virtualItem.parentId = parentId;
          await virtualItem.save();
        }

        // Real-time broadcast to all client tabs and mobile devices
        broadcaster.broadcast(req.user._id, 'file:created', { file: virtualItem });
        broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'upload_complete' });

        res.status(201).json({
          message: 'File uploaded and striped across pooled drives successfully',
          file: virtualItem,
        });
      } catch (uploadErr) {
        next(uploadErr);
      }
    });

    req.pipe(bb);
  } catch (err) {
    next(err);
  }
}

/**
 * Download file stream (Content-Disposition: attachment)
 */
export async function downloadFile(req, res, next) {
  try {
    const virtualItem = await VirtualItem.findOne({
      _id: req.params.id,
      userId: req.user._id,
      type: 'file',
    });

    if (!virtualItem) {
      return res.status(404).json({ error: 'File not found' });
    }

    await streamReassembledFile(virtualItem, req, res, 'attachment');
  } catch (err) {
    next(err);
  }
}

/**
 * Preview file stream (Content-Disposition: inline, Range support)
 */
export async function previewFile(req, res, next) {
  try {
    const virtualItem = await VirtualItem.findOne({
      _id: req.params.id,
      userId: req.user._id,
      type: 'file',
    });

    if (!virtualItem) {
      return res.status(404).json({ error: 'File not found' });
    }

    await streamReassembledFile(virtualItem, req, res, 'inline');
  } catch (err) {
    next(err);
  }
}

/**
 * Update metadata (rename, move, toggle star)
 */
export async function updateItem(req, res, next) {
  try {
    const { name, parentId, isStarred } = req.body;
    const item = await VirtualItem.findOne({ _id: req.params.id, userId: req.user._id });

    if (!item) {
      return res.status(404).json({ error: 'Item not found' });
    }

    if (name !== undefined && name.trim()) {
      item.name = name.trim();
    }

    if (parentId !== undefined) {
      item.parentId = parentId && parentId !== 'null' ? parentId : null;
    }

    if (isStarred !== undefined) {
      item.isStarred = Boolean(isStarred);
    }

    await item.save();

    broadcaster.broadcast(req.user._id, 'file:updated', { item });

    res.json({
      message: 'Item updated successfully',
      item,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Move item to trash
 */
export async function trashItem(req, res, next) {
  try {
    const item = await VirtualItem.findOne({ _id: req.params.id, userId: req.user._id });

    if (!item) {
      return res.status(404).json({ error: 'Item not found' });
    }

    item.isTrash = true;
    item.trashedAt = new Date();
    await item.save();

    broadcaster.broadcast(req.user._id, 'file:updated', { item });

    res.json({ message: 'Item moved to trash', item });
  } catch (err) {
    next(err);
  }
}

/**
 * Restore item from trash
 */
export async function restoreItem(req, res, next) {
  try {
    const item = await VirtualItem.findOne({ _id: req.params.id, userId: req.user._id });

    if (!item) {
      return res.status(404).json({ error: 'Item not found' });
    }

    item.isTrash = false;
    item.trashedAt = null;
    await item.save();

    broadcaster.broadcast(req.user._id, 'file:updated', { item });

    res.json({ message: 'Item restored from trash', item });
  } catch (err) {
    next(err);
  }
}

/**
 * Helper to recursively delete all items and their Google Drive chunks
 */
async function cascadingDelete(userId, itemId) {
  const item = await VirtualItem.findOne({ _id: itemId, userId });
  if (!item) return;

  if (item.type === 'file') {
    // Delete all chunks from respective Google Drives and deduct usedQuota
    if (item.chunks && item.chunks.length > 0) {
      for (const chunk of item.chunks) {
        try {
          const account = await DriveAccount.findById(chunk.driveAccountId);
          if (account) {
            await deleteChunk(account, chunk.googleDriveFileId);
            await DriveAccount.findByIdAndUpdate(account._id, {
              $inc: { usedQuota: -chunk.size },
            });
          }
        } catch (e) {
          console.warn(`[Delete] Error deleting chunk ${chunk.googleDriveFileId}:`, e.message);
        }
      }
    }
    await VirtualItem.findByIdAndDelete(item._id);
  } else if (item.type === 'folder') {
    // Find all children
    const children = await VirtualItem.find({ parentId: item._id, userId });
    for (const child of children) {
      await cascadingDelete(userId, child._id);
    }
    await VirtualItem.findByIdAndDelete(item._id);
  }
}

/**
 * Permanently delete item and its chunks across Google accounts
 */
export async function permanentlyDeleteItem(req, res, next) {
  try {
    await cascadingDelete(req.user._id, req.params.id);

    broadcaster.broadcast(req.user._id, 'file:deleted', { id: req.params.id });
    broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'permanent_delete' });

    res.json({ message: 'Item and associated Google Drive chunks permanently deleted' });
  } catch (err) {
    next(err);
  }
}

/**
 * Inspect chunk manifest details for a file
 */
export async function getChunkManifest(req, res, next) {
  try {
    const item = await VirtualItem.findOne({
      _id: req.params.id,
      userId: req.user._id,
      type: 'file',
    }).populate('chunks.driveAccountId', 'accountEmail displayName isMock');

    if (!item) {
      return res.status(404).json({ error: 'File not found' });
    }

    res.json({
      fileId: item._id,
      fileName: item.name,
      totalSize: item.size,
      checksum: item.checksum,
      isChunked: item.isChunked,
      totalChunks: item.totalChunks,
      chunks: item.chunks.map((c) => ({
        partIndex: c.partIndex,
        size: c.size,
        md5: c.md5,
        googleDriveFileId: c.googleDriveFileId,
        driveAccount: {
          id: c.driveAccountId?._id,
          email: c.driveAccountId?.accountEmail,
          accountEmail: c.driveAccountId?.accountEmail,
          displayName: c.driveAccountId?.displayName,
          isMock: c.driveAccountId?.isMock,
        },
      })),
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Unified pooled storage summary
 */
export async function getStorageSummary(req, res, next) {
  try {
    const accounts = await DriveAccount.find({ userId: req.user._id, isActive: true });

    const totalPooledLimit = accounts.reduce((sum, acc) => sum + (acc.totalQuota || 0), 0);
    const totalPooledUsage = accounts.reduce((sum, acc) => sum + (acc.usedQuota || 0), 0);
    const totalPooledFree = Math.max(0, totalPooledLimit - totalPooledUsage);

    const breakdown = accounts.map((acc) => ({
      id: acc._id,
      accountEmail: acc.accountEmail,
      displayName: acc.displayName || acc.accountEmail.split('@')[0],
      totalQuota: acc.totalQuota,
      usedQuota: acc.usedQuota,
      freeQuota: Math.max(0, acc.totalQuota - acc.usedQuota),
      usagePercentage: acc.totalQuota > 0 ? ((acc.usedQuota / acc.totalQuota) * 100).toFixed(1) : 0,
      isMock: acc.isMock,
    }));

    res.json({
      totalPooledLimit,
      totalPooledUsage,
      totalPooledFree,
      usagePercentage: totalPooledLimit > 0 ? ((totalPooledUsage / totalPooledLimit) * 100).toFixed(1) : 0,
      totalAccounts: accounts.length,
      accounts: breakdown,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Fast deduplication check for camera roll assets before upload.
 * Matches assets by deviceId + deviceAssetId, checksum, or name + size + takenAt.
 */
export async function checkMediaExists(req, res, next) {
  try {
    const { assets } = req.body;
    if (!Array.isArray(assets) || assets.length === 0) {
      return res.json({ existing: [], backedUpAssetIds: [] });
    }

    const deviceAssetIds = [];
    const checksums = [];
    const fallbackQueries = [];

    for (const a of assets) {
      if (a.deviceAssetId) deviceAssetIds.push(a.deviceAssetId);
      if (a.checksum) checksums.push(a.checksum);
      if (a.name && a.size) {
        fallbackQueries.push({ name: a.name, size: a.size });
      }
    }

    const orClauses = [];
    if (deviceAssetIds.length > 0) {
      orClauses.push({ 'mediaMetadata.deviceAssetId': { $in: deviceAssetIds } });
    }
    if (checksums.length > 0) {
      orClauses.push({ checksum: { $in: checksums } });
    }
    if (fallbackQueries.length > 0) {
      orClauses.push(...fallbackQueries);
    }

    if (orClauses.length === 0) {
      return res.json({ existing: [], backedUpAssetIds: [] });
    }

    const matches = await VirtualItem.find({
      userId: req.user._id,
      isTrash: false,
      $or: orClauses,
    }).select('_id name size checksum mediaMetadata');

    const backedUpSet = new Set();
    const existing = [];

    for (const match of matches) {
      if (match.mediaMetadata?.deviceAssetId) {
        backedUpSet.add(match.mediaMetadata.deviceAssetId);
      }
      existing.push({
        virtualFileId: match._id,
        name: match.name,
        checksum: match.checksum,
        deviceAssetId: match.mediaMetadata?.deviceAssetId,
      });
    }

    // Also match any assets that share checksum or name+size
    for (const a of assets) {
      const isMatched = matches.some(
        (m) =>
          (a.checksum && m.checksum === a.checksum) ||
          (a.deviceAssetId && m.mediaMetadata?.deviceAssetId === a.deviceAssetId) ||
          (m.name === a.name && m.size === a.size)
      );
      if (isMatched && a.deviceAssetId) {
        backedUpSet.add(a.deviceAssetId);
      }
    }

    res.json({
      existing,
      backedUpAssetIds: Array.from(backedUpSet),
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Google Photos-style photo timeline feed with cursor pagination.
 * Retrieves all media items sorted reverse-chronologically by capture date (takenAt).
 */
export async function getPhotoTimeline(req, res, next) {
  try {
    const { cursor, limit = 50 } = req.query;
    const pageLimit = Math.min(100, Math.max(1, parseInt(limit, 10) || 50));

    const filter = {
      userId: req.user._id,
      isTrash: false,
      status: 'ready',
      $or: [
        { mimeType: { $regex: '^image/', $options: 'i' } },
        { mimeType: { $regex: '^video/', $options: 'i' } },
        { 'mediaMetadata.takenAt': { $ne: null } },
      ],
    };

    if (cursor) {
      try {
        const cursorDate = new Date(cursor);
        if (!isNaN(cursorDate.getTime())) {
          filter.$or = [
            {
              $and: [
                {
                  $or: [
                    { mimeType: { $regex: '^image/', $options: 'i' } },
                    { mimeType: { $regex: '^video/', $options: 'i' } },
                    { 'mediaMetadata.takenAt': { $ne: null } },
                  ],
                },
                {
                  $or: [
                    { 'mediaMetadata.takenAt': { $lt: cursorDate } },
                    { 'mediaMetadata.takenAt': null, createdAt: { $lt: cursorDate } },
                  ],
                },
              ],
            },
          ];
        }
      } catch (cursorErr) {
        // ignore invalid cursor
      }
    }

    const items = await VirtualItem.find(filter)
      .sort({ 'mediaMetadata.takenAt': -1, createdAt: -1 })
      .limit(pageLimit + 1)
      .select('_id name size mimeType isChunked totalChunks thumbnail mediaMetadata isStarred parentId createdAt');

    const hasMore = items.length > pageLimit;
    const resultItems = hasMore ? items.slice(0, pageLimit) : items;

    let nextCursor = null;
    if (hasMore && resultItems.length > 0) {
      const lastItem = resultItems[resultItems.length - 1];
      nextCursor = (lastItem.mediaMetadata?.takenAt || lastItem.createdAt).toISOString();
    }

    const totalCount = await VirtualItem.countDocuments({
      userId: req.user._id,
      isTrash: false,
      status: 'ready',
      $or: [
        { mimeType: { $regex: '^image/', $options: 'i' } },
        { mimeType: { $regex: '^video/', $options: 'i' } },
        { 'mediaMetadata.takenAt': { $ne: null } },
      ],
    });

    res.json({
      items: resultItems,
      nextCursor,
      hasMore,
      totalCount,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Initializes a chunked upload session for large files (videos > 8MB).
 * Prevents PaaS reverse proxy timeouts by breaking transfer into small independent chunk requests.
 */
export async function initChunkedUpload(req, res, next) {
  try {
    const { filename, mimeType, totalSize, parentId, totalChunks, mediaMetadata } = req.body;
    if (!filename) {
      return res.status(400).json({ error: 'Filename is required' });
    }

    const accounts = await getActiveAccounts(req.user._id);
    if (!accounts || accounts.length === 0) {
      return res.status(400).json({
        error: 'No active Google Drive accounts connected. Please link an account first.',
      });
    }

    const totalPoolFree = accounts.reduce((acc, a) => acc + Math.max(0, a.totalQuota - a.usedQuota), 0);
    const expectedSize = Number(totalSize) || 0;
    if (expectedSize > 0 && expectedSize > totalPoolFree) {
      return res.status(507).json({
        error: `Insufficient storage pool capacity. Required ${expectedSize} bytes, available ${totalPoolFree} bytes.`,
      });
    }

    const virtualItem = new VirtualItem({
      userId: req.user._id,
      name: filename,
      type: 'file',
      parentId: parentId || null,
      mimeType: mimeType || 'application/octet-stream',
      status: 'uploading',
      isChunked: true,
      totalChunks: Number(totalChunks) || 1,
      size: expectedSize,
      chunks: [],
      mediaMetadata: mediaMetadata || null,
    });
    await virtualItem.save();

    res.status(201).json({
      uploadId: virtualItem._id.toString(),
      message: 'Chunked upload initialized',
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Receives an individual small chunk (3-5MB), uploads it directly to Google Drive,
 * updates quotas and records the chunk in the file manifest.
 */
export async function uploadChunkPart(req, res, next) {
  try {
    req.setTimeout(0);
    res.setTimeout(0);

    const bb = busboy({ headers: req.headers });
    let uploadId = null;
    let partIndex = null;
    let chunkPromise = null;

    bb.on('field', (name, val) => {
      if (name === 'uploadId') uploadId = val.trim();
      if (name === 'partIndex') partIndex = parseInt(val, 10);
    });

    bb.on('file', (name, stream) => {
      chunkPromise = (async () => {
        const chunks = [];
        for await (const chunk of stream) {
          chunks.push(chunk);
        }
        return Buffer.concat(chunks);
      })();
    });

    bb.on('error', (err) => next(err));

    bb.on('close', async () => {
      try {
        if (!uploadId || partIndex === null || isNaN(partIndex) || !chunkPromise) {
          return res.status(400).json({ error: 'Missing uploadId, partIndex, or chunk payload' });
        }

        const buffer = await chunkPromise;
        const virtualItem = await VirtualItem.findOne({ _id: uploadId, userId: req.user._id });
        if (!virtualItem) {
          return res.status(404).json({ error: 'Upload session not found' });
        }

        const accounts = await getActiveAccounts(req.user._id);
        if (!accounts || accounts.length === 0) {
          return res.status(400).json({ error: 'No active Google Drive accounts available' });
        }

        const targetAccount = pickBestAccount(accounts, buffer.length, partIndex);
        const chunkName = `_chk_${virtualItem._id}_part_${partIndex}.bin`;
        const chunkHash = crypto.createHash('md5').update(buffer).digest('hex');

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

        // Replace existing part if retried
        virtualItem.chunks = virtualItem.chunks.filter((c) => c.partIndex !== partIndex);
        virtualItem.chunks.push(partInfo);
        await virtualItem.save();

        // Increment quota
        await DriveAccount.findByIdAndUpdate(targetAccount._id, {
          $inc: { usedQuota: buffer.length },
        });

        res.json({
          message: 'Chunk uploaded successfully',
          partIndex,
          receivedSize: buffer.length,
          totalChunksRecorded: virtualItem.chunks.length,
        });
      } catch (err) {
        next(err);
      }
    });

    req.pipe(bb);
  } catch (err) {
    next(err);
  }
}

/**
 * Finalizes the chunked upload session, sets status to ready, and notifies clients via SSE.
 */
export async function completeChunkedUpload(req, res, next) {
  try {
    const { uploadId } = req.body;
    if (!uploadId) {
      return res.status(400).json({ error: 'uploadId is required' });
    }

    const virtualItem = await VirtualItem.findOne({ _id: uploadId, userId: req.user._id });
    if (!virtualItem) {
      return res.status(404).json({ error: 'Upload session not found' });
    }

    virtualItem.chunks.sort((a, b) => a.partIndex - b.partIndex);
    const actualSize = virtualItem.chunks.reduce((sum, c) => sum + c.size, 0);
    virtualItem.size = actualSize;
    virtualItem.totalChunks = virtualItem.chunks.length;
    virtualItem.status = 'ready';
    await virtualItem.save();

    broadcaster.broadcast(req.user._id, 'file:created', { file: virtualItem });
    broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'upload_complete' });

    res.json({
      message: 'File upload completed and striped across pool',
      file: virtualItem,
    });
  } catch (err) {
    next(err);
  }
}

export default {
  subscribeEvents,
  getFiles,
  createFolder,
  uploadFile,
  initChunkedUpload,
  uploadChunkPart,
  completeChunkedUpload,
  downloadFile,
  previewFile,
  updateItem,
  trashItem,
  restoreItem,
  permanentlyDeleteItem,
  getChunkManifest,
  getStorageSummary,
  checkMediaExists,
  getPhotoTimeline,
};
