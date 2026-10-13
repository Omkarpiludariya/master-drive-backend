import DriveAccount from '../models/DriveAccount.js';
import { getChunkStream } from './driveService.js';

/**
 * Streams reassembled chunks directly to client HTTP response with zero server disk buffering.
 *
 * @param {object} virtualItem - VirtualItem Mongoose document
 * @param {object} req - Express Request
 * @param {object} res - Express Response
 * @param {string} disposition - 'attachment' | 'inline'
 */
export async function streamReassembledFile(virtualItem, req, res, disposition = 'attachment') {
  if (!virtualItem.chunks || virtualItem.chunks.length === 0) {
    return res.status(404).json({ error: 'File contains no chunk data' });
  }

  // Sort chunks strictly by partIndex
  const sortedChunks = [...virtualItem.chunks].sort((a, b) => a.partIndex - b.partIndex);

  // Encode filename safely for Content-Disposition (RFC 5987 / RFC 6266)
  const safeName = virtualItem.name.replace(/["\r\n\\]/g, '_');
  const encodedName = encodeURIComponent(virtualItem.name);
  const dispositionHeader = `${disposition}; filename="${safeName}"; filename*=UTF-8''${encodedName}`;
  const totalSize = virtualItem.size;
  const mimeType = virtualItem.mimeType || 'application/octet-stream';

  const rangeHeader = req.headers.range;

  // Track active stream for graceful client disconnect
  let currentStream = null;
  let isClientDisconnected = false;

  res.on('close', () => {
    isClientDisconnected = true;
    if (currentStream && typeof currentStream.destroy === 'function') {
      currentStream.destroy();
    }
  });

  if (!rangeHeader) {
    // Standard full stream
    res.setHeader('Content-Type', mimeType);
    res.setHeader('Content-Length', totalSize);
    res.setHeader('Content-Disposition', dispositionHeader);
    res.setHeader('Accept-Ranges', 'bytes');

    for (let i = 0; i < sortedChunks.length; i++) {
      if (isClientDisconnected) break;

      const chunk = sortedChunks[i];
      const account = await DriveAccount.findById(chunk.driveAccountId);
      if (!account) {
        throw new Error(`Linked Drive account not found for chunk ${chunk.partIndex}`);
      }

      currentStream = await getChunkStream(account, chunk.googleDriveFileId);

      await new Promise((resolve, reject) => {
        currentStream.on('error', reject);
        currentStream.on('end', resolve);
        currentStream.pipe(res, { end: false });
      });
    }

    if (!isClientDisconnected) {
      res.end();
    }
  } else {
    // Range request handling (for seekable video/audio preview)
    let start, end;
    const suffixMatch = rangeHeader.match(/bytes=-(\d+)/);
    const standardMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);

    if (suffixMatch) {
      const suffixLen = parseInt(suffixMatch[1], 10);
      start = Math.max(0, totalSize - suffixLen);
      end = totalSize - 1;
    } else if (standardMatch) {
      start = parseInt(standardMatch[1], 10);
      end = standardMatch[2] ? parseInt(standardMatch[2], 10) : totalSize - 1;
    } else {
      res.setHeader('Content-Range', `bytes */${totalSize}`);
      return res.status(416).send('Requested Range Not Satisfiable');
    }

    if (start >= totalSize || end >= totalSize || start > end) {
      res.setHeader('Content-Range', `bytes */${totalSize}`);
      return res.status(416).send('Requested Range Not Satisfiable');
    }

    const contentLength = end - start + 1;
    res.status(206);
    res.setHeader('Content-Type', mimeType);
    res.setHeader('Content-Length', contentLength);
    res.setHeader('Content-Range', `bytes ${start}-${end}/${totalSize}`);
    res.setHeader('Content-Disposition', dispositionHeader);
    res.setHeader('Accept-Ranges', 'bytes');

    // Calculate byte offsets for each chunk
    let currentByteOffset = 0;

    for (const chunk of sortedChunks) {
      if (isClientDisconnected) break;

      const chunkStart = currentByteOffset;
      const chunkEnd = chunkStart + chunk.size - 1;
      currentByteOffset += chunk.size;

      // Check if chunk overlaps with the requested range
      if (chunkEnd < start || chunkStart > end) {
        continue; // Not in requested range
      }

      const account = await DriveAccount.findById(chunk.driveAccountId);
      if (!account) {
        throw new Error(`Drive account not found for chunk ${chunk.partIndex}`);
      }

      const sliceStartInChunk = Math.max(0, start - chunkStart);
      const sliceEndInChunk = Math.min(chunk.size - 1, end - chunkStart);
      const bytesToRead = sliceEndInChunk - sliceStartInChunk + 1;

      currentStream = await getChunkStream(account, chunk.googleDriveFileId);

      // Slice the stream to exact byte boundary
      let bytesConsumed = 0;
      await new Promise((resolve, reject) => {
        currentStream.on('data', (data) => {
          if (isClientDisconnected) return;

          const dataStart = bytesConsumed;
          const dataEnd = bytesConsumed + data.length - 1;
          bytesConsumed += data.length;

          if (dataEnd < sliceStartInChunk || dataStart > sliceEndInChunk) {
            return;
          }

          const offsetInChunk = Math.max(0, sliceStartInChunk - dataStart);
          const endOffsetInChunk = Math.min(data.length, sliceEndInChunk - dataStart + 1);

          res.write(data.subarray(offsetInChunk, endOffsetInChunk));
        });

        currentStream.on('end', resolve);
        currentStream.on('error', reject);
      });
    }

    if (!isClientDisconnected) {
      res.end();
    }
  }
}

export default {
  streamReassembledFile,
};
