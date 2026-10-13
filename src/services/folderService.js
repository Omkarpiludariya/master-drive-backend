import VirtualItem from '../models/VirtualItem.js';
import { broadcaster } from './eventBroadcaster.js';

/**
 * Recursively finds or creates a nested folder path atomically
 * e.g., ensureFolderPath(userId, '/Photos & Videos/2026/10')
 *
 * @param {string|object} userId
 * @param {string} folderPath
 * @returns {Promise<string|null>} Target leaf folder ID
 */
export async function ensureFolderPath(userId, folderPath) {
  if (!folderPath || typeof folderPath !== 'string') return null;

  const segments = folderPath
    .split('/')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  if (segments.length === 0) return null;

  let currentParentId = null;

  for (const segment of segments) {
    let folder = await VirtualItem.findOne({
      userId,
      parentId: currentParentId,
      name: segment,
      type: 'folder',
      isTrash: false,
    });

    if (!folder) {
      try {
        folder = new VirtualItem({
          userId,
          name: segment,
          type: 'folder',
          parentId: currentParentId,
          size: 0,
          mimeType: 'application/vnd.google-apps.folder',
          status: 'ready',
        });
        await folder.save();
        broadcaster.broadcast(userId, 'file:created', { folder });
      } catch (err) {
        // In case of parallel concurrent uploads creating the same path segment
        folder = await VirtualItem.findOne({
          userId,
          parentId: currentParentId,
          name: segment,
          type: 'folder',
          isTrash: false,
        });
        if (!folder) throw err;
      }
    }

    currentParentId = folder._id;
  }

  return currentParentId;
}

export default {
  ensureFolderPath,
};
