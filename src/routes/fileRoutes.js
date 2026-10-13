import { Router } from 'express';
import {
  subscribeEvents,
  getFiles,
  createFolder,
  uploadFile,
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
} from '../controllers/fileController.js';
import { ensureFolderPath } from '../services/folderService.js';
import { authenticate } from '../middleware/auth.js';

const router = Router();

router.use(authenticate);

// Real-time Server-Sent Events (SSE) Stream
router.get('/events', subscribeEvents);

// Storage pool summary
router.get('/storage/summary', getStorageSummary);

// Camera roll & photo timeline endpoints
router.post('/check-media-exists', checkMediaExists);
router.get('/photos/timeline', getPhotoTimeline);
router.post('/ensure-folder-path', async (req, res, next) => {
  try {
    const { path } = req.body;
    const folderId = await ensureFolderPath(req.user._id, path);
    res.json({ folderId, path });
  } catch (err) {
    next(err);
  }
});

// File & folder CRUD
router.get('/', getFiles);
router.post('/folders', createFolder);
router.post('/upload', uploadFile);

// Streaming download and inline preview
router.get('/:id/download', downloadFile);
router.get('/:id/preview', previewFile);

// Manifest inspection
router.get('/:id/manifest', getChunkManifest);

// Metadata updates
router.patch('/:id', updateItem);

// Trash & permanent deletion
router.post('/:id/trash', trashItem);
router.post('/:id/restore', restoreItem);
router.delete('/:id', permanentlyDeleteItem);

export default router;
