import { Router } from 'express';
import {
  getAccounts,
  getGoogleAuthUrl,
  handleGoogleCallback,
  syncAccount,
  disconnectAccount,
  addMockAccount,
  seedMockAccounts,
} from '../controllers/accountController.js';
import { authenticate } from '../middleware/auth.js';

const router = Router();

// Callback is public because Google redirects the browser here
router.get('/google/callback', handleGoogleCallback);

// All other account operations require user authentication
router.use(authenticate);

router.get('/', getAccounts);
router.get('/google/auth-url', getGoogleAuthUrl);
router.post('/:id/sync', syncAccount);
router.delete('/:id', disconnectAccount);

// Simulation / Mock accounts for instant testing
router.post('/mock/add', addMockAccount);
router.post('/mock/seed', seedMockAccounts);

export default router;
