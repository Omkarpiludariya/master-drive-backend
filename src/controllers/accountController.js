import { createOAuth2Client, GOOGLE_SCOPES } from '../config/google.js';
import { encrypt } from '../services/cryptoService.js';
import { syncAccountQuota, ensureChunksFolder } from '../services/driveService.js';
import DriveAccount from '../models/DriveAccount.js';
import VirtualItem from '../models/VirtualItem.js';
import User from '../models/User.js';
import { google } from 'googleapis';
import { broadcaster } from '../services/eventBroadcaster.js';

/**
 * Returns all linked Google Drive accounts for the current user
 */
export async function getAccounts(req, res, next) {
  try {
    const accounts = await DriveAccount.find({ userId: req.user._id }).sort({ createdAt: -1 });

    const formatted = accounts.map((acc) => ({
      id: acc._id,
      accountEmail: acc.accountEmail,
      displayName: acc.displayName || acc.accountEmail.split('@')[0],
      photoUrl: acc.photoUrl,
      totalQuota: acc.totalQuota,
      usedQuota: acc.usedQuota,
      freeQuota: Math.max(0, acc.totalQuota - acc.usedQuota),
      isActive: acc.isActive,
      isMock: acc.isMock,
      lastSyncedAt: acc.lastSyncedAt,
    }));

    res.json({ accounts: formatted });
  } catch (err) {
    next(err);
  }
}

/**
 * Generates Google OAuth consent URL
 */
export async function getGoogleAuthUrl(req, res, next) {
  try {
    if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET) {
      return res.status(400).json({
        error: 'Google OAuth credentials are not configured in server .env. You can use Mock Accounts to test immediately!',
        needsConfig: true,
      });
    }

    const { returnUrl } = req.query;
    const oauth2Client = createOAuth2Client();

    // Include userId and optional mobile returnUrl in state to correlate callback
    const state = JSON.stringify({
      userId: req.user._id.toString(),
      returnUrl: returnUrl || null,
    });

    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent', // Ensure refresh token is returned
      scope: GOOGLE_SCOPES,
      state: Buffer.from(state).toString('base64'),
    });

    res.json({ authUrl });
  } catch (err) {
    next(err);
  }
}

/**
 * Handles OAuth callback from Google
 */
export async function handleGoogleCallback(req, res, next) {
  let targetRedirectBase = process.env.CLIENT_URL || 'http://localhost:3001';

  const buildRedirect = (base, key, value) => {
    const sep = base.includes('?') ? '&' : '?';
    return `${base}${sep}${key}=${encodeURIComponent(value)}`;
  };

  try {
    const { code, state, error } = req.query;

    let userId = null;
    let returnUrl = null;
    if (state) {
      try {
        const decoded = JSON.parse(Buffer.from(state, 'base64').toString('utf8'));
        userId = decoded.userId;
        returnUrl = decoded.returnUrl;
      } catch (e1) {
        try {
          const raw = JSON.parse(state);
          userId = raw.userId;
          returnUrl = raw.returnUrl;
        } catch (e2) {
          // If state is not JSON (e.g. from test or manual link), fallback to recent user
          console.warn('[GoogleCallback] State is not valid JSON, looking for recent user...');
        }
      }
    }

    if (returnUrl) {
      targetRedirectBase = returnUrl;
    }

    if (error) {
      return res.redirect(buildRedirect(targetRedirectBase, 'error', error));
    }

    if (!code) {
      return res.redirect(buildRedirect(targetRedirectBase, 'error', 'Authorization code is missing'));
    }

    if (!userId) {
      const recentUser = await User.findOne().sort({ createdAt: -1 });
      if (recentUser) {
        userId = recentUser._id;
      } else {
        return res.redirect(buildRedirect(targetRedirectBase, 'error', 'No user account found. Please register or sign in first.'));
      }
    }

    const oauth2Client = createOAuth2Client();
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // Get user info and quota
    const oauth2 = google.oauth2({ version: 'v2', auth: oauth2Client });
    const drive = google.drive({ version: 'v3', auth: oauth2Client });

    const [userInfoRes, aboutRes] = await Promise.all([
      oauth2.userinfo.get(),
      drive.about.get({ fields: 'user, storageQuota' }),
    ]);

    const email = userInfoRes.data.email;
    const displayName = userInfoRes.data.name || email.split('@')[0];
    const photoUrl = userInfoRes.data.picture || '';
    const googleUserId = userInfoRes.data.id || '';

    const quota = aboutRes.data.storageQuota;
    const totalQuota = quota?.limit ? Number(quota.limit) : 15 * 1024 * 1024 * 1024;
    const usedQuota = quota?.usage ? Number(quota.usage) : 0;

    // Check if account already linked (active or previously unlinked)
    let account = await DriveAccount.findOne({ userId, accountEmail: email });

    const encryptedAccess = tokens.access_token ? encrypt(tokens.access_token) : '';
    const encryptedRefresh = tokens.refresh_token ? encrypt(tokens.refresh_token) : account?.encryptedRefreshToken || '';
    const tokenExpiry = tokens.expiry_date ? new Date(tokens.expiry_date) : new Date(Date.now() + 3600000);

    if (account) {
      // Re-activate and update existing account document (retaining account._id so all existing chunks remain valid!)
      account.encryptedAccessToken = encryptedAccess;
      if (encryptedRefresh) account.encryptedRefreshToken = encryptedRefresh;
      account.tokenExpiry = tokenExpiry;
      account.displayName = displayName;
      account.photoUrl = photoUrl;
      account.totalQuota = totalQuota;
      account.usedQuota = usedQuota;
      account.isActive = true;
      account.lastSyncedAt = new Date();

      // Ensure chunks folder is verified
      try {
        const folderId = await ensureChunksFolder(account);
        account.folderId = folderId;
      } catch (folderErr) {
        console.warn(`[GoogleDrive] Could not verify chunks folder for ${email}:`, folderErr.message);
      }

      await account.save();
    } else {
      account = new DriveAccount({
        userId,
        accountEmail: email,
        displayName,
        photoUrl,
        googleUserId,
        encryptedAccessToken: encryptedAccess,
        encryptedRefreshToken: encryptedRefresh,
        tokenExpiry,
        totalQuota,
        usedQuota,
        isActive: true,
        isMock: false,
      });
      await account.save();

      // Ensure chunks folder exists on this drive
      try {
        const folderId = await ensureChunksFolder(account);
        account.folderId = folderId;
        await account.save();
      } catch (folderErr) {
        console.warn(`[GoogleDrive] Could not create chunks folder for ${email}:`, folderErr.message);
      }
    }

    broadcaster.broadcast(userId, 'storage:updated', { reason: 'account_connected' });

    res.redirect(buildRedirect(targetRedirectBase, 'accountConnected', 'true'));
  } catch (err) {
    console.error('[GoogleCallback Error]', err);
    res.redirect(buildRedirect(targetRedirectBase, 'error', err.message));
  }
}

/**
 * Manually re-syncs quota for an account
 */
export async function syncAccount(req, res, next) {
  try {
    const account = await DriveAccount.findOne({ _id: req.params.id, userId: req.user._id });
    if (!account) {
      return res.status(404).json({ error: 'Account not found' });
    }

    const quotaData = await syncAccountQuota(account);
    account.totalQuota = quotaData.totalQuota;
    account.usedQuota = quotaData.usedQuota;
    if (quotaData.displayName) account.displayName = quotaData.displayName;
    if (quotaData.photoUrl) account.photoUrl = quotaData.photoUrl;
    account.lastSyncedAt = new Date();
    await account.save();

    broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'quota_synced' });

    res.json({
      message: 'Quota synced successfully',
      account: {
        id: account._id,
        accountEmail: account.accountEmail,
        displayName: account.displayName,
        totalQuota: account.totalQuota,
        usedQuota: account.usedQuota,
        freeQuota: Math.max(0, account.totalQuota - account.usedQuota),
        lastSyncedAt: account.lastSyncedAt,
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Disconnects an account
 */
export async function disconnectAccount(req, res, next) {
  try {
    const account = await DriveAccount.findOne({ _id: req.params.id, userId: req.user._id });
    if (!account) {
      return res.status(404).json({ error: 'Account not found' });
    }

    // Check if any active files have chunks stored on this account
    const dependentFilesCount = await VirtualItem.countDocuments({
      userId: req.user._id,
      'chunks.driveAccountId': account._id,
      isTrash: false,
    });

    if (dependentFilesCount > 0 && !req.query.force) {
      return res.status(400).json({
        error: `Cannot disconnect: ${dependentFilesCount} file(s) depend on chunks stored in this account. Use ?force=true to unlink safely (chunks are kept and will reactivate when you reconnect this account).`,
        dependentFilesCount,
      });
    }

    // If files depend on this account, SOFT DISCONNECT to preserve chunk integrity:
    // keep the document so existing files retain their chunk mappings, but deactivate and clear tokens.
    if (dependentFilesCount > 0) {
      account.isActive = false;
      account.encryptedAccessToken = '';
      account.encryptedRefreshToken = '';
      await account.save();

      broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'account_unlinked' });

      return res.json({
        message: 'Account unlinked. Stored chunks are preserved and will reactivate when you reconnect this account.',
        softDisconnected: true,
        dependentFilesCount,
      });
    }

    // If no files depend on this account, safely remove from database
    await DriveAccount.findByIdAndDelete(account._id);

    broadcaster.broadcast(req.user._id, 'storage:updated', { reason: 'account_deleted' });

    res.json({ message: 'Account disconnected successfully' });
  } catch (err) {
    next(err);
  }
}

/**
 * Adds a simulated mock Google Drive account for testing
 */
export async function addMockAccount(req, res, next) {
  try {
    const { email, displayName, quotaGB = 15 } = req.body;

    const accountEmail = email || `simulated.${Date.now()}@gmail.com`;
    const name = displayName || `Google Account (${quotaGB} GB)`;
    const totalBytes = quotaGB * 1024 * 1024 * 1024;

    const existing = await DriveAccount.findOne({ userId: req.user._id, accountEmail });
    if (existing) {
      return res.status(400).json({ error: 'An account with this email already exists' });
    }

    const mockAccount = new DriveAccount({
      userId: req.user._id,
      accountEmail,
      displayName: name,
      photoUrl: 'https://lh3.googleusercontent.com/a/default-user',
      totalQuota: totalBytes,
      usedQuota: 0,
      isActive: true,
      isMock: true,
      folderId: 'mock_chunks_root',
    });

    await mockAccount.save();

    res.status(201).json({
      message: 'Simulated account added successfully',
      account: {
        id: mockAccount._id,
        accountEmail: mockAccount.accountEmail,
        displayName: mockAccount.displayName,
        totalQuota: mockAccount.totalQuota,
        usedQuota: mockAccount.usedQuota,
        freeQuota: mockAccount.totalQuota,
        isMock: true,
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Seeds multiple simulated accounts for instant testing
 */
export async function seedMockAccounts(req, res, next) {
  try {
    const mockProfiles = [
      { email: 'personal.drive1@gmail.com', name: 'Personal Drive (15 GB)', quotaGB: 15 },
      { email: 'work.drive2@gmail.com', name: 'Work Drive (15 GB)', quotaGB: 15 },
      { email: 'archive.drive3@gmail.com', name: 'Archive Drive (15 GB)', quotaGB: 15 },
    ];

    const created = [];

    for (const profile of mockProfiles) {
      let acc = await DriveAccount.findOne({ userId: req.user._id, accountEmail: profile.email });
      if (!acc) {
        acc = new DriveAccount({
          userId: req.user._id,
          accountEmail: profile.email,
          displayName: profile.name,
          photoUrl: 'https://lh3.googleusercontent.com/a/default-user',
          totalQuota: profile.quotaGB * 1024 * 1024 * 1024,
          usedQuota: 0,
          isActive: true,
          isMock: true,
          folderId: 'mock_chunks_root',
        });
        await acc.save();
        created.push(acc);
      }
    }

    res.json({
      message: `Successfully seeded ${created.length} simulated accounts. Total pooled storage expanded!`,
      createdCount: created.length,
    });
  } catch (err) {
    next(err);
  }
}

export default {
  getAccounts,
  getGoogleAuthUrl,
  handleGoogleCallback,
  syncAccount,
  disconnectAccount,
  addMockAccount,
  seedMockAccounts,
};
