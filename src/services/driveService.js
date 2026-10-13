import { google } from 'googleapis';
import { createOAuth2Client, CHUNKS_FOLDER_NAME } from '../config/google.js';
import { decrypt, encrypt } from './cryptoService.js';
import mockDriveService from './mockDriveService.js';
import DriveAccount from '../models/DriveAccount.js';

/**
 * Exponential backoff wrapper for Google API calls
 */
export async function withRetry(fn, maxRetries = 3, baseDelay = 1000) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      const isRateLimit =
        err?.status === 429 ||
        err?.code === 429 ||
        err?.response?.status === 429 ||
        (err?.status === 403 &&
          (err?.message?.includes('rateLimit') ||
            err?.message?.includes('Rate Limit') ||
            err?.errors?.some?.((e) => e.reason?.includes('rateLimit') || e.reason?.includes('RateLimit')))) ||
        err?.message?.includes('Rate Limit');

      const isTransient =
        err?.status === 503 ||
        err?.code === 503 ||
        err?.response?.status === 503 ||
        isRateLimit;

      if (attempt <= maxRetries && isTransient) {
        const jitter = Math.floor(Math.random() * 300);
        const delay = baseDelay * Math.pow(2, attempt - 1) + jitter;
        console.warn(`[GoogleDrive API] Request failed with ${err.message}. Retrying in ${delay}ms (attempt ${attempt}/${maxRetries})...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        throw err;
      }
    }
  }
}

/**
 * Gets an authenticated Google Drive client for a specific account, auto-refreshing expired tokens.
 * @param {object} account - DriveAccount Mongoose document or plain object
 * @returns {Promise<google.drive_v3.Drive|null>}
 */
export async function getAuthenticatedClient(account) {
  if (account.isMock) {
    return null; // Mock accounts do not use googleapis
  }

  const oauth2Client = createOAuth2Client();

  const accessToken = account.encryptedAccessToken ? decrypt(account.encryptedAccessToken) : null;
  const refreshToken = account.encryptedRefreshToken ? decrypt(account.encryptedRefreshToken) : null;

  oauth2Client.setCredentials({
    access_token: accessToken,
    refresh_token: refreshToken,
    expiry_date: account.tokenExpiry ? new Date(account.tokenExpiry).getTime() : null,
  });

  // Check if token expired or about to expire (within 5 minutes)
  const isExpiringSoon = account.tokenExpiry && new Date(account.tokenExpiry).getTime() - Date.now() < 300000;

  if (isExpiringSoon && refreshToken) {
    try {
      console.log(`[GoogleDrive] Refreshing access token for ${account.accountEmail}...`);
      const { credentials } = await oauth2Client.refreshAccessToken();

      // Update account in database
      const updateData = {
        encryptedAccessToken: encrypt(credentials.access_token),
        tokenExpiry: credentials.expiry_date ? new Date(credentials.expiry_date) : new Date(Date.now() + 3600000),
      };

      if (credentials.refresh_token) {
        updateData.encryptedRefreshToken = encrypt(credentials.refresh_token);
      }

      await DriveAccount.findByIdAndUpdate(account._id, updateData);
      oauth2Client.setCredentials(credentials);
    } catch (refreshErr) {
      console.error(`[GoogleDrive] Failed to refresh token for ${account.accountEmail}:`, refreshErr.message);
      throw new Error(`Token refresh failed for account ${account.accountEmail}`);
    }
  }

  return google.drive({ version: 'v3', auth: oauth2Client });
}

/**
 * Ensures the dedicated /MasterDrive_Chunks folder exists in the given Google Drive account
 */
export async function ensureChunksFolder(account) {
  if (account.isMock) {
    return 'mock_chunks_folder';
  }

  const drive = await getAuthenticatedClient(account);

  return withRetry(async () => {
    // Search for existing folder
    const res = await drive.files.list({
      q: `name = '${CHUNKS_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
      fields: 'files(id, name)',
      spaces: 'drive',
    });

    if (res.data.files && res.data.files.length > 0) {
      return res.data.files[0].id;
    }

    // Create folder if not found
    const folderMetadata = {
      name: CHUNKS_FOLDER_NAME,
      mimeType: 'application/vnd.google-apps.folder',
      description: 'System folder for Master Google Drive virtual storage chunks. Do not modify.',
    };

    const createRes = await drive.files.create({
      requestBody: folderMetadata,
      fields: 'id',
    });

    return createRes.data.id;
  });
}

/**
 * Uploads a chunk stream to the target Drive account
 * @param {object} account
 * @param {ReadableStream} stream
 * @param {string} filename
 * @returns {Promise<{ id: string, size: number, md5: string }>}
 */
export async function uploadChunk(account, stream, filename) {
  if (account.isMock) {
    return mockDriveService.uploadMockChunk(account._id, stream, filename);
  }

  const drive = await getAuthenticatedClient(account);
  const folderId = account.folderId || (await ensureChunksFolder(account));

  return withRetry(async () => {
    const res = await drive.files.create({
      requestBody: {
        name: filename,
        parents: [folderId],
        description: 'MasterDrive Encrypted Storage Chunk',
      },
      media: {
        mimeType: 'application/octet-stream',
        body: stream,
      },
      fields: 'id, size, md5Checksum',
    });

    return {
      id: res.data.id,
      size: Number(res.data.size) || 0,
      md5: res.data.md5Checksum || '',
    };
  });
}

/**
 * Downloads a chunk stream from the target Drive account
 * @param {object} account
 * @param {string} fileId
 * @returns {Promise<ReadableStream>}
 */
export async function getChunkStream(account, fileId) {
  if (account.isMock) {
    return mockDriveService.getMockChunkStream(account._id, fileId);
  }

  const drive = await getAuthenticatedClient(account);

  return withRetry(async () => {
    const res = await drive.files.get(
      {
        fileId,
        alt: 'media',
      },
      { responseType: 'stream' }
    );
    return res.data;
  });
}

/**
 * Deletes a chunk file from Google Drive
 * @param {object} account
 * @param {string} fileId
 */
export async function deleteChunk(account, fileId) {
  if (account.isMock) {
    return mockDriveService.deleteMockChunk(account._id, fileId);
  }

  const drive = await getAuthenticatedClient(account);

  return withRetry(async () => {
    try {
      await drive.files.delete({ fileId });
    } catch (err) {
      if (err.status === 404 || err.code === 404) {
        // File already deleted or does not exist
        return;
      }
      throw err;
    }
  });
}

/**
 * Syncs storage quota from Google Drive API
 * @param {object} account
 * @returns {Promise<{ totalQuota: number, usedQuota: number }>}
 */
export async function syncAccountQuota(account) {
  if (account.isMock) {
    return {
      totalQuota: account.totalQuota,
      usedQuota: account.usedQuota,
    };
  }

  const drive = await getAuthenticatedClient(account);

  return withRetry(async () => {
    const res = await drive.about.get({
      fields: 'storageQuota, user',
    });

    const quota = res.data.storageQuota;
    const total = quota?.limit ? Number(quota.limit) : 15 * 1024 * 1024 * 1024;
    const used = quota?.usage ? Number(quota.usage) : 0;

    return {
      totalQuota: total,
      usedQuota: used,
      displayName: res.data.user?.displayName || account.displayName,
      photoUrl: res.data.user?.photoLink || account.photoUrl,
    };
  });
}

export default {
  withRetry,
  getAuthenticatedClient,
  ensureChunksFolder,
  uploadChunk,
  getChunkStream,
  deleteChunk,
  syncAccountQuota,
};
