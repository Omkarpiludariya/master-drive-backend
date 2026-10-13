import DriveAccount from '../models/DriveAccount.js';

export class InsufficientStorageError extends Error {
  constructor(message = 'Insufficient storage in virtual pool') {
    super(message);
    this.name = 'InsufficientStorageError';
    this.status = 507; // 507 Insufficient Storage
  }
}

/**
 * Balancer Service that selects target accounts based on available free space
 */
export async function getActiveAccounts(userId) {
  const accounts = await DriveAccount.find({ userId, isActive: true });
  return accounts.filter((acc) => acc.freeQuota > 0);
}

/**
 * Selects an account for a single chunk upload based on current maximum free quota
 * @param {Array<object>} accounts - List of DriveAccount objects with freeQuota
 * @param {number} chunkSize - Size of chunk in bytes
 * @returns {object} The chosen DriveAccount
 */
export function pickBestAccount(accounts, chunkSize, chunkIndex = 0) {
  if (!accounts || accounts.length === 0) {
    throw new InsufficientStorageError('No active Google Drive accounts available');
  }

  // Filter accounts that can accommodate this chunk
  const eligible = accounts.filter((acc) => acc.freeQuota >= chunkSize);

  if (eligible.length === 0) {
    // Check if all accounts are completely full
    if (accounts.every((a) => a.freeQuota <= 0)) {
      throw new InsufficientStorageError('All connected Google Drive accounts are full. No available free space.');
    }
    accounts.sort((a, b) => b.freeQuota - a.freeQuota);
    return accounts[0];
  }

  // Sort descending by available free quota
  eligible.sort((a, b) => b.freeQuota - a.freeQuota);

  // Stripe across eligible accounts based on chunkIndex
  const index = Math.abs(chunkIndex) % eligible.length;
  return eligible[index];
}

/**
 * Pre-plans chunk distribution across accounts for a file of totalSize
 * @param {string} userId
 * @param {number} totalSize - Total file size in bytes
 * @param {number} chunkSize - Standard chunk size in bytes
 * @returns {Promise<Array<{ partIndex: number, account: object, expectedSize: number }>>}
 */
export async function planChunkAllocation(userId, totalSize, chunkSize) {
  const accounts = await getActiveAccounts(userId);

  if (!accounts || accounts.length === 0) {
    throw new InsufficientStorageError('Please connect at least one Google Drive account before uploading.');
  }

  const totalPooledFree = accounts.reduce((sum, acc) => sum + acc.freeQuota, 0);

  if (totalSize > totalPooledFree) {
    throw new InsufficientStorageError(
      `File size (${(totalSize / (1024 * 1024)).toFixed(2)} MB) exceeds total pooled free storage (${(totalPooledFree / (1024 * 1024)).toFixed(2)} MB).`
    );
  }

  // Work with mutable tracking copies of remaining free space
  const trackedAccounts = accounts.map((acc) => ({
    _id: acc._id,
    accountEmail: acc.accountEmail,
    displayName: acc.displayName,
    isMock: acc.isMock,
    freeQuota: acc.freeQuota,
    folderId: acc.folderId,
    model: acc,
  }));

  const numChunks = Math.ceil(totalSize / chunkSize) || 1;
  const plan = [];

  let bytesRemaining = totalSize;

  for (let i = 0; i < numChunks; i++) {
    const currentChunkSize = Math.min(bytesRemaining, chunkSize);

    // Pick candidate accounts that have enough freeQuota
    const eligible = trackedAccounts.filter((a) => a.freeQuota >= currentChunkSize);

    let chosen;
    if (eligible.length > 0) {
      eligible.sort((a, b) => b.freeQuota - a.freeQuota);
      chosen = eligible[i % eligible.length];
    } else {
      trackedAccounts.sort((a, b) => b.freeQuota - a.freeQuota);
      chosen = trackedAccounts[0];
    }

    if (chosen.freeQuota < currentChunkSize && trackedAccounts.every((a) => a.freeQuota < currentChunkSize)) {
      throw new InsufficientStorageError('Could not stripe chunks across available drive accounts without exceeding quota limits.');
    }

    plan.push({
      partIndex: i,
      account: chosen.model,
      expectedSize: currentChunkSize,
    });

    // Deduct tracked space for balancing subsequent chunks
    chosen.freeQuota -= currentChunkSize;
    bytesRemaining -= currentChunkSize;
  }

  return plan;
}

export default {
  InsufficientStorageError,
  getActiveAccounts,
  pickBestAccount,
  planChunkAllocation,
};
