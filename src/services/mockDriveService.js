import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const STORAGE_DIR = path.join(__dirname, '../../.mock_storage');

// Ensure directory exists
if (!fs.existsSync(STORAGE_DIR)) {
  fs.mkdirSync(STORAGE_DIR, { recursive: true });
}

/**
 * Uploads a chunk to the mock storage system
 * @param {string} accountId
 * @param {ReadableStream} stream
 * @param {string} filename
 * @returns {Promise<{ id: string, size: number, md5: string }>}
 */
export async function uploadMockChunk(accountId, stream, filename) {
  const fileId = `mock_${crypto.randomUUID()}`;
  const accountDir = path.join(STORAGE_DIR, accountId.toString());
  if (!fs.existsSync(accountDir)) {
    fs.mkdirSync(accountDir, { recursive: true });
  }

  const filePath = path.join(accountDir, `${fileId}.bin`);
  const writeStream = fs.createWriteStream(filePath);
  const hash = crypto.createHash('md5');

  let size = 0;

  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => {
      size += chunk.length;
      hash.update(chunk);
    });

    stream.pipe(writeStream);

    writeStream.on('finish', () => {
      resolve({
        id: fileId,
        size,
        md5: hash.digest('hex'),
      });
    });

    writeStream.on('error', reject);
    stream.on('error', reject);
  });
}

/**
 * Streams a mock chunk back to caller
 * @param {string} accountId
 * @param {string} fileId
 * @returns {fs.ReadStream}
 */
export function getMockChunkStream(accountId, fileId) {
  const filePath = path.join(STORAGE_DIR, accountId.toString(), `${fileId}.bin`);
  if (!fs.existsSync(filePath)) {
    throw new Error(`Mock chunk file not found: ${fileId}`);
  }
  return fs.createReadStream(filePath);
}

/**
 * Deletes a mock chunk
 * @param {string} accountId
 * @param {string} fileId
 */
export async function deleteMockChunk(accountId, fileId) {
  const filePath = path.join(STORAGE_DIR, accountId.toString(), `${fileId}.bin`);
  if (fs.existsSync(filePath)) {
    try {
      fs.unlinkSync(filePath);
    } catch (e) {
      console.warn(`Failed to delete mock chunk file ${filePath}:`, e.message);
    }
  }
}

export default {
  uploadMockChunk,
  getMockChunkStream,
  deleteMockChunk,
};
