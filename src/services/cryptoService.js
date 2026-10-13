import crypto from 'crypto';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12; // Standard 96-bit IV for GCM
const AUTH_TAG_LENGTH = 16;

/**
 * Derives a 32-byte Buffer key from the environment variable
 */
function getKey() {
  const envKey = process.env.ENCRYPTION_KEY || '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  if (envKey.length === 64) {
    return Buffer.from(envKey, 'hex');
  }
  // If provided as raw string, hash it to ensure 32 bytes
  return crypto.createHash('sha256').update(envKey).digest();
}

/**
 * Encrypts plaintext using AES-256-GCM
 * @param {string} text - Plain text to encrypt
 * @returns {string} Combined format: iv:authTag:encryptedData (hex encoded)
 */
export function encrypt(text) {
  if (!text) return '';
  const key = getKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(text, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  const authTag = cipher.getAuthTag();

  return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
}

/**
 * Decrypts ciphertext using AES-256-GCM
 * @param {string} cipherText - Format: iv:authTag:encryptedData
 * @returns {string} Decrypted plaintext
 */
export function decrypt(cipherText) {
  if (!cipherText) return '';
  const parts = cipherText.split(':');
  if (parts.length !== 3) {
    throw new Error('Invalid encrypted token format');
  }

  const [ivHex, authTagHex, encryptedHex] = parts;
  const key = getKey();
  const iv = Buffer.from(ivHex, 'hex');
  const authTag = Buffer.from(authTagHex, 'hex');

  if (iv.length !== IV_LENGTH || authTag.length !== AUTH_TAG_LENGTH) {
    throw new Error('Invalid initialization vector or auth tag length');
  }

  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  let decrypted = decipher.update(encryptedHex, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return decrypted;
}

export default {
  encrypt,
  decrypt,
};
