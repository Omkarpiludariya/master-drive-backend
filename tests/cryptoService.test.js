import { test } from 'node:test';
import assert from 'node:assert/strict';
import { encrypt, decrypt } from '../src/services/cryptoService.js';

test('CryptoService - encrypt and decrypt tokens correctly', () => {
  const plainToken = 'ya29.a0AfH6SMA_sample_oauth_access_token_1234567890';
  const encrypted = encrypt(plainToken);

  assert.notEqual(encrypted, plainToken);
  assert.ok(encrypted.includes(':'));

  const decrypted = decrypt(encrypted);
  assert.equal(decrypted, plainToken);
});

test('CryptoService - throws on tampered token', () => {
  const plainToken = 'secret-refresh-token';
  const encrypted = encrypt(plainToken);
  const parts = encrypted.split(':');

  // Tamper ciphertext
  const tampered = `${parts[0]}:${parts[1]}:deadbeef${parts[2]}`;

  assert.throws(() => {
    decrypt(tampered);
  });
});
