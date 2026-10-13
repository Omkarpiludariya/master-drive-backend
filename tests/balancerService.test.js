import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pickBestAccount } from '../src/services/balancerService.js';

test('BalancerService - selects account with largest free quota', () => {
  const accounts = [
    { accountEmail: 'acc1@gmail.com', freeQuota: 5 * 1024 * 1024 * 1024 }, // 5GB
    { accountEmail: 'acc2@gmail.com', freeQuota: 10 * 1024 * 1024 * 1024 }, // 10GB
    { accountEmail: 'acc3@gmail.com', freeQuota: 2 * 1024 * 1024 * 1024 }, // 2GB
  ];

  const chosen = pickBestAccount(accounts, 1 * 1024 * 1024 * 1024);
  assert.equal(chosen.accountEmail, 'acc2@gmail.com');
});
