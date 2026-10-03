import { describe, it, expect, afterEach } from 'vitest';
import { mkdtempSync, mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WhatsAppClient } from './whatsapp.js';

let root: string;
const fresh = () => (root = mkdtempSync(join(tmpdir(), 'wa-auth-')));
afterEach(() => rmSync(root, { recursive: true, force: true }));

describe('resolveAuthDir (issue #4)', () => {
  it('uses .baileys_auth-<session> on a fresh install', () => {
    fresh();
    expect(WhatsAppClient.resolveAuthDir('work', root)).toBe(join(root, '.baileys_auth-work'));
  });
  it('keeps an existing legacy .baileys_auth so paired sessions do not move', () => {
    fresh();
    mkdirSync(join(root, '.baileys_auth'));
    expect(WhatsAppClient.resolveAuthDir('eric-session', root)).toBe(join(root, '.baileys_auth'));
  });
  it('prefers the named directory when both exist', () => {
    fresh();
    mkdirSync(join(root, '.baileys_auth'));
    mkdirSync(join(root, '.baileys_auth-work'));
    expect(WhatsAppClient.resolveAuthDir('work', root)).toBe(join(root, '.baileys_auth-work'));
  });
  it('separates two sessions once each has its own directory', () => {
    fresh();
    mkdirSync(join(root, '.baileys_auth-a'));
    expect(WhatsAppClient.resolveAuthDir('a', root)).not.toBe(WhatsAppClient.resolveAuthDir('b', root));
  });
  it('rejects session names that could escape the package root', () => {
    fresh();
    for (const bad of ['../x', 'a/b', '.hidden', '']) {
      expect(() => WhatsAppClient.resolveAuthDir(bad, root)).toThrow(/Invalid WHATSAPP_SESSION_NAME/);
    }
  });
});
