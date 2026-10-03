import { describe, it, expect } from 'vitest';
import { WhatsAppClient } from './whatsapp.js';

const G = '120363000000000000@g.us';
const P = '111@lid';
// Internals under test; the client is never initialized (no socket).
const client = new WhatsAppClient('unwrap-test') as unknown as {
  waMessageToEntry(m: unknown): { id: string; body: string; hasMedia: boolean; edited?: boolean; waKey: { id: string } } | null;
  buffer: { upsert(j: string, e: unknown[]): void; get(j: string, n: number): Array<{ id: string; body: string; timestamp: number; waKey: { id: string } }> };
};
const msg = (id: string, message: unknown, ts = 1000) => ({
  key: { remoteJid: G, id, participant: P, fromMe: false }, message, messageTimestamp: ts, pushName: 'Pat',
});

describe('wrapped messages (#5)', () => {
  it('captures an editedMessage-wrapped image caption', () => {
    const e = client.waMessageToEntry(msg('A', { editedMessage: { message: { imageMessage: { caption: 'Field trip Friday' } } } }));
    expect(e?.body).toBe('Field trip Friday');
    expect(e?.hasMedia).toBe(true);
  });
  it('captures ephemeral text', () => {
    expect(client.waMessageToEntry(msg('B', { ephemeralMessage: { message: { conversation: 'hi' } } }))?.body).toBe('hi');
  });
  it('captures documentWithCaptionMessage', () => {
    const e = client.waMessageToEntry(msg('C', { documentWithCaptionMessage: { message: { documentMessage: { caption: 'form.pdf' } } } }));
    expect(e?.body).toBe('form.pdf');
    expect(e?.hasMedia).toBe(true);
  });
  it('applies a live edit in place, keeping the original id, timestamp, and key', () => {
    const original = client.waMessageToEntry(msg('ORIG', { conversation: 'Meet at 3' }, 1000))!;
    client.buffer.upsert(G, [original]);
    const edit = client.waMessageToEntry(msg('EDITMSG', {
      protocolMessage: { type: 14, key: { remoteJid: G, id: 'ORIG' }, editedMessage: { conversation: 'Meet at 4' } },
    }, 2000))!;
    expect(edit.id).toBe('ORIG');
    expect(edit.edited).toBe(true);
    client.buffer.upsert(G, [edit]);
    const buf = client.buffer.get(G, 10);
    expect(buf).toHaveLength(1);
    expect(buf[0].body).toBe('Meet at 4');
    expect(buf[0].timestamp).toBe(1000);
    expect(buf[0].waKey.id).toBe('ORIG');
  });
});
