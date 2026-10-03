import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ContactBook } from './contacts.js';

let dir: string;
let file: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'contacts-'));
  file = join(dir, 'contacts.json');
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

describe('ContactBook', () => {
  it('only writes when a name actually changes', () => {
    const book = new ContactBook(file);
    expect(book.save()).toBe(false); // nothing to write yet
    expect(readdirSync(dir)).toEqual([]);

    expect(book.set('1@lid', 'Ann')).toBe(true);
    expect(book.save()).toBe(true);
    const written = statSync(file).mtimeMs;

    expect(book.set('1@lid', 'Ann')).toBe(false); // same name again
    expect(book.save()).toBe(false);
    expect(statSync(file).mtimeMs).toBe(written);

    expect(book.set('1@lid', 'Ann B')).toBe(true); // real change
    expect(book.save()).toBe(true);
    expect(JSON.parse(readFileSync(file, 'utf-8'))).toEqual({ '1@lid': 'Ann B' });
  });

  it('does not treat loading as a change', () => {
    writeFileSync(file, JSON.stringify({ '1@lid': 'Ann' }));
    const book = new ContactBook(file);
    book.load();
    expect(book.get('1@lid')).toBe('Ann');
    expect(book.save()).toBe(false);
  });

  it('replaces the file atomically and leaves no temp file behind', () => {
    const book = new ContactBook(file);
    book.set('1@lid', 'Ann');
    book.save();
    expect(readdirSync(dir)).toEqual(['contacts.json']);
  });

  it('leaves the target untouched, cleans up, and retries after a failed write', () => {
    // A non-empty directory at the target path makes the final rename fail.
    mkdirSync(file);
    writeFileSync(join(file, 'keep'), 'x');
    const book = new ContactBook(file);
    book.set('1@lid', 'Ann');

    expect(() => book.save()).toThrow();
    expect(readdirSync(dir)).toEqual(['contacts.json']); // no stray .tmp
    expect(readdirSync(file)).toEqual(['keep']);

    rmSync(file, { recursive: true });
    expect(book.save()).toBe(true); // still dirty, so the retry writes
    expect(JSON.parse(readFileSync(file, 'utf-8'))).toEqual({ '1@lid': 'Ann' });
  });

  it('keeps loaded names when saving new ones', () => {
    writeFileSync(file, JSON.stringify({ '1@lid': 'Ann' }));
    const book = new ContactBook(file);
    book.load();
    book.set('2@lid', 'Bo');
    book.save();
    expect(JSON.parse(readFileSync(file, 'utf-8'))).toEqual({ '1@lid': 'Ann', '2@lid': 'Bo' });
  });
});
