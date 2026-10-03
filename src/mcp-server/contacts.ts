import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'fs';
import { dirname } from 'path';

/**
 * ID -> display-name table persisted as JSON next to the session.
 *
 * Writes are skipped unless a name actually changed, and go through a temp
 * file + rename so a crash can never leave a half-written contacts.json.
 */
export class ContactBook {
  private names = new Map<string, string>();
  private dirty = false;

  constructor(private readonly path: string) {}

  get size(): number {
    return this.names.size;
  }

  get(id: string): string | undefined {
    return this.names.get(id);
  }

  /** Returns true when the stored name changed. */
  set(id: string, name: string): boolean {
    if (this.names.get(id) === name) return false;
    this.names.set(id, name);
    this.dirty = true;
    return true;
  }

  /** Load the file if present. Loading does not mark the book dirty. */
  load(): void {
    if (!existsSync(this.path)) return;
    const data = JSON.parse(readFileSync(this.path, 'utf-8'));
    for (const [id, name] of Object.entries(data)) this.names.set(id, String(name));
  }

  /** Returns true when a write happened. On failure the book stays dirty for the next attempt. */
  save(): boolean {
    if (!this.dirty) return false;
    const tmp = `${this.path}.${process.pid}.tmp`;
    try {
      mkdirSync(dirname(this.path), { recursive: true });
      writeFileSync(tmp, JSON.stringify(Object.fromEntries(this.names)), 'utf-8');
      renameSync(tmp, this.path);
    } catch (err) {
      try {
        unlinkSync(tmp);
      } catch {
        // temp file may not exist
      }
      throw err;
    }
    this.dirty = false;
    return true;
  }
}
