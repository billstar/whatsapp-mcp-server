import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
} from '@whiskeysockets/baileys';
import type { WASocket, WAMessage, GroupMetadata } from '@whiskeysockets/baileys';
import qrcode from 'qrcode-terminal';
import pino from 'pino';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { format } from 'date-fns';
import { ContactBook } from './contacts.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface WhatsAppGroupSummary {
  id: string;
  name: string;
  memberCount: number;
  lastMessage: string;
  lastActivityTimestamp: number;
}

export interface WhatsAppMessageEntry {
  id: string;
  body: string;
  author: string;
  authorName: string;
  timestamp: number;
  hasMedia: boolean;
  isForwarded: boolean;
  quotedMsg?: { body: string; author: string } | undefined;
}

export interface GroupParticipant {
  id: string;
  name: string;
  isAdmin: boolean;
}

export interface WhatsAppGroupInfo {
  id: string;
  name: string;
  description: string;
  participants: GroupParticipant[];
  createdAt: number;
}

export interface GetMessagesOptions {
  limit?: number;
  after?: number;
  before?: number;
}

// ---------------------------------------------------------------------------
// Logging — always stderr so MCP protocol (stdout) is not polluted
// ---------------------------------------------------------------------------

function log(level: 'info' | 'warn' | 'error', message: string, data?: unknown): void {
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [whatsapp-client] [${level.toUpperCase()}]`;
  if (data !== undefined) {
    process.stderr.write(`${prefix} ${message} ${JSON.stringify(data)}\n`);
  } else {
    process.stderr.write(`${prefix} ${message}\n`);
  }
}

// ---------------------------------------------------------------------------
// Async Mutex — serializes WhatsApp WebSocket calls (cheap insurance)
// ---------------------------------------------------------------------------

class AsyncMutex {
  private queue: Array<() => void> = [];
  private locked = false;
  private lastRelease = 0;

  constructor(private readonly minIntervalMs: number = 100) {}

  async acquire(): Promise<void> {
    if (this.locked) {
      await new Promise<void>((resolve) => this.queue.push(resolve));
    }
    this.locked = true;

    const now = Date.now();
    const elapsed = now - this.lastRelease;
    if (elapsed < this.minIntervalMs && this.lastRelease > 0) {
      const delay = this.minIntervalMs - elapsed;
      await new Promise<void>((resolve) => setTimeout(resolve, delay));
    }
  }

  release(): void {
    this.lastRelease = Date.now();
    const next = this.queue.shift();
    if (next) {
      next();
    } else {
      this.locked = false;
    }
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

// ---------------------------------------------------------------------------
// BufferEntry — internal type for the per-group ring buffer
// ---------------------------------------------------------------------------

interface BufferEntry {
  id: string;
  body: string;
  author: string;
  authorName: string;
  timestamp: number;
  hasMedia: boolean;
  isForwarded: boolean;
  fromMe: boolean;
  quotedMsg?: { body: string; author: string };
  waKey: any;
  waMessage: any;
}

// ---------------------------------------------------------------------------
// MessageBuffer — bounded per-group ring buffer with disk persistence
//
// Persists snapshot every 60s AND on graceful shutdown.  Rehydrates on
// startup.  This snapshot is load-bearing — messages.history-set is a
// one-shot on first pair and does NOT re-fire on reconnect.
// ---------------------------------------------------------------------------

class MessageBuffer {
  private buffers = new Map<string, BufferEntry[]>();
  readonly maxPerGroup = 1500;
  private readonly snapshotPath: string;
  private snapshotInterval: NodeJS.Timeout | null = null;
  private _lastUpsertTs = 0;

  constructor(authDir: string) {
    this.snapshotPath = join(authDir, 'buffer.json');
  }

  get lastUpsertTs(): number {
    return this._lastUpsertTs;
  }

  get totalSize(): number {
    let total = 0;
    for (const buf of this.buffers.values()) total += buf.length;
    return total;
  }

  get groupCount(): number {
    return this.buffers.size;
  }

  rehydrate(): boolean {
    try {
      if (!existsSync(this.snapshotPath)) return false;
      const raw = readFileSync(this.snapshotPath, 'utf-8');
      const data: Record<string, BufferEntry[]> = JSON.parse(raw);
      for (const [jid, entries] of Object.entries(data)) {
        this.buffers.set(jid, entries.slice(-this.maxPerGroup));
      }
      log('info', `Buffer rehydrated: ${this.totalSize} messages across ${this.groupCount} groups`);
      return true;
    } catch (err) {
      log('warn', 'Buffer rehydration failed', err);
      return false;
    }
  }

  upsert(jid: string, entries: BufferEntry[]): void {
    let buf = this.buffers.get(jid);
    if (!buf) {
      buf = [];
      this.buffers.set(jid, buf);
    }

    const existingIds = new Set(buf.map((e) => e.id));
    for (const entry of entries) {
      if (!existingIds.has(entry.id)) {
        buf.push(entry);
        existingIds.add(entry.id);
      }
    }

    buf.sort((a, b) => a.timestamp - b.timestamp);
    if (buf.length > this.maxPerGroup) {
      this.buffers.set(jid, buf.slice(-this.maxPerGroup));
    }

    this._lastUpsertTs = Date.now();
  }

  get(jid: string, limit: number, after?: number, before?: number): BufferEntry[] {
    const buf = this.buffers.get(jid) || [];
    let filtered: BufferEntry[] = buf;

    if (after !== undefined) {
      filtered = filtered.filter((e) => e.timestamp >= after);
    }
    if (before !== undefined) {
      filtered = filtered.filter((e) => e.timestamp <= before);
    }

    return filtered.slice(-limit);
  }

  getForGroup(jid: string): BufferEntry[] {
    return this.buffers.get(jid) || [];
  }

  search(query: string, jid?: string, limit = 50): BufferEntry[] {
    const lowerQuery = query.toLowerCase();
    const results: BufferEntry[] = [];

    const jids = jid ? [jid] : [...this.buffers.keys()];
    for (const j of jids) {
      const buf = this.buffers.get(j) || [];
      for (const entry of buf) {
        if (entry.body.toLowerCase().includes(lowerQuery)) {
          results.push(entry);
          if (results.length >= limit) return results;
        }
      }
    }

    return results;
  }

  findById(messageId: string): BufferEntry | undefined {
    for (const buf of this.buffers.values()) {
      const found = buf.find((e) => e.id === messageId);
      if (found) return found;
    }
    return undefined;
  }

  snapshot(): void {
    try {
      const data: Record<string, BufferEntry[]> = {};
      for (const [jid, entries] of this.buffers.entries()) {
        data[jid] = entries;
      }
      const dir = join(this.snapshotPath, '..');
      if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
      writeFileSync(this.snapshotPath, JSON.stringify(data), 'utf-8');
      log('info', `Buffer snapshot: ${this.totalSize} messages`);
    } catch (err) {
      log('error', 'Buffer snapshot failed', err);
    }
  }

  startPeriodicSnapshot(): void {
    if (this.snapshotInterval) return;
    this.snapshotInterval = setInterval(() => this.snapshot(), 60_000);
  }

  stopPeriodicSnapshot(): void {
    if (this.snapshotInterval) {
      clearInterval(this.snapshotInterval);
      this.snapshotInterval = null;
    }
  }
}

// ---------------------------------------------------------------------------
// Group list hygiene — pure, exported for unit testing
//
// groupFetchAllParticipating() can surface more than one JID with the same
// subject: the live group, a Community parent/announce shell, and stale or
// migrated JIDs left behind after a group upgrade. Those orphans show up with
// a single participant (just you) and no buffered activity. Dedupe by JID and
// drop the degenerate (<2 member) entries so name resolution and the daily
// brief are never fed phantom groups. Real targeting is by JID — see
// resolveGroup in tools.ts.
// ---------------------------------------------------------------------------

export function dedupeAndFilterGroups(
  summaries: WhatsAppGroupSummary[],
): WhatsAppGroupSummary[] {
  const byJid = new Map<string, WhatsAppGroupSummary>();
  for (const s of summaries) {
    if (s.memberCount < 2) continue; // orphaned / migrated JID — never a real target
    const existing = byJid.get(s.id);
    if (!existing || s.lastActivityTimestamp > existing.lastActivityTimestamp) {
      byJid.set(s.id, s);
    }
  }
  return [...byJid.values()].sort(
    (a, b) => b.lastActivityTimestamp - a.lastActivityTimestamp,
  );
}

// ---------------------------------------------------------------------------
// WhatsAppClient — Baileys WebSocket client
// ---------------------------------------------------------------------------

export class WhatsAppClient {
  private sock: WASocket | null = null;
  private ready = false;
  private buffer: MessageBuffer;
  private mutex = new AsyncMutex(100);
  private contacts: ContactBook;
  private connectionOpen = false;
  private bufferWarm = false;
  private readyResolve: (() => void) | null = null;
  private destroying = false;
  private contactsTimer: ReturnType<typeof setInterval> | null = null;
  private saveCreds: (() => Promise<void>) | null = null;

  // Anchored to the package root so the paired session is found no matter
  // which directory the MCP client launches the server from.
  /**
   * Session directory, anchored to the package root. Keyed by session name
   * (`.baileys_auth-<name>`) so separate sessions never share credentials.
   * Installs that predate per-session directories keep using `.baileys_auth`
   * when it exists and the named directory doesn't, so no paired session moves.
   */
  static resolveAuthDir(sessionName: string, root = join(dirname(fileURLToPath(import.meta.url)), '..', '..')): string {
    if (!/^[A-Za-z0-9._-]+$/.test(sessionName) || sessionName.startsWith('.')) {
      throw new Error(`Invalid WHATSAPP_SESSION_NAME "${sessionName}": use letters, digits, ".", "_" or "-"`);
    }
    const named = join(root, `.baileys_auth-${sessionName}`);
    const legacy = join(root, '.baileys_auth');
    if (existsSync(named)) return named;
    if (existsSync(legacy)) return legacy;
    return named;
  }

  private readonly authDir: string;
  private static readonly BAILEYS_LOGGER = pino({ level: 'silent' }, pino.destination(2));

  constructor(private readonly sessionName: string) {
    this.authDir = WhatsAppClient.resolveAuthDir(sessionName);
    this.buffer = new MessageBuffer(this.authDir);
    this.contacts = new ContactBook(join(this.authDir, 'contacts.json'));
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  async initialize(): Promise<void> {
    log('info', `Initializing WhatsApp client (Baileys), session dir: ${this.authDir}`);

    this.loadContacts();
    const rehydrated = this.buffer.rehydrate();
    this.bufferWarm = rehydrated;

    await this.createSocket();
    await this.waitForReady();

    this.buffer.startPeriodicSnapshot();
    this.contactsTimer = setInterval(() => this.saveContacts(), 60_000);
    log(
      'info',
      `WhatsApp client ready (buffer: ${this.buffer.totalSize} messages across ${this.buffer.groupCount} groups)`,
    );
  }

  private async createSocket(): Promise<void> {
    const { state, saveCreds } = await useMultiFileAuthState(this.authDir);
    this.saveCreds = saveCreds;

    const { version } = await fetchLatestBaileysVersion();
    log('info', `WA Web version: ${version.join('.')}`);

    this.sock = makeWASocket({
      auth: state,
      version,
      logger: WhatsAppClient.BAILEYS_LOGGER,
      // NOTE: do NOT set syncFullHistory:true + a desktop browser (Browsers.macOS)
      // here. That makes Baileys claim the DARWIN platform and request a full
      // history handshake, which this already-android-paired account rejects —
      // producing a tight statusCode=428 reconnect loop that never reaches
      // 'open'. The full seed only works on a fresh pairing anyway. On-demand
      // backfill (syncGroupHistory / fetchMessageHistory) works without it.
      getMessage: async (key) => {
        const entry = this.buffer.findById(key.id || '');
        return entry?.waMessage || undefined;
      },
    });

    this.sock.ev.on('creds.update', () => this.saveCreds?.());
    this.registerEventHandlers();
  }

  isReady(): boolean {
    return this.ready;
  }

  async destroy(): Promise<void> {
    log('info', 'Shutting down WhatsApp client...');
    this.destroying = true;
    this.ready = false;
    this.buffer.snapshot();
    this.buffer.stopPeriodicSnapshot();
    if (this.contactsTimer) clearInterval(this.contactsTimer);
    this.saveContacts();
    try {
      this.sock?.end(undefined);
    } catch {
      // socket may already be closed
    }
    log('info', 'WhatsApp client destroyed.');
  }

  // -----------------------------------------------------------------------
  // Groups
  // -----------------------------------------------------------------------

  async getGroups(): Promise<WhatsAppGroupSummary[]> {
    this.ensureReady();
    return this.mutex.run(async () => {
      log('info', 'getGroups: fetching participating groups...');
      const groups = await this.sock!.groupFetchAllParticipating();

      const summaries: WhatsAppGroupSummary[] = Object.values(groups).map(
        (g: GroupMetadata) => {
          const buf = this.buffer.getForGroup(g.id);
          const last = buf.length > 0 ? buf[buf.length - 1] : null;

          return {
            id: g.id,
            name: g.subject,
            memberCount: g.participants?.length ?? 0,
            lastMessage: last?.body ?? '',
            lastActivityTimestamp: last?.timestamp ?? 0,
          };
        },
      );

      const filtered = dedupeAndFilterGroups(summaries);
      log(
        'info',
        `getGroups: returning ${filtered.length} groups ` +
          `(${summaries.length - filtered.length} phantom/duplicate entries filtered)`,
      );
      return filtered;
    });
  }

  async getGroupMessages(
    groupId: string,
    options: GetMessagesOptions = {},
  ): Promise<WhatsAppMessageEntry[]> {
    this.ensureReady();
    const { limit = 200, after, before } = options;
    log('info', `getGroupMessages: groupId=${groupId}, limit=${limit}`);

    const entries = this.buffer.get(groupId, limit, after, before);

    const result: WhatsAppMessageEntry[] = entries.map((e) => ({
      id: e.id,
      body: e.body,
      author: e.author,
      authorName: this.entryName(e),
      timestamp: e.timestamp,
      hasMedia: e.hasMedia,
      isForwarded: e.isForwarded,
      quotedMsg: this.resolveQuoted(e.quotedMsg),
    }));

    log('info', `getGroupMessages: returning ${result.length} messages`);
    return result;
  }

  /**
   * On-demand history backfill for one group. Walks backwards from the oldest
   * message currently in the buffer, requesting older chunks from WhatsApp via
   * fetchMessageHistory. Results arrive asynchronously on the
   * messaging-history.set (ON_DEMAND) event, which upserts them into the same
   * buffer — so each round we fire a request and poll for the buffer to grow.
   *
   * Hard constraints, surfaced to the caller rather than hidden:
   *  - Needs an anchor: a group with zero buffered messages has nothing to walk
   *    back from. It must be seeded first (live message or full-history sync).
   *  - Bounded by MessageBuffer.maxPerGroup (the local per-group cap).
   *  - Bounded by what WhatsApp's servers still retain — when a round returns
   *    nothing new, older history is simply gone and we stop.
   */
  async syncGroupHistory(
    groupId: string,
    targetCount = 1500,
  ): Promise<{ synced: number; total: number; note?: string }> {
    this.ensureReady();
    const cap = Math.min(targetCount, this.buffer.maxPerGroup);
    const startCount = this.buffer.getForGroup(groupId).length;

    if (startCount === 0) {
      return {
        synced: 0,
        total: 0,
        note:
          'No anchor message for this group — on-demand history needs at least ' +
          'one existing message to walk back from. Seed it first via a fresh ' +
          'full-history sync (re-pair) or by waiting for a live message.',
      };
    }

    const MAX_ROUNDS = 12;
    for (let round = 0; round < MAX_ROUNDS; round++) {
      const buf = this.buffer.getForGroup(groupId);
      if (buf.length >= cap) break;

      const oldest = buf[0]; // buffer is sorted ascending by timestamp
      const want = Math.min(50, cap - buf.length);
      const baseline = buf.length;

      log(
        'info',
        `syncGroupHistory: ${groupId} round ${round + 1}, requesting ${want} before ts=${oldest.timestamp}`,
      );
      await this.mutex.run(() =>
        this.sock!.fetchMessageHistory(want, oldest.waKey, oldest.timestamp),
      );

      // ON_DEMAND results come back on the messaging-history.set event.
      const grew = await this.waitForBufferGrowth(groupId, baseline, 15_000);
      if (!grew) {
        log('info', `syncGroupHistory: ${groupId} no more history returned — stopping`);
        break;
      }
    }

    const total = this.buffer.getForGroup(groupId).length;
    return { synced: total - startCount, total };
  }

  private waitForBufferGrowth(
    groupId: string,
    baseline: number,
    timeoutMs: number,
  ): Promise<boolean> {
    return new Promise((resolve) => {
      const start = Date.now();
      const iv = setInterval(() => {
        if (this.buffer.getForGroup(groupId).length > baseline) {
          clearInterval(iv);
          resolve(true);
        } else if (Date.now() - start > timeoutMs) {
          clearInterval(iv);
          resolve(false);
        }
      }, 500);
    });
  }

  async getGroupInfo(groupId: string): Promise<WhatsAppGroupInfo> {
    this.ensureReady();
    return this.mutex.run(async () => {
      log('info', `getGroupInfo: groupId=${groupId}`);
      const meta = await this.sock!.groupMetadata(groupId);

      const participants: GroupParticipant[] = (meta.participants ?? []).map((p) => ({
        id: p.id,
        name: this.contacts.get(p.id) || p.id.replace(/@.*/, ''),
        isAdmin: p.admin === 'admin' || p.admin === 'superadmin',
      }));

      log(
        'info',
        `getGroupInfo: returning "${meta.subject}" with ${participants.length} participants`,
      );
      return {
        id: meta.id,
        name: meta.subject,
        description: meta.desc ?? '',
        participants,
        createdAt: meta.creation ?? 0,
      };
    });
  }

  // -----------------------------------------------------------------------
  // Search
  // -----------------------------------------------------------------------

  async searchMessages(
    query: string,
    groupId?: string,
    limit = 50,
  ): Promise<WhatsAppMessageEntry[]> {
    this.ensureReady();
    log('info', `searchMessages: query="${query}", groupId=${groupId ?? 'all'}, limit=${limit}`);

    const entries = this.buffer.search(query, groupId, limit);

    const results: WhatsAppMessageEntry[] = entries.map((e) => ({
      id: e.id,
      body: e.body,
      author: e.author,
      authorName: this.entryName(e),
      timestamp: e.timestamp,
      hasMedia: e.hasMedia,
      isForwarded: e.isForwarded,
      quotedMsg: this.resolveQuoted(e.quotedMsg),
    }));

    log('info', `searchMessages: returning ${results.length} results`);
    return results;
  }

  // -----------------------------------------------------------------------
  // Export
  // -----------------------------------------------------------------------

  async exportChat(groupId: string, limit = 500): Promise<string> {
    this.ensureReady();
    log('info', `exportChat: groupId=${groupId}, limit=${limit}`);

    const entries = this.buffer.get(groupId, limit);
    const lines: string[] = [];

    for (const e of entries) {
      const date = new Date(e.timestamp * 1000);
      const dateStr = format(date, 'dd/MM/yyyy, HH:mm:ss');
      const body = e.hasMedia ? '<Media omitted>' : (e.body || '');

      const bodyLines = body.split('\n');
      lines.push(`[${dateStr}] ${this.entryName(e)}: ${bodyLines[0]}`);
      for (let i = 1; i < bodyLines.length; i++) {
        lines.push(bodyLines[i]);
      }
    }

    log('info', `exportChat: returning ${lines.length} lines`);
    return lines.join('\n');
  }

  // -----------------------------------------------------------------------
  // Send / Reply
  // -----------------------------------------------------------------------

  async sendMessage(
    chatId: string,
    text: string,
    quotedMessageId?: string,
  ): Promise<{ id: string; timestamp: number }> {
    this.ensureReady();
    return this.mutex.run(async () => {
      log(
        'info',
        `sendMessage: chatId=${chatId}, quotedMessageId=${quotedMessageId ?? 'none'}`,
      );

      const options: any = {};
      if (quotedMessageId) {
        const quotedEntry = this.buffer.findById(quotedMessageId);
        if (quotedEntry?.waKey) {
          options.quoted = {
            key: quotedEntry.waKey,
            message: quotedEntry.waMessage,
            messageTimestamp: quotedEntry.timestamp,
          } as WAMessage;
        } else {
          log('warn', `sendMessage: quoted message ${quotedMessageId} not in buffer`);
        }
      }

      const sent = await this.sock!.sendMessage(chatId, { text }, options);
      const msgId = sent?.key?.id || '';
      const timestamp =
        typeof sent?.messageTimestamp === 'number'
          ? sent.messageTimestamp
          : Number(sent?.messageTimestamp) || Math.floor(Date.now() / 1000);

      if (sent) {
        const entry = this.waMessageToEntry(sent);
        if (entry) this.buffer.upsert(chatId, [entry]);
      }

      log('info', `sendMessage: sent id=${msgId}`);
      return { id: msgId, timestamp };
    });
  }

  // -----------------------------------------------------------------------
  // Private — event handlers
  // -----------------------------------------------------------------------

  private registerEventHandlers(): void {
    if (!this.sock) return;

    this.sock.ev.on('messages.upsert', ({ messages }) => {
      for (const msg of messages) {
        const jid = msg.key.remoteJid;
        if (jid) {
          const entry = this.waMessageToEntry(msg);
          if (entry) this.buffer.upsert(jid, [entry]);
        }
      }
    });

    this.sock.ev.on(
      'messaging-history.set',
      ({ messages, contacts: histContacts, isLatest }) => {
        log('info', `messaging-history.set: ${messages.length} msgs, isLatest=${isLatest}`);
        for (const c of histContacts) {
          const name = (c as any).notify || (c as any).name || '';
          if (name && c.id) this.contacts.set(c.id, name);
        }
        for (const msg of messages) {
          const jid = msg.key.remoteJid;
          if (jid) {
            const entry = this.waMessageToEntry(msg);
            if (entry) this.buffer.upsert(jid, [entry]);
          }
        }
        if (isLatest) {
          this.bufferWarm = true;
          if (this.connectionOpen) this.tryMarkReady();
        }
      },
    );

    this.sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        // First-time pairing. Render to stderr — stdout carries the MCP
        // protocol under the stdio transport and must not be polluted.
        log(
          'info',
          'Pair this device: WhatsApp → Settings → Linked Devices → Link a device, then scan:',
        );
        qrcode.generate(qr, { small: true }, (art) => process.stderr.write(`${art}\n`));
      }
      if (connection === 'open') {
        log('info', 'Connection open');
        this.connectionOpen = true;
        if (this.bufferWarm) this.tryMarkReady();
      } else if (connection === 'close') {
        this.connectionOpen = false;
        this.ready = false;
        if (this.destroying) return;
        const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
        if (statusCode === DisconnectReason.loggedOut) {
          log('error', 'Logged out — exiting hard');
          setTimeout(() => process.exit(1), 50);
        } else {
          log('warn', `Connection closed (statusCode=${statusCode}), reconnecting in 3s`);
          setTimeout(() => {
            if (!this.destroying) {
              this.createSocket().catch((err) =>
                log('error', 'Reconnect failed', err),
              );
            }
          }, 3000);
        }
      }
    });

    this.sock.ev.on('contacts.upsert', (contacts) => {
      for (const c of contacts) {
        const name = (c as any).notify || (c as any).name || '';
        if (name && c.id) this.contacts.set(c.id, name);
      }
    });

    this.sock.ev.on('contacts.update', (updates) => {
      for (const u of updates) {
        const name = (u as any).notify || (u as any).name;
        if (name && u.id) this.contacts.set(u.id, name);
      }
    });
  }

  private tryMarkReady(): void {
    if (this.ready) return;
    this.ready = true;
    if (this.readyResolve) {
      this.readyResolve();
      this.readyResolve = null;
    }
  }

  private waitForReady(): Promise<void> {
    if (this.ready) return Promise.resolve();

    return new Promise<void>((resolve) => {
      this.readyResolve = resolve;
      setTimeout(() => {
        if (!this.ready) {
          log('warn', 'Ready timeout (30s) — proceeding with current buffer state');
          this.bufferWarm = true;
          this.connectionOpen = true;
          this.tryMarkReady();
        }
      }, 30_000);
    });
  }

  private ensureReady(): void {
    if (!this.ready) {
      throw new Error(
        'WhatsApp client is not ready. Call initialize() first and wait for it to resolve.',
      );
    }
  }

  // -----------------------------------------------------------------------
  // Private — message conversion
  // -----------------------------------------------------------------------

  private waMessageToEntry(msg: WAMessage): BufferEntry | null {
    if (!msg.key.remoteJid || !msg.message) return null;

    const body = this.extractBody(msg);
    const hasMedia = this.checkMedia(msg);

    if (!body && !hasMedia) return null;

    const contextInfo = this.extractContextInfo(msg);

    let quotedMsg: { body: string; author: string } | undefined;
    if (contextInfo?.quotedMessage) {
      quotedMsg = {
        body:
          contextInfo.quotedMessage.conversation ||
          contextInfo.quotedMessage.extendedTextMessage?.text ||
          '',
        author: contextInfo.participant || '',
      };
    }

    // Group senders live in key.participant for live messages but in the
    // top-level WebMessageInfo.participant for history-synced ones.
    const sender = msg.key.participant || msg.participant || '';
    this.learnName(msg, sender);

    const timestamp =
      typeof msg.messageTimestamp === 'number'
        ? msg.messageTimestamp
        : Number(msg.messageTimestamp) || 0;

    return {
      id: msg.key.id || '',
      body,
      author: sender || msg.key.remoteJid || '',
      authorName: this.resolveAuthorName(msg, sender),
      timestamp,
      hasMedia,
      isForwarded: !!contextInfo?.isForwarded,
      fromMe: msg.key.fromMe || false,
      quotedMsg,
      // Keep the sender in the key so quoted replies in groups are well-formed.
      waKey: sender ? { ...msg.key, participant: sender } : msg.key,
      waMessage: msg.message,
    };
  }

  private loadContacts(): void {
    try {
      this.contacts.load();
      log('info', `Loaded ${this.contacts.size} contact names`);
    } catch (err) {
      log('warn', 'Could not load contacts.json', err);
    }
  }

  private saveContacts(): void {
    try {
      this.contacts.save();
    } catch (err) {
      log('warn', 'Could not save contacts.json', err);
    }
  }

  private entryName(e: BufferEntry): string {
    return e.fromMe ? e.authorName : this.nameFor(e.author, e.authorName);
  }

  private resolveQuoted(
    q: { body: string; author: string } | undefined,
  ): { body: string; author: string } | undefined {
    return q ? { ...q, author: q.author ? this.nameFor(q.author) : q.author } : q;
  }

  /** Record a sender's display name under every ID alias WhatsApp gave us. */
  private learnName(msg: WAMessage, sender: string): void {
    if (!msg.pushName || msg.key.fromMe) return;
    const key = msg.key as { participantPn?: string; participantLid?: string };
    for (const id of [sender, key.participantPn, key.participantLid]) {
      if (id) this.contacts.set(id, msg.pushName);
    }
  }

  private resolveAuthorName(msg: WAMessage, sender: string): string {
    if (msg.key.fromMe) return this.sock?.user?.name || 'Me';
    if (sender) return this.nameFor(sender, msg.pushName || '');
    return msg.pushName || 'Unknown';
  }

  /** Display name for a JID: latest known name, else the stored one, else the number. */
  private nameFor(jid: string, fallback = ''): string {
    return this.contacts.get(jid) || fallback || jid.replace(/@.*/, '') || 'Unknown';
  }

  private extractBody(msg: WAMessage): string {
    const m = msg.message;
    if (!m) return '';
    return (
      m.conversation ||
      m.extendedTextMessage?.text ||
      m.imageMessage?.caption ||
      m.videoMessage?.caption ||
      m.documentMessage?.caption ||
      m.listResponseMessage?.title ||
      m.buttonsResponseMessage?.selectedDisplayText ||
      m.templateButtonReplyMessage?.selectedDisplayText ||
      ''
    );
  }

  private checkMedia(msg: WAMessage): boolean {
    const m = msg.message;
    if (!m) return false;
    return !!(
      m.imageMessage ||
      m.videoMessage ||
      m.audioMessage ||
      m.documentMessage ||
      m.stickerMessage
    );
  }

  private extractContextInfo(msg: WAMessage): any {
    const m = msg.message;
    if (!m) return null;
    return (
      m.extendedTextMessage?.contextInfo ||
      m.imageMessage?.contextInfo ||
      m.videoMessage?.contextInfo ||
      m.audioMessage?.contextInfo ||
      m.documentMessage?.contextInfo ||
      null
    );
  }
}

// ---------------------------------------------------------------------------
// Singleton factory
// ---------------------------------------------------------------------------

const instances = new Map<string, WhatsAppClient>();

export function getWhatsAppClient(sessionName = 'default'): WhatsAppClient {
  let instance = instances.get(sessionName);
  if (!instance) {
    instance = new WhatsAppClient(sessionName);
    instances.set(sessionName, instance);
  }
  return instance;
}
