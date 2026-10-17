/** A SQL value as JavaScript sees it: INT -> number (BigInt past 2^53), DOUBLE -> number, BOOL -> boolean, TEXT and UUID -> string, BYTES -> Buffer, TIMESTAMP -> Date, NULL -> null. */
export type Value = number | bigint | string | boolean | Buffer | Date | null;
export type Row = Record<string, Value>;
export type Params = Value[] | Value;

/** Who a handle acts as when the app vouches for it: `role` or `roles` name existing roles; every other key is a
 *  session attribute (number, string, boolean, BigInt or Date), read by row policies as SESSION.<key>. */
export type IdentityInput = ({ role: string; roles?: never } | { roles: string[]; role?: never }) & { [attribute: string]: unknown };
export interface Identity { readonly roles: readonly string[]; readonly [attribute: string]: unknown }

export interface RunResult {
  /** Rows written (DELETE reports the count; INSERT and UPDATE report 1, or 0 when the row was not there). */
  changes: number;
  /** Sequence number of the commit, 0 inside a transaction. */
  seq: number;
  /** INSERT only: the primary key of the (last) inserted row — the automatic id, the one given, or a UUID string. */
  lastInsertId?: number | string;
  /** Resolves once the commit is fsynced. Immediate when the database was opened with { durable: true }. */
  readonly durable: Promise<void>;
}

export interface ChangeEvent { type: 'change'; op: 'insert' | 'update' | 'delete'; table: string; key: number; seq: number; /** the new row, or the old row for a delete */ row: Row | null; }
export interface LiveEvent { type: 'live'; rows: Row[]; added: Row[]; removed: Row[]; }
export interface OverflowEvent { type: 'overflow'; seq: number; }
export type Event = ChangeEvent | LiveEvent | OverflowEvent;

export class Subscription implements AsyncIterable<Event> {
  on(type: 'change', fn: (ev: ChangeEvent) => void): this;
  on(type: 'overflow', fn: (ev: OverflowEvent) => void): this;
  on(type: 'close', fn: () => void): this;
  /** The next event, or null once closed. */
  next(): Promise<Event | null>;
  /** Let the process exit while this subscription is open. */
  unref(): this;
  close(): void;
  [Symbol.asyncIterator](): AsyncIterator<Event>;
}

export interface Watch { close(): void; unref(): void; }
export interface WatchInfo { added: Row[]; removed: Row[]; initial?: boolean; overflow?: boolean; }

export class Statement {
  readonly sql: string;
  readonly columns: string[];
  get(...params: Value[] | [Value[]]): Row | null;
  all(...params: Value[] | [Value[]]): Row[];
  run(...params: Value[] | [Value[]]): RunResult;
  allAsync(...params: Value[] | [Value[]]): Promise<Row[]>;
}

export class Database {
  readonly path: string;
  readonly durable: boolean;
  /** The database user this handle acts as (`as('alice')`), or null. */
  readonly user: string | null;
  /** The identity this handle acts as (`as({ role, ...attributes })`), or null. */
  readonly identity: Identity | null;
  /** Transactions retried because another writer got there first. */
  retries: number;
  readonly rowCount: number;
  /** 'auto' (default), 'native' or 'packed': how rows become objects. */
  rowMode: 'auto' | 'native' | 'packed';
  prepare(sql: string): Statement;
  exec(sql: string, params?: Params): Row[];
  get(sql: string, params?: Params): Row | null;
  all(sql: string, params?: Params): Row[];
  run(sql: string, params?: Params): RunResult;
  flush(): Promise<void>;
  transaction<A extends unknown[], R>(fn: (tx: Database, ...args: A) => R, opts?: { retries?: number }): (...args: A) => R;
  subscribe(sql: string, params?: Params): Subscription;
  watch(sql: string, params: Params | undefined, fn: (rows: Row[] | null, info: WatchInfo) => void): Watch;
  /** Act as a user created in the database: their roles and attributes apply to every call. */
  as(user: string): Database;
  /** Act as an identity your app vouches for after its own login: existing roles, plus attributes that row policies
   *  read as SESSION.<name>. Nothing is stored; cheap enough per request. */
  as(identity: IdentityInput): Database;
  close(): void;
}

export class SqweezError extends Error {
  /** CONFLICT, DUPLICATE, FOREIGN_KEY, DENIED, NOT_FOUND, TYPE, ARGUMENTS, TRANSACTION, SYNTAX, SCHEMA, CONFIG, PATH (the path is a file,
   *  a non-empty directory that is not a database, or a database from a newer format), CLOSED or ERROR. */
  code: string;
}

export class RunResult { changes: number; seq: number; lastInsertId?: number | string; readonly durable: Promise<void>; }

/** A duration: '5ms', '1s', '30s', '2m', '1h', '7d', or milliseconds. */
export type DurationLike = string | number;
/** A size: '256MB', '1GB', or bytes. 0 turns a policy off. */
export type SizeLike = string | number;
export interface BackupOptions {
  endpoint: string; bucket: string; prefix?: string; region?: string; accessKey: string; secretKey: string;
  /** AES-256-GCM under a key derived from this passphrase; omit for plaintext (still compressed). */
  passphrase?: string;
  interval?: DurationLike; compress?: boolean; compactAfter?: DurationLike; compactEvery?: DurationLike;
  /** When the directory is empty and the bucket is not, restore the latest state before opening. */
  bootstrap?: boolean;
}
export interface OpenOptions {
  /** A TOML file with these same keys, read first and overridden by the options here. Only the application chooses it; nothing is read from the data directory. */
  configFile?: string;
  /** false (default): writes return when visible, `durable` promises cover the fsync. true: every commit waits for its fsync. 'cloud': every commit waits for the bucket. */
  durable?: boolean | 'cloud';
  /** Create the directory when it holds no database (default true). An existing database is never wiped. */
  create?: boolean;
  backup?: BackupOptions;
  /** Open as a read-only replica of a primary's directory on this host, or of the bucket a primary backs up to. */
  follow?: { dir?: string; bucket?: BackupOptions; poll?: DurationLike };
  /** When the engine snapshots on its own: after this much new log (default '64MB') or after this long without writes (default '30s'). */
  snapshot?: { logBytes?: SizeLike; idleAfter?: DurationLike };
  /**
   * Doors for other processes: a Unix socket path, a TCP address, and `http`, the web console served from this process
   * (e.g. '127.0.0.1:7780'). TCP and the console need `token`. Closing the database closes them and frees the ports.
   */
  listen?: { socket?: string; tcp?: string; http?: string; token?: string };
  live?: { debounce?: DurationLike; queue?: number };
  log?: { threadsIdleAfter?: DurationLike };
  /** How rows become objects (this binding only). */
  rowMode?: 'auto' | 'native' | 'packed';
}
/** Open or create the database directory with these options (and the file `configFile` names, if any). */
export function open(path: string, opts?: OpenOptions): Database;
