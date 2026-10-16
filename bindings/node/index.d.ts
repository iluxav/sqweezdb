/** A SQL value as JavaScript sees it: INT -> number (BigInt past 2^53), DOUBLE -> number, BOOL -> boolean, TEXT -> string, BYTES -> Buffer, TIMESTAMP -> Date, NULL -> null. */
export type Value = number | bigint | string | boolean | Buffer | Date | null;
export type Row = Record<string, Value>;
export type Params = Value[] | Value;

export interface RunResult {
  /** Rows written (DELETE reports the count; INSERT and UPDATE report 1, or 0 when the row was not there). */
  changes: number;
  /** Sequence number of the commit, 0 inside a transaction. */
  seq: number;
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
  readonly user: string | null;
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
  as(user: string): Database;
  close(): void;
}

export class SqweezError extends Error {
  /** CONFLICT, DUPLICATE, FOREIGN_KEY, DENIED, NOT_FOUND, TYPE, ARGUMENTS, TRANSACTION, SYNTAX, SCHEMA, CLOSED or ERROR. */
  code: string;
}

export class RunResult { changes: number; seq: number; readonly durable: Promise<void>; }

/** Options: durable (default false: writes return when visible and `durable` promises cover the fsync); rowMode (default 'auto'). */
export function open(path: string, opts?: { durable?: boolean; rowMode?: 'auto' | 'native' | 'packed' }): Database;
