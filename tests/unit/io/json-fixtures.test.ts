import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

const fixturesDir = path.join(__dirname, '../../fixtures');

describe('JSON and NDJSON I/O with test fixtures (US-026)', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-json-fix-'));
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe('fromJSON with users.json fixture', () => {
    it('reads flat JSON array correctly', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));

      expect(df.shape).toEqual([5, 4]);
      expect(df.columns).toEqual(['name', 'age', 'email', 'active']);
    });

    it('reads correct values from users.json', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));

      expect(df.col('name').toArray()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']);
      expect(df.col('age').get(0)).toBe(30);
      expect(df.col('age').get(1)).toBe(25);
      expect(df.col('active').get(0)).toBe(true);
      expect(df.col('active').get(1)).toBe(false);
    });

    it('handles null values in fixture', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));

      // Charlie has null email
      expect(df.col('email').get(2)).toBeNull();
      // Eve has null age
      expect(df.col('age').get(4)).toBeNull();
    });
  });

  describe('fromJSON with nested.json fixture', () => {
    it('reads nested data at path', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'nested.json'), {
        path: 'results.items',
      });

      expect(df.shape).toEqual([3, 3]);
      expect(df.columns).toEqual(['id', 'product', 'price']);
    });

    it('reads correct values from nested path', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'nested.json'), {
        path: 'results.items',
      });

      expect(df.col('id').toArray()).toEqual([1, 2, 3]);
      expect(df.col('product').toArray()).toEqual(['Widget', 'Gadget', 'Doohickey']);
      expect(df.col('price').get(0)).toBeCloseTo(9.99);
      expect(df.col('price').get(1)).toBeCloseTo(24.5);
      expect(df.col('price').get(2)).toBeCloseTo(4.75);
    });

    it('throws on invalid nested path', async () => {
      await expect(
        DataFrame.fromJSON(path.join(fixturesDir, 'nested.json'), {
          path: 'results.nonexistent',
        }),
      ).rejects.toThrow();
    });
  });

  describe('fromNDJSON with logs.ndjson fixture', () => {
    it('reads line-delimited JSON correctly', async () => {
      const df = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));

      expect(df.shape).toEqual([5, 4]);
      expect(df.columns).toEqual(['timestamp', 'level', 'message', 'code']);
    });

    it('reads correct values from logs.ndjson', async () => {
      const df = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));

      expect(df.col('level').toArray()).toEqual(['INFO', 'WARN', 'ERROR', 'INFO', 'DEBUG']);
      expect(df.col('message').get(0)).toBe('Server started');
      expect(df.col('code').get(0)).toBe(200);
    });

    it('handles null values in NDJSON fixture', async () => {
      const df = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));

      // Second log entry has null code
      expect(df.col('code').get(1)).toBeNull();
    });
  });

  describe('toJSON output formats', () => {
    it('produces valid compact JSON', () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', score: 95 },
        { name: 'Bob', score: 87 },
      ]);
      const json = df.toJSON();
      const parsed = JSON.parse(json) as Record<string, unknown>[];

      expect(parsed).toHaveLength(2);
      expect(parsed[0]).toEqual({ name: 'Alice', score: 95 });
      expect(parsed[1]).toEqual({ name: 'Bob', score: 87 });
      // Compact: no newlines within the array structure
      expect(json.startsWith('[')).toBe(true);
    });

    it('produces valid pretty-printed JSON', () => {
      const df = DataFrame.fromRows([{ x: 1, y: 2 }]);
      const json = df.toJSON({ pretty: true });
      const parsed = JSON.parse(json) as Record<string, unknown>[];

      expect(parsed).toEqual([{ x: 1, y: 2 }]);
      // Pretty: has indentation
      expect(json).toContain('\n');
      expect(json).toContain('  ');
    });

    it('writes JSON to file from fixture data', async () => {
      const df = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));
      const outPath = path.join(tmpDir, 'users-out.json');
      await df.toJSON(outPath);

      const content = await fs.readFile(outPath, 'utf-8');
      const parsed = JSON.parse(content) as Record<string, unknown>[];
      expect(parsed).toHaveLength(5);
      expect((parsed[0] as Record<string, unknown>).name).toBe('Alice');
    });

    it('writes pretty JSON to file', async () => {
      const df = DataFrame.fromRows([{ a: 1 }, { a: 2 }]);
      const outPath = path.join(tmpDir, 'pretty.json');
      await df.toJSON(outPath, { pretty: true });

      const content = await fs.readFile(outPath, 'utf-8');
      expect(content).toContain('\n');
      const parsed = JSON.parse(content) as Record<string, unknown>[];
      expect(parsed).toEqual([{ a: 1 }, { a: 2 }]);
    });
  });

  describe('toNDJSON output', () => {
    it('produces one JSON object per line', () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
      const ndjson = df.toNDJSON();
      const lines = ndjson.trim().split('\n');

      expect(lines).toHaveLength(2);
      expect(JSON.parse(lines[0]!)).toEqual({ name: 'Alice', age: 30 });
      expect(JSON.parse(lines[1]!)).toEqual({ name: 'Bob', age: 25 });
    });

    it('writes NDJSON to file', async () => {
      const df = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));
      const outPath = path.join(tmpDir, 'logs-out.ndjson');
      await df.toNDJSON(outPath);

      const content = await fs.readFile(outPath, 'utf-8');
      const lines = content.trim().split('\n');
      expect(lines).toHaveLength(5);

      const first = JSON.parse(lines[0]!) as Record<string, unknown>;
      expect(first.level).toBe('INFO');
      expect(first.message).toBe('Server started');
    });

    it('preserves null values in NDJSON output', () => {
      const df = DataFrame.fromColumns({ a: [1, null, 3], b: ['x', 'y', null] });
      const ndjson = df.toNDJSON();
      const lines = ndjson.trim().split('\n');

      const second = JSON.parse(lines[1]!) as Record<string, unknown>;
      expect(second.a).toBeNull();

      const third = JSON.parse(lines[2]!) as Record<string, unknown>;
      expect(third.b).toBeNull();
    });
  });

  describe('JSON roundtrip', () => {
    it('roundtrip: fixture -> toJSON -> fromJSON preserves data', async () => {
      const original = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));
      const json = original.toJSON();
      const restored = await DataFrame.fromJSON(json, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
      expect(restored.col('name').toArray()).toEqual(original.col('name').toArray());
      expect(restored.col('age').toArray()).toEqual(original.col('age').toArray());
    });

    it('roundtrip: fixture -> toJSON(file) -> fromJSON(file)', async () => {
      const original = await DataFrame.fromJSON(path.join(fixturesDir, 'users.json'));
      const outPath = path.join(tmpDir, 'roundtrip.json');
      await original.toJSON(outPath);
      const restored = await DataFrame.fromJSON(outPath);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.col('name').toArray()).toEqual(original.col('name').toArray());
    });

    it('roundtrip: nested fixture -> toJSON -> fromJSON', async () => {
      const original = await DataFrame.fromJSON(path.join(fixturesDir, 'nested.json'), {
        path: 'results.items',
      });
      const json = original.toJSON();
      const restored = await DataFrame.fromJSON(json, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.col('product').toArray()).toEqual(original.col('product').toArray());
      expect(restored.col('price').toArray()).toEqual(original.col('price').toArray());
    });
  });

  describe('NDJSON roundtrip', () => {
    it('roundtrip: fixture -> toNDJSON -> fromNDJSON preserves data', async () => {
      const original = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));
      const ndjson = original.toNDJSON();
      const restored = await DataFrame.fromNDJSON(ndjson, { parse: 'string' });

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
      expect(restored.col('level').toArray()).toEqual(original.col('level').toArray());
      expect(restored.col('message').toArray()).toEqual(original.col('message').toArray());
    });

    it('roundtrip: fixture -> toNDJSON(file) -> fromNDJSON(file)', async () => {
      const original = await DataFrame.fromNDJSON(path.join(fixturesDir, 'logs.ndjson'));
      const outPath = path.join(tmpDir, 'roundtrip.ndjson');
      await original.toNDJSON(outPath);
      const restored = await DataFrame.fromNDJSON(outPath);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.col('level').toArray()).toEqual(original.col('level').toArray());
    });

    it('roundtrip: mixed types with nulls', async () => {
      const df = DataFrame.fromRows([
        { id: 1, name: 'Alice', score: 95.5 },
        { id: 2, name: null, score: null },
        { id: 3, name: 'Charlie', score: 78.0 },
      ]);
      const ndjson = df.toNDJSON();
      const restored = await DataFrame.fromNDJSON(ndjson, { parse: 'string' });

      expect(restored.shape).toEqual(df.shape);
      expect(restored.col('id').toArray()).toEqual(df.col('id').toArray());
      expect(restored.col('name').get(1)).toBeNull();
      expect(restored.col('score').get(1)).toBeNull();
    });
  });
});
