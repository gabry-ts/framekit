import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('JSON reader/writer', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-json-'));
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe('fromJSON', () => {
    it('reads a JSON file containing array of objects', async () => {
      const data = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ];
      const filePath = path.join(tmpDir, 'test.json');
      await fs.writeFile(filePath, JSON.stringify(data), 'utf-8');

      const df = await DataFrame.fromJSON(filePath);
      expect(df.shape).toEqual([2, 2]);
      expect(df.columns).toEqual(['name', 'age']);
      expect(df.col('name').toArray()).toEqual(['Alice', 'Bob']);
      expect(df.col('age').toArray()).toEqual([30, 25]);
    });

    it('reads nested JSON array at given path', async () => {
      const data = {
        results: {
          items: [
            { id: 1, value: 'a' },
            { id: 2, value: 'b' },
          ],
        },
      };
      const filePath = path.join(tmpDir, 'nested.json');
      await fs.writeFile(filePath, JSON.stringify(data), 'utf-8');

      const df = await DataFrame.fromJSON(filePath, { path: 'results.items' });
      expect(df.shape).toEqual([2, 2]);
      expect(df.col('id').toArray()).toEqual([1, 2]);
      expect(df.col('value').toArray()).toEqual(['a', 'b']);
    });

    it('parses from string with parse option', async () => {
      const json = JSON.stringify([{ x: 1 }, { x: 2 }]);
      const df = await DataFrame.fromJSON(json, { parse: 'string' });
      expect(df.shape).toEqual([2, 1]);
      expect(df.col('x').toArray()).toEqual([1, 2]);
    });

    it('throws on invalid path', async () => {
      const data = { foo: { bar: [1, 2] } };
      const json = JSON.stringify(data);
      await expect(
        DataFrame.fromJSON(json, { parse: 'string', path: 'foo.baz' }),
      ).rejects.toThrow();
    });

    it('throws if JSON content is not an array', async () => {
      const json = JSON.stringify({ key: 'value' });
      await expect(
        DataFrame.fromJSON(json, { parse: 'string' }),
      ).rejects.toThrow('must be an array');
    });

    it('throws IOError for non-existent file', async () => {
      await expect(
        DataFrame.fromJSON('/tmp/nonexistent-file-xyz.json'),
      ).rejects.toThrow('Failed to read JSON');
    });

    it('handles empty array', async () => {
      const json = JSON.stringify([]);
      const df = await DataFrame.fromJSON(json, { parse: 'string' });
      expect(df.shape).toEqual([0, 0]);
    });

    it('handles null values in objects', async () => {
      const json = JSON.stringify([
        { a: 1, b: null },
        { a: null, b: 'hello' },
      ]);
      const df = await DataFrame.fromJSON(json, { parse: 'string' });
      expect(df.col('a').get(1)).toBeNull();
      expect(df.col('b').get(0)).toBeNull();
    });
  });

  describe('toJSON', () => {
    it('returns JSON string', () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
      const json = df.toJSON();
      const parsed = JSON.parse(json) as Record<string, unknown>[];
      expect(parsed).toEqual([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
    });

    it('returns pretty JSON with { pretty: true }', () => {
      const df = DataFrame.fromRows([{ x: 1 }]);
      const json = df.toJSON({ pretty: true });
      expect(json).toContain('\n');
      expect(json).toContain('  ');
      const parsed = JSON.parse(json) as Record<string, unknown>[];
      expect(parsed).toEqual([{ x: 1 }]);
    });

    it('writes JSON to file', async () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', age: 30 },
      ]);
      const filePath = path.join(tmpDir, 'output.json');
      await df.toJSON(filePath);

      const content = await fs.readFile(filePath, 'utf-8');
      const parsed = JSON.parse(content) as Record<string, unknown>[];
      expect(parsed).toEqual([{ name: 'Alice', age: 30 }]);
    });

    it('writes pretty JSON to file', async () => {
      const df = DataFrame.fromRows([{ x: 1 }]);
      const filePath = path.join(tmpDir, 'pretty.json');
      await df.toJSON(filePath, { pretty: true });

      const content = await fs.readFile(filePath, 'utf-8');
      expect(content).toContain('\n');
      expect(content).toContain('  ');
    });

    it('handles null values in output', () => {
      const df = DataFrame.fromColumns({ a: [1, null, 3] });
      const json = df.toJSON();
      const parsed = JSON.parse(json) as Record<string, unknown>[];
      expect((parsed[1] as Record<string, unknown>).a).toBeNull();
    });

    it('serializes Date values as ISO strings', () => {
      const date = new Date('2024-01-15T00:00:00.000Z');
      const df = DataFrame.fromColumns({ d: [date] });
      const json = df.toJSON();
      const parsed = JSON.parse(json) as Record<string, unknown>[];
      expect((parsed[0] as Record<string, unknown>).d).toBe('2024-01-15T00:00:00.000Z');
    });

    it('handles empty DataFrame', () => {
      const df = DataFrame.empty();
      const json = df.toJSON();
      expect(JSON.parse(json)).toEqual([]);
    });
  });

  describe('fromNDJSON', () => {
    it('reads NDJSON file', async () => {
      const ndjson = '{"name":"Alice","age":30}\n{"name":"Bob","age":25}\n';
      const filePath = path.join(tmpDir, 'test.ndjson');
      await fs.writeFile(filePath, ndjson, 'utf-8');

      const df = await DataFrame.fromNDJSON(filePath);
      expect(df.shape).toEqual([2, 2]);
      expect(df.col('name').toArray()).toEqual(['Alice', 'Bob']);
      expect(df.col('age').toArray()).toEqual([30, 25]);
    });

    it('parses from string with parse option', async () => {
      const ndjson = '{"x":1}\n{"x":2}\n';
      const df = await DataFrame.fromNDJSON(ndjson, { parse: 'string' });
      expect(df.shape).toEqual([2, 1]);
      expect(df.col('x').toArray()).toEqual([1, 2]);
    });

    it('handles blank lines in NDJSON', async () => {
      const ndjson = '{"a":1}\n\n{"a":2}\n\n';
      const df = await DataFrame.fromNDJSON(ndjson, { parse: 'string' });
      expect(df.shape).toEqual([2, 1]);
      expect(df.col('a').toArray()).toEqual([1, 2]);
    });

    it('throws IOError for non-existent file', async () => {
      await expect(
        DataFrame.fromNDJSON('/tmp/nonexistent-ndjson-xyz.ndjson'),
      ).rejects.toThrow('Failed to read NDJSON');
    });

    it('handles empty input', async () => {
      const df = await DataFrame.fromNDJSON('', { parse: 'string' });
      expect(df.shape).toEqual([0, 0]);
    });
  });

  describe('toNDJSON', () => {
    it('returns NDJSON string', () => {
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
      const df = DataFrame.fromRows([
        { x: 1 },
        { x: 2 },
      ]);
      const filePath = path.join(tmpDir, 'output.ndjson');
      await df.toNDJSON(filePath);

      const content = await fs.readFile(filePath, 'utf-8');
      const lines = content.trim().split('\n');
      expect(lines).toHaveLength(2);
      expect(JSON.parse(lines[0]!)).toEqual({ x: 1 });
      expect(JSON.parse(lines[1]!)).toEqual({ x: 2 });
    });

    it('handles null values', () => {
      const df = DataFrame.fromColumns({ a: [1, null, 3] });
      const ndjson = df.toNDJSON();
      const lines = ndjson.trim().split('\n');
      expect(JSON.parse(lines[1]!)).toEqual({ a: null });
    });

    it('serializes Date values as ISO strings', () => {
      const date = new Date('2024-01-15T00:00:00.000Z');
      const df = DataFrame.fromColumns({ d: [date] });
      const ndjson = df.toNDJSON();
      const parsed = JSON.parse(ndjson.trim()) as Record<string, unknown>;
      expect(parsed.d).toBe('2024-01-15T00:00:00.000Z');
    });

    it('roundtrip: toNDJSON -> fromNDJSON', async () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', score: 95.5 },
        { name: 'Bob', score: 87.0 },
      ]);
      const ndjson = df.toNDJSON();
      const df2 = await DataFrame.fromNDJSON(ndjson, { parse: 'string' });
      expect(df2.shape).toEqual(df.shape);
      expect(df2.col('name').toArray()).toEqual(df.col('name').toArray());
      expect(df2.col('score').toArray()).toEqual(df.col('score').toArray());
    });

    it('roundtrip: toJSON -> fromJSON', async () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', score: 95.5 },
        { name: 'Bob', score: 87.0 },
      ]);
      const json = df.toJSON();
      const df2 = await DataFrame.fromJSON(json, { parse: 'string' });
      expect(df2.shape).toEqual(df.shape);
      expect(df2.col('name').toArray()).toEqual(df.col('name').toArray());
      expect(df2.col('score').toArray()).toEqual(df.col('score').toArray());
    });
  });
});
