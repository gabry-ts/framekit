import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('DataFrame streaming NDJSON and sink (US-055)', () => {
  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-ndjson-stream-'));
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function writeFile(name: string, content: string): Promise<string> {
    const filePath = path.join(tmpDir, name);
    return fs.writeFile(filePath, content, 'utf-8').then(() => filePath);
  }

  describe('streamNDJSON', () => {
    it('streams an NDJSON file in chunks', async () => {
      const lines = [
        '{"id":1,"value":10}',
        '{"id":2,"value":20}',
        '{"id":3,"value":30}',
        '{"id":4,"value":40}',
        '{"id":5,"value":50}',
      ].join('\n') + '\n';
      const filePath = await writeFile('stream1.ndjson', lines);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(filePath, { chunkSize: 2 })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(3);
      expect(chunks[0]!.shape).toEqual([2, 2]);
      expect(chunks[1]!.shape).toEqual([2, 2]);
      expect(chunks[2]!.shape).toEqual([1, 2]);
    });

    it('parses each line independently', async () => {
      const lines = [
        '{"name":"Alice","age":30}',
        '{"name":"Bob","age":25}',
      ].join('\n') + '\n';
      const filePath = await writeFile('stream2.ndjson', lines);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(filePath)) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(1);
      expect(chunks[0]!.col('name').get(0)).toBe('Alice');
      expect(chunks[0]!.col('age').get(1)).toBe(25);
    });

    it('returns single chunk when file is small', async () => {
      const lines = '{"x":1}\n{"x":2}\n';
      const filePath = await writeFile('stream3.ndjson', lines);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(filePath, { chunkSize: 10000 })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(1);
      expect(chunks[0]!.shape).toEqual([2, 1]);
    });

    it('handles empty lines between records', async () => {
      const content = '{"a":1}\n\n{"a":2}\n\n';
      const filePath = await writeFile('stream4.ndjson', content);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(filePath)) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(1);
      expect(chunks[0]!.shape[0]).toBe(2);
    });
  });

  describe('scanNDJSON', () => {
    it('returns a LazyFrame', () => {
      const lf = DataFrame.scanNDJSON('/nonexistent');
      expect(lf).toBeDefined();
      expect(typeof lf.collect).toBe('function');
    });

    it('collects all data from an NDJSON file', async () => {
      const lines = [
        '{"id":1,"value":10}',
        '{"id":2,"value":20}',
        '{"id":3,"value":30}',
      ].join('\n') + '\n';
      const filePath = await writeFile('scan1.ndjson', lines);

      const lf = DataFrame.scanNDJSON(filePath, { chunkSize: 1 });
      const df = await lf.collect();

      expect(df.shape).toEqual([3, 2]);
      expect(df.col('id').toArray()).toEqual([1, 2, 3]);
      expect(df.col('value').toArray()).toEqual([10, 20, 30]);
    });
  });

  describe('LazyFrame.sink', () => {
    it('sinks to CSV file', async () => {
      const df = DataFrame.fromRows([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
      const lazy = df.lazy();
      const outPath = path.join(tmpDir, 'output.csv');

      await lazy.sink(outPath);

      const content = await fs.readFile(outPath, 'utf-8');
      expect(content).toContain('id');
      expect(content).toContain('Alice');
      expect(content).toContain('Bob');
    });

    it('sinks to NDJSON file', async () => {
      const df = DataFrame.fromRows([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
      const lazy = df.lazy();
      const outPath = path.join(tmpDir, 'output.ndjson');

      await lazy.sink(outPath);

      const content = await fs.readFile(outPath, 'utf-8');
      const lines = content.trim().split('\n');
      expect(lines.length).toBe(2);
      expect(JSON.parse(lines[0]!)).toEqual({ id: 1, name: 'Alice' });
      expect(JSON.parse(lines[1]!)).toEqual({ id: 2, name: 'Bob' });
    });

    it('sinks query results without materializing full result in caller', async () => {
      const lines = Array.from({ length: 100 }, (_, i) =>
        JSON.stringify({ id: i, value: i * 2 }),
      ).join('\n') + '\n';
      const filePath = await writeFile('sink-source.ndjson', lines);

      const lf = DataFrame.scanNDJSON(filePath, { chunkSize: 20 });
      const outPath = path.join(tmpDir, 'sink-output.csv');

      await lf.sink(outPath);

      const result = await DataFrame.fromCSV(outPath);
      expect(result.shape[0]).toBe(100);
    });
  });
});
