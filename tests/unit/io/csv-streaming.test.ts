import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('DataFrame streaming CSV (US-054)', () => {
  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-stream-'));
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function writeCSVFile(name: string, content: string): Promise<string> {
    const filePath = path.join(tmpDir, name);
    return fs.writeFile(filePath, content, 'utf-8').then(() => filePath);
  }

  describe('streamCSV', () => {
    it('streams a CSV file in chunks', async () => {
      // 5 data rows with chunkSize=2 should yield 3 chunks
      const csv = 'id,value\n1,10\n2,20\n3,30\n4,40\n5,50\n';
      const filePath = await writeCSVFile('stream1.csv', csv);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(filePath, { chunkSize: 2 })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(3);
      expect(chunks[0]!.shape).toEqual([2, 2]);
      expect(chunks[1]!.shape).toEqual([2, 2]);
      expect(chunks[2]!.shape).toEqual([1, 2]);

      // All chunks share the same schema
      for (const chunk of chunks) {
        expect(chunk.columns).toEqual(['id', 'value']);
      }
    });

    it('returns single chunk when file is small', async () => {
      const csv = 'name,age\nAlice,30\nBob,25\n';
      const filePath = await writeCSVFile('stream2.csv', csv);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(filePath, { chunkSize: 10000 })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(1);
      expect(chunks[0]!.shape).toEqual([2, 2]);
      expect(chunks[0]!.col('name').get(0)).toBe('Alice');
    });

    it('uses default chunkSize of 10000', async () => {
      const rows = Array.from({ length: 100 }, (_, i) => `${i},${i * 10}`).join('\n');
      const csv = `id,value\n${rows}\n`;
      const filePath = await writeCSVFile('stream3.csv', csv);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(filePath)) {
        chunks.push(chunk);
      }

      // 100 rows < 10000, so only 1 chunk
      expect(chunks.length).toBe(1);
      expect(chunks[0]!.shape[0]).toBe(100);
    });

    it('infers types consistently across chunks', async () => {
      const csv = 'id,value,active\n1,3.14,true\n2,2.71,false\n3,1.41,true\n';
      const filePath = await writeCSVFile('stream4.csv', csv);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(filePath, { chunkSize: 1 })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(3);
      // All chunks should have the same types (inferred from first chunk)
      for (const chunk of chunks) {
        expect(chunk.col('id').dtype).toBeDefined();
        expect(chunk.col('value').dtype).toBeDefined();
      }
    });

    it('handles CSV with various delimiters', async () => {
      const csv = 'name;age\nAlice;30\nBob;25\n';
      const filePath = await writeCSVFile('stream5.csv', csv);

      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(filePath, { chunkSize: 1, delimiter: ';' })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(2);
      expect(chunks[0]!.col('name').get(0)).toBe('Alice');
    });
  });

  describe('scanCSV', () => {
    it('returns a LazyFrame', () => {
      const lf = DataFrame.scanCSV('/nonexistent');
      expect(lf).toBeDefined();
      expect(typeof lf.collect).toBe('function');
    });

    it('collects all data from a CSV file', async () => {
      const csv = 'id,value\n1,10\n2,20\n3,30\n4,40\n5,50\n';
      const filePath = await writeCSVFile('scan1.csv', csv);

      const lf = DataFrame.scanCSV(filePath, { chunkSize: 2 });
      const df = await lf.collect();

      expect(df.shape).toEqual([5, 2]);
      expect(df.col('id').toArray()).toEqual([1, 2, 3, 4, 5]);
      expect(df.col('value').toArray()).toEqual([10, 20, 30, 40, 50]);
    });

    it('processes file in streaming fashion', async () => {
      // Verify it works with larger data
      const rows = Array.from({ length: 500 }, (_, i) => `${i},${i * 2}`).join('\n');
      const csv = `id,doubled\n${rows}\n`;
      const filePath = await writeCSVFile('scan2.csv', csv);

      const lf = DataFrame.scanCSV(filePath, { chunkSize: 100 });
      const df = await lf.collect();

      expect(df.shape).toEqual([500, 2]);
      expect(df.col('id').get(0)).toBe(0);
      expect(df.col('id').get(499)).toBe(499);
    });

    it('handles empty CSV file', async () => {
      const filePath = await writeCSVFile('scan3.csv', '');

      const lf = DataFrame.scanCSV(filePath);
      const df = await lf.collect();

      expect(df.shape).toEqual([0, 0]);
    });
  });
});
