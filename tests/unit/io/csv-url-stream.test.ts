import { describe, it, expect, vi, afterEach } from 'vitest';
import { Readable, Writable, PassThrough } from 'stream';
import { DataFrame } from '../../../src/dataframe';
import { IOError } from '../../../src/errors';

describe('DataFrame CSV from URLs and streams (US-080)', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('fromCSV with URL', () => {
    it('fetches and parses CSV from a URL', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25\n';
      vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response(csvContent, { status: 200, statusText: 'OK' }),
      );

      const df = await DataFrame.fromCSV('https://example.com/data.csv');

      expect(df.shape).toEqual([2, 2]);
      expect(df.columns).toEqual(['name', 'age']);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(0)).toBe(30);
      expect(fetch).toHaveBeenCalledWith('https://example.com/data.csv');
    });

    it('handles http:// URLs', async () => {
      const csvContent = 'x,y\n1,2\n';
      vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response(csvContent, { status: 200 }),
      );

      const df = await DataFrame.fromCSV('http://example.com/data.csv');
      expect(df.shape).toEqual([1, 2]);
    });

    it('throws IOError on HTTP error status', async () => {
      vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response('Not found', { status: 404, statusText: 'Not Found' }),
      );

      await expect(
        DataFrame.fromCSV('https://example.com/missing.csv'),
      ).rejects.toThrow(IOError);
      await expect(
        DataFrame.fromCSV('https://example.com/missing.csv'),
      ).rejects.toThrow(/HTTP 404/);
    });

    it('throws IOError on network failure', async () => {
      vi.spyOn(globalThis, 'fetch').mockRejectedValue(new TypeError('fetch failed'));

      await expect(
        DataFrame.fromCSV('https://example.com/data.csv'),
      ).rejects.toThrow(IOError);
      await expect(
        DataFrame.fromCSV('https://example.com/data.csv'),
      ).rejects.toThrow(/fetch failed/);
    });

    it('passes CSVReadOptions through for URL fetch', async () => {
      const csvContent = 'name;age\nAlice;30\n';
      vi.spyOn(globalThis, 'fetch').mockResolvedValue(
        new Response(csvContent, { status: 200 }),
      );

      const df = await DataFrame.fromCSV('https://example.com/data.csv', { delimiter: ';' });
      expect(df.shape).toEqual([1, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
    });
  });

  describe('fromCSV with ReadableStream', () => {
    it('reads CSV from a Node.js Readable stream', async () => {
      const csvContent = 'name,age\nAlice,30\nBob,25\n';
      const stream = Readable.from([Buffer.from(csvContent)]);

      const df = await DataFrame.fromCSV(stream);

      expect(df.shape).toEqual([2, 2]);
      expect(df.columns).toEqual(['name', 'age']);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(1)).toBe(25);
    });

    it('handles multiple chunks', async () => {
      const chunk1 = Buffer.from('name,age\nAli');
      const chunk2 = Buffer.from('ce,30\nBob,25\n');
      const stream = Readable.from([chunk1, chunk2]);

      const df = await DataFrame.fromCSV(stream);
      expect(df.shape).toEqual([2, 2]);
      expect(df.col('name').get(0)).toBe('Alice');
    });

    it('handles string chunks', async () => {
      const stream = new PassThrough();
      stream.end('name,age\nAlice,30\n');

      const df = await DataFrame.fromCSV(stream);
      expect(df.shape).toEqual([1, 2]);
    });

    it('throws IOError on stream error', async () => {
      const stream = new Readable({
        read() {
          this.destroy(new Error('stream broke'));
        },
      });

      const err = await DataFrame.fromCSV(stream).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(IOError);
      expect((err as Error).message).toMatch(/stream broke/);
    });

    it('passes CSVReadOptions through for stream', async () => {
      const csvContent = 'name;age\nAlice;30\n';
      const stream = Readable.from([Buffer.from(csvContent)]);

      const df = await DataFrame.fromCSV(stream, { delimiter: ';' });
      expect(df.shape).toEqual([1, 2]);
    });
  });

  describe('toCSV with WritableStream', () => {
    it('writes CSV to a Node.js Writable stream', async () => {
      const csv = 'name,age\nAlice,30\nBob,25\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      const chunks: Buffer[] = [];
      const writable = new Writable({
        write(chunk: Buffer, _encoding, callback) {
          chunks.push(chunk);
          callback();
        },
      });

      await df.toCSV(writable);
      const output = Buffer.concat(chunks).toString('utf-8');
      expect(output).toContain('name,age');
      expect(output).toContain('Alice,30');
      expect(output).toContain('Bob,25');
    });

    it('passes CSVWriteOptions through for stream', async () => {
      const csv = 'name,age\nAlice,30\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      const chunks: Buffer[] = [];
      const writable = new Writable({
        write(chunk: Buffer, _encoding, callback) {
          chunks.push(chunk);
          callback();
        },
      });

      await df.toCSV(writable, { delimiter: ';' });
      const output = Buffer.concat(chunks).toString('utf-8');
      expect(output).toContain('name;age');
      expect(output).toContain('Alice;30');
    });

    it('throws IOError on stream write error', async () => {
      const csv = 'name,age\nAlice,30\n';
      const df = await DataFrame.fromCSV(csv, { parse: 'string' });

      const makeFailingStream = () =>
        new Writable({
          write(_chunk, _encoding, callback) {
            callback(new Error('write failed'));
          },
        });

      const err = await df.toCSV(makeFailingStream()).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(IOError);
      expect((err as Error).message).toMatch(/write failed/);
    });
  });
});
