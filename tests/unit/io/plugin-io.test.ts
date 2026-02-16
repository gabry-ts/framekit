import { describe, it, expect } from 'vitest';
import { DataFrame, IOError } from '../../../src';
import type { ReaderFn, WriterFn } from '../../../src';

describe('Plugin I/O system', () => {
  describe('registerReader', () => {
    it('registers a reader for a custom extension', async () => {
      const reader: ReaderFn = (source) => {
        const text = typeof source === 'string' ? source : source.toString('utf-8');
        const rows = text.split('\n').filter(Boolean).map((line) => {
          const [name, value] = line.split('=');
          return { name, value };
        });
        return Promise.resolve(DataFrame.fromRows(rows));
      };

      DataFrame.registerReader('custom', reader);

      const fs = await import('fs/promises');
      const os = await import('os');
      const path = await import('path');
      const tmpFile = path.join(os.tmpdir(), `framekit-test-${Date.now()}.custom`);
      await fs.writeFile(tmpFile, 'alice=100\nbob=200\n', 'utf-8');

      try {
        const df = await DataFrame.fromFile(tmpFile);
        expect(df.length).toBe(2);
        expect(df.columns).toEqual(['name', 'value']);
        expect(df.col('name').toArray()).toEqual(['alice', 'bob']);
        expect(df.col('value').toArray()).toEqual(['100', '200']);
      } finally {
        await fs.unlink(tmpFile);
      }
    });

    it('normalizes extension with leading dot', () => {
      const reader: ReaderFn = () => Promise.resolve(DataFrame.empty());
      DataFrame.registerReader('.myext', reader);
    });
  });

  describe('registerWriter', () => {
    it('registers a writer for a custom extension', async () => {
      const fs = await import('fs/promises');
      const os = await import('os');
      const path = await import('path');

      const writer: WriterFn = async (df, filePath) => {
        const rows: string[] = [];
        for (const row of df) {
          const entries = Object.entries(row).map(([k, v]) => `${k}=${String(v)}`);
          rows.push(entries.join(','));
        }
        await fs.writeFile(filePath, rows.join('\n'), 'utf-8');
      };

      DataFrame.registerWriter('customout', writer);

      const df = DataFrame.fromRows([{ x: 1, y: 2 }, { x: 3, y: 4 }]);
      const tmpFile = path.join(os.tmpdir(), `framekit-test-${Date.now()}.customout`);

      try {
        await df.toFile(tmpFile);
        const content = await fs.readFile(tmpFile, 'utf-8');
        expect(content).toBe('x=1,y=2\nx=3,y=4');
      } finally {
        await fs.unlink(tmpFile).catch(() => {});
      }
    });
  });

  describe('fromFile', () => {
    it('throws IOError for unregistered extension', async () => {
      await expect(DataFrame.fromFile('data.unknownext123')).rejects.toThrow(IOError);
      await expect(DataFrame.fromFile('data.unknownext123')).rejects.toThrow(
        /No reader registered for extension/,
      );
    });

    it('throws IOError when file does not exist', async () => {
      DataFrame.registerReader('missingtest', () => Promise.resolve(DataFrame.empty()));
      await expect(DataFrame.fromFile('/tmp/nonexistent.missingtest')).rejects.toThrow(IOError);
    });
  });

  describe('toFile', () => {
    it('throws IOError for unregistered extension', async () => {
      const df = DataFrame.fromRows([{ a: 1 }]);
      await expect(df.toFile('output.unknownext456')).rejects.toThrow(IOError);
      await expect(df.toFile('output.unknownext456')).rejects.toThrow(
        /No writer registered for extension/,
      );
    });
  });

  describe('reader interface: (source: string | Buffer, options?) => Promise<DataFrame>', () => {
    it('reader receives Buffer source from fromFile', async () => {
      const fs = await import('fs/promises');
      const os = await import('os');
      const path = await import('path');

      let receivedSource: unknown;
      const reader: ReaderFn = (source) => {
        receivedSource = source;
        return Promise.resolve(DataFrame.empty());
      };

      DataFrame.registerReader('buftest', reader);
      const tmpFile = path.join(os.tmpdir(), `framekit-buf-${Date.now()}.buftest`);
      await fs.writeFile(tmpFile, 'hello', 'utf-8');

      try {
        await DataFrame.fromFile(tmpFile);
        expect(Buffer.isBuffer(receivedSource)).toBe(true);
        expect((receivedSource as Buffer).toString('utf-8')).toBe('hello');
      } finally {
        await fs.unlink(tmpFile);
      }
    });
  });

  describe('writer interface: (df: DataFrame, path: string, options?) => Promise<void>', () => {
    it('writer receives DataFrame and path', async () => {
      const os = await import('os');
      const path = await import('path');

      let receivedDf: unknown;
      let receivedPath: unknown;
      const writer: WriterFn = (df, filePath) => {
        receivedDf = df;
        receivedPath = filePath;
        return Promise.resolve();
      };

      DataFrame.registerWriter('wrtest', writer);
      const df = DataFrame.fromRows([{ a: 1 }]);
      const tmpFile = path.join(os.tmpdir(), `framekit-wr-${Date.now()}.wrtest`);

      await df.toFile(tmpFile);
      expect(receivedDf).toBe(df);
      expect(receivedPath).toBe(tmpFile);
    });
  });
});
