import { IOError } from '../../errors';

export interface StreamNDJSONOptions {
  chunkSize?: number | undefined;
  nRows?: number | undefined;
  encoding?: string | undefined;
}

/**
 * Stream an NDJSON file yielding chunks of parsed rows.
 * Each line is parsed independently as a JSON object.
 * Uses Node.js readable streams to keep memory bounded.
 */
export async function* streamNDJSONFile(
  filePath: string,
  options: StreamNDJSONOptions = {},
): AsyncIterable<object[]> {
  const chunkSize = options.chunkSize ?? 10000;
  const nRows = options.nRows;
  const fs = await import('fs');
  const { createReadStream } = fs;

  let stream: ReturnType<typeof createReadStream>;
  try {
    stream = createReadStream(filePath, { encoding: (options.encoding ?? 'utf-8') as BufferEncoding });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new IOError(`Failed to open NDJSON file '${filePath}': ${message}`);
  }

  let buffer = '';
  let chunk: object[] = [];
  let totalEmitted = 0;

  for await (const rawChunk of stream as AsyncIterable<string>) {
    buffer += rawChunk;

    let lineStart = 0;
    for (let i = 0; i < buffer.length; i++) {
      const ch = buffer[i]!;
      if (ch === '\n' || ch === '\r') {
        const line = buffer.slice(lineStart, i);
        if (ch === '\r' && i + 1 < buffer.length && buffer[i + 1] === '\n') {
          i++; // skip \n after \r
        }
        lineStart = i + 1;

        if (line.trim().length > 0) {
          chunk.push(JSON.parse(line) as object);

          if (chunk.length >= chunkSize) {
            yield chunk;
            totalEmitted += chunk.length;
            chunk = [];

            if (nRows !== undefined && totalEmitted >= nRows) {
              stream.destroy();
              return;
            }
          }
        }
      }
    }

    buffer = buffer.slice(lineStart);
  }

  // Process remaining buffer
  if (buffer.trim().length > 0) {
    chunk.push(JSON.parse(buffer) as object);
  }

  // Yield final chunk
  if (chunk.length > 0) {
    if (nRows !== undefined) {
      const remaining = nRows - totalEmitted;
      if (remaining <= 0) return;
      if (remaining < chunk.length) {
        chunk = chunk.slice(0, remaining);
      }
    }
    yield chunk;
  }
}
