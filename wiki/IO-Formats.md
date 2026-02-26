# IO Formats

FrameKit supports practical read/write workflows across common analytics formats.

| Format     | Read | Write |
| ---------- | ---- | ----- |
| CSV        | Yes  | Yes   |
| JSON       | Yes  | Yes   |
| NDJSON     | Yes  | Yes   |
| Arrow IPC  | Yes  | Yes   |
| Excel      | Yes  | Yes   |
| Parquet    | Yes  | Yes   |
| SQL output | No   | Yes   |

## Typical Flow

```ts
import { DataFrame } from 'framekit';

const df = await DataFrame.fromCSV('./input.csv');
await df.toParquet('./output.parquet');
```
