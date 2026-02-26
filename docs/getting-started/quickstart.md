# Quickstart

## Install

```bash
npm install framekit-js
```

## Create a DataFrame

```ts
import { DataFrame, col } from 'framekit-js';

const df = DataFrame.fromRows([
  { city: 'Rome', temp: 26 },
  { city: 'Milan', temp: 31 },
  { city: 'Turin', temp: 28 },
]);

const out = df.filter(col<number>('temp').gt(27)).sortBy('temp', 'desc');

console.log(out.toArray());
```
