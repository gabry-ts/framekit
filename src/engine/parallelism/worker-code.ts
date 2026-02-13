/**
 * Inline worker code for parallel aggregation.
 * This is compiled to a string and executed inside worker_threads.
 */

export const WORKER_CODE = `
'use strict';
const { parentPort } = require('worker_threads');

function getNumericValue(data, nullMask, index, dtype) {
  // Check null mask
  if (nullMask) {
    const byteIndex = index >> 3;
    const bitIndex = index & 7;
    if (!((nullMask[byteIndex] >> bitIndex) & 1)) {
      return null; // null value
    }
  }
  if (dtype === 'float64' || dtype === 'int32' || dtype === 'date') {
    return data[index];
  }
  return null;
}

function getStringValue(data, nullMask, index) {
  if (nullMask) {
    const byteIndex = index >> 3;
    const bitIndex = index & 7;
    if (!((nullMask[byteIndex] >> bitIndex) & 1)) {
      return null;
    }
  }
  return data[index];
}

function getValue(colData, index) {
  if (colData.dtype === 'utf8') {
    return getStringValue(colData.data, colData.nullMaskArr, index);
  }
  return getNumericValue(colData.dataArr, colData.nullMaskArr, index, colData.dtype);
}

function computeAgg(colData, indices, aggType) {
  switch (aggType) {
    case 'sum': {
      let total = 0;
      let hasValue = false;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null && typeof v === 'number') {
          total += v;
          hasValue = true;
        }
      }
      return hasValue ? total : null;
    }
    case 'mean': {
      let total = 0;
      let count = 0;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null && typeof v === 'number') {
          total += v;
          count++;
        }
      }
      return count > 0 ? total / count : null;
    }
    case 'count': {
      let count = 0;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null) count++;
      }
      return count;
    }
    case 'count_distinct': {
      const seen = new Set();
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null) seen.add(v);
      }
      return seen.size;
    }
    case 'min': {
      let result = null;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null && typeof v === 'number') {
          if (result === null || v < result) result = v;
        }
      }
      return result;
    }
    case 'max': {
      let result = null;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null && typeof v === 'number') {
          if (result === null || v > result) result = v;
        }
      }
      return result;
    }
    case 'std': {
      let sum = 0;
      let sumSq = 0;
      let count = 0;
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null && typeof v === 'number') {
          sum += v;
          sumSq += v * v;
          count++;
        }
      }
      if (count < 2) return null;
      const mean = sum / count;
      const variance = sumSq / count - mean * mean;
      return Math.sqrt(variance);
    }
    case 'first': {
      for (let i = 0; i < indices.length; i++) {
        const v = getValue(colData, indices[i]);
        if (v !== null) return v;
      }
      return null;
    }
    case 'last': {
      for (let i = indices.length - 1; i >= 0; i--) {
        const v = getValue(colData, indices[i]);
        if (v !== null) return v;
      }
      return null;
    }
    default:
      return null;
  }
}

parentPort.on('message', (msg) => {
  const { columns, groups, aggSpecs, keyColumns } = msg;

  // Pre-process column data for fast access
  const processedColumns = {};
  for (const [name, colData] of Object.entries(columns)) {
    const processed = { ...colData };
    if (colData.dtype === 'float64' || colData.dtype === 'date') {
      processed.dataArr = new Float64Array(colData.data);
    } else if (colData.dtype === 'int32') {
      processed.dataArr = new Int32Array(colData.data);
    } else if (colData.dtype === 'boolean') {
      processed.dataArr = new Uint8Array(colData.data);
    }
    // utf8: data is already string[]
    if (colData.nullMask) {
      processed.nullMaskArr = new Uint8Array(colData.nullMask);
    } else {
      processed.nullMaskArr = null;
    }
    processedColumns[name] = processed;
  }

  const results = [];
  const aggEntries = Object.entries(aggSpecs);

  for (const [groupIndex, indices] of groups) {
    // Extract key values from first row
    const keyValues = [];
    const firstIndex = indices[0];
    for (const keyCol of keyColumns) {
      const colData = processedColumns[keyCol];
      if (colData) {
        keyValues.push(getValue(colData, firstIndex));
      } else {
        keyValues.push(null);
      }
    }

    // Compute aggregations
    const aggValues = {};
    for (const [outputName, spec] of aggEntries) {
      const colData = processedColumns[spec.columnName];
      if (colData) {
        aggValues[outputName] = computeAgg(colData, indices, spec.aggType);
      } else {
        aggValues[outputName] = null;
      }
    }

    results.push({ groupIndex, keyValues, aggValues });
  }

  parentPort.postMessage({ results });
});
`;
