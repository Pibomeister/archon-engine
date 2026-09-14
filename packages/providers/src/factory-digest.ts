import { createHash } from 'node:crypto';

function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return (
      '{' +
      Object.keys(record)
        .filter(key => record[key] !== undefined)
        .sort()
        .map(key => JSON.stringify(key) + ':' + canonical(record[key]))
        .join(',') +
      '}'
    );
  }
  return JSON.stringify(value);
}
export function factoryRequestDigest(value: unknown): string {
  return createHash('sha256').update(canonical(value)).digest('hex');
}
