import { describe, expect, it } from 'vitest';
import { uuidv7, uuidv7Timestamp, generateMediaId, generateObservationId } from './identifiers';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** Hex nibble at the position RFC 9562 reserves for the version. */
function versionNibble(id: string): string {
  return id[14] as string;
}

/** Top two bits of the variant octet, as a number in 0..3. */
function variantBits(id: string): number {
  return parseInt(id.slice(19, 20), 16) >> 2;
}

describe('uuidv7', () => {
  it('is shaped like a UUID', () => {
    expect(uuidv7()).toMatch(UUID_PATTERN);
  });

  it('sets version 7 and the RFC 9562 variant', () => {
    for (let i = 0; i < 64; i += 1) {
      const id = uuidv7();
      expect(versionNibble(id)).toBe('7');
      // 0b10 in the top two bits of octet 8.
      expect(variantBits(id)).toBe(0b10);
    }
  });

  it('encodes the current time in the leading 48 bits', () => {
    const before = Date.now();
    const id = uuidv7();
    const after = Date.now();

    const encoded = uuidv7Timestamp(id);
    expect(encoded).toBeGreaterThanOrEqual(before);
    // The counter may borrow one millisecond from the future when it overflows,
    // so allow a single millisecond of lead over the observed clock.
    expect(encoded).toBeLessThanOrEqual(after + 1);
  });

  it('is unique across a tight burst', () => {
    const ids = new Set(Array.from({ length: 10_000 }, () => uuidv7()));
    expect(ids.size).toBe(10_000);
  });

  it('sorts lexicographically in generation order', () => {
    // The point of v7 over v4. A burst inside one millisecond is the hard case:
    // the timestamp prefix is identical, so only the counter can order them.
    const ids = Array.from({ length: 5_000 }, () => uuidv7());
    const sorted = [...ids].sort();
    expect(sorted).toEqual(ids);
  });

  it('never emits a timestamp below one already issued', () => {
    const timestamps = Array.from({ length: 5_000 }, () => uuidv7Timestamp(uuidv7()));
    for (let i = 1; i < timestamps.length; i += 1) {
      expect(timestamps[i]).toBeGreaterThanOrEqual(timestamps[i - 1] as number);
    }
  });
});

describe('aggregate identifier generators', () => {
  it('issues UUIDv7 for observations and media', () => {
    // These key large append-only tables, so index locality is the reason they
    // are v7 rather than v4 (TIS ch.2 §2, §6).
    expect(versionNibble(generateObservationId())).toBe('7');
    expect(versionNibble(generateMediaId())).toBe('7');
  });

  it('keeps observation and media identifiers distinct from each other', () => {
    const ids = new Set([
      ...Array.from({ length: 500 }, () => generateObservationId() as string),
      ...Array.from({ length: 500 }, () => generateMediaId() as string),
    ]);
    expect(ids.size).toBe(1_000);
  });
});
