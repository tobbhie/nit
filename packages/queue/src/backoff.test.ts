import { describe, it, expect } from 'vitest';
import { computeBackoffMs, nextAvailableAt } from './backoff';

const POLICY = { baseMs: 1000, maxMs: 60000 };

describe('computeBackoffMs', () => {
  it('grows exponentially from the base delay', () => {
    expect(computeBackoffMs(1, POLICY)).toBe(1000);
    expect(computeBackoffMs(2, POLICY)).toBe(2000);
    expect(computeBackoffMs(3, POLICY)).toBe(4000);
    expect(computeBackoffMs(4, POLICY)).toBe(8000);
  });

  it('clamps at the configured maximum', () => {
    expect(computeBackoffMs(20, POLICY)).toBe(60000);
    expect(computeBackoffMs(100, POLICY)).toBe(60000);
  });

  it('honours a custom growth factor', () => {
    expect(computeBackoffMs(3, { baseMs: 100, maxMs: 10000, factor: 3 })).toBe(900);
  });

  it('treats attempt numbers below one as the first attempt', () => {
    expect(computeBackoffMs(0, POLICY)).toBe(1000);
    expect(computeBackoffMs(-5, POLICY)).toBe(1000);
  });

  it('is deterministic across repeated calls', () => {
    const runs = Array.from({ length: 50 }, () => computeBackoffMs(5, POLICY));
    expect(new Set(runs).size).toBe(1);
  });
});

describe('nextAvailableAt', () => {
  it('offsets the given instant by the computed backoff', () => {
    const from = new Date('2020-01-01T00:00:00.000Z');
    expect(nextAvailableAt(from, 3, POLICY).toISOString()).toBe('2020-01-01T00:00:04.000Z');
  });

  it('produces identical results for identical inputs', () => {
    const from = new Date('2020-01-01T00:00:00.000Z');
    expect(nextAvailableAt(from, 4, POLICY).getTime()).toBe(
      nextAvailableAt(from, 4, POLICY).getTime()
    );
  });
});
