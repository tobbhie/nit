import { describe, it, expect } from 'vitest';
import { redact } from './redaction';

describe('redact', () => {
  it('masks credential-bearing keys', () => {
    const result = redact({
      password: 'hunter2',
      apiKey: 'abc123',
      api_key: 'abc123',
      secret: 's',
      accessToken: 't',
      credential: 'c',
      privateKey: 'k',
    }) as Record<string, unknown>;

    for (const value of Object.values(result)) {
      expect(value).toBe('[REDACTED]');
    }
  });

  it('masks authorization headers and cookies', () => {
    const result = redact({
      authorization: 'Bearer abc.def.ghi',
      Cookie: 'session=xyz',
      sessionId: 'abc',
    }) as Record<string, unknown>;

    expect(result['authorization']).toBe('[REDACTED]');
    expect(result['Cookie']).toBe('[REDACTED]');
    expect(result['sessionId']).toBe('[REDACTED]');
  });

  it('omits raw provider payloads rather than masking them', () => {
    const result = redact({
      rawPayload: { huge: 'blob' },
      raw_response: 'body',
      providerPayload: {},
    }) as Record<string, unknown>;

    expect(result['rawPayload']).toBe('[OMITTED_RAW_PAYLOAD]');
    expect(result['raw_response']).toBe('[OMITTED_RAW_PAYLOAD]');
    expect(result['providerPayload']).toBe('[OMITTED_RAW_PAYLOAD]');
  });

  it('masks inline bearer tokens inside free text', () => {
    const result = redact('called with Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig');
    expect(result).toBe('called with Bearer [REDACTED]');
  });

  it('redacts nested structures', () => {
    const result = redact({
      outer: { inner: { password: 'x', safe: 'keep' } },
    }) as { outer: { inner: Record<string, unknown> } };

    expect(result.outer.inner['password']).toBe('[REDACTED]');
    expect(result.outer.inner['safe']).toBe('keep');
  });

  it('redacts inside arrays', () => {
    const result = redact([{ token: 'a' }, { safe: 'b' }]) as Array<Record<string, unknown>>;
    expect(result[0]?.['token']).toBe('[REDACTED]');
    expect(result[1]?.['safe']).toBe('b');
  });

  it('preserves non-sensitive scalars', () => {
    const result = redact({ count: 5, ok: true, name: 'ingestion' }) as Record<string, unknown>;
    expect(result).toEqual({ count: 5, ok: true, name: 'ingestion' });
  });

  it('serialises dates and errors without leaking stacks', () => {
    const result = redact({
      at: new Date('2020-01-01T00:00:00.000Z'),
      err: new Error('boom'),
    }) as Record<string, unknown>;

    expect(result['at']).toBe('2020-01-01T00:00:00.000Z');
    expect(result['err']).toEqual({ name: 'Error', message: 'boom' });
  });

  it('truncates very long strings', () => {
    const result = redact('x'.repeat(5000)) as string;
    expect(result.endsWith('…[truncated]')).toBe(true);
    expect(result.length).toBeLessThan(5000);
  });

  it('stops at a bounded depth instead of recursing forever', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic['self'] = cyclic;
    expect(() => redact(cyclic)).not.toThrow();
  });
});
