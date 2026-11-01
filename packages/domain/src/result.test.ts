import { describe, it, expect } from 'vitest';
import {
  success,
  failure,
  isSuccess,
  isFailure,
  unwrap,
  unwrapOr,
  type Result,
} from './result';

describe('Result constructors', () => {
  it('tags success values on the kind discriminant', () => {
    const result = success(42);
    expect(result.kind).toBe('success');
    expect(result.value).toBe(42);
  });

  it('tags failures on the kind discriminant', () => {
    const error = new Error('synthetic failure');
    const result = failure(error);
    expect(result.kind).toBe('failure');
    expect(result.error).toBe(error);
  });
});

describe('Result guards', () => {
  it('narrows a success', () => {
    const result: Result<string> = success('ok');
    expect(isSuccess(result)).toBe(true);
    expect(isFailure(result)).toBe(false);
  });

  it('narrows a failure', () => {
    const result: Result<string> = failure(new Error('synthetic failure'));
    expect(isFailure(result)).toBe(true);
    expect(isSuccess(result)).toBe(false);
  });
});

describe('unwrap', () => {
  it('returns the value of a success', () => {
    expect(unwrap(success('value'))).toBe('value');
  });

  it('throws the contained error for a failure', () => {
    const error = new Error('synthetic failure');
    expect(() => unwrap(failure(error))).toThrow(error);
  });
});

describe('unwrapOr', () => {
  it('returns the value when present', () => {
    expect(unwrapOr(success(1), 99)).toBe(1);
  });

  it('returns the default on failure', () => {
    expect(unwrapOr(failure<Error>(new Error('synthetic failure')), 99)).toBe(99);
  });

  it('returns falsy contained values rather than the default', () => {
    expect(unwrapOr(success(0), 99)).toBe(0);
  });
});
