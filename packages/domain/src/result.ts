/**
 * Result type for operations that can fail
 */
export type Result<T, E = Error> = Success<T> | Failure<E>;

export interface Success<T> {
  readonly kind: 'success';
  readonly value: T;
}

export interface Failure<E> {
  readonly kind: 'failure';
  readonly error: E;
}

export function success<T>(value: T): Success<T> {
  return { kind: 'success', value };
}

export function failure<E>(error: E): Failure<E> {
  return { kind: 'failure', error };
}

export function isSuccess<T, E>(result: Result<T, E>): result is Success<T> {
  return result.kind === 'success';
}

export function isFailure<T, E>(result: Result<T, E>): result is Failure<E> {
  return result.kind === 'failure';
}

export function unwrap<T, E>(result: Result<T, E>): T {
  if (isSuccess(result)) {
    return result.value;
  }
  throw result.error;
}

export function unwrapOr<T, E>(result: Result<T, E>, defaultValue: T): T {
  if (isSuccess(result)) {
    return result.value;
  }
  return defaultValue;
}
