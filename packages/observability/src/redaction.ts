/**
 * Central redaction. Every log record passes through this before emission.
 */

const REDACTED = '[REDACTED]';

/** Keys whose values must never reach a log sink. */
const SENSITIVE_KEY_PATTERNS: readonly RegExp[] = [
  /password/i,
  /secret/i,
  /token/i,
  /api[-_]?key/i,
  /credential/i,
  /authorization/i,
  /^auth$/i,
  /cookie/i,
  /session[-_]?id/i,
  /private[-_]?key/i,
  /access[-_]?key/i,
  /signature/i,
  /bearer/i,
];

/** Keys carrying raw provider payloads, which are dropped rather than masked. */
const RAW_PAYLOAD_KEY_PATTERNS: readonly RegExp[] = [
  /^raw$/i,
  /raw[-_]?payload/i,
  /raw[-_]?response/i,
  /raw[-_]?body/i,
  /provider[-_]?payload/i,
];

const MAX_DEPTH = 6;
const MAX_STRING_LENGTH = 2048;

function isSensitiveKey(key: string): boolean {
  return SENSITIVE_KEY_PATTERNS.some((pattern) => pattern.test(key));
}

function isRawPayloadKey(key: string): boolean {
  return RAW_PAYLOAD_KEY_PATTERNS.some((pattern) => pattern.test(key));
}

function redactString(value: string): string {
  // Mask bearer tokens and long opaque blobs that appear inline.
  const withoutBearer = value.replace(/\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi, `Bearer ${REDACTED}`);
  if (withoutBearer.length > MAX_STRING_LENGTH) {
    return `${withoutBearer.slice(0, MAX_STRING_LENGTH)}…[truncated]`;
  }
  return withoutBearer;
}

export function redact(value: unknown, depth = 0): unknown {
  if (depth > MAX_DEPTH) {
    return '[MAX_DEPTH]';
  }

  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'string') {
    return redactString(value);
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof Error) {
    return { name: value.name, message: redactString(value.message) };
  }

  if (Array.isArray(value)) {
    return value.map((item) => redact(item, depth + 1));
  }

  if (typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
      if (isSensitiveKey(key)) {
        out[key] = REDACTED;
        continue;
      }
      if (isRawPayloadKey(key)) {
        out[key] = '[OMITTED_RAW_PAYLOAD]';
        continue;
      }
      out[key] = redact(nested, depth + 1);
    }
    return out;
  }

  return '[UNSERIALIZABLE]';
}
