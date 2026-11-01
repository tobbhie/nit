import { createHash } from 'crypto';
import { ValidationError } from '@nit/domain';

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export type PolicyKind = 'entry' | 'exit' | 'protection' | 'risk';

export interface PolicyVersionDefinition {
  readonly kind: PolicyKind;
  readonly policyId: string;
  readonly version: number;
  readonly config: JsonValue;
  readonly compatibleEntryPolicyIds?: readonly string[];
}

export interface StrategyCombinationDefinition {
  readonly entry: PolicyVersionDefinition;
  readonly exit: PolicyVersionDefinition;
  readonly protection: PolicyVersionDefinition;
  readonly risk: PolicyVersionDefinition;
}

export interface StrategySnapshot extends StrategyCombinationDefinition {
  readonly displayId: string;
  readonly configHash: string;
}

function cloneJson(value: JsonValue): JsonValue {
  return JSON.parse(canonicalJson(value)) as JsonValue;
}

function freezeJson(value: JsonValue): JsonValue {
  if (value !== null && typeof value === 'object') {
    if (Array.isArray(value)) {
      for (const child of value as readonly JsonValue[]) {
        freezeJson(child);
      }
    } else {
      for (const child of Object.values(value as { readonly [key: string]: JsonValue })) {
        freezeJson(child);
      }
    }
    Object.freeze(value);
  }
  return value;
}

function immutablePolicy(definition: PolicyVersionDefinition): PolicyVersionDefinition {
  const compatibleEntryPolicyIds = definition.compatibleEntryPolicyIds;
  return Object.freeze({
    kind: definition.kind,
    policyId: definition.policyId,
    version: definition.version,
    config: freezeJson(cloneJson(definition.config)),
    ...(compatibleEntryPolicyIds === undefined
      ? {}
      : { compatibleEntryPolicyIds: Object.freeze([...compatibleEntryPolicyIds]) }),
  });
}

function canonicalJson(value: JsonValue): string {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new ValidationError('Strategy configuration cannot contain non-finite numbers');
    }
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }

  const record = value as { readonly [key: string]: JsonValue };
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key] as JsonValue)}`)
    .join(',')}}`;
}

function assertPolicy(definition: PolicyVersionDefinition, expected: PolicyKind): void {
  if (definition.kind !== expected) {
    throw new ValidationError(`Expected ${expected} policy, received ${definition.kind}`);
  }
  if (!/^[A-Z][A-Z0-9_-]*$/.test(definition.policyId)) {
    throw new ValidationError(`Invalid policy id: ${definition.policyId}`);
  }
  if (!Number.isInteger(definition.version) || definition.version < 1) {
    throw new ValidationError(`Invalid policy version: ${definition.version}`);
  }
}

export function createStrategySnapshot(
  definition: StrategyCombinationDefinition
): StrategySnapshot {
  assertPolicy(definition.entry, 'entry');
  assertPolicy(definition.exit, 'exit');
  assertPolicy(definition.protection, 'protection');
  assertPolicy(definition.risk, 'risk');

  const compatibleEntries = definition.exit.compatibleEntryPolicyIds;
  if (compatibleEntries !== undefined && !compatibleEntries.includes(definition.entry.policyId)) {
    throw new ValidationError(
      `${definition.exit.policyId}.v${definition.exit.version} is incompatible with ${definition.entry.policyId}`
    );
  }

  const immutableDefinition: StrategyCombinationDefinition = Object.freeze({
    entry: immutablePolicy(definition.entry),
    exit: immutablePolicy(definition.exit),
    protection: immutablePolicy(definition.protection),
    risk: immutablePolicy(definition.risk),
  });

  const displayId = [
    immutableDefinition.entry,
    immutableDefinition.exit,
    immutableDefinition.protection,
    immutableDefinition.risk,
  ]
    .map((policy) => `${policy.policyId}.v${policy.version}`)
    .join('/');
  const hashInput: JsonValue = {
    entry: immutableDefinition.entry as unknown as JsonValue,
    exit: immutableDefinition.exit as unknown as JsonValue,
    protection: immutableDefinition.protection as unknown as JsonValue,
    risk: immutableDefinition.risk as unknown as JsonValue,
  };
  const configHash = createHash('sha256').update(canonicalJson(hashInput)).digest('hex');

  return Object.freeze({ ...immutableDefinition, displayId, configHash });
}
