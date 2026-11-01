import { describe, it, expect } from 'vitest';
import {
  generateAggregateId,
  generateCorrelationId,
  generateEventId,
  generateJobId,
  createProviderId,
  createProviderReferenceId,
} from './identifiers';
import { createProvenance } from './provenance';
import { createVersion } from './version';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe('internal identifier generators', () => {
  const generators = {
    event: generateEventId,
    job: generateJobId,
    aggregate: generateAggregateId,
    correlation: generateCorrelationId,
  };

  for (const [name, generate] of Object.entries(generators)) {
    it(`produces a UUID for ${name} ids`, () => {
      expect(generate()).toMatch(UUID_PATTERN);
    });

    it(`produces distinct ${name} ids`, () => {
      const ids = new Set(Array.from({ length: 100 }, () => generate()));
      expect(ids.size).toBe(100);
    });
  }
});

describe('external identifier constructors', () => {
  it('preserves the provider-supplied string verbatim', () => {
    expect(createProviderId('toy-provider')).toBe('toy-provider');
    expect(createProviderReferenceId('toy-ref-1')).toBe('toy-ref-1');
  });
});

describe('createProvenance', () => {
  const providerId = createProviderId('toy-provider');
  const referenceId = createProviderReferenceId('toy-ref-1');
  const retrievedAt = new Date('2020-01-01T00:00:00.000Z');
  const ingestedAt = new Date('2020-01-01T00:00:05.000Z');

  it('records the required lineage timestamps', () => {
    const provenance = createProvenance(providerId, referenceId, retrievedAt, ingestedAt);
    expect(provenance.providerId).toBe(providerId);
    expect(provenance.providerReferenceId).toBe(referenceId);
    expect(provenance.retrievedAt).toBe(retrievedAt);
    expect(provenance.ingestedAt).toBe(ingestedAt);
  });

  it('omits sourceTimestamp entirely when the source did not supply one', () => {
    const provenance = createProvenance(providerId, referenceId, retrievedAt, ingestedAt);
    expect('sourceTimestamp' in provenance).toBe(false);
  });

  it('records sourceTimestamp when the source did supply one', () => {
    const sourceTimestamp = new Date('2019-12-31T23:59:00.000Z');
    const provenance = createProvenance(
      providerId,
      referenceId,
      retrievedAt,
      ingestedAt,
      sourceTimestamp
    );
    expect(provenance.sourceTimestamp).toBe(sourceTimestamp);
  });
});

describe('createVersion', () => {
  it('always carries the schema version and build id', () => {
    const version = createVersion('1.0.0', 'build-1');
    expect(version.schemaVersion).toBe('1.0.0');
    expect(version.buildId).toBe('build-1');
  });

  it('omits optional build metadata when the environment does not provide it', () => {
    const previousCommit = process.env['GIT_COMMIT'];
    const previousDeployed = process.env['DEPLOYED_AT'];
    delete process.env['GIT_COMMIT'];
    delete process.env['DEPLOYED_AT'];

    try {
      const version = createVersion('1.0.0', 'build-1');
      expect('commitHash' in version).toBe(false);
      expect('deployedAt' in version).toBe(false);
    } finally {
      if (previousCommit !== undefined) {
        process.env['GIT_COMMIT'] = previousCommit;
      }
      if (previousDeployed !== undefined) {
        process.env['DEPLOYED_AT'] = previousDeployed;
      }
    }
  });
});
