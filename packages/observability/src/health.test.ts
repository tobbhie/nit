import { describe, it, expect } from 'vitest';
import { HealthReporter } from './health';

const VERSION = { schemaVersion: '1.2.3', buildId: 'build-9' };

describe('HealthReporter', () => {
  it('reports the required operational fields', async () => {
    const reporter = new HealthReporter('svc', VERSION, {
      queueDepth: (): Promise<number> => Promise.resolve(7),
      lastSuccessfulCycleAt: (): Date | null => new Date('2020-01-01T00:00:00.000Z'),
      errorCount: (): number => 0,
    });

    const report = await reporter.report();

    expect(report.service).toBe('svc');
    expect(report.version).toBe('1.2.3');
    expect(report.buildId).toBe('build-9');
    expect(report.queueDepth).toBe(7);
    expect(report.lastSuccessfulCycleAt).toBe('2020-01-01T00:00:00.000Z');
    expect(report.errorCount).toBe(0);
    expect(typeof report.uptimeSeconds).toBe('number');
  });

  it('computes uptime from the injected clock', async () => {
    let current = new Date('2020-01-01T00:00:00.000Z');
    const reporter = new HealthReporter(
      'svc',
      VERSION,
      {
        queueDepth: (): Promise<number> => Promise.resolve(0),
        lastSuccessfulCycleAt: (): Date | null => null,
        errorCount: (): number => 0,
      },
      (): Date => current
    );

    current = new Date('2020-01-01T00:01:30.000Z');
    expect((await reporter.report()).uptimeSeconds).toBe(90);
  });

  it('marks the service degraded when errors are present', async () => {
    const reporter = new HealthReporter('svc', VERSION, {
      queueDepth: (): Promise<number> => Promise.resolve(0),
      lastSuccessfulCycleAt: (): Date | null => null,
      errorCount: (): number => 3,
    });

    expect((await reporter.report()).status).toBe('degraded');
  });

  it('reports a null last cycle before any work completes', async () => {
    const reporter = new HealthReporter('svc', VERSION, {
      queueDepth: (): Promise<number> => Promise.resolve(0),
      lastSuccessfulCycleAt: (): Date | null => null,
      errorCount: (): number => 0,
    });

    expect((await reporter.report()).lastSuccessfulCycleAt).toBeNull();
  });

  it('never exposes configuration values or credentials', async () => {
    const reporter = new HealthReporter('svc', VERSION, {
      queueDepth: (): Promise<number> => Promise.resolve(1),
      lastSuccessfulCycleAt: (): Date | null => null,
      errorCount: (): number => 0,
    });

    const keys = Object.keys(await reporter.report()).sort();
    expect(keys).toEqual([
      'buildId',
      'errorCount',
      'lastSuccessfulCycleAt',
      'queueDepth',
      'service',
      'status',
      'uptimeSeconds',
      'version',
    ]);
  });
});
