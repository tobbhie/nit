import { Version } from '@nit/domain';

export interface HealthReport {
  service: string;
  version: string;
  buildId: string;
  commitHash?: string;
  uptimeSeconds: number;
  queueDepth: number;
  lastSuccessfulCycleAt: string | null;
  errorCount: number;
  status: 'ok' | 'degraded';
}

export interface HealthSources {
  queueDepth: () => Promise<number>;
  lastSuccessfulCycleAt: () => Date | null;
  errorCount: () => number;
}

/**
 * Health reporting. Emits operational counters only — never configuration
 * values, credentials, hostnames, or provider identities.
 */
export class HealthReporter {
  private readonly startedAt: Date;
  private readonly clock: () => Date;

  constructor(
    private readonly service: string,
    private readonly version: Version,
    private readonly sources: HealthSources,
    clock?: () => Date
  ) {
    this.clock = clock ?? ((): Date => new Date());
    this.startedAt = this.clock();
  }

  async report(): Promise<HealthReport> {
    const queueDepth = await this.sources.queueDepth();
    const lastCycle = this.sources.lastSuccessfulCycleAt();
    const errorCount = this.sources.errorCount();

    const report: HealthReport = {
      service: this.service,
      version: this.version.schemaVersion,
      buildId: this.version.buildId,
      uptimeSeconds: Math.floor((this.clock().getTime() - this.startedAt.getTime()) / 1000),
      queueDepth,
      lastSuccessfulCycleAt: lastCycle ? lastCycle.toISOString() : null,
      errorCount,
      status: errorCount > 0 ? 'degraded' : 'ok',
    };

    if (this.version.commitHash !== undefined) {
      report.commitHash = this.version.commitHash;
    }

    return report;
  }
}
