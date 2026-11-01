import { describe, it, expect } from 'vitest';
import { JobStatus } from '@nit/domain';
import { canTransition, allowedPredecessors, PermanentJobFailure } from './job';

describe('job lifecycle transitions', () => {
  it('allows the documented happy path', () => {
    expect(canTransition(JobStatus.PENDING, JobStatus.LEASED)).toBe(true);
    expect(canTransition(JobStatus.LEASED, JobStatus.RUNNING)).toBe(true);
    expect(canTransition(JobStatus.RUNNING, JobStatus.COMPLETED)).toBe(true);
  });

  it('allows retry back to pending from either in-flight status', () => {
    // A retry is a pending job with a future available_at — there is no
    // separate failed status to pass through.
    expect(canTransition(JobStatus.RUNNING, JobStatus.PENDING)).toBe(true);
    expect(canTransition(JobStatus.LEASED, JobStatus.PENDING)).toBe(true);
  });

  it('allows dead-lettering from running', () => {
    expect(canTransition(JobStatus.RUNNING, JobStatus.DEAD_LETTER)).toBe(true);
  });

  it('allows dead-lettering straight from leased', () => {
    // A worker can recognise an unretryable payload before marking the job
    // running. If this edge were missing, fail({permanent: true}) would be
    // ignored and the job would retry until its attempts ran out.
    expect(canTransition(JobStatus.LEASED, JobStatus.DEAD_LETTER)).toBe(true);
  });

  it('treats completed and dead-letter as terminal', () => {
    for (const target of Object.values(JobStatus)) {
      expect(canTransition(JobStatus.COMPLETED, target)).toBe(false);
      expect(canTransition(JobStatus.DEAD_LETTER, target)).toBe(false);
    }
  });

  it('rejects skipping the lease step', () => {
    expect(canTransition(JobStatus.PENDING, JobStatus.RUNNING)).toBe(false);
    expect(canTransition(JobStatus.PENDING, JobStatus.COMPLETED)).toBe(false);
  });

  it('has no unreachable status', () => {
    // Every status other than the entry point must be reachable from
    // somewhere, or the queue is carrying a state it can never occupy — which
    // is what an unused `failed` status was doing before it was removed.
    for (const status of Object.values(JobStatus)) {
      if (status === JobStatus.PENDING) continue;
      expect(allowedPredecessors(status).length).toBeGreaterThan(0);
    }
  });
});

describe('allowedPredecessors', () => {
  it('is the inverse of canTransition', () => {
    for (const to of Object.values(JobStatus)) {
      const predecessors = allowedPredecessors(to);
      for (const from of Object.values(JobStatus)) {
        expect(predecessors.includes(from)).toBe(canTransition(from, to));
      }
    }
  });
});

describe('PermanentJobFailure', () => {
  it('is an Error with a recognisable name', () => {
    const error = new PermanentJobFailure('unsupported payload version');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(PermanentJobFailure);
    expect(error.name).toBe('PermanentJobFailure');
  });
});
