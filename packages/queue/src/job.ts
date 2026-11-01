import { JobId, JobStatus, CorrelationId, Timestamp } from '@nit/domain';

/**
 * A durable job. Payload is versioned so handlers can migrate safely.
 */
export interface Job<TPayload = unknown> {
  jobId: JobId;
  queueName: string;
  payload: TPayload;
  payloadVersion: string;
  status: JobStatus;
  attemptCount: number;
  maxAttempts: number;
  leaseDeadline: Timestamp | null;
  leasedBy: string | null;
  idempotencyKey: string;
  availableAt: Timestamp;
  lastError: string | null;
  correlationId: CorrelationId | null;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  completedAt: Timestamp | null;
}

export interface EnqueueRequest<TPayload = unknown> {
  queueName: string;
  payload: TPayload;
  payloadVersion: string;
  idempotencyKey: string;
  maxAttempts?: number;
  availableAt?: Timestamp;
  correlationId?: CorrelationId;
}

export interface DeadLetter<TPayload = unknown> {
  jobId: JobId;
  queueName: string;
  payload: TPayload;
  payloadVersion: string;
  attemptCount: number;
  idempotencyKey: string;
  failureReason: string;
  lastError: string | null;
  correlationId: CorrelationId | null;
  deadLetteredAt: Timestamp;
}

/**
 * Signals a failure that must never be retried.
 */
export class PermanentJobFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentJobFailure';
    Object.setPrototypeOf(this, PermanentJobFailure.prototype);
  }
}

const ALLOWED_TRANSITIONS: Record<JobStatus, readonly JobStatus[]> = {
  [JobStatus.PENDING]: [JobStatus.LEASED],
  // DEAD_LETTER is reachable directly from LEASED because fail() is public: a
  // worker that has claimed a job may detect a permanent failure — an
  // unsupported payload version, say — before it ever marks the job running.
  // Without this edge that failure is silently ignored, the lease expires,
  // recoverExpiredLeases returns the job to PENDING, and a job that can never
  // succeed is retried until it exhausts maxAttempts. TIS ch.3 §6 requires
  // permanent failures to reach the dead-letter queue unconditionally.
  //
  // LEASED and RUNNING both reach PENDING: that edge covers both a scheduled
  // retry and lease recovery after a worker crash.
  [JobStatus.LEASED]: [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.DEAD_LETTER],
  [JobStatus.RUNNING]: [JobStatus.COMPLETED, JobStatus.PENDING, JobStatus.DEAD_LETTER],
  [JobStatus.COMPLETED]: [],
  [JobStatus.DEAD_LETTER]: [],
};

export function canTransition(from: JobStatus, to: JobStatus): boolean {
  return (ALLOWED_TRANSITIONS[from] ?? []).includes(to);
}

/**
 * Every status a job may legally hold immediately before entering `to`.
 *
 * The queue passes this into its UPDATE guards so the state machine above is
 * the single authority for legal transitions, enforced in SQL rather than
 * merely asserted in unit tests.
 */
export function allowedPredecessors(to: JobStatus): readonly JobStatus[] {
  return (Object.keys(ALLOWED_TRANSITIONS) as JobStatus[]).filter((from) =>
    canTransition(from, to)
  );
}
