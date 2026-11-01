/**
 * Time utilities - all times stored in UTC
 */
export type Timestamp = Date;

export interface TimeProvenance {
  sourceTime?: Timestamp; // Time reported by external source
  retrievalTime: Timestamp; // When we retrieved/received the data
  ingestionTime: Timestamp; // When we persisted it
  completionTime?: Timestamp; // When processing completed
  deliveryTime?: Timestamp; // When we delivered the result
}

export function now(): Timestamp {
  return new Date();
}

export function parseTimestamp(iso: string): Timestamp {
  return new Date(iso);
}

export function toISOString(timestamp: Timestamp): string {
  return timestamp.toISOString();
}

export function createTimeProvenance(retrievalTime: Timestamp): TimeProvenance {
  return {
    retrievalTime,
    ingestionTime: now(),
  };
}
