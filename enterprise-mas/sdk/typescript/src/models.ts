"""mas SDK TypeScript models."""

export interface StableApiError {
  code: string;
  message: string;
  details?: any;
}

export interface ExecutionResponse {
  id: string;
  status: ExecutionStatus;
  input?: any;
  output?: any;
  createdAt?: string;
  startedAt?: string;
  completedAt?: string;
  idempotentReplay?: boolean;
}

export enum ExecutionStatus {
  Completed = 'completed',
  Cancelled = 'cancelled',
  Failed = 'failed',
  Disabled = 'disabled',
}

export interface ApiEnvelope {
  data: any;
  envelope: {
    errors?: any[];
    idempotentReplay?: boolean;
  };
}