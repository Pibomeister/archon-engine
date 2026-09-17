import type { FactoryProviderScope } from './factory-sandbox';
import type { ResolvedModel, TokenUsage } from './types';

export interface FactoryInvocationContext {
  runId: string;
  nodeId: string;
  launchId?: string;
  attemptId?: string;
  iteration?: number;
  reask?: number;
}

export type FactoryInvocationOutcome =
  | 'completed'
  | 'provider-error'
  | 'auth-failure'
  | 'human-input-required'
  | 'uncertain-termination';

export interface FactoryInvocationSignal {
  kind: 'factory-invocation-outcome';
  outcome: FactoryInvocationOutcome;
  provider: string;
  invocationId: string;
  requestDigest: string;
  leaseId: string;
  context: {
    runId: string;
    nodeId: string;
    launchId?: string;
    attemptId?: string;
    iteration?: number;
    reask?: number;
  };
  usage?: TokenUsage;
  sessionId?: string;
  stopReason?: string;
  errorSubtype?: string;
  errors?: string[];
  resumed?: boolean;
  resolvedModel?: ResolvedModel;
  humanInput?: {
    message: string;
    reason?: string;
    sessionId?: string;
    choices?: readonly string[];
    questions?: readonly unknown[];
  };
  occurredAt: string;
}

export interface FactoryHumanInputRequest {
  [key: string]: unknown;
  type: 'human_input_request';
  message: string;
  reason?: string;
  sessionId?: string;
  choices?: readonly string[];
  signal?: FactoryInvocationSignal;
}

export type FactoryObservationChunk = {
  type: 'factory_observation';
  signal: FactoryInvocationSignal;
};

declare module './types' {
  interface SendQueryOptions {
    factoryInvocation?: FactoryInvocationContext;
    factoryScope?: FactoryProviderScope;
    factoryTransportClosed?: () => void;
  }
  interface ProviderCapabilities {
    humanInputRequests?: boolean;
  }
}
