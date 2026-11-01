import {
  type AgentRunId,
  type AgentTaskId,
  type AgentToolCallId,
  ValidationError,
} from '@nit/domain';
import { describe, expect, it } from 'vitest';
import {
  AgentCapability,
  AgentToolCallRequest,
  AgentToolDefinition,
  AgentToolName,
  validateAgentToolCall,
} from '.';

function definition(): AgentToolDefinition {
  return {
    name: AgentToolName.REPLAY_RUN,
    version: '1.0.0',
    requiredCapability: AgentCapability.RUN_BACKTEST,
    mutating: true,
  };
}

function request(): AgentToolCallRequest {
  return {
    toolCallId: 'call-1' as AgentToolCallId,
    agentRunId: 'run-1' as AgentRunId,
    taskId: 'task-1' as AgentTaskId,
    requestId: 'request-1',
    callerIdentity: 'agent-runtime',
    tool: AgentToolName.REPLAY_RUN,
    toolVersion: '1.0.0',
    capability: AgentCapability.RUN_BACKTEST,
    arguments: { from: '2026-01-01', to: '2026-01-02' },
    idempotencyKey: 'experiment-1',
    requestedAt: new Date('2026-01-03T00:00:00.000Z'),
  };
}

describe('agent tool authority', () => {
  it('accepts an exact capability, version, and idempotency binding', () => {
    expect(() => validateAgentToolCall(definition(), request())).not.toThrow();
  });

  it('rejects capability escalation', () => {
    const call = { ...request(), capability: AgentCapability.REQUEST_PAPER_TRADE };
    expect(() => validateAgentToolCall(definition(), call)).toThrow(ValidationError);
  });

  it('rejects a duplicate-prone mutation without an idempotency key', () => {
    const { idempotencyKey, ...call } = request();
    expect(idempotencyKey).toBeDefined();
    expect(() => validateAgentToolCall(definition(), call)).toThrow('idempotency');
  });

  it('rejects a handler that misclassifies a mutating tool as read-only', () => {
    const invalid = { ...definition(), mutating: false };
    expect(() => validateAgentToolCall(invalid, request())).toThrow(ValidationError);
  });

  it('exposes no arbitrary shell, SQL, signer, or risk-override tool', () => {
    const names = Object.values(AgentToolName);
    expect(names.some((name) => /shell|sql|sign|risk\.override|permission/.test(name))).toBe(false);
  });
});
