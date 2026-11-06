import { randomUUID } from 'node:crypto';
import { DatabasePool, Executor, clientExecutor } from '@nit/database-public';
import { AgentCapability, AgentToolCallRequest } from '@nit/agent-contracts';
import {
  AgentRunId,
  AgentTaskId,
  generateAgentRunId,
  generateAgentTaskId,
  generateAgentToolCallId,
  JobId,
} from '@nit/domain';
import { contentHash, evidenceHash } from '@nit/signals';
import { JobQueue } from '@nit/queue';
import { Clock, paperRows } from '@nit/trading';
import {
  AgentConfiguration,
  AgentConclusion,
  AgentPrincipal,
  GuideBundle,
  InvestigationTask,
  LimitsSchema,
  ModelInput,
  ModelResponseSchema,
  ReadTools,
  ReasoningModel,
  ScopeSchema,
  TaskSchema,
  assertScope,
  modelCost,
  sameIdentity,
  ToolResult,
} from './types';
import { OBSERVE_TOOL_CATALOG, ReadOnlyAgentTools } from './tools';

const QUEUE = 'agent.observe';
type Terminal =
  | 'completed'
  | 'failed'
  | 'timed_out'
  | 'cancelled'
  | 'budget_exhausted'
  | 'rejected'
  | 'interrupted';
interface TaskRow {
  task_id: AgentTaskId;
  task: InvestigationTask;
  settings: Settings;
  content_hash: string;
}
interface Settings {
  configuration: AgentConfiguration;
  guideHash: string;
  catalogHash: string;
}
interface Status {
  task: InvestigationTask;
  task_id: string;
  principal: string;
  status: string;
  run_id: string | null;
  reason: string | null;
  updated_at: Date;
}
export class AgentRuntime {
  private readonly queue: JobQueue;
  private readonly config: AgentConfiguration;
  private readonly settings: Settings;
  constructor(
    private readonly db: DatabasePool,
    configuration: AgentConfiguration,
    private readonly guides: GuideBundle,
    private readonly tools: ReadOnlyAgentTools,
    private readonly model: ReasoningModel | null,
    private readonly clock: Clock
  ) {
    this.config = JSON.parse(JSON.stringify(configuration)) as AgentConfiguration;
    LimitsSchema.parse(this.config.limits);
    if (
      configuration.version !== 1 ||
      typeof configuration.enabled !== 'boolean' ||
      typeof configuration.model.isToyImplementation !== 'boolean' ||
      !['development', 'production'].includes(configuration.environment) ||
      !configuration.codeVersion.trim() ||
      configuration.principals.length === 0 ||
      configuration.principals.length > 16 ||
      !configuration.model.provider.trim() ||
      !configuration.model.model.trim() ||
      !/^[a-f0-9]{64}$/.test(configuration.model.configurationHash)
    )
      throw new Error('INVALID_AGENT_CONFIGURATION');
    const allowed = new Set<AgentCapability>(Object.values(ReadTools));
    const identities = new Set<string>();
    for (const principal of this.config.principals) {
      ScopeSchema.parse(principal.scope);
      ScopeSchema.shape.asOf.parse(principal.expiresAt);
      if (!Number.isFinite(Date.parse(principal.expiresAt)))
        throw new Error('INVALID_PRINCIPAL_EXPIRY');
      if (
        typeof principal.allowExternalContext !== 'boolean' ||
        !principal.identity.trim() ||
        identities.has(principal.identity) ||
        principal.capabilities.some((c) => !allowed.has(c))
      )
        throw new Error('INVALID_OBSERVE_PRINCIPAL');
      identities.add(principal.identity);
    }
    if (
      guides.version !== 1 ||
      evidenceHash(guides.files) !== guides.hash ||
      Object.keys(guides.files).length !== 3 ||
      !['SOUL.md', 'INVESTIGATION.md', 'OUTPUT.md'].every(
        (name) => guides.files[name] !== undefined
      ) ||
      Object.values(guides.files).some((f) => contentHash(f.text) !== f.hash) ||
      Buffer.byteLength(JSON.stringify(guides)) > 32000
    )
      throw new Error('INVALID_GUIDE_BUNDLE');
    if (
      configuration.enabled &&
      configuration.environment === 'production' &&
      (configuration.model.isToyImplementation || model?.identity.isToyImplementation)
    )
      throw new Error('TOY_MODEL_REJECTED');
    if (model !== null && !sameIdentity(configuration.model, model.identity))
      throw new Error('MODEL_IDENTITY_MISMATCH');
    this.settings = {
      configuration: this.config,
      guideHash: guides.hash,
      catalogHash: evidenceHash(OBSERVE_TOOL_CATALOG),
    };
    if (Buffer.byteLength(JSON.stringify(this.settings)) > 64000)
      throw new Error('AGENT_SETTINGS_TOO_LARGE');
    this.queue = new JobQueue(db, {
      leaseTimeoutMs: this.config.limits.maxWallMs + 10000,
      defaultMaxAttempts: 1,
      backoff: { baseMs: 1000, maxMs: 1000 },
      clock: (): Date => this.clock.now(),
    });
  }
  async enqueue(raw: unknown): Promise<{ taskId: AgentTaskId; existing: boolean }> {
    const task = TaskSchema.parse(raw);
    const principal = this.principal(task.principal);
    assertScope(task.scope, principal.scope, this.config.limits, this.clock.now());
    const digest = evidenceHash({ task, settings: this.settings });
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      await paperRows(db, 'SELECT pg_advisory_xact_lock(hashtextextended($1,0))', ['agent-intake']);
      const existing = await paperRows<{ task_id: AgentTaskId; content_hash: string }>(
        db,
        'SELECT task_id,content_hash FROM agent_tasks WHERE principal=$1 AND request_id=$2',
        [task.principal, task.requestId]
      );
      if (existing[0] !== undefined) {
        if (existing[0].content_hash !== digest) throw new Error('REQUEST_ID_COLLISION');
        return { taskId: existing[0].task_id, existing: true };
      }
      const pending = await paperRows<{ count: string }>(
        db,
        `SELECT COUNT(*)::text AS count FROM agent_task_state s JOIN agent_tasks t USING(task_id)
        WHERE s.status IN ('queued','running')`,
        []
      );
      if (Number(pending[0]?.count ?? 0) >= this.config.limits.maxQueuedTasks)
        throw new Error('AGENT_QUEUE_FULL');
      await paperRows(
        db,
        'INSERT INTO agent_guides(guide_hash,bundle) VALUES ($1,$2) ON CONFLICT DO NOTHING',
        [this.guides.hash, JSON.stringify(this.guides)]
      );
      const taskId = generateAgentTaskId();
      const now = this.clock.now();
      await paperRows(db, 'INSERT INTO agent_tasks VALUES ($1,$2,$3,$4,$5,$6,$7,$8)', [
        taskId,
        task.principal,
        task.requestId,
        JSON.stringify(task),
        JSON.stringify(this.settings),
        digest,
        this.guides.hash,
        now,
      ]);
      await paperRows(
        db,
        "INSERT INTO agent_task_state(task_id,status,updated_at) VALUES ($1,'queued',$2)",
        [taskId, now]
      );
      const job = await this.queue.enqueue(
        {
          queueName: QUEUE,
          payload: { taskId },
          payloadVersion: '1',
          idempotencyKey: taskId,
          maxAttempts: 1,
        },
        client
      );
      if (job.kind === 'failure') throw job.error;
      return { taskId, existing: false };
    });
    if (result.kind === 'failure') throw result.error;
    return result.value;
  }
  async status(principal: string, taskId: string): Promise<unknown> {
    const current = this.principal(principal);
    const found = await paperRows<Status>(
      this.db,
      `SELECT t.task_id,t.principal,t.task,s.status,s.run_id,s.reason,s.updated_at
      FROM agent_tasks t JOIN agent_task_state s USING(task_id) WHERE task_id=$1 AND principal=$2`,
      [taskId, principal]
    );
    if (found[0] === undefined) throw new Error('TASK_NOT_AUTHORIZED');
    assertScope(found[0].task.scope, current.scope, this.config.limits, this.clock.now());
    const conclusion = await paperRows<{
      conclusion: unknown;
      content_hash: string;
      evidence_ids: string[];
    }>(
      this.db,
      'SELECT conclusion,content_hash,evidence_ids FROM agent_conclusions WHERE task_id=$1',
      [taskId]
    );
    if (
      conclusion[0] !== undefined &&
      evidenceHash(conclusion[0].conclusion) !== conclusion[0].content_hash
    )
      throw new Error('CONCLUSION_HASH_MISMATCH');
    return { task: found[0], conclusion: conclusion[0] ?? null };
  }
  async cancel(principal: string, taskId: string): Promise<void> {
    this.principal(principal);
    const owned = await paperRows(
      this.db,
      'SELECT 1 FROM agent_tasks WHERE task_id=$1 AND principal=$2',
      [taskId, principal]
    );
    if (owned.length === 0) throw new Error('TASK_NOT_AUTHORIZED');
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const rows = await paperRows<{ run_id: AgentRunId | null; status: string }>(
        db,
        'SELECT * FROM agent_task_state WHERE task_id=$1 FOR UPDATE',
        [taskId]
      );
      if (!['queued', 'running'].includes(rows[0]!.status)) return;
      await paperRows(
        db,
        "INSERT INTO agent_task_cancellations VALUES ($1,$2,$3,'OPERATOR_CANCELLED') ON CONFLICT DO NOTHING",
        [taskId, principal, this.clock.now()]
      );
      await paperRows(
        db,
        "UPDATE agent_task_state SET status='cancelled',reason='OPERATOR_CANCELLED',updated_at=$2 WHERE task_id=$1",
        [taskId, this.clock.now()]
      );
      if (rows[0]!.run_id !== null)
        await this.event(db, rows[0]!.run_id, 'cancelled', { reason: 'OPERATOR_CANCELLED' });
    });
    if (result.kind === 'failure') throw result.error;
  }
  async trace(principal: string, taskId: string): Promise<unknown> {
    await this.status(principal, taskId);
    const runs = await paperRows<{ run_id: string }>(
      this.db,
      'SELECT * FROM agent_runs WHERE task_id=$1',
      [taskId]
    );
    const runId = runs[0]?.run_id;
    const cancellations = await paperRows(
      this.db,
      'SELECT * FROM agent_task_cancellations WHERE task_id=$1',
      [taskId]
    );
    if (runId === undefined) return { taskId, cancellations, runs: [] };
    const events = await paperRows(
      this.db,
      'SELECT * FROM agent_run_events WHERE run_id=$1 ORDER BY ordinal',
      [runId]
    );
    const modelCalls = await paperRows(
      this.db,
      `SELECT a.*,r.outcome,r.response,r.usage,r.completed_at FROM agent_model_attempts a
      LEFT JOIN agent_model_results r USING(call_id) WHERE a.run_id=$1 ORDER BY step`,
      [runId]
    );
    const tools = await paperRows(
      this.db,
      'SELECT * FROM agent_tool_calls WHERE run_id=$1 ORDER BY step',
      [runId]
    );
    return { taskId, cancellations, runs, events, modelCalls, tools };
  }
  async recover(): Promise<void> {
    const recovered = await this.queue.recoverExpiredLeases();
    if (recovered.kind === 'failure') throw recovered.error;
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const expired = await paperRows<{ run_id: AgentRunId; task_id: string }>(
        db,
        `SELECT r.run_id,r.task_id FROM agent_runs r
        JOIN agent_task_state s USING(task_id) WHERE s.status='running' AND r.deadline_at<$1 FOR UPDATE OF s`,
        [this.clock.now()]
      );
      for (const run of expired) {
        await paperRows(
          db,
          "UPDATE agent_task_state SET status='interrupted',reason='INTERRUPTED_NO_AUTOMATIC_MODEL_RETRY',updated_at=$2 WHERE task_id=$1",
          [run.task_id, this.clock.now()]
        );
        await paperRows(
          db,
          `INSERT INTO agent_model_results(call_id,outcome,completed_at)
          SELECT a.call_id,'unknown_after_interruption',$2 FROM agent_model_attempts a WHERE a.run_id=$1
          AND NOT EXISTS(SELECT 1 FROM agent_model_results r WHERE r.call_id=a.call_id) ON CONFLICT DO NOTHING`,
          [run.run_id, this.clock.now()]
        );
        await this.event(db, run.run_id, 'interrupted', {
          reason: 'INTERRUPTED_NO_AUTOMATIC_MODEL_RETRY',
        });
      }
      await paperRows(
        db,
        `UPDATE agent_task_state s SET status='interrupted',reason='QUEUE_LEASE_EXPIRED_BEFORE_START',updated_at=$1
        FROM durable_jobs j WHERE s.status='queued' AND j.queue_name=$2 AND j.payload->>'taskId'=s.task_id::text AND j.status='dead_letter'`,
        [this.clock.now(), QUEUE]
      );
    });
    if (result.kind === 'failure') throw result.error;
  }
  async runOne(
    signal?: AbortSignal
  ): Promise<{ handled: boolean; reason: string; taskId?: string }> {
    if (!this.config.enabled || this.model === null)
      return { handled: false, reason: 'MODEL_DISABLED' };
    const limits = this.config.limits;
    const pressure = await paperRows(
      this.db,
      `SELECT 1 FROM paper_portfolios p JOIN strategy_runs r USING(run_id)
      LEFT JOIN paper_dispatch_health h USING(portfolio_id) WHERE r.mode='forward_paper'
      AND (EXISTS(SELECT 1 FROM positions x WHERE x.portfolio_id=p.portfolio_id AND x.open_quantity>0)
        OR EXISTS(SELECT 1 FROM orders o WHERE o.portfolio_id=p.portfolio_id AND NOT EXISTS(SELECT 1 FROM paper_execution_results e WHERE e.order_id=o.order_id)))
      AND (h.checked_at IS NULL OR h.checked_at<$1) LIMIT 1`,
      [new Date(this.clock.now().getTime() - limits.maxProtectionLagMs)]
    );
    if (pressure.length > 0) return { handled: false, reason: 'PROTECTION_PRIORITY' };
    await this.recover();
    const owner = randomUUID();
    const runId = generateAgentRunId();
    const lease = await paperRows(
      this.db,
      `INSERT INTO agent_runtime_lease VALUES ('observe-only',$1,$2,$3)
      ON CONFLICT(lease_key) DO UPDATE SET owner=EXCLUDED.owner,run_id=EXCLUDED.run_id,expires_at=EXCLUDED.expires_at
      WHERE agent_runtime_lease.expires_at<=$4 RETURNING owner`,
      [
        owner,
        runId,
        new Date(this.clock.now().getTime() + limits.maxWallMs + 10000),
        this.clock.now(),
      ]
    );
    if (lease.length === 0) return { handled: false, reason: 'AGENT_BUSY' };
    let jobId: JobId | null = null;
    let taskId: AgentTaskId | null = null;
    const controller = new AbortController();
    const abort = (): void => controller.abort();
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    const deadlineTimer = setTimeout(abort, limits.maxWallMs);
    try {
      const claim = await this.queue.claim(QUEUE, owner);
      if (claim.kind === 'failure') throw claim.error;
      if (claim.value === null) return { handled: false, reason: 'NO_TASK' };
      jobId = claim.value.jobId;
      const marked = await this.queue.markRunning(jobId, { workerId: owner });
      if (marked.kind === 'failure') throw marked.error;
      if (!marked.value) throw new Error('QUEUE_LEASE_LOST');
      const payload = claim.value.payload as { taskId: string };
      const found = await paperRows<TaskRow>(
        this.db,
        'SELECT * FROM agent_tasks WHERE task_id=$1',
        [payload.taskId]
      );
      const row = found[0];
      if (row === undefined) throw new Error('TASK_MISSING');
      taskId = row.task_id;
      const started = await this.db.transaction(async (client) => {
        const db = clientExecutor(client);
        const state = await paperRows<{ status: string }>(
          db,
          'SELECT status FROM agent_task_state WHERE task_id=$1 FOR UPDATE',
          [taskId]
        );
        if (state[0]?.status !== 'queued') return false;
        await paperRows(db, 'INSERT INTO agent_runs VALUES ($1,$2,$3,$4,$5,$6)', [
          runId,
          taskId,
          this.clock.now(),
          new Date(this.clock.now().getTime() + limits.maxWallMs),
          evidenceHash(row.settings),
          JSON.stringify(this.model!.identity),
        ]);
        await paperRows(
          db,
          "UPDATE agent_task_state SET status='running',run_id=$2,updated_at=$3 WHERE task_id=$1",
          [taskId, runId, this.clock.now()]
        );
        await this.event(db, runId, 'started', {
          mode: 'observe-only',
          guideHash: row.settings.guideHash,
        });
        return true;
      });
      if (started.kind === 'failure') throw started.error;
      if (!started.value) {
        await this.completeJob(jobId, owner);
        return { handled: true, reason: 'TASK_ALREADY_TERMINAL', taskId };
      }
      if (
        evidenceHash(row.settings) !== evidenceHash(this.settings) ||
        row.content_hash !== evidenceHash({ task: row.task, settings: row.settings })
      ) {
        await this.finish(runId, taskId, 'rejected', 'PINNED_SETTINGS_CHANGED');
        await this.completeJob(jobId, owner);
        return { handled: true, reason: 'PINNED_SETTINGS_CHANGED', taskId };
      }
      const principal = this.principal(row.task.principal);
      assertScope(row.task.scope, principal.scope, limits, this.clock.now());
      if (!principal.allowExternalContext) {
        await this.finish(runId, taskId, 'rejected', 'EXTERNAL_CONTEXT_NOT_AUTHORIZED');
        await this.completeJob(jobId, owner);
        return { handled: true, reason: 'EXTERNAL_CONTEXT_NOT_AUTHORIZED', taskId };
      }
      const history: unknown[] = [];
      const evidence = new Map<string, string>();
      let toolCount = 0;
      for (let step = 0; step < limits.maxSteps; step++) {
        this.principal(row.task.principal);
        await this.checkActive(runId, taskId, owner, controller.signal);
        const requestId = `${runId}:${step}`;
        const input: ModelInput = {
          budget: {
            remainingSteps: limits.maxSteps - step,
            remainingToolCalls: limits.maxToolCalls - toolCount,
            remainingReservedTokens:
              limits.maxRunTokens - step * (limits.maxInputTokens + limits.maxOutputTokens),
            remainingReservedCostMicros:
              limits.maxRunCostMicros -
              step * modelCost(limits.maxInputTokens, limits.maxOutputTokens, limits),
          },
          protocolVersion: 1,
          requestId,
          identity: this.model.identity,
          guides: this.guides,
          task: row.task,
          tools: OBSERVE_TOOL_CATALOG.filter((t) =>
            principal.capabilities.includes(ReadTools[t.name])
          ),
          history,
          maxInputTokens: limits.maxInputTokens,
          maxOutputTokens: limits.maxOutputTokens,
        };
        const inputBytes = Buffer.byteLength(JSON.stringify(input));
        if (inputBytes + limits.requestOverheadTokens > limits.maxInputTokens)
          throw new Error('BUDGET_CONTEXT_EXHAUSTED');
        const callId = randomUUID();
        await this.reserve(runId, row.task.principal, step, callId, requestId, input, inputBytes);
        const callController = new AbortController();
        const stopCall = (): void => callController.abort();
        controller.signal.addEventListener('abort', stopCall, { once: true });
        if (controller.signal.aborted) stopCall();
        const timer = setTimeout(stopCall, limits.modelTimeoutMs);
        let parsed: ReturnType<typeof ModelResponseSchema.parse>;
        try {
          if (callController.signal.aborted) throw new Error('MODEL_TIMED_OUT');
          this.principal(row.task.principal);
          await this.checkActive(runId, taskId, owner, controller.signal);
          const raw = await abortable(
            this.model.plan(input, callController.signal),
            callController.signal
          );
          if (Buffer.byteLength(JSON.stringify(raw)) > 96000)
            throw new Error('MODEL_RESPONSE_TOO_LARGE');
          parsed = ModelResponseSchema.parse(raw);
          if (
            parsed.requestId !== requestId ||
            parsed.usage.inputTokens > limits.maxInputTokens ||
            parsed.usage.outputTokens > limits.maxOutputTokens
          )
            throw new Error('MODEL_USAGE_OR_ID_INVALID');
          await paperRows(
            this.db,
            'INSERT INTO agent_model_results VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
            [
              callId,
              'received',
              JSON.stringify(parsed),
              JSON.stringify({
                ...parsed.usage,
                computedCostMicros: modelCost(
                  parsed.usage.inputTokens,
                  parsed.usage.outputTokens,
                  limits
                ),
              }),
              this.clock.now(),
            ]
          );
        } catch {
          await paperRows(
            this.db,
            'INSERT INTO agent_model_results(call_id,outcome,completed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
            [
              callId,
              callController.signal.aborted ? 'timeout_or_cancelled' : 'unavailable_or_invalid',
              this.clock.now(),
            ]
          );
          throw new Error(
            callController.signal.aborted ? 'MODEL_TIMED_OUT' : 'MODEL_UNAVAILABLE_OR_INVALID'
          );
        } finally {
          clearTimeout(timer);
          callController.abort();
          controller.signal.removeEventListener('abort', stopCall);
        }
        await this.checkActive(runId, taskId, owner, controller.signal);
        if (parsed.action.kind === 'conclusion') {
          const conclusion = parsed.action.conclusion;
          if (
            conclusion.recommendation !== 'INSUFFICIENT_EVIDENCE' &&
            conclusion.claims.length === 0
          )
            throw new Error('UNGROUNDED_CONCLUSION');
          const cited = [...new Set(conclusion.claims.flatMap((c) => c.evidenceIds))];
          if (cited.some((id) => !evidence.has(id)))
            throw new Error('FABRICATED_OR_UNSEEN_EVIDENCE');
          await this.finish(runId, taskId, 'completed', 'OBSERVE_ONLY_CONCLUSION', {
            ...conclusion,
            interpretationLabel: 'MODEL_INTERPRETATION_NOT_FACT_VERIFIED',
            evidence: cited.map((id) => ({ evidenceId: id, hash: evidence.get(id) })),
            guideHash: this.guides.hash,
            settingsHash: evidenceHash(this.settings),
            model: this.model.identity,
          });
          await this.completeJob(jobId, owner);
          return { handled: true, reason: 'COMPLETED', taskId };
        }
        if (toolCount >= limits.maxToolCalls) throw new Error('BUDGET_TOOL_CALLS_EXHAUSTED');
        toolCount++;
        const action = parsed.action;
        const toolCallId = generateAgentToolCallId();
        const began = this.clock.now();
        let result: ToolResult | null = null;
        let outcome = 'received';
        const argsBytes = Buffer.byteLength(JSON.stringify(action.arguments ?? null));
        const auditedArguments =
          argsBytes <= 8192
            ? (action.arguments ?? null)
            : { omitted: true, bytes: argsBytes, hash: evidenceHash(action.arguments ?? null) };
        const capability = Object.prototype.hasOwnProperty.call(ReadTools, action.tool)
          ? ReadTools[action.tool as keyof typeof ReadTools]
          : null;
        try {
          if (capability === null) throw new Error('TOOL_NOT_ALLOWED');
          if (argsBytes > 8192) throw new Error('INVALID_TOOL_ARGUMENTS');
          const request: AgentToolCallRequest = {
            toolCallId,
            agentRunId: runId,
            taskId,
            requestId,
            callerIdentity: row.task.principal,
            tool: action.tool as keyof typeof ReadTools,
            toolVersion: action.version,
            capability,
            arguments: action.arguments,
            requestedAt: began,
          };
          result = await abortable(
            this.tools.execute(
              request,
              row.task.scope,
              principal.capabilities,
              limits.maxResultBytes
            ),
            controller.signal
          );
          for (const record of result.records) {
            const prior = evidence.get(record.evidenceId);
            if (prior !== undefined && prior !== record.hash)
              throw new Error('EVIDENCE_IDENTITY_COLLISION');
          }
          for (const record of result.records) evidence.set(record.evidenceId, record.hash);
        } catch (error) {
          outcome = safeToolError(error);
          result = null;
        }
        await paperRows(
          this.db,
          `INSERT INTO agent_tool_calls(tool_call_id,run_id,task_id,request_id,caller_identity,step,tool,tool_version,capability,arguments,
          outcome,result,evidence_ids,result_hash,started_at,completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
          [
            toolCallId,
            runId,
            taskId,
            requestId,
            row.task.principal,
            step,
            action.tool,
            action.version,
            capability,
            JSON.stringify(auditedArguments),
            outcome,
            result === null ? null : JSON.stringify(result),
            result?.records.map((r) => r.evidenceId) ?? [],
            result === null ? null : evidenceHash(result),
            began,
            this.clock.now(),
          ]
        );
        history.push({
          action: { ...action, arguments: auditedArguments },
          result: result ?? { error: outcome, dataQuality: 'insufficient' },
        });
      }
      throw new Error('BUDGET_STEPS_EXHAUSTED');
    } catch (error) {
      const code = safeRuntimeError(error, controller.signal.aborted);
      if (taskId !== null)
        await this.finish(
          runId,
          taskId,
          code.startsWith('BUDGET')
            ? 'budget_exhausted'
            : code === 'TASK_CANCELLED'
              ? 'cancelled'
              : code.includes('TIMED_OUT')
                ? 'timed_out'
                : 'failed',
          code
        );
      if (jobId !== null) await this.completeJob(jobId, owner);
      return { handled: jobId !== null, reason: code, ...(taskId === null ? {} : { taskId }) };
    } finally {
      clearTimeout(deadlineTimer);
      signal?.removeEventListener('abort', abort);
      await paperRows(
        this.db,
        "DELETE FROM agent_runtime_lease WHERE lease_key='observe-only' AND owner=$1",
        [owner]
      );
    }
  }
  private principal(identity: string): AgentPrincipal {
    const principal = this.config.principals.find((p) => p.identity === identity);
    if (principal === undefined) throw new Error('PRINCIPAL_DENIED');
    if (Date.parse(principal.expiresAt) <= this.clock.now().getTime())
      throw new Error('PRINCIPAL_EXPIRED');
    return principal;
  }
  private async checkActive(
    runId: string,
    taskId: string,
    owner: string,
    signal: AbortSignal
  ): Promise<void> {
    const rows = await paperRows<{
      status: string;
      deadline_at: Date;
      owner: string | null;
      principal: string;
    }>(
      this.db,
      `SELECT s.status,r.deadline_at,l.owner,t.principal FROM agent_task_state s JOIN agent_tasks t USING(task_id)
      JOIN agent_runs r USING(run_id) LEFT JOIN agent_runtime_lease l ON l.run_id=r.run_id WHERE s.task_id=$1 AND r.run_id=$2`,
      [taskId, runId]
    );
    if (rows[0]?.status === 'cancelled') throw new Error('TASK_CANCELLED');
    if (rows[0] !== undefined) this.principal(rows[0].principal);
    if (
      signal.aborted ||
      rows[0] === undefined ||
      rows[0].status !== 'running' ||
      rows[0].owner !== owner ||
      rows[0].deadline_at <= this.clock.now()
    )
      throw new Error('TASK_TIMED_OUT');
    if (this.model === null || !sameIdentity(this.config.model, this.model.identity))
      throw new Error('MODEL_IDENTITY_CHANGED');
  }
  private async reserve(
    runId: AgentRunId,
    principal: string,
    step: number,
    callId: string,
    requestId: string,
    input: ModelInput,
    inputBytes: number
  ): Promise<void> {
    const limits = this.config.limits;
    const tokens = limits.maxInputTokens + limits.maxOutputTokens;
    const cost = modelCost(limits.maxInputTokens, limits.maxOutputTokens, limits);
    const trace = inputBytes + 192000 + limits.maxResultBytes + 32768;
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const day = this.clock.now().toISOString().slice(0, 10);
      await paperRows(
        db,
        'INSERT INTO agent_daily_budgets(principal,day) VALUES ($1,$2) ON CONFLICT DO NOTHING',
        [principal, day]
      );
      const daily = await paperRows<{
        reserved_tokens: string;
        reserved_cost_micros: string;
        reserved_trace_bytes: string;
      }>(db, 'SELECT * FROM agent_daily_budgets WHERE principal=$1 AND day=$2 FOR UPDATE', [
        principal,
        day,
      ]);
      const run = await paperRows<{ tokens: string; cost: string; trace: string }>(
        db,
        `SELECT COALESCE(SUM(reserved_tokens),0)::text AS tokens,
        COALESCE(SUM(reserved_cost_micros),0)::text AS cost,COALESCE(SUM(reserved_trace_bytes),0)::text AS trace FROM agent_model_attempts WHERE run_id=$1`,
        [runId]
      );
      if (
        Number(run[0]?.tokens ?? 0) + tokens > limits.maxRunTokens ||
        Number(run[0]?.cost ?? 0) + cost > limits.maxRunCostMicros ||
        Number(run[0]?.trace ?? 0) + trace > limits.maxTraceBytes ||
        Number(daily[0]!.reserved_tokens) + tokens > limits.dailyTokens ||
        Number(daily[0]!.reserved_cost_micros) + cost > limits.dailyCostMicros ||
        Number(daily[0]!.reserved_trace_bytes) + trace > limits.dailyTraceBytes
      )
        throw new Error('BUDGET_RESERVATION_DENIED');
      await paperRows(
        db,
        'UPDATE agent_daily_budgets SET reserved_tokens=reserved_tokens+$3,reserved_cost_micros=reserved_cost_micros+$4,reserved_trace_bytes=reserved_trace_bytes+$5 WHERE principal=$1 AND day=$2',
        [principal, day, tokens, cost, trace]
      );
      await paperRows(
        db,
        'INSERT INTO agent_model_attempts VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
        [
          callId,
          runId,
          step,
          requestId,
          evidenceHash(input),
          JSON.stringify(input),
          tokens,
          cost,
          trace,
          this.clock.now(),
        ]
      );
    });
    if (result.kind === 'failure') throw result.error;
  }
  private async event(
    db: Executor,
    runId: AgentRunId,
    kind: string,
    detail: unknown
  ): Promise<void> {
    await paperRows(
      db,
      `INSERT INTO agent_run_events(event_id,run_id,ordinal,kind,detail,occurred_at)
      SELECT $1,$2,COALESCE(MAX(ordinal),0)+1,$3,$4,$5 FROM agent_run_events WHERE run_id=$2`,
      [randomUUID(), runId, kind, JSON.stringify(detail), this.clock.now()]
    );
  }
  private async finish(
    runId: AgentRunId,
    taskId: string,
    status: Terminal,
    reason: string,
    conclusion?: AgentConclusion & Record<string, unknown>
  ): Promise<void> {
    const result = await this.db.transaction(async (client) => {
      const db = clientExecutor(client);
      const state = await paperRows<{ status: string; run_id: string | null }>(
        db,
        'SELECT * FROM agent_task_state WHERE task_id=$1 FOR UPDATE',
        [taskId]
      );
      if (state[0]?.run_id !== runId || state[0].status !== 'running') return;
      if (conclusion !== undefined) {
        const evidenceIds = [...new Set(conclusion.claims.flatMap((c) => c.evidenceIds))];
        await paperRows(db, 'INSERT INTO agent_conclusions VALUES ($1,$2,$3,$4,$5,$6)', [
          runId,
          taskId,
          JSON.stringify(conclusion),
          evidenceHash(conclusion),
          evidenceIds,
          this.clock.now(),
        ]);
      }
      await paperRows(
        db,
        'UPDATE agent_task_state SET status=$2,reason=$3,updated_at=$4 WHERE task_id=$1',
        [taskId, status, reason, this.clock.now()]
      );
      await this.event(db, runId, status, { reason });
    });
    if (result.kind === 'failure') throw result.error;
  }
  private async completeJob(id: JobId, owner: string): Promise<void> {
    const result = await this.queue.complete(id, { workerId: owner });
    if (result.kind === 'failure') throw result.error;
  }
}
async function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw new Error('TASK_TIMED_OUT');
  let abort = (): void => undefined;
  const cancellation = new Promise<never>((_resolve, reject) => {
    abort = (): void => reject(new Error('TASK_TIMED_OUT'));
    signal.addEventListener('abort', abort, { once: true });
  });
  try {
    return await Promise.race([promise, cancellation]);
  } finally {
    signal.removeEventListener('abort', abort);
  }
}
function safeToolError(error: unknown): string {
  const message = error instanceof Error ? error.message : '';
  return /^(TOOL_NOT_ALLOWED|CAPABILITY_DENIED|INVALID_TOOL_ARGUMENTS|[A-Z_]*SCOPE_DENIED|[A-Z_]*TOO_LARGE|EVIDENCE_IDENTITY_COLLISION|REPORT_HASH_MISMATCH)$/.test(
    message
  )
    ? message
    : 'TOOL_UNAVAILABLE';
}
function safeRuntimeError(error: unknown, aborted: boolean): string {
  const message = error instanceof Error ? error.message : '';
  if (aborted) return 'TASK_TIMED_OUT';
  return /^(BUDGET_[A-Z_]+|MODEL_[A-Z_]+|TASK_CANCELLED|TASK_TIMED_OUT|UNGROUNDED_CONCLUSION|FABRICATED_OR_UNSEEN_EVIDENCE|SCOPE_DENIED)$/.test(
    message
  )
    ? message
    : 'AGENT_FAILED_CLOSED';
}
