import { getDialect, getDatabaseType, getDatabase } from './connection';
import { insertWorkflowEvent } from './workflow-events';
import { createLogger } from '@archon/paths';
import {
  FACTORY_HUMAN_INPUT_METADATA_KEY,
  factoryHumanInputContextSchema,
  isFactoryHumanInputContext,
  type FactoryHumanInputContext,
} from '@archon/workflows/factory-human-input';

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('db.workflow-factory');
  return cachedLog;
}

function rowLockClause(): string {
  return getDatabaseType() === 'postgresql' ? ' FOR UPDATE' : '';
}

function parseJsonObject(raw: unknown): Record<string, unknown> | null {
  let value = raw;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value) as unknown;
    } catch {
      return null;
    }
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function normalizeMetadata(raw: unknown): Record<string, unknown> {
  return parseJsonObject(raw) ?? {};
}

function replaceFactoryHumanInputMetadata(paramIndex: number): string {
  const value = `$${String(paramIndex)}`;
  return getDatabaseType() === 'postgresql'
    ? `jsonb_set(metadata - 'wait', '{${FACTORY_HUMAN_INPUT_METADATA_KEY}}', ${value}::jsonb, true)`
    : `json_set(json_remove(metadata, '$.wait'), '$.${FACTORY_HUMAN_INPUT_METADATA_KEY}', json(${value}))`;
}

function factoryHumanInputExpr(field: string): string {
  return getDatabaseType() === 'postgresql'
    ? `metadata->'${FACTORY_HUMAN_INPUT_METADATA_KEY}'->>'${field}'`
    : `json_extract(metadata, '$.${FACTORY_HUMAN_INPUT_METADATA_KEY}.${field}')`;
}

function factoryHumanInputResponseMissingClause(): string {
  return getDatabaseType() === 'postgresql'
    ? `metadata->'${FACTORY_HUMAN_INPUT_METADATA_KEY}'->'response' IS NULL`
    : `json_extract(metadata, '$.${FACTORY_HUMAN_INPUT_METADATA_KEY}.response') IS NULL`;
}

export async function pauseWorkflowRunForFactoryHumanInput(
  id: string,
  context: FactoryHumanInputContext
): Promise<void> {
  const parsed = factoryHumanInputContextSchema.parse(context);
  try {
    await getDatabase().withTransaction(async query => {
      const result = await query(
        `UPDATE remote_agent_workflow_runs
         SET status = 'paused', metadata = ${replaceFactoryHumanInputMetadata(2)}
         WHERE id = $1 AND status = 'running'`,
        [id, JSON.stringify(parsed)]
      );
      if ((result.rowCount ?? 0) === 0) {
        throw new Error(`Workflow run not found or not in running state (id: ${id})`);
      }
      await insertWorkflowEvent(query, {
        workflow_run_id: id,
        event_type: 'factory_human_input_requested',
        step_name: parsed.nodeId,
        data: {
          invocation_id: parsed.invocationId,
          request_digest: parsed.requestDigest,
          lease_id: parsed.leaseId,
          launch_id: parsed.launchId,
          attempt_id: parsed.attemptId,
          message: parsed.message,
          reason: parsed.reason,
          session_id: parsed.sessionId,
          requested_at: parsed.requestedAt,
        },
      });
    });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Workflow run not found')) throw error;
    const err = error as Error;
    getLog().error({ err, workflowRunId: id }, 'db.workflow_run_factory_human_input_pause_failed');
    throw new Error(`Failed to pause workflow run for factory human input: ${err.message}`);
  }
}

export async function resolveFactoryHumanInput(
  id: string,
  context: FactoryHumanInputContext
): Promise<{ resolved: boolean }> {
  const parsed = factoryHumanInputContextSchema.parse(context);
  const dialect = getDialect();
  try {
    return await getDatabase().withTransaction(async query => {
      const result = await query(
        `UPDATE remote_agent_workflow_runs
         SET metadata = ${dialect.jsonMerge('metadata', 2)}
         WHERE id = $1
           AND status = 'paused'
           AND ${factoryHumanInputExpr('nodeId')} = $3
           AND ${factoryHumanInputExpr('invocationId')} = $4
           AND ${factoryHumanInputExpr('requestDigest')} = $5
           AND ${factoryHumanInputExpr('leaseId')} = $6
           AND ${factoryHumanInputResponseMissingClause()}`,
        [
          id,
          JSON.stringify({ [FACTORY_HUMAN_INPUT_METADATA_KEY]: parsed }),
          parsed.nodeId,
          parsed.invocationId,
          parsed.requestDigest,
          parsed.leaseId,
        ]
      );
      const resolved = (result.rowCount ?? 0) > 0;
      if (resolved) {
        await insertWorkflowEvent(query, {
          workflow_run_id: id,
          event_type: 'factory_human_input_received',
          step_name: parsed.nodeId,
          data: {
            invocation_id: parsed.invocationId,
            request_digest: parsed.requestDigest,
            lease_id: parsed.leaseId,
            command_id: parsed.response?.commandId,
            responded_at: parsed.response?.respondedAt,
          },
        });
      }
      return { resolved };
    });
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, workflowRunId: id }, 'db.workflow_run_factory_human_input_resolve_failed');
    throw new Error(`Failed to resolve factory human input: ${err.message}`);
  }
}

export async function consumeFactoryHumanInputResponse(
  id: string,
  context: FactoryHumanInputContext
): Promise<{ consumed: boolean }> {
  const parsed = factoryHumanInputContextSchema.parse(context);
  const parsedResponse = parsed.response;
  if (parsedResponse === undefined) return { consumed: false };
  try {
    return await getDatabase().withTransaction(async query => {
      const row = (
        await query<{ metadata: unknown }>(
          `SELECT metadata FROM remote_agent_workflow_runs WHERE id = $1 AND status = 'running'${rowLockClause()}`,
          [id]
        )
      ).rows[0];
      const metadata = normalizeMetadata(row?.metadata);
      const stored = metadata[FACTORY_HUMAN_INPUT_METADATA_KEY];
      if (!isFactoryHumanInputContext(stored) || stored.response === undefined) {
        return { consumed: false };
      }
      const matches =
        stored.nodeId === parsed.nodeId &&
        stored.invocationId === parsed.invocationId &&
        stored.requestDigest === parsed.requestDigest &&
        stored.leaseId === parsed.leaseId &&
        stored.iteration === parsed.iteration &&
        stored.reask === parsed.reask &&
        stored.response.commandId === parsedResponse.commandId &&
        stored.response.text === parsedResponse.text &&
        stored.response.respondedAt === parsedResponse.respondedAt &&
        stored.response.consumedAt === undefined;
      if (!matches) return { consumed: false };
      const consumedAt = new Date().toISOString();
      metadata[FACTORY_HUMAN_INPUT_METADATA_KEY] = {
        ...stored,
        response: { ...stored.response, consumedAt },
      };
      await query(
        `UPDATE remote_agent_workflow_runs SET metadata = $2${getDatabaseType() === 'postgresql' ? '::jsonb' : ''} WHERE id = $1 AND status = 'running'`,
        [id, JSON.stringify(metadata)]
      );
      await insertWorkflowEvent(query, {
        workflow_run_id: id,
        event_type: 'factory_human_input_received',
        step_name: stored.nodeId,
        data: {
          invocation_id: stored.invocationId,
          request_digest: stored.requestDigest,
          lease_id: stored.leaseId,
          command_id: stored.response.commandId,
          consumed_at: consumedAt,
        },
      });
      return { consumed: true };
    });
  } catch (error) {
    const err = error as Error;
    getLog().error({ err, workflowRunId: id }, 'db.workflow_run_factory_human_input_consume_failed');
    throw new Error(`Failed to consume factory human input: ${err.message}`);
  }
}
