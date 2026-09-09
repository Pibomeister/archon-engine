import { normalizeEgressPolicy } from './policy';
import type { NormalizedEgressPolicy, RestrictedEgressPolicy } from './policy';
import { normalizeStrictHttpGrants } from './strict-https-proxy';
import type { StrictHttpGrant } from './strict-https-proxy';

export interface StrictEgressPolicy {
  schema: 'archon.strict-egress.v1';
  transport: NormalizedEgressPolicy;
  grants: StrictHttpGrant[];
}

type StrictEgressConfig = RestrictedEgressPolicy & { httpGrants: StrictHttpGrant[] };
const MAX_POLICY_BYTES = 65_536;

export function encodeStrictEgressPolicy(config: StrictEgressConfig): string {
  const { httpGrants, ...transport } = config;
  const policy = normalizePolicy({
    schema: 'archon.strict-egress.v1',
    transport,
    grants: httpGrants,
  });
  const bytes = Buffer.from(JSON.stringify(policy));
  if (bytes.length > MAX_POLICY_BYTES) throw new Error('Strict egress policy is too large.');
  return bytes.toString('base64');
}

export function decodeStrictEgressPolicy(encoded: string): StrictEgressPolicy {
  if (typeof encoded !== 'string' || encoded.length > 90_000) {
    throw new Error('Invalid strict egress policy encoding.');
  }
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.toString('base64') !== encoded || bytes.length > MAX_POLICY_BYTES) {
    throw new Error('Invalid strict egress policy encoding.');
  }
  return normalizePolicy(JSON.parse(bytes.toString('utf8')));
}

function normalizePolicy(input: unknown): StrictEgressPolicy {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new Error('Strict egress policy must be an object.');
  }
  const record = input as Record<string, unknown>;
  if (Object.keys(record).some(key => !['schema', 'transport', 'grants'].includes(key))) {
    throw new Error('Strict egress policy contains unsupported settings.');
  }
  if (record.schema !== 'archon.strict-egress.v1')
    throw new Error('Unsupported egress policy version.');
  const transport = normalizeEgressPolicy(record.transport as RestrictedEgressPolicy);
  if (transport.targets.length > 32) throw new Error('Too many strict egress targets.');
  if (!Array.isArray(record.grants) || record.grants.length > 128) {
    throw new Error('Strict egress requires bounded HTTP grants.');
  }
  const grants = normalizeStrictHttpGrants(record.grants as StrictHttpGrant[]);
  assertTargetCoverage(transport, grants);
  return { schema: 'archon.strict-egress.v1', transport, grants };
}

function assertTargetCoverage(transport: NormalizedEgressPolicy, grants: StrictHttpGrant[]): void {
  const key = (target: { host: string; port: number }): string => `${target.host}:${target.port}`;
  const targets = new Set(transport.targets.map(key));
  const granted = new Set(grants.map(key));
  if ([...granted].some(value => !targets.has(value))) {
    throw new Error('HTTP grant is outside the frozen transport targets.');
  }
  if ([...targets].some(value => !granted.has(value))) {
    throw new Error('Every transport target requires an explicit HTTP grant.');
  }
}
