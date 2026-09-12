import {
  assertKnownRunConfigKeys,
  invalidRunConfigValue,
  normalizeRunConfigString,
} from '../shared/run-config';

export interface GrokProviderDefaults {
  model?: string;
  grokBinaryPath?: string;
}

export function parseGrokConfig(raw: Record<string, unknown>): GrokProviderDefaults {
  const result: GrokProviderDefaults = {};
  if (typeof raw.model === 'string') result.model = raw.model;
  if (typeof raw.grokBinaryPath === 'string') result.grokBinaryPath = raw.grokBinaryPath;
  return result;
}

export function parseGrokRunConfig(raw: Record<string, unknown>): GrokProviderDefaults {
  assertKnownRunConfigKeys(raw, ['model', 'grokBinaryPath']);
  const model = normalizeRunConfigString(raw.model, 'model');
  const grokBinaryPath = normalizeRunConfigString(raw.grokBinaryPath, 'grokBinaryPath');
  if (raw.model !== undefined && model === undefined) {
    invalidRunConfigValue('model', 'a non-blank string');
  }
  return {
    ...parseGrokConfig(raw),
    ...(model === undefined ? {} : { model }),
    ...(grokBinaryPath === undefined ? {} : { grokBinaryPath }),
  };
}
