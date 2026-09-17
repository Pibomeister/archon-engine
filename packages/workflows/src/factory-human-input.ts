import { z } from 'zod';

export const FACTORY_HUMAN_INPUT_METADATA_KEY = 'factory_human_input';

export const factoryHumanInputContextSchema = z.strictObject({
  version: z.literal('archon.factory-human-input.v1'),
  runId: z.string().min(1),
  nodeId: z.string().min(1),
  invocationId: z.string().min(1),
  requestDigest: z.string().min(1),
  leaseId: z.string().min(1),
  launchId: z.string().min(1).optional(),
  attemptId: z.string().min(1).optional(),
  iteration: z.number().int().nonnegative().optional(),
  reask: z.number().int().nonnegative().optional(),
  message: z.string().min(1),
  reason: z.string().min(1).optional(),
  sessionId: z.string().min(1).optional(),
  requestedAt: z.string().datetime(),
  response: z
    .strictObject({
      commandId: z.string().min(1),
      text: z.string(),
      respondedAt: z.string().datetime(),
      consumedAt: z.string().datetime().optional(),
    })
    .optional(),
});
export type FactoryHumanInputContext = z.infer<typeof factoryHumanInputContextSchema>;

export function isFactoryHumanInputContext(value: unknown): value is FactoryHumanInputContext {
  return factoryHumanInputContextSchema.safeParse(value).success;
}
