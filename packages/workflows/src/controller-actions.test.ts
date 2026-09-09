import { describe, expect, test } from 'bun:test';
import {
  computeControllerActionManifestDigest,
  isControllerActionManifestSealed,
  sealControllerActionManifest,
} from './controller-actions';

const LEGACY_BARE_INPUT_DIGEST = 'be4ecfe84a3545a818e5598121bbd4ca8be5861aa1c8541124a4e1b2b3b1aedc';
const PUBLISH_V1_DIGEST = '79c5292718207a23e0852f2f6a113e675826cfe276949817d66c8216da99b492';

describe('controller action manifests', () => {
  test('binds manifest digest to both id and input', () => {
    const input = { run: 'run-1', approved: true };
    const digest = computeControllerActionManifestDigest({ id: 'approval-check', input });

    expect(isControllerActionManifestSealed({ id: 'approval-check', digest, input })).toBe(true);
    expect(isControllerActionManifestSealed({ id: 'different-action', digest, input })).toBe(false);
  });

  test('rejects legacy bare-input digests as unsealed', () => {
    const input = { run: 'run-1', approved: true };

    expect(
      isControllerActionManifestSealed({
        id: 'approval-check',
        digest: LEGACY_BARE_INPUT_DIGEST,
        input,
      })
    ).toBe(false);
  });

  test('seal preserves a prevalidated literal id-bound digest', () => {
    const input = { nested: { value: 1 } };
    const sealed = sealControllerActionManifest({
      id: 'publish:v1',
      digest: PUBLISH_V1_DIGEST,
      input,
    });

    expect(computeControllerActionManifestDigest({ id: 'publish:v1', input })).toBe(
      PUBLISH_V1_DIGEST
    );
    expect(sealed.digest).toBe(PUBLISH_V1_DIGEST);
    expect(sealed.input).toEqual(input);
    expect(isControllerActionManifestSealed(sealed)).toBe(true);
  });

  test('seal rejects invalid, legacy, or id-mutated digests instead of recomputing', () => {
    const input = { nested: { value: 1 } };

    expect(() =>
      sealControllerActionManifest({ id: 'publish:v1', digest: 'ignored', input })
    ).toThrow(/not sealed/);
    expect(() =>
      sealControllerActionManifest({ id: 'other:v1', digest: PUBLISH_V1_DIGEST, input })
    ).toThrow(/not sealed/);
    expect(() =>
      sealControllerActionManifest({ id: ' publish', digest: PUBLISH_V1_DIGEST, input })
    ).toThrow(/not sealed/);
  });
});
