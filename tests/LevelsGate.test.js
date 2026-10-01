import { pickOutcome, GAP_EDGE_MIN } from '../src/levels/gate.js';

// A per-level outcome block shaped like level_probabilities.json, with overridable
// calibration verdict and gap bucket.
const block = (over = {}) => ({
  unconditional: { rate: 0.27, ci_low: 0.26, ci_high: 0.29, n: 2492 },
  by_gap: {
    up: { rate: 0.60, ci_low: 0.56, ci_high: 0.64, n: 702, sufficient: true },
    ...(over.by_gap || {}),
  },
  calibration: {
    gap_beats_unconditional: true,
    gap_rel_improvement: 0.22,
    ...(over.calibration || {}),
  },
});

describe('pickOutcome §7/§10 material-edge gate', () => {
  test('shows the gap-conditioned rate when the verdict and bucket both qualify', () => {
    const r = pickOutcome(block(), { bucket: 'up' });
    expect(r.mode).toBe('gap');
    expect(r.rate).toBeCloseTo(0.60);
  });

  test('falls back to base rate when the gap edge is below the material-edge cut', () => {
    const r = pickOutcome(
      block({ calibration: { gap_beats_unconditional: true, gap_rel_improvement: 0.04 } }),
      { bucket: 'up' },
    );
    expect(r.mode).toBe('base');
    expect(r.rate).toBeCloseTo(0.27);
  });

  test('falls back when gap does not beat unconditional, even with a large rel number', () => {
    const r = pickOutcome(
      block({ calibration: { gap_beats_unconditional: false, gap_rel_improvement: 0.10 } }),
      { bucket: 'up' },
    );
    expect(r.mode).toBe('base');
  });

  test('falls back when the gap bucket is insufficiently sampled', () => {
    const r = pickOutcome(
      block({ by_gap: { up: { rate: 0.6, n: 10, sufficient: false } } }),
      { bucket: 'up' },
    );
    expect(r.mode).toBe('base');
  });

  test('falls back pre-open when no gap info is available', () => {
    expect(pickOutcome(block(), null).mode).toBe('base');
  });

  test('falls back when the requested gap bucket is missing from by_gap', () => {
    expect(pickOutcome(block(), { bucket: 'down' }).mode).toBe('base');
  });

  test('the boundary rel == GAP_EDGE_MIN qualifies (>=, not >)', () => {
    const r = pickOutcome(
      block({ calibration: { gap_beats_unconditional: true, gap_rel_improvement: GAP_EDGE_MIN } }),
      { bucket: 'up' },
    );
    expect(r.mode).toBe('gap');
  });

  test('GAP_EDGE_MIN is the 0.05 material-edge threshold', () => {
    expect(GAP_EDGE_MIN).toBe(0.05);
  });
});
