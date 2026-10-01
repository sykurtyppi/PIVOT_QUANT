// §7/§10 "material edge" gate for the /levels per-level probability display.
// Extracted into an importable, unit-tested module so an inverted or dropped gate
// cannot ship silently (the page consumed this logic inline and untested before).

// Only present a gap-conditioned rate when it beats the unconditional base rate
// out-of-sample by at least this much (relative Brier improvement). Shared by the
// per-level drawer and the today's-setup line so they never disagree.
export const GAP_EDGE_MIN = 0.05;

// Pick the honest display for an outcome block given today's gap info (gi may be null
// before the session opens). Returns the gap-conditioned bucket row (mode:'gap') ONLY
// when the §7 verdict earns it (gap_beats_unconditional AND the improvement clears the
// material-edge cut) AND that gap bucket is sufficiently sampled; otherwise the
// unconditional base rate (mode:'base').
export function pickOutcome(block, gi, gapEdgeMin = GAP_EDGE_MIN) {
  const c = (block && block.calibration) || {};
  if (gi && c.gap_beats_unconditional && c.gap_rel_improvement >= gapEdgeMin) {
    const row = block.by_gap && block.by_gap[gi.bucket];
    if (row && row.sufficient) return { ...row, mode: 'gap' };
  }
  return { ...block.unconditional, mode: 'base' };
}
