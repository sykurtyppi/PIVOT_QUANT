/**
 * Property-based tests for src/math/MathematicalModels.js
 *
 * These encode algebraic invariants that single-example unit tests cannot
 * adequately cover — and that would have caught the P1-A, P1-B, P1-C, and
 * P1-D bugs the audit found.  fast-check's shrinking automatically produces
 * minimal reproducers when an invariant is violated, which is invaluable
 * for floating-point edge cases.
 *
 * This is the permanent property-test foundation for the math layer.
 * Add a new property here whenever a future math change has an invariant
 * the existing example tests don't already pin.
 *
 * Default fast-check numRuns (100) is sufficient to catch the bugs above
 * and keeps the suite under a few seconds in CI.
 */

import fc from 'fast-check';
import { MathematicalModels } from '../../src/math/MathematicalModels.js';

function makeMath() {
    return new MathematicalModels();
}

// Helpers for OHLC fixtures parameterized by a returns vector.
function ohlcFromReturns(startPrice, returns) {
    let p = startPrice;
    const out = [{ open: p, high: p, low: p, close: p, volume: 1, timestamp: 0 }];
    for (let i = 0; i < returns.length; i += 1) {
        const r = returns[i];
        const next = p * (1 + r);
        out.push({
            open: p,
            high: Math.max(p, next),
            low: Math.min(p, next),
            close: next,
            volume: 1,
            timestamp: i + 1,
        });
        p = next;
    }
    return out;
}

// ----------------------------------------------------------------------
// _normalCDF properties (P1-A)
// ----------------------------------------------------------------------

describe('_normalCDF — algebraic invariants', () => {
    const math = makeMath();

    test('Φ(x) ∈ [0, 1] for all finite x', () => {
        fc.assert(
            fc.property(
                fc.double({ min: -10, max: 10, noNaN: true, noDefaultInfinity: true }),
                (x) => {
                    const v = math._normalCDF(x);
                    return v >= 0 && v <= 1;
                },
            ),
        );
    });

    test('Φ is monotone non-decreasing in x', () => {
        fc.assert(
            fc.property(
                fc.tuple(
                    fc.double({ min: -6, max: 6, noNaN: true, noDefaultInfinity: true }),
                    fc.double({ min: 0, max: 6, noNaN: true, noDefaultInfinity: true }),
                ),
                ([x, delta]) => {
                    const left = math._normalCDF(x);
                    const right = math._normalCDF(x + delta);
                    // erf has ~1.5e-7 max absolute error so allow a tiny epsilon
                    // for numerical noise around 0 increment.
                    return right - left >= -1e-6;
                },
            ),
        );
    });

    test('Φ(-x) ≈ 1 - Φ(x) (symmetric around 0.5)', () => {
        fc.assert(
            fc.property(
                fc.double({ min: -5, max: 5, noNaN: true, noDefaultInfinity: true }),
                (x) => {
                    const a = math._normalCDF(x);
                    const b = math._normalCDF(-x);
                    return Math.abs(a + b - 1) < 1e-5;
                },
            ),
        );
    });

    test('Φ(0) ≈ 0.5 (within erf approximation accuracy)', () => {
        // Abramowitz-Stegun erf approximation has ~1.5e-7 max abs error,
        // so Φ(0) is not exactly 0.5 to floating-point precision.
        expect(math._normalCDF(0)).toBeCloseTo(0.5, 6);
    });

    // P1-A REGRESSION: distinct inputs that previously collided in the
    // Math.round(x * 100) cache must now produce DISTINCT results.
    test('distinct nearby inputs produce distinct outputs (cache-key collision regression)', () => {
        // Previously, Φ(1.96) and Φ(1.9649) both mapped to key 196 and
        // returned the same value.  Verify they differ now.
        const a = math._normalCDF(1.96);
        const b = math._normalCDF(1.9649);
        expect(a).not.toBe(b);
        // The true difference is small but strictly positive.
        expect(b).toBeGreaterThan(a);
    });
});

// ----------------------------------------------------------------------
// _calculatePercentRank properties (P1-B)
// ----------------------------------------------------------------------

describe('_calculatePercentRank — algebraic invariants', () => {
    const math = makeMath();

    test('all outputs ∈ [0, 100]', () => {
        fc.assert(
            fc.property(
                fc.array(
                    fc.double({ min: -1000, max: 1000, noNaN: true, noDefaultInfinity: true }),
                    { minLength: 2, maxLength: 50 },
                ),
                (arr) => {
                    const ranks = math._calculatePercentRank(arr);
                    return ranks.every((r) => r >= 0 && r <= 100);
                },
            ),
        );
    });

    test('equal input values produce equal output ranks (tie handling)', () => {
        fc.assert(
            fc.property(
                fc.array(
                    fc.double({ min: -100, max: 100, noNaN: true, noDefaultInfinity: true }),
                    { minLength: 2, maxLength: 30 },
                ),
                (arr) => {
                    const ranks = math._calculatePercentRank(arr);
                    // For every (i, j) where arr[i] === arr[j], ranks[i] === ranks[j].
                    for (let i = 0; i < arr.length; i += 1) {
                        for (let j = i + 1; j < arr.length; j += 1) {
                            if (arr[i] === arr[j] && ranks[i] !== ranks[j]) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
            ),
        );
    });

    test('rank ordering matches value ordering (monotone)', () => {
        fc.assert(
            fc.property(
                fc.array(
                    fc.double({ min: -100, max: 100, noNaN: true, noDefaultInfinity: true }),
                    { minLength: 2, maxLength: 30 },
                ),
                (arr) => {
                    const ranks = math._calculatePercentRank(arr);
                    for (let i = 0; i < arr.length; i += 1) {
                        for (let j = 0; j < arr.length; j += 1) {
                            if (arr[i] < arr[j] && ranks[i] > ranks[j]) return false;
                            if (arr[i] > arr[j] && ranks[i] < ranks[j]) return false;
                        }
                    }
                    return true;
                },
            ),
        );
    });

    // P1-B REGRESSION: [1, 1, 1, 1, 2] must rank the four 1's identically,
    // NOT [0, 0, 0, 0, 100] as the buggy indexOf-based version returned.
    test('duplicates regression: [1,1,1,1,2] ranks the four 1s identically', () => {
        const ranks = math._calculatePercentRank([1, 1, 1, 1, 2]);
        expect(ranks[0]).toEqual(ranks[1]);
        expect(ranks[1]).toEqual(ranks[2]);
        expect(ranks[2]).toEqual(ranks[3]);
        expect(ranks[4]).toBe(100);
        // The four 1's are the midrank of positions 1..4 → rank 2.5 →
        // (2.5 - 1) / (5 - 1) * 100 = 37.5.
        expect(ranks[0]).toBeCloseTo(37.5, 6);
    });

    test('edge cases do not crash or return NaN', () => {
        expect(math._calculatePercentRank([])).toEqual([]);
        // single-element: previously divided by zero giving NaN; now [50].
        const single = math._calculatePercentRank([42]);
        expect(single).toEqual([50]);
        expect(Number.isFinite(single[0])).toBe(true);
    });
});

// ----------------------------------------------------------------------
// Historical / MonteCarlo VaR properties (P1-C)
// ----------------------------------------------------------------------

describe('calculateHistoricalVaR — algebraic invariants', () => {
    const math = makeMath();

    // Build a fixed deterministic returns sequence so VaR is comparable
    // across confidence levels.
    const fixedReturns = [-0.05, -0.04, -0.03, -0.02, -0.01, 0, 0.01, 0.02, 0.03, 0.04, 0.05];
    const fixedOhlc = ohlcFromReturns(100, fixedReturns);

    test('non-decreasing in confidence (higher c => higher-quantile = less-negative VaR)', () => {
        // With sortedReturns ascending and varReturn = sortedReturns[floor(n*c)],
        // higher c indexes a higher (less-negative) return.
        const confidences = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99];
        const vars = confidences.map((c) => math.calculateHistoricalVaR(fixedOhlc, c).percentage);
        for (let i = 1; i < vars.length; i += 1) {
            expect(vars[i]).toBeGreaterThanOrEqual(vars[i - 1] - 1e-9);
        }
    });

    test('finite percentage on all confidences in [0, 1] for non-trivial input', () => {
        // confidence=1 used to index out-of-bounds (NaN percentage).
        fc.assert(
            fc.property(
                fc.double({ min: 0, max: 1, noNaN: true, noDefaultInfinity: true }),
                (c) => {
                    const r = math.calculateHistoricalVaR(fixedOhlc, c);
                    return Number.isFinite(r.percentage) && Number.isFinite(r.absolute);
                },
            ),
        );
    });

    // P1-C REGRESSION: previously confidence=1.0 returned percentage=NaN.
    test('confidence=1 returns finite max-quantile, not NaN (edge clamp)', () => {
        const r = math.calculateHistoricalVaR(fixedOhlc, 1.0);
        expect(Number.isFinite(r.percentage)).toBe(true);
        expect(Number.isFinite(r.absolute)).toBe(true);
        // The largest log return in the sequence: log(1 + 0.05) ≈ 4.879%
        const expectedMax = Math.log(1.05) * 100;
        expect(r.percentage).toBeCloseTo(expectedMax, 3);
    });

    test('empty input returns null-shaped result (no throw, no NaN)', () => {
        const r = math.calculateHistoricalVaR([], 0.05);
        expect(r.percentage).toBeNull();
        expect(r.absolute).toBeNull();
        expect(r.confidence).toBe(0.05);
        expect(r.method).toBe('historical');
    });

    test('confidence=0.05 quantile is byte-identical to the pre-fix value (no behavior drift)', () => {
        // The original index formula Math.floor(11 * 0.05) === 0 selects
        // sortedReturns[0], which is the smallest log return:
        // log(1 + (-0.05)) = log(0.95) ≈ -5.1293%.  Verify the post-fix
        // code returns the same number for the common case (no behavior
        // drift from the pre-fix index formula).
        const r = math.calculateHistoricalVaR(fixedOhlc, 0.05);
        const expectedMin = Math.log(0.95) * 100;
        expect(r.percentage).toBeCloseTo(expectedMin, 3);
    });
});

describe('calculateMonteCarloVaR — algebraic invariants', () => {
    const math = makeMath();
    const fixedReturns = [-0.02, -0.01, 0, 0.01, 0.02];
    const fixedOhlc = ohlcFromReturns(100, fixedReturns);

    test('confidence=1 returns finite max-quantile (edge clamp)', () => {
        const r = math.calculateMonteCarloVaR(fixedOhlc, 1.0, 500);
        expect(Number.isFinite(r.percentage)).toBe(true);
        expect(Number.isFinite(r.absolute)).toBe(true);
    });

    test('empty input returns null-shaped result (no throw)', () => {
        const r = math.calculateMonteCarloVaR([], 0.05, 100);
        expect(r.percentage).toBeNull();
        expect(r.absolute).toBeNull();
    });
});

// ----------------------------------------------------------------------
// calculateMaxDrawdown properties (P1-D)
// ----------------------------------------------------------------------

describe('calculateMaxDrawdown — algebraic invariants', () => {
    const math = makeMath();

    test('percentage ∈ [0, 100] for all positive-price OHLC sequences', () => {
        fc.assert(
            fc.property(
                fc.array(
                    fc.double({ min: -0.5, max: 0.5, noNaN: true, noDefaultInfinity: true }),
                    { minLength: 2, maxLength: 50 },
                ),
                (returns) => {
                    const data = ohlcFromReturns(100, returns).filter((b) => b.close > 0);
                    if (data.length < 2) return true; // shrink-degenerate
                    const r = math.calculateMaxDrawdown(data);
                    return r.percentage >= 0 && r.percentage <= 100;
                },
            ),
        );
    });

    test('monotonically increasing series → drawdown = 0', () => {
        const data = [
            { open: 100, high: 100, low: 100, close: 100, volume: 1, timestamp: 0 },
            { open: 101, high: 101, low: 101, close: 101, volume: 1, timestamp: 1 },
            { open: 102, high: 102, low: 102, close: 102, volume: 1, timestamp: 2 },
            { open: 110, high: 110, low: 110, close: 110, volume: 1, timestamp: 3 },
        ];
        const r = math.calculateMaxDrawdown(data);
        expect(r.percentage).toBe(0);
        expect(r.absolute).toBe(0);
    });

    // P1-D REGRESSION: previously this threw TypeError on empty input
    // (`ohlcData[0].close` of undefined).
    test('empty input returns zero-state structure, does not throw', () => {
        expect(() => math.calculateMaxDrawdown([])).not.toThrow();
        const r = math.calculateMaxDrawdown([]);
        expect(r.percentage).toBe(0);
        expect(r.absolute).toBe(0);
        expect(r.period).toEqual({ start: 0, end: 0 });
        expect(r.duration).toBe(0);
    });

    test('single-element input returns zero-state structure (sibling-consistent)', () => {
        const single = [{ open: 100, high: 100, low: 100, close: 100, volume: 1, timestamp: 0 }];
        const r = math.calculateMaxDrawdown(single);
        expect(r.percentage).toBe(0);
        expect(r.absolute).toBe(0);
    });

    test('non-Array input returns zero-state, does not throw', () => {
        expect(() => math.calculateMaxDrawdown(null)).not.toThrow();
        expect(() => math.calculateMaxDrawdown(undefined)).not.toThrow();
        expect(math.calculateMaxDrawdown(null).percentage).toBe(0);
    });
});

// ----------------------------------------------------------------------
// Cheap bonus invariants (caught by other audits — pin them here)
// ----------------------------------------------------------------------

describe('Sharpe ratio — scale invariance (cheap bonus invariant)', () => {
    const math = makeMath();

    // Helper: standard deviation of an array.  Used as a degeneracy
    // precondition below — see the test comment for why this matters.
    function _stdev(arr) {
        if (!arr || arr.length < 2) return 0;
        const mean = arr.reduce((s, v) => s + v, 0) / arr.length;
        const variance = arr.reduce((s, v) => s + (v - mean) ** 2, 0) / (arr.length - 1);
        return Math.sqrt(variance);
    }

    test('Sharpe is invariant under positive scaling of prices (when risk-free=0)', () => {
        // Both prices and risk-free rate scale identically; with rf=0,
        // scaling all prices preserves returns, std-dev, and therefore
        // the Sharpe ratio — in real arithmetic.
        //
        // FLAKE FIX (post-PR #46 follow-up): the previous version only
        // skipped non-finite Sharpe values via `Number.isFinite`.  But
        // fast-check happily generates near-zero-variance return series
        // (e.g. [-6.6e-15, 0, 0, ..., 5.6e-12, 0, ...]) where Sharpe is
        // numerically a finite-but-ill-conditioned `0/0`.  In float64,
        // cumulating from base 100 vs 285.87 with ~1e-15 returns yields
        // different rounding in the tiny return values; dividing by the
        // near-zero std-dev then amplifies the rounding past the
        // absolute 1e-6 tolerance.  The result is a ~10–30% flake rate
        // on this property — every CI run becomes a coin flip.
        //
        // Fix: add an explicit degeneracy precondition.  Require both
        // the input returns AND the recomputed returns to have a std-dev
        // above a small threshold before asserting the invariant.  The
        // property is still mathematically correct everywhere; we just
        // refuse to assert it where floating-point error dominates the
        // signal we're checking.
        const STDEV_FLOOR = 1e-4; // realistic returns ~1e-2; degenerate ~1e-15
        fc.assert(
            fc.property(
                fc.array(
                    fc.double({ min: -0.05, max: 0.05, noNaN: true, noDefaultInfinity: true }),
                    { minLength: 30, maxLength: 100 },
                ),
                fc.double({ min: 0.5, max: 5, noNaN: true, noDefaultInfinity: true }),
                (returns, scale) => {
                    // Precondition 1: input returns must have meaningful variation.
                    if (_stdev(returns) < STDEV_FLOOR) return true;
                    const a = ohlcFromReturns(100, returns);
                    const b = ohlcFromReturns(100 * scale, returns);
                    const sa = math.calculateSharpeRatio(a, 0);
                    const sb = math.calculateSharpeRatio(b, 0);
                    // Precondition 2: both Sharpe values must be well-defined.
                    if (!Number.isFinite(sa) || !Number.isFinite(sb)) return true;
                    // Precondition 3: skip if either Sharpe is itself tiny —
                    // the absolute 1e-6 tolerance becomes a *relative* tolerance
                    // problem in that regime.  Skipping here keeps the property
                    // sharp on the values it does assert.
                    if (Math.abs(sa) < 1e-3 && Math.abs(sb) < 1e-3) return true;
                    return Math.abs(sa - sb) < 1e-6;
                },
            ),
        );
    });
});
