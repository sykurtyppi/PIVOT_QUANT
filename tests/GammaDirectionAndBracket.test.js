/**
 * Regression tests for the two src/ P0 fixes:
 *
 *   P0-1  src/math/CalibratedSPYEngine.js:129
 *         Direction convention was inverted — engine returned 'bullish' when
 *         price was below the 21-day EMA, opposite of GammaFlipEngine.
 *
 *   P0-2  src/math/GammaFlipEngine.js:96
 *         _calculateDealerGamma had `+ emaDistortion` on both `high` and
 *         `low` branches, shifting the entire bracket up by emaDistortion
 *         rather than widening it symmetrically around `price`.
 *
 * These tests pin:
 *   1. Both engines AGREE on direction for price above/below ema21d.
 *   2. Dealer-gamma `high`/`low` are symmetrically centered on `price` when
 *      EMA distance is nonzero.
 *   3. The fixes don't change unrelated parts of the engine outputs.
 */

import { GammaFlipEngine } from '../src/math/GammaFlipEngine.js';
import { CalibratedSPYEngine } from '../src/math/CalibratedSPYEngine.js';

describe('Gamma direction convention (P0-1)', () => {
    const cases = [
        { label: 'price above ema21d', price: 680, ema21d: 670, expected: 'bullish' },
        { label: 'price below ema21d', price: 660, ema21d: 670, expected: 'bearish' },
        { label: 'price equal to ema21d', price: 670, ema21d: 670, expected: 'bearish' }, // strict > convention
    ];

    test.each(cases)(
        'GammaFlipEngine + CalibratedSPYEngine AGREE: $label -> $expected',
        ({ price, ema21d, expected }) => {
            // GammaFlipEngine consumes a flat emaLevels object.
            const flip = new GammaFlipEngine();
            const flipResult = flip._calculateDealerGamma(price, {
                ema21d,
                ema9d: ema21d,
                ema50d: ema21d,
                ema9w: ema21d,
            });

            // CalibratedSPYEngine has hardcoded targetLevels; mutate the two
            // fields the direction decision reads so we can drive both cases.
            const calib = new CalibratedSPYEngine();
            calib.targetLevels.currentPrice = price;
            calib.targetLevels.ema21d = ema21d;
            const calibResult = calib.calculateAnalysis(null);

            expect(flipResult.direction).toBe(expected);
            expect(calibResult.gammaFlip.direction).toBe(expected);
            // Most important: the two engines no longer disagree on the
            // same inputs.
            expect(calibResult.gammaFlip.direction).toBe(flipResult.direction);
        }
    );

    test('regression scenario — exact audit numbers', () => {
        // From the audit: price=670.62, ema21d=672.52 (price < ema21d).
        // Before the fix: Calibrated said 'bullish', GammaFlip said 'bearish'.
        // After the fix:  both say 'bearish'.
        const flip = new GammaFlipEngine();
        const flipResult = flip._calculateDealerGamma(670.62, {
            ema21d: 672.52,
            ema9d: 670,
            ema50d: 660,
            ema9w: 650,
        });

        const calib = new CalibratedSPYEngine(); // uses default 670.62 / 672.52
        const calibResult = calib.calculateAnalysis(null);

        expect(flipResult.direction).toBe('bearish');
        expect(calibResult.gammaFlip.direction).toBe('bearish');
    });
});

describe('Dealer-gamma bracket symmetry (P0-2)', () => {
    const sameEMAFamily = (ema21d) => ({
        ema21d,
        ema9d: ema21d,
        ema50d: ema21d,
        ema9w: ema21d,
    });

    test('bracket midpoint equals price exactly when ema21d > price', () => {
        const flip = new GammaFlipEngine();
        const price = 100;
        const result = flip._calculateDealerGamma(price, sameEMAFamily(120));
        const mid = (result.high + result.low) / 2;
        expect(mid).toBeCloseTo(price, 9);
        // And the bracket actually has width (not degenerate).
        expect(result.high).toBeGreaterThan(result.low);
    });

    test('bracket midpoint equals price exactly when ema21d < price', () => {
        const flip = new GammaFlipEngine();
        const price = 100;
        const result = flip._calculateDealerGamma(price, sameEMAFamily(80));
        const mid = (result.high + result.low) / 2;
        expect(mid).toBeCloseTo(price, 9);
        expect(result.high).toBeGreaterThan(result.low);
    });

    test('bracket midpoint equals price when ema21d == price (no distortion)', () => {
        const flip = new GammaFlipEngine();
        const price = 100;
        const result = flip._calculateDealerGamma(price, sameEMAFamily(100));
        const mid = (result.high + result.low) / 2;
        expect(mid).toBeCloseTo(price, 9);
        expect(result.high - result.low).toBeCloseTo(2 * price * 0.002, 9);
    });

    test('half-width is exactly volatilityAdjustment + emaDistortion on both sides', () => {
        const flip = new GammaFlipEngine();
        const price = 100;
        const ema21d = 80; // |distortion| = 20 * 0.001 = 0.02
        const result = flip._calculateDealerGamma(price, sameEMAFamily(ema21d));
        const expectedHalfWidth = price * 0.002 + Math.abs(price - ema21d) * 0.001;
        expect(result.high - price).toBeCloseTo(expectedHalfWidth, 9);
        expect(price - result.low).toBeCloseTo(expectedHalfWidth, 9);
    });

    test('SPY-scale audit scenario — price=670.62, ema21d=672.52', () => {
        const flip = new GammaFlipEngine();
        const price = 670.62;
        const ema21d = 672.52;
        const result = flip._calculateDealerGamma(price, sameEMAFamily(ema21d));
        const mid = (result.high + result.low) / 2;
        expect(mid).toBeCloseTo(price, 9);
    });
});

describe('No unrelated API behavior changes', () => {
    test('GammaFlipEngine._calculateDealerGamma still returns the same shape', () => {
        const flip = new GammaFlipEngine();
        const result = flip._calculateDealerGamma(100, {
            ema21d: 100,
            ema9d: 100,
            ema50d: 100,
            ema9w: 100,
        });
        expect(result).toHaveProperty('high');
        expect(result).toHaveProperty('low');
        expect(result).toHaveProperty('strength');
        expect(result).toHaveProperty('direction');
        expect(typeof result.high).toBe('number');
        expect(typeof result.low).toBe('number');
        expect(typeof result.strength).toBe('number');
        expect(typeof result.direction).toBe('string');
    });

    test('CalibratedSPYEngine.calculateAnalysis returns the same top-level keys', () => {
        const calib = new CalibratedSPYEngine();
        const result = calib.calculateAnalysis(null);
        // Spot-check the structural keys downstream consumers rely on.
        expect(result).toHaveProperty('emaLevels');
        expect(result).toHaveProperty('gammaFlip');
        expect(result.gammaFlip).toHaveProperty('level');
        expect(result.gammaFlip).toHaveProperty('strength');
        expect(result.gammaFlip).toHaveProperty('direction');
        expect(result).toHaveProperty('reversalLevels');
        expect(Array.isArray(result.reversalLevels)).toBe(true);
        // The strength constant did not change.
        expect(result.gammaFlip.strength).toBe(0.75);
    });

    test('CalibratedSPYEngine default-target direction matches its hardcoded inputs', () => {
        // Default targetLevels: currentPrice=670.62, ema21d=672.52.
        // Post-fix: currentPrice < ema21d => 'bearish'.
        const calib = new CalibratedSPYEngine();
        expect(calib.targetLevels.currentPrice).toBeLessThan(calib.targetLevels.ema21d);
        const result = calib.calculateAnalysis(null);
        expect(result.gammaFlip.direction).toBe('bearish');
    });
});
