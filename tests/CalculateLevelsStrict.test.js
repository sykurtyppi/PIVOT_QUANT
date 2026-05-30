/**
 * Regression tests for src/index.js P0-3 fix:
 *
 *   QuantPivot.calculateLevels(ohlcData, method) previously returned
 *   `results.levels[method]` directly.  If the engine ever produced a
 *   result whose `levels` object lacked the requested method key (e.g.
 *   via a future default-method substitution path), the caller would
 *   receive `undefined` with no indication anything went wrong.  This
 *   PR makes the contract explicit: throw with a helpful message
 *   listing the available method keys.
 */

import { QuantPivot } from '../src/index.js';

function makeData(n = 60) {
    const out = [];
    for (let i = 0; i < n; i += 1) {
        const close = 100 + Math.sin(i / 5) * 2 + i * 0.05;
        const open = close - 0.2;
        const high = Math.max(open, close) + 0.5;
        const low = Math.min(open, close) - 0.5;
        out.push({ open, high, low, close, volume: 1_000_000, timestamp: i * 60_000 });
    }
    return out;
}

describe('calculateLevels — strict-on-missing-method contract (P0-3)', () => {
    let qp;
    afterEach(() => { qp && qp.dispose(); qp = null; });

    test('happy path: requested method returned successfully (no regression)', async () => {
        qp = new QuantPivot({}, 'testing');
        const data = makeData();
        const levels = await qp.calculateLevels(data, 'standard');
        // We don't pin the shape here (math layer owns it); just verify
        // that the call succeeded and produced a truthy result — the
        // ONLY thing the strict guard could have broken.
        expect(levels).toBeDefined();
        expect(levels).not.toBeNull();
    });

    test('strict guard: throws if engine result omits the requested method', async () => {
        qp = new QuantPivot({}, 'testing');
        const data = makeData();

        // Simulate the silent-drop scenario: monkey-patch calculate() to
        // return a results object whose `levels` object does NOT contain
        // the requested method (e.g. the engine substituted defaults).
        const originalCalculate = qp.calculate.bind(qp);
        qp.calculate = async () => {
            // Force a result that has `levels` but is missing 'demark':
            return {
                levels: {
                    standard: { pivot: 100, r1: 102, s1: 98 },
                    fibonacci: { pivot: 100, r1: 101.6, s1: 98.4 },
                    // 'demark' deliberately missing
                },
            };
        };

        await expect(qp.calculateLevels(data, 'demark')).rejects.toThrow(
            /requested method 'demark' was not produced/,
        );
        // Error message must list the available method keys to help the
        // caller debug.
        await expect(qp.calculateLevels(data, 'demark')).rejects.toThrow(/standard/);
        await expect(qp.calculateLevels(data, 'demark')).rejects.toThrow(/fibonacci/);

        qp.calculate = originalCalculate;
    });

    test('strict guard: throws if engine result has null/missing levels', async () => {
        qp = new QuantPivot({}, 'testing');
        const data = makeData();

        const originalCalculate = qp.calculate.bind(qp);
        qp.calculate = async () => ({ /* no `levels` at all */ });

        await expect(qp.calculateLevels(data, 'standard')).rejects.toThrow(
            /requested method 'standard' was not produced/,
        );
        // With no levels, the available-methods list should report (none).
        await expect(qp.calculateLevels(data, 'standard')).rejects.toThrow(/\(none\)/);

        qp.calculate = originalCalculate;
    });

    test('strict guard: returned-value undefined fails fast (no silent drop)', async () => {
        // Pin that the buggy behavior — returning `undefined` to the caller
        // — never occurs.  Either we get a real value or we throw.
        qp = new QuantPivot({}, 'testing');
        const data = makeData();

        const originalCalculate = qp.calculate.bind(qp);
        qp.calculate = async () => ({ levels: { standard: undefined } });

        await expect(qp.calculateLevels(data, 'standard')).rejects.toThrow(
            /requested method 'standard' was not produced/,
        );

        qp.calculate = originalCalculate;
    });
});

describe('No unrelated API behavior changes', () => {
    let qp;
    afterEach(() => { qp && qp.dispose(); qp = null; });

    test('default method (standard) still works without explicit argument', async () => {
        qp = new QuantPivot({}, 'testing');
        const data = makeData();
        // Default method is 'standard' — must succeed without an explicit arg.
        const levels = await qp.calculateLevels(data);
        expect(levels).toBeDefined();
        expect(levels).not.toBeNull();
    });
});
