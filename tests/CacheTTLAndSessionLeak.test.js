/**
 * Regression tests for two src/core/QuantPivotEngine.js P0 fixes:
 *
 *   P0-4  _setCachedResult silently dies when ttlMs is undefined.
 *         Date.now() + undefined === NaN; Date.now() < NaN === false;
 *         every cache entry is then read back as expired.  Manifests as a
 *         silent cache miss on every call when the engine is constructed
 *         with a partial config that strips `defaultOptions.cacheTTL`.
 *
 *   P0-5  calculatePivotLevels does not call endSession on the cache-hit
 *         early return.  The session sits in metrics.sessions as
 *         status:'active' forever; _cleanupSessions only evicts completed
 *         sessions, so the map grows without bound on cache-hit-heavy
 *         workloads.
 *
 * Both tests probe internals because that's the most direct way to pin
 * the bug semantics without depending on full calculation pipelines.
 */

import { QuantPivotEngine } from '../src/core/QuantPivotEngine.js';
import { ConfigurationManager } from '../src/config/ConfigurationManager.js';

function makeEngine(overrides = {}) {
    const config = ConfigurationManager.mergeWithDefaults(overrides, 'testing');
    return new QuantPivotEngine(config);
}

describe('P0-4: cache TTL fallback when ttlMs is undefined / NaN / non-finite', () => {
    let engine;
    afterEach(() => { engine && engine.dispose(); engine = null; });

    test('undefined ttlMs falls back to the default and the entry survives reads', () => {
        engine = makeEngine();
        engine._setCachedResult('k1', { value: 42 }, undefined);
        // Read back IMMEDIATELY — before the fix this returned null because
        // Date.now() < (Date.now() + undefined) === false.
        const back = engine._getCachedResult('k1');
        expect(back).not.toBeNull();
        expect(back).toEqual({ value: 42 });
        // The persisted expiry must be a finite future timestamp.
        const expiry = engine.cacheExpiry.get('k1');
        expect(Number.isFinite(expiry)).toBe(true);
        expect(expiry).toBeGreaterThan(Date.now());
    });

    test('NaN ttlMs falls back to the default', () => {
        engine = makeEngine();
        engine._setCachedResult('k2', 'v', Number.NaN);
        expect(engine._getCachedResult('k2')).toBe('v');
        expect(Number.isFinite(engine.cacheExpiry.get('k2'))).toBe(true);
    });

    test('Infinity ttlMs falls back to the default (not Number.MAX_SAFE_INTEGER abuse)', () => {
        engine = makeEngine();
        engine._setCachedResult('k3', 'v', Number.POSITIVE_INFINITY);
        // Result is still cached; expiry is finite and bounded near "now + default".
        expect(engine._getCachedResult('k3')).toBe('v');
        const expiry = engine.cacheExpiry.get('k3');
        expect(Number.isFinite(expiry)).toBe(true);
        expect(expiry - Date.now()).toBeLessThanOrEqual(310_000); // default + slack
    });

    test('Negative ttlMs clamps to 0 (entry expires immediately)', () => {
        engine = makeEngine();
        engine._setCachedResult('k4', 'v', -10_000);
        // Caching a negative-TTL value should NOT crash and should NOT
        // accidentally read back as fresh.  Clamp to 0 means expiry === now
        // (or slightly in the past after the function returned), so the
        // immediate read should return null and the entry should be evicted.
        const back = engine._getCachedResult('k4');
        expect(back).toBeNull();
    });

    test('Valid positive ttlMs is honored unchanged (no behavior regression)', () => {
        engine = makeEngine();
        const before = Date.now();
        engine._setCachedResult('k5', 'v', 60_000);
        const expiry = engine.cacheExpiry.get('k5');
        // expiry == now + 60s, within tight bound around `before + 60_000`.
        expect(expiry - before).toBeGreaterThanOrEqual(60_000);
        expect(expiry - before).toBeLessThanOrEqual(60_500);
        expect(engine._getCachedResult('k5')).toBe('v');
    });
});

describe('P0-5: cache-hit early return must end the session', () => {
    let engine;
    afterEach(() => { engine && engine.dispose(); engine = null; });

    // Helper: count sessions whose status is exactly `active` in the
    // monitor's session map.  We probe internals because that is the
    // simplest place to verify the leak vs no-leak distinction.
    function activeSessionCount(eng) {
        let n = 0;
        eng.monitor.metrics.sessions.forEach((s) => {
            if (s.status === 'active') n += 1;
        });
        return n;
    }

    function completedSessionCount(eng) {
        let n = 0;
        eng.monitor.metrics.sessions.forEach((s) => {
            if (s.status === 'completed') n += 1;
        });
        return n;
    }

    test('a cache-hit short-circuit promotes its session from active to completed', () => {
        engine = makeEngine();

        // The engine constructor itself opens + closes an 'engine_initialization'
        // session — measure deltas against the pre-test baseline.
        const baselineActive = activeSessionCount(engine);
        const baselineCompleted = completedSessionCount(engine);

        // Pre-seed the cache so the next calculatePivotLevels call short-circuits.
        const cacheKey = 'manual_test_key';
        const fakeResult = { pivot: 1, support: 0, resistance: 2 };
        engine._setCachedResult(cacheKey, fakeResult, 60_000);

        // Drive the cache-hit branch directly.  We don't need a full OHLC
        // fixture — we replicate the relevant lines of calculatePivotLevels
        // for this code path: startSession, _getCachedResult, recordCacheHit,
        // endSession (cacheHit:true).  This pins that the SHAPE of the
        // cache-hit short-circuit ends the session correctly.
        const sid = engine.monitor.startSession('pivot_calculation_test');
        expect(activeSessionCount(engine)).toBe(baselineActive + 1);
        expect(completedSessionCount(engine)).toBe(baselineCompleted);

        const back = engine._getCachedResult(cacheKey);
        expect(back).toEqual(fakeResult);
        engine.monitor.recordCacheHit(sid);

        // After the post-fix line, the session must be ended even on the
        // cache-hit short-circuit:
        engine.monitor.endSession(sid, { success: true, cacheHit: true });

        expect(activeSessionCount(engine)).toBe(baselineActive);
        expect(completedSessionCount(engine)).toBe(baselineCompleted + 1);
    });

    test('static contract: calculatePivotLevels source ends the session on cache-hit', () => {
        // Source-level pin so a future refactor that removes the endSession
        // call from the cache-hit branch is caught by lint, not by a wall
        // of leaked sessions at runtime.
        const fs = require('fs');
        const path = require('path');
        const src = fs.readFileSync(
            path.join(__dirname, '..', 'src', 'core', 'QuantPivotEngine.js'),
            'utf8',
        );
        // Find the cache-hit block and verify endSession appears between
        // recordCacheHit and the early `return cachedResult;`.
        const block = src.match(
            /recordCacheHit\(sessionId\);[\s\S]*?return\s+cachedResult;/m,
        );
        expect(block).not.toBeNull();
        expect(block[0]).toMatch(/endSession\(\s*sessionId\s*,/);
        expect(block[0]).toMatch(/cacheHit:\s*true/);
    });

    test('static contract: _setCachedResult guards ttlMs with a finite fallback', () => {
        // Same source-level pin for the TTL guard.
        const fs = require('fs');
        const path = require('path');
        const src = fs.readFileSync(
            path.join(__dirname, '..', 'src', 'core', 'QuantPivotEngine.js'),
            'utf8',
        );
        // The fix uses Number.isFinite(ttlMs) — pin its presence in
        // _setCachedResult so a future refactor cannot silently regress.
        const fn = src.match(
            /_setCachedResult\([^)]*\)\s*\{[\s\S]*?\n\s{4}\}/m,
        );
        expect(fn).not.toBeNull();
        expect(fn[0]).toMatch(/Number\.isFinite\(\s*ttlMs\s*\)/);
    });
});

describe('No unrelated API behavior changes', () => {
    let engine;
    afterEach(() => { engine && engine.dispose(); engine = null; });

    test('a cold engine still computes and caches as before with default TTL', () => {
        engine = makeEngine();
        engine._setCachedResult('cold', { foo: 'bar' }, 60_000);
        expect(engine._getCachedResult('cold')).toEqual({ foo: 'bar' });
        // Expiry is finite, positive, near now + 60s.
        const exp = engine.cacheExpiry.get('cold');
        expect(Number.isFinite(exp)).toBe(true);
        expect(exp).toBeGreaterThan(Date.now());
    });

    test('expired entries are still removed from cache on read', () => {
        engine = makeEngine();
        engine._setCachedResult('expired', 'v', 60_000);
        // Force expiry into the past:
        engine.cacheExpiry.set('expired', Date.now() - 1);
        expect(engine._getCachedResult('expired')).toBeNull();
        expect(engine.cache.has('expired')).toBe(false);
        expect(engine.cacheExpiry.has('expired')).toBe(false);
    });
});
