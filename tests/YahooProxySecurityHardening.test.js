/**
 * Behavior tests for the yahoo_proxy.js security-hardening PR.
 *
 *   P0-1 safeNextPath           — block protocol-relative / backslash open redirects.
 *   P1-A validateYahooRange/Interval — allowlist Yahoo enum params at the boundary.
 *   P1-C isSameOriginPost       — defense-in-depth CSRF guard for write endpoints.
 *   P1-H fetchJson redirect cap — bounded hops + same-host invariant (SSRF guard).
 *
 * server/yahoo_proxy.js starts a real http.createServer().listen() at import
 * time, so we cannot `import` it directly in a unit test.  Instead we
 * extract each helper-function definition by source-slicing and evaluate
 * it in a fresh vm.runInNewContext sandbox.  This tests REAL helper code
 * with zero risk of binding an HTTP port during the test run.
 */

// Use CommonJS-style requires inside the file body to match the pattern
// already used by tests/CacheTTLAndSessionLeak.test.js and friends —
// Jest's babel-jest transform provides __dirname automatically.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const PROXY_SRC = fs.readFileSync(
    path.join(__dirname, '..', 'server', 'yahoo_proxy.js'),
    'utf8',
);

// Extract a top-level `function name(...)` declaration with brace-counting.
function extractFunctionSource(name) {
    const re = new RegExp(`\\nfunction ${name}\\b\\s*\\(`, 'g');
    const match = re.exec(PROXY_SRC);
    if (!match) throw new Error(`function ${name} not found in yahoo_proxy.js`);
    // Find the opening brace of the function body.
    let i = re.lastIndex;
    while (i < PROXY_SRC.length && PROXY_SRC[i] !== '{') i += 1;
    if (i >= PROXY_SRC.length) throw new Error(`opening brace for ${name} not found`);
    // Brace-count forward to find the matching closing brace.  Naive enough
    // for the helpers in this file (no strings or template literals contain
    // unbalanced braces); good enough for tests.
    let depth = 0;
    let j = i;
    while (j < PROXY_SRC.length) {
        const ch = PROXY_SRC[j];
        if (ch === '{') depth += 1;
        else if (ch === '}') {
            depth -= 1;
            if (depth === 0) {
                j += 1;
                break;
            }
        }
        j += 1;
    }
    return PROXY_SRC.slice(match.index + 1, j); // skip leading newline
}

// Helpers we want testable.  These four are pure (URL is the only external).
const HELPER_NAMES = [
    'safeNextPath',
    'validateYahooRange',
    'validateYahooInterval',
    'isSameOriginPost',
];

function loadHelpers() {
    const bodies = HELPER_NAMES.map(extractFunctionSource).join('\n\n');
    // Also pull the allowlist sets the validators reference.
    const setsRegion = PROXY_SRC.match(
        /const ALLOWED_YAHOO_RANGES[\s\S]*?const ALLOWED_YAHOO_INTERVALS = new Set\(\[[\s\S]*?\]\);/,
    );
    if (!setsRegion) throw new Error('ALLOWED_YAHOO_* sets not found');
    const ctx = { URL, console };
    vm.createContext(ctx);
    vm.runInContext(`${setsRegion[0]}\n${bodies}\nthis.__helpers = { ${HELPER_NAMES.join(', ')} };`, ctx);
    return ctx.__helpers;
}

const helpers = loadHelpers();

describe('P0-1: safeNextPath blocks open-redirect vectors', () => {
    const { safeNextPath } = helpers;

    test('protocol-relative //evil.com is reduced to /', () => {
        expect(safeNextPath('//evil.com')).toBe('/');
        expect(safeNextPath('//evil.com/path?q=1')).toBe('/');
    });
    test('backslash escape /\\evil.com is reduced to /', () => {
        expect(safeNextPath('/\\evil.com')).toBe('/');
        expect(safeNextPath('/\\evil.com/path')).toBe('/');
    });
    test('absolute https URL is reduced to /', () => {
        expect(safeNextPath('https://evil.com')).toBe('/');
        expect(safeNextPath('http://evil.com')).toBe('/');
    });
    test('empty / null / non-string is reduced to /', () => {
        expect(safeNextPath('')).toBe('/');
        expect(safeNextPath(null)).toBe('/');
        expect(safeNextPath(undefined)).toBe('/');
        expect(safeNextPath(42)).toBe('/');
    });
    test('schema-less without leading slash is reduced to /', () => {
        expect(safeNextPath('evil.com')).toBe('/');
        expect(safeNextPath('dashboard')).toBe('/');
    });
    test('legitimate site-relative paths pass through unchanged', () => {
        expect(safeNextPath('/dashboard')).toBe('/dashboard');
        expect(safeNextPath('/api/whatever?x=1')).toBe('/api/whatever?x=1');
        expect(safeNextPath('/')).toBe('/');
    });
});

describe('P1-A: range/interval allowlist', () => {
    const { validateYahooRange, validateYahooInterval } = helpers;

    test('valid Yahoo ranges accepted', () => {
        for (const r of ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', 'ytd', 'max']) {
            expect(validateYahooRange(r)).toBe(r);
        }
    });
    test('valid Yahoo intervals accepted', () => {
        for (const iv of ['1m', '5m', '15m', '30m', '60m', '1h', '1d', '1wk', '1mo']) {
            expect(validateYahooInterval(iv)).toBe(iv);
        }
    });
    test('range rejects unknown / injection strings, returns fallback', () => {
        expect(validateYahooRange('foo')).toBe('3mo');
        expect(validateYahooRange('3mo&evil=1')).toBe('3mo'); // injection attempt
        expect(validateYahooRange('3mo;rm -rf')).toBe('3mo');
        expect(validateYahooRange('3mo\r\nX-Injected: 1')).toBe('3mo');
        expect(validateYahooRange('')).toBe('3mo');
        expect(validateYahooRange(null)).toBe('3mo');
        expect(validateYahooRange(undefined)).toBe('3mo');
    });
    test('interval rejects unknown / injection strings, returns fallback', () => {
        expect(validateYahooInterval('foo')).toBe('1d');
        expect(validateYahooInterval('1d&evil=1')).toBe('1d');
        expect(validateYahooInterval('1d;rm -rf')).toBe('1d');
        expect(validateYahooInterval('')).toBe('1d');
        expect(validateYahooInterval(null)).toBe('1d');
    });
    test('custom fallback honored', () => {
        expect(validateYahooRange('foo', '1y')).toBe('1y');
        expect(validateYahooInterval('foo', '5m')).toBe('5m');
    });
});

describe('P1-C: isSameOriginPost (CSRF defense-in-depth)', () => {
    const { isSameOriginPost } = helpers;

    function makeReq(headers) {
        return { headers: Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v])) };
    }

    test('matching Origin header allowed', () => {
        const req = makeReq({ host: 'app.example:3000', origin: 'https://app.example:3000' });
        expect(isSameOriginPost(req)).toBe(true);
    });
    test('mismatched Origin header blocked', () => {
        const req = makeReq({ host: 'app.example:3000', origin: 'https://evil.com' });
        expect(isSameOriginPost(req)).toBe(false);
    });
    test('matching Referer header allowed (when no Origin)', () => {
        const req = makeReq({ host: 'app.example:3000', referer: 'https://app.example:3000/dashboard' });
        expect(isSameOriginPost(req)).toBe(true);
    });
    test('mismatched Referer header blocked (when no Origin)', () => {
        const req = makeReq({ host: 'app.example:3000', referer: 'https://evil.com/path' });
        expect(isSameOriginPost(req)).toBe(false);
    });
    test('Origin overrides Referer (only Origin matters when both present)', () => {
        const req = makeReq({
            host: 'app.example:3000',
            origin: 'https://app.example:3000',
            referer: 'https://evil.com/anything',
        });
        expect(isSameOriginPost(req)).toBe(true);
        const req2 = makeReq({
            host: 'app.example:3000',
            origin: 'https://evil.com',
            referer: 'https://app.example:3000/legit',
        });
        expect(isSameOriginPost(req2)).toBe(false);
    });
    test('"null" Origin (file:// / sandboxed iframe) is treated as absent', () => {
        // When Origin is the literal string 'null', we ignore it and fall
        // back to Referer.  No Referer + null Origin => allow (assumed
        // server-to-server caller).
        const req = makeReq({ host: 'app.example:3000', origin: 'null' });
        expect(isSameOriginPost(req)).toBe(true);
    });
    test('no Origin AND no Referer → allow (server-to-server caller)', () => {
        const req = makeReq({ host: 'app.example:3000' });
        expect(isSameOriginPost(req)).toBe(true);
    });
    test('malformed Origin is blocked', () => {
        const req = makeReq({ host: 'app.example:3000', origin: 'not a URL' });
        expect(isSameOriginPost(req)).toBe(false);
    });
    test('host case-insensitive', () => {
        const req = makeReq({ host: 'App.Example:3000', origin: 'https://app.example:3000' });
        expect(isSameOriginPost(req)).toBe(true);
    });
    test('host with port matches exactly (no port stripping)', () => {
        const req = makeReq({ host: 'app.example:3000', origin: 'https://app.example:8443' });
        expect(isSameOriginPost(req)).toBe(false);
    });
    test('missing Host header → allow (cannot compare)', () => {
        const req = { headers: { origin: 'https://anywhere' } };
        expect(isSameOriginPost(req)).toBe(true);
    });
});

describe('P1-H: fetchJson redirect bounds + same-host invariant', () => {
    // fetchJson uses https.get; rather than spin up a real TLS server we
    // assert the FIX is wired into the production source — a focused
    // source-pin so that any future refactor that drops the cap or the
    // host check fails this test immediately.  The behavior of the bound
    // itself (a numeric `redirectsLeft` counter that decrements toward 0)
    // is unit-testable purely from source-shape.
    test('FETCH_JSON_MAX_REDIRECTS constant defined to a small positive number', () => {
        const m = PROXY_SRC.match(/const FETCH_JSON_MAX_REDIRECTS\s*=\s*(\d+)\s*;/);
        expect(m).not.toBeNull();
        const n = Number(m[1]);
        expect(Number.isInteger(n)).toBe(true);
        expect(n).toBeGreaterThan(0);
        expect(n).toBeLessThanOrEqual(5); // hardening cap; today's value is 3
    });

    test('fetchJson signature takes a redirectsLeft parameter defaulted to the constant', () => {
        const m = PROXY_SRC.match(
            /function fetchJson\s*\(\s*url\s*,\s*redirectsLeft\s*=\s*FETCH_JSON_MAX_REDIRECTS\s*\)/,
        );
        expect(m).not.toBeNull();
    });

    test('redirect handler rejects when redirectsLeft <= 0', () => {
        expect(PROXY_SRC).toMatch(/if \(redirectsLeft <= 0\)/);
        expect(PROXY_SRC).toMatch(/Too many redirects/);
    });

    test('redirect handler enforces same-host invariant before recursing', () => {
        expect(PROXY_SRC).toMatch(/target\.host\.toLowerCase\(\)\s*!==\s*originHost/);
        expect(PROXY_SRC).toMatch(/Cross-host redirect blocked/);
    });

    test('recursive call decrements redirectsLeft', () => {
        expect(PROXY_SRC).toMatch(/fetchJson\(target\.toString\(\),\s*redirectsLeft\s*-\s*1\)/);
    });
});

describe('Production wiring: fixes are applied at the right call sites', () => {
    // Defense in depth: each fix is unit-tested above (P0-1, P1-A, P1-C) or
    // structurally-pinned (P1-H), but we also assert that the production
    // code actually USES the helpers at the right call sites.  This is the
    // belt-and-suspenders cousin of the unit tests — if a refactor drops a
    // call site, this test fails fast.

    test('P0-1: safeNextPath is called at both /auth/login entry points', () => {
        // GET branch
        expect(PROXY_SRC).toMatch(
            /const nextPath = safeNextPath\(url\.searchParams\.get\('next'\)\);/,
        );
        // POST branch
        expect(PROXY_SRC).toMatch(/const nextPath = safeNextPath\(form\.next\);/);
        // And the legacy buggy check is GONE.
        expect(PROXY_SRC).not.toMatch(
            /nextPath\s*=\s*form\.next\s*&&\s*form\.next\.startsWith\('\/'\)/,
        );
    });

    test('P1-A: validateYahooRange/Interval called at /api/market and /api/ib/market', () => {
        // /api/market handler
        const apiMarket = PROXY_SRC.match(
            /url\.pathname === '\/api\/market'[\s\S]*?validateYahooRange[\s\S]*?validateYahooInterval/,
        );
        expect(apiMarket).not.toBeNull();
        // /api/ib/market handler
        const ibMarket = PROXY_SRC.match(
            /url\.pathname === '\/api\/ib\/market'[\s\S]*?validateYahooInterval[\s\S]*?validateYahooRange/,
        );
        expect(ibMarket).not.toBeNull();
    });

    test('P1-C: CSRF gate sits on the write-endpoint path', () => {
        expect(PROXY_SRC).toMatch(
            /WRITE_ENDPOINTS\.has\(url\.pathname\)\s*&&\s*req\.method === 'POST'\s*&&\s*!isSameOriginPost\(req\)/,
        );
        expect(PROXY_SRC).toMatch(/Cross-origin write requests are not permitted/);
    });
});
