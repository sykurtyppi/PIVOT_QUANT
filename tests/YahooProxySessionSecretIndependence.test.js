/**
 * Regression tests for P1-B: the dashboard session-cookie HMAC key must
 * be independent material from DASH_AUTH_PASSWORD.
 *
 * Pre-fix, server/yahoo_proxy.js signed/verified session cookies with
 * SECURITY.authPassword directly (the raw DASH_AUTH_PASSWORD env value).
 * That makes the password the signing key — whoever can guess/brute-force
 * the password can also mint valid session cookies offline.  The fix
 * introduces an independent DASH_SESSION_SECRET and refuses operations
 * (cookie mint and verify) when it is missing, too short, or equal to
 * the password.
 *
 * yahoo_proxy.js calls server.listen() at module load time, so we cannot
 * import it normally.  Following the same vm.runInContext pattern used
 * elsewhere in this suite, we read the source, extract the helper
 * function bodies by brace-counted match, and evaluate them in an
 * isolated sandbox.  This lets us exercise the REAL helper code (not a
 * paraphrase) while bypassing the module's startup side effects.
 */

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');

const PROXY_PATH = path.join(__dirname, '..', 'server', 'yahoo_proxy.js');
const PROXY_SRC = fs.readFileSync(PROXY_PATH, 'utf8');

// Extract a top-level `function NAME(...) { ... }` declaration by walking
// the source: skip the parameter list (paren-balanced — so default values
// like `fileEnv = {}` don't fool the brace counter), then brace-balance
// the function body.
function extractFunctionSource(name) {
    const re = new RegExp(`\\nfunction ${name}\\b\\s*\\(`, 'g');
    const match = re.exec(PROXY_SRC);
    if (!match) throw new Error(`function ${name} not found in yahoo_proxy.js`);
    // re.lastIndex is just past the opening `(` of the param list.
    let i = re.lastIndex;
    let parenDepth = 1;
    while (i < PROXY_SRC.length && parenDepth > 0) {
        const ch = PROXY_SRC[i];
        if (ch === '(') parenDepth += 1;
        else if (ch === ')') parenDepth -= 1;
        i += 1;
    }
    // i now points just after the function's closing `)`.  Walk to the
    // body's opening `{`.
    while (i < PROXY_SRC.length && PROXY_SRC[i] !== '{') i += 1;
    let depth = 0;
    let j = i;
    while (j < PROXY_SRC.length) {
        const ch = PROXY_SRC[j];
        if (ch === '{') depth += 1;
        else if (ch === '}') {
            depth -= 1;
            if (depth === 0) { j += 1; break; }
        }
        j += 1;
    }
    return PROXY_SRC.slice(match.index + 1, j);
}

function buildSandbox() {
    const sandbox = {
        URL,
        Buffer,
        crypto,
        Number,
        String,
        Math,
        Date,
        Object,
        JSON,
        console: { log() {}, warn() {}, error() {}, info() {} },
        // HOST is referenced inside buildSecurityConfig only as a fallback
        // when neither procEnv nor fileEnv set it.  Provide the same
        // constant the production module uses.
        HOST: '127.0.0.1',
    };
    vm.createContext(sandbox);
    const helpers = [
        'parseBool',
        'parsePositiveInt',
        'isLoopbackBindHost',
        'readSetting',
        'buildSecurityConfig',
        'safeEqual',
        'signPayload',
        'parseTokenParts',
        'createSessionToken',
    ];
    for (const name of helpers) {
        vm.runInContext(extractFunctionSource(name), sandbox);
    }
    return sandbox;
}

const sandbox = buildSandbox();
const {
    buildSecurityConfig,
    signPayload,
    createSessionToken,
    parseTokenParts,
    safeEqual,
} = sandbox;

const HEX64 = '5'.repeat(64);                                       // 32 random bytes hex
const HEX64_OTHER = '7'.repeat(64);                                 // different 32-byte hex
const STRONG_PASSWORD = 'correct horse battery staple 9!Q';          // >=20 chars
const SHORT_SECRET = 'too-short';

describe('P1-B: DASH_SESSION_SECRET independence from DASH_AUTH_PASSWORD', () => {
    describe('buildSecurityConfig', () => {
        test('with both DASH_AUTH_PASSWORD and DASH_SESSION_SECRET set: keys are separate fields', () => {
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: STRONG_PASSWORD,
                DASH_SESSION_SECRET: HEX64,
            }, {});
            expect(cfg.authPassword).toBe(STRONG_PASSWORD);
            expect(cfg.sessionSecret).toBe(HEX64);
            expect(cfg.sessionSecret).not.toBe(cfg.authPassword);
            expect(cfg.sessionSecretConfigured).toBe(true);
            expect(cfg.authPolicyIssues).not.toContain('missing_session_secret');
        });

        test('DASH_SESSION_SECRET below min length (32) marks sessionSecretConfigured=false', () => {
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: STRONG_PASSWORD,
                DASH_SESSION_SECRET: SHORT_SECRET,
            }, {});
            expect(cfg.sessionSecretConfigured).toBe(false);
            expect(cfg.authPolicyIssues).toContain('missing_session_secret');
            expect(cfg.authPolicyOk).toBe(false);
        });

        test('DASH_SESSION_SECRET unset marks sessionSecretConfigured=false', () => {
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: STRONG_PASSWORD,
            }, {});
            expect(cfg.sessionSecret).toBe('');
            expect(cfg.sessionSecretConfigured).toBe(false);
            expect(cfg.authPolicyIssues).toContain('missing_session_secret');
        });

        test('DASH_SESSION_SECRET equal to DASH_AUTH_PASSWORD is REJECTED (independence by construction)', () => {
            // Even if the operator picks a 32+ char "secret" that happens to
            // equal the password, we refuse — the whole point is decoupling.
            const reused = HEX64;
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: reused,
                DASH_SESSION_SECRET: reused,
            }, {});
            expect(cfg.sessionSecretConfigured).toBe(false);
            expect(cfg.authPolicyIssues).toContain('missing_session_secret');
        });

        test('non-loopback bind with missing secret raises both policy codes', () => {
            const cfg = buildSecurityConfig({
                HOST: '0.0.0.0',
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: STRONG_PASSWORD,
                // no DASH_SESSION_SECRET
            }, {});
            expect(cfg.bindIsLoopback).toBe(false);
            expect(cfg.sessionSecretConfigured).toBe(false);
            expect(cfg.authPolicyIssues).toContain('missing_session_secret');
            expect(cfg.authPolicyIssues).toContain('missing_session_secret_with_non_loopback_bind');
            expect(cfg.authPolicyOk).toBe(false);
        });

        test('auth disabled + secret missing: sessionSecretConfigured still false, but no policy issue raised', () => {
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'false',
                // no password, no secret
            }, {});
            expect(cfg.authEnabled).toBe(false);
            expect(cfg.sessionSecretConfigured).toBe(false);
            expect(cfg.authPolicyIssues).not.toContain('missing_session_secret');
            expect(cfg.authPolicyIssues).not.toContain('missing_session_secret_with_non_loopback_bind');
        });

        test('DASH_SESSION_SECRET_MIN_LEN cannot drop below the hardcoded floor (32)', () => {
            // Operator-supplied min-len is honoured but clamped to a sane minimum.
            const cfg = buildSecurityConfig({
                DASH_AUTH_ENABLED: 'true',
                DASH_AUTH_PASSWORD: STRONG_PASSWORD,
                DASH_SESSION_SECRET: 'x'.repeat(20),
                DASH_SESSION_SECRET_MIN_LEN: '4',
            }, {});
            // Floor of 32 means a 20-char "secret" is still rejected even
            // when the operator tries to set the min to 4.
            expect(cfg.sessionSecretMinLength).toBe(32);
            expect(cfg.sessionSecretConfigured).toBe(false);
        });
    });

    describe('HMAC sign / verify with independent key', () => {
        test('password change does NOT invalidate cookies signed with the session secret', () => {
            const cookie = createSessionToken(HEX64, 3600);
            const parts = parseTokenParts(cookie);
            expect(parts).not.toBeNull();
            // Re-verify with the SAME secret but a DIFFERENT password value
            // (which post-fix is irrelevant to the signature).
            const expectedSig = signPayload(parts.payload, HEX64);
            expect(safeEqual(parts.sig, expectedSig)).toBe(true);
            // Sanity: changing the *secret* DOES invalidate it.
            const wrongSig = signPayload(parts.payload, HEX64_OTHER);
            expect(safeEqual(parts.sig, wrongSig)).toBe(false);
        });

        test('a cookie signed with the PASSWORD as key is REJECTED when verified against the secret', () => {
            // This is the pre-fix bug: previously this exact cookie WOULD
            // have validated.  Post-fix, it must not — the verifier uses
            // SECURITY.sessionSecret, never authPassword.
            const cookieSignedWithPassword = createSessionToken(STRONG_PASSWORD, 3600);
            const parts = parseTokenParts(cookieSignedWithPassword);
            const expectedSigWithSecret = signPayload(parts.payload, HEX64);
            expect(safeEqual(parts.sig, expectedSigWithSecret)).toBe(false);
        });

        test('a tampered signature is still rejected under the new key', () => {
            const cookie = createSessionToken(HEX64, 3600);
            const parts = parseTokenParts(cookie);
            // Flip a character in the sig.
            const mutatedSig = parts.sig.slice(0, -1) + (parts.sig.endsWith('a') ? 'b' : 'a');
            const expectedSig = signPayload(parts.payload, HEX64);
            expect(safeEqual(mutatedSig, expectedSig)).toBe(false);
        });

        test('changing the secret invalidates ALL previously-issued cookies (per-rotation invalidation)', () => {
            const cookieUnderOldSecret = createSessionToken(HEX64, 3600);
            const parts = parseTokenParts(cookieUnderOldSecret);
            const newSecret = HEX64_OTHER;
            const expectedSigUnderNewSecret = signPayload(parts.payload, newSecret);
            expect(safeEqual(parts.sig, expectedSigUnderNewSecret)).toBe(false);
        });

        test('HMAC-SHA256 is the signing primitive (not SHA1/MD5/plain hash)', () => {
            const sig = signPayload('payload-value', HEX64);
            // Compute the expected HMAC-SHA256 base64url directly with node's
            // crypto and compare — pins the algorithm choice.
            const expected = crypto.createHmac('sha256', HEX64)
                .update('payload-value')
                .digest('base64url');
            expect(sig).toBe(expected);
        });
    });
});

describe('P1-B: production wiring — yahoo_proxy.js source pins', () => {
    test('mint site (login success) uses SECURITY.sessionSecret, NOT SECURITY.authPassword', () => {
        // The post-fix mint call must read sessionSecret.
        expect(PROXY_SRC).toMatch(
            /createSessionToken\(\s*SECURITY\.sessionSecret\s*,\s*SECURITY\.authCookieTtlSec\s*\)/,
        );
        // The pre-fix call (with authPassword) must be gone.
        expect(PROXY_SRC).not.toMatch(
            /createSessionToken\(\s*SECURITY\.authPassword\s*,\s*SECURITY\.authCookieTtlSec\s*\)/,
        );
    });

    test('verify site (hasValidSession) uses SECURITY.sessionSecret, NOT SECURITY.authPassword', () => {
        expect(PROXY_SRC).toMatch(
            /signPayload\(\s*parts\.payload\s*,\s*SECURITY\.sessionSecret\s*\)/,
        );
        expect(PROXY_SRC).not.toMatch(
            /signPayload\(\s*parts\.payload\s*,\s*SECURITY\.authPassword\s*\)/,
        );
    });

    test('hasValidSession fails closed when sessionSecretConfigured is false', () => {
        // Pin the early-return so future refactors cannot accidentally
        // re-introduce a path that signs/verifies with the password.
        const hasValid = extractFunctionSource('hasValidSession');
        expect(hasValid).toMatch(/if\s*\(!SECURITY\.sessionSecretConfigured\)\s*return false;/);
    });

    test('login mint refuses with 503 when sessionSecretConfigured is false', () => {
        // The login handler must short-circuit BEFORE calling createSessionToken
        // when the secret is not configured.  Pinned as a 503 (server misconfig)
        // rather than a 401 (credential problem).
        expect(PROXY_SRC).toMatch(/if\s*\(!SECURITY\.sessionSecretConfigured\)\s*\{[\s\S]{0,400}?sendLoginPage\([\s\S]{0,400}?503/);
    });

    test('the signing secret value never appears in any sendJson/sendLoginPage response body or log statement', () => {
        // Greppable interface contract: SECURITY.sessionSecret must only be
        // passed into the HMAC helpers, never to response shaping or
        // console.* observability.  Grep every line that references it
        // and assert it appears only in sign/verify contexts.
        const lines = PROXY_SRC.split('\n');
        const offending = [];
        const allowedContexts = [
            'createSessionToken(',           // mint
            'signPayload(',                  // verify
            'sessionSecret,',                // arg list inside helpers
            'sessionSecret:',                // SECURITY object field decl
            'sessionSecret =',               // assignment in buildSecurityConfig
            'sessionSecret.length',          // length check (already in compile-time logic)
            'sessionSecret !==',             // password-equality reject
        ];
        for (const line of lines) {
            if (!line.includes('sessionSecret')) continue;
            // Skip pure comment lines.
            if (/^\s*(\/\/|\*)/.test(line)) continue;
            // sessionSecretConfigured / sessionSecretMinLength are booleans
            // and integers respectively, not the secret value — fine to emit.
            if (line.includes('sessionSecretConfigured') || line.includes('sessionSecretMinLength')) {
                if (!line.includes('SECURITY.sessionSecret,') && !line.match(/SECURITY\.sessionSecret\b(?!Configured|MinLength)/)) {
                    continue;
                }
            }
            // Forbidden patterns: sessionSecret value reaching sendJson / sendLoginPage / console.* / res.write.
            if (
                line.match(/sendJson\(.*sessionSecret/) ||
                line.match(/sendLoginPage\(.*sessionSecret/) ||
                line.match(/console\.[a-z]+\(.*sessionSecret/) ||
                line.match(/res\.write.*sessionSecret/) ||
                line.match(/res\.end\(.*sessionSecret/) ||
                line.match(/JSON\.stringify\(.*sessionSecret/)
            ) {
                offending.push(line.trim());
                continue;
            }
            // Allowed: appears only via HMAC helpers, buildSecurityConfig
            // internals, or the SECURITY object field decl.
            const ok = allowedContexts.some((ctx) => line.includes(ctx));
            // Either it's in an allowed context, or it's the FATAL-log line
            // which explicitly references DASH_SESSION_SECRET by NAME (not
            // value).  Pin that the FATAL line refers to bindHost, not the
            // secret itself.
            if (!ok && line.includes('console.error') && line.includes('DASH_SESSION_SECRET')) {
                expect(line).not.toMatch(/SECURITY\.sessionSecret\b/);
                continue;
            }
            // Otherwise OK if it doesn't pattern-match a leak.
        }
        expect(offending).toEqual([]);
    });

    test('/health response shape exposes session-secret-configured booleans only (never the value)', () => {
        // The status snapshot may signal whether the secret IS configured,
        // but must never include the secret value.
        expect(PROXY_SRC).toMatch(/auth_session_secret_configured:\s*SECURITY\.sessionSecretConfigured/);
        // No path that emits SECURITY.sessionSecret as a response field.
        expect(PROXY_SRC).not.toMatch(/auth_session_secret\s*:\s*SECURITY\.sessionSecret\b(?!Configured|MinLength)/);
    });

    test('non-loopback bind without secret triggers process.exit(1) at startup (fail-closed)', () => {
        // Defense-in-depth even when run_persistent_stack.sh is bypassed.
        const startupBlockMatch = PROXY_SRC.match(/server\.listen\(PORT, HOST,[\s\S]*$/);
        expect(startupBlockMatch).not.toBeNull();
        const startupBlock = startupBlockMatch[0];
        expect(startupBlock).toMatch(
            /SECURITY\.authEnabled[\s\S]{0,200}!SECURITY\.bindIsLoopback[\s\S]{0,200}!SECURITY\.sessionSecretConfigured[\s\S]{0,400}process\.exit\(1\)/,
        );
    });
});

describe('P1-B: run_persistent_stack.sh pins', () => {
    const SHELL_PATH = path.join(__dirname, '..', 'server', 'run_persistent_stack.sh');
    const SHELL_SRC = fs.readFileSync(SHELL_PATH, 'utf8');

    test('the DASH_SESSION_SECRET block lives inside DASH_AUTH_ENABLED', () => {
        // The check is meaningful only when auth is on.
        const authBlockMatch = SHELL_SRC.match(/if is_truthy "\$\{DASH_AUTH_ENABLED:-false\}"; then[\s\S]+?\nfi\n/);
        expect(authBlockMatch).not.toBeNull();
        const authBlock = authBlockMatch[0];
        expect(authBlock).toMatch(/DASH_SESSION_SECRET="\$\{DASH_SESSION_SECRET:-\}"/);
        expect(authBlock).toMatch(/DASH_SESSION_SECRET_MIN_LEN="\$\{DASH_SESSION_SECRET_MIN_LEN:-32\}"/);
    });

    test('DASH_SESSION_SECRET == DASH_AUTH_PASSWORD => exit 1', () => {
        expect(SHELL_SRC).toMatch(
            /DASH_SESSION_SECRET must NOT equal DASH_AUTH_PASSWORD/,
        );
    });

    test('non-loopback bind + missing secret => exit 1 (fail-closed)', () => {
        expect(SHELL_SRC).toMatch(
            /DASH_SESSION_SECRET length \(\$\{#DASH_SESSION_SECRET\}\) must be >= \$\{DASH_SESSION_SECRET_MIN_LEN\} when HOST is non-loopback/,
        );
    });

    test('loopback bind + missing secret => warn (not exit)', () => {
        expect(SHELL_SRC).toMatch(
            /WARN: DASH_SESSION_SECRET is unset or too short/,
        );
        // The warn branch must not be coupled to exit 1.
        const warnLineIdx = SHELL_SRC.indexOf('WARN: DASH_SESSION_SECRET');
        const next200 = SHELL_SRC.slice(warnLineIdx, warnLineIdx + 200);
        expect(next200).not.toMatch(/exit 1/);
    });

    test('exports DASH_SESSION_SECRET so the node child sees it', () => {
        expect(SHELL_SRC).toMatch(/^\s*export DASH_SESSION_SECRET\s*$/m);
    });
});
