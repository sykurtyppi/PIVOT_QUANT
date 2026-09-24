/**
 * Strict CLI argument parser for the publish/score entry points.
 *
 * Boolean flags take no value; value flags require one. Unknown flags, missing values, and a
 * boolean flag accidentally consuming the next flag are all rejected — so a safety flag like
 * `--production` can never be silently swallowed or ignored regardless of argument order.
 */

export function parseCliArgs(argv, { booleanFlags = [], valueFlags = [] } = {}) {
  const booleans = new Set(booleanFlags);
  const values = new Set(valueFlags);
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (typeof token !== 'string' || !token.startsWith('--')) {
      throw new Error(`unexpected argument: ${token}`);
    }
    let key = token.slice(2);
    let inlineValue;
    const eq = key.indexOf('=');
    if (eq >= 0) { inlineValue = key.slice(eq + 1); key = key.slice(0, eq); }

    if (booleans.has(key)) {
      args[key] = inlineValue === undefined ? true : inlineValue === 'true';
      continue;
    }
    if (values.has(key)) {
      const value = inlineValue !== undefined ? inlineValue : argv[i + 1];
      if (value === undefined || value.startsWith('--')) throw new Error(`--${key} requires a value`);
      args[key] = value;
      if (inlineValue === undefined) i += 1;
      continue;
    }
    throw new Error(`unknown flag: --${key}`);
  }
  return args;
}
