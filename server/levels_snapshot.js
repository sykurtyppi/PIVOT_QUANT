// Read-only canonical artifact bridge. Importing this module has no service,
// .env, DB, provider or notification side effects.
import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SYMBOL = /^[A-Z0-9][A-Z0-9._-]{0,31}$/;
const validSymbol = (symbol) => typeof symbol === 'string' && SYMBOL.test(symbol) && !symbol.includes('\n');
const unavailable = (symbol, reason) => ({ status: 'unavailable', symbol, reason });
// Bound request fan-out as well as individual subprocess resources.
let activeReaders = 0;

export async function readLevelsSnapshot(symbol, { env = process.env, run = execFile } = {}) {
  if (!validSymbol(symbol)) return unavailable(symbol, 'invalid_symbol');
  if (activeReaders >= 4) return unavailable(symbol, 'reader_busy');
  activeReaders += 1;
  try {
    return await new Promise((resolve) => {
      // Reuse the repository's explicit PYTHON_BIN -> venv -> system resolver.
      // No shell interpolation, user paths, .env sourcing, or cached responses.
      run(path.join(ROOT, 'scripts/_pybin_exec.sh'),
        [path.join(ROOT, 'scripts/levels_product/read_snapshot.py'), '--symbol', symbol],
        { cwd: ROOT, env, shell: false, timeout: 5000, maxBuffer: 524288, killSignal: 'SIGKILL' },
        (error, stdout) => {
          if (error) return resolve(unavailable(symbol, 'reader_failed'));
          try {
            const result = JSON.parse(stdout);
            if (result.symbol !== symbol || !['available', 'unavailable'].includes(result.status)) {
              throw new Error('reader contract mismatch');
            }
            resolve(result);
          } catch { resolve(unavailable(symbol, 'reader_failed')); }
        });
    });
  } catch { return unavailable(symbol, 'reader_failed'); }
  finally { activeReaders -= 1; }
}

export async function handleLevelsSnapshot(req, res, url, { read = readLevelsSnapshot } = {}) {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  const send = (code, result) => { res.writeHead(code); res.end(JSON.stringify(result)); };
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    send(405, unavailable(null, 'method_not_allowed'));
    return;
  }
  const symbols = url.searchParams.getAll('symbol');
  if (symbols.length !== 1 || !validSymbol(symbols[0]) || [...url.searchParams.keys()].some((key) => key !== 'symbol')) {
    send(400, unavailable(null, 'invalid_query'));
    return;
  }
  try { send(200, await read(symbols[0])); }
  catch { send(503, unavailable(symbols[0], 'reader_failed')); }
}
