// Shared by the launcher and the dashboard import, before server initialization.
// Keep package.json's approved engine range authoritative; do not infer a new
// security patch floor here. Fail closed if its range format changes.
import fs from 'fs';

const { engines } = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const policy = engines?.node;
const range = /^>=(\d+)\.(\d+)\.(\d+) <(\d+)$/.exec(policy || '');
const version = /^(\d+)\.(\d+)\.(\d+)$/.exec(process.versions.node);
let supported = false;
if (range && version) {
  const [, major, minor, patch] = version.map(Number);
  const [, minMajor, minMinor, minPatch, maxMajor] = range.map(Number);
  supported = major < maxMajor && (
    major > minMajor || (major === minMajor && (
      minor > minMinor || (minor === minMinor && patch >= minPatch)
    ))
  );
}
if (!supported) {
  // eslint-disable-next-line no-console
  console.error(`[runtime] Unsupported Node ${process.versions.node}; required ${policy || 'valid package.json engines.node policy'}. Select an approved Node executable (NODE_BIN for run_all.sh) and restart.`);
  process.exit(1);
}
