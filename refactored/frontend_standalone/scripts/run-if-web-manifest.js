#!/usr/bin/env node
/*
 * run-if-web-manifest.js
 *
 * A small cross-platform shim (Windows + *nix + git bash) that runs the given
 * npm script inside the `web/` workspace *only* if web/package.json exists.
 *
 * Why this exists:
 *   The VeloxFrontend upstream repo that this directory was synced from ships
 *   only a web/package-lock.json + dist bundles + a dark_editor/ subdir - no
 *   web/package.json, no web/src/. Until somebody authors web/package.json +
 *   web/src/, calling `npm run build/lint/test` at this directory must NOT
 *   fail loudly. It must surface a single-line TODO so the operator notices
 *   the missing source, and otherwise behave as a quiet no-op.
 *
 * Re-enable npm workspaces:
 *   1. Author web/package.json referencing the same deps as web/package-lock.json.
 *   2. Delete this file.
 *   3. Re-add `"workspaces": ["web"]` to package.json.
 *
 * Usage:
 *   node scripts/run-if-web-manifest.js <build|lint|test|sync-check> [--strict]
 *
 * Exit codes:
 *   0  - ran successfully OR no-op was the right call (web/package.json missing)
 *   78 - EX_CONFIG: web/package.json missing AND --strict was passed
 *   non-zero - the upstream `npm run <script>` returned non-zero (real failure)
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const args = process.argv.slice(2);
const strict = args.includes('--strict');
const script = args.find((a) => !a.startsWith('--'));

if (!script) {
  console.error('usage: node scripts/run-if-web-manifest.js <script> [--strict]');
  process.exit(2);
}

const repoRoot = path.resolve(__dirname, '..');
const webDir = path.join(repoRoot, 'web');
const manifest = path.join(webDir, 'package.json');

if (!fs.existsSync(manifest)) {
  const msg =
    '[frontend-velox] web/package.json is not authored yet. ' +
    'See refactored/frontend_standalone/README.md "When web/src/ gets authored" ' +
    'for the steps to bring the SPA source online.';
  if (strict) {
    console.error(msg);
    process.exit(78); // EX_CONFIG
  }
  console.warn(msg);
  process.exit(0);
}

// web/package.json exists -> delegate to the workspace's npm script with --if-present
// semantics so a missing target script (e.g. the upstream has no "test") is also a
// quiet success.
//
// We use --prefix web instead of --workspace web on purpose: --workspace requires
// the root package.json to declare `"workspaces": ["web"]`, and this directory
// deliberately does NOT have that config yet. --prefix lets us target web/
// without forcing a workspaces set-up, so the shim works today AND after
// web/package.json lands (which is when the README wants you to delete the
// shim and re-add the workspaces field).
//
// Cross-platform spawn notes:
//   - On Windows npm ships as npm.cmd (a batch file). spawnSync cannot execute
//     .cmd files directly; it errors with EINVAL. We force shell:true on win32
//     so the OS shell (.cmd / cmd.exe) does the dispatch. Static argument list
//     (no user input), so the shell-true security caveat does not apply.
//   - On *nix, npm is a real ELF shebang and shell:false is fine.
const npmBin = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const result = spawnSync(npmBin, ['--prefix', 'web', 'run', script, '--if-present'], {
  cwd: repoRoot,
  stdio: 'inherit',
  shell: process.platform === 'win32',
});
if (result.status === null) {
  // spawnSync failed (e.g. ENOENT for npm.cmd on a broken PATH). Use 127
  // ("command not found") so CI can distinguish a broken toolchain from a
  // genuine build failure.
  console.error('[frontend-velox] failed to spawn npm:', result.error || result.signal);
  process.exit(127);
}
// On Windows with shell:true, spawnSync returns status = 0 even when npm.cmd
// printed a non-zero exit. The error text lands in result.stderr. Capture it
// and propagate a non-zero status so CI flags the failure.
if (process.platform === 'win32' && result.stderr && result.stderr.length > 0) {
  const looksLikeError = /\b(err|error|ERR!|fail|not recognized|EPERM|EACCES|ELIFECYCLE)\b/i.test(
    result.stderr
  );
  if (looksLikeError && result.status === 0) {
    process.stderr.write(result.stderr);
    process.exit(1);
  }
}
process.exit(result.status);
