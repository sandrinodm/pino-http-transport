# Contributing

Use a current Node.js 24 release (24.15 or later for release tooling) and pnpm 12. Before opening a pull request, run:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
pnpm build
pnpm test:coverage
node --test test/runtime.test.cjs
npm pack --dry-run --json
```

Keep changes focused and add regression tests for behavior changes. Vite+ runs
Oxfmt, Oxlint, and strict TypeScript checks through `pnpm check`. Configure them
alongside tests in `vite.config.ts`; run `pnpm check:fix` to apply safe fixes.
The Vite+ development tools require Node 24.11 or later, while releases require
24.15 or later. The published transport supports Node 24.0.0 or later.
Keep transport tests in `test/`, including worker and package-loading coverage
where relevant.

Document exported declarations and meaningful internal helpers with concise
JSDoc. Describe behavior and invariants rather than restating TypeScript types.
The package is ESM-only and supports Node.js 24 or later.

Write concise commit subjects that explain the user-visible change. Releases
are created from `main` with `release-it`; GitHub generates the release notes
from commits since the previous tag. Do not edit the version manually.
