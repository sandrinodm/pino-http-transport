import { defineConfig } from 'vite-plus';

export default defineConfig({
  fmt: {
    tabWidth: 2,
    printWidth: 120,
    singleQuote: true,
    trailingComma: 'es5',
    semi: true,
    ignorePatterns: ['dist/**', 'coverage/**', 'pnpm-lock.yaml'],
  },
  lint: {
    ignorePatterns: ['dist/**', 'coverage/**'],
    options: {
      typeAware: true,
      typeCheck: true,
    },
    categories: { correctness: 'error' },
    rules: {
      'typescript/consistent-type-imports': 'error',
      'typescript/no-non-null-assertion': 'error',
    },
    overrides: [{ files: ['test/**/*.ts'], rules: { 'typescript/no-non-null-assertion': 'off' } }],
  },
  test: {
    environment: 'node',
    include: ['test/**/*.test.ts'],
    restoreMocks: true,
    silent: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*.ts'],
      thresholds: {
        branches: 80,
        functions: 90,
        lines: 90,
        statements: 90,
      },
    },
  },
});
