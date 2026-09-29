module.exports = {
  testEnvironment: 'node',
  collectCoverageFrom: [
    'src/**/*.js',
    '!src/**/*.test.js',
  ],
  // Sprint D: JS coverage is still reported, but hard gating is relaxed because
  // primary system trust now lives in validate:local (Python APIs + dashboard smoke).
  // This keeps visibility without blocking non-JS-stack release validation.
  coverageReporters: ['text'],
  modulePathIgnorePatterns: ['<rootDir>/.claude/worktrees/'],
  testMatch: [
    '**/tests/**/*.test.js',
  ],
  transform: {
    '^.+\\.js$': 'babel-jest',
  },
};
