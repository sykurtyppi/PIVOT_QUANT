import { parseCliArgs } from '../../src/forecast/parseCliArgs.js';

const OPTS = {
  booleanFlags: ['dry-run', 'production', 'allow-dirty'],
  valueFlags: ['symbol', 'proxy', 'range'],
};
const parse = (argv) => parseCliArgs(argv, OPTS);

describe('parseCliArgs', () => {
  test('a boolean flag never consumes the following flag, in any order', () => {
    expect(parse(['--production', '--dry-run'])).toEqual({ production: true, 'dry-run': true });
    expect(parse(['--dry-run', '--production'])).toEqual({ 'dry-run': true, production: true });
    expect(parse(['--production', '--proxy', 'http://x'])).toEqual({ production: true, proxy: 'http://x' });
  });

  test('a bare trailing boolean flag is honored (not dropped as undefined)', () => {
    expect(parse(['--symbol', 'SPY', '--production'])).toEqual({ symbol: 'SPY', production: true });
  });

  test('value flags accept --k v and --k=v', () => {
    expect(parse(['--symbol', 'SPY'])).toEqual({ symbol: 'SPY' });
    expect(parse(['--symbol=SPY'])).toEqual({ symbol: 'SPY' });
  });

  test('explicit --production=false is respected', () => {
    expect(parse(['--production=false'])).toEqual({ production: false });
    expect(parse(['--production=true'])).toEqual({ production: true });
  });

  test('a value flag missing its value is rejected (does not swallow the next flag)', () => {
    expect(() => parse(['--symbol', '--production'])).toThrow(/--symbol requires a value/);
    expect(() => parse(['--proxy'])).toThrow(/--proxy requires a value/);
  });

  test('unknown flags and stray positionals are rejected', () => {
    expect(() => parse(['--frobnicate'])).toThrow(/unknown flag/);
    expect(() => parse(['positional'])).toThrow(/unexpected argument/);
  });
});
