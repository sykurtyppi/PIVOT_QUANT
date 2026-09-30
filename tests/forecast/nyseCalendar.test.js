import {
  CALENDAR_VERSION,
  holidaysForYear,
  holidaysInRange,
  isTradingSession,
  nextTradingSession,
  priorTradingSession,
} from '../../src/forecast/nyseCalendar.js';

describe('nyseCalendar', () => {
  test('generates the 2026 NYSE holiday set with weekend observance', () => {
    const set = holidaysForYear(2026);
    expect([...set].sort()).toEqual([
      '2026-01-01', // New Year's Day (Thu)
      '2026-01-19', // MLK (3rd Mon Jan)
      '2026-02-16', // Washington's Birthday (3rd Mon Feb)
      '2026-04-03', // Good Friday
      '2026-05-25', // Memorial Day (last Mon May)
      '2026-06-19', // Juneteenth (Fri)
      '2026-07-03', // Independence Day observed (Jul 4 is Sat)
      '2026-09-07', // Labor Day (1st Mon Sep)
      '2026-11-26', // Thanksgiving (4th Thu Nov)
      '2026-12-25', // Christmas (Fri)
    ]);
  });

  test('Independence Day Saturday is observed on the preceding Friday', () => {
    expect(holidaysForYear(2026).has('2026-07-03')).toBe(true);
    expect(isTradingSession('2026-07-03')).toBe(false);
  });

  test('New Year Saturday is not observed on the prior Dec 31', () => {
    // 2022-01-01 is a Saturday -> no Dec 31 2021 observance.
    expect(holidaysForYear(2021).has('2021-12-31')).toBe(false);
    expect(isTradingSession('2021-12-31')).toBe(true);
  });

  test('Juneteenth is only observed from 2022 onward', () => {
    expect(holidaysForYear(2019).has('2019-06-19')).toBe(false);
    expect(holidaysForYear(2022).has('2022-06-20')).toBe(true); // Jun 19 2022 Sun -> Mon observed
  });

  test('ad-hoc closures (Hurricane Sandy) are included', () => {
    const set = holidaysForYear(2012);
    expect(set.has('2012-10-29')).toBe(true);
    expect(set.has('2012-10-30')).toBe(true);
  });

  test('isTradingSession is false on weekends and holidays, true on regular days', () => {
    expect(isTradingSession('2026-11-26')).toBe(false); // Thanksgiving
    expect(isTradingSession('2026-11-27')).toBe(true); // day after (half day, still a session)
    expect(isTradingSession('2026-09-26')).toBe(false); // Saturday
    expect(isTradingSession('2026-09-24')).toBe(true); // Thursday
  });

  test('priorTradingSession skips weekends and holidays', () => {
    // Tue 2026-07-06: back over Jul 4 (Sat, observed Fri Jul 3) + weekend -> Thu Jul 2.
    expect(priorTradingSession('2026-07-06')).toBe('2026-07-02');
    // Tue 2026-01-20 after MLK Mon -> Fri 2026-01-16.
    expect(priorTradingSession('2026-01-20')).toBe('2026-01-16');
  });

  test('nextTradingSession skips the Christmas cluster', () => {
    // Fri 2026-12-25 Christmas, 26 Sat, 27 Sun -> Mon 28.
    expect(nextTradingSession('2026-12-25')).toBe('2026-12-28');
  });

  test('holidaysInRange is sorted, bounded, and deterministic', () => {
    const a = holidaysInRange('2026-06-01', '2026-07-31');
    const b = holidaysInRange('2026-06-01', '2026-07-31');
    expect(a).toEqual(b);
    expect(a).toEqual(['2026-06-19', '2026-07-03']);
  });

  test('holidaysInRange bounds are inclusive on both ends', () => {
    // Holidays sitting exactly on fromDate and toDate must be included (kills >/< mutants).
    expect(holidaysInRange('2026-06-19', '2026-07-03')).toEqual(['2026-06-19', '2026-07-03']);
    expect(holidaysInRange('2026-06-20', '2026-07-02')).toEqual([]);
  });

  test('holidaysInRange rejects an inverted range', () => {
    expect(() => holidaysInRange('2026-07-31', '2026-06-01')).toThrow(/after/);
  });

  test('Good Friday is correct across multiple years', () => {
    expect(holidaysForYear(2024).has('2024-03-29')).toBe(true);
    expect(holidaysForYear(2025).has('2025-04-18')).toBe(true);
    expect(holidaysForYear(2027).has('2027-03-26')).toBe(true);
  });

  test('the first 1998 session can resolve its 1997 prior session', () => {
    // Regression: the support floor sits below 1998 so early-1998 asOf is fully usable.
    expect(isTradingSession('1998-01-02')).toBe(true);
    expect(priorTradingSession('1998-01-02')).toBe('1997-12-31');
  });

  test('refuses genuinely unsupported (pre-1996) years rather than guessing', () => {
    expect(() => holidaysForYear(1995)).toThrow(/>= 1996/);
    expect(() => isTradingSession('1990-01-02')).toThrow(/>= 1996/);
  });

  test('exposes a stable calendar version', () => {
    expect(CALENDAR_VERSION).toBe('nyse-rulegen-1.0.0');
  });
});
