/**
 * Authoritative NYSE trading calendar (rule-generated).
 *
 * The volatility engine (src/math/MultiHorizonVolatilityLevels.js) trusts a
 * caller-supplied `holidays` array. The forecast ledger must not: a wrong or missing
 * holiday silently shifts every point-in-time anchor. This module generates the NYSE
 * holiday set from the published observance rules so the ledger can supply an
 * authoritative calendar and pin exactly which ruleset produced it.
 *
 * Scope and limitations (documented on purpose):
 *  - Covers the modern NYSE holiday schedule (MLK from 1998, Juneteenth from 2022).
 *    Dates before MIN_SUPPORTED_YEAR are NOT modeled and `isTradingSession` throws for them,
 *    so the calendar can never silently return a wrong pre-modern session.
 *  - Models full-day closures only. Early-close (half) days are still trading sessions for a
 *    close-to-close daily model, so they are intentionally not in the holiday set.
 *  - One-off historical closures (e.g. 2001-09-11..14, funerals, Sandy 2012-10-29..30)
 *    are enumerated in AD_HOC_CLOSURES; extend that table rather than the rules.
 *
 * All dates are handled as UTC calendar days (YYYY-MM-DD), matching the engine's own
 * `dateKey`, so there is no local-timezone drift.
 */

export const CALENDAR_VERSION = 'nyse-rulegen-1.0.0';

const MLK_FIRST_YEAR = 1998; // MLK Day observed by NYSE from 1998.
const JUNETEENTH_FIRST_YEAR = 2022; // Juneteenth observed by NYSE from 2022.
// The ruleset (with MLK gated to >=1998 and Juneteenth to >=2022) is correct back to the
// mid-1990s. The floor sits a couple of years below 1998 so a forecast whose asOf is the
// first trading day of 1998 can still resolve its 1997 prior session, rather than hitting an
// opaque year-guard error at the boundary.
const MIN_SUPPORTED_YEAR = 1996;

// Full-day ad-hoc closures not derivable from the annual rules.
const AD_HOC_CLOSURES = new Set([
  '2001-09-11', '2001-09-12', '2001-09-13', '2001-09-14', // 9/11
  '2004-06-11', // Reagan national day of mourning
  '2007-01-02', // Ford national day of mourning
  '2012-10-29', '2012-10-30', // Hurricane Sandy
  '2018-12-05', // Bush national day of mourning
  '2025-01-09', // Carter national day of mourning
]);

function pad2(value) {
  return String(value).padStart(2, '0');
}

function ymd(year, month, day) {
  return `${year}-${pad2(month)}-${pad2(day)}`;
}

function utc(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day));
}

function weekday(dateUtc) {
  return dateUtc.getUTCDay(); // 0 Sun .. 6 Sat
}

/** Nth weekday of a month, e.g. 3rd Monday. weekdayIndex: 0=Sun..6=Sat. */
function nthWeekdayOfMonth(year, month, weekdayIndex, n) {
  const first = utc(year, month, 1);
  const offset = (weekdayIndex - first.getUTCDay() + 7) % 7;
  return utc(year, month, 1 + offset + (n - 1) * 7);
}

/** Last given weekday of a month, e.g. last Monday. */
function lastWeekdayOfMonth(year, month, weekdayIndex) {
  const last = utc(year, month + 1, 0); // day 0 of next month = last day of this month
  const offset = (last.getUTCDay() - weekdayIndex + 7) % 7;
  return utc(year, month, last.getUTCDate() - offset);
}

/** Easter Sunday (Anonymous Gregorian algorithm); Good Friday is two days earlier. */
function easterSunday(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return utc(year, month, day);
}

function addUtcDays(dateUtc, days) {
  const result = new Date(dateUtc.getTime());
  result.setUTCDate(result.getUTCDate() + days);
  return result;
}

/**
 * Apply the NYSE weekend-observance rule to a fixed-date holiday:
 * Saturday -> observed the preceding Friday; Sunday -> observed the following Monday.
 * New Year's Day is special-cased: a Saturday Jan 1 is NOT observed on Dec 31 of the
 * prior year (NYSE does not close), so it yields no holiday in this year's set.
 */
function observedFixedDate(dateUtc, { isNewYears = false } = {}) {
  const dow = weekday(dateUtc);
  if (dow === 6) {
    // Saturday
    if (isNewYears) return null; // no Dec-31 observance for New Year's
    return addUtcDays(dateUtc, -1);
  }
  if (dow === 0) {
    // Sunday
    return addUtcDays(dateUtc, 1);
  }
  return dateUtc;
}

/**
 * Full-day NYSE holidays for a year, as a Set of YYYY-MM-DD strings.
 * Includes weekend-observed dates and applicable ad-hoc closures.
 */
export function holidaysForYear(year) {
  if (!Number.isInteger(year) || year < MIN_SUPPORTED_YEAR) {
    throw new Error(`NYSE calendar only supports years >= ${MIN_SUPPORTED_YEAR}; got ${year}`);
  }

  const dates = new Set();
  const add = (dateUtc) => {
    if (dateUtc) dates.add(ymd(dateUtc.getUTCFullYear(), dateUtc.getUTCMonth() + 1, dateUtc.getUTCDate()));
  };

  add(observedFixedDate(utc(year, 1, 1), { isNewYears: true })); // New Year's Day
  if (year >= MLK_FIRST_YEAR) add(nthWeekdayOfMonth(year, 1, 1, 3)); // MLK: 3rd Monday Jan
  add(nthWeekdayOfMonth(year, 2, 1, 3)); // Washington's Birthday: 3rd Monday Feb
  add(addUtcDays(easterSunday(year), -2)); // Good Friday
  add(lastWeekdayOfMonth(year, 5, 1)); // Memorial Day: last Monday May
  if (year >= JUNETEENTH_FIRST_YEAR) add(observedFixedDate(utc(year, 6, 19))); // Juneteenth
  add(observedFixedDate(utc(year, 7, 4))); // Independence Day
  add(nthWeekdayOfMonth(year, 9, 1, 1)); // Labor Day: 1st Monday Sep
  add(nthWeekdayOfMonth(year, 11, 4, 4)); // Thanksgiving: 4th Thursday Nov
  add(observedFixedDate(utc(year, 12, 25))); // Christmas

  for (const closure of AD_HOC_CLOSURES) {
    if (closure.startsWith(`${year}-`)) dates.add(closure);
  }
  return dates;
}

function parseDateKey(dateKey) {
  if (typeof dateKey !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(dateKey)) {
    throw new Error(`date must be YYYY-MM-DD, got ${dateKey}`);
  }
  const [y, m, d] = dateKey.split('-').map(Number);
  const dt = utc(y, m, d);
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() + 1 !== m || dt.getUTCDate() !== d) {
    throw new Error(`invalid calendar date: ${dateKey}`);
  }
  return dt;
}

function dateKeyOf(dateUtc) {
  return ymd(dateUtc.getUTCFullYear(), dateUtc.getUTCMonth() + 1, dateUtc.getUTCDate());
}

/** True when `dateKey` is a full NYSE trading session (weekday, not a holiday). */
export function isTradingSession(dateKey) {
  const dt = parseDateKey(dateKey);
  const year = dt.getUTCFullYear();
  if (year < MIN_SUPPORTED_YEAR) {
    throw new Error(`NYSE calendar only supports years >= ${MIN_SUPPORTED_YEAR}; got ${dateKey}`);
  }
  const dow = weekday(dt);
  if (dow === 0 || dow === 6) return false;
  return !holidaysForYear(year).has(dateKey);
}

/** The immediately preceding NYSE trading session before `dateKey` (exclusive). */
export function priorTradingSession(dateKey) {
  let cursor = addUtcDays(parseDateKey(dateKey), -1);
  for (let guard = 0; guard < 15; guard += 1) {
    const key = dateKeyOf(cursor);
    if (isTradingSession(key)) return key;
    cursor = addUtcDays(cursor, -1);
  }
  throw new Error(`no trading session found within 15 days before ${dateKey}`);
}

/** The immediately following NYSE trading session after `dateKey` (exclusive). */
export function nextTradingSession(dateKey) {
  let cursor = addUtcDays(parseDateKey(dateKey), 1);
  for (let guard = 0; guard < 15; guard += 1) {
    const key = dateKeyOf(cursor);
    if (isTradingSession(key)) return key;
    cursor = addUtcDays(cursor, 1);
  }
  throw new Error(`no trading session found within 15 days after ${dateKey}`);
}

/**
 * Holiday YYYY-MM-DD strings across an inclusive [fromDate, toDate] range — the format
 * the volatility engine's `options.holidays` expects. Deterministic (sorted).
 */
export function holidaysInRange(fromDateKey, toDateKey) {
  const from = parseDateKey(fromDateKey);
  const to = parseDateKey(toDateKey);
  if (from > to) throw new Error(`fromDate ${fromDateKey} is after toDate ${toDateKey}`);
  const out = [];
  for (let y = from.getUTCFullYear(); y <= to.getUTCFullYear(); y += 1) {
    for (const key of holidaysForYear(y)) {
      if (key >= fromDateKey && key <= toDateKey) out.push(key);
    }
  }
  return out.sort();
}
