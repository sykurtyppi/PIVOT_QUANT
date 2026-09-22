/**
 * Point-in-time daily, weekly, and monthly realized-volatility reference levels.
 *
 * The engine deliberately keeps these levels separate from option-derived expected
 * moves and proprietary support/resistance. Every horizon is anchored to the last
 * close from the immediately preceding session, completed week, or completed
 * month and uses only returns available at that anchor and by `generatedAt`.
 */

const ANNUALIZATION_SESSIONS = 252;
const DEFAULT_WINDOWS = [20, 60];
const DEFAULT_HORIZONS = ['daily', 'weekly', 'monthly'];

const HORIZON_RULES = {
  daily: 'prior_session_close',
  weekly: 'prior_completed_week_close',
  monthly: 'prior_completed_month_close',
};

function parseDateOnly(value, fieldName) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`${fieldName} must be an ISO date in YYYY-MM-DD format`);
  }
  const date = new Date(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== value) {
    throw new Error(`${fieldName} must be a valid ISO date`);
  }
  return date;
}

function parseTimestamp(value, fieldName) {
  if (typeof value !== 'string') {
    throw new Error(`${fieldName} must be an ISO timestamp with an explicit timezone`);
  }

  const match = value.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?(Z|[+-]\d{2}:\d{2})$/i
  );
  if (!match) {
    throw new Error(`${fieldName} must be an ISO timestamp with an explicit timezone`);
  }

  const [, yearText, monthText, dayText, hourText, minuteText, secondText = '0', , zone] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const maxDay = daysInMonth[month - 1] ?? 0;
  let validZone = true;
  if (zone.toUpperCase() !== 'Z') {
    const [offsetHours, offsetMinutes] = zone.slice(1).split(':').map(Number);
    validZone = offsetMinutes <= 59 &&
      (offsetHours < 14 || (offsetHours === 14 && offsetMinutes === 0));
  }

  if (
    month < 1 || month > 12 || day < 1 || day > maxDay ||
    hour > 23 || minute > 59 || second > 59 || !validZone
  ) {
    throw new Error(`${fieldName} must be a valid ISO timestamp with an explicit timezone`);
  }

  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    throw new Error(`${fieldName} must be a valid ISO timestamp with an explicit timezone`);
  }
  return date;
}

function dateKey(date) {
  return date.toISOString().slice(0, 10);
}

function addUtcDays(date, days) {
  const result = new Date(date.getTime());
  result.setUTCDate(result.getUTCDate() + days);
  return result;
}

function startOfIsoWeek(date) {
  const day = date.getUTCDay();
  const daysSinceMonday = day === 0 ? 6 : day - 1;
  return addUtcDays(date, -daysSinceMonday);
}

function endOfIsoWeek(date) {
  return addUtcDays(startOfIsoWeek(date), 4);
}

function startOfMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function endOfMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));
}

function isTradingSession(date, holidays) {
  const day = date.getUTCDay();
  return day !== 0 && day !== 6 && !holidays.has(dateKey(date));
}

function countTradingSessions(start, end, holidays) {
  let count = 0;
  for (let cursor = new Date(start.getTime()); cursor <= end; cursor = addUtcDays(cursor, 1)) {
    if (isTradingSession(cursor, holidays)) {
      count += 1;
    }
  }
  return count;
}

function sampleStandardDeviation(values) {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const squaredDeviations = values.reduce((sum, value) => sum + (value - mean) ** 2, 0);
  return Math.sqrt(squaredDeviations / (values.length - 1));
}

function periodEnd(horizon, asOfDate) {
  if (horizon === 'daily') {
    return asOfDate;
  }
  if (horizon === 'weekly') {
    return endOfIsoWeek(asOfDate);
  }
  return endOfMonth(asOfDate);
}

function parseInputBars(bars) {
  if (!Array.isArray(bars) || bars.length === 0) {
    throw new Error('bars must be a non-empty array');
  }

  return bars.map((bar, index) => {
    if (bar == null || typeof bar !== 'object') {
      throw new Error(`bars[${index}] must be an object`);
    }
    return {
      timestamp: parseTimestamp(bar.timestamp, `bars[${index}].timestamp`),
      close: bar.close,
      inputIndex: index,
    };
  });
}

function validateBars(bars, holidays) {
  let previousTimestamp = -Infinity;
  let previousSessionDate = null;
  return bars.map(bar => {
    const { timestamp, close, inputIndex } = bar;
    if (timestamp.getTime() <= previousTimestamp) {
      throw new Error('bar timestamps must be strictly increasing and unique');
    }
    const sessionDate = dateKey(timestamp);
    if (!isTradingSession(timestamp, holidays)) {
      throw new Error(`bars[${inputIndex}].timestamp must identify a trading session`);
    }
    if (sessionDate === previousSessionDate) {
      throw new Error('bars must contain at most one close per UTC session date');
    }
    if (typeof close !== 'number' || !Number.isFinite(close) || close <= 0) {
      throw new Error(`bars[${inputIndex}] must contain a positive finite close`);
    }
    previousTimestamp = timestamp.getTime();
    previousSessionDate = sessionDate;
    return { timestamp, close };
  });
}

function validateWindows(windows) {
  if (!Array.isArray(windows) || windows.length === 0) {
    throw new Error('volatilityWindows must be a non-empty array');
  }
  const normalized = windows.map(window => {
    if (!Number.isInteger(window) || window < 2) {
      throw new Error('volatilityWindows must contain integers greater than or equal to 2');
    }
    return window;
  });
  if (new Set(normalized).size !== normalized.length) {
    throw new Error('volatilityWindows must not contain duplicates');
  }
  return normalized;
}

function validateHorizons(horizons) {
  if (!Array.isArray(horizons) || horizons.length === 0) {
    throw new Error('horizons must be a non-empty array');
  }
  const requested = new Set(horizons);
  for (const horizon of requested) {
    if (!DEFAULT_HORIZONS.includes(horizon)) {
      throw new Error(`unsupported horizon: ${horizon}`);
    }
  }
  return DEFAULT_HORIZONS.filter(horizon => requested.has(horizon));
}

function buildVolatilityEstimates(history, windows) {
  const returns = [];
  for (let index = 1; index < history.length; index += 1) {
    returns.push(Math.log(history[index].close) - Math.log(history[index - 1].close));
  }

  return Object.fromEntries(
    windows.map(window => {
      if (returns.length < window) {
        return [
          String(window),
          {
            status: 'insufficient_data',
            requiredReturnObservations: window,
            availableReturnObservations: returns.length,
          },
        ];
      }

      const selectedReturns = returns.slice(-window);
      const daily = sampleStandardDeviation(selectedReturns);
      return [
        String(window),
        {
          status: 'available',
          returnObservations: window,
          daily,
          annualized: daily * Math.sqrt(ANNUALIZATION_SESSIONS),
          calculationDataFrom: history[history.length - window - 1].timestamp.toISOString(),
          calculationDataThrough: history[history.length - 1].timestamp.toISOString(),
        },
      ];
    })
  );
}

function priorPeriodAnchorDate(horizon, asOfDate, holidays) {
  let candidate;
  let lowerBound = null;

  if (horizon === 'daily') {
    candidate = addUtcDays(asOfDate, -1);
  } else if (horizon === 'weekly') {
    const currentWeekStart = startOfIsoWeek(asOfDate);
    candidate = addUtcDays(currentWeekStart, -3);
    lowerBound = addUtcDays(currentWeekStart, -7);
  } else {
    const currentMonthStart = startOfMonth(asOfDate);
    candidate = addUtcDays(currentMonthStart, -1);
    lowerBound = startOfMonth(candidate);
  }

  while (!isTradingSession(candidate, holidays)) {
    candidate = addUtcDays(candidate, -1);
  }

  if (lowerBound != null && candidate < lowerBound) {
    return null;
  }
  return candidate;
}

function findBarIndexBySessionDate(bars, targetDate) {
  if (targetDate == null) {
    return -1;
  }
  const target = dateKey(targetDate);
  for (let index = bars.length - 1; index >= 0; index -= 1) {
    if (dateKey(bars[index].timestamp) === target) {
      return index;
    }
  }
  return -1;
}

function unavailableHorizon(reason, sessionsRemaining, windows, volatilityEstimates = {}) {
  return {
    status: 'unavailable',
    reason,
    sessionsRemaining,
    anchor: null,
    selectedVolatilityWindow: null,
    realizedVolatility: null,
    volatilityEstimates,
    levels: null,
    quality: {
      status: 'unavailable',
      requestedWindow: windows[0],
      usedWindow: null,
    },
  };
}

function buildHorizon(horizon, bars, asOfDate, windows, holidays) {
  const expectedAnchorDate = priorPeriodAnchorDate(horizon, asOfDate, holidays);
  const anchorIndex = findBarIndexBySessionDate(bars, expectedAnchorDate);
  const sessionsRemaining = countTradingSessions(
    asOfDate,
    periodEnd(horizon, asOfDate),
    holidays
  );

  if (anchorIndex < 0) {
    return unavailableHorizon('missing_prior_period_close', sessionsRemaining, windows);
  }

  const anchorBar = bars[anchorIndex];
  const history = bars.slice(0, anchorIndex + 1);
  const volatilityEstimates = buildVolatilityEstimates(history, windows);
  const selectedWindow = windows.find(window => volatilityEstimates[String(window)].status === 'available');

  const base = {
    status: selectedWindow == null ? 'unavailable' : 'available',
    sessionsRemaining,
    anchor: {
      close: anchorBar.close,
      timestamp: anchorBar.timestamp.toISOString(),
      rule: HORIZON_RULES[horizon],
    },
    selectedVolatilityWindow: selectedWindow ?? null,
    volatilityEstimates,
  };

  if (selectedWindow == null) {
    return {
      ...base,
      reason: 'insufficient_volatility_history',
      realizedVolatility: null,
      levels: null,
      quality: {
        status: 'unavailable',
        requestedWindow: windows[0],
        usedWindow: null,
      },
    };
  }

  const selected = volatilityEstimates[String(selectedWindow)];
  const horizonSigma = selected.daily * Math.sqrt(sessionsRemaining);
  const levels = {
    anchor: anchorBar.close,
    lower2Sigma: anchorBar.close * Math.exp(-2 * horizonSigma),
    lower1Sigma: anchorBar.close * Math.exp(-horizonSigma),
    upper1Sigma: anchorBar.close * Math.exp(horizonSigma),
    upper2Sigma: anchorBar.close * Math.exp(2 * horizonSigma),
    horizonSigmaLogReturn: horizonSigma,
  };

  if (!Object.values(levels).every(Number.isFinite)) {
    return {
      ...base,
      status: 'unavailable',
      reason: 'non_finite_projected_levels',
      realizedVolatility: {
        daily: selected.daily,
        annualized: selected.annualized,
        returnObservations: selected.returnObservations,
      },
      levels: null,
      quality: {
        status: 'unavailable',
        requestedWindow: windows[0],
        usedWindow: selectedWindow,
      },
    };
  }

  return {
    ...base,
    realizedVolatility: {
      daily: selected.daily,
      annualized: selected.annualized,
      returnObservations: selected.returnObservations,
    },
    levels,
    quality: {
      status: selectedWindow === windows[0] ? 'complete' : 'fallback',
      requestedWindow: windows[0],
      usedWindow: selectedWindow,
    },
  };
}

export class MultiHorizonVolatilityLevels {
  /**
   * Calculate point-in-time volatility levels.
   *
   * `asOf` is the target session date. Bars after `generatedAt` cannot influence
   * any horizon calculation, and each horizon requires its immediately prior period close.
   */
  static calculate(bars, options = {}) {
    const symbol = typeof options.symbol === 'string' ? options.symbol.trim().toUpperCase() : '';
    if (!symbol) {
      throw new Error('symbol is required');
    }

    const asOfDate = parseDateOnly(options.asOf, 'asOf');
    const generatedAtDate = parseTimestamp(options.generatedAt, 'generatedAt');
    const windows = validateWindows(options.volatilityWindows ?? DEFAULT_WINDOWS);
    const horizons = validateHorizons(options.horizons ?? DEFAULT_HORIZONS);
    const holidayValues = options.holidays ?? [];
    if (!Array.isArray(holidayValues)) {
      throw new Error('holidays must be an array of YYYY-MM-DD values');
    }
    const holidays = new Set(
      holidayValues.map((holiday, index) => dateKey(parseDateOnly(holiday, `holidays[${index}]`)))
    );
    const inputBars = parseInputBars(bars);
    const calculationBars = validateBars(
      inputBars.filter(bar => bar.timestamp.getTime() <= generatedAtDate.getTime()),
      holidays
    );
    if (!isTradingSession(asOfDate, holidays)) {
      throw new Error('asOf must be a weekday trading session not listed in holidays');
    }

    const inputRange = inputBars.reduce(
      (range, bar) => ({
        from: Math.min(range.from, bar.timestamp.getTime()),
        through: Math.max(range.through, bar.timestamp.getTime()),
      }),
      { from: Infinity, through: -Infinity }
    );
    const inputDataFrom = new Date(inputRange.from).toISOString();
    const inputDataThrough = new Date(inputRange.through).toISOString();

    const horizonResults = Object.fromEntries(
      horizons.map(horizon => [
        horizon,
        buildHorizon(horizon, calculationBars, asOfDate, windows, holidays),
      ])
    );
    const calculationTimestamps = Object.values(horizonResults)
      .map(result => result.anchor?.timestamp)
      .filter(Boolean)
      .sort();

    return {
      schemaVersion: '1.0.0',
      symbol,
      asOf: dateKey(asOfDate),
      generatedAt: generatedAtDate.toISOString(),
      methodology: {
        name: 'realized_volatility_log_symmetric',
        estimator: 'sample_standard_deviation_of_close_to_close_log_returns',
        sessionScaling: 'square_root_of_sessions_remaining',
        sessionCalendar: 'weekday_with_explicit_holidays',
        bands: ['lower2Sigma', 'lower1Sigma', 'anchor', 'upper1Sigma', 'upper2Sigma'],
      },
      provenance: {
        priceField: 'close',
        returnType: 'log',
        annualizationSessions: ANNUALIZATION_SESSIONS,
        volatilityWindows: windows,
        inputBars: inputBars.length,
        inputDataFrom,
        inputDataThrough,
        calculationDataThrough:
          calculationTimestamps.length > 0
            ? calculationTimestamps[calculationTimestamps.length - 1]
            : null,
        lookAheadPolicy:
          'exclude_bars_after_generated_at_and_require_the_immediately_prior_period_close',
      },
      horizons: horizonResults,
    };
  }
}
