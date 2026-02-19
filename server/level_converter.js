const DEFAULT_PRICES = Object.freeze({
  SPY: 580.0,
  SPX: 5800.0,
  US500: 5800.0,
  ES: 5806.0,
});

const INSTRUMENT_ALIASES = new Map([
  ['SPY', 'SPY'],
  ['SPX', 'SPX'],
  ['S&P500', 'SPX'],
  ['S&P 500', 'SPX'],
  ['SP500', 'SPX'],
  ['US500', 'US500'],
  ['US 500', 'US500'],
  ['ES', 'ES'],
]);

function toFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function mathInstrument(instrument) {
  return instrument === 'US500' ? 'SPX' : instrument;
}

export function normalizeInstrument(rawInstrument, fallback = 'SPY') {
  const candidate = String(rawInstrument || '').trim().toUpperCase();
  if (!candidate) return fallback;
  return INSTRUMENT_ALIASES.get(candidate) || fallback;
}

export function buildConversionSnapshot({
  prices = {},
  mode = 'prior_close',
  source = 'mixed',
  asOf = new Date().toISOString(),
  esBasisMode = true,
} = {}) {
  const spy = toFiniteNumber(prices.SPY, DEFAULT_PRICES.SPY);
  const spx = toFiniteNumber(prices.SPX, DEFAULT_PRICES.SPX);
  const us500 = toFiniteNumber(prices.US500, spx);
  const es = toFiniteNumber(prices.ES, spx + (DEFAULT_PRICES.ES - DEFAULT_PRICES.SPX));

  const ratios = {
    SPX_SPY: spx / Math.max(spy, 0.000001),
    ES_SPX: es / Math.max(spx, 0.000001),
    ES_SPY: es / Math.max(spy, 0.000001),
  };

  return {
    mode,
    source,
    asOf,
    esBasisMode: Boolean(esBasisMode),
    prices: {
      SPY: spy,
      SPX: spx,
      US500: us500,
      ES: es,
    },
    ratios,
    esBasis: es - spx,
  };
}

function toSpxValue(value, instrument, snapshot, esBasisMode) {
  const mathInst = mathInstrument(instrument);
  if (mathInst === 'SPY') {
    return value * snapshot.ratios.SPX_SPY;
  }
  if (mathInst === 'SPX') {
    return value;
  }
  if (mathInst === 'ES') {
    if (esBasisMode) {
      return value - snapshot.esBasis;
    }
    return value / snapshot.ratios.ES_SPX;
  }
  return value;
}

function fromSpxValue(spxValue, instrument, snapshot, esBasisMode) {
  const mathInst = mathInstrument(instrument);
  if (mathInst === 'SPY') {
    return spxValue / snapshot.ratios.SPX_SPY;
  }
  if (mathInst === 'SPX') {
    return spxValue;
  }
  if (mathInst === 'ES') {
    if (esBasisMode) {
      return spxValue + snapshot.esBasis;
    }
    return spxValue * snapshot.ratios.ES_SPX;
  }
  return spxValue;
}

export function convertValue({
  value,
  fromInstrument,
  toInstrument,
  snapshot,
  esBasisMode = true,
} = {}) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return numericValue;

  const fromInst = normalizeInstrument(fromInstrument, 'SPY');
  const toInst = normalizeInstrument(toInstrument, 'SPX');
  const safeSnapshot = snapshot || buildConversionSnapshot({});
  const useBasis = Boolean(esBasisMode);

  const spxValue = toSpxValue(numericValue, fromInst, safeSnapshot, useBasis);
  return fromSpxValue(spxValue, toInst, safeSnapshot, useBasis);
}

export function convertLevels({
  levels,
  fromInstrument,
  toInstrument,
  snapshot,
  esBasisMode = true,
} = {}) {
  if (!Array.isArray(levels)) {
    throw new Error('levels must be an array');
  }

  const fromInst = normalizeInstrument(fromInstrument, 'SPY');
  const toInst = normalizeInstrument(toInstrument, 'SPX');
  const safeSnapshot = snapshot || buildConversionSnapshot({});
  const useBasis = Boolean(esBasisMode);
  const ratio = convertValue({
    value: 1.0,
    fromInstrument: fromInst,
    toInstrument: toInst,
    snapshot: safeSnapshot,
    esBasisMode: useBasis,
  });

  const convertedLevels = levels.map((level) => {
    const baseRecord = level && typeof level === 'object' ? { ...level } : { value: level };
    const rawValue = Number(baseRecord.value);
    if (!Number.isFinite(rawValue)) {
      return baseRecord;
    }
    return {
      ...baseRecord,
      value: convertValue({
        value: rawValue,
        fromInstrument: fromInst,
        toInstrument: toInst,
        snapshot: safeSnapshot,
        esBasisMode: useBasis,
      }),
    };
  });

  return {
    levels: convertedLevels,
    metadata: {
      fromInstrument: fromInst,
      toInstrument: toInst,
      ratio,
      mode: safeSnapshot.mode,
      source: safeSnapshot.source,
      asOf: safeSnapshot.asOf,
      prices: safeSnapshot.prices,
      ratios: safeSnapshot.ratios,
      esBasis: safeSnapshot.esBasis,
      esBasisMode: useBasis,
    },
  };
}
