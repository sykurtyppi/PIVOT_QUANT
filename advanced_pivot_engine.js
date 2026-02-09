/* ---------- advanced_pivot_engine.js ----------
   Professional pivot calculations with ATR zones and gamma levels
   Designed for scalpers and institutional traders
   ------------------------------------------------*/

window.advancedPivotEngine = (() => {

    // ========= ATR CALCULATION =========
    function calculateATR(candles, period = 14) {
        if (candles.length < period + 1) return null;

        const trueRanges = [];

        for (let i = 1; i < candles.length; i++) {
            const current = candles[i];
            const previous = candles[i - 1];

            const tr1 = current.high - current.low;
            const tr2 = Math.abs(current.high - previous.close);
            const tr3 = Math.abs(current.low - previous.close);

            const trueRange = Math.max(tr1, tr2, tr3);
            trueRanges.push(trueRange);
        }

        // Calculate ATR using Wilder's smoothing method
        let atr = trueRanges.slice(0, period).reduce((sum, tr) => sum + tr, 0) / period;

        for (let i = period; i < trueRanges.length; i++) {
            atr = ((atr * (period - 1)) + trueRanges[i]) / period;
        }

        return atr;
    }

    // ========= PIVOT CALCULATIONS =========
    function calculateStandardPivots(high, low, close) {
        const pivot = (high + low + close) / 3;

        return {
            PIVOT: pivot,
            R1: 2 * pivot - low,
            R2: pivot + (high - low),
            R3: 2 * pivot + (high - low),
            S1: 2 * pivot - high,
            S2: pivot - (high - low),
            S3: 2 * pivot - (high - low)
        };
    }

    function calculateFibonacciPivots(high, low, close) {
        const pivot = (high + low + close) / 3;
        const range = high - low;

        return {
            PIVOT: pivot,
            'R0.236': pivot + 0.236 * range,
            'R0.382': pivot + 0.382 * range,
            'R0.618': pivot + 0.618 * range,
            'R1.000': pivot + range,
            'R1.272': pivot + 1.272 * range,
            'R1.618': pivot + 1.618 * range,
            'S0.236': pivot - 0.236 * range,
            'S0.382': pivot - 0.382 * range,
            'S0.618': pivot - 0.618 * range,
            'S1.000': pivot - range,
            'S1.272': pivot - 1.272 * range,
            'S1.618': pivot - 1.618 * range
        };
    }

    function calculateCamarillaPivots(high, low, close) {
        const range = high - low;

        return {
            PIVOT: close,
            R1: close + (range * 1.1 / 12),
            R2: close + (range * 1.1 / 6),
            R3: close + (range * 1.1 / 4),
            R4: close + (range * 1.1 / 2),
            S1: close - (range * 1.1 / 12),
            S2: close - (range * 1.1 / 6),
            S3: close - (range * 1.1 / 4),
            S4: close - (range * 1.1 / 2)
        };
    }

    // ========= GAMMA FLIP CALCULATION =========
    function calculateGammaFlip(candles, currentPrice, atr) {
        if (!candles || candles.length < 20) return null;

        // Simplified gamma flip approximation using volume-weighted price levels
        // In practice, you'd need options flow data for true gamma calculations

        const recentCandles = candles.slice(-20);
        const volumes = recentCandles.map(c => c.volume || 0);
        const totalVolume = volumes.reduce((sum, vol) => sum + vol, 0);

        if (totalVolume === 0) {
            // Fallback: estimate gamma flip as midpoint of recent range
            const highs = recentCandles.map(c => c.high);
            const lows = recentCandles.map(c => c.low);
            return (Math.max(...highs) + Math.min(...lows)) / 2;
        }

        // Volume-weighted average price as gamma proxy
        let vwap = 0;
        let cumVolume = 0;

        recentCandles.forEach(candle => {
            const volume = candle.volume || 0;
            const typicalPrice = (candle.high + candle.low + candle.close) / 3;
            vwap += typicalPrice * volume;
            cumVolume += volume;
        });

        if (cumVolume === 0) return currentPrice;

        vwap = vwap / cumVolume;

        // Adjust gamma flip based on ATR and volatility
        const _volatilityAdjustment = atr * 0.5;
        const distanceFromVWAP = Math.abs(currentPrice - vwap);

        if (distanceFromVWAP > atr) {
            // High volatility: gamma flip closer to current price
            return currentPrice + (vwap - currentPrice) * 0.3;
        }

        return vwap;
    }

    // ========= ATR-BASED ZONES =========
    function createATRZones(pivotLevels, atr, multipliers = { tight: 0.5, normal: 1.0, wide: 1.5 }) {
        const zones = {};

        Object.entries(pivotLevels).forEach(([label, price]) => {
            zones[label] = {
                value: price,
                zones: {
                    tight: {
                        high: price + (atr * multipliers.tight),
                        low: price - (atr * multipliers.tight)
                    },
                    normal: {
                        high: price + (atr * multipliers.normal),
                        low: price - (atr * multipliers.normal)
                    },
                    wide: {
                        high: price + (atr * multipliers.wide),
                        low: price - (atr * multipliers.wide)
                    }
                }
            };
        });

        return zones;
    }

    // ========= PIVOT STRENGTH ANALYSIS =========
    function analyzePivotStrength(pivotLevels, candles, currentPrice) {
        const analysis = {};
        const recentCandles = candles.slice(-10);

        Object.entries(pivotLevels).forEach(([label, price]) => {
            let touches = 0;
            let bounces = 0;
            let breaks = 0;

            recentCandles.forEach((candle, index) => {
                const tolerance = (candle.high - candle.low) * 0.1;

                // Check for touch
                if (Math.abs(candle.high - price) <= tolerance ||
                    Math.abs(candle.low - price) <= tolerance ||
                    (candle.low <= price && candle.high >= price)) {
                    touches++;

                    // Check for bounce (price returns to other side within 3 candles)
                    if (index < recentCandles.length - 3) {
                        const nextCandles = recentCandles.slice(index + 1, index + 4);
                        const isAboveLevel = price < candle.close;

                        const bounced = nextCandles.some(next => {
                            return isAboveLevel ? next.close < price : next.close > price;
                        });

                        if (bounced) bounces++;
                        else breaks++;
                    }
                }
            });

            const strength = touches > 0 ? (bounces / touches) : 0;

            analysis[label] = {
                price,
                touches,
                bounces,
                breaks,
                strength: Math.round(strength * 100),
                reliability: touches >= 2 ? (strength > 0.6 ? 'High' : 'Medium') : 'Low',
                distanceFromCurrent: ((price - currentPrice) / currentPrice * 100).toFixed(2)
            };
        });

        return analysis;
    }

    // ========= PUBLIC API =========
    function calculateAdvancedPivots(candles, type = 'standard', atrPeriod = 14) {
        if (!candles || candles.length < Math.max(atrPeriod + 1, 3)) {
            return null;
        }

        const latest = candles[candles.length - 1];
        const { high, low, close } = latest;
        const currentPrice = close;

        // Calculate ATR
        const atr = calculateATR(candles, atrPeriod);

        // Calculate pivot levels based on type
        let pivotLevels;
        switch (type) {
            case 'fibonacci':
                pivotLevels = calculateFibonacciPivots(high, low, close);
                break;
            case 'camarilla':
                pivotLevels = calculateCamarillaPivots(high, low, close);
                break;
            default:
                pivotLevels = calculateStandardPivots(high, low, close);
        }

        // Calculate gamma flip level
        const gammaFlip = calculateGammaFlip(candles, currentPrice, atr);

        // Add gamma flip to pivot levels
        if (gammaFlip) {
            pivotLevels['GAMMA_FLIP'] = gammaFlip;
        }

        // Create ATR-based zones
        const atrZones = createATRZones(pivotLevels, atr);

        // Analyze pivot strength
        const strength = analyzePivotStrength(pivotLevels, candles, currentPrice);

        return {
            type,
            atr: atr ? atr.toFixed(4) : null,
            atrPercent: atr ? ((atr / currentPrice) * 100).toFixed(2) : null,
            pivotLevels,
            atrZones,
            gammaFlip: gammaFlip ? gammaFlip.toFixed(2) : null,
            strength,
            metadata: {
                dataPoints: candles.length,
                atrPeriod,
                calculatedAt: new Date().toISOString(),
                currentPrice: currentPrice.toFixed(2)
            }
        };
    }

    // ========= ZONE VISUALIZATION HELPERS =========
    function getZoneConfiguration(type) {
        const configs = {
            standard: {
                colors: {
                    resistance: 'rgba(239, 83, 80, 0.15)',
                    support: 'rgba(102, 187, 106, 0.15)',
                    pivot: 'rgba(253, 216, 53, 0.2)',
                    gamma: 'rgba(147, 51, 234, 0.2)'
                }
            },
            fibonacci: {
                colors: {
                    resistance: 'rgba(245, 158, 11, 0.15)',
                    support: 'rgba(59, 130, 246, 0.15)',
                    pivot: 'rgba(253, 216, 53, 0.2)',
                    gamma: 'rgba(147, 51, 234, 0.2)'
                }
            },
            camarilla: {
                colors: {
                    resistance: 'rgba(168, 85, 247, 0.15)',
                    support: 'rgba(34, 197, 94, 0.15)',
                    pivot: 'rgba(253, 216, 53, 0.2)',
                    gamma: 'rgba(147, 51, 234, 0.2)'
                }
            }
        };

        return configs[type] || configs.standard;
    }

    function formatPivotData(advancedData) {
        if (!advancedData) return null;

        const formatted = [];

        Object.entries(advancedData.pivotLevels).forEach(([label, price]) => {
            const strengthData = advancedData.strength[label];
            const isGamma = label === 'GAMMA_FLIP';

            let type = 'pivot';
            if (!isGamma) {
                if (label.startsWith('R') || label.includes('R')) type = 'resistance';
                else if (label.startsWith('S') || label.includes('S')) type = 'support';
            } else {
                type = 'gamma';
            }

            formatted.push({
                label,
                price: Number(price).toFixed(2),
                type,
                atrZone: advancedData.atrZones[label]?.zones?.normal,
                strength: strengthData?.strength || 0,
                reliability: strengthData?.reliability || 'Unknown',
                distance: strengthData?.distanceFromCurrent || '0.00'
            });
        });

        // Sort by price (highest to lowest)
        formatted.sort((a, b) => Number(b.price) - Number(a.price));

        return formatted;
    }

    return {
        calculateAdvancedPivots,
        calculateATR,
        getZoneConfiguration,
        formatPivotData,
        analyzePivotStrength
    };
})();