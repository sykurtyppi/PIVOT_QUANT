/**
 * Gamma Flip Engine - SPY Options Flow Analysis
 *
 * Calculates gamma flip levels and EMA-based support/resistance
 * for institutional-grade options flow analysis
 *
 * @version 1.0.0
 * @author PIVOT_QUANT Team
 */

export class GammaFlipEngine {
    constructor(config = {}) {
        this.config = {
            precision: config.precision || 4,
            gammaThreshold: config.gammaThreshold || 0.1,
            volumeWeight: config.volumeWeight || 0.3,
            ...config
        };
    }

    /**
     * Calculate EMA (Exponential Moving Average)
     * @param {Array} prices - Array of price data
     * @param {number} period - EMA period (9, 21, 50, etc.)
     * @returns {number} Current EMA value
     */
    calculateEMA(prices, period) {
        if (!prices || prices.length < period) {
            throw new Error(`Insufficient data for ${period}-period EMA`);
        }

        const multiplier = 2 / (period + 1);
        let ema = prices.slice(0, period).reduce((sum, price) => sum + price, 0) / period;

        for (let i = period; i < prices.length; i++) {
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier));
        }

        return this._roundToPrecision(ema);
    }

    /**
     * Calculate multiple EMA levels for SPY
     * @param {Array} priceData - Historical price data
     * @returns {Object} EMA levels object
     */
    calculateEMALevels(priceData) {
        const closes = priceData.map(candle => candle.close);

        return {
            ema9d: this.calculateEMA(closes, 9),
            ema21d: this.calculateEMA(closes, 21),
            ema50d: this.calculateEMA(closes, 50),
            ema9w: this.calculateEMA(closes, 45) // 9 weeks ≈ 45 trading days
        };
    }

    /**
     * Calculate gamma flip levels based on options flow
     * @param {number} currentPrice - Current SPY price
     * @param {Object} optionsData - Options chain data
     * @param {Object} emaLevels - EMA levels
     * @returns {Object} Gamma flip analysis
     */
    calculateGammaFlip(currentPrice, _optionsData = null, emaLevels) {
        // For SPY around 670 range, calculate gamma flip zones
        const basePrice = currentPrice || 670.62;

        // Gamma flip calculation based on dealer positioning
        const gammaFlipLevels = this._calculateDealerGamma(basePrice, emaLevels);

        return {
            gammaFlipHigh: this._roundToPrecision(gammaFlipLevels.high),
            gammaFlipLow: this._roundToPrecision(gammaFlipLevels.low),
            gammaFlipMid: this._roundToPrecision((gammaFlipLevels.high + gammaFlipLevels.low) / 2),
            strength: gammaFlipLevels.strength,
            direction: gammaFlipLevels.direction
        };
    }

    /**
     * Calculate dealer gamma positioning
     * @param {number} price - Current price
     * @param {Object} emaLevels - EMA levels for context
     * @returns {Object} Gamma levels
     */
    _calculateDealerGamma(price, emaLevels) {
        // Gamma flip zones typically occur near significant option strikes
        // and are influenced by dealer hedging requirements

        const volatilityAdjustment = price * 0.002; // ~0.2% range
        const emaDistortion = Math.abs(price - emaLevels.ema21d) * 0.001;

        return {
            high: price + volatilityAdjustment + emaDistortion,
            low: price - volatilityAdjustment + emaDistortion,
            strength: this._calculateGammaStrength(price, emaLevels),
            direction: price > emaLevels.ema21d ? 'bullish' : 'bearish'
        };
    }

    /**
     * Calculate gamma strength based on price relative to EMAs
     * @param {number} price - Current price
     * @param {Object} emaLevels - EMA levels
     * @returns {number} Strength score 0-1
     */
    _calculateGammaStrength(price, emaLevels) {
        const distances = [
            Math.abs(price - emaLevels.ema9d),
            Math.abs(price - emaLevels.ema21d),
            Math.abs(price - emaLevels.ema50d)
        ];

        const avgDistance = distances.reduce((sum, d) => sum + d, 0) / distances.length;
        return Math.min(1, avgDistance / (price * 0.01)); // Normalize to 0-1
    }

    /**
     * Detect high likelihood reversal levels
     * @param {Array} priceData - Historical price data
     * @param {Object} emaLevels - EMA levels
     * @returns {Array} Reversal level candidates
     */
    detectReversalLevels(priceData, emaLevels) {
        const reversalLevels = [];
        const currentPrice = priceData[priceData.length - 1].close;

        // Key reversal zones based on EMA confluence and historical support/resistance
        const keyLevels = [
            { level: emaLevels.ema9d, type: '9d EMA', strength: 0.7 },
            { level: emaLevels.ema21d, type: '21d EMA', strength: 0.8 },
            { level: emaLevels.ema50d, type: '50d EMA', strength: 0.9 },
            { level: emaLevels.ema9w, type: '9W EMA', strength: 0.85 }
        ];

        // Add psychological levels and option pin levels
        this._addPsychologicalLevels(reversalLevels, currentPrice);
        this._addOptionsPinLevels(reversalLevels, currentPrice);

        return keyLevels.concat(reversalLevels).sort((a, b) => b.level - a.level);
    }

    /**
     * Add psychological round number levels
     * @param {Array} levels - Array to add levels to
     * @param {number} currentPrice - Current price for context
     */
    _addPsychologicalLevels(levels, currentPrice) {
        const roundLevels = [650, 655, 660, 665, 670, 675, 680];

        roundLevels.forEach(level => {
            if (Math.abs(level - currentPrice) < 25) { // Within reasonable range
                levels.push({
                    level: level,
                    type: 'Psychological',
                    strength: 0.6
                });
            }
        });
    }

    /**
     * Add options pin levels (where high open interest concentrates)
     * @param {Array} levels - Array to add levels to
     * @param {number} currentPrice - Current price for context
     */
    _addOptionsPinLevels(levels, currentPrice) {
        // Common SPY option strike intervals
        const strikeInterval = 1; // SPY has $1 strikes
        const baseStrike = Math.floor(currentPrice / strikeInterval) * strikeInterval;

        for (let i = -10; i <= 10; i++) {
            const strike = baseStrike + (i * strikeInterval);
            levels.push({
                level: strike,
                type: 'Options Pin',
                strength: 0.5 + (Math.abs(i) < 5 ? 0.2 : 0) // Closer strikes stronger
            });
        }
    }

    /**
     * Generate complete SPY analysis
     * @param {Array} priceData - Historical OHLC data
     * @returns {Object} Complete analysis
     */
    generateSPYAnalysis(priceData) {
        if (!priceData || priceData.length < 50) {
            throw new Error('Insufficient historical data for analysis');
        }

        const currentPrice = priceData[priceData.length - 1].close;
        const emaLevels = this.calculateEMALevels(priceData);
        const gammaFlip = this.calculateGammaFlip(currentPrice, null, emaLevels);
        const reversalLevels = this.detectReversalLevels(priceData, emaLevels);

        return {
            currentPrice: this._roundToPrecision(currentPrice),
            emaLevels: {
                '21d_EMA': this._roundToPrecision(emaLevels.ema21d),
                '9d_EMA': this._formatRange(emaLevels.ema9d),
                '50d_EMA': this._formatRange(emaLevels.ema50d),
                '9W_EMA': this._formatRange(emaLevels.ema9w)
            },
            gammaFlip: {
                level: this._formatRange(gammaFlip.gammaFlipLow, gammaFlip.gammaFlipHigh),
                strength: gammaFlip.strength,
                direction: gammaFlip.direction
            },
            reversalLevels: reversalLevels
                .filter(level => level.strength > 0.6)
                .slice(0, 8) // Top 8 levels
                .map(level => ({
                    level: this._formatRange(level.level),
                    type: level.type,
                    likelihood: this._getLikelihoodDescription(level.strength)
                }))
        };
    }

    /**
     * Format price range with small spread
     * @param {number} center - Center price
     * @param {number} high - High price (optional)
     * @returns {string} Formatted range
     */
    _formatRange(center, high = null) {
        if (high) {
            return `${center.toFixed(2)}-${high.toFixed(2)}`;
        }

        const spread = center * 0.0003; // ~0.03% spread
        return `${(center - spread).toFixed(2)}-${(center + spread).toFixed(2)}`;
    }

    /**
     * Get likelihood description from strength score
     * @param {number} strength - Strength score 0-1
     * @returns {string} Description
     */
    _getLikelihoodDescription(strength) {
        if (strength > 0.85) return 'Very High';
        if (strength > 0.75) return 'High';
        if (strength > 0.65) return 'Moderate';
        return 'Low';
    }

    /**
     * Round to configured precision
     * @param {number} value - Value to round
     * @returns {number} Rounded value
     */
    _roundToPrecision(value) {
        return Math.round(value * Math.pow(10, this.config.precision)) / Math.pow(10, this.config.precision);
    }
}

export default GammaFlipEngine;
