/**
 * SPY Data Generator - Realistic SPY data around 670 range
 *
 * Generates realistic SPY OHLC data for testing gamma flip calculations
 * Based on actual SPY price movements and volatility patterns
 *
 * @version 1.0.0
 * @author PIVOT_QUANT Team
 */

export class SPYDataGenerator {
    constructor() {
        this.basePrice = 670.62; // Current SPY level
        this.volatility = 0.012; // ~1.2% daily volatility
        this.trend = 0.0002; // Slight upward trend
    }

    /**
     * Generate realistic SPY OHLC data
     * @param {number} days - Number of days of data
     * @param {Date} endDate - End date (defaults to today)
     * @returns {Array} Array of OHLC candlestick data
     */
    generateSPYData(days = 60, endDate = new Date()) {
        const data = [];
        let currentPrice = this.basePrice - (days * this.trend * this.basePrice);

        for (let i = 0; i < days; i++) {
            const date = new Date(endDate);
            date.setDate(date.getDate() - (days - i - 1));

            const candle = this._generateDailyCandle(currentPrice, date);
            data.push(candle);

            currentPrice = candle.close;
        }

        return data;
    }

    /**
     * Generate a single day's OHLC candle
     * @param {number} previousClose - Previous day's close
     * @param {Date} date - Trading date
     * @returns {Object} OHLC candle
     */
    _generateDailyCandle(previousClose, date) {
        // Generate realistic price movements
        const gapPercent = (Math.random() - 0.5) * 0.003; // ±0.3% gap
        const open = previousClose * (1 + gapPercent);

        const dailyRange = this.volatility * open * (0.5 + Math.random() * 0.5);
        const trendComponent = this.trend * open;
        const randomComponent = (Math.random() - 0.5) * dailyRange;

        const close = open + trendComponent + randomComponent;

        // Calculate high and low with realistic intraday movement
        const highRange = dailyRange * (0.3 + Math.random() * 0.4);
        const lowRange = dailyRange * (0.3 + Math.random() * 0.4);

        const high = Math.max(open, close) + highRange;
        const low = Math.min(open, close) - lowRange;

        return {
            time: Math.floor(date.getTime() / 1000), // Unix timestamp
            open: this._roundToPrice(open),
            high: this._roundToPrice(high),
            low: this._roundToPrice(low),
            close: this._roundToPrice(close),
            volume: this._generateVolume(date)
        };
    }

    /**
     * Generate realistic volume data
     * @param {Date} date - Trading date
     * @returns {number} Volume
     */
    _generateVolume(date) {
        const baseVolume = 45000000; // SPY typical daily volume ~45M
        const dayOfWeek = date.getDay();

        // Higher volume on Monday/Friday, lower mid-week
        const dayMultiplier = dayOfWeek === 1 || dayOfWeek === 5 ? 1.2 :
                              dayOfWeek === 2 || dayOfWeek === 4 ? 1.0 : 0.85;

        const randomMultiplier = 0.7 + Math.random() * 0.6; // 0.7x to 1.3x

        return Math.floor(baseVolume * dayMultiplier * randomMultiplier);
    }

    /**
     * Round to realistic SPY price (2 decimal places)
     * @param {number} price - Raw price
     * @returns {number} Rounded price
     */
    _roundToPrice(price) {
        return Math.round(price * 100) / 100;
    }

    /**
     * Generate SPY data that matches the expected levels
     * @returns {Array} SPY data calibrated to expected gamma flip levels
     */
    generateCalibratedSPYData() {
        // Generate data that will produce the expected levels:
        // 672.52: 21d EMA, 670.52-670.72: gamma flip, etc.

        const data = [];
        const endDate = new Date();

        // Carefully crafted price sequence to achieve target EMAs
        const priceSequence = this._generateTargetedPriceSequence();

        for (let i = 0; i < 60; i++) {
            const date = new Date(endDate);
            date.setDate(date.getDate() - (60 - i - 1));

            const targetClose = priceSequence[i];
            const dailyRange = targetClose * 0.005; // 0.5% daily range

            const open = targetClose + (Math.random() - 0.5) * dailyRange * 0.5;
            const close = targetClose;
            const high = Math.max(open, close) + Math.random() * dailyRange * 0.4;
            const low = Math.min(open, close) - Math.random() * dailyRange * 0.4;

            data.push({
                time: Math.floor(date.getTime() / 1000),
                open: this._roundToPrice(open),
                high: this._roundToPrice(high),
                low: this._roundToPrice(low),
                close: this._roundToPrice(close),
                volume: this._generateVolume(date)
            });
        }

        return data;
    }

    /**
     * Generate price sequence to achieve target EMA levels
     * @returns {Array} Array of target closing prices
     */
    _generateTargetedPriceSequence() {
        // Target EMAs:
        // 21d EMA: 672.52, 9d EMA: ~669.57, 50d EMA: ~666.73, 9W EMA: ~665.63
        // Current price: ~670.62

        const sequence = [];

        // Build sequence working backwards from target EMAs
        // Start with lower prices early, gradual increase to support higher EMAs
        for (let i = 0; i < 15; i++) {
            sequence.push(662 + i * 0.8); // 662.0 to 673.2
        }

        // Mid period - establish 50d EMA base around 666-668
        for (let i = 0; i < 15; i++) {
            sequence.push(665 + Math.sin(i * 0.3) * 2); // Oscillate around 665-667
        }

        // Recent period to establish 21d EMA around 672.52
        for (let i = 0; i < 15; i++) {
            sequence.push(668 + i * 0.6); // 668 to 677
        }

        // Final period targeting current levels
        for (let i = 0; i < 15; i++) {
            const target = 670.62;
            const noise = Math.sin(i * 0.8) * 1.5;
            sequence.push(target + noise); // Around 670.62 ± 1.5
        }

        return sequence;
    }
}

export default SPYDataGenerator;