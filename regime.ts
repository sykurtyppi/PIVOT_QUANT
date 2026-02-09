/**
 * Regime Classification Module
 * Labels each bar as UP-TREND, DOWN-TREND, or RANGE based on ADX and EMA slope analysis
 */

export interface PriceBar {
    date: string;
    timestamp: number;
    open: number;
    high: number;
    low: number;
    close: number;
}

export interface RegimeData {
    date: string;
    regime: 'UP-TREND' | 'DOWN-TREND' | 'RANGE';
    adx: number;
    emaSlope: number;
    emaR2: number;
    ema21: number;
}

export class RegimeClassifier {
    private priceData: PriceBar[] = [];
    private regimeData: Map<string, RegimeData> = new Map();
    private adxPeriod: number = 14;
    private emaPeriod: number = 21;
    private slopePeriod: number = 10; // Period for slope calculation

    constructor(adxPeriod: number = 14, emaPeriod: number = 21) {
        this.adxPeriod = adxPeriod;
        this.emaPeriod = emaPeriod;
    }

    /**
     * Update regime data with new price bars
     */
    updateData(priceData: PriceBar[]): void {
        this.priceData = [...priceData].sort((a, b) => a.timestamp - b.timestamp);
        this.calculateRegimes();
    }

    /**
     * Get regime for specific date
     */
    getRegimeForDate(date: string): RegimeData | null {
        return this.regimeData.get(date) || null;
    }

    /**
     * Get current (latest) regime
     */
    getCurrentRegime(): RegimeData | null {
        if (this.priceData.length === 0) return null;
        const latestDate = this.priceData[this.priceData.length - 1].date;
        return this.getRegimeForDate(latestDate);
    }

    /**
     * Get all regime data
     */
    getAllRegimes(): RegimeData[] {
        return Array.from(this.regimeData.values()).sort((a, b) =>
            new Date(a.date).getTime() - new Date(b.date).getTime()
        );
    }

    /**
     * Main calculation method
     */
    private calculateRegimes(): void {
        if (this.priceData.length < Math.max(this.adxPeriod, this.emaPeriod, this.slopePeriod) + 5) {
            return; // Not enough data
        }

        // Calculate EMA21 for all bars
        const ema21Values = this.calculateEMA(this.priceData.map(bar => bar.close), this.emaPeriod);

        // Calculate ADX for all bars
        const adxValues = this.calculateADX(this.priceData, this.adxPeriod);

        // Calculate regime for each bar (starting from where we have enough data)
        const startIndex = Math.max(this.adxPeriod, this.emaPeriod, this.slopePeriod) + 2;

        for (let i = startIndex; i < this.priceData.length; i++) {
            const bar = this.priceData[i];
            const adx = adxValues[i] || 0;
            const ema21 = ema21Values[i] || 0;

            // Calculate EMA slope and R² for the last slopePeriod bars
            const slopeData = this.calculateEMASlope(ema21Values, i, this.slopePeriod);

            // Determine regime
            let regime: 'UP-TREND' | 'DOWN-TREND' | 'RANGE';
            if (adx >= 25) {
                if (slopeData.slope > 0) {
                    regime = 'UP-TREND';
                } else {
                    regime = 'DOWN-TREND';
                }
            } else {
                regime = 'RANGE';
            }

            const regimeData: RegimeData = {
                date: bar.date,
                regime,
                adx: isFinite(adx) ? adx : 0,
                emaSlope: isFinite(slopeData.slope) ? slopeData.slope : 0,
                emaR2: isFinite(slopeData.r2) ? slopeData.r2 : 0,
                ema21: isFinite(ema21) ? ema21 : 0
            };

            this.regimeData.set(bar.date, regimeData);
        }
    }

    /**
     * Calculate Exponential Moving Average
     */
    private calculateEMA(prices: number[], period: number): number[] {
        const ema: number[] = [];
        const multiplier = 2 / (period + 1);

        // Start with simple moving average for first value
        let sum = 0;
        for (let i = 0; i < period && i < prices.length; i++) {
            sum += prices[i];
        }
        ema[period - 1] = sum / period;

        // Calculate EMA for remaining values
        for (let i = period; i < prices.length; i++) {
            ema[i] = (prices[i] * multiplier) + (ema[i - 1] * (1 - multiplier));
        }

        return ema;
    }

    /**
     * Calculate Average Directional Index (ADX)
     */
    private calculateADX(bars: PriceBar[], period: number): number[] {
        const adx: number[] = [];
        const trueRanges: number[] = [];
        const plusDMs: number[] = [];
        const minusDMs: number[] = [];

        // Calculate True Range, +DM, and -DM
        for (let i = 1; i < bars.length; i++) {
            const current = bars[i];
            const previous = bars[i - 1];

            // True Range
            const tr = Math.max(
                current.high - current.low,
                Math.abs(current.high - previous.close),
                Math.abs(current.low - previous.close)
            );
            trueRanges.push(tr);

            // Directional Movement
            const highDiff = current.high - previous.high;
            const lowDiff = previous.low - current.low;

            const plusDM = (highDiff > lowDiff && highDiff > 0) ? highDiff : 0;
            const minusDM = (lowDiff > highDiff && lowDiff > 0) ? lowDiff : 0;

            plusDMs.push(plusDM);
            minusDMs.push(minusDM);
        }

        // Calculate smoothed TR, +DM, -DM
        const smoothedTR = this.calculateSmoothedAverage(trueRanges, period);
        const smoothedPlusDM = this.calculateSmoothedAverage(plusDMs, period);
        const smoothedMinusDM = this.calculateSmoothedAverage(minusDMs, period);

        // Calculate +DI, -DI, and ADX
        for (let i = 0; i < smoothedTR.length; i++) {
            if (smoothedTR[i] === 0) {
                adx.push(0);
                continue;
            }

            const plusDI = (smoothedPlusDM[i] / smoothedTR[i]) * 100;
            const minusDI = (smoothedMinusDM[i] / smoothedTR[i]) * 100;

            const diSum = plusDI + minusDI;
            const dx = diSum === 0 ? 0 : (Math.abs(plusDI - minusDI) / diSum) * 100;

            adx.push(dx);
        }

        // Smooth the DX values to get ADX
        const adxSmoothed = this.calculateSmoothedAverage(adx.slice(period - 1), period);

        // Pad the beginning with zeros
        const result = new Array(period).fill(0);
        return result.concat(adxSmoothed);
    }

    /**
     * Calculate smoothed average (Wilder's smoothing)
     */
    private calculateSmoothedAverage(values: number[], period: number): number[] {
        const result: number[] = [];

        if (values.length < period) return result;

        // First smoothed value is simple average
        let sum = 0;
        for (let i = 0; i < period; i++) {
            sum += values[i];
        }
        result.push(sum / period);

        // Subsequent values use Wilder's smoothing
        for (let i = period; i < values.length; i++) {
            const smoothed = (result[result.length - 1] * (period - 1) + values[i]) / period;
            result.push(smoothed);
        }

        return result;
    }

    /**
     * Calculate EMA slope and R² over specified period
     */
    private calculateEMASlope(emaValues: number[], currentIndex: number, period: number): { slope: number; r2: number } {
        if (currentIndex < period - 1) {
            return { slope: 0, r2: 0 };
        }

        const startIndex = currentIndex - period + 1;
        const yValues = emaValues.slice(startIndex, currentIndex + 1);
        const xValues = Array.from({ length: period }, (_, i) => i);

        // Linear regression calculation
        const n = period;
        const sumX = xValues.reduce((a, b) => a + b, 0);
        const sumY = yValues.reduce((a, b) => a + b, 0);
        const sumXY = xValues.reduce((sum, x, i) => sum + x * yValues[i], 0);
        const sumXX = xValues.reduce((sum, x) => sum + x * x, 0);
        const sumYY = yValues.reduce((sum, y) => sum + y * y, 0);

        // Slope calculation
        const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);

        // R² calculation
        const meanY = sumY / n;
        const ssTotal = yValues.reduce((sum, y) => sum + Math.pow(y - meanY, 2), 0);
        const ssResidual = yValues.reduce((sum, y, i) => {
            const predicted = slope * xValues[i] + (sumY - slope * sumX) / n;
            return sum + Math.pow(y - predicted, 2);
        }, 0);

        const r2 = ssTotal === 0 ? 1 : 1 - (ssResidual / ssTotal);

        return {
            slope: isFinite(slope) ? slope : 0,
            r2: isFinite(r2) ? Math.max(0, Math.min(1, r2)) : 0
        };
    }

    /**
     * Get regime statistics
     */
    getRegimeStats(): { upTrend: number; downTrend: number; range: number; total: number } {
        const regimes = this.getAllRegimes();
        const stats = {
            upTrend: 0,
            downTrend: 0,
            range: 0,
            total: regimes.length
        };

        regimes.forEach(regime => {
            switch (regime.regime) {
                case 'UP-TREND':
                    stats.upTrend++;
                    break;
                case 'DOWN-TREND':
                    stats.downTrend++;
                    break;
                case 'RANGE':
                    stats.range++;
                    break;
            }
        });

        return stats;
    }
}

// Create a global instance for easy access
export const regimeClassifier = new RegimeClassifier();

// Convenience functions for global access
export function getRegimeForDate(date: string): RegimeData | null {
    return regimeClassifier.getRegimeForDate(date);
}

export function getCurrentRegime(): RegimeData | null {
    return regimeClassifier.getCurrentRegime();
}

export function updateRegimeData(priceData: PriceBar[]): void {
    regimeClassifier.updateData(priceData);
}

export function getRegimeStats(): { upTrend: number; downTrend: number; range: number; total: number } {
    return regimeClassifier.getRegimeStats();
}