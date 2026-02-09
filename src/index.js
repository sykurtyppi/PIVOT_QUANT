/**
 * PIVOT_QUANT - Institutional-Grade Pivot Point Analysis System
 *
 * Main entry point for the QuantPivot system, providing a clean API
 * for institutional trading applications.
 *
 * @version 2.0.0
 * @author PIVOT_QUANT Team
 */

import { QuantPivotEngine } from './core/QuantPivotEngine.js';
import { ConfigurationManager } from './config/ConfigurationManager.js';
import { ValidationFramework } from './validation/ValidationFramework.js';
import { PerformanceMonitor } from './monitoring/PerformanceMonitor.js';
import { MathematicalModels } from './math/MathematicalModels.js';

    /**
     * Main QuantPivot class - Institutional API
     */
    export class QuantPivot {
        constructor(config = {}, environment = 'production') {
            this.environment = environment;
            this.config = ConfigurationManager.mergeWithDefaults(config, environment);
            this.engine = new QuantPivotEngine(this.config);

            // Initialize components
            this.validator = new ValidationFramework(this.config.validation);
            this.monitor = new PerformanceMonitor(this.config.performance);
            this.math = new MathematicalModels(this.config.mathematical);

            // State management
            this.isInitialized = true;
            this.version = '2.0.0';

            this._setupEventHandlers();
            this._logInitialization();
        }

    /**
     * Calculate comprehensive pivot analysis
     * @param {Array} ohlcData - OHLC price data with volume
     * @param {Object} options - Calculation options
     * @returns {Promise<Object>} Complete analysis results
     */
    async calculate(ohlcData, options = {}) {
        if (!this.isInitialized) {
            throw new Error('QuantPivot not initialized');
        }

        try {
            const sessionId = this.monitor.startSession('pivot_calculation', { options });

            // Pre-validation
            const validation = await this.validator.validateOHLCData(ohlcData, options);
            if (!validation.isValid) {
                throw new Error(`Data validation failed: ${validation.errors.join(', ')}`);
            }

            this.monitor.addCheckpoint(sessionId, 'validation_complete');

            // Execute calculation
            const results = await this.engine.calculatePivotLevels(ohlcData, options);

            this.monitor.addCheckpoint(sessionId, 'calculation_complete');

            // Post-validation
            const resultValidation = this.validator.validateCalculationResults(results);
            if (!resultValidation.isValid) {
                throw new Error(`Result validation failed: ${resultValidation.errors.join(', ')}`);
            }

            this.monitor.endSession(sessionId, { success: true });

            return {
                ...results,
                systemInfo: {
                    version: this.version,
                    environment: this.environment,
                    sessionId: sessionId,
                    dataQuality: validation.dataQuality
                }
            };

        } catch (error) {
            this.monitor.recordError('calculation', error);
            throw error;
        }
    }

    /**
     * Calculate pivot levels only (simplified API)
     * @param {Array} ohlcData - OHLC price data
     * @param {string} method - Pivot method ('standard', 'fibonacci', etc.)
     * @returns {Promise<Object>} Pivot levels
     */
    async calculateLevels(ohlcData, method = 'standard') {
        const results = await this.calculate(ohlcData, {
            methods: [method],
            includePerformance: false,
            statisticalAnalysis: false
        });

        return results.levels[method];
    }

    /**
     * Calculate ATR (Average True Range)
     * @param {Array} ohlcData - OHLC price data
     * @param {number} period - ATR period (default: 14)
     * @param {string} method - Calculation method ('wilder', 'ema', 'sma')
     * @returns {Promise<Object>} ATR data
     */
    async calculateATR(ohlcData, period = 14, method = 'wilder') {
        const trueRanges = await this.math.calculateTrueRange(ohlcData);
        return await this.math.calculateATR(trueRanges, period, method);
    }

    /**
     * Validate data quality
     * @param {Array} ohlcData - OHLC price data to validate
     * @returns {Promise<Object>} Validation report
     */
    async validateData(ohlcData) {
        return await this.validator.validateOHLCData(ohlcData);
    }

    /**
     * Get system performance metrics
     * @returns {Object} Performance metrics
     */
    getPerformanceMetrics() {
        return {
            engine: this.engine.getEngineStatus(),
            monitor: this.monitor.getMetrics(),
            version: this.version,
            environment: this.environment
        };
    }

    /**
     * Update system configuration
     * @param {Object} newConfig - Configuration updates
     */
    updateConfiguration(newConfig) {
        this.config = ConfigurationManager.mergeWithDefaults(newConfig, this.environment);
        this.engine.updateConfiguration(this.config);
        this.validator.updateConfig(this.config.validation);
        this.math.updateConfig(this.config.mathematical);
    }

    /**
     * Generate comprehensive system report
     * @param {Object} options - Report options
     * @returns {Object} System report
     */
    generateReport(options = {}) {
        return {
            timestamp: new Date().toISOString(),
            version: this.version,
            environment: this.environment,
            configuration: this.config,
            performance: this.getPerformanceMetrics(),
            monitoring: this.monitor.generateReport(options)
        };
    }

    /**
     * Reset system state
     */
    reset() {
        this.engine.reset();
        this.monitor.reset();
        this._logInfo('System reset completed');
    }

    /**
     * Dispose and cleanup resources
     */
    dispose() {
        this.engine.dispose();
        this.monitor.dispose();
        this.isInitialized = false;
        this._logInfo('System disposed');
    }

    // =================================================================================
    // ADVANCED API METHODS
    // =================================================================================

    /**
     * Batch process multiple datasets
     * @param {Array} datasets - Array of OHLC datasets
     * @param {Object} options - Processing options
     * @returns {Promise<Array>} Batch results
     */
    async batchProcess(datasets, options = {}) {
        const {
            concurrent = true,
            maxConcurrency = 5,
            onProgress = null
        } = options;

        if (concurrent) {
            // Process datasets concurrently with concurrency limit
            const results = [];
            const chunks = [];

            for (let i = 0; i < datasets.length; i += maxConcurrency) {
                chunks.push(datasets.slice(i, i + maxConcurrency));
            }

            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];
                const chunkPromises = chunk.map(async (data, index) => {
                    try {
                        const result = await this.calculate(data, options);
                        if (onProgress) {
                            onProgress(i * maxConcurrency + index + 1, datasets.length);
                        }
                        return { success: true, result, index: i * maxConcurrency + index };
                    } catch (error) {
                        return { success: false, error, index: i * maxConcurrency + index };
                    }
                });

                const chunkResults = await Promise.all(chunkPromises);
                results.push(...chunkResults);
            }

            return results;
        } else {
            // Process datasets sequentially
            const results = [];

            for (let i = 0; i < datasets.length; i++) {
                try {
                    const result = await this.calculate(datasets[i], options);
                    results.push({ success: true, result, index: i });
                } catch (error) {
                    results.push({ success: false, error, index: i });
                }

                if (onProgress) {
                    onProgress(i + 1, datasets.length);
                }
            }

            return results;
        }
    }

    /**
     * Real-time streaming calculation
     * @param {Function} dataSource - Function that returns new OHLC data
     * @param {Object} options - Streaming options
     * @returns {Object} Streaming controller
     */
    createStream(dataSource, options = {}) {
        const {
            interval = 1000, // 1 second
            bufferSize = 100,
            onUpdate = null,
            onError = null
        } = options;

        let isRunning = false;
        let intervalId = null;
        let buffer = [];

        const controller = {
            start() {
                if (isRunning) return;

                isRunning = true;
                intervalId = setInterval(async () => {
                    try {
                        const newData = await dataSource();
                        if (newData) {
                            buffer.push(newData);

                            if (buffer.length > bufferSize) {
                                buffer = buffer.slice(-bufferSize);
                            }

                            if (buffer.length >= 2) {
                                const results = await this.calculate(buffer, options);
                                if (onUpdate) {
                                    onUpdate(results);
                                }
                            }
                        }
                    } catch (error) {
                        if (onError) {
                            onError(error);
                        }
                    }
                }, interval);
            },

            stop() {
                if (!isRunning) return;

                isRunning = false;
                if (intervalId) {
                    clearInterval(intervalId);
                    intervalId = null;
                }
            },

            isRunning() {
                return isRunning;
            },

            getBuffer() {
                return [...buffer];
            },

            clearBuffer() {
                buffer = [];
            }
        };

        return controller;
    }

    /**
     * Historical backtesting
     * @param {Array} historicalData - Historical OHLC data
     * @param {Object} strategy - Backtesting strategy
     * @returns {Promise<Object>} Backtest results
     */
    async backtest(historicalData, strategy) {
        const {
            lookbackPeriod = 100,
            rebalanceFrequency = 1, // days
            initialCapital = 100000,
            commission = 0.001
        } = strategy;

        const results = {
            trades: [],
            performance: {},
            metrics: {},
            periods: []
        };

        for (let i = lookbackPeriod; i < historicalData.length; i += rebalanceFrequency) {
            const window = historicalData.slice(i - lookbackPeriod, i);
            const currentBar = historicalData[i];

            try {
                const pivotResults = await this.calculate(window);
                const signals = strategy.generateSignals(pivotResults, currentBar);

                if (signals.length > 0) {
                    signals.forEach(signal => {
                        const trade = {
                            timestamp: currentBar.timestamp,
                            type: signal.type,
                            price: signal.price,
                            size: signal.size,
                            pivot: signal.pivot,
                            confidence: signal.confidence
                        };

                        results.trades.push(trade);
                    });
                }

                results.periods.push({
                    timestamp: currentBar.timestamp,
                    pivotLevels: pivotResults.levels,
                    signals: signals
                });

            } catch (error) {
                this.monitor.recordError('backtest', error);
                /* eslint-disable-next-line no-console */
                console.warn(`Backtest error at index ${i}:`, error.message);
            }
        }

        // Calculate performance metrics
        results.performance = this._calculateBacktestPerformance(
            results.trades,
            initialCapital,
            commission
        );

        return results;
    }

    // =================================================================================
    // PRIVATE METHODS
    // =================================================================================

    _setupEventHandlers() {
        // Handle configuration changes
        ConfigurationManager.getInstance().subscribe((env, _config) => {
            if (env === this.environment) {
                this._logInfo('Configuration updated');
            }
        });

        // Handle unhandled errors
        if (typeof window !== 'undefined') {
            window.addEventListener('unhandledrejection', (event) => {
                this.monitor.recordError('unhandled_rejection', event.reason);
            });
        }
    }

    _calculateBacktestPerformance(trades, initialCapital, commission) {
        if (trades.length === 0) {
            return { totalReturn: 0, sharpeRatio: 0, maxDrawdown: 0 };
        }

        let capital = initialCapital;
        let peak = initialCapital;
        let maxDrawdown = 0;
        const returns = [];

        trades.forEach(trade => {
            const tradeCost = trade.price * trade.size;
            const commissionCost = tradeCost * commission;

            if (trade.type === 'buy') {
                capital -= (tradeCost + commissionCost);
            } else {
                capital += (tradeCost - commissionCost);
            }

            if (capital > peak) peak = capital;
            const drawdown = ((peak - capital) / peak) * 100;
            if (drawdown > maxDrawdown) maxDrawdown = drawdown;

            const returnPct = ((capital - initialCapital) / initialCapital) * 100;
            returns.push(returnPct);
        });

        const totalReturn = ((capital - initialCapital) / initialCapital) * 100;
        const avgReturn = returns.reduce((sum, ret) => sum + ret, 0) / returns.length;
        const stdReturn = Math.sqrt(
            returns.reduce((sum, ret) => sum + Math.pow(ret - avgReturn, 2), 0) / returns.length
        );
        const sharpeRatio = stdReturn > 0 ? avgReturn / stdReturn : 0;

        return {
            totalReturn,
            sharpeRatio,
            maxDrawdown,
            finalCapital: capital,
            numberOfTrades: trades.length,
            avgReturn,
            volatility: stdReturn
        };
    }

    _logInitialization() {
        this._logInfo('QuantPivot System Initialized', {
            version: this.version,
            environment: this.environment,
            features: {
                validation: true,
                monitoring: true,
                mathematical: true,
                performance: true
            }
        });
    }

    _logInfo(message, data = {}) {
        if (this.config.logging.level >= 2) {
            /* eslint-disable-next-line no-console */
            console.info(`[QuantPivot] ${message}`, data);
        }
    }
}

// Export individual components for advanced usage
export {
    QuantPivotEngine,
    ConfigurationManager,
    ValidationFramework,
    PerformanceMonitor,
    MathematicalModels
};

// Export factory functions for common use cases
export const createQuantPivot = (_config, environment) => new QuantPivot(_config, environment);

export const createDevelopmentInstance = (config = {}) =>
    new QuantPivot(_config, 'development');

export const createProductionInstance = (config = {}) =>
    new QuantPivot(_config, 'production');

export const createHFTInstance = (config = {}) =>
    new QuantPivot(_config, 'hft');

// Default export for convenience
export default QuantPivot;
