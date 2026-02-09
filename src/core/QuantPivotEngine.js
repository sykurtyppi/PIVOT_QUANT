/**
 * QuantPivot Engine - Institutional-Grade Pivot Point Analysis
 *
 * Precision-engineered pivot calculation system for quantitative trading firms.
 * Implements rigorous mathematical models with comprehensive error handling,
 * performance optimization, and institutional-grade validation.
 *
 * @version 2.0.0
 * @author PIVOT_QUANT Team
 * @license Proprietary - Internal Use Only
 */

import { ValidationFramework } from '../validation/ValidationFramework.js';
import { MathematicalModels } from '../math/MathematicalModels.js';
import { PerformanceMonitor } from '../monitoring/PerformanceMonitor.js';
import { ConfigurationManager } from '../config/ConfigurationManager.js';

export class QuantPivotEngine {
    /**
     * Initialize the Quant Pivot Engine with institutional-grade configuration
     * @param {Object} config - Engine configuration
     */
    constructor(config = {}) {
        this.config = ConfigurationManager.mergeWithDefaults(config);
        this.validator = new ValidationFramework(this.config.validation);
        this.mathModels = new MathematicalModels(this.config.mathematical);
        this.monitor = new PerformanceMonitor(this.config.performance);

        // Cache for performance optimization
        this.cache = new Map();
        this.cacheExpiry = new Map();

        // State management
        this.state = {
            isInitialized: false,
            lastCalculation: null,
            calculationCount: 0,
            errorCount: 0
        };

        this._initialize();
    }

    /**
     * Internal initialization with comprehensive setup
     * @private
     */
    _initialize() {
        try {
            this.monitor.startSession('engine_initialization');

            // Validate configuration
            this.validator.validateEngineConfig(this.config);

            // Initialize mathematical models
            this.mathModels.initialize();

            // Set up error handling
            this._setupErrorHandling();

            // Initialize cache management
            this._setupCacheManagement();

            this.state.isInitialized = true;
            this.monitor.endSession('engine_initialization');

            this._logInfo('QuantPivotEngine initialized successfully');
        } catch (error) {
            this._handleError('Initialization failed', error);
            throw new Error(`QuantPivotEngine initialization failed: ${error.message}`);
        }
    }

    /**
     * Calculate institutional-grade pivot levels with comprehensive analysis
     * @param {Array} ohlcData - OHLC price data with volume
     * @param {Object} options - Calculation options
     * @returns {Promise<Object>} Comprehensive pivot analysis results
     */
    async calculatePivotLevels(ohlcData, options = {}) {
        const sessionId = this.monitor.startSession('pivot_calculation');

        try {
            // Input validation with detailed error reporting
            const validationResult = await this.validator.validateOHLCData(ohlcData, options);
            if (!validationResult.isValid) {
                throw new Error(`Data validation failed: ${validationResult.errors.join(', ')}`);
            }

            // Check cache for identical calculations
            const cacheKey = this._generateCacheKey(ohlcData, options);
            const cachedResult = this._getCachedResult(cacheKey);
            if (cachedResult) {
                this.monitor.recordCacheHit(sessionId);
                return cachedResult;
            }

            // Merge options with defaults
            const calcOptions = {
                ...this.config.defaultOptions,
                ...options
            };

            // Perform comprehensive calculations
            const results = await this._performCalculations(ohlcData, calcOptions);

            // Cache results for performance
            this._setCachedResult(cacheKey, results, calcOptions.cacheTTL);

            // Update state and monitoring
            this.state.lastCalculation = Date.now();
            this.state.calculationCount++;
            this.monitor.endSession(sessionId, { success: true });

            return results;

        } catch (error) {
            this.state.errorCount++;
            this.monitor.endSession(sessionId, { success: false, error: error.message });
            this._handleError('Pivot calculation failed', error);
            throw error;
        }
    }

    /**
     * Core calculation engine with advanced mathematical models
     * @private
     */
    async _performCalculations(ohlcData, options) {
        const results = {
            metadata: {
                timestamp: Date.now(),
                dataPoints: ohlcData.length,
                calculationType: options.type,
                configuration: { ...options }
            },
            levels: {},
            analysis: {},
            risk: {},
            performance: {}
        };

        // Extract latest price data
        const latestData = this._extractLatestData(ohlcData, options.lookback);

        // 1. Calculate True Range and ATR with institutional precision
        const trueRangeData = await this.mathModels.calculateTrueRange(latestData);
        const _atrData = await this.mathModels.calculateATR(
            trueRangeData,
            options.atrPeriod,
            options.atrMethod
        );

        // 2. Calculate multiple pivot methodologies
        const pivotMethods = {
            standard: () => this.mathModels.calculateStandardPivots(latestData),
            fibonacci: () => this.mathModels.calculateFibonacciPivots(latestData),
            camarilla: () => this.mathModels.calculateCamarillaPivots(latestData),
            woodie: () => this.mathModels.calculateWoodiePivots(latestData),
            demark: () => this.mathModels.calculateDeMarkPivots(latestData)
        };

        const selectedMethods = options.methods || ['standard', 'fibonacci'];
        for (const method of selectedMethods) {
            if (pivotMethods[method]) {
                results.levels[method] = await pivotMethods[method]();
            }
        }

        // 3. Advanced zone analysis with probability weighting
        results.analysis.zones = await this.mathModels.calculateProbabilityZones(
            results.levels,
            _atrData,
            options.zoneMultipliers
        );

        // 4. Gamma exposure estimation
        if (options.includeGamma) {
            results.analysis.gamma = await this.mathModels.estimateGammaExposure(
                ohlcData,
                results.levels,
                options.gammaConfig
            );
        }

        // 5. Statistical significance testing
        if (options.statisticalAnalysis) {
            results.analysis.significance = await this.mathModels.performSignificanceAnalysis(
                ohlcData,
                results.levels,
                options.significanceConfig
            );
        }

        // 6. Risk metrics calculation
        results.risk = await this._calculateRiskMetrics(ohlcData, results.levels, _atrData);

        // 7. Performance attribution
        if (options.includePerformance) {
            results.performance = await this._calculatePerformanceMetrics(
                ohlcData,
                results.levels
            );
        }

        // 8. Quality scores for each level
        results.analysis.qualityScores = await this._calculateQualityScores(
            results.levels,
            ohlcData,
            _atrData
        );

        return results;
    }

    /**
     * Calculate comprehensive risk metrics
     * @private
     */
    async _calculateRiskMetrics(ohlcData, levels, _atrData) {
        return {
            volatility: {
                realized: this.mathModels.calculateRealizedVolatility(ohlcData),
                implied: this.mathModels.estimateImpliedVolatility(ohlcData),
                regime: this.mathModels.classifyVolatilityRegime(ohlcData)
            },
            drawdown: {
                maximum: this.mathModels.calculateMaxDrawdown(ohlcData),
                current: this.mathModels.calculateCurrentDrawdown(ohlcData),
                duration: this.mathModels.calculateDrawdownDuration(ohlcData)
            },
            var: {
                parametric: this.mathModels.calculateParametricVaR(ohlcData, 0.05),
                historical: this.mathModels.calculateHistoricalVaR(ohlcData, 0.05),
                monteCarlo: this.mathModels.calculateMonteCarloVaR(ohlcData, 0.05)
            },
            correlation: {
                levelCorrelation: this.mathModels.calculateLevelCorrelations(levels),
                priceCorrelation: this.mathModels.calculatePriceCorrelations(ohlcData)
            }
        };
    }

    /**
     * Calculate performance attribution metrics
     * @private
     */
    async _calculatePerformanceMetrics(ohlcData, levels) {
        return {
            levelAccuracy: this.mathModels.calculateLevelAccuracy(ohlcData, levels),
            sharpeRatio: this.mathModels.calculateSharpeRatio(ohlcData),
            informationRatio: this.mathModels.calculateInformationRatio(ohlcData),
            calmarRatio: this.mathModels.calculateCalmarRatio(ohlcData),
            sortinoRatio: this.mathModels.calculateSortinoRatio(ohlcData),
            treynorRatio: this.mathModels.calculateTreynorRatio(ohlcData),
            trackingError: this.mathModels.calculateTrackingError(ohlcData),
            alpha: this.mathModels.calculateAlpha(ohlcData),
            beta: this.mathModels.calculateBeta(ohlcData)
        };
    }

    /**
     * Calculate quality scores for pivot levels
     * @private
     */
    async _calculateQualityScores(levels, ohlcData, _atrData) {
        const scores = {};

        for (const [method, methodLevels] of Object.entries(levels)) {
            scores[method] = {};

            for (const [levelName, levelValue] of Object.entries(methodLevels)) {
                scores[method][levelName] = {
                    reliability: this.mathModels.calculateReliabilityScore(
                        levelValue, ohlcData, _atrData
                    ),
                    strength: this.mathModels.calculateStrengthScore(
                        levelValue, ohlcData
                    ),
                    confidence: this.mathModels.calculateConfidenceInterval(
                        levelValue, ohlcData, 0.95
                    ),
                    probability: this.mathModels.calculateHitProbability(
                        levelValue, ohlcData
                    )
                };
            }
        }

        return scores;
    }

    /**
     * Extract latest data based on lookback period
     * @private
     */
    _extractLatestData(ohlcData, lookback) {
        const startIndex = Math.max(0, ohlcData.length - lookback);
        return ohlcData.slice(startIndex);
    }

    /**
     * Cache management methods
     * @private
     */
    _generateCacheKey(ohlcData, options) {
        const dataHash = this._hashData(ohlcData.slice(-5)); // Last 5 bars for uniqueness
        const optionsHash = this._hashData(options);
        return `${dataHash}_${optionsHash}`;
    }

    _hashData(data) {
        return this.mathModels.calculateHash(JSON.stringify(data));
    }

    _getCachedResult(key) {
        if (this.cache.has(key)) {
            const expiry = this.cacheExpiry.get(key);
            if (Date.now() < expiry) {
                return this.cache.get(key);
            } else {
                this.cache.delete(key);
                this.cacheExpiry.delete(key);
            }
        }
        return null;
    }

    _setCachedResult(key, result, ttlMs) {
        const maxCacheSize = this.config.performance.maxCacheSize || 100;

        if (this.cache.size >= maxCacheSize) {
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
            this.cacheExpiry.delete(oldestKey);
        }

        this.cache.set(key, result);
        this.cacheExpiry.set(key, Date.now() + ttlMs);
    }

    /**
     * Error handling setup
     * @private
     */
    _setupErrorHandling() {
        this.errorHandlers = {
            validation: (error) => {
                this._logError('Validation Error', error);
                throw new Error(`Data validation failed: ${error.message}`);
            },
            calculation: (error) => {
                this._logError('Calculation Error', error);
                throw new Error(`Mathematical calculation failed: ${error.message}`);
            },
            system: (error) => {
                this._logError('System Error', error);
                throw new Error(`System error: ${error.message}`);
            }
        };
    }

    _setupCacheManagement() {
        // Clean expired cache entries every 5 minutes
        this.cacheCleanupInterval = setInterval(() => {
            const now = Date.now();
            for (const [key, expiry] of this.cacheExpiry.entries()) {
                if (now >= expiry) {
                    this.cache.delete(key);
                    this.cacheExpiry.delete(key);
                }
            }
        }, 5 * 60 * 1000);
    }

    _handleError(context, error) {
        this.state.errorCount++;
        this.monitor.recordError(context, error);
        this._logError(context, error);
    }

    /**
     * Logging methods
     * @private
     */
    _logInfo(message, data = {}) {
        if (this.config.logging.level <= 2) {
            /* eslint-disable-next-line no-console */
            console.info(`[QuantPivotEngine] ${message}`, data);
        }
    }

    _logWarn(message, data = {}) {
        if (this.config.logging.level <= 1) {
            /* eslint-disable-next-line no-console */
            console.warn(`[QuantPivotEngine] ${message}`, data);
        }
    }

    _logError(message, error = {}) {
        if (this.config.logging.level <= 0) {
            console.error(`[QuantPivotEngine] ${message}`, error);
        }
    }

    /**
     * Public API methods
     */

    /**
     * Get engine health and performance metrics
     */
    getEngineStatus() {
        return {
            state: { ...this.state },
            performance: this.monitor.getMetrics(),
            cache: {
                size: this.cache.size,
                hitRate: this.monitor.getCacheHitRate()
            },
            memory: this.monitor.getMemoryUsage()
        };
    }

    /**
     * Update engine configuration
     */
    updateConfiguration(newConfig) {
        this.config = ConfigurationManager.mergeWithDefaults(newConfig);
        this.validator.updateConfig(this.config.validation);
        this.mathModels.updateConfig(this.config.mathematical);
        this._logInfo('Configuration updated');
    }

    /**
     * Clear cache and reset state
     */
    reset() {
        this.cache.clear();
        this.cacheExpiry.clear();
        this.state.calculationCount = 0;
        this.state.errorCount = 0;
        this.monitor.reset();
        this._logInfo('Engine reset completed');
    }

    /**
     * Cleanup and resource disposal
     */
    dispose() {
        if (this.cacheCleanupInterval) {
            clearInterval(this.cacheCleanupInterval);
        }
        this.cache.clear();
        this.cacheExpiry.clear();
        this.monitor.dispose();
        this.state.isInitialized = false;
        this._logInfo('Engine disposed');
    }
}

// Export singleton instance for institutional use
export const quantPivotEngine = new QuantPivotEngine();