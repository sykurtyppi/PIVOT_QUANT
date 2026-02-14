/**
 * ValidationFramework - Institutional-Grade Data Validation
 *
 * Comprehensive validation system ensuring data integrity, mathematical
 * consistency, and institutional compliance standards.
 *
 * @version 2.0.0
 * @author PIVOT_QUANT Team
 */

export class ValidationFramework {
    constructor(config = {}) {
        this.config = {
            strictMode: config.strictMode ?? true,
            toleranceLevel: config.toleranceLevel || 1e-8,
            maxDataPoints: config.maxDataPoints || 10000,
            minDataPoints: config.minDataPoints || 2,
            priceRange: config.priceRange || { min: 0.01, max: 1000000 },
            volumeRange: config.volumeRange || { min: 0, max: Number.MAX_SAFE_INTEGER },
            ...config
        };

        this.validationRules = this._initializeValidationRules();
        this.errorCodes = this._initializeErrorCodes();
    }

    updateConfig(newConfig) {
        this.config = { ...this.config, ...newConfig };
    }

    /**
     * Validate engine configuration
     * @param {Object} engineConfig - Engine configuration object
     * @throws {Error} If configuration is invalid
     */
    validateEngineConfig(engineConfig) {
        const errors = [];
        const warnings = [];

        // Required configuration sections
        const requiredSections = ['mathematical', 'performance', 'validation', 'logging'];

        requiredSections.forEach(section => {
            if (!engineConfig[section]) {
                errors.push(`Missing required configuration section: ${section}`);
            }
        });

        // Mathematical configuration validation
        if (engineConfig.mathematical) {
            const mathConfig = engineConfig.mathematical;

            if (mathConfig.precision && (mathConfig.precision < 1 || mathConfig.precision > 15)) {
                errors.push('Mathematical precision must be between 1 and 15 decimal places');
            }

            if (mathConfig.maxIterations && mathConfig.maxIterations < 100) {
                warnings.push('Low maxIterations may affect convergence quality');
            }

            if (mathConfig.convergenceThreshold && mathConfig.convergenceThreshold > 1e-5) {
                warnings.push('High convergence threshold may reduce accuracy');
            }
        }

        // Performance configuration validation
        if (engineConfig.performance) {
            const perfConfig = engineConfig.performance;

            if (perfConfig.maxCacheSize && perfConfig.maxCacheSize < 10) {
                warnings.push('Very small cache size may impact performance');
            }
        }

        if (errors.length > 0) {
            throw new Error(`Configuration validation failed: ${errors.join('; ')}`);
        }

        if (warnings.length > 0 && this.config.strictMode) {
            /* eslint-disable-next-line no-console */
            console.warn('[ValidationFramework] Configuration warnings:', warnings);
        }

        return { isValid: true, errors: [], warnings };
    }

    /**
     * Comprehensive OHLC data validation
     * @param {Array} ohlcData - OHLC price data
     * @param {Object} options - Validation options
     * @returns {Object} Validation result with detailed error reporting
     */
    async validateOHLCData(ohlcData, options = {}) {
        const validationResult = {
            isValid: false,
            errors: [],
            warnings: [],
            dataQuality: {},
            statistics: {},
            recommendations: []
        };

        try {
            // 1. Basic structure validation
            const structureValidation = this._validateDataStructure(ohlcData);
            if (!structureValidation.isValid) {
                validationResult.errors.push(...structureValidation.errors);
                return validationResult;
            }

            // 2. Data range validation
            const rangeValidation = this._validateDataRanges(ohlcData);
            validationResult.errors.push(...rangeValidation.errors);
            validationResult.warnings.push(...rangeValidation.warnings);

            // 3. OHLC relationship validation
            const ohlcValidation = this._validateOHLCRelationships(ohlcData);
            validationResult.errors.push(...ohlcValidation.errors);
            validationResult.warnings.push(...ohlcValidation.warnings);

            // 4. Time series consistency
            const timeValidation = this._validateTimeSeriesConsistency(ohlcData);
            validationResult.warnings.push(...timeValidation.warnings);

            // 5. Data quality assessment
            validationResult.dataQuality = await this._assessDataQuality(ohlcData);

            // 6. Statistical outlier detection
            const outlierAnalysis = this._detectStatisticalOutliers(ohlcData);
            validationResult.warnings.push(...outlierAnalysis.warnings);
            validationResult.dataQuality.outliers = outlierAnalysis.outliers;

            // 7. Market microstructure validation
            const microstructureValidation = this._validateMarketMicrostructure(ohlcData);
            validationResult.warnings.push(...microstructureValidation.warnings);

            // 8. Generate recommendations
            validationResult.recommendations = this._generateRecommendations(
                validationResult.errors,
                validationResult.warnings,
                validationResult.dataQuality
            );

            // 9. Calculate overall data statistics
            validationResult.statistics = this._calculateDataStatistics(ohlcData);

            // Determine overall validation result
            validationResult.isValid = validationResult.errors.length === 0;

            return validationResult;

        } catch (error) {
            validationResult.errors.push(`Validation process failed: ${error.message}`);
            return validationResult;
        }
    }

    /**
     * Validate calculation options
     * @param {Object} options - Calculation options to validate
     * @returns {Object} Validation result
     */
    validateCalculationOptions(options) {
        const result = { isValid: true, errors: [], warnings: [] };

        // ATR period validation
        if (options.atrPeriod !== undefined) {
            if (!Number.isInteger(options.atrPeriod) || options.atrPeriod < 1 || options.atrPeriod > 100) {
                result.errors.push('ATR period must be an integer between 1 and 100');
            }
        }

        // Lookback validation
        if (options.lookback !== undefined) {
            if (!Number.isInteger(options.lookback) || options.lookback < 10 || options.lookback > 1000) {
                result.errors.push('Lookback period must be an integer between 10 and 1000');
            }
        }

        // Methods validation
        if (options.methods !== undefined) {
            const validMethods = ['standard', 'fibonacci', 'camarilla', 'woodie', 'demark'];
            const invalidMethods = options.methods.filter(method => !validMethods.includes(method));

            if (invalidMethods.length > 0) {
                result.errors.push(`Invalid methods: ${invalidMethods.join(', ')}. Valid methods: ${validMethods.join(', ')}`);
            }
        }

        // Zone multipliers validation
        if (options.zoneMultipliers !== undefined) {
            if (!Array.isArray(options.zoneMultipliers)) {
                result.errors.push('Zone multipliers must be an array');
            } else {
                const invalidMultipliers = options.zoneMultipliers.filter(mult =>
                    typeof mult !== 'number' || mult <= 0 || mult > 5
                );

                if (invalidMultipliers.length > 0) {
                    result.errors.push('Zone multipliers must be positive numbers between 0 and 5');
                }
            }
        }

        // Cache TTL validation
        if (options.cacheTTL !== undefined) {
            if (!Number.isInteger(options.cacheTTL) || options.cacheTTL < 1000 || options.cacheTTL > 3600000) {
                result.warnings.push('Cache TTL should be between 1 second and 1 hour');
            }
        }

        result.isValid = result.errors.length === 0;
        return result;
    }

    /**
     * Validate pivot calculation results
     * @param {Object} results - Calculation results to validate
     * @returns {Object} Validation result
     */
    validateCalculationResults(results) {
        const result = { isValid: true, errors: [], warnings: [] };

        // Check required result structure
        const requiredFields = ['metadata', 'levels', 'analysis', 'risk'];
        requiredFields.forEach(field => {
            if (!results[field]) {
                result.errors.push(`Missing required result field: ${field}`);
            }
        });

        // Validate metadata
        if (results.metadata) {
            if (!results.metadata.timestamp || !results.metadata.dataPoints) {
                result.errors.push('Invalid metadata structure');
            }
        }

        // Validate pivot levels
        if (results.levels) {
            for (const [method, levels] of Object.entries(results.levels)) {
                if (typeof levels !== 'object') {
                    result.errors.push(`Invalid levels structure for method: ${method}`);
                    continue;
                }

                // Check for NaN or infinite values
                for (const [levelName, levelValue] of Object.entries(levels)) {
                    if (levelName === 'metadata') continue;

                    if (!Number.isFinite(levelValue)) {
                        result.errors.push(`Invalid level value for ${method}.${levelName}: ${levelValue}`);
                    }

                    // Check for reasonable price ranges
                    if (levelValue < this.config.priceRange.min || levelValue > this.config.priceRange.max) {
                        result.warnings.push(`Level value outside expected range: ${method}.${levelName} = ${levelValue}`);
                    }
                }
            }
        }

        // Validate mathematical consistency
        if (results.levels) {
            const consistencyErrors = this._validateMathematicalConsistency(results.levels);
            result.errors.push(...consistencyErrors);
        }

        result.isValid = result.errors.length === 0;
        return result;
    }

    // =================================================================================
    // PRIVATE VALIDATION METHODS
    // =================================================================================

    _validateDataStructure(ohlcData) {
        const result = { isValid: true, errors: [] };

        // Check if data is an array
        if (!Array.isArray(ohlcData)) {
            result.errors.push('Data must be an array');
            result.isValid = false;
            return result;
        }

        // Check minimum data points
        if (ohlcData.length < this.config.minDataPoints) {
            result.errors.push(`Insufficient data: need at least ${this.config.minDataPoints} data points, got ${ohlcData.length}`);
            result.isValid = false;
        }

        // Check maximum data points
        if (ohlcData.length > this.config.maxDataPoints) {
            result.errors.push(`Too much data: maximum ${this.config.maxDataPoints} data points, got ${ohlcData.length}`);
            result.isValid = false;
        }

        // Check each data point structure
        const requiredFields = ['high', 'low', 'close'];
        const _optionalFields = ['open', 'volume', 'timestamp'];

        ohlcData.forEach((point, index) => {
            if (!point || typeof point !== 'object') {
                result.errors.push(`Invalid data point at index ${index}: must be an object`);
                return;
            }

            requiredFields.forEach(field => {
                if (!(field in point)) {
                    result.errors.push(`Missing required field '${field}' at index ${index}`);
                } else if (typeof point[field] !== 'number' || !Number.isFinite(point[field])) {
                    result.errors.push(`Invalid ${field} value at index ${index}: must be a finite number`);
                }
            });
        });

        result.isValid = result.errors.length === 0;
        return result;
    }

    _validateDataRanges(ohlcData) {
        const result = { errors: [], warnings: [] };

        ohlcData.forEach((point, index) => {
            // Price range validation
            ['high', 'low', 'close', 'open'].forEach(field => {
                if (point[field] !== undefined) {
                    if (point[field] < this.config.priceRange.min) {
                        result.errors.push(`${field} value too low at index ${index}: ${point[field]}`);
                    }
                    if (point[field] > this.config.priceRange.max) {
                        result.errors.push(`${field} value too high at index ${index}: ${point[field]}`);
                    }
                }
            });

            // Volume range validation
            if (point.volume !== undefined) {
                if (point.volume < this.config.volumeRange.min) {
                    result.warnings.push(`Negative volume at index ${index}: ${point.volume}`);
                }
                if (point.volume > this.config.volumeRange.max) {
                    result.warnings.push(`Extremely high volume at index ${index}: ${point.volume}`);
                }
            }
        });

        return result;
    }

    _validateOHLCRelationships(ohlcData) {
        const result = { errors: [], warnings: [] };

        ohlcData.forEach((point, index) => {
            // Basic OHLC relationship: High >= Low
            if (point.high < point.low) {
                result.errors.push(`High < Low at index ${index}: High=${point.high}, Low=${point.low}`);
            }

            // OHLC containment: High >= Close >= Low
            if (point.close > point.high) {
                result.errors.push(`Close > High at index ${index}: Close=${point.close}, High=${point.high}`);
            }
            if (point.close < point.low) {
                result.errors.push(`Close < Low at index ${index}: Close=${point.close}, Low=${point.low}`);
            }

            // Open validation (if available)
            if (point.open !== undefined) {
                if (point.open > point.high) {
                    result.errors.push(`Open > High at index ${index}: Open=${point.open}, High=${point.high}`);
                }
                if (point.open < point.low) {
                    result.errors.push(`Open < Low at index ${index}: Open=${point.open}, Low=${point.low}`);
                }
            }

            // Zero range validation
            if (Math.abs(point.high - point.low) < this.config.toleranceLevel) {
                result.warnings.push(`Zero or very small range at index ${index}: Range=${point.high - point.low}`);
            }
        });

        return result;
    }

    _validateTimeSeriesConsistency(ohlcData) {
        const result = { warnings: [] };

        // Check for gaps in price continuity
        for (let i = 1; i < ohlcData.length; i++) {
            const prevClose = ohlcData[i - 1].close;
            const currentOpen = ohlcData[i].open || ohlcData[i].close;

            const gapSize = Math.abs(currentOpen - prevClose) / prevClose;

            if (gapSize > 0.1) { // 10% gap
                result.warnings.push(`Large price gap at index ${i}: ${(gapSize * 100).toFixed(2)}%`);
            }
        }

        // Check for timestamp consistency (if available)
        const timestampedData = ohlcData.filter(point => point.timestamp);
        if (timestampedData.length > 1) {
            for (let i = 1; i < timestampedData.length; i++) {
                if (timestampedData[i].timestamp <= timestampedData[i - 1].timestamp) {
                    result.warnings.push(`Non-increasing timestamp at index ${i}`);
                }
            }
        }

        return result;
    }

    async _assessDataQuality(ohlcData) {
        const quality = {};

        // Calculate data completeness
        const totalFields = ohlcData.length * 4; // OHLC
        const missingFields = ohlcData.reduce((count, point) => {
            return count + (['high', 'low', 'close', 'open'].filter(field =>
                point[field] === undefined || point[field] === null
            ).length);
        }, 0);

        quality.completeness = ((totalFields - missingFields) / totalFields) * 100;

        // Calculate data consistency score
        let consistencyScore = 100;
        const priceRanges = ohlcData.map(point => point.high - point.low);
        const avgRange = priceRanges.reduce((sum, range) => sum + range, 0) / priceRanges.length;

        // Penalize for extreme ranges
        const extremeRanges = priceRanges.filter(range => range > avgRange * 5).length;
        consistencyScore -= (extremeRanges / ohlcData.length) * 20;

        quality.consistency = Math.max(0, consistencyScore);

        // Calculate temporal regularity (if timestamps available)
        const timestampedData = ohlcData.filter(point => point.timestamp);
        if (timestampedData.length > 2) {
            const intervals = [];
            for (let i = 1; i < timestampedData.length; i++) {
                intervals.push(timestampedData[i].timestamp - timestampedData[i - 1].timestamp);
            }

            const avgInterval = intervals.reduce((sum, interval) => sum + interval, 0) / intervals.length;
            const irregularIntervals = intervals.filter(interval =>
                Math.abs(interval - avgInterval) > avgInterval * 0.5
            ).length;

            quality.temporalRegularity = ((intervals.length - irregularIntervals) / intervals.length) * 100;
        } else {
            quality.temporalRegularity = null;
        }

        // Overall quality score
        const weights = { completeness: 0.4, consistency: 0.4, temporalRegularity: 0.2 };
        quality.overall = (
            quality.completeness * weights.completeness +
            quality.consistency * weights.consistency +
            (quality.temporalRegularity || 100) * weights.temporalRegularity
        );

        return quality;
    }

    _detectStatisticalOutliers(ohlcData) {
        const result = { warnings: [], outliers: [] };

        const prices = ohlcData.map(point => point.close);
        const returns = [];

        for (let i = 1; i < prices.length; i++) {
            returns.push(Math.log(prices[i] / prices[i - 1]));
        }

        // Calculate Z-scores for returns
        const mean = returns.reduce((sum, ret) => sum + ret, 0) / returns.length;
        const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / returns.length;
        const stdDev = Math.sqrt(variance);

        const zScores = returns.map(ret => Math.abs((ret - mean) / stdDev));

        // Flag outliers (|z-score| > 3)
        zScores.forEach((zScore, index) => {
            if (zScore > 3) {
                result.outliers.push({
                    index: index + 1,
                    return: returns[index],
                    zScore: zScore,
                    severity: zScore > 5 ? 'extreme' : 'moderate'
                });
            }
        });

        if (result.outliers.length > 0) {
            result.warnings.push(`${result.outliers.length} statistical outliers detected in price returns`);
        }

        return result;
    }

    _validateMarketMicrostructure(ohlcData) {
        const result = { warnings: [] };

        // Check for unusual bid-ask spread patterns (simplified)
        const spreads = ohlcData.map(point => (point.high - point.low) / point.close);
        const avgSpread = spreads.reduce((sum, spread) => sum + spread, 0) / spreads.length;

        const extremeSpreads = spreads.filter(spread => spread > avgSpread * 10).length;
        if (extremeSpreads > ohlcData.length * 0.05) { // More than 5% extreme spreads
            result.warnings.push(`${extremeSpreads} bars with unusually wide spreads detected`);
        }

        // Check for volume-price relationships (if volume available)
        const volumeData = ohlcData.filter(point => point.volume !== undefined);
        if (volumeData.length > 10) {
            const priceChanges = [];
            const volumes = [];

            for (let i = 1; i < volumeData.length; i++) {
                priceChanges.push(Math.abs(volumeData[i].close - volumeData[i - 1].close) / volumeData[i - 1].close);
                volumes.push(volumeData[i].volume);
            }

            // Simplified correlation check
            const highVolumeWithLowChange = priceChanges.filter((change, i) =>
                change < 0.001 && volumes[i] > volumes.reduce((sum, vol) => sum + vol, 0) / volumes.length * 2
            ).length;

            if (highVolumeWithLowChange > priceChanges.length * 0.1) {
                result.warnings.push('Unusual volume-price patterns detected');
            }
        }

        return result;
    }

    _validateMathematicalConsistency(levels) {
        const errors = [];

        Object.entries(levels).forEach(([method, methodLevels]) => {
            const levelValues = Object.entries(methodLevels)
                .filter(([name]) => name !== 'metadata')
                .map(([name, value]) => ({ name, value }));

            // Check for duplicate levels
            const values = levelValues.map(level => level.value);
            const uniqueValues = [...new Set(values)];

            if (values.length !== uniqueValues.length) {
                errors.push(`Duplicate pivot levels detected in ${method} method`);
            }

            // Check resistance/support ordering
            const resistanceLevels = levelValues.filter(level => level.name.startsWith('R'))
                .sort((a, b) => a.value - b.value);
            const supportLevels = levelValues.filter(level => level.name.startsWith('S'))
                .sort((a, b) => b.value - a.value);

            // Validate resistance levels are increasing
            for (let i = 1; i < resistanceLevels.length; i++) {
                if (resistanceLevels[i].value <= resistanceLevels[i - 1].value) {
                    errors.push(`Resistance levels not properly ordered in ${method} method`);
                    break;
                }
            }

            // Validate support levels are decreasing
            for (let i = 1; i < supportLevels.length; i++) {
                if (supportLevels[i].value >= supportLevels[i - 1].value) {
                    errors.push(`Support levels not properly ordered in ${method} method`);
                    break;
                }
            }
        });

        return errors;
    }

    _calculateDataStatistics(ohlcData) {
        const prices = ohlcData.map(point => point.close);
        const volumes = ohlcData.filter(point => point.volume !== undefined).map(point => point.volume);

        const priceStats = this._calculateBasicStatistics(prices);
        const volumeStats = volumes.length > 0 ? this._calculateBasicStatistics(volumes) : null;

        return {
            dataPoints: ohlcData.length,
            priceRange: { min: Math.min(...prices), max: Math.max(...prices) },
            priceStatistics: priceStats,
            volumeStatistics: volumeStats,
            dataSpan: {
                start: ohlcData[0].timestamp ? new Date(ohlcData[0].timestamp) : null,
                end: ohlcData[ohlcData.length - 1].timestamp ? new Date(ohlcData[ohlcData.length - 1].timestamp) : null
            }
        };
    }

    _calculateBasicStatistics(array) {
        if (array.length === 0) return null;

        const sorted = [...array].sort((a, b) => a - b);
        const sum = array.reduce((total, value) => total + value, 0);
        const mean = sum / array.length;
        const variance = array.reduce((total, value) => total + Math.pow(value - mean, 2), 0) / array.length;

        return {
            min: sorted[0],
            max: sorted[sorted.length - 1],
            mean: mean,
            median: sorted[Math.floor(sorted.length / 2)],
            stdDev: Math.sqrt(variance),
            q25: sorted[Math.floor(sorted.length * 0.25)],
            q75: sorted[Math.floor(sorted.length * 0.75)]
        };
    }

    _generateRecommendations(errors, warnings, dataQuality) {
        const recommendations = [];

        // Data quality recommendations
        if (dataQuality.completeness < 95) {
            recommendations.push('Consider filling missing data points using interpolation methods');
        }

        if (dataQuality.consistency < 80) {
            recommendations.push('Review data source for consistency issues');
        }

        if (dataQuality.temporalRegularity !== null && dataQuality.temporalRegularity < 90) {
            recommendations.push('Consider resampling data to regular intervals');
        }

        // Error-based recommendations
        if (errors.some(error => error.includes('OHLC'))) {
            recommendations.push('Implement data cleaning pipeline to fix OHLC relationship violations');
        }

        if (warnings.some(warning => warning.includes('outlier'))) {
            recommendations.push('Consider implementing outlier detection and handling procedures');
        }

        if (warnings.some(warning => warning.includes('gap'))) {
            recommendations.push('Implement gap adjustment procedures for more accurate calculations');
        }

        return recommendations;
    }

    _initializeValidationRules() {
        return {
            priceRanges: {
                min: this.config.priceRange.min,
                max: this.config.priceRange.max
            },
            volumeRanges: {
                min: this.config.volumeRange.min,
                max: this.config.volumeRange.max
            },
            ohlcRelationships: {
                highLowOrder: true,
                closeContainment: true,
                openContainment: true
            },
            temporalConsistency: {
                increasing: true,
                maxGap: 0.1 // 10% price gap threshold
            }
        };
    }

    _initializeErrorCodes() {
        return {
            STRUCTURE_ERROR: 'STRUCT_ERR',
            RANGE_ERROR: 'RANGE_ERR',
            OHLC_ERROR: 'OHLC_ERR',
            TEMPORAL_ERROR: 'TEMP_ERR',
            QUALITY_WARNING: 'QUAL_WARN',
            OUTLIER_WARNING: 'OUT_WARN',
            MICROSTRUCTURE_WARNING: 'MICRO_WARN',
            CONSISTENCY_ERROR: 'CONSIST_ERR'
        };
    }
}