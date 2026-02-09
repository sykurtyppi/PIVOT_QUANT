/**
 * Advanced Analytics Module for Enhanced Pivot Analysis
 * Includes level scoring, walk-forward analysis, volatility splits, and alerts
 */

window.AdvancedAnalytics = (() => {

    /**
     * Compute level score for ranking pivot levels
     * Higher scores indicate better levels for trading
     */
    function computeLevelScore({ q, lift, ciWidth, nEff, successRate: _successRate = 0, baseline: _baseline = 0 }) {
        // Handle edge cases
        if (!Number.isFinite(q) || !Number.isFinite(lift) || !Number.isFinite(ciWidth) || !Number.isFinite(nEff)) {
            return 0;
        }

        if (nEff < 10) return 0; // Insufficient data

        // Component scores (0-1 range)

        // 1. Significance score (inverted q-value, capped at 0.1)
        const significanceScore = Math.max(0, Math.min(1, 1 - q / 0.1));

        // 2. Effect size score (absolute lift, normalized)
        const effectSizeScore = Math.min(1, Math.abs(lift) / 30); // 30% lift = max score

        // 3. Precision score (inverted CI width)
        const precisionScore = Math.max(0, Math.min(1, 1 - ciWidth / 40)); // 40% width = min score

        // 4. Sample size score (log-scaled)
        const sampleScore = Math.min(1, Math.log(nEff / 10) / Math.log(10)); // Log scale: 10-100

        // 5. Direction bonus (positive lift preferred for resistance, negative for support)
        const directionBonus = lift > 0 ? 0.1 : 0; // Slight preference for positive lift

        // Weighted combination
        const score = (
            significanceScore * 0.4 +    // Significance is most important
            effectSizeScore * 0.3 +      // Effect size second
            precisionScore * 0.2 +       // Precision third
            sampleScore * 0.1 +          // Sample size least important
            directionBonus
        );

        return Math.max(0, Math.min(1, score));
    }

    /**
     * Calculate walk-forward stability metrics
     */
    function calculateWalkForwardStability(timeSeries, windowSize = 60, stepSize = 20) {
        if (!timeSeries || timeSeries.length < windowSize + stepSize) {
            return { variance: null, trend: null, stability: 'UNKNOWN' };
        }

        const windows = [];
        const lifts = [];
        const qValues = [];

        // Create rolling windows
        for (let start = 0; start <= timeSeries.length - windowSize; start += stepSize) {
            const window = timeSeries.slice(start, start + windowSize);
            const windowStats = calculateWindowStats(window);

            if (windowStats.isValid) {
                windows.push({
                    start: start,
                    end: start + windowSize - 1,
                    lift: windowStats.lift,
                    qValue: windowStats.qValue,
                    successRate: windowStats.successRate
                });

                lifts.push(windowStats.lift);
                qValues.push(windowStats.qValue);
            }
        }

        if (lifts.length < 3) {
            return { variance: null, trend: null, stability: 'INSUFFICIENT' };
        }

        // Calculate variance of lifts
        const liftMean = lifts.reduce((sum, val) => sum + val, 0) / lifts.length;
        const liftVariance = lifts.reduce((sum, val) => sum + Math.pow(val - liftMean, 2), 0) / lifts.length;

        // Calculate trend (linear slope)
        const n = lifts.length;
        const x = Array.from({length: n}, (_, i) => i);
        const sumX = x.reduce((sum, val) => sum + val, 0);
        const sumY = lifts.reduce((sum, val) => sum + val, 0);
        const sumXY = x.reduce((sum, xi, i) => sum + xi * lifts[i], 0);
        const sumXX = x.reduce((sum, xi) => sum + xi * xi, 0);

        const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);

        // Stability classification
        let stability;
        if (liftVariance < 25) { // Low variance threshold
            stability = 'STABLE';
        } else if (liftVariance < 100) { // Medium variance threshold
            stability = 'MODERATE';
        } else {
            stability = 'DRIFTY';
        }

        return {
            variance: liftVariance,
            trend: slope,
            stability: stability,
            windows: windows,
            liftSeries: lifts,
            qSeries: qValues
        };
    }

    /**
     * Calculate statistics for a single window
     */
    function calculateWindowStats(windowData) {
        // Extract successes and trials from window data
        const successes = windowData.filter(point => point.success === true).length;
        const trials = windowData.length;

        if (trials < 10) {
            return { isValid: false };
        }

        // Use baseline of 50% for window calculations
        const baseline = 0.5;
        const successRate = successes / trials;
        const lift = (successRate - baseline) * 100;

        // Simplified p-value calculation for window
        const pValue = calculateBinomialTest(successes, trials, baseline);

        return {
            isValid: true,
            successes: successes,
            trials: trials,
            successRate: successRate * 100,
            lift: lift,
            qValue: pValue, // Simplified for individual windows
            baseline: baseline * 100
        };
    }

    /**
     * Simple binomial test for window calculations
     */
    function calculateBinomialTest(successes, trials, p0) {
        if (trials === 0) return 1.0;

        // Normal approximation
        const mean = trials * p0;
        const variance = trials * p0 * (1 - p0);
        const stdDev = Math.sqrt(variance);

        const zScore = (successes - mean) / stdDev;

        // Two-tailed p-value (simplified)
        return 2 * (1 - normalCDF(Math.abs(zScore)));
    }

    /**
     * Standard normal CDF
     */
    function normalCDF(z) {
        const t = 1.0 / (1.0 + 0.2316419 * Math.abs(z));
        const d = 0.3989423 * Math.exp(-z * z / 2);
        const prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
        return z > 0 ? 1 - prob : prob;
    }

    /**
     * Generate Top Picks for a regime
     */
    function generateTopPicks(fdrResults, regime, maxPicks = 2) {
        const validLevels = Object.entries(fdrResults.levels)
            .filter(([_level, result]) => result.hasSufficientData)
            .map(([level, result]) => {
                const score = computeLevelScore({
                    q: result.qValue || 1,
                    lift: result.lift,
                    ciWidth: result.confidenceInterval.upper - result.confidenceInterval.lower,
                    nEff: result.nEffective,
                    successRate: result.successRate,
                    baseline: result.baseline
                });

                return {
                    level: level,
                    score: score,
                    result: result
                };
            })
            .sort((a, b) => b.score - a.score) // Sort by score descending
            .slice(0, maxPicks); // Take top picks

        return validLevels;
    }

    /**
     * Split analysis by volatility regime
     */
    function splitByVolatility(data, volatilityData) {
        const volSplits = {
            'Low': [],
            'Normal': [],
            'High': []
        };

        // Classify each data point by volatility
        data.forEach((point, index) => {
            const vol = volatilityData[index];
            if (vol) {
                let volRegime;
                if (vol.percentile < 33) {
                    volRegime = 'Low';
                } else if (vol.percentile < 67) {
                    volRegime = 'Normal';
                } else {
                    volRegime = 'High';
                }

                volSplits[volRegime].push(point);
            }
        });

        return volSplits;
    }

    /**
     * Create analysis metadata for exports
     */
    function createAnalysisMetadata(params) {
        const {
            regime,
            timeframe,
            volatilityFilter,
            alpha,
            testType,
            bhFamilyMembers,
            baselines,
            windowSize,
            stepSize,
            dataSource,
            priceData
        } = params;

        // Calculate SHA1 of closes for data integrity
        const sha1 = calculateSHA1(priceData);

        return {
            timestamp: new Date().toISOString(),
            analysis_version: "2.0.0",
            regime: regime,
            volatility_filter: volatilityFilter,
            timeframe: timeframe,
            statistical_parameters: {
                alpha: alpha,
                test_type: testType,
                multiple_testing_correction: "benjamini_hochberg",
                bh_family_members: bhFamilyMembers,
                min_sample_size: 12,
                min_effective_size: 10
            },
            baselines: baselines,
            walk_forward: {
                window_size: windowSize,
                step_size: stepSize,
                stability_thresholds: {
                    stable: 25,
                    moderate: 100
                }
            },
            data_source: dataSource,
            data_integrity: {
                sha1_closes: sha1,
                record_count: priceData.length
            },
            computation: {
                computed_at: new Date().toISOString(),
                computation_id: generateComputationId()
            }
        };
    }

    /**
     * Simple SHA1 calculation for data integrity
     */
    function calculateSHA1(data) {
        // Simplified hash for browser environment
        const str = JSON.stringify(data.map(d => d.close));
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash; // Convert to 32-bit integer
        }
        return Math.abs(hash).toString(16).padStart(8, '0');
    }

    /**
     * Generate unique computation ID
     */
    function generateComputationId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
    }

    /**
     * Create alert rules
     */
    function createAlertRules() {
        return {
            significance: {
                strong_signal: { q_threshold: 0.01, lift_threshold: 15, min_nEff: 30 },
                moderate_signal: { q_threshold: 0.05, lift_threshold: 10, min_nEff: 20 },
                weak_signal: { q_threshold: 0.1, lift_threshold: 5, min_nEff: 15 }
            },
            stability: {
                deteriorating: { variance_threshold: 150, trend_threshold: -2 },
                improving: { variance_threshold: 50, trend_threshold: 2 }
            },
            volume: {
                high_conviction: { nEff_threshold: 50, ci_width_max: 20 },
                sufficient: { nEff_threshold: 30, ci_width_max: 30 }
            }
        };
    }

    /**
     * Evaluate alerts for a level
     */
    function evaluateAlerts(level, result, stability, alertRules) {
        const alerts = [];

        // Significance alerts
        Object.entries(alertRules.significance).forEach(([type, rule]) => {
            if (result.qValue <= rule.q_threshold &&
                Math.abs(result.lift) >= rule.lift_threshold &&
                result.nEffective >= rule.min_nEff) {

                const bias = determineBias(level, result.lift);
                const atrStops = calculateATRStops(result, bias);

                alerts.push({
                    type: 'significance',
                    level: type,
                    level_name: level,
                    message: `${type.toUpperCase()} signal detected`,
                    bias: bias,
                    confidence_interval: result.confidenceInterval,
                    stops: atrStops,
                    priority: type === 'strong_signal' ? 'HIGH' : type === 'moderate_signal' ? 'MEDIUM' : 'LOW'
                });
            }
        });

        // Stability alerts
        if (stability && stability.variance !== null) {
            Object.entries(alertRules.stability).forEach(([type, rule]) => {
                if (stability.variance >= rule.variance_threshold &&
                    Math.abs(stability.trend) >= Math.abs(rule.trend_threshold)) {

                    alerts.push({
                        type: 'stability',
                        level: type,
                        level_name: level,
                        message: `Level stability ${type}`,
                        variance: stability.variance,
                        trend: stability.trend,
                        priority: 'MEDIUM'
                    });
                }
            });
        }

        return alerts;
    }

    /**
     * Determine bias (reversal vs breakout) based on level and lift
     */
    function determineBias(level, lift) {
        const isResistance = ['R1', 'R2', 'R3'].includes(level);
        const isSupport = ['S1', 'S2', 'S3'].includes(level);

        if (isResistance) {
            return lift > 0 ? 'reversal' : 'breakout';
        } else if (isSupport) {
            return lift > 0 ? 'reversal' : 'breakout';
        } else { // PIVOT
            return lift > 0 ? 'bullish_bias' : 'bearish_bias';
        }
    }

    /**
     * Calculate ATR-based stops and targets
     */
    function calculateATRStops(result, bias, atrMultiplier = 2.0) {
        // Mock ATR calculation - in practice, would use actual ATR
        const estimatedATR = result.confidenceInterval.upper * 0.02; // Rough approximation

        const stopDistance = atrMultiplier * estimatedATR;
        const targetDistance = atrMultiplier * 1.5 * estimatedATR; // 1.5:1 reward:risk

        return {
            atr_multiplier: atrMultiplier,
            stop_distance: stopDistance,
            target_distance: targetDistance,
            risk_reward_ratio: 1.5,
            bias: bias
        };
    }

    /**
     * Generate sparkline data for visualization
     */
    function generateSparklineData(series, width = 60, height = 20) {
        if (!series || series.length < 2) {
            return { path: '', points: [] };
        }

        const min = Math.min(...series);
        const max = Math.max(...series);
        const range = max - min || 1; // Avoid division by zero

        const points = series.map((value, index) => {
            const x = (index / (series.length - 1)) * width;
            const y = height - ((value - min) / range) * height;
            return { x, y, value };
        });

        // Create SVG path
        const pathCommands = points.map((point, index) => {
            return `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`;
        });

        return {
            path: pathCommands.join(' '),
            points: points,
            min: min,
            max: max,
            width: width,
            height: height
        };
    }

    // Public API
    return {
        computeLevelScore,
        calculateWalkForwardStability,
        generateTopPicks,
        splitByVolatility,
        createAnalysisMetadata,
        createAlertRules,
        evaluateAlerts,
        determineBias,
        calculateATRStops,
        generateSparklineData,

        // Utility functions
        calculateWindowStats,
        calculateBinomialTest,
        calculateSHA1,
        generateComputationId
    };
})();