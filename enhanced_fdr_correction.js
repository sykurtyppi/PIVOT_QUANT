/**
 * Enhanced Statistical significance testing with regime-aware baselines
 * and Benjamini-Hochberg FDR correction for pivot level analysis
 */

window.EnhancedFDRCorrection = (() => {

    // Default regime-specific baselines (can be customized)
    const DEFAULT_REGIME_BASELINES = {
        'UP-TREND': {
            R3: 0.45, R2: 0.50, R1: 0.55, PIVOT: 0.50, S1: 0.65, S2: 0.70, S3: 0.75
        },
        'DOWN-TREND': {
            R3: 0.75, R2: 0.70, R1: 0.65, PIVOT: 0.50, S1: 0.55, S2: 0.50, S3: 0.45
        },
        'RANGE': {
            R3: 0.60, R2: 0.55, R1: 0.52, PIVOT: 0.50, S1: 0.52, S2: 0.55, S3: 0.60
        }
    };

    /**
     * One-sided binomial test: H1: p > p0
     * @param {number} successes - Number of successes
     * @param {number} trials - Number of trials
     * @param {number} p0 - Null hypothesis probability (baseline)
     * @param {boolean} oneSided - If true, one-sided test (default), if false, two-sided
     */
    function oneSidedBinomialTest(successes, trials, p0 = 0.5, oneSided = true) {
        if (trials === 0) return 1.0;
        if (successes < 0 || successes > trials) return 1.0;

        const _observedRate = successes / trials;

        // Use normal approximation for large samples
        if (trials * p0 > 5 && trials * (1 - p0) > 5) {
            return normalApproximationOneSided(successes, trials, p0, oneSided);
        }

        // Exact binomial test for small samples
        return exactBinomialOneSided(successes, trials, p0, oneSided);
    }

    /**
     * Normal approximation for one-sided test
     */
    function normalApproximationOneSided(successes, trials, p0, oneSided) {
        const mean = trials * p0;
        const variance = trials * p0 * (1 - p0);
        const stdDev = Math.sqrt(variance);

        // Apply continuity correction
        const correction = successes > mean ? -0.5 : 0.5;
        const zScore = (successes + correction - mean) / stdDev;

        if (oneSided) {
            // One-sided: P(X >= successes) for H1: p > p0
            return 1 - normalCDF(zScore);
        } else {
            // Two-sided test
            return 2 * (1 - normalCDF(Math.abs(zScore)));
        }
    }

    /**
     * Exact one-sided binomial test
     */
    function exactBinomialOneSided(successes, trials, p0, oneSided) {
        if (oneSided) {
            // One-sided: P(X >= successes | H0: p = p0)
            let pValue = 0;
            for (let k = successes; k <= trials; k++) {
                pValue += binomialPMF(k, trials, p0);
            }
            return Math.min(pValue, 1.0);
        } else {
            // Two-sided test (original implementation)
            const observedProb = binomialPMF(successes, trials, p0);
            let pValue = 0;

            for (let k = 0; k <= trials; k++) {
                const prob = binomialPMF(k, trials, p0);
                if (prob <= observedProb + 1e-10) {
                    pValue += prob;
                }
            }
            return Math.min(pValue, 1.0);
        }
    }

    /**
     * Wilson score confidence interval
     */
    function wilsonConfidenceInterval(successes, trials, confidence = 0.95) {
        if (trials === 0) return { lower: 0, upper: 1, center: 0 };

        const z = normalInverse((1 + confidence) / 2);
        const p = successes / trials;
        const n = trials;

        const denominator = 1 + (z * z) / n;
        const center = (p + (z * z) / (2 * n)) / denominator;
        const margin = z * Math.sqrt((p * (1 - p) + (z * z) / (4 * n)) / n) / denominator;

        return {
            lower: Math.max(0, center - margin),
            upper: Math.min(1, center + margin),
            center: center
        };
    }

    /**
     * Inverse normal CDF (approximation)
     */
    function normalInverse(p) {
        // Beasley-Springer-Moro algorithm
        if (p <= 0 || p >= 1) return p <= 0 ? -Infinity : Infinity;

        const a = [0, -3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
        const b = [0, -5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01];
        const c = [0, -7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
        const d = [0, 7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00];

        const pLow = 0.02425;
        const pHigh = 1 - pLow;

        let x;
        if (p < pLow) {
            const q = Math.sqrt(-2 * Math.log(p));
            x = (((((c[1] * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) * q + c[6]) / ((((d[1] * q + d[2]) * q + d[3]) * q + d[4]) * q + 1);
        } else if (p <= pHigh) {
            const q = p - 0.5;
            const r = q * q;
            x = (((((a[1] * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * r + a[6]) * q / (((((b[1] * r + b[2]) * r + b[3]) * r + b[4]) * r + b[5]) * r + 1);
        } else {
            const q = Math.sqrt(-2 * Math.log(1 - p));
            x = -(((((c[1] * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) * q + c[6]) / ((((d[1] * q + d[2]) * q + d[3]) * q + d[4]) * q + 1);
        }

        return x;
    }

    /**
     * Enhanced Benjamini-Hochberg FDR correction with monotonicity guarantee
     */
    function enhancedBenjaminiHochbergCorrection(pValues, alpha = 0.05) {
        const n = pValues.length;
        if (n === 0) return { qValues: [], significant: [] };

        // Handle edge cases
        const validPValues = pValues.map(p => {
            if (!Number.isFinite(p) || p < 0) return 0;
            if (p > 1) return 1;
            return p;
        });

        // Create indexed array and sort by p-value
        const indexed = validPValues.map((p, i) => ({ index: i, pValue: p }));
        indexed.sort((a, b) => a.pValue - b.pValue);

        // Calculate q-values with monotonicity guarantee
        const qValues = new Array(n);
        const significant = new Array(n).fill(false);

        // Working backwards to ensure monotonicity
        let minQValue = 1.0;

        for (let i = n - 1; i >= 0; i--) {
            const rank = i + 1; // BH rank (1-indexed)
            const adjustedP = indexed[i].pValue * n / rank;

            // Ensure monotonicity: q-values should be non-decreasing
            const qValue = Math.min(adjustedP, minQValue);
            minQValue = qValue;

            // Clamp to [0, 1]
            qValues[indexed[i].index] = Math.max(0, Math.min(qValue, 1.0));
            significant[indexed[i].index] = qValues[indexed[i].index] <= alpha;
        }

        return { qValues, significant };
    }

    /**
     * Calculate effective sample size accounting for regime clustering
     */
    function calculateEffectiveN(trials, regime, timeframe) {
        // Adjust for temporal clustering based on timeframe
        const clusteringFactors = {
            daily: 0.9,    // Less clustering in daily data
            weekly: 0.8,   // Moderate clustering
            monthly: 0.7   // More clustering in monthly data
        };

        const factor = clusteringFactors[timeframe] || 0.8;
        return Math.round(trials * factor);
    }

    /**
     * Permutation test for additional validation
     */
    function permutationTest(stats, p0, K = 200) {
        const { successes, trials } = stats;
        if (trials === 0) return 1.0;

        const observedP = oneSidedBinomialTest(successes, trials, p0, true);
        let permutedBetter = 0;

        // Generate K permutations
        for (let k = 0; k < K; k++) {
            // Simulate random success count under null hypothesis
            let permutedSuccesses = 0;
            for (let i = 0; i < trials; i++) {
                if (Math.random() < p0) permutedSuccesses++;
            }

            const permutedP = oneSidedBinomialTest(permutedSuccesses, trials, p0, true);
            if (permutedP <= observedP) permutedBetter++;
        }

        return permutedBetter / K;
    }

    /**
     * Enhanced regime-aware pivot significance analysis
     */
    function analyzePivotSignificanceEnhanced(pivotStats, regime = 'RANGE', timeframe = 'weekly', options = {}) {
        const {
            alpha = 0.05,
            oneSided = true,
            debugMode = false,
            customBaselines = null,
            permutationTests = false,
            minN = 12,
            minNEff = 10
        } = options;

        // Get regime-specific baselines
        const baselines = customBaselines || DEFAULT_REGIME_BASELINES[regime] || DEFAULT_REGIME_BASELINES['RANGE'];

        const levels = Object.keys(pivotStats);
        const pValues = [];
        const results = {};
        const validLevels = [];

        // Calculate p-values for each level
        levels.forEach(level => {
            const stats = pivotStats[level];
            const p0 = baselines[level] || 0.5; // Fallback to 0.5 if baseline not found
            const nEff = calculateEffectiveN(stats.trials, regime, timeframe);

            // Check sample size requirements
            const hasSufficientData = stats.trials >= minN && nEff >= minNEff;

            let pValue = 1.0;
            let qValue = null;
            let significant = false;
            let permPValue = null;

            if (hasSufficientData) {
                pValue = oneSidedBinomialTest(stats.successes, stats.trials, p0, oneSided);
                pValues.push(pValue);
                validLevels.push(level);

                // Permutation test if requested
                if (permutationTests && debugMode) {
                    permPValue = permutationTest(stats, p0);
                }
            }

            // Calculate confidence interval
            const ci = wilsonConfidenceInterval(stats.successes, stats.trials, 0.95);

            // Calculate lift (observed rate - baseline)
            const observedRate = stats.trials > 0 ? stats.successes / stats.trials : 0;
            const lift = observedRate - p0;

            results[level] = {
                successes: stats.successes,
                trials: stats.trials,
                nEffective: nEff,
                successRate: observedRate * 100,
                baseline: p0 * 100,
                lift: lift * 100,
                pValue: pValue,
                qValue: qValue,
                significant: significant,
                hasSufficientData: hasSufficientData,
                confidenceInterval: {
                    lower: ci.lower * 100,
                    upper: ci.upper * 100,
                    center: ci.center * 100
                },
                permutationPValue: permPValue,
                testType: oneSided ? 'one-sided' : 'two-sided',
                hypothesis: oneSided ? `H1: p > ${(p0*100).toFixed(1)}%` : `H1: p ≠ ${(p0*100).toFixed(1)}%`
            };
        });

        // Apply FDR correction only to levels with sufficient data
        if (pValues.length > 0) {
            const fdrResults = enhancedBenjaminiHochbergCorrection(pValues, alpha);

            // Update results with FDR-corrected values
            let validIndex = 0;
            levels.forEach(level => {
                if (results[level].hasSufficientData) {
                    results[level].qValue = fdrResults.qValues[validIndex];
                    results[level].significant = fdrResults.significant[validIndex];
                    validIndex++;
                }
            });
        }

        return {
            levels: results,
            summary: {
                totalLevels: levels.length,
                validLevels: validLevels.length,
                significantLevels: validLevels.filter(level => results[level].significant).length,
                insufficientDataLevels: levels.length - validLevels.length,
                fdrLevel: alpha,
                regime: regime,
                timeframe: timeframe,
                testType: oneSided ? 'one-sided' : 'two-sided'
            },
            familyInfo: {
                regime: regime,
                timeframe: timeframe,
                baselines: baselines,
                testParameters: { alpha, oneSided, minN, minNEff }
            }
        };
    }

    /**
     * Generate comprehensive tooltip text
     */
    function generateTooltip(level, result, debugMode = false) {
        const parts = [];

        if (!result.hasSufficientData) {
            parts.push(`❌ Insufficient data (n=${result.trials}, n_eff=${result.nEffective})`);
            parts.push(`Minimum required: n≥12, n_eff≥10`);
            return parts.join('\n');
        }

        // Main statistics
        parts.push(`📊 ${level} Significance Analysis`);
        parts.push(`Success Rate: ${result.successRate.toFixed(1)}% (${result.successes}/${result.trials})`);
        parts.push(`Baseline (${result.testType}): ${result.baseline.toFixed(1)}%`);
        parts.push(`Lift: ${result.lift >= 0 ? '+' : ''}${result.lift.toFixed(1)}%`);
        parts.push('');

        // Statistical tests
        parts.push(`p-value (${result.testType}): ${result.pValue < 0.001 ? '<0.001' : result.pValue.toFixed(4)}`);
        if (result.qValue !== null) {
            parts.push(`q-value (FDR): ${result.qValue < 0.001 ? '<0.001' : result.qValue.toFixed(4)}`);
        }
        parts.push(`${result.hypothesis}`);
        parts.push('');

        // Sample size information
        parts.push(`Sample Size: n=${result.trials}, n_eff=${result.nEffective}`);
        parts.push(`95% CI: [${result.confidenceInterval.lower.toFixed(1)}%, ${result.confidenceInterval.upper.toFixed(1)}%]`);

        // Debug information
        if (debugMode && result.permutationPValue !== null) {
            parts.push('');
            parts.push(`🔬 Debug Info:`);
            parts.push(`Permutation p-value: ${result.permutationPValue.toFixed(4)}`);
        }

        return parts.join('\n');
    }

    /**
     * Format significance display for table
     */
    function formatSignificanceDisplay(level, result, showTooltip = false) {
        if (!result.hasSufficientData) {
            const display = `${level}: Insufficient data (n=${result.trials})`;
            return showTooltip ? { display, tooltip: generateTooltip(level, result) } : display;
        }

        const rate = result.successRate.toFixed(1);
        const qValue = result.qValue;
        const mark = result.significant ? '✓' : '✗';

        let qText = '';
        if (qValue !== null) {
            if (qValue < 0.001) {
                qText = 'q<0.001';
            } else if (qValue < 0.01) {
                qText = 'q<0.01';
            } else {
                qText = `q=${qValue.toFixed(3)}`;
            }
        }

        const display = qValue !== null ?
            `${level}: ${rate}% (FDR ${qText}) ${mark}` :
            `${level}: ${rate}% ${mark}`;

        return showTooltip ? { display, tooltip: generateTooltip(level, result) } : display;
    }

    // Include all original helper functions
    function binomialPMF(k, n, p) {
        if (k < 0 || k > n) return 0;
        if (n === 0) return k === 0 ? 1 : 0;
        if (p === 0) return k === 0 ? 1 : 0;
        if (p === 1) return k === n ? 1 : 0;

        return binomialCoefficient(n, k) * Math.pow(p, k) * Math.pow(1 - p, n - k);
    }

    function binomialCoefficient(n, k) {
        if (k > n - k) k = n - k;
        if (k === 0) return 1;

        let result = 1;
        for (let i = 0; i < k; i++) {
            result = result * (n - i) / (i + 1);
        }
        return result;
    }

    function normalCDF(z) {
        const t = 1.0 / (1.0 + 0.2316419 * Math.abs(z));
        const d = 0.3989423 * Math.exp(-z * z / 2);
        const prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));

        return z > 0 ? 1 - prob : prob;
    }

    // Public API
    return {
        analyzePivotSignificanceEnhanced,
        oneSidedBinomialTest,
        enhancedBenjaminiHochbergCorrection,
        permutationTest,
        generateTooltip,
        formatSignificanceDisplay,
        wilsonConfidenceInterval,
        DEFAULT_REGIME_BASELINES
    };
})();