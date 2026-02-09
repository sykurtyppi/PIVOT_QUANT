/**
 * Statistical significance testing with Benjamini-Hochberg FDR correction
 * for pivot level analysis
 */

window.FDRCorrection = (() => {

    /**
     * Calculate binomial test p-value (two-tailed)
     * Tests if success rate significantly differs from null hypothesis (50%)
     */
    function binomialTest(successes, trials, nullProb = 0.5) {
        if (trials === 0) return 1.0;

        const _observedRate = successes / trials;
        const _expectedSuccesses = trials * nullProb;

        // Use normal approximation for large samples (np > 5 and n(1-p) > 5)
        if (trials * nullProb > 5 && trials * (1 - nullProb) > 5) {
            return normalApproximationTest(successes, trials, nullProb);
        }

        // Exact binomial test for small samples
        return exactBinomialTest(successes, trials, nullProb);
    }

    /**
     * Normal approximation to binomial test with continuity correction
     */
    function normalApproximationTest(successes, trials, nullProb) {
        const mean = trials * nullProb;
        const variance = trials * nullProb * (1 - nullProb);
        const stdDev = Math.sqrt(variance);

        // Apply continuity correction
        const correction = successes > mean ? -0.5 : 0.5;
        const zScore = (successes + correction - mean) / stdDev;

        // Two-tailed test
        return 2 * (1 - normalCDF(Math.abs(zScore)));
    }

    /**
     * Exact binomial test using cumulative probabilities
     */
    function exactBinomialTest(successes, trials, nullProb) {
        const observedProb = binomialPMF(successes, trials, nullProb);

        // Calculate two-tailed p-value
        let pValue = 0;

        for (let k = 0; k <= trials; k++) {
            const prob = binomialPMF(k, trials, nullProb);
            if (prob <= observedProb + 1e-10) { // Small epsilon for floating point comparison
                pValue += prob;
            }
        }

        return Math.min(pValue, 1.0);
    }

    /**
     * Binomial probability mass function
     */
    function binomialPMF(k, n, p) {
        if (k < 0 || k > n) return 0;
        return binomialCoefficient(n, k) * Math.pow(p, k) * Math.pow(1 - p, n - k);
    }

    /**
     * Binomial coefficient C(n,k)
     */
    function binomialCoefficient(n, k) {
        if (k > n - k) k = n - k; // Take advantage of symmetry

        let result = 1;
        for (let i = 0; i < k; i++) {
            result = result * (n - i) / (i + 1);
        }
        return result;
    }

    /**
     * Standard normal cumulative distribution function
     */
    function normalCDF(z) {
        // Abramowitz and Stegun approximation
        const t = 1.0 / (1.0 + 0.2316419 * Math.abs(z));
        const d = 0.3989423 * Math.exp(-z * z / 2);
        const prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));

        return z > 0 ? 1 - prob : prob;
    }

    /**
     * Benjamini-Hochberg FDR correction
     * @param {Array} pValues - Array of p-values to correct
     * @param {number} alpha - Desired FDR level (default 0.05)
     * @returns {Object} - {qValues: Array, significant: Array}
     */
    function benjaminiHochbergCorrection(pValues, alpha = 0.05) {
        const n = pValues.length;
        if (n === 0) return { qValues: [], significant: [] };

        // Create array of [index, pValue] pairs and sort by p-value
        const indexed = pValues.map((p, i) => ({ index: i, pValue: p }));
        indexed.sort((a, b) => a.pValue - b.pValue);

        // Calculate q-values (FDR-adjusted p-values)
        const qValues = new Array(n);
        const significant = new Array(n).fill(false);

        // Working backwards from largest p-value
        let minQValue = 1.0;

        for (let i = n - 1; i >= 0; i--) {
            const rank = i + 1; // BH rank (1-indexed)
            const adjustedP = indexed[i].pValue * n / rank;

            // Ensure monotonicity (q-values should be non-decreasing)
            const qValue = Math.min(adjustedP, minQValue);
            minQValue = qValue;

            qValues[indexed[i].index] = Math.min(qValue, 1.0);
            significant[indexed[i].index] = qValue <= alpha;
        }

        return { qValues, significant };
    }

    /**
     * Analyze pivot level significance with FDR correction
     * @param {Object} pivotStats - Object with level names as keys, {successes, trials} as values
     * @param {number} alpha - FDR level (default 0.05)
     * @returns {Object} - Results for each level with p-values, q-values, and significance
     */
    function analyzePivotSignificance(pivotStats, alpha = 0.05) {
        const levels = Object.keys(pivotStats);
        const pValues = [];
        const results = {};

        // Calculate p-values for each level
        levels.forEach(level => {
            const stats = pivotStats[level];
            const pValue = binomialTest(stats.successes, stats.trials);
            pValues.push(pValue);

            results[level] = {
                successes: stats.successes,
                trials: stats.trials,
                successRate: stats.trials > 0 ? (stats.successes / stats.trials * 100) : 0,
                pValue: pValue,
                qValue: null, // Will be filled after FDR correction
                significant: false // Will be filled after FDR correction
            };
        });

        // Apply FDR correction
        const fdrResults = benjaminiHochbergCorrection(pValues, alpha);

        // Update results with FDR-corrected values
        levels.forEach((level, index) => {
            results[level].qValue = fdrResults.qValues[index];
            results[level].significant = fdrResults.significant[index];
        });

        return {
            levels: results,
            summary: {
                totalLevels: levels.length,
                significantLevels: fdrResults.significant.filter(s => s).length,
                fdrLevel: alpha
            }
        };
    }

    /**
     * Format significance display text
     */
    function formatSignificanceDisplay(level, result) {
        const rate = result.successRate.toFixed(1);
        const qValue = result.qValue;
        const mark = result.significant ? '✓' : '✗ Not significant';

        if (result.trials === 0) {
            return `${level}: No data`;
        }

        if (qValue < 0.001) {
            return `${level}: ${rate}% (FDR q<0.001) ${mark}`;
        } else if (qValue < 0.01) {
            return `${level}: ${rate}% (FDR q<0.01) ${mark}`;
        } else {
            return `${level}: ${rate}% (FDR q=${qValue.toFixed(3)}) ${mark}`;
        }
    }

    // Public API
    return {
        binomialTest,
        benjaminiHochbergCorrection,
        analyzePivotSignificance,
        formatSignificanceDisplay
    };
})();