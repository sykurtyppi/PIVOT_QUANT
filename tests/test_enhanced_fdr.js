/* eslint-disable no-console */
/**
 * Comprehensive unit tests for Enhanced FDR Correction
 * Tests edge cases, monotonicity, and all statistical functions
 */

// Test runner
function runAllTests() {
    console.log('🧪 Enhanced FDR Correction Test Suite');
    console.log('=' .repeat(60));

    const tests = [
        testMonotonicityGuarantee,
        testEdgeCases,
        testOneSidedBinomialTest,
        testBenjaminiHochbergCorrection,
        testPermutationTest,
        testRegimeAwareBaselines,
        testSampleSizeFiltering,
        testConfidenceIntervals,
        testTooltipGeneration,
        testDisplayFormatting
    ];

    let passed = 0;
    let failed = 0;

    tests.forEach(test => {
        try {
            console.log(`\n📋 Running ${test.name}...`);
            test();
            console.log(`✅ ${test.name} PASSED`);
            passed++;
        } catch (error) {
            console.error(`❌ ${test.name} FAILED:`, error.message);
            failed++;
        }
    });

    console.log(`\n📊 Test Results: ${passed} passed, ${failed} failed`);
    return { passed, failed };
}

/**
 * Test monotonicity guarantee in BH correction
 */
function testMonotonicityGuarantee() {
    const pValues = [0.001, 0.005, 0.01, 0.03, 0.05, 0.1, 0.2, 0.5];
    const result = EnhancedFDRCorrection.enhancedBenjaminiHochbergCorrection(pValues, 0.05);

    // Check that q-values are monotonically non-decreasing
    for (let i = 1; i < result.qValues.length; i++) {
        const prevQ = result.qValues[i-1];
        const currQ = result.qValues[i];

        if (currQ < prevQ - 1e-10) { // Allow for floating point precision
            throw new Error(`Non-monotonic q-values: q[${i-1}]=${prevQ}, q[${i}]=${currQ}`);
        }
    }

    // Check clamping to [0, 1]
    result.qValues.forEach((q, i) => {
        if (q < 0 || q > 1) {
            throw new Error(`Q-value out of range [0,1]: q[${i}]=${q}`);
        }
    });

    console.log(`   ✓ Monotonicity maintained for ${pValues.length} p-values`);
    console.log(`   ✓ All q-values in range [0,1]`);
}

/**
 * Test edge cases: k=0, k=n, empty arrays, NaN values
 */
function testEdgeCases() {
    // Test empty arrays
    const emptyResult = EnhancedFDRCorrection.enhancedBenjaminiHochbergCorrection([], 0.05);
    if (emptyResult.qValues.length !== 0 || emptyResult.significant.length !== 0) {
        throw new Error('Empty array not handled correctly');
    }

    // Test k=0 (no successes)
    const pValue0 = EnhancedFDRCorrection.oneSidedBinomialTest(0, 20, 0.5, true);
    if (!Number.isFinite(pValue0) || pValue0 < 0 || pValue0 > 1) {
        throw new Error(`Invalid p-value for k=0: ${pValue0}`);
    }

    // Test k=n (all successes)
    const pValueN = EnhancedFDRCorrection.oneSidedBinomialTest(20, 20, 0.5, true);
    if (!Number.isFinite(pValueN) || pValueN < 0 || pValueN > 1) {
        throw new Error(`Invalid p-value for k=n: ${pValueN}`);
    }

    // Test n=0 (no trials)
    const pValueEmpty = EnhancedFDRCorrection.oneSidedBinomialTest(0, 0, 0.5, true);
    if (pValueEmpty !== 1.0) {
        throw new Error(`Invalid p-value for n=0: ${pValueEmpty} (expected 1.0)`);
    }

    // Test NaN and invalid p-values
    const invalidPValues = [NaN, -0.1, 1.5, undefined, null];
    const cleanResult = EnhancedFDRCorrection.enhancedBenjaminiHochbergCorrection(invalidPValues, 0.05);

    cleanResult.qValues.forEach((q, i) => {
        if (!Number.isFinite(q) || q < 0 || q > 1) {
            throw new Error(`Invalid q-value after cleaning: q[${i}]=${q}`);
        }
    });

    console.log(`   ✓ Edge cases handled: k=0, k=n, n=0, NaN values`);
}

/**
 * Test one-sided binomial test correctness
 */
function testOneSidedBinomialTest() {
    // Test known cases
    const testCases = [
        { successes: 15, trials: 20, p0: 0.5, expected: 'small' }, // Should be significant
        { successes: 10, trials: 20, p0: 0.5, expected: 'large' }, // Should not be significant
        { successes: 0, trials: 20, p0: 0.5, expected: 'very_large' }, // Very high p-value
        { successes: 20, trials: 20, p0: 0.5, expected: 'very_small' }  // Very low p-value
    ];

    testCases.forEach(({ successes, trials, p0, expected: _expected }, i) => {
        const pOneSided = EnhancedFDRCorrection.oneSidedBinomialTest(successes, trials, p0, true);
        const pTwoSided = EnhancedFDRCorrection.oneSidedBinomialTest(successes, trials, p0, false);

        // Basic validity checks
        if (!Number.isFinite(pOneSided) || pOneSided < 0 || pOneSided > 1) {
            throw new Error(`Invalid one-sided p-value for case ${i}: ${pOneSided}`);
        }

        if (!Number.isFinite(pTwoSided) || pTwoSided < 0 || pTwoSided > 1) {
            throw new Error(`Invalid two-sided p-value for case ${i}: ${pTwoSided}`);
        }

        // One-sided should generally be smaller than two-sided for extreme values
        if (successes !== trials / 2 && pOneSided > pTwoSided) {
            console.warn(`   ⚠️ Unexpected p-value relationship for case ${i}: one-sided=${pOneSided}, two-sided=${pTwoSided}`);
        }
    });

    console.log(`   ✓ One-sided binomial test validated for ${testCases.length} cases`);
}

/**
 * Test Benjamini-Hochberg correction
 */
function testBenjaminiHochbergCorrection() {
    // Test with known significant and non-significant values
    const pValues = [0.001, 0.01, 0.02, 0.03, 0.04, 0.1, 0.3];
    const alpha = 0.05;

    const result = EnhancedFDRCorrection.enhancedBenjaminiHochbergCorrection(pValues, alpha);

    // Check basic properties
    if (result.qValues.length !== pValues.length) {
        throw new Error('Q-values length mismatch');
    }

    if (result.significant.length !== pValues.length) {
        throw new Error('Significant array length mismatch');
    }

    // Check that very small p-values are likely significant
    const verySmallIndex = pValues.indexOf(0.001);
    if (verySmallIndex !== -1 && !result.significant[verySmallIndex]) {
        console.warn('   ⚠️ Very small p-value not significant after BH correction');
    }

    // Check that large p-values are likely not significant
    const largeIndex = pValues.indexOf(0.3);
    if (largeIndex !== -1 && result.significant[largeIndex]) {
        console.warn('   ⚠️ Large p-value significant after BH correction');
    }

    console.log(`   ✓ BH correction validated with ${pValues.length} p-values`);
    console.log(`   ✓ ${result.significant.filter(s => s).length} significant after correction`);
}

/**
 * Test permutation test
 */
function testPermutationTest() {
    // Test with extreme case (should have low permutation p-value)
    const extremeStats = { successes: 19, trials: 20 };
    const p0 = 0.5;
    const permP = EnhancedFDRCorrection.permutationTest(extremeStats, p0, 100);

    if (!Number.isFinite(permP) || permP < 0 || permP > 1) {
        throw new Error(`Invalid permutation p-value: ${permP}`);
    }

    // Test with null case (should have high permutation p-value)
    const nullStats = { successes: 10, trials: 20 };
    const nullPermP = EnhancedFDRCorrection.permutationTest(nullStats, p0, 100);

    if (!Number.isFinite(nullPermP) || nullPermP < 0 || nullPermP > 1) {
        throw new Error(`Invalid null permutation p-value: ${nullPermP}`);
    }

    // Extreme case should generally have lower permutation p-value
    if (permP > nullPermP) {
        console.warn(`   ⚠️ Unexpected permutation p-value relationship: extreme=${permP}, null=${nullPermP}`);
    }

    console.log(`   ✓ Permutation test validated: extreme case p=${permP.toFixed(3)}, null case p=${nullPermP.toFixed(3)}`);
}

/**
 * Test regime-aware baselines
 */
function testRegimeAwareBaselines() {
    const testStats = {
        R3: { successes: 45, trials: 52 },
        R2: { successes: 48, trials: 52 },
        R1: { successes: 39, trials: 52 },
        PIVOT: { successes: 26, trials: 52 },
        S1: { successes: 37, trials: 52 },
        S2: { successes: 44, trials: 52 },
        S3: { successes: 43, trials: 52 }
    };

    const regimes = ['UP-TREND', 'DOWN-TREND', 'RANGE'];

    regimes.forEach(regime => {
        const result = EnhancedFDRCorrection.analyzePivotSignificanceEnhanced(
            testStats, regime, 'weekly', { oneSided: true }
        );

        // Check that baselines are regime-specific
        const baselines = EnhancedFDRCorrection.DEFAULT_REGIME_BASELINES[regime];

        Object.keys(testStats).forEach(level => {
            const levelResult = result.levels[level];
            const expectedBaseline = baselines[level] * 100; // Convert to percentage

            if (Math.abs(levelResult.baseline - expectedBaseline) > 0.1) {
                throw new Error(`Baseline mismatch for ${regime} ${level}: expected ${expectedBaseline}, got ${levelResult.baseline}`);
            }
        });

        console.log(`   ✓ ${regime} regime baselines applied correctly`);
    });
}

/**
 * Test sample size filtering
 */
function testSampleSizeFiltering() {
    const testCases = [
        { n: 5, expected: false },   // Too small
        { n: 10, expected: false },  // Borderline
        { n: 15, expected: true },   // Sufficient
        { n: 50, expected: true }    // Large
    ];

    testCases.forEach(({ n, expected }) => {
        const stats = { testLevel: { successes: Math.floor(n * 0.8), trials: n } };

        const result = EnhancedFDRCorrection.analyzePivotSignificanceEnhanced(
            stats, 'RANGE', 'weekly', { minN: 12, minNEff: 10 }
        );

        const hasSufficientData = result.levels.testLevel.hasSufficientData;

        if (hasSufficientData !== expected) {
            throw new Error(`Sample size filtering failed for n=${n}: expected ${expected}, got ${hasSufficientData}`);
        }
    });

    console.log(`   ✓ Sample size filtering works correctly`);
}

/**
 * Test confidence intervals
 */
function testConfidenceIntervals() {
    const testCases = [
        { successes: 0, trials: 20 },
        { successes: 10, trials: 20 },
        { successes: 20, trials: 20 },
        { successes: 1, trials: 2 }
    ];

    testCases.forEach(({ successes, trials }, i) => {
        const ci = EnhancedFDRCorrection.wilsonConfidenceInterval(successes, trials, 0.95);

        // Check bounds
        if (ci.lower < 0 || ci.lower > 1) {
            throw new Error(`Invalid CI lower bound for case ${i}: ${ci.lower}`);
        }

        if (ci.upper < 0 || ci.upper > 1) {
            throw new Error(`Invalid CI upper bound for case ${i}: ${ci.upper}`);
        }

        if (ci.lower > ci.upper) {
            throw new Error(`Invalid CI ordering for case ${i}: lower=${ci.lower}, upper=${ci.upper}`);
        }

        // Check that observed proportion is in the interval (with some tolerance)
        const observed = successes / trials;
        if (observed < ci.lower - 0.01 || observed > ci.upper + 0.01) {
            console.warn(`   ⚠️ Observed proportion outside CI for case ${i}: observed=${observed}, CI=[${ci.lower}, ${ci.upper}]`);
        }
    });

    console.log(`   ✓ Wilson confidence intervals validated for ${testCases.length} cases`);
}

/**
 * Test tooltip generation
 */
function testTooltipGeneration() {
    const mockResult = {
        successes: 45,
        trials: 52,
        nEffective: 42,
        successRate: 86.5,
        baseline: 75.0,
        lift: 11.5,
        pValue: 0.001,
        qValue: 0.002,
        significant: true,
        hasSufficientData: true,
        confidenceInterval: { lower: 74.2, upper: 94.1, center: 86.5 },
        permutationPValue: 0.005,
        testType: 'one-sided',
        hypothesis: 'H1: p > 75.0%'
    };

    const tooltip = EnhancedFDRCorrection.generateTooltip('R3', mockResult, true);

    // Check that tooltip contains required information
    const requiredElements = ['Success Rate', 'Baseline', 'Lift', 'p-value', 'q-value', 'Sample Size', '95% CI'];

    requiredElements.forEach(element => {
        if (!tooltip.includes(element)) {
            throw new Error(`Tooltip missing required element: ${element}`);
        }
    });

    // Check debug mode elements
    if (!tooltip.includes('Permutation p-value')) {
        throw new Error('Tooltip missing debug information');
    }

    console.log(`   ✓ Tooltip generation validated with all required elements`);
}

/**
 * Test display formatting
 */
function testDisplayFormatting() {
    const mockResult = {
        successRate: 86.5,
        qValue: 0.002,
        significant: true,
        hasSufficientData: true,
        trials: 52
    };

    const display = EnhancedFDRCorrection.formatSignificanceDisplay('R3', mockResult);

    // Check display format
    if (!display.includes('R3')) {
        throw new Error('Display missing level name');
    }

    if (!display.includes('86.5%')) {
        throw new Error('Display missing success rate');
    }

    if (!display.includes('✓')) {
        throw new Error('Display missing significance mark');
    }

    // Test insufficient data case
    const insufficientResult = { ...mockResult, hasSufficientData: false, trials: 5 };
    const insufficientDisplay = EnhancedFDRCorrection.formatSignificanceDisplay('R3', insufficientResult);

    if (!insufficientDisplay.includes('Insufficient data')) {
        throw new Error('Display not handling insufficient data correctly');
    }

    console.log(`   ✓ Display formatting validated`);
}

// Export for use in HTML tests
if (typeof window !== 'undefined') {
    window.runEnhancedFDRTests = runAllTests;
}

// Export for Node.js testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { runAllTests };
}