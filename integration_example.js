/**
 * Example integration of FDR correction into existing pivot calculator
 *
 * To integrate into V3_PIVOT_CALCULATOR_PRO.html:
 * 1. Include fdr_correction.js script
 * 2. Include pivot_fdr_integration.js script
 * 3. Replace the displayResults function call
 */

// Example of how to integrate FDR correction into your existing pivot calculator:

/*
STEP 1: Add script tags to your HTML head section:
<script src="fdr_correction.js"></script>
<script src="pivot_fdr_integration.js"></script>

STEP 2: Modify your calculatePivots() function:
*/

function _calculatePivotsWithFDR() {
    const high = parseFloat(document.getElementById('input-high').value);
    const low = parseFloat(document.getElementById('input-low').value);
    const close = parseFloat(document.getElementById('input-close').value);
    const current = parseFloat(document.getElementById('input-current').value);

    if (!high || !low || !close) {
        return;
    }

    // Calculate all pivot methods (existing code)
    const standard = calculateStandardPivots(high, low, close);
    const camarilla = calculateCamarillaPivots(high, low, close);
    const fibonacci = calculateFibonacciLevels(high, low, close);

    const data = {
        standard,
        camarilla,
        fibonacci,
        current: current || null
    };

    // Store current data globally
    window.currentData = data;

    // Use FDR-enhanced display instead of original displayResults
    if (window.PivotFDRIntegration) {
        window.PivotFDRIntegration.displayResultsWithFDR(data);
    } else {
        // Fallback to original display if FDR module not loaded
        displayResults(data);
    }
}

/*
STEP 3: Customize historical data for your specific use case:
You can replace the SAMPLE_HISTORICAL_DATA in pivot_fdr_integration.js
with your actual backtest results
*/

const _YOUR_HISTORICAL_DATA = {
    R3: { successes: 45, trials: 52 }, // Replace with your backtest results
    R2: { successes: 48, trials: 52 },
    R1: { successes: 39, trials: 52 },
    PIVOT: { successes: 41, trials: 52 },
    S1: { successes: 37, trials: 52 },
    S2: { successes: 44, trials: 52 },
    S3: { successes: 43, trials: 52 }
};

/*
STEP 4: Call with your data:
window.PivotFDRIntegration.displayResultsWithFDR(data, YOUR_HISTORICAL_DATA);
*/

// Example output format after integration:
/*
┌─────────┬────────────┬──────────────────┬──────────────┬─────────────────────────────────┐
│ Level   │ Price      │ Distance         │ Strength     │ Significance (FDR)              │
├─────────┼────────────┼──────────────────┼──────────────┼─────────────────────────────────┤
│ R3      │ 4564.34    │ +64.34 (1.43%)   │ 🟢 Strong   │ 86.5% (FDR q<0.001) ✓         │
│ R2      │ 4542.42    │ +42.42 (0.94%)   │ 🟡 Medium   │ 92.3% (FDR q<0.001) ✓         │
│ R1      │ 4524.09    │ +24.09 (0.54%)   │ 🟡 Medium   │ 75.0% (FDR q<0.001) ✓         │
│ Pivot   │ 4502.17    │ +2.17 (0.05%)    │ 🔴 Critical │ 78.8% (FDR q<0.001) ✓         │
│ S1      │ 4483.84    │ -16.16 (-0.36%)  │ 🟡 Medium   │ 71.2% (FDR q<0.01) ✓          │
│ S2      │ 4465.51    │ -34.49 (-0.77%)  │ 🟡 Medium   │ 84.6% (FDR q<0.001) ✓         │
│ S3      │ 4447.59    │ -52.41 (-1.16%)  │ 🟢 Strong   │ 82.7% (FDR q<0.001) ✓         │
└─────────┴────────────┴──────────────────┴──────────────┴─────────────────────────────────┘

📊 Statistical Significance Analysis (FDR Corrected)
• 7/7 levels significant | FDR = 5.0%
• Benjamini-Hochberg FDR Correction: Controls false discovery rate across all 7 pivot levels
• Q-values represent the expected proportion of false positives among significant results
• Levels marked ✓ show statistically significant deviation from 50% success rate after multiple testing correction
*/

/* eslint-disable-next-line no-console */
console.log('✅ FDR Integration Example Ready');
/* eslint-disable-next-line no-console */
console.log('📊 Follow the 4 steps above to integrate FDR correction into your pivot calculator');