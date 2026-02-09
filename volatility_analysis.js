/**
 * Volatility-Based Analysis Module
 * Implements BH correction per volatility subfamily within regime
 */

window.VolatilityAnalysis = (() => {

    /**
     * Classify volatility regime based on historical data
     */
    function classifyVolatility(priceData, windowSize = 20) {
        const volatilities = calculateRollingVolatility(priceData, windowSize);
        const percentiles = calculatePercentiles(volatilities);

        return priceData.map((bar, index) => {
            const vol = volatilities[index];
            if (vol === null) return { regime: null, value: null, percentile: null };

            let regime;
            if (percentiles[index] < 33) {
                regime = 'Low';
            } else if (percentiles[index] < 67) {
                regime = 'Normal';
            } else {
                regime = 'High';
            }

            return {
                regime: regime,
                value: vol,
                percentile: percentiles[index]
            };
        });
    }

    /**
     * Calculate rolling volatility (realized volatility)
     */
    function calculateRollingVolatility(priceData, windowSize) {
        const volatilities = [];

        for (let i = 0; i < priceData.length; i++) {
            if (i < windowSize - 1) {
                volatilities.push(null);
                continue;
            }

            const window = priceData.slice(i - windowSize + 1, i + 1);
            const returns = [];

            for (let j = 1; j < window.length; j++) {
                const ret = Math.log(window[j].close / window[j-1].close);
                returns.push(ret);
            }

            // Calculate standard deviation of returns
            const mean = returns.reduce((sum, ret) => sum + ret, 0) / returns.length;
            const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / returns.length;
            const volatility = Math.sqrt(variance) * Math.sqrt(252); // Annualized

            volatilities.push(volatility);
        }

        return volatilities;
    }

    /**
     * Calculate percentiles for volatility ranking
     */
    function calculatePercentiles(volatilities) {
        const validVols = volatilities.filter(vol => vol !== null);
        const sortedVols = [...validVols].sort((a, b) => a - b);

        return volatilities.map(vol => {
            if (vol === null) return null;

            // Find percentile rank
            let rank = 0;
            for (let i = 0; i < sortedVols.length; i++) {
                if (sortedVols[i] <= vol) rank = i + 1;
            }

            return (rank / sortedVols.length) * 100;
        });
    }

    /**
     * Run BH correction per volatility subfamily
     */
    function runVolatilitySubfamilyAnalysis(pivotStats, regime, volatilityData, options = {}) {
        const {
            alpha = 0.05,
            oneSided = true,
            minNEff = 10
        } = options;

        // Split data by volatility regime
        const volSplits = splitDataByVolatility(pivotStats, volatilityData);

        const results = {
            families: {},
            summary: {
                totalFamilies: 0,
                validFamilies: 0,
                significantLevels: 0,
                regime: regime
            }
        };

        // Process each volatility family
        Object.entries(volSplits).forEach(([volRegime, volData]) => {
            if (Object.keys(volData).length === 0) {
                results.families[volRegime] = {
                    levels: {},
                    summary: { validLevels: 0, significantLevels: 0, hasData: false }
                };
                return;
            }

            // Calculate statistics for this volatility subfamily
            const familyResults = analyzePivotSubfamily(volData, regime, volRegime, {
                alpha,
                oneSided,
                minNEff
            });

            results.families[volRegime] = familyResults;
            results.summary.totalFamilies++;

            if (familyResults.summary.validLevels > 0) {
                results.summary.validFamilies++;
                results.summary.significantLevels += familyResults.summary.significantLevels;
            }
        });

        return results;
    }

    /**
     * Split pivot statistics by volatility
     */
    function splitDataByVolatility(pivotStats, volatilityData) {
        const splits = {
            'Low': {},
            'Normal': {},
            'High': {}
        };

        // For each level, split the data by volatility
        Object.entries(pivotStats).forEach(([level, stats]) => {
            const volSplit = splitLevelByVolatility(stats, volatilityData);

            Object.entries(volSplit).forEach(([volRegime, volStats]) => {
                if (volStats.trials > 0) {
                    splits[volRegime][level] = volStats;
                }
            });
        });

        return splits;
    }

    /**
     * Split individual level data by volatility
     */
    function splitLevelByVolatility(levelStats, _volatilityData) {
        // Mock implementation - in practice would track individual bar outcomes
        // and split based on volatility regime at each bar

        const split = {
            'Low': { successes: 0, trials: 0 },
            'Normal': { successes: 0, trials: 0 },
            'High': { successes: 0, trials: 0 }
        };

        // Distribute the data across volatility regimes
        // This is a simplified mock - real implementation would track each bar
        const totalTrials = levelStats.trials;
        const successRate = levelStats.successes / totalTrials;

        // Assume roughly equal distribution across vol regimes for demo
        const volRegimes = ['Low', 'Normal', 'High'];
        volRegimes.forEach(volRegime => {
            const volTrials = Math.floor(totalTrials / 3);
            const volSuccesses = Math.round(volTrials * successRate * (0.8 + Math.random() * 0.4)); // Add some variance

            split[volRegime] = {
                successes: Math.min(volSuccesses, volTrials),
                trials: volTrials
            };
        });

        return split;
    }

    /**
     * Analyze a single volatility subfamily
     */
    function analyzePivotSubfamily(volData, regime, volRegime, options) {
        const { alpha, oneSided, minNEff } = options;

        // Get regime-specific baselines
        const baselines = window.EnhancedFDRCorrection.DEFAULT_REGIME_BASELINES[regime] ||
                          window.EnhancedFDRCorrection.DEFAULT_REGIME_BASELINES['RANGE'];

        const levels = Object.keys(volData);
        const pValues = [];
        const results = {};
        const validLevels = [];

        // Calculate p-values for each level in this volatility subfamily
        levels.forEach(level => {
            const stats = volData[level];
            const p0 = baselines[level] || 0.5;
            const nEff = Math.round(stats.trials * 0.8); // Simplified effective N

            // Check sample size requirements
            const hasSufficientData = stats.trials >= Math.max(12, minNEff) && nEff >= minNEff;

            let pValue = 1.0;
            let qValue = null;
            let significant = false;

            if (hasSufficientData) {
                pValue = window.EnhancedFDRCorrection.oneSidedBinomialTest(
                    stats.successes, stats.trials, p0, oneSided
                );
                pValues.push(pValue);
                validLevels.push(level);
            }

            // Calculate confidence interval
            const ci = window.EnhancedFDRCorrection.wilsonConfidenceInterval(
                stats.successes, stats.trials, 0.95
            );

            // Calculate lift
            const observedRate = stats.trials > 0 ? stats.successes / stats.trials : 0;
            const lift = (observedRate - p0) * 100;

            results[level] = {
                successes: stats.successes,
                trials: stats.trials,
                nEffective: nEff,
                successRate: observedRate * 100,
                baseline: p0 * 100,
                lift: lift,
                pValue: pValue,
                qValue: qValue,
                significant: significant,
                hasSufficientData: hasSufficientData,
                volatilityRegime: volRegime,
                confidenceInterval: {
                    lower: ci.lower * 100,
                    upper: ci.upper * 100,
                    center: ci.center * 100
                }
            };
        });

        // Apply BH correction to this subfamily
        if (pValues.length > 0) {
            const fdrResults = window.EnhancedFDRCorrection.enhancedBenjaminiHochbergCorrection(pValues, alpha);

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
                hasData: levels.length > 0,
                volatilityRegime: volRegime,
                fdrLevel: alpha
            }
        };
    }

    /**
     * Create volatility matrix display with BH results
     */
    function createVolatilityMatrix(volAnalysisResults, regime) {
        const matrix = document.createElement('div');
        matrix.className = 'volatility-matrix enhanced';

        const levels = ['R3', 'R2', 'R1', 'PIVOT', 'S1', 'S2', 'S3'];
        const volRegimes = ['Low', 'Normal', 'High'];

        const headerHTML = `
            <div class="matrix-header">
                <h4>📊 Volatility × ${regime} Matrix (BH Corrected)</h4>
                <div class="matrix-subtitle">Separate BH correction per volatility subfamily</div>
            </div>
            <div class="matrix-grid">
                <div class="matrix-header-row">
                    <div class="matrix-corner"></div>
                    ${levels.map(level => `<div class="level-header">${level}</div>`).join('')}
                </div>
                ${volRegimes.map(volRegime => createVolMatrixRow(volRegime, levels, volAnalysisResults)).join('')}
            </div>
            <div class="matrix-legend">
                <div class="legend-row">
                    <span class="legend-item">
                        <span class="cell-sample significant"></span> q ≤ 0.05 (significant)
                    </span>
                    <span class="legend-item">
                        <span class="cell-sample not-significant"></span> q > 0.05
                    </span>
                    <span class="legend-item">
                        <span class="cell-sample insufficient"></span> n_eff < 10 (grayed)
                    </span>
                </div>
                <div class="legend-note">
                    Each volatility row uses separate Benjamini-Hochberg correction (α=5%)
                </div>
            </div>
        `;

        matrix.innerHTML = headerHTML;
        return matrix;
    }

    /**
     * Create a row in the volatility matrix
     */
    function createVolMatrixRow(volRegime, levels, volAnalysisResults) {
        const familyData = volAnalysisResults.families[volRegime];

        const cells = levels.map(level => {
            const result = familyData.levels[level];

            if (!result || !result.hasSufficientData) {
                return `
                    <div class="vol-matrix-cell insufficient" title="Insufficient data">
                        <div class="cell-rate">—</div>
                        <div class="cell-q">—</div>
                    </div>
                `;
            }

            const cellClass = result.significant ? 'significant' : 'not-significant';
            const qDisplay = result.qValue < 0.001 ? '<0.001' : result.qValue.toFixed(3);

            return `
                <div class="vol-matrix-cell ${cellClass}"
                     title="${level} @ ${volRegime} Vol: ${result.successRate.toFixed(1)}% vs ${result.baseline.toFixed(1)}% baseline, q=${qDisplay}, n_eff=${result.nEffective}">
                    <div class="cell-rate">${result.successRate.toFixed(0)}%</div>
                    <div class="cell-q">q=${qDisplay}</div>
                    <div class="cell-lift">${result.lift >= 0 ? '+' : ''}${result.lift.toFixed(1)}%</div>
                </div>
            `;
        }).join('');

        const summaryText = familyData.summary.hasData ?
            `${familyData.summary.significantLevels}/${familyData.summary.validLevels} sig` :
            'No data';

        return `
            <div class="matrix-row">
                <div class="vol-regime-label">
                    <div class="vol-name">${volRegime} Vol</div>
                    <div class="vol-summary">${summaryText}</div>
                </div>
                ${cells}
            </div>
        `;
    }

    /**
     * Generate mock historical price data for testing
     */
    function generateMockPriceData(length = 200) {
        const data = [];
        let price = 4500; // Starting price

        for (let i = 0; i < length; i++) {
            const dailyReturn = (Math.random() - 0.5) * 0.04; // ±2% daily moves
            price *= (1 + dailyReturn);

            data.push({
                date: new Date(Date.now() - (length - i) * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                timestamp: Date.now() - (length - i) * 24 * 60 * 60 * 1000,
                open: price * (1 + (Math.random() - 0.5) * 0.01),
                high: price * (1 + Math.random() * 0.015),
                low: price * (1 - Math.random() * 0.015),
                close: price
            });
        }

        return data;
    }

    /**
     * Add volatility-specific styles
     */
    function addVolatilityStyles() {
        if (document.getElementById('volatility-analysis-styles')) return;

        const style = document.createElement('style');
        style.id = 'volatility-analysis-styles';
        style.textContent = `
            .volatility-matrix.enhanced {
                background: rgba(103, 58, 183, 0.1);
                border-left: 4px solid #673AB7;
            }

            .volatility-matrix.enhanced .matrix-header h4 {
                color: #673AB7;
            }

            .matrix-header-row {
                display: grid;
                grid-template-columns: 100px repeat(7, 1fr);
                gap: 4px;
                margin-bottom: 8px;
            }

            .matrix-corner {
                background: rgba(255, 255, 255, 0.05);
                border-radius: 4px;
            }

            .level-header {
                background: rgba(103, 58, 183, 0.2);
                color: #ffffff;
                padding: 8px 4px;
                text-align: center;
                font-size: 12px;
                font-weight: bold;
                border-radius: 4px;
            }

            .matrix-row {
                display: grid;
                grid-template-columns: 100px repeat(7, 1fr);
                gap: 4px;
                margin-bottom: 4px;
            }

            .vol-regime-label {
                background: rgba(255, 255, 255, 0.05);
                padding: 8px;
                border-radius: 4px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
            }

            .vol-name {
                font-size: 12px;
                font-weight: bold;
                color: #673AB7;
                margin-bottom: 2px;
            }

            .vol-summary {
                font-size: 10px;
                color: #aaa;
            }

            .vol-matrix-cell {
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 6px 4px;
                text-align: center;
                min-height: 55px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                transition: all 0.2s ease;
            }

            .vol-matrix-cell:hover {
                background: rgba(255, 255, 255, 0.1);
                transform: scale(1.02);
            }

            .vol-matrix-cell.significant {
                background: rgba(76, 175, 80, 0.15);
                border-color: #4CAF50;
                border-width: 2px;
            }

            .vol-matrix-cell.not-significant {
                background: rgba(255, 152, 0, 0.1);
                border-color: #ff9800;
            }

            .vol-matrix-cell.insufficient {
                background: rgba(128, 128, 128, 0.1);
                color: #666;
                border-color: rgba(128, 128, 128, 0.2);
            }

            .cell-rate {
                font-size: 12px;
                font-weight: bold;
                color: #ffffff;
                margin-bottom: 2px;
            }

            .cell-q {
                font-size: 9px;
                color: #cccccc;
                margin-bottom: 2px;
            }

            .cell-lift {
                font-size: 9px;
                color: #aaa;
            }

            .vol-matrix-cell.significant .cell-rate {
                color: #4CAF50;
            }

            .vol-matrix-cell.not-significant .cell-rate {
                color: #ff9800;
            }

            .legend-row {
                display: flex;
                gap: 20px;
                margin-bottom: 8px;
            }

            .legend-note {
                font-size: 10px;
                color: #aaa;
                font-style: italic;
            }

            .cell-sample.significant {
                background: rgba(76, 175, 80, 0.3);
                border: 1px solid #4CAF50;
            }

            .cell-sample.not-significant {
                background: rgba(255, 152, 0, 0.3);
                border: 1px solid #ff9800;
            }
        `;

        document.head.appendChild(style);
    }

    // Public API
    return {
        classifyVolatility,
        calculateRollingVolatility,
        runVolatilitySubfamilyAnalysis,
        splitDataByVolatility,
        analyzePivotSubfamily,
        createVolatilityMatrix,
        generateMockPriceData,
        addVolatilityStyles,

        // Utility functions
        calculatePercentiles,
        splitLevelByVolatility,
        createVolMatrixRow
    };
})();