/**
 * Enhanced pivot display with regime-aware significance testing
 * Integrates with Level Reliability and adds Significance column
 */

window.EnhancedPivotDisplay = (() => {

    /**
     * Create enhanced Level Reliability table with Significance column
     */
    function createEnhancedLevelReliabilityTable(fdrResults, regime, timeframe, debugMode = false) {
        const table = document.createElement('table');
        table.className = 'level-reliability-table enhanced';

        // Create header
        const thead = document.createElement('thead');
        thead.innerHTML = `
            <tr>
                <th>Level</th>
                <th>Success Rate</th>
                <th>Baseline</th>
                <th>Lift</th>
                <th>Sample Size</th>
                <th>95% CI</th>
                <th>Significance</th>
                ${debugMode ? '<th>Debug</th>' : ''}
            </tr>
        `;
        table.appendChild(thead);

        // Create body
        const tbody = document.createElement('tbody');
        const levels = ['R3', 'R2', 'R1', 'PIVOT', 'S1', 'S2', 'S3'];

        levels.forEach(level => {
            const result = fdrResults.levels[level];
            if (!result) return;

            const row = document.createElement('tr');

            // Apply CSS classes based on significance and data sufficiency
            if (!result.hasSufficientData) {
                row.classList.add('insufficient-data');
            } else if (result.significant) {
                row.classList.add('significant');
            } else {
                row.classList.add('not-significant');
            }

            // Level name
            const levelCell = document.createElement('td');
            levelCell.textContent = level;
            levelCell.className = 'level-name';
            row.appendChild(levelCell);

            // Success rate
            const rateCell = document.createElement('td');
            rateCell.textContent = `${result.successRate.toFixed(1)}% (${result.successes}/${result.trials})`;
            rateCell.className = 'success-rate';
            row.appendChild(rateCell);

            // Baseline
            const baselineCell = document.createElement('td');
            baselineCell.textContent = `${result.baseline.toFixed(1)}%`;
            baselineCell.className = 'baseline';
            row.appendChild(baselineCell);

            // Lift
            const liftCell = document.createElement('td');
            const liftValue = result.lift;
            liftCell.textContent = `${liftValue >= 0 ? '+' : ''}${liftValue.toFixed(1)}%`;
            liftCell.className = `lift ${liftValue >= 0 ? 'positive' : 'negative'}`;
            row.appendChild(liftCell);

            // Sample size
            const sampleCell = document.createElement('td');
            sampleCell.textContent = `${result.trials} (${result.nEffective})`;
            sampleCell.className = 'sample-size';
            if (!result.hasSufficientData) {
                sampleCell.classList.add('insufficient');
            }
            row.appendChild(sampleCell);

            // Confidence interval
            const ciCell = document.createElement('td');
            ciCell.textContent = `[${result.confidenceInterval.lower.toFixed(1)}%, ${result.confidenceInterval.upper.toFixed(1)}%]`;
            ciCell.className = 'confidence-interval';
            row.appendChild(ciCell);

            // Significance
            const sigCell = document.createElement('td');
            sigCell.className = 'significance-cell';

            if (!result.hasSufficientData) {
                sigCell.innerHTML = '<span class="sig-gray">—</span>';
                sigCell.title = `Insufficient data (n=${result.trials}, n_eff=${result.nEffective}). Minimum required: n≥12, n_eff≥10`;
            } else {
                const mark = result.significant ? '✓' : '✗';
                const markClass = result.significant ? 'sig-pass' : 'sig-fail';
                sigCell.innerHTML = `<span class="${markClass}">${mark}</span>`;

                // Add comprehensive tooltip
                const tooltip = window.EnhancedFDRCorrection.generateTooltip(level, result, debugMode);
                sigCell.title = tooltip;
            }
            row.appendChild(sigCell);

            // Debug column
            if (debugMode) {
                const debugCell = document.createElement('td');
                debugCell.className = 'debug-info';

                if (result.hasSufficientData) {
                    const debugInfo = [];
                    debugInfo.push(`p=${result.pValue.toFixed(4)}`);
                    if (result.qValue !== null) {
                        debugInfo.push(`q=${result.qValue.toFixed(4)}`);
                    }
                    if (result.permutationPValue !== null) {
                        debugInfo.push(`p_perm=${result.permutationPValue.toFixed(4)}`);
                    }
                    debugCell.textContent = debugInfo.join(', ');
                } else {
                    debugCell.textContent = 'N/A';
                }
                row.appendChild(debugCell);
            }

            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        return table;
    }

    /**
     * Create legend and controls
     */
    function createLegendAndControls(fdrResults, debugMode = false, onToggle = null) {
        const container = document.createElement('div');
        container.className = 'significance-legend-container';

        // Legend
        const legend = document.createElement('div');
        legend.className = 'significance-legend';
        legend.innerHTML = `
            <div class="legend-header">
                <h4>📊 Statistical Significance Legend</h4>
                <div class="legend-items">
                    <span class="legend-item"><span class="sig-pass">✓</span> Significant (q ≤ 0.05)</span>
                    <span class="legend-item"><span class="sig-fail">✗</span> Not significant (q > 0.05)</span>
                    <span class="legend-item"><span class="sig-gray">—</span> Insufficient data</span>
                </div>
            </div>
            <div class="legend-note">
                Tests are regime-aware (${fdrResults.summary.regime}) and FDR-corrected (BH, α=5%).
                One-sided tests: H1: success rate > regime baseline.
            </div>
        `;

        // Controls
        const controls = document.createElement('div');
        controls.className = 'significance-controls';

        // Test mode toggle
        const testModeToggle = document.createElement('label');
        testModeToggle.className = 'toggle-control';
        testModeToggle.innerHTML = `
            <input type="checkbox" id="test-mode-toggle" ${fdrResults.summary.testType === 'two-sided' ? 'checked' : ''}>
            <span class="toggle-label">Two-sided test (for debugging)</span>
        `;

        // Debug mode toggle
        const debugToggle = document.createElement('label');
        debugToggle.className = 'toggle-control';
        debugToggle.innerHTML = `
            <input type="checkbox" id="debug-mode-toggle" ${debugMode ? 'checked' : ''}>
            <span class="toggle-label">Debug mode (show p-values, permutation tests)</span>
        `;

        controls.appendChild(testModeToggle);
        controls.appendChild(debugToggle);

        // Add event listeners
        if (onToggle) {
            testModeToggle.querySelector('input').addEventListener('change', (e) => {
                onToggle('testMode', e.target.checked);
            });

            debugToggle.querySelector('input').addEventListener('change', (e) => {
                onToggle('debugMode', e.target.checked);
            });
        }

        container.appendChild(legend);
        container.appendChild(controls);
        return container;
    }

    /**
     * Create summary statistics panel
     */
    function createSummaryPanel(fdrResults) {
        const panel = document.createElement('div');
        panel.className = 'significance-summary-panel';

        const summary = fdrResults.summary;

        panel.innerHTML = `
            <div class="summary-header">
                <h4>📈 ${summary.regime} Regime Analysis (${summary.timeframe})</h4>
            </div>
            <div class="summary-stats">
                <div class="stat-group">
                    <div class="stat-item">
                        <span class="stat-value significant">${summary.significantLevels}</span>
                        <span class="stat-label">Significant levels</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-value total">${summary.validLevels}</span>
                        <span class="stat-label">Valid levels</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-value insufficient">${summary.insufficientDataLevels}</span>
                        <span class="stat-label">Insufficient data</span>
                    </div>
                </div>
                <div class="test-info">
                    <span class="test-type">${summary.testType.toUpperCase()}</span>
                    <span class="fdr-level">FDR = ${(summary.fdrLevel * 100).toFixed(1)}%</span>
                </div>
            </div>
        `;

        return panel;
    }

    /**
     * Add CSS styles for enhanced display
     */
    function addEnhancedStyles() {
        const style = document.createElement('style');
        style.id = 'enhanced-significance-styles';
        style.textContent = `
            .level-reliability-table.enhanced {
                width: 100%;
                border-collapse: collapse;
                background: rgba(255, 255, 255, 0.03);
                border-radius: 8px;
                overflow: hidden;
                margin: 15px 0;
            }

            .level-reliability-table.enhanced th,
            .level-reliability-table.enhanced td {
                padding: 12px 8px;
                text-align: left;
                border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                font-size: 13px;
            }

            .level-reliability-table.enhanced th {
                background: rgba(255, 255, 255, 0.1);
                font-weight: 600;
                color: #ffffff;
                text-align: center;
            }

            .level-reliability-table.enhanced .level-name {
                font-weight: bold;
                color: #00d4ff;
            }

            .level-reliability-table.enhanced .lift.positive {
                color: #4CAF50;
            }

            .level-reliability-table.enhanced .lift.negative {
                color: #ff6b6b;
            }

            .level-reliability-table.enhanced .sample-size.insufficient {
                color: #ff9800;
                font-weight: bold;
            }

            .level-reliability-table.enhanced tr.significant {
                background: rgba(76, 175, 80, 0.15);
                border-left: 3px solid #4CAF50;
            }

            .level-reliability-table.enhanced tr.not-significant {
                background: rgba(255, 107, 107, 0.15);
                border-left: 3px solid #ff6b6b;
            }

            .level-reliability-table.enhanced tr.insufficient-data {
                background: rgba(158, 158, 158, 0.15);
                border-left: 3px solid #9e9e9e;
                color: #cccccc;
            }

            .significance-cell {
                text-align: center;
                font-size: 16px;
                font-weight: bold;
            }

            .sig-pass {
                color: #4CAF50;
                font-size: 18px;
            }

            .sig-fail {
                color: #ff6b6b;
                font-size: 16px;
            }

            .sig-gray {
                color: #9e9e9e;
                font-size: 16px;
            }

            .debug-info {
                font-family: 'Courier New', monospace;
                font-size: 11px;
                color: #cccccc;
            }

            .significance-legend-container {
                margin: 20px 0;
                padding: 15px;
                background: rgba(255, 255, 255, 0.05);
                border-radius: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }

            .significance-legend .legend-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }

            .significance-legend h4 {
                margin: 0;
                color: #ffffff;
                font-size: 14px;
            }

            .legend-items {
                display: flex;
                gap: 20px;
                flex-wrap: wrap;
            }

            .legend-item {
                display: flex;
                align-items: center;
                gap: 5px;
                font-size: 12px;
                color: #cccccc;
            }

            .legend-note {
                color: #aaaaaa;
                font-size: 11px;
                line-height: 1.4;
                margin-top: 8px;
                font-style: italic;
            }

            .significance-controls {
                margin-top: 15px;
                display: flex;
                gap: 20px;
                flex-wrap: wrap;
            }

            .toggle-control {
                display: flex;
                align-items: center;
                gap: 8px;
                cursor: pointer;
                font-size: 12px;
                color: #cccccc;
            }

            .toggle-control input[type="checkbox"] {
                margin: 0;
                transform: scale(1.1);
            }

            .significance-summary-panel {
                margin: 15px 0;
                padding: 15px;
                background: rgba(0, 150, 199, 0.1);
                border-left: 4px solid #0096c7;
                border-radius: 0 6px 6px 0;
            }

            .summary-header h4 {
                margin: 0 0 10px 0;
                color: #00d4ff;
                font-size: 16px;
            }

            .summary-stats {
                display: flex;
                justify-content: space-between;
                align-items: center;
                flex-wrap: wrap;
                gap: 20px;
            }

            .stat-group {
                display: flex;
                gap: 20px;
            }

            .stat-item {
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 4px;
            }

            .stat-value {
                font-size: 20px;
                font-weight: bold;
            }

            .stat-value.significant {
                color: #4CAF50;
            }

            .stat-value.total {
                color: #2196F3;
            }

            .stat-value.insufficient {
                color: #ff9800;
            }

            .stat-label {
                font-size: 10px;
                color: #cccccc;
                text-align: center;
            }

            .test-info {
                display: flex;
                flex-direction: column;
                gap: 4px;
                align-items: flex-end;
            }

            .test-type {
                background: #2196F3;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 11px;
                font-weight: bold;
            }

            .fdr-level {
                background: #ff9800;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 11px;
            }
        `;

        if (!document.getElementById('enhanced-significance-styles')) {
            document.head.appendChild(style);
        }
    }

    /**
     * Create complete enhanced display
     */
    function createCompleteEnhancedDisplay(pivotStats, regime = 'RANGE', timeframe = 'weekly', options = {}) {
        const defaultOptions = {
            alpha: 0.05,
            oneSided: true,
            debugMode: false,
            permutationTests: false
        };

        const finalOptions = { ...defaultOptions, ...options };

        // Perform FDR analysis
        const fdrResults = window.EnhancedFDRCorrection.analyzePivotSignificanceEnhanced(
            pivotStats, regime, timeframe, finalOptions
        );

        // Create container
        const container = document.createElement('div');
        container.className = 'enhanced-significance-display';

        // Add styles
        addEnhancedStyles();

        // Create toggle handler
        const handleToggle = (type, value) => {
            if (type === 'testMode') {
                finalOptions.oneSided = !value;
            } else if (type === 'debugMode') {
                finalOptions.debugMode = value;
                finalOptions.permutationTests = value;
            }

            // Recreate display with new options
            const newDisplay = createCompleteEnhancedDisplay(pivotStats, regime, timeframe, finalOptions);
            container.parentNode.replaceChild(newDisplay, container);
        };

        // Add components
        container.appendChild(createSummaryPanel(fdrResults));
        container.appendChild(createEnhancedLevelReliabilityTable(fdrResults, regime, timeframe, finalOptions.debugMode));
        container.appendChild(createLegendAndControls(fdrResults, finalOptions.debugMode, handleToggle));

        return container;
    }

    // Public API
    return {
        createCompleteEnhancedDisplay,
        createEnhancedLevelReliabilityTable,
        createLegendAndControls,
        createSummaryPanel,
        addEnhancedStyles
    };
})();