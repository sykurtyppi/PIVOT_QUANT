/**
 * Integration of FDR correction into pivot calculator display
 * Adds significance testing to standard pivot levels (R3, R2, R1, PIVOT, S1, S2, S3)
 */

window.PivotFDRIntegration = (() => {

    /**
     * Sample historical data for demonstration
     * In practice, this would come from actual backtest results
     */
    const SAMPLE_HISTORICAL_DATA = {
        R3: { successes: 45, trials: 52 }, // 86.5% success rate
        R2: { successes: 48, trials: 52 }, // 92.3% success rate
        R1: { successes: 39, trials: 52 }, // 75.0% success rate
        PIVOT: { successes: 41, trials: 52 }, // 78.8% success rate
        S1: { successes: 37, trials: 52 }, // 71.2% success rate
        S2: { successes: 44, trials: 52 }, // 84.6% success rate
        S3: { successes: 43, trials: 52 }  // 82.7% success rate
    };

    /**
     * Enhanced table header with significance column
     */
    function createEnhancedTableHeader() {
        return `
            <tr>
                <th style="text-align: left;">Level</th>
                <th>Price</th>
                <th>Distance</th>
                <th>Strength</th>
                <th>Significance (FDR)</th>
            </tr>
        `;
    }

    /**
     * Create enhanced display results with FDR correction
     */
    function displayResultsWithFDR(data, historicalData = SAMPLE_HISTORICAL_DATA) {
        const container = document.getElementById('tables-container');
        container.innerHTML = '';

        // Get levels to show based on current pivot type
        let levelsToShow = {};
        if (window.currentPivotType === 'standard') {
            levelsToShow = data.standard;
        } else if (window.currentPivotType === 'camarilla') {
            levelsToShow = data.camarilla;
        } else if (window.currentPivotType === 'fibonacci') {
            levelsToShow = data.fibonacci;
        } else {
            levelsToShow = {...data.standard, ...data.camarilla, ...data.fibonacci};
        }

        // Only analyze FDR for standard pivot levels
        const standardLevels = ['R3', 'R2', 'R1', 'Pivot', 'S1', 'S2', 'S3'];
        const pivotStatsForFDR = {};

        standardLevels.forEach(level => {
            if (historicalData[level.toUpperCase()] || historicalData[level]) {
                const key = level.toUpperCase() === 'PIVOT' ? 'PIVOT' : level.toUpperCase();
                pivotStatsForFDR[level] = historicalData[key] || { successes: 0, trials: 0 };
            }
        });

        // Perform FDR analysis
        const fdrResults = window.FDRCorrection.analyzePivotSignificance(pivotStatsForFDR, 0.05);

        // Sort levels by value (descending)
        const sortedLevels = Object.entries(levelsToShow).sort((a, b) => b[1] - a[1]);

        // Create table
        const table = document.createElement('table');
        table.className = 'pivot-table';

        const thead = document.createElement('thead');
        thead.innerHTML = createEnhancedTableHeader();
        table.appendChild(thead);

        const tbody = document.createElement('tbody');

        // Find confluence zones
        const confluenceZones = findConfluenceZones(data);

        sortedLevels.forEach(([label, value]) => {
            const row = document.createElement('tr');

            // Level label
            const labelCell = document.createElement('td');
            labelCell.className = 'level-label';
            labelCell.textContent = label;
            row.appendChild(labelCell);

            // Price value
            const valueCell = document.createElement('td');
            valueCell.className = 'value-cell';

            const strength = getStrength(value, confluenceZones);
            valueCell.classList.add(strength);
            valueCell.textContent = value.toFixed(2);
            row.appendChild(valueCell);

            // Distance from current price
            const distanceCell = document.createElement('td');
            distanceCell.className = 'distance-cell';
            if (data.current) {
                const distance = value - data.current;
                const percentDistance = ((distance / data.current) * 100).toFixed(2);

                if (Math.abs(distance) < 5) {
                    distanceCell.className += ' at-price';
                    distanceCell.textContent = `AT PRICE ±${Math.abs(distance).toFixed(2)}`;
                } else if (distance > 0) {
                    distanceCell.className += ' above-price';
                    distanceCell.textContent = `+${distance.toFixed(2)} (${percentDistance}%)`;
                } else {
                    distanceCell.className += ' below-price';
                    distanceCell.textContent = `${distance.toFixed(2)} (${percentDistance}%)`;
                }
            } else {
                distanceCell.textContent = '-';
            }
            row.appendChild(distanceCell);

            // Strength indicator
            const strengthCell = document.createElement('td');
            const strengthLabels = {
                'strength-weak': '⚪ Weak',
                'strength-medium': '🟡 Medium',
                'strength-strong': '🟢 Strong',
                'strength-critical': '🔴 Critical'
            };
            strengthCell.textContent = strengthLabels[strength] || '⚪ Weak';
            row.appendChild(strengthCell);

            // FDR Significance cell
            const significanceCell = document.createElement('td');
            significanceCell.className = 'significance-cell';

            // Check if this level has FDR results
            const levelKey = label === 'Pivot' ? 'Pivot' : label;
            const fdrResult = fdrResults.levels[levelKey];

            if (fdrResult) {
                const displayText = window.FDRCorrection.formatSignificanceDisplay(label, fdrResult);
                significanceCell.innerHTML = formatSignificanceHTML(displayText, fdrResult);

                // Add styling based on significance
                if (fdrResult.significant) {
                    significanceCell.classList.add('significant');
                } else {
                    significanceCell.classList.add('not-significant');
                }
            } else {
                significanceCell.textContent = 'No data';
                significanceCell.classList.add('no-data');
            }

            row.appendChild(significanceCell);
            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);

        // Add FDR summary
        addFDRSummary(container, fdrResults);

        // Display confluence zones
        if (typeof displayConfluenceZones === 'function') {
            displayConfluenceZones(confluenceZones);
        }
    }

    /**
     * Format significance display as HTML with proper styling
     */
    function formatSignificanceHTML(displayText, result) {
        const parts = displayText.split('(FDR q');
        const beforeQ = parts[0];
        const afterQ = parts[1] ? '(FDR q' + parts[1] : '';

        const mark = result.significant ?
            '<span class="sig-mark significant">✓</span>' :
            '<span class="sig-mark not-significant">✗ Not significant</span>';

        return `<div class="sig-display">
            <div class="success-rate">${beforeQ.split(':')[1]?.trim() || beforeQ}</div>
            <div class="q-value">${afterQ.replace(/[✓✗].*$/, '')}</div>
            <div class="significance">${mark}</div>
        </div>`;
    }

    /**
     * Add FDR correction summary
     */
    function addFDRSummary(container, fdrResults) {
        const summary = document.createElement('div');
        summary.className = 'fdr-summary';

        const significantCount = fdrResults.summary.significantLevels;
        const totalCount = fdrResults.summary.totalLevels;

        summary.innerHTML = `
            <div class="fdr-header">
                <h3>📊 Statistical Significance Analysis (FDR Corrected)</h3>
                <div class="fdr-stats">
                    <span class="significant-count">${significantCount}/${totalCount} levels significant</span>
                    <span class="fdr-level">FDR = ${(fdrResults.summary.fdrLevel * 100).toFixed(1)}%</span>
                </div>
            </div>
            <div class="fdr-explanation">
                <p><strong>Benjamini-Hochberg FDR Correction:</strong> Controls false discovery rate across all ${totalCount} pivot levels.
                Q-values represent the expected proportion of false positives among significant results.</p>
                <p><strong>Interpretation:</strong> Levels marked ✓ show statistically significant deviation from 50% success rate after multiple testing correction.</p>
            </div>
        `;

        container.appendChild(summary);
    }

    /**
     * Add CSS styles for FDR display
     */
    function addFDRStyles() {
        const style = document.createElement('style');
        style.textContent = `
            .significance-cell {
                text-align: center;
                font-size: 12px;
                padding: 8px 4px;
            }

            .sig-display {
                display: flex;
                flex-direction: column;
                gap: 2px;
                align-items: center;
            }

            .success-rate {
                font-weight: bold;
                color: #ffffff;
            }

            .q-value {
                font-size: 10px;
                color: #cccccc;
            }

            .sig-mark.significant {
                color: #4CAF50;
                font-weight: bold;
            }

            .sig-mark.not-significant {
                color: #ff6b6b;
                font-size: 10px;
            }

            .significance-cell.significant {
                background: rgba(76, 175, 80, 0.1);
                border-left: 3px solid #4CAF50;
            }

            .significance-cell.not-significant {
                background: rgba(255, 107, 107, 0.1);
                border-left: 3px solid #ff6b6b;
            }

            .significance-cell.no-data {
                background: rgba(128, 128, 128, 0.1);
                color: #888;
            }

            .fdr-summary {
                margin-top: 20px;
                padding: 15px;
                background: rgba(255, 255, 255, 0.05);
                border-radius: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }

            .fdr-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }

            .fdr-header h3 {
                margin: 0;
                color: #ffffff;
                font-size: 16px;
            }

            .fdr-stats {
                display: flex;
                gap: 15px;
                font-size: 12px;
            }

            .significant-count {
                background: #4CAF50;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-weight: bold;
            }

            .fdr-level {
                background: #2196F3;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
            }

            .fdr-explanation {
                color: #cccccc;
                font-size: 12px;
                line-height: 1.4;
            }

            .fdr-explanation p {
                margin: 5px 0;
            }
        `;

        if (!document.getElementById('fdr-styles')) {
            style.id = 'fdr-styles';
            document.head.appendChild(style);
        }
    }

    /**
     * Initialize FDR integration
     */
    function initialize() {
        addFDRStyles();

        // Replace the original displayResults function if it exists
        if (typeof window.displayResults === 'function') {
            window.originalDisplayResults = window.displayResults;
            window.displayResults = displayResultsWithFDR;
        }
    }

    /**
     * Test with sample data
     */
    function testWithSampleData() {
        const sampleData = {
            standard: {
                'R3': 4564.34,
                'R2': 4542.42,
                'R1': 4524.09,
                'Pivot': 4502.17,
                'S1': 4483.84,
                'S2': 4465.51,
                'S3': 4447.59
            },
            camarilla: {},
            fibonacci: {},
            current: 4500.00
        };

        displayResultsWithFDR(sampleData);
    }

    // Public API
    return {
        initialize,
        displayResultsWithFDR,
        testWithSampleData,
        SAMPLE_HISTORICAL_DATA
    };
})();

// Auto-initialize when script loads
document.addEventListener('DOMContentLoaded', () => {
    window.PivotFDRIntegration.initialize();
});