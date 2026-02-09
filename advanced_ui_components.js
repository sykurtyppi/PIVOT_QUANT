/**
 * Advanced UI Components for Enhanced Pivot Analysis
 * Top Picks cards, sparklines, volatility matrix, walk-forward analysis
 */

window.AdvancedUIComponents = (() => {

    /**
     * Create Top Picks card for a regime
     */
    function createTopPicksCard(fdrResults, regime, walkForwardData = null) {
        const topPicks = window.AdvancedAnalytics.generateTopPicks(fdrResults, regime, 2);

        const card = document.createElement('div');
        card.className = 'top-picks-card';

        if (topPicks.length === 0) {
            card.innerHTML = `
                <div class="top-picks-header">
                    <h4>🎯 Top Picks - ${regime}</h4>
                    <span class="no-picks-badge">No Significant Levels</span>
                </div>
                <div class="no-picks-message">
                    No levels meet the significance and quality thresholds for ${regime} regime.
                </div>
            `;
            return card;
        }

        const picksHTML = topPicks.map((pick, index) => {
            const result = pick.result;
            const stability = walkForwardData ? walkForwardData[pick.level] : null;
            const stabilityBadge = createStabilityBadge(stability);

            return `
                <div class="pick-item ${index === 0 ? 'primary-pick' : 'secondary-pick'}">
                    <div class="pick-header">
                        <div class="pick-level">${pick.level}</div>
                        <div class="pick-score">Score: ${(pick.score * 100).toFixed(0)}</div>
                        ${stabilityBadge}
                    </div>
                    <div class="pick-metrics">
                        <div class="metric">
                            <span class="metric-label">Success Rate</span>
                            <span class="metric-value">${result.successRate.toFixed(1)}%</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">Lift</span>
                            <span class="metric-value ${result.lift > 0 ? 'positive' : 'negative'}">
                                ${result.lift > 0 ? '+' : ''}${result.lift.toFixed(1)}%
                            </span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">Confidence</span>
                            <span class="metric-value">
                                q=${result.qValue < 0.001 ? '<0.001' : result.qValue.toFixed(3)}
                            </span>
                        </div>
                    </div>
                    <div class="pick-ci">
                        95% CI: [${result.confidenceInterval.lower.toFixed(1)}%, ${result.confidenceInterval.upper.toFixed(1)}%]
                    </div>
                    ${stability ? createSparklineDisplay(stability, pick.level) : ''}
                </div>
            `;
        }).join('');

        card.innerHTML = `
            <div class="top-picks-header">
                <h4>🎯 Top Picks - ${regime}</h4>
                <span class="picks-count">${topPicks.length} Strong Level${topPicks.length > 1 ? 's' : ''}</span>
            </div>
            <div class="picks-container">
                ${picksHTML}
            </div>
        `;

        return card;
    }

    /**
     * Create stability badge based on walk-forward variance
     */
    function createStabilityBadge(stability) {
        if (!stability || stability.variance === null) {
            return '<span class="stability-badge unknown">UNKNOWN</span>';
        }

        const badgeClass = stability.stability.toLowerCase();
        const trendIcon = stability.trend > 1 ? '↗' : stability.trend < -1 ? '↘' : '→';

        return `<span class="stability-badge ${badgeClass}" title="Variance: ${stability.variance.toFixed(1)}, Trend: ${stability.trend.toFixed(2)}">${stability.stability} ${trendIcon}</span>`;
    }

    /**
     * Create sparkline display for walk-forward data
     */
    function createSparklineDisplay(stability, _level) {
        if (!stability.liftSeries || stability.liftSeries.length < 2) {
            return '<div class="sparkline-container">Insufficient data for trend</div>';
        }

        const liftSparkline = window.AdvancedAnalytics.generateSparklineData(stability.liftSeries, 80, 25);
        const qSparkline = window.AdvancedAnalytics.generateSparklineData(stability.qSeries, 80, 25);

        return `
            <div class="sparkline-container">
                <div class="sparkline-group">
                    <div class="sparkline-label">Lift Trend:</div>
                    <svg class="sparkline" width="80" height="25" viewBox="0 0 80 25">
                        <path d="${liftSparkline.path}" fill="none" stroke="#4CAF50" stroke-width="1.5"/>
                        <circle cx="${liftSparkline.points[liftSparkline.points.length-1].x}"
                                cy="${liftSparkline.points[liftSparkline.points.length-1].y}"
                                r="2" fill="#4CAF50"/>
                    </svg>
                    <span class="sparkline-value">${liftSparkline.points[liftSparkline.points.length-1].value.toFixed(1)}%</span>
                </div>
                <div class="sparkline-group">
                    <div class="sparkline-label">Q-value Trend:</div>
                    <svg class="sparkline" width="80" height="25" viewBox="0 0 80 25">
                        <path d="${qSparkline.path}" fill="none" stroke="#2196F3" stroke-width="1.5"/>
                        <circle cx="${qSparkline.points[qSparkline.points.length-1].x}"
                                cy="${qSparkline.points[qSparkline.points.length-1].y}"
                                r="2" fill="#2196F3"/>
                    </svg>
                    <span class="sparkline-value">${qSparkline.points[qSparkline.points.length-1].value.toFixed(3)}</span>
                </div>
            </div>
        `;
    }

    /**
     * Create volatility × regime matrix
     */
    function createVolatilityMatrix(data, regime, timeframe) {
        const matrix = document.createElement('div');
        matrix.className = 'volatility-matrix';

        // Mock volatility data for demonstration
        const volatilityData = generateMockVolatilityData(data);
        const volSplits = window.AdvancedAnalytics.splitByVolatility(data, volatilityData);

        const matrixHTML = `
            <div class="matrix-header">
                <h4>📊 Volatility × ${regime} Analysis</h4>
                <div class="matrix-subtitle">${timeframe} timeframe</div>
            </div>
            <div class="matrix-grid">
                ${createVolatilityRow('Low Vol', volSplits.Low, regime)}
                ${createVolatilityRow('Normal Vol', volSplits.Normal, regime)}
                ${createVolatilityRow('High Vol', volSplits.High, regime)}
            </div>
            <div class="matrix-legend">
                <span class="legend-item">
                    <span class="cell-sample sufficient"></span> n_eff ≥ 10
                </span>
                <span class="legend-item">
                    <span class="cell-sample insufficient"></span> n_eff < 10 (grayed)
                </span>
            </div>
        `;

        matrix.innerHTML = matrixHTML;
        return matrix;
    }

    /**
     * Create a row in the volatility matrix
     */
    function createVolatilityRow(volLabel, volData, _regime) {
        const levels = ['R3', 'R2', 'R1', 'PIVOT', 'S1', 'S2', 'S3'];

        const cells = levels.map(level => {
            // Mock calculation for demo
            const nEff = volData.length > 0 ? Math.floor(volData.length * 0.8) : 0;
            const sufficient = nEff >= 10;
            const successRate = sufficient ? 60 + Math.random() * 30 : 0;
            const qValue = sufficient ? Math.random() * 0.1 : 1;

            const cellClass = sufficient ? 'matrix-cell sufficient' : 'matrix-cell insufficient';

            return `
                <div class="${cellClass}" title="${level} @ ${volLabel}: n_eff=${nEff}">
                    <div class="cell-level">${level}</div>
                    <div class="cell-rate">${sufficient ? successRate.toFixed(0) + '%' : '—'}</div>
                    <div class="cell-q">${sufficient ? (qValue < 0.001 ? '<0.001' : qValue.toFixed(3)) : '—'}</div>
                </div>
            `;
        }).join('');

        return `
            <div class="matrix-row">
                <div class="row-label">${volLabel}</div>
                <div class="row-cells">${cells}</div>
            </div>
        `;
    }

    /**
     * Generate mock volatility data for demonstration
     */
    function generateMockVolatilityData(data) {
        return data.map((_, _index) => ({
            percentile: Math.random() * 100,
            value: 0.1 + Math.random() * 0.4 // 10-50% volatility
        }));
    }

    /**
     * Create walk-forward summary panel
     */
    function createWalkForwardSummary(_data, _regime) {
        const panel = document.createElement('div');
        panel.className = 'walk-forward-summary';

        // Mock walk-forward analysis for each level
        const levels = ['R3', 'R2', 'R1', 'PIVOT', 'S1', 'S2', 'S3'];
        const walkForwardResults = {};

        levels.forEach(level => {
            // Generate mock time series data
            const timeSeries = generateMockTimeSeries(level, 200);
            const stability = window.AdvancedAnalytics.calculateWalkForwardStability(timeSeries, 60, 20);
            walkForwardResults[level] = stability;
        });

        const summaryHTML = `
            <div class="summary-header">
                <h4>📈 Walk-Forward Analysis Summary</h4>
                <div class="summary-subtitle">60-bar windows, 20-bar steps</div>
            </div>
            <div class="summary-grid">
                ${levels.map(level => createWalkForwardRow(level, walkForwardResults[level])).join('')}
            </div>
            <div class="summary-note">
                Sparklines show lift evolution over rolling windows. Stability based on lift variance.
            </div>
        `;

        panel.innerHTML = summaryHTML;
        return panel;
    }

    /**
     * Create a row in the walk-forward summary
     */
    function createWalkForwardRow(level, stability) {
        if (!stability || !stability.liftSeries) {
            return `
                <div class="wf-row">
                    <div class="wf-level">${level}</div>
                    <div class="wf-sparkline">No data</div>
                    <div class="wf-stability">
                        <span class="stability-badge unknown">UNKNOWN</span>
                    </div>
                </div>
            `;
        }

        const sparklineData = window.AdvancedAnalytics.generateSparklineData(stability.liftSeries, 100, 20);
        const stabilityBadge = createStabilityBadge(stability);

        return `
            <div class="wf-row">
                <div class="wf-level">${level}</div>
                <div class="wf-sparkline">
                    <svg width="100" height="20" viewBox="0 0 100 20">
                        <path d="${sparklineData.path}" fill="none" stroke="#00d4ff" stroke-width="1.5"/>
                        <circle cx="${sparklineData.points[sparklineData.points.length-1].x}"
                                cy="${sparklineData.points[sparklineData.points.length-1].y}"
                                r="1.5" fill="#00d4ff"/>
                    </svg>
                </div>
                <div class="wf-stability">
                    ${stabilityBadge}
                    <div class="wf-stats">
                        σ²=${stability.variance.toFixed(0)}, trend=${stability.trend.toFixed(2)}
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Generate mock time series for walk-forward analysis
     */
    function generateMockTimeSeries(level, length) {
        const series = [];
        let baseSuccessRate = 0.6; // Base 60% success rate

        // Adjust base rate by level
        if (['R2', 'S2'].includes(level)) baseSuccessRate = 0.75;
        if (['R3', 'S3'].includes(level)) baseSuccessRate = 0.55;

        for (let i = 0; i < length; i++) {
            // Add some drift and volatility
            const drift = Math.sin(i / 20) * 0.1; // Cyclical drift
            const noise = (Math.random() - 0.5) * 0.2; // Random noise
            const successProb = Math.max(0.2, Math.min(0.9, baseSuccessRate + drift + noise));

            series.push({
                index: i,
                success: Math.random() < successProb,
                successProb: successProb
            });
        }

        return series;
    }

    /**
     * Create alerts panel
     */
    function createAlertsPanel(fdrResults, regime, walkForwardData = null) {
        const panel = document.createElement('div');
        panel.className = 'alerts-panel';

        const alertRules = window.AdvancedAnalytics.createAlertRules();
        const allAlerts = [];

        // Evaluate alerts for each level
        Object.entries(fdrResults.levels).forEach(([level, result]) => {
            if (result.hasSufficientData) {
                const stability = walkForwardData ? walkForwardData[level] : null;
                const alerts = window.AdvancedAnalytics.evaluateAlerts(level, result, stability, alertRules);
                allAlerts.push(...alerts);
            }
        });

        // Sort alerts by priority
        allAlerts.sort((a, b) => {
            const priorityOrder = { 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1 };
            return priorityOrder[b.priority] - priorityOrder[a.priority];
        });

        const alertsHTML = allAlerts.length > 0 ?
            allAlerts.map(alert => createAlertItem(alert)).join('') :
            '<div class="no-alerts">No active alerts for current thresholds</div>';

        panel.innerHTML = `
            <div class="alerts-header">
                <h4>🚨 Active Alerts</h4>
                <span class="alerts-count">${allAlerts.length} Alert${allAlerts.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="alerts-container">
                ${alertsHTML}
            </div>
        `;

        return panel;
    }

    /**
     * Create individual alert item
     */
    function createAlertItem(alert) {
        const priorityClass = alert.priority.toLowerCase();
        const typeIcon = alert.type === 'significance' ? '📊' : '⚠️';

        let detailsHTML = '';

        if (alert.type === 'significance') {
            detailsHTML = `
                <div class="alert-details">
                    <div class="alert-bias">Bias: <strong>${alert.bias}</strong></div>
                    <div class="alert-ci">CI: [${alert.confidence_interval.lower.toFixed(1)}%, ${alert.confidence_interval.upper.toFixed(1)}%]</div>
                    <div class="alert-stops">
                        Stop: ${alert.stops.stop_distance.toFixed(2)} |
                        Target: ${alert.stops.target_distance.toFixed(2)}
                        (${alert.stops.risk_reward_ratio}:1 R:R)
                    </div>
                </div>
            `;
        } else if (alert.type === 'stability') {
            detailsHTML = `
                <div class="alert-details">
                    <div class="alert-variance">Variance: ${alert.variance.toFixed(1)}</div>
                    <div class="alert-trend">Trend: ${alert.trend.toFixed(2)}</div>
                </div>
            `;
        }

        return `
            <div class="alert-item ${priorityClass}">
                <div class="alert-header">
                    <span class="alert-icon">${typeIcon}</span>
                    <span class="alert-level">${alert.level_name}</span>
                    <span class="alert-priority ${priorityClass}">${alert.priority}</span>
                </div>
                <div class="alert-message">${alert.message}</div>
                ${detailsHTML}
            </div>
        `;
    }

    /**
     * Add styles for all advanced components
     */
    function addAdvancedStyles() {
        if (document.getElementById('advanced-ui-styles')) return;

        const style = document.createElement('style');
        style.id = 'advanced-ui-styles';
        style.textContent = `
            /* Top Picks Card */
            .top-picks-card {
                background: rgba(76, 175, 80, 0.1);
                border-left: 4px solid #4CAF50;
                border-radius: 0 8px 8px 0;
                padding: 15px;
                margin: 15px 0;
            }

            .top-picks-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
            }

            .top-picks-header h4 {
                margin: 0;
                color: #4CAF50;
                font-size: 16px;
            }

            .picks-count {
                background: #4CAF50;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: bold;
            }

            .no-picks-badge {
                background: #ff9800;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
            }

            .picks-container {
                display: flex;
                flex-direction: column;
                gap: 12px;
            }

            .pick-item {
                background: rgba(255, 255, 255, 0.05);
                border-radius: 6px;
                padding: 12px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }

            .primary-pick {
                border-left: 3px solid #FFD700;
            }

            .secondary-pick {
                border-left: 3px solid #C0C0C0;
            }

            .pick-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 8px;
            }

            .pick-level {
                font-size: 18px;
                font-weight: bold;
                color: #00d4ff;
            }

            .pick-score {
                font-size: 12px;
                color: #cccccc;
            }

            .stability-badge {
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: bold;
            }

            .stability-badge.stable {
                background: #4CAF50;
                color: white;
            }

            .stability-badge.moderate {
                background: #ff9800;
                color: white;
            }

            .stability-badge.drifty {
                background: #f44336;
                color: white;
            }

            .stability-badge.unknown {
                background: #666;
                color: white;
            }

            .pick-metrics {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
                margin-bottom: 8px;
            }

            .metric {
                display: flex;
                flex-direction: column;
                align-items: center;
            }

            .metric-label {
                font-size: 10px;
                color: #aaa;
                margin-bottom: 2px;
            }

            .metric-value {
                font-size: 14px;
                font-weight: bold;
                color: #ffffff;
            }

            .metric-value.positive {
                color: #4CAF50;
            }

            .metric-value.negative {
                color: #f44336;
            }

            .pick-ci {
                font-size: 11px;
                color: #cccccc;
                text-align: center;
                margin-bottom: 8px;
            }

            /* Sparklines */
            .sparkline-container {
                background: rgba(0, 0, 0, 0.2);
                border-radius: 4px;
                padding: 8px;
            }

            .sparkline-group {
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 4px;
            }

            .sparkline-label {
                font-size: 10px;
                color: #aaa;
                min-width: 60px;
            }

            .sparkline {
                background: rgba(255, 255, 255, 0.02);
                border-radius: 2px;
            }

            .sparkline-value {
                font-size: 10px;
                color: #ffffff;
                min-width: 40px;
                text-align: right;
            }

            /* Volatility Matrix */
            .volatility-matrix {
                background: rgba(33, 150, 243, 0.1);
                border-left: 4px solid #2196F3;
                border-radius: 0 8px 8px 0;
                padding: 15px;
                margin: 15px 0;
            }

            .matrix-header h4 {
                margin: 0 0 5px 0;
                color: #2196F3;
                font-size: 16px;
            }

            .matrix-subtitle {
                font-size: 12px;
                color: #aaa;
                margin-bottom: 15px;
            }

            .matrix-grid {
                display: flex;
                flex-direction: column;
                gap: 8px;
            }

            .matrix-row {
                display: flex;
                align-items: center;
                gap: 10px;
            }

            .row-label {
                min-width: 80px;
                font-size: 12px;
                color: #cccccc;
                font-weight: bold;
            }

            .row-cells {
                display: grid;
                grid-template-columns: repeat(7, 1fr);
                gap: 4px;
                flex: 1;
            }

            .matrix-cell {
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 6px 4px;
                text-align: center;
                min-height: 50px;
                display: flex;
                flex-direction: column;
                justify-content: center;
            }

            .matrix-cell.insufficient {
                background: rgba(128, 128, 128, 0.1);
                color: #666;
                border-color: rgba(128, 128, 128, 0.2);
            }

            .cell-level {
                font-size: 10px;
                font-weight: bold;
                color: #00d4ff;
                margin-bottom: 2px;
            }

            .cell-rate {
                font-size: 12px;
                color: #ffffff;
                margin-bottom: 2px;
            }

            .cell-q {
                font-size: 9px;
                color: #cccccc;
            }

            .matrix-legend {
                display: flex;
                gap: 15px;
                margin-top: 10px;
                font-size: 11px;
                color: #aaa;
            }

            .legend-item {
                display: flex;
                align-items: center;
                gap: 5px;
            }

            .cell-sample {
                width: 12px;
                height: 12px;
                border-radius: 2px;
            }

            .cell-sample.sufficient {
                background: rgba(255, 255, 255, 0.1);
                border: 1px solid rgba(255, 255, 255, 0.2);
            }

            .cell-sample.insufficient {
                background: rgba(128, 128, 128, 0.1);
                border: 1px solid rgba(128, 128, 128, 0.2);
            }

            /* Walk-Forward Summary */
            .walk-forward-summary {
                background: rgba(255, 193, 7, 0.1);
                border-left: 4px solid #FFC107;
                border-radius: 0 8px 8px 0;
                padding: 15px;
                margin: 15px 0;
            }

            .summary-header h4 {
                margin: 0 0 5px 0;
                color: #FFC107;
                font-size: 16px;
            }

            .summary-grid {
                display: flex;
                flex-direction: column;
                gap: 8px;
            }

            .wf-row {
                display: grid;
                grid-template-columns: 60px 120px 1fr;
                gap: 15px;
                align-items: center;
                padding: 8px;
                background: rgba(255, 255, 255, 0.03);
                border-radius: 4px;
            }

            .wf-level {
                font-size: 14px;
                font-weight: bold;
                color: #00d4ff;
            }

            .wf-sparkline svg {
                background: rgba(0, 0, 0, 0.2);
                border-radius: 2px;
            }

            .wf-stability {
                display: flex;
                align-items: center;
                gap: 10px;
            }

            .wf-stats {
                font-size: 10px;
                color: #aaa;
            }

            .summary-note {
                font-size: 11px;
                color: #aaa;
                margin-top: 10px;
                font-style: italic;
            }

            /* Alerts Panel */
            .alerts-panel {
                background: rgba(244, 67, 54, 0.1);
                border-left: 4px solid #f44336;
                border-radius: 0 8px 8px 0;
                padding: 15px;
                margin: 15px 0;
            }

            .alerts-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
            }

            .alerts-header h4 {
                margin: 0;
                color: #f44336;
                font-size: 16px;
            }

            .alerts-count {
                background: #f44336;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: bold;
            }

            .alerts-container {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }

            .alert-item {
                background: rgba(255, 255, 255, 0.05);
                border-radius: 6px;
                padding: 12px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }

            .alert-item.high {
                border-left: 3px solid #f44336;
            }

            .alert-item.medium {
                border-left: 3px solid #ff9800;
            }

            .alert-item.low {
                border-left: 3px solid #4CAF50;
            }

            .alert-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 8px;
            }

            .alert-level {
                font-size: 14px;
                font-weight: bold;
                color: #00d4ff;
            }

            .alert-priority {
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: bold;
            }

            .alert-priority.high {
                background: #f44336;
                color: white;
            }

            .alert-priority.medium {
                background: #ff9800;
                color: white;
            }

            .alert-priority.low {
                background: #4CAF50;
                color: white;
            }

            .alert-message {
                color: #ffffff;
                margin-bottom: 8px;
                font-size: 14px;
            }

            .alert-details {
                font-size: 12px;
                color: #cccccc;
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 8px;
            }

            .no-alerts {
                text-align: center;
                color: #666;
                padding: 20px;
                font-style: italic;
            }
        `;

        document.head.appendChild(style);
    }

    // Public API
    return {
        createTopPicksCard,
        createVolatilityMatrix,
        createWalkForwardSummary,
        createAlertsPanel,
        createStabilityBadge,
        createSparklineDisplay,
        addAdvancedStyles,

        // Utility components
        createAlertItem,
        createWalkForwardRow,
        createVolatilityRow,
        generateMockTimeSeries,
        generateMockVolatilityData
    };
})();