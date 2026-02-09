/* ---------- performance_tracker.js ----------
   Advanced performance analytics and tracking system
   for professional pivot level analysis
   ------------------------------------------------*/

class PerformanceTracker {
    constructor() {
        this.tradingResults = new Map();
        this.levelPerformance = new Map();
        this.sessionData = new Map();
        this.benchmarks = {
            SPX: { yearlyReturn: 0.10, volatility: 0.16 },
            NDX: { yearlyReturn: 0.12, volatility: 0.20 },
            DJI: { yearlyReturn: 0.08, volatility: 0.15 }
        };

        this.metrics = {
            accuracy: 0,
            profitFactor: 0,
            winRate: 0,
            sharpeRatio: 0,
            maxDrawdown: 0,
            avgReturn: 0,
            totalTrades: 0,
            avgHoldTime: 0
        };

        this.init();
    }

    init() {
        this.createPerformanceInterface();
        this.loadHistoricalData();
        this.startPerformanceTracking();
    }

    // ========= PERFORMANCE INTERFACE =========
    createPerformanceInterface() {
        const modal = document.createElement('div');
        modal.id = 'performance-modal';
        modal.className = 'performance-modal';
        modal.style.display = 'none';

        modal.innerHTML = `
            <style>
                .performance-modal {
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(12, 17, 27, 0.9);
                    backdrop-filter: blur(4px);
                    z-index: 10000;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }

                .performance-content {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 16px;
                    width: 95%;
                    max-width: 1200px;
                    max-height: 90vh;
                    overflow-y: auto;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
                }

                .performance-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 24px 24px 16px;
                    border-bottom: 1px solid var(--border-soft);
                }

                .performance-title {
                    color: var(--accent-blue);
                    font-size: 1.4rem;
                    font-weight: 600;
                    margin: 0;
                }

                .performance-close {
                    background: none;
                    border: none;
                    color: var(--text-secondary);
                    font-size: 24px;
                    cursor: pointer;
                    padding: 4px;
                    border-radius: 50%;
                    width: 32px;
                    height: 32px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }

                .performance-tabs {
                    display: flex;
                    border-bottom: 1px solid var(--border-soft);
                }

                .performance-tab {
                    flex: 1;
                    background: none;
                    border: none;
                    color: var(--text-secondary);
                    padding: 16px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                    border-bottom: 2px solid transparent;
                }

                .performance-tab.active {
                    color: var(--accent-blue);
                    border-bottom-color: var(--accent-blue);
                }

                .performance-body {
                    padding: 24px;
                }

                .performance-section {
                    display: none;
                }

                .performance-section.active {
                    display: block;
                }

                .metrics-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 16px;
                    margin-bottom: 24px;
                }

                .metric-card {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 12px;
                    padding: 20px;
                    text-align: center;
                    position: relative;
                    overflow: hidden;
                }

                .metric-card::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 3px;
                    background: linear-gradient(90deg, var(--accent-blue), var(--accent-green));
                }

                .metric-value {
                    font-size: 2rem;
                    font-weight: 700;
                    color: var(--accent-blue);
                    font-family: var(--font-mono);
                    margin-bottom: 8px;
                }

                .metric-label {
                    color: var(--text-primary);
                    font-weight: 500;
                    margin-bottom: 4px;
                }

                .metric-change {
                    font-size: 0.9rem;
                    font-family: var(--font-mono);
                }

                .metric-change.positive {
                    color: var(--accent-green);
                }

                .metric-change.negative {
                    color: var(--accent-red);
                }

                .metric-change.neutral {
                    color: var(--text-secondary);
                }

                .performance-chart {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 12px;
                    padding: 20px;
                    height: 300px;
                    margin-bottom: 24px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: var(--text-secondary);
                    font-size: 14px;
                }

                .level-performance-table {
                    width: 100%;
                    border-collapse: collapse;
                    background: var(--bg-panel);
                    border-radius: 12px;
                    overflow: hidden;
                }

                .level-performance-table th,
                .level-performance-table td {
                    padding: 12px 16px;
                    text-align: left;
                    border-bottom: 1px solid var(--border-soft);
                }

                .level-performance-table th {
                    background: var(--bg-accent);
                    color: var(--accent-blue);
                    font-weight: 600;
                }

                .level-performance-table tr:hover td {
                    background: rgba(66, 165, 245, 0.05);
                }

                .trade-log {
                    max-height: 400px;
                    overflow-y: auto;
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 12px;
                    padding: 16px;
                }

                .trade-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 12px 0;
                    border-bottom: 1px solid var(--border-soft);
                }

                .trade-item:last-child {
                    border-bottom: none;
                }

                .trade-details {
                    flex: 1;
                }

                .trade-title {
                    color: var(--text-primary);
                    font-weight: 500;
                    margin-bottom: 4px;
                }

                .trade-meta {
                    color: var(--text-secondary);
                    font-size: 13px;
                }

                .trade-result {
                    font-family: var(--font-mono);
                    font-weight: 600;
                    padding: 4px 8px;
                    border-radius: 4px;
                }

                .trade-result.profit {
                    color: var(--accent-green);
                    background: rgba(102, 187, 106, 0.1);
                }

                .trade-result.loss {
                    color: var(--accent-red);
                    background: rgba(239, 83, 80, 0.1);
                }

                .export-controls {
                    display: flex;
                    gap: 12px;
                    margin-bottom: 24px;
                    flex-wrap: wrap;
                }

                .performance-btn {
                    background: var(--accent-blue);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 10px 16px;
                    cursor: pointer;
                    font-size: 14px;
                    transition: background 0.2s ease;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }

                .performance-btn:hover {
                    background: #1976d2;
                }

                .performance-btn.secondary {
                    background: var(--bg-panel);
                    color: var(--text-primary);
                    border: 1px solid var(--border-soft);
                }

                .benchmark-comparison {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 16px;
                    margin-bottom: 24px;
                }

                .benchmark-card {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 12px;
                    padding: 16px;
                }

                .benchmark-title {
                    color: var(--accent-blue);
                    font-weight: 600;
                    margin-bottom: 12px;
                    text-align: center;
                }

                .benchmark-metrics {
                    display: grid;
                    gap: 8px;
                }

                .benchmark-metric {
                    display: flex;
                    justify-content: space-between;
                    padding: 4px 0;
                }

                .benchmark-label {
                    color: var(--text-secondary);
                }

                .benchmark-value {
                    color: var(--text-primary);
                    font-family: var(--font-mono);
                    font-weight: 500;
                }

                .settings-grid {
                    display: grid;
                    gap: 16px;
                }

                .setting-row {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 12px 0;
                    border-bottom: 1px solid var(--border-soft);
                }

                .setting-label {
                    color: var(--text-primary);
                    font-weight: 500;
                }

                .setting-description {
                    color: var(--text-secondary);
                    font-size: 13px;
                    margin-top: 2px;
                }

                .setting-input {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 6px;
                    padding: 6px 10px;
                    color: var(--text-primary);
                    width: 120px;
                }
            </style>

            <div class="performance-content">
                <div class="performance-header">
                    <h2 class="performance-title">📊 Performance Analytics</h2>
                    <button class="performance-close" onclick="document.getElementById('performance-modal').style.display='none'">×</button>
                </div>

                <div class="performance-tabs">
                    <button class="performance-tab active" data-tab="overview">Overview</button>
                    <button class="performance-tab" data-tab="levels">Level Analysis</button>
                    <button class="performance-tab" data-tab="trades">Trade Log</button>
                    <button class="performance-tab" data-tab="benchmark">Benchmark</button>
                    <button class="performance-tab" data-tab="settings">Settings</button>
                </div>

                <div class="performance-body">
                    <!-- Overview Tab -->
                    <div class="performance-section active" id="perf-overview">
                        <div class="export-controls">
                            <button class="performance-btn" onclick="window.performanceTracker.exportReport()">
                                📄 Export Report
                            </button>
                            <button class="performance-btn secondary" onclick="window.performanceTracker.resetData()">
                                🗑️ Reset Data
                            </button>
                            <button class="performance-btn secondary" onclick="window.performanceTracker.refreshMetrics()">
                                🔄 Refresh
                            </button>
                        </div>

                        <div class="metrics-grid" id="metrics-grid">
                            <!-- Dynamic content -->
                        </div>

                        <div class="performance-chart" id="performance-chart">
                            📈 Performance Chart (Chart.js integration coming soon)
                        </div>
                    </div>

                    <!-- Level Analysis Tab -->
                    <div class="performance-section" id="perf-levels">
                        <table class="level-performance-table" id="level-performance-table">
                            <thead>
                                <tr>
                                    <th>Level</th>
                                    <th>Touches</th>
                                    <th>Success Rate</th>
                                    <th>Avg Move</th>
                                    <th>Best Move</th>
                                    <th>Worst Move</th>
                                    <th>Reliability</th>
                                </tr>
                            </thead>
                            <tbody id="level-performance-body">
                                <!-- Dynamic content -->
                            </tbody>
                        </table>
                    </div>

                    <!-- Trade Log Tab -->
                    <div class="performance-section" id="perf-trades">
                        <div class="export-controls">
                            <button class="performance-btn secondary" onclick="window.performanceTracker.addTrade()">
                                ➕ Add Trade
                            </button>
                            <button class="performance-btn secondary" onclick="window.performanceTracker.clearTrades()">
                                🗑️ Clear All
                            </button>
                        </div>

                        <div class="trade-log" id="trade-log">
                            <!-- Dynamic content -->
                        </div>
                    </div>

                    <!-- Benchmark Tab -->
                    <div class="performance-section" id="perf-benchmark">
                        <div class="benchmark-comparison" id="benchmark-comparison">
                            <!-- Dynamic content -->
                        </div>
                    </div>

                    <!-- Settings Tab -->
                    <div class="performance-section" id="perf-settings">
                        <div class="settings-grid">
                            <div class="setting-row">
                                <div>
                                    <div class="setting-label">Risk-Free Rate (%)</div>
                                    <div class="setting-description">Used for Sharpe ratio calculation</div>
                                </div>
                                <input type="number" class="setting-input" id="risk-free-rate" value="2.5" step="0.1">
                            </div>

                            <div class="setting-row">
                                <div>
                                    <div class="setting-label">Trade Commission ($)</div>
                                    <div class="setting-description">Cost per trade for profit calculations</div>
                                </div>
                                <input type="number" class="setting-input" id="trade-commission" value="0" step="0.01">
                            </div>

                            <div class="setting-row">
                                <div>
                                    <div class="setting-label">Initial Capital ($)</div>
                                    <div class="setting-description">Starting capital for performance calculations</div>
                                </div>
                                <input type="number" class="setting-input" id="initial-capital" value="10000" step="100">
                            </div>

                            <div class="setting-row">
                                <div>
                                    <div class="setting-label">Position Size (%)</div>
                                    <div class="setting-description">Percentage of capital per trade</div>
                                </div>
                                <input type="number" class="setting-input" id="position-size" value="5" step="1" min="1" max="100">
                            </div>

                            <button class="performance-btn" onclick="window.performanceTracker.saveSettings()">
                                💾 Save Settings
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(modal);
        this.setupModalEvents();
    }

    setupModalEvents() {
        // Tab switching
        document.querySelectorAll('.performance-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const targetTab = tab.dataset.tab;

                // Update tab appearance
                document.querySelectorAll('.performance-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');

                // Show corresponding section
                document.querySelectorAll('.performance-section').forEach(section => {
                    section.classList.remove('active');
                });
                document.getElementById(`perf-${targetTab}`).classList.add('active');

                // Load section-specific content
                this.loadTabContent(targetTab);
            });
        });
    }

    loadTabContent(tab) {
        switch (tab) {
            case 'overview':
                this.updateMetricsGrid();
                break;
            case 'levels':
                this.updateLevelAnalysis();
                break;
            case 'trades':
                this.updateTradeLog();
                break;
            case 'benchmark':
                this.updateBenchmarkComparison();
                break;
        }
    }

    // ========= METRICS CALCULATION =========
    calculateMetrics() {
        const trades = Array.from(this.tradingResults.values());

        if (trades.length === 0) {
            return {
                totalTrades: 0,
                winRate: 0,
                profitFactor: 0,
                avgReturn: 0,
                sharpeRatio: 0,
                maxDrawdown: 0,
                totalPnL: 0,
                avgWin: 0,
                avgLoss: 0
            };
        }

        const wins = trades.filter(t => t.pnl > 0);
        const losses = trades.filter(t => t.pnl <= 0);

        const totalPnL = trades.reduce((sum, t) => sum + t.pnl, 0);
        const winRate = (wins.length / trades.length) * 100;

        const totalWins = wins.reduce((sum, t) => sum + t.pnl, 0);
        const totalLosses = Math.abs(losses.reduce((sum, t) => sum + t.pnl, 0));
        const profitFactor = totalLosses > 0 ? totalWins / totalLosses : totalWins > 0 ? 999 : 0;

        const avgReturn = totalPnL / trades.length;
        const avgWin = wins.length > 0 ? totalWins / wins.length : 0;
        const avgLoss = losses.length > 0 ? totalLosses / losses.length : 0;

        // Calculate Sharpe Ratio
        const returns = trades.map(t => t.pnl);
        const avgReturns = returns.reduce((sum, r) => sum + r, 0) / returns.length;
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - avgReturns, 2), 0) / returns.length;
        const stdDev = Math.sqrt(variance);
        const riskFreeRate = parseFloat(document.getElementById('risk-free-rate')?.value || '2.5') / 100;
        const sharpeRatio = stdDev > 0 ? (avgReturns - riskFreeRate) / stdDev : 0;

        // Calculate Max Drawdown
        let runningPnL = 0;
        let peak = 0;
        let maxDrawdown = 0;

        trades.forEach(trade => {
            runningPnL += trade.pnl;
            if (runningPnL > peak) peak = runningPnL;
            const drawdown = ((peak - runningPnL) / Math.max(peak, 1)) * 100;
            if (drawdown > maxDrawdown) maxDrawdown = drawdown;
        });

        return {
            totalTrades: trades.length,
            winRate,
            profitFactor,
            avgReturn,
            sharpeRatio,
            maxDrawdown,
            totalPnL,
            avgWin,
            avgLoss
        };
    }

    updateMetricsGrid() {
        const metrics = this.calculateMetrics();
        const grid = document.getElementById('metrics-grid');

        if (!grid) return;

        const metricCards = [
            {
                label: 'Total Trades',
                value: metrics.totalTrades.toString(),
                change: null,
                format: 'integer'
            },
            {
                label: 'Win Rate',
                value: metrics.winRate.toFixed(1) + '%',
                change: metrics.winRate >= 50 ? 'positive' : 'negative',
                format: 'percentage'
            },
            {
                label: 'Profit Factor',
                value: metrics.profitFactor.toFixed(2),
                change: metrics.profitFactor >= 1.5 ? 'positive' : metrics.profitFactor >= 1 ? 'neutral' : 'negative',
                format: 'ratio'
            },
            {
                label: 'Avg Return',
                value: '$' + metrics.avgReturn.toFixed(2),
                change: metrics.avgReturn > 0 ? 'positive' : metrics.avgReturn < 0 ? 'negative' : 'neutral',
                format: 'currency'
            },
            {
                label: 'Sharpe Ratio',
                value: metrics.sharpeRatio.toFixed(2),
                change: metrics.sharpeRatio >= 1 ? 'positive' : metrics.sharpeRatio >= 0.5 ? 'neutral' : 'negative',
                format: 'ratio'
            },
            {
                label: 'Max Drawdown',
                value: metrics.maxDrawdown.toFixed(1) + '%',
                change: metrics.maxDrawdown <= 10 ? 'positive' : metrics.maxDrawdown <= 20 ? 'neutral' : 'negative',
                format: 'percentage'
            },
            {
                label: 'Total P&L',
                value: '$' + metrics.totalPnL.toFixed(2),
                change: metrics.totalPnL > 0 ? 'positive' : metrics.totalPnL < 0 ? 'negative' : 'neutral',
                format: 'currency'
            },
            {
                label: 'Avg Win',
                value: '$' + metrics.avgWin.toFixed(2),
                change: null,
                format: 'currency'
            }
        ];

        grid.innerHTML = metricCards.map(card => `
            <div class="metric-card">
                <div class="metric-value">${card.value}</div>
                <div class="metric-label">${card.label}</div>
                ${card.change ? `<div class="metric-change ${card.change}">
                    ${card.change === 'positive' ? '↗' : card.change === 'negative' ? '↘' : '→'}
                </div>` : ''}
            </div>
        `).join('');
    }

    updateLevelAnalysis() {
        const tbody = document.getElementById('level-performance-body');
        if (!tbody) return;

        const levelStats = Array.from(this.levelPerformance.values());

        tbody.innerHTML = levelStats.length ? levelStats.map(stat => `
            <tr>
                <td style="font-weight: 600; color: var(--accent-blue);">${stat.level}</td>
                <td>${stat.touches}</td>
                <td style="color: ${stat.successRate >= 70 ? 'var(--accent-green)' : stat.successRate >= 50 ? 'var(--accent-gold)' : 'var(--accent-red)'};">
                    ${stat.successRate.toFixed(1)}%
                </td>
                <td>$${stat.avgMove.toFixed(2)}</td>
                <td style="color: var(--accent-green);">$${stat.bestMove.toFixed(2)}</td>
                <td style="color: var(--accent-red);">$${stat.worstMove.toFixed(2)}</td>
                <td>
                    <span style="
                        padding: 4px 8px;
                        border-radius: 4px;
                        font-size: 12px;
                        font-weight: 600;
                        background: ${stat.reliability === 'High' ? 'rgba(102, 187, 106, 0.2)' :
                                    stat.reliability === 'Medium' ? 'rgba(253, 216, 53, 0.2)' :
                                    'rgba(239, 83, 80, 0.2)'};
                        color: ${stat.reliability === 'High' ? 'var(--accent-green)' :
                               stat.reliability === 'Medium' ? 'var(--accent-gold)' :
                               'var(--accent-red)'};
                    ">${stat.reliability}</span>
                </td>
            </tr>
        `).join('') : '<tr><td colspan="7" style="text-align: center; color: var(--text-secondary); padding: 40px;">No level performance data available.</td></tr>';
    }

    updateTradeLog() {
        const logContainer = document.getElementById('trade-log');
        if (!logContainer) return;

        const trades = Array.from(this.tradingResults.values())
            .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
            .slice(0, 50); // Show last 50 trades

        logContainer.innerHTML = trades.length ? trades.map(trade => `
            <div class="trade-item">
                <div class="trade-details">
                    <div class="trade-title">${trade.asset} ${trade.type} @ ${trade.entryPrice}</div>
                    <div class="trade-meta">
                        ${new Date(trade.timestamp).toLocaleString()} •
                        Exit: ${trade.exitPrice} •
                        Duration: ${trade.holdTime}
                    </div>
                </div>
                <div class="trade-result ${trade.pnl >= 0 ? 'profit' : 'loss'}">
                    ${trade.pnl >= 0 ? '+' : ''}$${trade.pnl.toFixed(2)}
                </div>
            </div>
        `).join('') : '<div style="text-align: center; color: var(--text-secondary); padding: 40px;">No trades logged yet.</div>';
    }

    updateBenchmarkComparison() {
        const container = document.getElementById('benchmark-comparison');
        if (!container) return;

        const ourMetrics = this.calculateMetrics();
        const annualizedReturn = this.calculateAnnualizedReturn();

        const benchmarks = [
            {
                name: 'Your Strategy',
                return: (annualizedReturn * 100).toFixed(1) + '%',
                sharpe: ourMetrics.sharpeRatio.toFixed(2),
                maxDD: ourMetrics.maxDrawdown.toFixed(1) + '%',
                winRate: ourMetrics.winRate.toFixed(1) + '%'
            },
            {
                name: 'S&P 500 (SPX)',
                return: '10.0%',
                sharpe: '0.65',
                maxDD: '19.8%',
                winRate: '64.2%'
            },
            {
                name: 'NASDAQ 100',
                return: '12.5%',
                sharpe: '0.58',
                maxDD: '28.2%',
                winRate: '61.8%'
            }
        ];

        container.innerHTML = benchmarks.map((benchmark, _index) => `
            <div class="benchmark-card">
                <div class="benchmark-title">${benchmark.name}</div>
                <div class="benchmark-metrics">
                    <div class="benchmark-metric">
                        <span class="benchmark-label">Annual Return</span>
                        <span class="benchmark-value">${benchmark.return}</span>
                    </div>
                    <div class="benchmark-metric">
                        <span class="benchmark-label">Sharpe Ratio</span>
                        <span class="benchmark-value">${benchmark.sharpe}</span>
                    </div>
                    <div class="benchmark-metric">
                        <span class="benchmark-label">Max Drawdown</span>
                        <span class="benchmark-value">${benchmark.maxDD}</span>
                    </div>
                    <div class="benchmark-metric">
                        <span class="benchmark-label">Win Rate</span>
                        <span class="benchmark-value">${benchmark.winRate}</span>
                    </div>
                </div>
            </div>
        `).join('');
    }

    // ========= TRADE MANAGEMENT =========
    addTrade(tradeData = null) {
        if (!tradeData) {
            // Show simple input dialog
            const entryPrice = prompt('Entry Price:');
            const exitPrice = prompt('Exit Price:');
            const quantity = prompt('Quantity:', '1');

            if (!entryPrice || !exitPrice || !quantity) return;

            tradeData = {
                asset: document.getElementById('assetSelector')?.value || 'SPX',
                type: 'Manual Entry',
                entryPrice: parseFloat(entryPrice),
                exitPrice: parseFloat(exitPrice),
                quantity: parseFloat(quantity),
                timestamp: new Date().toISOString()
            };
        }

        const trade = {
            id: `trade_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            ...tradeData,
            pnl: (tradeData.exitPrice - tradeData.entryPrice) * tradeData.quantity,
            holdTime: this.calculateHoldTime(tradeData.entryTime, tradeData.exitTime)
        };

        this.tradingResults.set(trade.id, trade);
        this.saveData();

        if (window.professionalUI) {
            window.professionalUI.showNotification('success', 'Trade Added',
                `${trade.type} trade logged: ${trade.pnl >= 0 ? '+' : ''}$${trade.pnl.toFixed(2)}`);
        }

        this.refreshMetrics();
    }

    calculateHoldTime(entryTime, exitTime) {
        if (!entryTime || !exitTime) return 'N/A';

        const entry = new Date(entryTime);
        const exit = new Date(exitTime);
        const diff = exit - entry;

        const hours = Math.floor(diff / (1000 * 60 * 60));
        const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));

        if (hours > 24) {
            const days = Math.floor(hours / 24);
            return `${days}d ${hours % 24}h`;
        } else if (hours > 0) {
            return `${hours}h ${minutes}m`;
        } else {
            return `${minutes}m`;
        }
    }

    calculateAnnualizedReturn() {
        const trades = Array.from(this.tradingResults.values());
        if (trades.length === 0) return 0;

        const totalPnL = trades.reduce((sum, t) => sum + t.pnl, 0);
        const initialCapital = parseFloat(document.getElementById('initial-capital')?.value || '10000');

        const firstTrade = trades.reduce((earliest, trade) =>
            new Date(trade.timestamp) < new Date(earliest.timestamp) ? trade : earliest
        );

        const daysSinceFirst = Math.max(1, (Date.now() - new Date(firstTrade.timestamp).getTime()) / (1000 * 60 * 60 * 24));
        const annualFactor = 365 / daysSinceFirst;

        return (totalPnL / initialCapital) * annualFactor;
    }

    // ========= DATA PERSISTENCE =========
    saveData() {
        const data = {
            tradingResults: Array.from(this.tradingResults.entries()),
            levelPerformance: Array.from(this.levelPerformance.entries()),
            sessionData: Array.from(this.sessionData.entries()),
            timestamp: new Date().toISOString()
        };

        localStorage.setItem('performanceData', JSON.stringify(data));
    }

    loadHistoricalData() {
        const saved = localStorage.getItem('performanceData');
        if (saved) {
            try {
                const data = JSON.parse(saved);
                this.tradingResults = new Map(data.tradingResults || []);
                this.levelPerformance = new Map(data.levelPerformance || []);
                this.sessionData = new Map(data.sessionData || []);
            } catch (error) {
                /* eslint-disable-next-line no-console */
                console.warn('Could not load performance data:', error);
            }
        }
    }

    saveSettings() {
        const settings = {
            riskFreeRate: document.getElementById('risk-free-rate')?.value,
            tradeCommission: document.getElementById('trade-commission')?.value,
            initialCapital: document.getElementById('initial-capital')?.value,
            positionSize: document.getElementById('position-size')?.value
        };

        localStorage.setItem('performanceSettings', JSON.stringify(settings));

        if (window.professionalUI) {
            window.professionalUI.showNotification('success', 'Settings Saved',
                'Performance tracking settings have been saved');
        }
    }

    // ========= PERFORMANCE TRACKING =========
    startPerformanceTracking() {
        // Listen for pivot level interactions
        document.addEventListener('pivotLevelTouch', (event) => {
            this.recordLevelInteraction(event.detail);
        });

        // Monitor for significant price movements
        this.trackingInterval = setInterval(() => {
            this.monitorPerformance();
        }, 60000); // Check every minute
    }

    recordLevelInteraction(levelData) {
        const levelKey = `${levelData.asset}_${levelData.level}_${levelData.timeframe}`;

        if (!this.levelPerformance.has(levelKey)) {
            this.levelPerformance.set(levelKey, {
                level: levelData.levelName,
                touches: 0,
                successes: 0,
                moves: [],
                bestMove: 0,
                worstMove: 0,
                avgMove: 0,
                successRate: 0,
                reliability: 'Low'
            });
        }

        const levelStat = this.levelPerformance.get(levelKey);
        levelStat.touches++;

        // Track the move after level touch
        if (levelData.moveAfterTouch) {
            levelStat.moves.push(levelData.moveAfterTouch);
            if (levelData.moveAfterTouch > 0) levelStat.successes++;

            levelStat.bestMove = Math.max(levelStat.bestMove, levelData.moveAfterTouch);
            levelStat.worstMove = Math.min(levelStat.worstMove, levelData.moveAfterTouch);
            levelStat.avgMove = levelStat.moves.reduce((sum, move) => sum + move, 0) / levelStat.moves.length;
            levelStat.successRate = (levelStat.successes / levelStat.touches) * 100;

            // Update reliability rating
            if (levelStat.successRate >= 70 && levelStat.touches >= 10) {
                levelStat.reliability = 'High';
            } else if (levelStat.successRate >= 50 && levelStat.touches >= 5) {
                levelStat.reliability = 'Medium';
            } else {
                levelStat.reliability = 'Low';
            }
        }

        this.saveData();
    }

    monitorPerformance() {
        // This would integrate with real-time price data to track ongoing performance
        // For now, we'll update metrics if the modal is open
        if (document.getElementById('performance-modal').style.display !== 'none') {
            this.refreshMetrics();
        }
    }

    // ========= EXPORT FUNCTIONALITY =========
    exportReport() {
        const metrics = this.calculateMetrics();
        const reportData = {
            timestamp: new Date().toISOString(),
            totalTrades: metrics.totalTrades,
            winRate: metrics.winRate,
            profitFactor: metrics.profitFactor,
            sharpeRatio: metrics.sharpeRatio,
            maxDrawdown: metrics.maxDrawdown,
            totalPnL: metrics.totalPnL,
            trades: Array.from(this.tradingResults.values()),
            levelPerformance: Array.from(this.levelPerformance.values())
        };

        const csvContent = this.generateCSVReport(reportData);
        this.downloadFile('performance_report.csv', csvContent);

        if (window.professionalUI) {
            window.professionalUI.showNotification('success', 'Report Exported',
                'Performance report downloaded as CSV file');
        }
    }

    generateCSVReport(data) {
        let csv = 'Performance Report\n';
        csv += `Generated: ${data.timestamp}\n\n`;

        csv += 'Summary Metrics\n';
        csv += 'Metric,Value\n';
        csv += `Total Trades,${data.totalTrades}\n`;
        csv += `Win Rate,${data.winRate.toFixed(1)}%\n`;
        csv += `Profit Factor,${data.profitFactor.toFixed(2)}\n`;
        csv += `Sharpe Ratio,${data.sharpeRatio.toFixed(2)}\n`;
        csv += `Max Drawdown,${data.maxDrawdown.toFixed(1)}%\n`;
        csv += `Total P&L,$${data.totalPnL.toFixed(2)}\n\n`;

        csv += 'Trade Log\n';
        csv += 'Timestamp,Asset,Type,Entry,Exit,Quantity,P&L,Hold Time\n';
        data.trades.forEach(trade => {
            csv += `${trade.timestamp},${trade.asset},${trade.type},${trade.entryPrice},${trade.exitPrice},${trade.quantity},${trade.pnl.toFixed(2)},${trade.holdTime}\n`;
        });

        csv += '\nLevel Performance\n';
        csv += 'Level,Touches,Success Rate,Avg Move,Best Move,Worst Move,Reliability\n';
        data.levelPerformance.forEach(level => {
            csv += `${level.level},${level.touches},${level.successRate.toFixed(1)}%,${level.avgMove.toFixed(2)},${level.bestMove.toFixed(2)},${level.worstMove.toFixed(2)},${level.reliability}\n`;
        });

        return csv;
    }

    downloadFile(filename, content) {
        const blob = new Blob([content], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
    }

    // ========= PUBLIC API =========
    showPerformanceTracker() {
        document.getElementById('performance-modal').style.display = 'flex';
        this.updateMetricsGrid();
    }

    refreshMetrics() {
        this.updateMetricsGrid();
        this.updateLevelAnalysis();
        this.updateTradeLog();
    }

    resetData() {
        if (confirm('Are you sure you want to reset all performance data? This cannot be undone.')) {
            this.tradingResults.clear();
            this.levelPerformance.clear();
            this.sessionData.clear();
            localStorage.removeItem('performanceData');

            this.refreshMetrics();

            if (window.professionalUI) {
                window.professionalUI.showNotification('info', 'Data Reset',
                    'All performance data has been cleared');
            }
        }
    }

    clearTrades() {
        if (confirm('Clear all trade logs?')) {
            this.tradingResults.clear();
            this.saveData();
            this.updateTradeLog();
            this.updateMetricsGrid();
        }
    }
}

// Initialize performance tracker
document.addEventListener('DOMContentLoaded', () => {
    window.performanceTracker = new PerformanceTracker();

    // Wire up the performance button
    const perfBtn = document.getElementById('performanceTracker');
    if (perfBtn) {
        perfBtn.addEventListener('click', () => {
            window.performanceTracker.showPerformanceTracker();
        });
    }
});