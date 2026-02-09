/* ---------- help_system.js ----------
   Comprehensive help system and documentation
   for professional pivot calculator
   ------------------------------------------*/

class ProfessionalHelpSystem {
    constructor() {
        this.helpContent = this.initializeHelpContent();
        this.searchIndex = new Map();
        this.userProgress = new Map();

        this.init();
    }

    init() {
        this.createHelpInterface();
        this.buildSearchIndex();
        this.loadUserProgress();
    }

    // ========= HELP CONTENT =========
    initializeHelpContent() {
        return {
            quickStart: {
                title: "🚀 Quick Start Guide",
                sections: [
                    {
                        title: "Getting Started",
                        content: `
                            <h3>Welcome to Professional Pivot Calculator</h3>
                            <p>This powerful tool helps you analyze pivot points, ATR zones, and market structure with professional-grade precision.</p>

                            <h4>First Steps:</h4>
                            <ol>
                                <li><strong>Select Asset:</strong> Choose from SPX, NDX, DJI, ES, NQ, or YM</li>
                                <li><strong>Fetch Data:</strong> Click "📊 Fetch Market Data" to get live prices</li>
                                <li><strong>Review Levels:</strong> Analyze the calculated pivot levels and zones</li>
                                <li><strong>Set Alerts:</strong> Use "🔔 Level Alerts" for price notifications</li>
                            </ol>
                        `,
                        video: null,
                        difficulty: "Beginner"
                    },
                    {
                        title: "Understanding Pivot Levels",
                        content: `
                            <h3>Pivot Point Calculations</h3>
                            <p>Our system calculates traditional pivot points using the classic formula:</p>

                            <div class="formula-box">
                                <strong>Pivot Point (PP) = (High + Low + Close) / 3</strong><br>
                                <strong>Resistance 1 (R1) = (2 × PP) - Low</strong><br>
                                <strong>Resistance 2 (R2) = PP + (High - Low)</strong><br>
                                <strong>Support 1 (S1) = (2 × PP) - High</strong><br>
                                <strong>Support 2 (S2) = PP - (High - Low)</strong>
                            </div>

                            <h4>ATR Zones</h4>
                            <p>Each pivot level includes an ATR-based zone (±25% of ATR) that represents the optimal entry/exit area around the level.</p>
                        `,
                        difficulty: "Intermediate"
                    }
                ]
            },

            features: {
                title: "🔧 Features Guide",
                sections: [
                    {
                        title: "Multi-Asset Support",
                        content: `
                            <h3>Supported Assets</h3>
                            <ul>
                                <li><strong>SPX (S&P 500):</strong> Broad market index</li>
                                <li><strong>NDX (NASDAQ 100):</strong> Technology-heavy index</li>
                                <li><strong>DJI (Dow Jones):</strong> Blue-chip industrial average</li>
                                <li><strong>ES (E-mini S&P):</strong> Futures contract</li>
                                <li><strong>NQ (E-mini NASDAQ):</strong> Tech futures</li>
                                <li><strong>YM (E-mini Dow):</strong> Dow futures</li>
                            </ul>

                            <p>Switch assets using the dropdown in the header. Data fetching automatically adapts to the selected asset.</p>
                        `,
                        difficulty: "Beginner"
                    },
                    {
                        title: "Alert System",
                        content: `
                            <h3>Professional Alerts</h3>
                            <p>Set up intelligent price alerts for pivot level interactions:</p>

                            <h4>Alert Types:</h4>
                            <ul>
                                <li><strong>Level Touch:</strong> Price approaches within tolerance</li>
                                <li><strong>Level Break:</strong> Price breaks through level decisively</li>
                                <li><strong>Confluence:</strong> EMA and pivot level alignment</li>
                                <li><strong>Volatility:</strong> ATR-based volatility changes</li>
                            </ul>

                            <h4>Notification Methods:</h4>
                            <ul>
                                <li>🔊 Sound alerts (customizable volume)</li>
                                <li>🔔 Browser notifications</li>
                                <li>💬 Discord webhooks</li>
                                <li>📧 Email alerts (setup required)</li>
                            </ul>
                        `,
                        difficulty: "Intermediate"
                    },
                    {
                        title: "Performance Tracking",
                        content: `
                            <h3>Advanced Analytics</h3>
                            <p>Track your trading performance with institutional-grade metrics:</p>

                            <h4>Key Metrics:</h4>
                            <ul>
                                <li><strong>Win Rate:</strong> Percentage of profitable trades</li>
                                <li><strong>Profit Factor:</strong> Gross profit ÷ gross loss</li>
                                <li><strong>Sharpe Ratio:</strong> Risk-adjusted returns</li>
                                <li><strong>Max Drawdown:</strong> Largest peak-to-trough decline</li>
                            </ul>

                            <h4>Level Analysis:</h4>
                            <p>Track individual pivot level performance including:</p>
                            <ul>
                                <li>Touch frequency and success rates</li>
                                <li>Average price movements after touches</li>
                                <li>Best and worst case scenarios</li>
                                <li>Reliability ratings (High/Medium/Low)</li>
                            </ul>
                        `,
                        difficulty: "Advanced"
                    }
                ]
            },

            trading: {
                title: "📈 Trading Strategies",
                sections: [
                    {
                        title: "Bounce Strategy",
                        content: `
                            <h3>Support/Resistance Bounce Trading</h3>
                            <p>Trade price bounces from key pivot levels with high probability setups.</p>

                            <h4>Setup Criteria:</h4>
                            <ol>
                                <li>Price approaches S1/S2 (support) or R1/R2 (resistance)</li>
                                <li>Price enters the ATR zone around the level</li>
                                <li>Look for confluence with EMAs (9 or 21)</li>
                                <li>Monitor for reversal signals (candlestick patterns, momentum)</li>
                            </ol>

                            <h4>Entry Rules:</h4>
                            <ul>
                                <li><strong>Support Bounce:</strong> Buy when price bounces off support zone</li>
                                <li><strong>Resistance Bounce:</strong> Sell when price rejects resistance zone</li>
                                <li><strong>Stop Loss:</strong> Below support or above resistance (beyond ATR zone)</li>
                                <li><strong>Take Profit:</strong> Next pivot level or 2:1 risk-reward</li>
                            </ul>

                            <h4>Risk Management:</h4>
                            <p>Use the ATR zones as natural stop-loss levels. A break beyond the zone typically indicates a failed bounce.</p>
                        `,
                        difficulty: "Intermediate"
                    },
                    {
                        title: "Breakout Strategy",
                        content: `
                            <h3>Pivot Level Breakout Trading</h3>
                            <p>Trade directional moves when price breaks through key levels with momentum.</p>

                            <h4>Breakout Confirmation:</h4>
                            <ol>
                                <li>Price closes beyond pivot level + ATR zone</li>
                                <li>Volume expansion (if available)</li>
                                <li>EMA alignment in breakout direction</li>
                                <li>No immediate reversal back into zone</li>
                            </ol>

                            <h4>Entry Strategy:</h4>
                            <ul>
                                <li><strong>Resistance Break:</strong> Buy above R1/R2 + ATR</li>
                                <li><strong>Support Break:</strong> Sell below S1/S2 - ATR</li>
                                <li><strong>Retest Entry:</strong> Enter on pullback to broken level</li>
                            </ul>

                            <h4>Targets:</h4>
                            <ul>
                                <li>Next major pivot level (R2 after R1 break)</li>
                                <li>Previous day's high/low</li>
                                <li>ATR-based targets (1.5x or 2x ATR)</li>
                            </ul>
                        `,
                        difficulty: "Advanced"
                    },
                    {
                        title: "Confluence Trading",
                        content: `
                            <h3>Multi-Timeframe Confluence</h3>
                            <p>Combine daily and weekly pivot levels with EMA confluence for high-probability setups.</p>

                            <h4>Confluence Factors:</h4>
                            <ul>
                                <li><strong>Pivot + EMA:</strong> Level aligns with 9 or 21 EMA</li>
                                <li><strong>Daily + Weekly:</strong> Multiple timeframe level alignment</li>
                                <li><strong>Multiple Levels:</strong> S1 near S2 or R1 near R2</li>
                                <li><strong>Round Numbers:</strong> Pivot near psychological levels</li>
                            </ul>

                            <h4>Trading Rules:</h4>
                            <ol>
                                <li>Identify confluence zones using weekly timeframe</li>
                                <li>Wait for daily price to approach confluence area</li>
                                <li>Look for reversal or continuation signals</li>
                                <li>Enter with tight stops and extended targets</li>
                            </ol>

                            <p><strong>Note:</strong> Confluence zones often provide the highest win-rate setups but may have fewer opportunities.</p>
                        `,
                        difficulty: "Advanced"
                    }
                ]
            },

            technical: {
                title: "⚙️ Technical Reference",
                sections: [
                    {
                        title: "Keyboard Shortcuts",
                        content: `
                            <h3>Productivity Shortcuts</h3>
                            <p>Master these keyboard shortcuts to work more efficiently:</p>

                            <table class="shortcuts-table">
                                <thead>
                                    <tr>
                                        <th>Shortcut</th>
                                        <th>Action</th>
                                        <th>Description</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr><td><code>Ctrl + F</code></td><td>Fetch Data</td><td>Fetch latest market data</td></tr>
                                    <tr><td><code>Ctrl + R</code></td><td>Refresh</td><td>Recalculate all levels</td></tr>
                                    <tr><td><code>Ctrl + E</code></td><td>Export</td><td>Export data to CSV</td></tr>
                                    <tr><td><code>Ctrl + H</code></td><td>Help</td><td>Show this help system</td></tr>
                                    <tr><td><code>Alt + 1</code></td><td>Daily</td><td>Switch to daily timeframe</td></tr>
                                    <tr><td><code>Alt + 2</code></td><td>Weekly</td><td>Switch to weekly timeframe</td></tr>
                                    <tr><td><code>Escape</code></td><td>Close</td><td>Close modals and notifications</td></tr>
                                </tbody>
                            </table>
                        `,
                        difficulty: "Beginner"
                    },
                    {
                        title: "Data Sources & APIs",
                        content: `
                            <h3>Market Data Integration</h3>
                            <p>The calculator uses multiple data sources for maximum reliability:</p>

                            <h4>Primary Sources:</h4>
                            <ol>
                                <li><strong>Yahoo Finance:</strong> Real-time quotes and historical data</li>
                                <li><strong>Finnhub API:</strong> Professional market data (requires API key)</li>
                                <li><strong>Alpha Vantage:</strong> Technical indicators and EOD data</li>
                            </ol>

                            <h4>Fallback System:</h4>
                            <p>The system automatically tries sources in order:</p>
                            <ul>
                                <li>Yahoo Finance (free, no key required)</li>
                                <li>Finnhub (if API key configured)</li>
                                <li>Alpha Vantage (if API key configured)</li>
                                <li>CORS proxies for access restrictions</li>
                            </ul>

                            <h4>Rate Limiting:</h4>
                            <ul>
                                <li>Yahoo: 2000 requests/minute</li>
                                <li>Finnhub: 60 requests/minute</li>
                                <li>Alpha Vantage: 5 requests/minute</li>
                            </ul>
                        `,
                        difficulty: "Advanced"
                    },
                    {
                        title: "Calculations & Formulas",
                        content: `
                            <h3>Mathematical Foundation</h3>

                            <h4>Pivot Points:</h4>
                            <div class="formula-section">
                                <p><strong>Standard Pivot Point:</strong></p>
                                <code>PP = (High + Low + Close) / 3</code>

                                <p><strong>Resistance Levels:</strong></p>
                                <code>R1 = (2 × PP) - Low</code><br>
                                <code>R2 = PP + (High - Low)</code><br>
                                <code>R3 = R1 + (High - Low)</code>

                                <p><strong>Support Levels:</strong></p>
                                <code>S1 = (2 × PP) - High</code><br>
                                <code>S2 = PP - (High - Low)</code><br>
                                <code>S3 = S1 - (High - Low)</code>
                            </div>

                            <h4>ATR (Average True Range):</h4>
                            <div class="formula-section">
                                <p><strong>True Range:</strong></p>
                                <code>TR = MAX(High-Low, |High-PrevClose|, |Low-PrevClose|)</code>

                                <p><strong>ATR (14-period):</strong></p>
                                <code>ATR = EMA(TR, 14)</code>

                                <p><strong>Zone Calculation:</strong></p>
                                <code>Zone High = Level + (ATR × 0.25)</code><br>
                                <code>Zone Low = Level - (ATR × 0.25)</code>
                            </div>

                            <h4>Weekly Calculation:</h4>
                            <p>Weekly pivots use weekly OHLC data:</p>
                            <ul>
                                <li>Monday open → Weekly open</li>
                                <li>Highest high of week → Weekly high</li>
                                <li>Lowest low of week → Weekly low</li>
                                <li>Friday close → Weekly close</li>
                            </ul>
                        `,
                        difficulty: "Advanced"
                    }
                ]
            },

            troubleshooting: {
                title: "🔧 Troubleshooting",
                sections: [
                    {
                        title: "Common Issues",
                        content: `
                            <h3>Data Fetching Problems</h3>

                            <h4>Problem: "No data available" or loading fails</h4>
                            <p><strong>Solutions:</strong></p>
                            <ol>
                                <li>Check your internet connection</li>
                                <li>Try a different asset (some may have temporary issues)</li>
                                <li>Wait 30 seconds and retry (rate limiting)</li>
                                <li>Refresh the page to reset connections</li>
                            </ol>

                            <h4>Problem: Outdated or incorrect prices</h4>
                            <p><strong>Solutions:</strong></p>
                            <ol>
                                <li>Click "🔄 Refresh" to get latest data</li>
                                <li>Check if markets are currently open</li>
                                <li>Verify the asset symbol is correct</li>
                                <li>Clear browser cache if data seems stale</li>
                            </ol>

                            <h4>Problem: Pivot levels seem wrong</h4>
                            <p><strong>Check:</strong></p>
                            <ul>
                                <li>Correct timeframe selected (Daily vs Weekly)</li>
                                <li>Data inputs are from the right session</li>
                                <li>Manual inputs haven't overridden fetched data</li>
                                <li>Compare with external pivot calculators</li>
                            </ul>
                        `,
                        difficulty: "Beginner"
                    },
                    {
                        title: "Performance Issues",
                        content: `
                            <h3>Optimization Tips</h3>

                            <h4>Browser Performance:</h4>
                            <ul>
                                <li>Use Chrome or Firefox for best performance</li>
                                <li>Close unnecessary tabs to free memory</li>
                                <li>Disable browser extensions that may interfere</li>
                                <li>Update to the latest browser version</li>
                            </ul>

                            <h4>Chart Rendering:</h4>
                            <ul>
                                <li>Reduce chart timeframe if rendering slowly</li>
                                <li>Close chart if not needed to save resources</li>
                                <li>Restart browser if charts become unresponsive</li>
                            </ul>

                            <h4>Memory Management:</h4>
                            <ul>
                                <li>Clear notifications if many have accumulated</li>
                                <li>Reset performance data if database is large</li>
                                <li>Refresh page daily for long sessions</li>
                            </ul>
                        `,
                        difficulty: "Intermediate"
                    },
                    {
                        title: "Alert System Issues",
                        content: `
                            <h3>Alert Troubleshooting</h3>

                            <h4>Problem: Alerts not triggering</h4>
                            <p><strong>Check:</strong></p>
                            <ol>
                                <li>Alert is active (green status)</li>
                                <li>Price tolerance is appropriate</li>
                                <li>Data is updating regularly</li>
                                <li>Browser notifications are enabled</li>
                            </ol>

                            <h4>Problem: No sound alerts</h4>
                            <p><strong>Solutions:</strong></p>
                            <ol>
                                <li>Check browser sound permissions</li>
                                <li>Verify sound is enabled in settings</li>
                                <li>Adjust volume slider in alert settings</li>
                                <li>Test with browser's audio test page</li>
                            </ol>

                            <h4>Problem: Too many alerts</h4>
                            <p><strong>Solutions:</strong></p>
                            <ol>
                                <li>Adjust price tolerance to reduce sensitivity</li>
                                <li>Use "Level Break" instead of "Level Touch"</li>
                                <li>Pause alerts temporarily during volatile periods</li>
                                <li>Delete alerts for old levels</li>
                            </ol>
                        `,
                        difficulty: "Intermediate"
                    }
                ]
            }
        };
    }

    // ========= HELP INTERFACE =========
    createHelpInterface() {
        const helpModal = document.createElement('div');
        helpModal.id = 'help-modal';
        helpModal.className = 'help-modal';
        helpModal.style.display = 'none';

        helpModal.innerHTML = `
            <style>
                .help-modal {
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(12, 17, 27, 0.95);
                    backdrop-filter: blur(4px);
                    z-index: 10001;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }

                .help-content {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 16px;
                    width: 95%;
                    max-width: 1200px;
                    height: 90vh;
                    display: flex;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
                    overflow: hidden;
                }

                .help-sidebar {
                    width: 300px;
                    background: var(--bg-panel);
                    border-right: 1px solid var(--border-soft);
                    display: flex;
                    flex-direction: column;
                }

                .help-header {
                    padding: 24px 20px 16px;
                    border-bottom: 1px solid var(--border-soft);
                }

                .help-title {
                    color: var(--accent-blue);
                    font-size: 1.3rem;
                    font-weight: 600;
                    margin: 0 0 12px 0;
                }

                .help-search {
                    width: 100%;
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 6px;
                    padding: 8px 12px;
                    color: var(--text-primary);
                    font-size: 14px;
                }

                .help-search::placeholder {
                    color: var(--text-secondary);
                }

                .help-navigation {
                    flex: 1;
                    overflow-y: auto;
                    padding: 16px 0;
                }

                .help-category {
                    margin-bottom: 8px;
                }

                .help-category-header {
                    padding: 12px 20px 8px;
                    color: var(--accent-blue);
                    font-weight: 600;
                    font-size: 13px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                }

                .help-category-items {
                    max-height: 0;
                    overflow: hidden;
                    transition: max-height 0.3s ease;
                }

                .help-category.expanded .help-category-items {
                    max-height: 500px;
                }

                .help-category-toggle {
                    transition: transform 0.2s ease;
                }

                .help-category.expanded .help-category-toggle {
                    transform: rotate(90deg);
                }

                .help-nav-item {
                    padding: 8px 20px 8px 32px;
                    cursor: pointer;
                    color: var(--text-secondary);
                    font-size: 14px;
                    border-left: 3px solid transparent;
                    transition: all 0.2s ease;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                }

                .help-nav-item:hover {
                    background: var(--bg-accent);
                    color: var(--text-primary);
                    border-left-color: var(--accent-blue);
                }

                .help-nav-item.active {
                    background: var(--bg-accent);
                    color: var(--accent-blue);
                    border-left-color: var(--accent-blue);
                    font-weight: 500;
                }

                .help-difficulty {
                    font-size: 11px;
                    padding: 2px 6px;
                    border-radius: 10px;
                    font-weight: 500;
                }

                .help-difficulty.beginner {
                    background: rgba(102, 187, 106, 0.2);
                    color: var(--accent-green);
                }

                .help-difficulty.intermediate {
                    background: rgba(253, 216, 53, 0.2);
                    color: var(--accent-gold);
                }

                .help-difficulty.advanced {
                    background: rgba(239, 83, 80, 0.2);
                    color: var(--accent-red);
                }

                .help-main {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                }

                .help-main-header {
                    padding: 24px 24px 16px;
                    border-bottom: 1px solid var(--border-soft);
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                }

                .help-main-title {
                    color: var(--text-primary);
                    font-size: 1.1rem;
                    font-weight: 600;
                    margin: 0;
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .help-close {
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
                    transition: all 0.2s ease;
                }

                .help-close:hover {
                    background: var(--bg-panel);
                    color: var(--text-primary);
                }

                .help-body {
                    flex: 1;
                    overflow-y: auto;
                    padding: 24px;
                }

                .help-article {
                    max-width: 800px;
                    line-height: 1.6;
                    color: var(--text-primary);
                }

                .help-article h3 {
                    color: var(--accent-blue);
                    font-size: 1.2rem;
                    margin: 0 0 16px 0;
                }

                .help-article h4 {
                    color: var(--text-primary);
                    font-size: 1rem;
                    margin: 20px 0 12px 0;
                }

                .help-article p {
                    margin-bottom: 16px;
                    color: var(--text-primary);
                }

                .help-article ul, .help-article ol {
                    margin-bottom: 16px;
                    padding-left: 24px;
                }

                .help-article li {
                    margin-bottom: 6px;
                    color: var(--text-primary);
                }

                .help-article strong {
                    color: var(--accent-blue);
                }

                .help-article code {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 4px;
                    padding: 2px 6px;
                    font-family: var(--font-mono);
                    font-size: 13px;
                    color: var(--accent-blue);
                }

                .formula-box, .formula-section {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                    padding: 16px;
                    margin: 16px 0;
                    font-family: var(--font-mono);
                }

                .shortcuts-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 16px 0;
                    background: var(--bg-panel);
                    border-radius: 8px;
                    overflow: hidden;
                }

                .shortcuts-table th,
                .shortcuts-table td {
                    padding: 12px 16px;
                    text-align: left;
                    border-bottom: 1px solid var(--border-soft);
                }

                .shortcuts-table th {
                    background: var(--bg-accent);
                    color: var(--accent-blue);
                    font-weight: 600;
                }

                .shortcuts-table code {
                    background: var(--bg-accent);
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-weight: 600;
                }

                .help-progress {
                    margin-top: 24px;
                    padding: 16px;
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                }

                .help-progress-bar {
                    width: 100%;
                    height: 6px;
                    background: var(--bg-accent);
                    border-radius: 3px;
                    overflow: hidden;
                    margin-top: 8px;
                }

                .help-progress-fill {
                    height: 100%;
                    background: linear-gradient(90deg, var(--accent-blue), var(--accent-green));
                    border-radius: 3px;
                    transition: width 0.3s ease;
                }

                .help-search-results {
                    margin-bottom: 24px;
                }

                .help-search-result {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 12px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }

                .help-search-result:hover {
                    border-color: var(--accent-blue);
                }

                .help-search-result-title {
                    color: var(--accent-blue);
                    font-weight: 600;
                    margin-bottom: 8px;
                }

                .help-search-result-snippet {
                    color: var(--text-secondary);
                    font-size: 14px;
                }

                @media (max-width: 768px) {
                    .help-content {
                        width: 98%;
                        height: 95vh;
                        flex-direction: column;
                    }

                    .help-sidebar {
                        width: 100%;
                        height: 200px;
                    }

                    .help-main {
                        height: calc(95vh - 200px);
                    }
                }
            </style>

            <div class="help-content">
                <div class="help-sidebar">
                    <div class="help-header">
                        <h2 class="help-title">📚 Help Center</h2>
                        <input type="text" class="help-search" placeholder="Search help articles..." id="help-search">
                    </div>

                    <div class="help-navigation" id="help-navigation">
                        <!-- Dynamic navigation content -->
                    </div>
                </div>

                <div class="help-main">
                    <div class="help-main-header">
                        <h3 class="help-main-title" id="help-main-title">Welcome to Help Center</h3>
                        <button class="help-close" onclick="window.helpSystem.hideHelp()">×</button>
                    </div>

                    <div class="help-body">
                        <div class="help-article" id="help-article">
                            <div class="help-search-results" id="help-search-results" style="display: none;">
                                <!-- Search results -->
                            </div>

                            <div id="help-content-area">
                                <h3>Welcome to Professional Pivot Calculator Help</h3>
                                <p>Select a topic from the left sidebar to get started, or use the search box to find specific information.</p>

                                <h4>Popular Topics:</h4>
                                <ul>
                                    <li><strong>Quick Start Guide:</strong> Get up and running in minutes</li>
                                    <li><strong>Pivot Level Trading:</strong> Learn professional trading strategies</li>
                                    <li><strong>Alert System:</strong> Set up intelligent price notifications</li>
                                    <li><strong>Performance Tracking:</strong> Analyze your trading results</li>
                                </ul>

                                <h4>Need More Help?</h4>
                                <p>This help system includes:</p>
                                <ul>
                                    <li>🚀 Step-by-step tutorials</li>
                                    <li>📈 Trading strategy guides</li>
                                    <li>⚙️ Technical reference</li>
                                    <li>🔧 Troubleshooting solutions</li>
                                </ul>
                            </div>

                            <div class="help-progress">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <span style="color: var(--text-primary); font-weight: 500;">Learning Progress</span>
                                    <span style="color: var(--text-secondary); font-size: 14px;" id="help-progress-text">0% Complete</span>
                                </div>
                                <div class="help-progress-bar">
                                    <div class="help-progress-fill" id="help-progress-fill" style="width: 0%"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(helpModal);
        this.setupHelpEvents();
        this.buildNavigation();
    }

    setupHelpEvents() {
        // Search functionality
        document.getElementById('help-search').addEventListener('input', (e) => {
            this.performSearch(e.target.value);
        });

        // Category expansion
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('help-category-header')) {
                const category = e.target.parentElement;
                category.classList.toggle('expanded');
            }
        });
    }

    buildNavigation() {
        const nav = document.getElementById('help-navigation');
        const categories = Object.entries(this.helpContent);

        nav.innerHTML = categories.map(([key, category]) => `
            <div class="help-category ${key === 'quickStart' ? 'expanded' : ''}">
                <div class="help-category-header">
                    ${category.title}
                    <span class="help-category-toggle">▶</span>
                </div>
                <div class="help-category-items">
                    ${category.sections.map((section, index) => `
                        <div class="help-nav-item"
                             data-category="${key}"
                             data-section="${index}"
                             onclick="window.helpSystem.showSection('${key}', ${index})">
                            <span>${section.title}</span>
                            <span class="help-difficulty ${section.difficulty.toLowerCase()}">${section.difficulty}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');
    }

    // ========= CONTENT MANAGEMENT =========
    showSection(categoryKey, sectionIndex) {
        const category = this.helpContent[categoryKey];
        const section = category.sections[sectionIndex];

        if (!section) return;

        // Update active nav item
        document.querySelectorAll('.help-nav-item').forEach(item => {
            item.classList.remove('active');
        });

        document.querySelector(`[data-category="${categoryKey}"][data-section="${sectionIndex}"]`)?.classList.add('active');

        // Update main content
        document.getElementById('help-main-title').innerHTML = `
            ${section.title}
            <span class="help-difficulty ${section.difficulty.toLowerCase()}">${section.difficulty}</span>
        `;

        document.getElementById('help-content-area').innerHTML = section.content;
        document.getElementById('help-search-results').style.display = 'none';

        // Mark as read and update progress
        this.markSectionAsRead(categoryKey, sectionIndex);
        this.updateProgress();

        // Scroll to top
        document.querySelector('.help-body').scrollTop = 0;
    }

    markSectionAsRead(categoryKey, sectionIndex) {
        const key = `${categoryKey}_${sectionIndex}`;
        this.userProgress.set(key, {
            read: true,
            timestamp: new Date().toISOString()
        });
        this.saveUserProgress();
    }

    updateProgress() {
        const totalSections = Object.values(this.helpContent).reduce((sum, cat) => sum + cat.sections.length, 0);
        const readSections = this.userProgress.size;
        const percentage = Math.round((readSections / totalSections) * 100);

        document.getElementById('help-progress-text').textContent = `${percentage}% Complete (${readSections}/${totalSections})`;
        document.getElementById('help-progress-fill').style.width = `${percentage}%`;
    }

    // ========= SEARCH FUNCTIONALITY =========
    buildSearchIndex() {
        Object.entries(this.helpContent).forEach(([categoryKey, category]) => {
            category.sections.forEach((section, sectionIndex) => {
                const content = section.content.toLowerCase();
                const title = section.title.toLowerCase();

                // Extract words for indexing
                const words = [...new Set([
                    ...title.split(/\\W+/),
                    ...content.replace(/<[^>]*>/g, '').split(/\\W+/)
                ])].filter(word => word.length > 2);

                words.forEach(word => {
                    if (!this.searchIndex.has(word)) {
                        this.searchIndex.set(word, []);
                    }

                    this.searchIndex.get(word).push({
                        categoryKey,
                        sectionIndex,
                        title: section.title,
                        content: section.content,
                        difficulty: section.difficulty
                    });
                });
            });
        });
    }

    performSearch(query) {
        const searchResults = document.getElementById('help-search-results');
        const contentArea = document.getElementById('help-content-area');

        if (!query.trim()) {
            searchResults.style.display = 'none';
            contentArea.style.display = 'block';
            return;
        }

        const terms = query.toLowerCase().split(/\\s+/).filter(term => term.length > 1);
        const results = new Map();

        terms.forEach(term => {
            this.searchIndex.forEach((entries, word) => {
                if (word.includes(term)) {
                    entries.forEach(entry => {
                        const key = `${entry.categoryKey}_${entry.sectionIndex}`;
                        if (!results.has(key)) {
                            results.set(key, { ...entry, score: 0 });
                        }
                        results.get(key).score += word === term ? 2 : 1; // Exact match bonus
                    });
                }
            });
        });

        // Sort by relevance score
        const sortedResults = Array.from(results.values()).sort((a, b) => b.score - a.score);

        if (sortedResults.length === 0) {
            searchResults.innerHTML = '<div style="text-align: center; color: var(--text-secondary); padding: 40px;">No results found.</div>';
        } else {
            searchResults.innerHTML = sortedResults.slice(0, 8).map(result => {
                const snippet = this.extractSnippet(result.content, query);
                return `
                    <div class="help-search-result"
                         onclick="window.helpSystem.showSection('${result.categoryKey}', ${result.sectionIndex})">
                        <div class="help-search-result-title">
                            ${result.title}
                            <span class="help-difficulty ${result.difficulty.toLowerCase()}">${result.difficulty}</span>
                        </div>
                        <div class="help-search-result-snippet">${snippet}</div>
                    </div>
                `;
            }).join('');
        }

        searchResults.style.display = 'block';
        contentArea.style.display = 'none';
    }

    extractSnippet(content, query, maxLength = 150) {
        const cleanContent = content.replace(/<[^>]*>/g, ' ').replace(/\\s+/g, ' ').trim();
        const queryLower = query.toLowerCase();
        const contentLower = cleanContent.toLowerCase();

        const index = contentLower.indexOf(queryLower);
        if (index === -1) {
            return cleanContent.substring(0, maxLength) + (cleanContent.length > maxLength ? '...' : '');
        }

        const start = Math.max(0, index - 50);
        const end = Math.min(cleanContent.length, index + query.length + 50);
        const snippet = cleanContent.substring(start, end);

        return (start > 0 ? '...' : '') + snippet + (end < cleanContent.length ? '...' : '');
    }

    // ========= DATA PERSISTENCE =========
    saveUserProgress() {
        localStorage.setItem('helpProgress', JSON.stringify(Array.from(this.userProgress.entries())));
    }

    loadUserProgress() {
        const saved = localStorage.getItem('helpProgress');
        if (saved) {
            try {
                this.userProgress = new Map(JSON.parse(saved));
                this.updateProgress();
            } catch (error) {
                /* eslint-disable-next-line no-console */
                console.warn('Could not load help progress:', error);
            }
        }
    }

    // ========= PUBLIC API =========
    showHelp(categoryKey = null, sectionIndex = null) {
        document.getElementById('help-modal').style.display = 'flex';

        if (categoryKey && sectionIndex !== null) {
            this.showSection(categoryKey, sectionIndex);
        }

        // Clear search
        document.getElementById('help-search').value = '';
        document.getElementById('help-search-results').style.display = 'none';
        document.getElementById('help-content-area').style.display = 'block';
    }

    hideHelp() {
        document.getElementById('help-modal').style.display = 'none';
    }

    showQuickStart() {
        this.showHelp('quickStart', 0);
    }

    showTradingGuide() {
        this.showHelp('trading', 0);
    }

    showTroubleshooting() {
        this.showHelp('troubleshooting', 0);
    }
}

// Initialize help system
document.addEventListener('DOMContentLoaded', () => {
    window.helpSystem = new ProfessionalHelpSystem();

    // Wire up help button
    const helpBtn = document.getElementById('helpSystem');
    if (helpBtn) {
        helpBtn.addEventListener('click', () => {
            window.helpSystem.showHelp();
        });
    }

    // Show welcome tutorial for first-time users
    const hasSeenTutorial = localStorage.getItem('hasSeenTutorial');
    if (!hasSeenTutorial) {
        setTimeout(() => {
            if (window.professionalUI) {
                window.professionalUI.showNotification('info', 'Welcome!',
                    'New to the Professional Pivot Calculator? Click the Help button for tutorials.', {
                        duration: 8000,
                        actions: [
                            {
                                id: 'quickstart',
                                label: 'Quick Start',
                                handler: () => {
                                    window.helpSystem.showQuickStart();
                                    localStorage.setItem('hasSeenTutorial', 'true');
                                }
                            }
                        ]
                    });
            }
        }, 2000);
    }
});