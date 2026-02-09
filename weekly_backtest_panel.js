/* ---------- weekly_backtest_panel.js ----------
   Enhanced Weekly Backtest Panel with Wilson CI + Sample Adequacy
   Reuses daily engine; adds statistical confidence layers
---------------------------------------------------*/

window.weeklyBacktestPanel = (() => {
    /* eslint-disable-next-line no-console */
    console.log("🔧 Weekly Backtest Panel: Loading...");

    const state = {
        weeklyResults: null,
        priorWeekData: null,
        wilsonCI: null,
        sampleBadges: null,
        isRunning: false
    };

    // ========= PRIOR WEEK COMPUTATION =========
    function computePriorWeekPivots(historicalData) {
        if (!historicalData || historicalData.length < 7) {
            throw new Error("Insufficient data for prior week analysis");
        }

        // Use fallback weekly aggregation (same logic as main engine)
        const weeklyData = fallbackWeeklyAggregation(historicalData);

        if (weeklyData.length < 2) {
            throw new Error("Need at least 2 weeks of data for prior week pivots");
        }

        // Prior week = second-to-last week (excluding current incomplete week)
        const priorWeek = weeklyData[weeklyData.length - 2];
        const { high, low, close } = priorWeek;

        // Calculate pivots using prior week's H/L/C
        const pivot = (high + low + close) / 3;
        const range = high - low;

        state.priorWeekData = {
            high, low, close, pivot, range,
            R1: 2 * pivot - low,
            R2: pivot + range,
            R3: 2 * pivot - low + range,
            S1: 2 * pivot - high,
            S2: pivot - range,
            S3: 2 * pivot - high - range,
            weekStart: priorWeek.date,
            timestamp: priorWeek.timestamp
        };

        return state.priorWeekData;
    }

    // ========= WEEKLY BACKTEST WITH CONFIDENCE =========
    async function runWeeklyBacktest(days = 90) {
        if (state.isRunning) return notice('Weekly backtest already running…');
        state.isRunning = true;
        updateWeeklyUI('running', `Fetching ${days} days for weekly analysis…`);

        try {
            // First try to use existing historical data, otherwise run regular backtest first
            let data = window.backtestEngine?.state?.historicalData;

            if (!data || data.length < 14) {
                updateWeeklyUI('running', 'No existing data found. Running daily backtest first...');
                await window.backtestEngine.runBacktest(days, 'daily');
                data = window.backtestEngine.state.historicalData;
            }

            if (!data || data.length < 14) throw new Error("Need at least 2 weeks of data");

            updateWeeklyUI('running', 'Aggregating to weekly timeframe…');
            const weeklyData = fallbackWeeklyAggregation(data);

            updateWeeklyUI('running', 'Computing prior week pivots…');
            const priorWeekPivots = computePriorWeekPivots(data);

            updateWeeklyUI('running', 'Calculating weekly EMAs (9W, 21W)…');
            const _emaHistory = [];
        const weeklyEMAs = calculateWeeklyEMAs(weeklyData);

            updateWeeklyUI('running', 'Running enhanced weekly backtest…');
            const enhancedResults = await runEnhancedWeeklyBacktest(weeklyData, weeklyEMAs);

            updateWeeklyUI('running', 'Computing Wilson confidence intervals…');
            const wilsonCI = computeWilsonCI(enhancedResults.reversalStats);

            updateWeeklyUI('running', 'Analyzing sample adequacy…');
            const sampleBadges = computeSampleAdequacy(enhancedResults.reversalStats, enhancedResults.dataPoints);

            updateWeeklyUI('running', 'Detecting EMA-Pivot confluence…');
            const confluenceAnalysis = analyzeEMAConfluence(enhancedResults.pivotHistory, weeklyEMAs);

            state.weeklyResults = {
                ...enhancedResults,
                priorWeekPivots,
                weeklyEMAs,
                wilsonCI,
                sampleBadges,
                confluenceAnalysis,
                enhancedTimestamp: new Date().toISOString()
            };

            displayWeeklyResults(state.weeklyResults);
            updateWeeklyUI('complete', `Enhanced weekly backtest complete: ${enhancedResults.dataPoints} weeks analyzed`);

        } catch (e) {
            /* eslint-disable-next-line no-console */
            console.error('Weekly backtest error:', e);
            updateWeeklyUI('error', `Weekly backtest failed: ${e.message}`);
            alert(`Weekly backtest failed: ${e.message}`);
        } finally {
            state.isRunning = false;
        }
    }

    // ========= WILSON CI COMPUTATION =========
    function computeWilsonCI(reversalStats) {
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        const wilsonResults = {};

        levels.forEach(level => {
            const stat = reversalStats[level];
            if (stat.touches === 0) {
                wilsonResults[level] = {
                    lower: 0, upper: 0, center: 0,
                    width: 0, confidence: 0.95,
                    status: 'insufficient_data'
                };
                return;
            }

            // Use existing Wilson CI from stats_confidence.js
            const successes = stat.reversals;
            const trials = stat.touches;

            try {
                const ci = window.statsConfidence.wilsonScoreInterval(successes, trials, 0.95);
                wilsonResults[level] = {
                    lower: ci.lower * 100,  // Convert to percentage
                    upper: ci.upper * 100,
                    center: (successes / trials) * 100,
                    width: (ci.upper - ci.lower) * 100,
                    confidence: 0.95,
                    status: 'computed'
                };
            } catch (e) {
                /* eslint-disable-next-line no-console */
                console.warn(`Wilson CI failed for ${level}:`, e);
                wilsonResults[level] = {
                    lower: 0, upper: 0, center: 0,
                    width: 0, confidence: 0.95,
                    status: 'computation_error'
                };
            }
        });

        return wilsonResults;
    }

    // ========= WEEKLY EMA CALCULATIONS =========
    function calculateWeeklyEMAs(weeklyData) {
        if (!weeklyData || weeklyData.length < 21) {
            return { ema9: [], ema21: [], current: { ema9: null, ema21: null } };
        }

        const ema9 = calculateEMA(weeklyData.map(d => d.close), 9);
        const ema21 = calculateEMA(weeklyData.map(d => d.close), 21);

        return {
            ema9,
            ema21,
            current: {
                ema9: ema9[ema9.length - 1],
                ema21: ema21[ema21.length - 1]
            }
        };
    }

    function calculateEMA(prices, period) {
        const ema = [];
        const multiplier = 2 / (period + 1);

        // Start with SMA for first value
        let sum = 0;
        for (let i = 0; i < period && i < prices.length; i++) {
            sum += prices[i];
            if (i === period - 1) {
                ema[i] = sum / period;
            }
        }

        // Calculate EMA for remaining values
        for (let i = period; i < prices.length; i++) {
            ema[i] = (prices[i] * multiplier) + (ema[i - 1] * (1 - multiplier));
        }

        return ema;
    }

    // ========= ENHANCED WEEKLY BACKTEST =========
    async function runEnhancedWeeklyBacktest(weeklyData, weeklyEMAs) {
        const pivotHistory = [];
        const _emaHistory2 = [];

        // Calculate historical pivots and EMAs for each week
        for (let i = 1; i < weeklyData.length; i++) {
            const prev = weeklyData[i - 1];
            const cur = weeklyData[i];

            // Standard pivot calculation
            const pivot = (prev.high + prev.low + prev.close) / 3;
            const range = prev.high - prev.low;
            const pivotLevels = {
                pivot,
                r1: 2 * pivot - prev.low,
                r2: pivot + range,
                r3: 2 * pivot - prev.low + range,
                s1: 2 * pivot - prev.high,
                s2: pivot - range,
                s3: 2 * pivot - prev.high - range
            };

            // EMA levels for this week
            const emaLevels = {
                ema9: weeklyEMAs.ema9[i - 1] || null,
                ema21: weeklyEMAs.ema21[i - 1] || null
            };

            pivotHistory.push({
                date: cur.date,
                ...pivotLevels,
                ...emaLevels,
                actualHigh: cur.high,
                actualLow: cur.low,
                actualClose: cur.close,
                prevClose: prev.close,
                atr: calculateWeeklyATR(weeklyData.slice(Math.max(0, i - 14), i))
            });
        }

        // Calculate reversal stats for all levels (pivots + EMAs)
        const reversalStats = calculateEnhancedReversalStats(pivotHistory);

        return {
            period: `${weeklyData.length * 7} days (estimated)`,
            dataPoints: weeklyData.length,
            timeframe: 'weekly_enhanced',
            reversalStats,
            pivotHistory,
            timestamp: new Date().toISOString()
        };
    }

    function calculateWeeklyATR(weeklyBars) {
        if (weeklyBars.length < 2) return 0;
        const trs = [];
        for (let i = 1; i < weeklyBars.length; i++) {
            const H = weeklyBars[i].high;
            const L = weeklyBars[i].low;
            const pc = weeklyBars[i - 1].close;
            trs.push(Math.max(H - L, Math.abs(H - pc), Math.abs(L - pc)));
        }
        return trs.reduce((a, b) => a + b, 0) / trs.length;
    }

    function calculateEnhancedReversalStats(pivotHistory) {
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        const stats = Object.fromEntries(levels.map(l => [l, { touches: 0, reversals: 0, breaks: 0, reliability: 'N/A' }]));

        pivotHistory.forEach(d => {
            const thr = d.atr * 0.5; // Touch window = 0.5*ATR
            const hi = d.actualHigh;
            const lo = d.actualLow;
            const cls = d.actualClose;

            levels.forEach(levelKey => {
                const lv = d[levelKey];
                if (!lv || !Number.isFinite(lv)) return; // Skip null/undefined EMAs

                if (hi >= lv - thr && lo <= lv + thr) {
                    const s = stats[levelKey];
                    s.touches++;

                    // Determine if it's resistance or support
                    const isResistance = levelKey.startsWith('r') || levelKey === 'ema9' || levelKey === 'ema21';
                    const isSupport = levelKey.startsWith('s') || levelKey === 'pivot' || levelKey === 'ema9' || levelKey === 'ema21';

                    if (isResistance) {
                        if (cls < lv) s.reversals++;
                        else if (cls > lv + thr) s.breaks++;
                    }
                    if (isSupport) {
                        if (cls > lv) s.reversals++;
                        else if (cls < lv - thr) s.breaks++;
                    }
                }
            });
        });

        // Calculate reliability percentages
        levels.forEach(levelKey => {
            const s = stats[levelKey];
            if (s.touches > 0) {
                s.reliability = ((s.reversals / s.touches) * 100).toFixed(1);
            }
        });

        return stats;
    }

    // ========= EMA CONFLUENCE ANALYSIS =========
    function analyzeEMAConfluence(pivotHistory, _weeklyEMAs) {
        const confluences = [];
        const threshold = 0.01; // 1% threshold for confluence

        pivotHistory.forEach(d => {
            const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3'];

            levels.forEach(level => {
                const pivotValue = d[level];
                if (!pivotValue) return;

                const confluenceData = {
                    date: d.date,
                    level,
                    pivotValue,
                    confluences: []
                };

                // Check confluence with 9W EMA
                if (d.ema9 && Math.abs(pivotValue - d.ema9) / pivotValue < threshold) {
                    confluenceData.confluences.push({
                        type: 'EMA9',
                        value: d.ema9,
                        deviation: ((Math.abs(pivotValue - d.ema9) / pivotValue) * 100).toFixed(2)
                    });
                }

                // Check confluence with 21W EMA
                if (d.ema21 && Math.abs(pivotValue - d.ema21) / pivotValue < threshold) {
                    confluenceData.confluences.push({
                        type: 'EMA21',
                        value: d.ema21,
                        deviation: ((Math.abs(pivotValue - d.ema21) / pivotValue) * 100).toFixed(2)
                    });
                }

                if (confluenceData.confluences.length > 0) {
                    confluences.push(confluenceData);
                }
            });
        });

        return {
            totalConfluences: confluences.length,
            confluenceData: confluences,
            summary: {
                ema9Confluences: confluences.filter(c => c.confluences.some(conf => conf.type === 'EMA9')).length,
                ema21Confluences: confluences.filter(c => c.confluences.some(conf => conf.type === 'EMA21')).length,
                doubleConfluences: confluences.filter(c => c.confluences.length > 1).length
            }
        };
    }

    // ========= SAMPLE ADEQUACY BADGES =========
    function computeSampleAdequacy(reversalStats, totalWeeks) {
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        const badges = {};

        levels.forEach(level => {
            const stat = reversalStats[level];
            const touches = stat.touches;
            const sampleRate = touches / totalWeeks;

            // Sample adequacy thresholds
            let adequacy, color, symbol;

            if (touches >= 20) {
                adequacy = 'EXCELLENT';
                color = 'var(--accent-green)';
                symbol = '🟢';
            } else if (touches >= 10) {
                adequacy = 'GOOD';
                color = 'var(--accent-blue)';
                symbol = '🔵';
            } else if (touches >= 5) {
                adequacy = 'MARGINAL';
                color = 'var(--accent-gold)';
                symbol = '🟡';
            } else if (touches > 0) {
                adequacy = 'INSUFFICIENT';
                color = 'var(--accent-red)';
                symbol = '🔴';
            } else {
                adequacy = 'NO_DATA';
                color = 'var(--text-secondary)';
                symbol = '⚪';
            }

            badges[level] = {
                adequacy,
                color,
                symbol,
                touches,
                sampleRate: (sampleRate * 100).toFixed(1),
                recommendation: getAdequacyRecommendation(touches)
            };
        });

        return badges;
    }

    function getAdequacyRecommendation(touches) {
        if (touches >= 20) return "Statistically robust";
        if (touches >= 10) return "Reliable for trading decisions";
        if (touches >= 5) return "Use with caution";
        if (touches > 0) return "Collect more data";
        return "Level not tested";
    }

    // ========= UI DISPLAY =========
    function displayWeeklyResults(results) {
        const container = document.getElementById('weeklyBacktestResults');
        if (!container) {
            /* eslint-disable-next-line no-console */
            console.warn('weeklyBacktestResults container not found');
            return;
        }

        container.style.display = 'block';
        container.innerHTML = generateWeeklyResultsHTML(results);
    }

    function generateWeeklyResultsHTML(results) {
        return `
            <div class="weekly-backtest-container">
                <div class="weekly-header">
                    <h3>📊 Enhanced Weekly Backtest Panel</h3>
                    <div class="prior-week-info">
                        <strong>Prior Week Reference:</strong>
                        H: ${results.priorWeekPivots.high.toFixed(2)} |
                        L: ${results.priorWeekPivots.low.toFixed(2)} |
                        C: ${results.priorWeekPivots.close.toFixed(2)}
                        <br>
                        <small>Week starting: ${results.priorWeekPivots.weekStart}</small>
                    </div>
                    ${results.weeklyEMAs ? `
                    <div class="ema-info" style="margin-top:8px;font-size:12px">
                        <strong>Current Weekly EMAs:</strong>
                        9W: ${results.weeklyEMAs.current.ema9 ? results.weeklyEMAs.current.ema9.toFixed(2) : 'N/A'} |
                        21W: ${results.weeklyEMAs.current.ema21 ? results.weeklyEMAs.current.ema21.toFixed(2) : 'N/A'}
                    </div>
                    ` : ''}
                    ${results.confluenceAnalysis ? `
                    <div class="confluence-summary" style="margin-top:8px;font-size:11px;color:var(--text-secondary)">
                        <strong>Confluence Found:</strong> ${results.confluenceAnalysis.totalConfluences} instances |
                        EMA9: ${results.confluenceAnalysis.summary.ema9Confluences} |
                        EMA21: ${results.confluenceAnalysis.summary.ema21Confluences} |
                        Double: ${results.confluenceAnalysis.summary.doubleConfluences}
                    </div>
                    ` : ''}
                </div>

                <div class="weekly-results-grid">
                    <div class="weekly-section">
                        <h4>🎯 Level Performance with Confidence</h4>
                        <table class="weekly-table">
                            <thead>
                                <tr>
                                    <th>Level</th>
                                    <th>Touches</th>
                                    <th>Reliability</th>
                                    <th>Wilson CI (95%)</th>
                                    <th>Sample</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${generateConfidenceRows(results)}
                            </tbody>
                        </table>
                    </div>

                    <div class="weekly-section">
                        <h4>📈 Sample Adequacy Dashboard</h4>
                        <div class="adequacy-grid">
                            ${generateAdequacyBadges(results.sampleBadges)}
                        </div>
                    </div>
                </div>

                <div class="weekly-export-actions">
                    <button onclick="weeklyBacktestPanel.exportWeeklyCSV()" class="action-btn">
                        📥 Export Weekly CSV
                    </button>
                    <button onclick="weeklyBacktestPanel.exportWeeklyReport()" class="action-btn">
                        📄 Export Weekly Report
                    </button>
                </div>
            </div>
        `;
    }

    function generateConfidenceRows(results) {
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        const labels = {
            r3:'R3', r2:'R2', r1:'R1', pivot:'PIVOT', s1:'S1', s2:'S2', s3:'S3',
            ema9:'EMA 9W', ema21:'EMA 21W'
        };

        return levels.map(level => {
            const stat = results.reversalStats[level];
            const wilson = results.wilsonCI[level];
            const badge = results.sampleBadges[level];

            const reliability = stat.reliability === 'N/A' ? 'N/A' : `${stat.reliability}%`;
            const ciDisplay = wilson.status === 'computed' ?
                `${wilson.lower.toFixed(1)}% - ${wilson.upper.toFixed(1)}%` :
                'N/A';

            return `
                <tr>
                    <td><strong>${labels[level]}</strong></td>
                    <td>${stat.touches}</td>
                    <td>${reliability}</td>
                    <td>${ciDisplay}</td>
                    <td>
                        <span class="adequacy-badge" style="color: ${badge.color}">
                            ${badge.symbol} ${badge.adequacy}
                        </span>
                    </td>
                </tr>
            `;
        }).join('');
    }

    function generateAdequacyBadges(badges) {
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        const labels = {
            r3:'R3', r2:'R2', r1:'R1', pivot:'PIVOT', s1:'S1', s2:'S2', s3:'S3',
            ema9:'EMA 9W', ema21:'EMA 21W'
        };

        return levels.map(level => {
            const badge = badges[level];
            return `
                <div class="adequacy-card">
                    <div class="adequacy-header">
                        <span class="level-name">${labels[level]}</span>
                        <span class="adequacy-symbol" style="color: ${badge.color}">
                            ${badge.symbol}
                        </span>
                    </div>
                    <div class="adequacy-body">
                        <div class="adequacy-status">${badge.adequacy}</div>
                        <div class="adequacy-details">
                            ${badge.touches} touches (${badge.sampleRate}%)
                        </div>
                        <div class="adequacy-rec">${badge.recommendation}</div>
                    </div>
                </div>
            `;
        }).join('');
    }

    // ========= EXPORT FUNCTIONS =========
    function exportWeeklyCSV() {
        if (!state.weeklyResults) {
            alert("Run weekly backtest first.");
            return;
        }

        const results = state.weeklyResults;
        let csv = 'Weekly Pivot Backtest Results with Confidence Intervals\n\n';

        csv += `Period,${results.period}\n`;
        csv += `Timeframe,${results.timeframe}\n`;
        csv += `Data Points,${results.dataPoints} weeks\n`;
        csv += `Prior Week Reference,"H:${results.priorWeekPivots.high.toFixed(2)} L:${results.priorWeekPivots.low.toFixed(2)} C:${results.priorWeekPivots.close.toFixed(2)}"\n`;
        csv += `Generated,${new Date(results.enhancedTimestamp).toLocaleString()}\n\n`;

        csv += 'Level,Touches,Reversals,Breaks,Reliability,Wilson_CI_Lower,Wilson_CI_Upper,CI_Width,Sample_Adequacy,Recommendation\n';

        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        levels.forEach(level => {
            const stat = results.reversalStats[level];
            const wilson = results.wilsonCI[level];
            const badge = results.sampleBadges[level];

            csv += `${level.toUpperCase()},${stat.touches},${stat.reversals},${stat.breaks},${stat.reliability},`;
            csv += `${wilson.lower.toFixed(2)},${wilson.upper.toFixed(2)},${wilson.width.toFixed(2)},`;
            csv += `${badge.adequacy},"${badge.recommendation}"\n`;
        });

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `weekly_pivot_backtest_${Date.now()}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    }

    function exportWeeklyReport() {
        if (!state.weeklyResults) {
            alert("Run weekly backtest first.");
            return;
        }

        const results = state.weeklyResults;
        let txt = `WEEKLY PIVOT BACKTEST REPORT WITH CONFIDENCE ANALYSIS\n`;
        txt += `${'='.repeat(60)}\n\n`;

        txt += `Period: ${results.period}\n`;
        txt += `Timeframe: ${results.timeframe}\n`;
        txt += `Weeks Analyzed: ${results.dataPoints}\n`;
        txt += `Prior Week Reference: H:${results.priorWeekPivots.high.toFixed(2)} L:${results.priorWeekPivots.low.toFixed(2)} C:${results.priorWeekPivots.close.toFixed(2)}\n`;
        txt += `Generated: ${new Date(results.enhancedTimestamp).toLocaleString()}\n\n`;

        txt += `--- LEVEL PERFORMANCE WITH CONFIDENCE INTERVALS ---\n`;
        const levels = ['r3', 'r2', 'r1', 'pivot', 's1', 's2', 's3', 'ema9', 'ema21'];
        levels.forEach(level => {
            const stat = results.reversalStats[level];
            const wilson = results.wilsonCI[level];
            const badge = results.sampleBadges[level];

            txt += `\n${level.toUpperCase()}:\n`;
            txt += `  Touches: ${stat.touches}\n`;
            txt += `  Reversals: ${stat.reversals}\n`;
            txt += `  Reliability: ${stat.reliability}${stat.reliability !== 'N/A' ? '%' : ''}\n`;

            if (wilson.status === 'computed') {
                txt += `  Wilson CI (95%): ${wilson.lower.toFixed(1)}% - ${wilson.upper.toFixed(1)}%\n`;
                txt += `  CI Width: ${wilson.width.toFixed(1)}%\n`;
            }

            txt += `  Sample Adequacy: ${badge.adequacy}\n`;
            txt += `  Recommendation: ${badge.recommendation}\n`;
        });

        txt += `\n--- SAMPLE ADEQUACY SUMMARY ---\n`;
        const adequacyCounts = {};
        levels.forEach(level => {
            const adequacy = results.sampleBadges[level].adequacy;
            adequacyCounts[adequacy] = (adequacyCounts[adequacy] || 0) + 1;
        });

        Object.entries(adequacyCounts).forEach(([adequacy, count]) => {
            txt += `${adequacy}: ${count} levels\n`;
        });

        const blob = new Blob([txt], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `weekly_pivot_report_${Date.now()}.txt`;
        a.click();
        URL.revokeObjectURL(url);
    }

    // ========= UTILITIES =========
    function updateWeeklyUI(status, message) {
        const statusEl = document.getElementById('weeklyBacktestStatus');
        if (statusEl) {
            const colors = {
                running: 'var(--accent-blue)',
                complete: 'var(--accent-green)',
                error: 'var(--accent-red)'
            };
            statusEl.textContent = message;
            statusEl.style.color = colors[status] || 'var(--text-primary)';
        }
    }

    /* eslint-disable-next-line no-console */
    const notice = (msg) => console.log('WeeklyBacktest:', msg);

    // Fallback weekly aggregation if backtest engine not available
    function fallbackWeeklyAggregation(dailyData) {
        // Simple weekly aggregation fallback
        const weeks = {};
        dailyData.forEach(day => {
            const date = new Date(day.timestamp * 1000);
            const weekKey = `${date.getUTCFullYear()}-W${getWeekNumber(date)}`;

            if (!weeks[weekKey]) {
                weeks[weekKey] = {
                    timestamp: day.timestamp,
                    date: day.date,
                    open: day.open,
                    high: day.high,
                    low: day.low,
                    close: day.close
                };
            } else {
                weeks[weekKey].high = Math.max(weeks[weekKey].high, day.high);
                weeks[weekKey].low = Math.min(weeks[weekKey].low, day.low);
                weeks[weekKey].close = day.close;
                weeks[weekKey].timestamp = day.timestamp;
                weeks[weekKey].date = day.date;
            }
        });

        return Object.values(weeks);
    }

    function getWeekNumber(date) {
        const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
        const dayNum = d.getUTCDay() || 7;
        d.setUTCDate(d.getUTCDate() + 4 - dayNum);
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
        return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    }

    // Note: fetchHistoricalData is handled through the main backtestEngine.runBacktest() call

    // ========= INITIALIZATION =========
    // Note: Event listener is now handled in main HTML file to avoid timing issues

    // ========= PUBLIC API =========
    const api = {
        runWeeklyBacktest,
        exportWeeklyCSV,
        exportWeeklyReport,
        computePriorWeekPivots,
        state
    };

    /* eslint-disable-next-line no-console */
    console.log("✅ Weekly Backtest Panel: Ready!");
    return api;

})();