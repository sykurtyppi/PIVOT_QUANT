/* ---------- enhanced_chart_engine.js ----------
   Clean pivot/ATR zone bands with proper layering
   Replaces full-height columns with focused zone visualization
-----------------------------------------------------*/

window.enhancedChartEngine = (() => {
    let chart, candleSeries, ema9Series, ema21Series;
    const zoneLayers = new Map(); // Organized layer management
    const pivotLines = new Map();

    // ========= CHART INITIALIZATION =========
    function create() {
        const container = document.getElementById("chartContainer");
        chart = LightweightCharts.createChart(container, {
            layout: {
                background: { color: "transparent" },
                textColor: "#b0bec5",
            },
            grid: {
                vertLines: { color: "rgba(255,255,255,0.03)" },
                horzLines: { color: "rgba(255,255,255,0.03)" },
            },
            timeScale: {
                borderColor: "rgba(255,255,255,0.1)",
                timeVisible: true,
                secondsVisible: false
            },
            rightPriceScale: {
                borderColor: "rgba(255,255,255,0.1)",
                autoScale: true,
                alignLabels: true
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: { color: 'rgba(224, 227, 235, 0.1)' },
                horzLine: { color: 'rgba(224, 227, 235, 0.1)' }
            },
            handleScroll: true,
            handleScale: true,
        });

        // Layer order: background zones → price data → pivot lines → EMAs
        createPriceSeries();
        createEMASeries();

        resize();
        window.addEventListener("resize", resize);
    }

    function createPriceSeries() {
        candleSeries = chart.addCandlestickSeries({
            upColor: "#66bb6a",
            downColor: "#ef5350",
            borderVisible: false,
            wickUpColor: "#66bb6a",
            wickDownColor: "#ef5350",
            priceLineVisible: false
        });
    }

    function createEMASeries() {
        ema9Series = chart.addLineSeries({
            color: "#ffffff",
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: "EMA 9"
        });

        ema21Series = chart.addLineSeries({
            color: "#cccccc",
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: "EMA 21"
        });
    }

    function resize() {
        if (!chart) return;
        const container = document.getElementById("chartContainer");
        chart.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight
        });
    }

    // ========= CLEAN ZONE BAND SYSTEM =========
    function updateChart(zones, inputs, timeframe) {
        if (!chart || !zones) return;

        clearAllLayers();

        const currentTime = Math.floor(Date.now() / 1000);
        const timeRange = getTimeRange(currentTime);

        // Render in proper layer order
        renderZoneBands(zones, timeRange, timeframe);
        renderPivotLines(zones, timeRange, timeframe);
        renderEMALines(inputs, timeRange);

        // Auto-fit to show all levels
        autoFitChart(zones, inputs);
    }

    function renderZoneBands(zones, timeRange, timeframe) {
        const zoneConfig = getZoneConfiguration(timeframe);

        Object.entries(zones).forEach(([levelName, zone]) => {
            const config = zoneConfig[levelName] || zoneConfig.default;

            // Skip zone bands that are too narrow (< 0.1% of price)
            const zoneWidth = Math.abs(zone.high - zone.low);
            const priceLevel = zone.value;
            if (zoneWidth / priceLevel < 0.001) return;

            createCleanZoneBand(levelName, zone, config, timeRange);
        });
    }

    function createCleanZoneBand(levelName, zone, config, timeRange) {
        // Create focused horizontal band (not full-height column)
        const zoneSeries = chart.addAreaSeries({
            lineColor: 'transparent',
            topColor: config.bandColor,
            bottomColor: config.bandColor,
            lineWidth: 0,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
            lastValueVisible: false
        });

        // Define precise zone boundaries
        const zoneData = [
            { time: timeRange.start, value: zone.high },
            { time: timeRange.end, value: zone.high }
        ];

        const zoneLowerSeries = chart.addAreaSeries({
            lineColor: 'transparent',
            topColor: 'transparent',
            bottomColor: config.bandColor,
            lineWidth: 0,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
            lastValueVisible: false
        });

        const zoneLowerData = [
            { time: timeRange.start, value: zone.low },
            { time: timeRange.end, value: zone.low }
        ];

        zoneSeries.setData(zoneData);
        zoneLowerSeries.setData(zoneLowerData);

        // Store for cleanup
        zoneLayers.set(`${levelName}_upper`, zoneSeries);
        zoneLayers.set(`${levelName}_lower`, zoneLowerSeries);
    }

    function renderPivotLines(zones, timeRange, timeframe) {
        const lineConfig = getPivotLineConfiguration(timeframe);

        Object.entries(zones).forEach(([levelName, zone]) => {
            const config = lineConfig[levelName] || lineConfig.default;

            const pivotLine = chart.addLineSeries({
                color: config.lineColor,
                lineWidth: config.lineWidth,
                lineStyle: config.lineStyle,
                priceLineVisible: true,
                lastValueVisible: true,
                title: `${levelName.toUpperCase()}: ${zone.value.toFixed(2)}`,
                crosshairMarkerVisible: true
            });

            const lineData = [
                { time: timeRange.start, value: zone.value },
                { time: timeRange.end, value: zone.value }
            ];

            pivotLine.setData(lineData);
            pivotLines.set(levelName, pivotLine);
        });
    }

    function renderEMALines(inputs, timeRange) {
        if (inputs.ema9 && Number.isFinite(inputs.ema9)) {
            const ema9Data = [
                { time: timeRange.start, value: inputs.ema9 },
                { time: timeRange.end, value: inputs.ema9 }
            ];
            ema9Series.setData(ema9Data);
        }

        if (inputs.ema21 && Number.isFinite(inputs.ema21)) {
            const ema21Data = [
                { time: timeRange.start, value: inputs.ema21 },
                { time: timeRange.end, value: inputs.ema21 }
            ];
            ema21Series.setData(ema21Data);
        }
    }

    // ========= CONFIGURATION SYSTEMS =========
    function getZoneConfiguration(timeframe) {
        const baseOpacity = timeframe === 'weekly' ? 0.12 : 0.08;

        return {
            R3: { bandColor: `rgba(239, 83, 80, ${baseOpacity * 0.6})` },    // Lightest resistance
            R2: { bandColor: `rgba(239, 83, 80, ${baseOpacity * 0.8})` },    // Medium resistance
            R1: { bandColor: `rgba(239, 83, 80, ${baseOpacity})` },          // Strong resistance
            PIVOT: { bandColor: `rgba(253, 216, 53, ${baseOpacity * 1.2})` }, // Pivot highlight
            S1: { bandColor: `rgba(102, 187, 106, ${baseOpacity})` },        // Strong support
            S2: { bandColor: `rgba(102, 187, 106, ${baseOpacity * 0.8})` },  // Medium support
            S3: { bandColor: `rgba(102, 187, 106, ${baseOpacity * 0.6})` },  // Lightest support
            default: { bandColor: `rgba(180, 180, 180, ${baseOpacity * 0.5})` }
        };
    }

    function getPivotLineConfiguration(timeframe) {
        const lineWidth = timeframe === 'weekly' ? 2 : 1;

        return {
            R3: { lineColor: 'rgba(239, 83, 80, 0.6)', lineWidth, lineStyle: 2 },
            R2: { lineColor: 'rgba(239, 83, 80, 0.8)', lineWidth, lineStyle: 0 },
            R1: { lineColor: 'rgba(239, 83, 80, 1.0)', lineWidth: lineWidth + 1, lineStyle: 0 },
            PIVOT: { lineColor: 'rgba(253, 216, 53, 1.0)', lineWidth: lineWidth + 1, lineStyle: 0 },
            S1: { lineColor: 'rgba(102, 187, 106, 1.0)', lineWidth: lineWidth + 1, lineStyle: 0 },
            S2: { lineColor: 'rgba(102, 187, 106, 0.8)', lineWidth, lineStyle: 0 },
            S3: { lineColor: 'rgba(102, 187, 106, 0.6)', lineWidth, lineStyle: 2 },
            default: { lineColor: 'rgba(180, 180, 180, 0.7)', lineWidth: 1, lineStyle: 1 }
        };
    }

    // ========= UTILITY FUNCTIONS =========
    function getTimeRange(currentTime) {
        return {
            start: currentTime - (24 * 3600), // 24 hours ago
            end: currentTime + (6 * 3600)     // 6 hours ahead
        };
    }

    function clearAllLayers() {
        // Clear zone bands
        zoneLayers.forEach(series => {
            try {
                chart.removeSeries(series);
            } catch (e) {
                /* eslint-disable-next-line no-console */
                console.warn('Could not remove zone series:', e);
            }
        });
        zoneLayers.clear();

        // Clear pivot lines
        pivotLines.forEach(series => {
            try {
                chart.removeSeries(series);
            } catch (e) {
                /* eslint-disable-next-line no-console */
                console.warn('Could not remove pivot series:', e);
            }
        });
        pivotLines.clear();
    }

    function autoFitChart(zones, inputs) {
        if (!zones || Object.keys(zones).length === 0) return;

        const allValues = Object.values(zones).flatMap(z => [z.high, z.low, z.value]);
        if (inputs.ema9) allValues.push(inputs.ema9);
        if (inputs.ema21) allValues.push(inputs.ema21);

        const min = Math.min(...allValues);
        const max = Math.max(...allValues);
        const _padding = (max - min) * 0.1; // 10% padding

        chart.applyOptions({
            rightPriceScale: {
                autoScale: false,
                scaleMargins: { top: 0.1, bottom: 0.1 }
            }
        });

        // Restore auto-scale after brief manual scale
        setTimeout(() => {
            chart.applyOptions({
                rightPriceScale: { autoScale: true }
            });
        }, 100);
    }

    // ========= DATA LOADING =========
    async function loadSPXData() {
        try {
            const corsUrls = [
                'https://query1.finance.yahoo.com/v8/finance/chart/^GSPC?range=1mo&interval=1d',
                'https://api.allorigins.win/raw?url=' + encodeURIComponent('https://query1.finance.yahoo.com/v8/finance/chart/^GSPC?range=1mo&interval=1d')
            ];

            let data = null;
            for (const url of corsUrls) {
                try {
                    const res = await fetch(url);
                    const json = await res.json();
                    if (json.chart?.result?.[0]) {
                        data = json;
                        break;
                    }
                } catch (e) {
                    /* eslint-disable-next-line no-console */
                    console.warn('URL failed:', url, e);
                }
            }

            if (!data) throw new Error('All data sources failed');

            const result = data.chart.result[0];
            const timestamps = result.timestamp;
            const ohlc = result.indicators.quote[0];

            const candles = timestamps.map((time, i) => ({
                time: time,
                open: ohlc.open[i],
                high: ohlc.high[i],
                low: ohlc.low[i],
                close: ohlc.close[i]
            })).filter(candle =>
                Number.isFinite(candle.open) &&
                Number.isFinite(candle.high) &&
                Number.isFinite(candle.low) &&
                Number.isFinite(candle.close)
            );

            candleSeries.setData(candles);

            // Calculate and display EMAs
            const closes = candles.map(c => c.close);
            const ema9Data = calculateEMA(closes, 9);
            const ema21Data = calculateEMA(closes, 21);

            const ema9SeriesData = timestamps.map((time, i) => ({
                time,
                value: ema9Data[i]
            })).filter(d => Number.isFinite(d.value));

            const ema21SeriesData = timestamps.map((time, i) => ({
                time,
                value: ema21Data[i]
            })).filter(d => Number.isFinite(d.value));

            ema9Series.setData(ema9SeriesData);
            ema21Series.setData(ema21SeriesData);

            // Auto-populate inputs with latest data
            const latest = candles[candles.length - 1];
            if (latest) {
                updateInputsFromData(latest, ema9Data[ema9Data.length - 1], ema21Data[ema21Data.length - 1]);
            }

        } catch (error) {
            /* eslint-disable-next-line no-console */
            console.warn('Enhanced chart engine data load failed, fallback system will handle:', error);
            // Don't show alert - let the fallback system in V3PIVOT_CALCULATOR.html handle this gracefully
            throw error; // Re-throw so the fallback system knows to take over
        }
    }

    function calculateEMA(values, period) {
        const k = 2 / (period + 1);
        const ema = [values[0]];

        for (let i = 1; i < values.length; i++) {
            ema.push(values[i] * k + ema[i - 1] * (1 - k));
        }

        return ema;
    }

    function updateInputsFromData(latestCandle, ema9, ema21) {
        const elements = {
            highInput: latestCandle.high,
            lowInput: latestCandle.low,
            closeInput: latestCandle.close,
            ema9Input: ema9,
            ema21Input: ema21
        };

        Object.entries(elements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element && Number.isFinite(value)) {
                element.value = value.toFixed(2);
            }
        });

        // Trigger pivot calculation update
        if (window.pivotCore && typeof window.pivotCore.updateFromUI === 'function') {
            window.pivotCore.updateFromUI();
        }
    }

    // ========= PUBLIC API =========
    return {
        create,
        updateChart,
        loadSPXData,
        resize,
        clearAllLayers
    };
})();

// Replace old chart engine when loaded
document.addEventListener('DOMContentLoaded', () => {
    // Override old chart engine
    if (window.chartEngine) {
        window.chartEngine = window.enhancedChartEngine;
    }

    setTimeout(() => {
        if (window.LightweightCharts && window.enhancedChartEngine) {
            window.enhancedChartEngine.create();
            setTimeout(() => window.enhancedChartEngine.loadSPXData(), 800);
        }
    }, 1000);
});