/* ---------- enhanced_data_fetcher.js ----------
   Professional-grade data fetching with robust error handling,
   rate limiting, caching, and multiple asset support
   ------------------------------------------------*/

class EnhancedDataFetcher {
    constructor() {
        // API Configuration
        this.config = {
            finnhub: {
                key: "d20nlk1r01qvvf1j7l9gd20nlk1r01qvvf1j7la0",
                baseUrl: "https://finnhub.io/api/v1",
                rateLimitPerMinute: 60
            },
            alpha: {
                key: "IG1WEQHD1ZP1Q7X1",
                baseUrl: "https://www.alphavantage.co/query",
                rateLimitPerMinute: 5
            },
            yahoo: {
                baseUrl: "https://query1.finance.yahoo.com/v8/finance/chart",
                rateLimitPerMinute: 2000
            }
        };

        // Asset symbols mapping
        this.symbols = {
            SPX: {
                yahoo: ['^GSPC', 'US500USD=X', '^SPX'],
                finnhub: 'SPY',
                alpha: 'SPY',
                name: 'S&P 500'
            },
            NDX: {
                yahoo: ['^NDX', '^IXIC'],
                finnhub: 'QQQ',
                alpha: 'QQQ',
                name: 'NASDAQ 100'
            },
            DJI: {
                yahoo: ['^DJI'],
                finnhub: 'DIA',
                alpha: 'DIA',
                name: 'Dow Jones'
            },
            ES: {
                yahoo: ['ES=F'],
                finnhub: 'ES',
                alpha: 'SPY',
                name: 'E-mini S&P 500'
            },
            NQ: {
                yahoo: ['NQ=F'],
                finnhub: 'NQ',
                alpha: 'QQQ',
                name: 'E-mini NASDAQ'
            },
            YM: {
                yahoo: ['YM=F'],
                finnhub: 'YM',
                alpha: 'DIA',
                name: 'E-mini Dow'
            }
        };

        // State management
        this.cache = new Map();
        this.requestCounts = new Map();
        this.lastRequestTime = new Map();
        this.currentAsset = 'SPX';
        this.isLive = false;
        this.retryCount = 0;
        this.maxRetries = 3;

        // CORS proxies with reliability scoring
        this.corsProxies = [
            {
                url: (u) => `https://api.allorigins.win/raw?url=${encodeURIComponent(u)}`,
                reliability: 0.8,
                name: 'AllOrigins'
            },
            {
                url: (u) => `https://thingproxy.freeboard.io/fetch/${u}`,
                reliability: 0.6,
                name: 'ThingProxy'
            },
            {
                url: (u) => `https://cors-anywhere.herokuapp.com/${u}`,
                reliability: 0.4,
                name: 'CORS Anywhere'
            }
        ];

        this.initializeEventHandlers();
    }

    // ========= PUBLIC API =========
    async fetchAssetData(assetSymbol = null, timeRange = '3mo') {
        const asset = assetSymbol || this.currentAsset;

        try {
            this.updateStatus('fetching', `Loading ${this.symbols[asset]?.name || asset} data...`);

            // Check cache first
            const cacheKey = `${asset}_${timeRange}`;
            const cached = this.getCachedData(cacheKey);
            if (cached) {
                this.updateStatus('success', 'Data loaded from cache');
                return cached;
            }

            // Fetch with fallback strategy
            const data = await this.fetchWithFallbacks(asset, timeRange);

            // Cache successful result
            this.setCachedData(cacheKey, data);

            this.updateStatus('success', `${this.symbols[asset]?.name || asset} data loaded`);
            return data;

        } catch (error) {
            /* eslint-disable-next-line no-console */
            console.error('Data fetch failed:', error);
            this.updateStatus('error', error.message);
            throw error;
        }
    }

    async fetchLiveData(assetSymbol = null) {
        if (this.isLive) return; // Prevent multiple live sessions

        const asset = assetSymbol || this.currentAsset;
        this.isLive = true;

        try {
            while (this.isLive) {
                await this.fetchAssetData(asset, '1d');
                await this.sleep(60000); // 1 minute intervals
            }
        } catch (error) {
            /* eslint-disable-next-line no-console */
            console.error('Live data error:', error);
            this.stopLiveData();
        }
    }

    stopLiveData() {
        this.isLive = false;
        this.updateStatus('idle', 'Live data stopped');
    }

    setAsset(assetSymbol) {
        if (!this.symbols[assetSymbol]) {
            throw new Error(`Unsupported asset: ${assetSymbol}`);
        }
        this.currentAsset = assetSymbol;
        this.updateStatus('idle', `Asset changed to ${this.symbols[assetSymbol].name}`);
    }

    // ========= FETCH STRATEGIES =========
    async fetchWithFallbacks(asset, timeRange) {
        const strategies = [
            () => this.fetchFromYahoo(asset, timeRange),
            () => this.fetchFromFinnhub(asset, timeRange),
            () => this.fetchFromAlpha(asset, timeRange)
        ];

        let lastError;

        for (const [index, strategy] of strategies.entries()) {
            try {
                if (this.canMakeRequest(`strategy_${index}`)) {
                    const data = await strategy();
                    this.retryCount = 0;
                    return data;
                }
            } catch (error) {
                lastError = error;
                /* eslint-disable-next-line no-console */
                console.warn(`Strategy ${index} failed:`, error.message);

                // Exponential backoff for retries
                if (this.retryCount < this.maxRetries) {
                    await this.sleep(Math.pow(2, this.retryCount) * 1000);
                    this.retryCount++;
                }
            }
        }

        throw new Error(`All data sources failed. Last error: ${lastError?.message || 'Unknown error'}`);
    }

    async fetchFromYahoo(asset, timeRange) {
        const symbols = this.symbols[asset].yahoo;

        for (const symbol of symbols) {
            try {
                const url = `${this.config.yahoo.baseUrl}/${encodeURIComponent(symbol)}?range=${timeRange}&interval=1d`;
                const data = await this.fetchWithProxy(url);

                if (!data?.chart?.result?.[0]) {
                    throw new Error('Invalid Yahoo response structure');
                }

                return this.processYahooData(data, asset);

            } catch (error) {
                /* eslint-disable-next-line no-console */
                console.warn(`Yahoo fetch failed for ${symbol}:`, error);
                if (symbol === symbols[symbols.length - 1]) throw error;
            }
        }
    }

    async fetchFromFinnhub(asset, timeRange) {
        if (!this.config.finnhub.key || this.config.finnhub.key.includes('INSERT')) {
            throw new Error('Finnhub API key not configured');
        }

        const symbol = this.symbols[asset].finnhub;
        const { from, to } = this.getTimeRange(timeRange);

        const url = `${this.config.finnhub.baseUrl}/stock/candle?symbol=${symbol}&resolution=D&from=${from}&to=${to}&token=${this.config.finnhub.key}`;

        const data = await this.fetchJSON(url);

        if (data.s !== 'ok') {
            throw new Error(`Finnhub error: ${data.s}`);
        }

        return this.processFinnhubData(data, asset);
    }

    async fetchFromAlpha(asset, _timeRange) {
        if (!this.config.alpha.key || this.config.alpha.key.includes('INSERT')) {
            throw new Error('Alpha Vantage API key not configured');
        }

        const symbol = this.symbols[asset].alpha;
        const url = `${this.config.alpha.baseUrl}?function=TIME_SERIES_DAILY_ADJUSTED&symbol=${symbol}&outputsize=compact&apikey=${this.config.alpha.key}`;

        const data = await this.fetchJSON(url);

        if (data['Error Message'] || data['Note'] || !data['Time Series (Daily)']) {
            throw new Error('Alpha Vantage API limit or error');
        }

        return this.processAlphaData(data, asset);
    }

    // ========= DATA PROCESSING =========
    processYahooData(data, asset) {
        const result = data.chart.result[0];
        const timestamps = result.timestamp;
        const quote = result.indicators.quote[0];

        const candles = timestamps.map((time, i) => ({
            timestamp: time,
            date: new Date(time * 1000).toISOString().slice(0, 10),
            open: quote.open[i],
            high: quote.high[i],
            low: quote.low[i],
            close: quote.close[i],
            volume: quote.volume?.[i] || 0
        })).filter(candle =>
            Number.isFinite(candle.open) &&
            Number.isFinite(candle.high) &&
            Number.isFinite(candle.low) &&
            Number.isFinite(candle.close)
        );

        return this.enrichData(candles, asset);
    }

    processFinnhubData(data, asset) {
        const candles = data.t.map((time, i) => ({
            timestamp: time,
            date: new Date(time * 1000).toISOString().slice(0, 10),
            open: data.o[i],
            high: data.h[i],
            low: data.l[i],
            close: data.c[i],
            volume: data.v[i]
        }));

        return this.enrichData(candles, asset);
    }

    processAlphaData(data, asset) {
        const timeSeries = data['Time Series (Daily)'];
        const candles = Object.entries(timeSeries).map(([date, values]) => ({
            timestamp: Math.floor(new Date(date).getTime() / 1000),
            date: date,
            open: parseFloat(values['1. open']),
            high: parseFloat(values['2. high']),
            low: parseFloat(values['3. low']),
            close: parseFloat(values['4. close']),
            volume: parseInt(values['6. volume'])
        })).reverse();

        return this.enrichData(candles, asset);
    }

    enrichData(candles, asset) {
        if (candles.length < 21) {
            throw new Error('Insufficient historical data');
        }

        // Calculate technical indicators
        const closes = candles.map(c => c.close);
        const highs = candles.map(c => c.high);
        const lows = candles.map(c => c.low);

        const atr = this.calculateATR(highs, lows, closes, 14);
        const ema9 = this.calculateEMA(closes, 9);
        const ema21 = this.calculateEMA(closes, 21);
        const rsi = this.calculateRSI(closes, 14);
        const bb = this.calculateBollingerBands(closes, 20, 2);

        const latest = candles[candles.length - 1];

        return {
            asset,
            symbol: this.symbols[asset],
            candles,
            latest: {
                ...latest,
                atr: atr[atr.length - 1],
                ema9: ema9[ema9.length - 1],
                ema21: ema21[ema21.length - 1],
                rsi: rsi[rsi.length - 1],
                bollinger: bb[bb.length - 1]
            },
            indicators: {
                atr,
                ema9,
                ema21,
                rsi,
                bollingerBands: bb
            },
            metadata: {
                fetchTime: new Date().toISOString(),
                dataPoints: candles.length,
                timeRange: `${candles[0].date} to ${latest.date}`
            }
        };
    }

    // ========= TECHNICAL INDICATORS =========
    calculateATR(highs, lows, closes, period = 14) {
        const trueRanges = [];

        for (let i = 1; i < highs.length; i++) {
            const high = highs[i];
            const low = lows[i];
            const prevClose = closes[i - 1];

            const tr = Math.max(
                high - low,
                Math.abs(high - prevClose),
                Math.abs(low - prevClose)
            );
            trueRanges.push(tr);
        }

        return this.calculateSMA(trueRanges, period);
    }

    calculateEMA(values, period) {
        const k = 2 / (period + 1);
        const ema = [values[0]];

        for (let i = 1; i < values.length; i++) {
            ema.push(values[i] * k + ema[i - 1] * (1 - k));
        }

        return ema;
    }

    calculateSMA(values, period) {
        const sma = [];

        for (let i = period - 1; i < values.length; i++) {
            const sum = values.slice(i - period + 1, i + 1).reduce((a, b) => a + b, 0);
            sma.push(sum / period);
        }

        return sma;
    }

    calculateRSI(values, period = 14) {
        const changes = [];
        for (let i = 1; i < values.length; i++) {
            changes.push(values[i] - values[i - 1]);
        }

        const gains = changes.map(c => c > 0 ? c : 0);
        const losses = changes.map(c => c < 0 ? Math.abs(c) : 0);

        const avgGains = this.calculateSMA(gains, period);
        const avgLosses = this.calculateSMA(losses, period);

        return avgGains.map((gain, i) => {
            const loss = avgLosses[i];
            const rs = loss === 0 ? 100 : gain / loss;
            return 100 - (100 / (1 + rs));
        });
    }

    calculateBollingerBands(values, period = 20, multiplier = 2) {
        const sma = this.calculateSMA(values, period);
        const bands = [];

        for (let i = 0; i < sma.length; i++) {
            const slice = values.slice(i, i + period);
            const mean = sma[i];
            const variance = slice.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / period;
            const stdDev = Math.sqrt(variance);

            bands.push({
                upper: mean + (stdDev * multiplier),
                middle: mean,
                lower: mean - (stdDev * multiplier)
            });
        }

        return bands;
    }

    // ========= UTILITY FUNCTIONS =========
    async fetchWithProxy(url) {
        // Try direct fetch first
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (directError) {
            /* eslint-disable-next-line no-console */
            console.warn('Direct fetch failed, trying proxies...');
        }

        // Try proxies in order of reliability
        const sortedProxies = [...this.corsProxies].sort((a, b) => b.reliability - a.reliability);

        for (const proxy of sortedProxies) {
            try {
                const proxyUrl = proxy.url(url);
                const response = await fetch(proxyUrl);
                if (!response.ok) throw new Error(`Proxy HTTP ${response.status}`);

                const data = await response.json();

                // Update proxy reliability on success
                proxy.reliability = Math.min(1.0, proxy.reliability + 0.1);

                return data;
            } catch (error) {
                /* eslint-disable-next-line no-console */
                console.warn(`Proxy ${proxy.name} failed:`, error);
                // Decrease proxy reliability on failure
                proxy.reliability = Math.max(0.1, proxy.reliability - 0.1);
            }
        }

        throw new Error('All proxy attempts failed');
    }

    async fetchJSON(url) {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return await response.json();
    }

    // ========= RATE LIMITING =========
    canMakeRequest(apiKey) {
        const now = Date.now();
        const lastRequest = this.lastRequestTime.get(apiKey) || 0;
        const requestCount = this.requestCounts.get(apiKey) || 0;

        // Reset counter every minute
        if (now - lastRequest > 60000) {
            this.requestCounts.set(apiKey, 0);
        }

        const limit = this.config[apiKey]?.rateLimitPerMinute || 60;

        if (requestCount >= limit) {
            throw new Error(`Rate limit exceeded for ${apiKey}`);
        }

        this.requestCounts.set(apiKey, requestCount + 1);
        this.lastRequestTime.set(apiKey, now);

        return true;
    }

    // ========= CACHING =========
    getCachedData(key) {
        const cached = this.cache.get(key);
        if (!cached) return null;

        const age = Date.now() - cached.timestamp;
        const maxAge = 5 * 60 * 1000; // 5 minutes

        if (age > maxAge) {
            this.cache.delete(key);
            return null;
        }

        return cached.data;
    }

    setCachedData(key, data) {
        this.cache.set(key, {
            data,
            timestamp: Date.now()
        });

        // Prevent memory leaks - limit cache size
        if (this.cache.size > 50) {
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
        }
    }

    // ========= HELPERS =========
    getTimeRange(timeRange) {
        const now = Math.floor(Date.now() / 1000);
        const ranges = {
            '1d': now - 86400,
            '1w': now - (7 * 86400),
            '1mo': now - (30 * 86400),
            '3mo': now - (90 * 86400),
            '6mo': now - (180 * 86400),
            '1y': now - (365 * 86400)
        };

        return {
            from: ranges[timeRange] || ranges['3mo'],
            to: now
        };
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    updateStatus(type, message) {
        const event = new CustomEvent('dataFetcherStatus', {
            detail: { type, message, timestamp: new Date().toISOString() }
        });
        document.dispatchEvent(event);

        // Update UI elements if they exist
        const statusEl = document.getElementById('volStatus');
        const labelEl = document.getElementById('volLabel');

        if (statusEl && labelEl) {
            labelEl.textContent = `SRC: ${this.symbols[this.currentAsset]?.name || this.currentAsset}`;
            statusEl.textContent = message;

            const colors = {
                'success': 'var(--accent-green)',
                'error': 'var(--accent-red)',
                'fetching': 'var(--accent-blue)',
                'idle': 'var(--text-secondary)'
            };

            statusEl.style.color = colors[type] || colors.idle;
        }
    }

    initializeEventHandlers() {
        // Integration with existing UI
        document.addEventListener('DOMContentLoaded', () => {
            const fetchButtons = {
                'fetchYahoo': () => this.fetchAssetData('SPX'),
                'fetchAlpha': () => this.fetchAssetData('SPX'),
                'fetchLive': () => this.fetchLiveData(),
                'stopLive': () => this.stopLiveData()
            };

            Object.entries(fetchButtons).forEach(([id, handler]) => {
                const btn = document.getElementById(id);
                if (btn) {
                    btn.addEventListener('click', async () => {
                        try {
                            await handler();
                        } catch (error) {
                            /* eslint-disable-next-line no-console */
                            console.error(`${id} failed:`, error);
                            alert(`${id} failed: ${error.message}`);
                        }
                    });
                }
            });

            // Asset selector if it exists
            const assetSelector = document.getElementById('assetSelector');
            if (assetSelector) {
                assetSelector.addEventListener('change', (e) => {
                    this.setAsset(e.target.value);
                });
            }
        });
    }
}

// Global instance
window.enhancedDataFetcher = new EnhancedDataFetcher();

// Backward compatibility
window.dataFetcher = window.enhancedDataFetcher;