/**
 * PerformanceMonitor - Institutional-Grade Performance Analytics
 *
 * Comprehensive monitoring system for tracking engine performance,
 * resource utilization, and operational metrics in real-time.
 *
 * @version 2.0.0
 * @author PIVOT_QUANT Team
 */

export class PerformanceMonitor {
    constructor(config = {}) {
        this.config = {
            enableMetrics: config.enableMetrics !== false,
            enableProfiling: config.enableProfiling || false,
            metricsInterval: config.metricsInterval || 60000, // 1 minute
            maxSessionHistory: config.maxSessionHistory || 1000,
            performanceThresholds: {
                calculationTime: config.calculationTime || 1000, // ms
                memoryUsage: config.memoryUsage || 100 * 1024 * 1024, // 100MB
                cacheHitRate: config.cacheHitRate || 80, // percentage
                ...config.performanceThresholds
            },
            ...config
        };

        this.metrics = {
            sessions: new Map(),
            aggregated: this._initializeAggregatedMetrics(),
            system: this._initializeSystemMetrics(),
            cache: this._initializeCacheMetrics(),
            errors: this._initializeErrorMetrics()
        };

        this.timers = new Map();
        this.counters = new Map();
        this.profiler = this._initializeProfiler();

        if (this.config.enableMetrics) {
            this._startMetricsCollection();
        }
    }

    /**
     * Start a performance session
     * @param {string} sessionId - Unique session identifier
     * @param {Object} metadata - Session metadata
     * @returns {string} Session ID
     */
    startSession(sessionId = null, metadata = {}) {
        if (!this.config.enableMetrics) return null;

        const id = sessionId || this._generateSessionId();
        const session = {
            id,
            startTime: performance.now(),
            startTimestamp: Date.now(),
            metadata,
            checkpoints: [],
            memorySnapshots: [],
            status: 'active',
            endTime: null,
            duration: null,
            success: null,
            error: null
        };

        // Take initial memory snapshot
        if (this.config.enableProfiling) {
            session.memorySnapshots.push(this._captureMemorySnapshot());
        }

        this.metrics.sessions.set(id, session);
        this._updateCounter('sessions.started');

        return id;
    }

    /**
     * End a performance session
     * @param {string} sessionId - Session identifier
     * @param {Object} result - Session result data
     */
    endSession(sessionId, result = {}) {
        if (!this.config.enableMetrics || !sessionId) return;

        const session = this.metrics.sessions.get(sessionId);
        if (!session) {
            /* eslint-disable-next-line no-console */
            console.warn(`[PerformanceMonitor] Session not found: ${sessionId}`);
            return;
        }

        const endTime = performance.now();
        session.endTime = endTime;
        session.duration = endTime - session.startTime;
        session.status = 'completed';
        session.success = result.success !== false;
        session.error = result.error || null;

        // Take final memory snapshot
        if (this.config.enableProfiling) {
            session.memorySnapshots.push(this._captureMemorySnapshot());
        }

        // Update aggregated metrics
        this._updateAggregatedMetrics(session);

        // Check performance thresholds
        this._checkPerformanceThresholds(session);

        // Clean up old sessions
        this._cleanupSessions();

        this._updateCounter('sessions.completed');
    }

    /**
     * Add a checkpoint to an active session
     * @param {string} sessionId - Session identifier
     * @param {string} checkpointName - Checkpoint name
     * @param {Object} data - Checkpoint data
     */
    addCheckpoint(sessionId, checkpointName, data = {}) {
        if (!this.config.enableMetrics || !sessionId) return;

        const session = this.metrics.sessions.get(sessionId);
        if (!session || session.status !== 'active') return;

        const checkpoint = {
            name: checkpointName,
            time: performance.now(),
            relativeTime: performance.now() - session.startTime,
            data
        };

        session.checkpoints.push(checkpoint);
    }

    /**
     * Record cache hit
     * @param {string} sessionId - Session identifier
     */
    recordCacheHit(sessionId = null) {
        this._updateCounter('cache.hits');
        this.metrics.cache.totalHits++;

        if (sessionId) {
            this.addCheckpoint(sessionId, 'cache_hit');
        }
    }

    /**
     * Record cache miss
     * @param {string} sessionId - Session identifier
     */
    recordCacheMiss(sessionId = null) {
        this._updateCounter('cache.misses');
        this.metrics.cache.totalMisses++;

        if (sessionId) {
            this.addCheckpoint(sessionId, 'cache_miss');
        }
    }

    /**
     * Record an error
     * @param {string} context - Error context
     * @param {Error} error - Error object
     * @param {string} sessionId - Optional session identifier
     */
    recordError(context, error, sessionId = null) {
        this._updateCounter('errors.total');
        this._updateCounter(`errors.by_context.${context}`);

        const errorRecord = {
            context,
            message: error.message,
            stack: error.stack,
            timestamp: Date.now(),
            sessionId
        };

        this.metrics.errors.recent.push(errorRecord);

        // Keep only last 100 errors
        if (this.metrics.errors.recent.length > 100) {
            this.metrics.errors.recent.shift();
        }

        this.metrics.errors.byContext[context] = (this.metrics.errors.byContext[context] || 0) + 1;
    }

    /**
     * Get comprehensive metrics
     * @returns {Object} Current metrics
     */
    getMetrics() {
        const now = Date.now();

        return {
            timestamp: now,
            uptime: now - this.metrics.system.startTime,
            sessions: {
                active: Array.from(this.metrics.sessions.values()).filter(s => s.status === 'active').length,
                completed: Array.from(this.metrics.sessions.values()).filter(s => s.status === 'completed').length,
                total: this.metrics.sessions.size,
                averageDuration: this._calculateAverageDuration(),
                successRate: this._calculateSuccessRate()
            },
            performance: {
                averageCalculationTime: this.metrics.aggregated.totalDuration / Math.max(1, this.metrics.aggregated.totalSessions),
                peakCalculationTime: this.metrics.aggregated.peakDuration,
                throughput: this._calculateThroughput(),
                efficiency: this._calculateEfficiency()
            },
            cache: {
                hitRate: this.getCacheHitRate(),
                totalHits: this.metrics.cache.totalHits,
                totalMisses: this.metrics.cache.totalMisses,
                totalRequests: this.metrics.cache.totalHits + this.metrics.cache.totalMisses
            },
            memory: this.getMemoryUsage(),
            errors: {
                total: this.counters.get('errors.total') || 0,
                rate: this._calculateErrorRate(),
                byContext: { ...this.metrics.errors.byContext },
                recent: this.metrics.errors.recent.slice(-10) // Last 10 errors
            },
            counters: Object.fromEntries(this.counters.entries()),
            health: this._calculateHealthScore()
        };
    }

    /**
     * Get cache hit rate percentage
     * @returns {number} Cache hit rate percentage
     */
    getCacheHitRate() {
        const totalRequests = this.metrics.cache.totalHits + this.metrics.cache.totalMisses;
        if (totalRequests === 0) return 0;
        return Math.round((this.metrics.cache.totalHits / totalRequests) * 100 * 100) / 100;
    }

    /**
     * Get memory usage information
     * @returns {Object} Memory usage data
     */
    getMemoryUsage() {
        if (typeof performance !== 'undefined' && performance.memory) {
            return {
                used: performance.memory.usedJSHeapSize,
                total: performance.memory.totalJSHeapSize,
                limit: performance.memory.jsHeapSizeLimit,
                percentage: Math.round((performance.memory.usedJSHeapSize / performance.memory.totalJSHeapSize) * 100)
            };
        }

        // Fallback for environments without performance.memory
        return {
            used: null,
            total: null,
            limit: null,
            percentage: null,
            available: false
        };
    }

    /**
     * Get session history
     * @param {Object} filters - Optional filters
     * @returns {Array} Session history
     */
    getSessionHistory(filters = {}) {
        const sessions = Array.from(this.metrics.sessions.values());

        let filtered = sessions;

        if (filters.status) {
            filtered = filtered.filter(s => s.status === filters.status);
        }

        if (filters.success !== undefined) {
            filtered = filtered.filter(s => s.success === filters.success);
        }

        if (filters.minDuration !== undefined) {
            filtered = filtered.filter(s => s.duration >= filters.minDuration);
        }

        if (filters.maxDuration !== undefined) {
            filtered = filtered.filter(s => s.duration <= filters.maxDuration);
        }

        return filtered
            .sort((a, b) => b.startTimestamp - a.startTimestamp)
            .slice(0, filters.limit || 100);
    }

    /**
     * Generate performance report
     * @param {Object} options - Report options
     * @returns {Object} Performance report
     */
    generateReport(options = {}) {
        const {
            includeSessions = true,
            includeErrors = true,
            includeSystemInfo = true,
            timeRange = 3600000 // 1 hour default
        } = options;

        const now = Date.now();
        const cutoffTime = now - timeRange;

        const report = {
            generated: new Date(now).toISOString(),
            timeRange: { start: new Date(cutoffTime).toISOString(), end: new Date(now).toISOString() },
            summary: this.getMetrics()
        };

        if (includeSessions) {
            report.sessions = this.getSessionHistory({
                limit: 1000
            }).filter(s => s.startTimestamp >= cutoffTime);
        }

        if (includeErrors) {
            report.errors = this.metrics.errors.recent.filter(e => e.timestamp >= cutoffTime);
        }

        if (includeSystemInfo) {
            report.system = {
                userAgent: typeof navigator !== 'undefined' ? navigator.userAgent : 'Unknown',
                platform: typeof navigator !== 'undefined' ? navigator.platform : 'Unknown',
                memory: this.getMemoryUsage(),
                timestamp: now
            };
        }

        return report;
    }

    /**
     * Reset all metrics
     */
    reset() {
        this.metrics.sessions.clear();
        this.metrics.aggregated = this._initializeAggregatedMetrics();
        this.metrics.cache = this._initializeCacheMetrics();
        this.metrics.errors = this._initializeErrorMetrics();
        this.counters.clear();
        this.timers.clear();
    }

    /**
     * Dispose of the performance monitor
     */
    dispose() {
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
        }

        this.reset();
    }

    // =================================================================================
    // PRIVATE METHODS
    // =================================================================================

    _initializeAggregatedMetrics() {
        return {
            totalSessions: 0,
            successfulSessions: 0,
            totalDuration: 0,
            peakDuration: 0,
            averageDuration: 0,
            throughputSamples: []
        };
    }

    _initializeSystemMetrics() {
        return {
            startTime: Date.now(),
            lastActivity: Date.now(),
            cpuSamples: [],
            memorySamples: []
        };
    }

    _initializeCacheMetrics() {
        return {
            totalHits: 0,
            totalMisses: 0,
            hitRateSamples: []
        };
    }

    _initializeErrorMetrics() {
        return {
            byContext: {},
            recent: []
        };
    }

    _initializeProfiler() {
        if (!this.config.enableProfiling) return null;

        return {
            marks: new Map(),
            measures: new Map()
        };
    }

    _generateSessionId() {
        return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    _updateCounter(name, increment = 1) {
        const current = this.counters.get(name) || 0;
        this.counters.set(name, current + increment);
    }

    _updateAggregatedMetrics(session) {
        this.metrics.aggregated.totalSessions++;

        if (session.success) {
            this.metrics.aggregated.successfulSessions++;
        }

        this.metrics.aggregated.totalDuration += session.duration;

        if (session.duration > this.metrics.aggregated.peakDuration) {
            this.metrics.aggregated.peakDuration = session.duration;
        }

        // Update average
        this.metrics.aggregated.averageDuration =
            this.metrics.aggregated.totalDuration / this.metrics.aggregated.totalSessions;

        // Update throughput samples (sessions per minute)
        const now = Date.now();
        this.metrics.aggregated.throughputSamples.push({
            timestamp: now,
            sessionCount: 1
        });

        // Keep only last hour of samples
        const cutoff = now - 3600000;
        this.metrics.aggregated.throughputSamples =
            this.metrics.aggregated.throughputSamples.filter(sample => sample.timestamp >= cutoff);
    }

    _checkPerformanceThresholds(session) {
        const thresholds = this.config.performanceThresholds;

        // Check calculation time threshold
        if (session.duration > thresholds.calculationTime) {
            /* eslint-disable-next-line no-console */
            console.warn(`[PerformanceMonitor] Slow calculation detected: ${session.duration.toFixed(2)}ms (threshold: ${thresholds.calculationTime}ms)`);
        }

        // Check memory usage
        const memory = this.getMemoryUsage();
        if (memory.used && memory.used > thresholds.memoryUsage) {
            /* eslint-disable-next-line no-console */
            console.warn(`[PerformanceMonitor] High memory usage: ${(memory.used / 1024 / 1024).toFixed(2)}MB`);
        }

        // Check cache hit rate
        const hitRate = this.getCacheHitRate();
        if (hitRate < thresholds.cacheHitRate) {
            /* eslint-disable-next-line no-console */
            console.warn(`[PerformanceMonitor] Low cache hit rate: ${hitRate.toFixed(2)}% (threshold: ${thresholds.cacheHitRate}%)`);
        }
    }

    _cleanupSessions() {
        if (this.metrics.sessions.size <= this.config.maxSessionHistory) return;

        // Remove oldest completed sessions
        const sessions = Array.from(this.metrics.sessions.entries())
            .filter(([_, session]) => session.status === 'completed')
            .sort(([_, a], [__, b]) => a.endTime - b.endTime);

        const toRemove = sessions.slice(0, sessions.length - this.config.maxSessionHistory + 50);

        toRemove.forEach(([id]) => {
            this.metrics.sessions.delete(id);
        });
    }

    _captureMemorySnapshot() {
        const memory = this.getMemoryUsage();

        return {
            timestamp: Date.now(),
            used: memory.used,
            total: memory.total,
            percentage: memory.percentage
        };
    }

    _calculateAverageDuration() {
        const completedSessions = Array.from(this.metrics.sessions.values())
            .filter(s => s.status === 'completed' && s.duration !== null);

        if (completedSessions.length === 0) return 0;

        const totalDuration = completedSessions.reduce((sum, session) => sum + session.duration, 0);
        return totalDuration / completedSessions.length;
    }

    _calculateSuccessRate() {
        const completedSessions = Array.from(this.metrics.sessions.values())
            .filter(s => s.status === 'completed' && s.success !== null);

        if (completedSessions.length === 0) return 100;

        const successfulSessions = completedSessions.filter(s => s.success === true);
        return (successfulSessions.length / completedSessions.length) * 100;
    }

    _calculateThroughput() {
        const samples = this.metrics.aggregated.throughputSamples;
        if (samples.length === 0) return 0;

        const now = Date.now();
        const oneMinuteAgo = now - 60000;

        const recentSamples = samples.filter(sample => sample.timestamp >= oneMinuteAgo);
        return recentSamples.length; // Sessions per minute
    }

    _calculateEfficiency() {
        const avgDuration = this._calculateAverageDuration();
        const cacheHitRate = this.getCacheHitRate();
        const successRate = this._calculateSuccessRate();

        // Efficiency score based on speed, cache performance, and reliability
        const speedScore = Math.max(0, 100 - (avgDuration / 10)); // Assumes 1000ms = 0 points
        const cacheScore = cacheHitRate;
        const reliabilityScore = successRate;

        return (speedScore * 0.4 + cacheScore * 0.3 + reliabilityScore * 0.3);
    }

    _calculateErrorRate() {
        const totalErrors = this.counters.get('errors.total') || 0;
        const totalSessions = this.metrics.aggregated.totalSessions;

        if (totalSessions === 0) return 0;
        return (totalErrors / totalSessions) * 100;
    }

    _calculateHealthScore() {
        const metrics = {
            efficiency: this._calculateEfficiency(),
            cacheHitRate: this.getCacheHitRate(),
            successRate: this._calculateSuccessRate(),
            errorRate: this._calculateErrorRate()
        };

        // Health score calculation
        const efficiencyScore = Math.min(100, metrics.efficiency);
        const cacheScore = metrics.cacheHitRate;
        const successScore = metrics.successRate;
        const errorScore = Math.max(0, 100 - (metrics.errorRate * 10)); // 10% error rate = 0 score

        const healthScore = (efficiencyScore * 0.3 + cacheScore * 0.2 + successScore * 0.3 + errorScore * 0.2);

        let healthStatus;
        if (healthScore >= 90) healthStatus = 'excellent';
        else if (healthScore >= 75) healthStatus = 'good';
        else if (healthScore >= 50) healthStatus = 'fair';
        else healthStatus = 'poor';

        return {
            score: Math.round(healthScore),
            status: healthStatus,
            components: metrics
        };
    }

    _startMetricsCollection() {
        this.metricsInterval = setInterval(() => {
            this._collectSystemMetrics();
        }, this.config.metricsInterval);
    }

    _collectSystemMetrics() {
        const now = Date.now();
        this.metrics.system.lastActivity = now;

        // Collect memory samples
        const memory = this.getMemoryUsage();
        if (memory.used !== null) {
            this.metrics.system.memorySamples.push({
                timestamp: now,
                used: memory.used,
                percentage: memory.percentage
            });

            // Keep only last hour of samples
            const cutoff = now - 3600000;
            this.metrics.system.memorySamples =
                this.metrics.system.memorySamples.filter(sample => sample.timestamp >= cutoff);
        }

        // Collect cache hit rate samples
        const hitRate = this.getCacheHitRate();
        this.metrics.cache.hitRateSamples.push({
            timestamp: now,
            hitRate: hitRate
        });

        // Keep only last hour of samples
        const cutoff = now - 3600000;
        this.metrics.cache.hitRateSamples =
            this.metrics.cache.hitRateSamples.filter(sample => sample.timestamp >= cutoff);
    }
}
