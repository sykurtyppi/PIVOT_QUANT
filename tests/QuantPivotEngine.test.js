/**
 * Comprehensive Test Suite for QuantPivotEngine
 *
 * Institutional-grade testing covering mathematical precision,
 * performance benchmarks, and edge case handling.
 */

import { QuantPivotEngine } from '../src/core/QuantPivotEngine.js';
import { ConfigurationManager } from '../src/config/ConfigurationManager.js';

describe('QuantPivotEngine - Institutional Testing Suite', () => {
    let engine;
    let testConfig;
    let mockOHLCData;

    beforeEach(() => {
        testConfig = ConfigurationManager.mergeWithDefaults({}, 'testing');
        engine = new QuantPivotEngine(testConfig);
        mockOHLCData = generateMockOHLCData(100);
    });

    afterEach(() => {
        engine.dispose();
    });

    describe('Engine Initialization', () => {
        test('should initialize with default configuration', () => {
            const defaultEngine = new QuantPivotEngine();
            expect(defaultEngine.state.isInitialized).toBe(true);
            defaultEngine.dispose();
        });

        test('should validate configuration during initialization', () => {
            const invalidConfig = {
                mathematical: { precision: -1 } // Invalid precision
            };

            expect(() => {
                new QuantPivotEngine(invalidConfig);
            }).toThrow('QuantPivotEngine initialization failed');
        });

        test('should initialize with custom configuration', () => {
            const customConfig = {
                mathematical: { precision: 10 },
                performance: { maxCacheSize: 200 }
            };

            const customEngine = new QuantPivotEngine(customConfig);
            expect(customEngine.config.mathematical.precision).toBe(10);
            expect(customEngine.config.performance.maxCacheSize).toBe(200);
            customEngine.dispose();
        });
    });

    describe('Mathematical Precision Tests', () => {
        test('should calculate standard pivots with institutional precision', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData, {
                type: 'standard',
                methods: ['standard']
            });

            expect(results.levels.standard.PP).toBeDefined();
            expect(results.levels.standard.R1).toBeDefined();
            expect(results.levels.standard.S1).toBeDefined();

            // Verify mathematical relationships
            expect(results.levels.standard.R1).toBeGreaterThan(results.levels.standard.PP);
            expect(results.levels.standard.S1).toBeLessThan(results.levels.standard.PP);
        });

        test('should maintain precision across multiple calculations', async () => {
            const results1 = await engine.calculatePivotLevels(mockOHLCData);
            const results2 = await engine.calculatePivotLevels(mockOHLCData);

            // Results should be identical for same input
            expect(results1.levels.standard.PP).toBe(results2.levels.standard.PP);
            expect(results1.levels.standard.R1).toBe(results2.levels.standard.R1);
        });

        test('should handle extreme price values', async () => {
            const extremeData = [
                { high: 0.02, low: 0.01, close: 0.015 },
                { high: 999999, low: 999999, close: 999999 }
            ];

            await expect(
                engine.calculatePivotLevels(extremeData)
            ).resolves.toBeDefined();
        });

        test('should calculate all pivot methodologies consistently', async () => {
            const methods = ['standard', 'fibonacci', 'camarilla', 'woodie', 'demark'];
            const results = await engine.calculatePivotLevels(mockOHLCData, { methods });

            methods.forEach(method => {
                expect(results.levels[method]).toBeDefined();
                expect(results.levels[method].PP).toBeDefined();
            });
        });
    });

    describe('Performance Benchmarks', () => {
        test('should complete calculation within performance threshold', async () => {
            const startTime = performance.now();
            await engine.calculatePivotLevels(mockOHLCData);
            const endTime = performance.now();

            const duration = endTime - startTime;
            expect(duration).toBeLessThan(testConfig.performance.performanceThresholds.calculationTime);
        });

        test('should handle large datasets efficiently', async () => {
            const maxDataPoints = engine.config.validation.maxDataPoints;
            const largeDataset = generateMockOHLCData(maxDataPoints);
            const startTime = performance.now();

            await engine.calculatePivotLevels(largeDataset);
            const endTime = performance.now();

            expect(endTime - startTime).toBeLessThan(5000); // 5 second max for large dataset
        });

        test('should utilize cache effectively', async () => {
            const data = generateMockOHLCData(50);

            // First calculation
            await engine.calculatePivotLevels(data);

            // Second identical calculation should be faster
            const startTime = performance.now();
            await engine.calculatePivotLevels(data);
            const endTime = performance.now();

            expect(endTime - startTime).toBeLessThan(100); // Should be very fast from cache
        });

        test('should maintain memory efficiency', async () => {
            const initialMemory = engine.getEngineStatus().memory;

            // Perform multiple calculations
            for (let i = 0; i < 10; i++) {
                const data = generateMockOHLCData(100);
                await engine.calculatePivotLevels(data);
            }

            const finalMemory = engine.getEngineStatus().memory;

            // Memory growth should be reasonable
            if (finalMemory.used && initialMemory.used) {
                const memoryGrowth = finalMemory.used - initialMemory.used;
                expect(memoryGrowth).toBeLessThan(10 * 1024 * 1024); // Less than 10MB growth
            }
        });
    });

    describe('Validation and Error Handling', () => {
        test('should reject invalid OHLC data', async () => {
            const invalidData = [
                { high: 100, low: 110, close: 105 } // High < Low
            ];

            await expect(
                engine.calculatePivotLevels(invalidData)
            ).rejects.toThrow('Data validation failed');
        });

        test('should handle insufficient data gracefully', async () => {
            const insufficientData = [
                { high: 100, low: 90, close: 95 }
            ];

            await expect(
                engine.calculatePivotLevels(insufficientData)
            ).rejects.toThrow('Insufficient data');
        });

        test('should validate calculation options', async () => {
            const invalidOptions = {
                atrPeriod: -1,
                methods: ['invalid_method']
            };

            await expect(
                engine.calculatePivotLevels(mockOHLCData, invalidOptions)
            ).rejects.toThrow();
        });

        test('should recover from calculation errors', async () => {
            // Force an error condition
            const corruptedData = mockOHLCData.map(bar => ({
                ...bar,
                high: NaN
            }));

            await expect(
                engine.calculatePivotLevels(corruptedData)
            ).rejects.toThrow();

            // Engine should still work with valid data after error
            const validResults = await engine.calculatePivotLevels(mockOHLCData);
            expect(validResults.levels.standard.PP).toBeDefined();
        });
    });

    describe('Risk Metrics Validation', () => {
        test('should calculate comprehensive risk metrics', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData, {
                includePerformance: true
            });

            expect(results.risk).toBeDefined();
            expect(results.risk.volatility).toBeDefined();
            expect(results.risk.drawdown).toBeDefined();
            expect(results.risk.var).toBeDefined();
        });

        test('should provide realistic VaR calculations', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData, {
                includePerformance: true
            });

            const var95 = results.risk.var.parametric;
            expect(var95.confidence).toBe(0.05);
            expect(var95.percentage).toBeLessThan(0); // VaR should be negative
            expect(Math.abs(var95.percentage)).toBeLessThan(50); // Reasonable VaR range
        });

        test('should calculate volatility regimes correctly', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData, {
                includePerformance: true
            });

            const volRegime = results.risk.volatility.regime;
            expect(['LOW', 'NORMAL', 'HIGH']).toContain(volRegime.regime);
        });
    });

    describe('Statistical Analysis', () => {
        test('should perform significance testing when enabled', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData, {
                statisticalAnalysis: true,
                significanceConfig: {
                    alpha: 0.05,
                    minSampleSize: 20
                }
            });

            expect(results.analysis.significance).toBeDefined();

            Object.values(results.analysis.significance).forEach(methodResults => {
                Object.values(methodResults).forEach(levelResult => {
                    if (levelResult.pValue !== undefined) {
                        expect(levelResult.pValue).toBeGreaterThanOrEqual(0);
                        expect(levelResult.pValue).toBeLessThanOrEqual(1);
                        expect(typeof levelResult.isSignificant).toBe('boolean');
                    }
                });
            });
        });

        test('should provide quality scores for pivot levels', async () => {
            const results = await engine.calculatePivotLevels(mockOHLCData);

            expect(results.analysis.qualityScores).toBeDefined();

            Object.values(results.analysis.qualityScores).forEach(methodScores => {
                Object.values(methodScores).forEach(levelScore => {
                    expect(levelScore.reliability).toBeGreaterThanOrEqual(0);
                    expect(levelScore.reliability).toBeLessThanOrEqual(1);
                    expect(levelScore.strength).toBeGreaterThanOrEqual(0);
                    expect(levelScore.strength).toBeLessThanOrEqual(1);
                });
            });
        });
    });

    describe('Configuration Updates', () => {
        test('should update configuration at runtime', () => {
            const newConfig = {
                mathematical: { precision: 6 }
            };

            engine.updateConfiguration(newConfig);
            expect(engine.config.mathematical.precision).toBe(6);
        });

        test('should validate configuration updates', () => {
            const invalidConfig = {
                mathematical: { precision: -1 }
            };

            expect(() => {
                engine.updateConfiguration(invalidConfig);
            }).toThrow();
        });
    });

    describe('Engine Status and Monitoring', () => {
        test('should provide comprehensive engine status', () => {
            const status = engine.getEngineStatus();

            expect(status.state).toBeDefined();
            expect(status.performance).toBeDefined();
            expect(status.cache).toBeDefined();
            expect(status.memory).toBeDefined();

            expect(typeof status.state.calculationCount).toBe('number');
            expect(typeof status.state.errorCount).toBe('number');
        });

        test('should track calculation statistics', async () => {
            const initialStatus = engine.getEngineStatus();
            const initialCount = initialStatus.state.calculationCount;

            await engine.calculatePivotLevels(mockOHLCData);

            const finalStatus = engine.getEngineStatus();
            expect(finalStatus.state.calculationCount).toBe(initialCount + 1);
        });
    });

    describe('Concurrent Operations', () => {
        test('should handle concurrent calculations safely', async () => {
            const promises = [];

            // Start multiple concurrent calculations
            for (let i = 0; i < 5; i++) {
                const data = generateMockOHLCData(50 + i);
                promises.push(engine.calculatePivotLevels(data));
            }

            const results = await Promise.all(promises);

            // All calculations should complete successfully
            results.forEach(result => {
                expect(result.levels.standard.PP).toBeDefined();
                expect(result.metadata.dataPoints).toBeDefined();
            });
        });

        test('should maintain cache consistency under concurrent access', async () => {
            const data = generateMockOHLCData(50);
            const promises = [];

            // Multiple identical requests
            for (let i = 0; i < 10; i++) {
                promises.push(engine.calculatePivotLevels(data));
            }

            const results = await Promise.all(promises);

            // All results should be identical
            const firstResult = results[0];
            results.forEach(result => {
                expect(result.levels.standard.PP).toBe(firstResult.levels.standard.PP);
            });
        });
    });

    describe('Memory Management', () => {
        test('should clean up resources properly', () => {
            const _initialCacheSize = engine.cache.size;

            // Create many cached results
            const promises = [];
            for (let i = 0; i < 150; i++) {
                const data = generateMockOHLCData(10, i);
                promises.push(engine.calculatePivotLevels(data));
            }

            return Promise.all(promises).then(() => {
                // Cache should not grow indefinitely
                expect(engine.cache.size).toBeLessThanOrEqual(engine.config.performance.maxCacheSize);
            });
        });

        test('should handle cache expiration correctly', async () => {
            const data = generateMockOHLCData(50);
            await engine.calculatePivotLevels(data, { cacheTTL: 100 });

            // Wait for cache to expire
            await new Promise(resolve => setTimeout(resolve, 150));

            const cacheKey = engine._generateCacheKey(data, {
                ...engine.config.defaultOptions,
                cacheTTL: 100
            });
            expect(engine._getCachedResult(cacheKey)).toBeNull();
        });
    });
});

// Utility function to generate mock OHLC data
function generateMockOHLCData(length = 100, seed = 0) {
    const data = [];
    let price = 1000 + seed;

    for (let i = 0; i < length; i++) {
        const variation = (Math.sin(i * 0.1 + seed) * 0.02) + (Math.random() - 0.5) * 0.01;
        price *= (1 + variation);

        const open = price * (1 + (Math.random() - 0.5) * 0.002);
        const high = Math.max(open, price) * (1 + Math.random() * 0.005);
        const low = Math.min(open, price) * (1 - Math.random() * 0.005);
        const close = price;

        data.push({
            timestamp: Date.now() - (length - i) * 60000, // 1 minute bars
            open: parseFloat(open.toFixed(2)),
            high: parseFloat(high.toFixed(2)),
            low: parseFloat(low.toFixed(2)),
            close: parseFloat(close.toFixed(2)),
            volume: Math.floor(Math.random() * 1000000)
        });
    }

    return data;
}

// Performance stress tests
describe('Performance Stress Tests', () => {
    let engine;

    beforeAll(() => {
        engine = new QuantPivotEngine(ConfigurationManager.mergeWithDefaults({}, 'production'));
    });

    afterAll(() => {
        engine.dispose();
    });

    test('should handle maximum data points efficiently', async () => {
        const maxData = generateMockOHLCData(10000); // Maximum allowed
        const startTime = performance.now();

        const results = await engine.calculatePivotLevels(maxData);
        const endTime = performance.now();

        expect(results.levels.standard.PP).toBeDefined();
        expect(endTime - startTime).toBeLessThan(10000); // 10 second max
    }, 15000); // 15 second timeout

    test('should maintain performance under repeated calculations', async () => {
        const times = [];

        for (let i = 0; i < 20; i++) {
            const data = generateMockOHLCData(500);
            const startTime = performance.now();
            await engine.calculatePivotLevels(data);
            const endTime = performance.now();
            times.push(endTime - startTime);
        }

        // Performance should not degrade significantly
        const avgTime = times.reduce((sum, time) => sum + time, 0) / times.length;
        const lastFive = times.slice(-5);
        const lastFiveAvg = lastFive.reduce((sum, time) => sum + time, 0) / lastFive.length;

        expect(lastFiveAvg).toBeLessThan(avgTime * 2); // No more than 2x degradation
    }, 30000);
});

// Edge case testing
describe('Edge Cases and Boundary Conditions', () => {
    let engine;

    beforeAll(() => {
        engine = new QuantPivotEngine();
    });

    afterAll(() => {
        engine.dispose();
    });

    test('should handle zero volatility data', async () => {
        const flatData = Array(50).fill({
            high: 100,
            low: 100,
            close: 100,
            volume: 1000
        }).map((bar, i) => ({
            ...bar,
            timestamp: Date.now() - (50 - i) * 60000
        }));

        const results = await engine.calculatePivotLevels(flatData);
        expect(results.levels.standard.PP).toBe(100);
        expect(results.levels.standard.R1).toBe(100);
        expect(results.levels.standard.S1).toBe(100);
    });

    test('should handle extreme volatility data', async () => {
        const volatileData = [];
        let price = 100;

        for (let i = 0; i < 50; i++) {
            const change = (Math.random() - 0.5) * 0.4; // ±20% moves
            price *= (1 + change);

            volatileData.push({
                timestamp: Date.now() - (50 - i) * 60000,
                high: price * 1.1,
                low: price * 0.9,
                close: price,
                volume: Math.floor(Math.random() * 1000000)
            });
        }

        const results = await engine.calculatePivotLevels(volatileData);
        expect(results.levels.standard.PP).toBeDefined();
        expect(Number.isFinite(results.levels.standard.PP)).toBe(true);
    });

    test('should handle data with missing volume', async () => {
        const dataWithoutVolume = generateMockOHLCData(50).map(({ _volume, ...bar }) => bar);

        const results = await engine.calculatePivotLevels(dataWithoutVolume);
        expect(results.levels.standard.PP).toBeDefined();
    });

    test('should handle minimum viable dataset', async () => {
        const minimalData = generateMockOHLCData(2);

        const results = await engine.calculatePivotLevels(minimalData, {
            atrPeriod: 1
        });

        expect(results.levels.standard.PP).toBeDefined();
    });
});

export { generateMockOHLCData };
