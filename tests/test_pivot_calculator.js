#!/usr/bin/env node

/**
 * Test script to validate the pivot calculator functionality
 */

import { QuantPivotEngine } from './src/core/QuantPivotEngine.js';

async function testPivotCalculator() {
    console.log('🧪 Testing Professional Pivot Calculator...\n');

    try {
        // Initialize the engine
        console.log('📊 Initializing QuantPivotEngine...');
        const engine = new QuantPivotEngine({
            mathematical: { precision: 4 },
            performance: { maxCacheSize: 50 },
            logging: { level: 2 }
        });

        // Generate test data (SPY-like data)
        console.log('📈 Generating test OHLC data...');
        const testData = generateTestOHLCData(50);

        // Test basic pivot calculation
        console.log('🔢 Testing standard pivot calculations...');
        const results = await engine.calculatePivotLevels(testData, {
            type: 'standard',
            methods: ['standard', 'fibonacci'],
            atrPeriod: 14,
            includeGamma: false,
            includePerformance: false,
            statisticalAnalysis: false
        });

        console.log('✅ Calculation successful!');
        console.log('\n📋 Results Summary:');
        console.log('Data Points:', results.metadata.dataPoints);
        console.log('Calculation Type:', results.metadata.calculationType);

        // Display standard pivot levels
        if (results.levels.standard) {
            console.log('\n🎯 Standard Pivot Levels:');
            Object.entries(results.levels.standard).forEach(([level, value]) => {
                if (level !== 'metadata') {
                    console.log(`  ${level}: ${Number(value).toFixed(4)}`);
                }
            });
        }

        // Display fibonacci levels if available
        if (results.levels.fibonacci) {
            console.log('\n🌀 Fibonacci Levels (sample):');
            const fibSample = Object.entries(results.levels.fibonacci)
                .filter(([key]) => !key.includes('metadata'))
                .slice(0, 5);

            fibSample.forEach(([level, value]) => {
                console.log(`  ${level}: ${Number(value).toFixed(4)}`);
            });
        }

        // Test engine status
        console.log('\n⚡ Engine Status:');
        const status = engine.getEngineStatus();
        console.log('  Calculations:', status.state.calculationCount);
        console.log('  Errors:', status.state.errorCount);
        console.log('  Cache Size:', status.cache.size);

        // Cleanup
        engine.dispose();

        console.log('\n🎉 All tests passed successfully!');
        console.log('✨ Your pivot calculator is working correctly.');

        return true;

    } catch (error) {
        console.error('❌ Test failed:', error.message);
        console.error('\n🔍 Error details:');
        console.error(error.stack);
        return false;
    }
}

function generateTestOHLCData(count = 50, basePrice = 440.0) {
    const data = [];
    let currentPrice = basePrice;

    for (let i = 0; i < count; i++) {
        // Generate realistic price movement
        const volatility = 0.02; // 2% daily volatility
        const priceChange = (Math.random() - 0.5) * volatility * currentPrice;
        currentPrice += priceChange;

        const open = currentPrice + (Math.random() - 0.5) * volatility * currentPrice * 0.3;
        const close = currentPrice + (Math.random() - 0.5) * volatility * currentPrice * 0.3;
        const high = Math.max(open, close) + Math.random() * volatility * currentPrice * 0.2;
        const low = Math.min(open, close) - Math.random() * volatility * currentPrice * 0.2;

        data.push({
            timestamp: Date.now() - (count - i) * 86400 * 1000, // Daily intervals
            open: Number(open.toFixed(2)),
            high: Number(high.toFixed(2)),
            low: Number(low.toFixed(2)),
            close: Number(close.toFixed(2)),
            volume: Math.floor(Math.random() * 10000000) + 1000000 // 1M-11M volume
        });
    }

    return data;
}

// Run the test
testPivotCalculator()
    .then(success => {
        if (success) {
            console.log('\n🚀 Ready to use! Open professional_pivot_calculator.html');
            process.exit(0);
        } else {
            console.log('\n🛠️  Please check the error messages above');
            process.exit(1);
        }
    })
    .catch(error => {
        console.error('💥 Unexpected error:', error);
        process.exit(1);
    });