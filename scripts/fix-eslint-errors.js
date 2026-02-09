#!/usr/bin/env node

/**
 * ESLint Error Fix Script
 * Automatically fixes all specified ESLint errors in the PIVOT_QUANT project
 */

import fs from 'fs';
import path from 'path';

class ESLintFixer {
    constructor() {
        this.fixesApplied = 0;
        this.errors = [];
    }

    log(message) {
        console.log(`[ESLint Fixer] ${message}`);
    }

    error(message) {
        console.error(`[ERROR] ${message}`);
        this.errors.push(message);
    }

    fileExists(filePath) {
        try {
            return fs.existsSync(filePath);
        } catch (error) {
            return false;
        }
    }

    readFile(filePath) {
        try {
            return fs.readFileSync(filePath, 'utf8');
        } catch (error) {
            this.error(`Failed to read ${filePath}: ${error.message}`);
            return null;
        }
    }

    writeFile(filePath, content) {
        try {
            fs.writeFileSync(filePath, content, 'utf8');
            this.fixesApplied++;
            this.log(`Fixed: ${filePath}`);
            return true;
        } catch (error) {
            this.error(`Failed to write ${filePath}: ${error.message}`);
            return false;
        }
    }

    addConsoleDisable(content, lineNumber, searchPattern) {
        const lines = content.split('\n');

        // Find the line containing the pattern
        let targetLineIndex = -1;
        for (let i = 0; i < lines.length; i++) {
            if (searchPattern && lines[i].includes(searchPattern)) {
                targetLineIndex = i;
                break;
            } else if (!searchPattern && i === lineNumber - 1) {
                targetLineIndex = i;
                break;
            }
        }

        if (targetLineIndex === -1) {
            this.error(`Could not find target line with pattern: ${searchPattern}`);
            return content;
        }

        // Check if disable comment already exists
        if (targetLineIndex > 0 && lines[targetLineIndex - 1].includes('eslint-disable-next-line no-console')) {
            return content;
        }

        // Insert the disable comment
        const indent = lines[targetLineIndex].match(/^(\s*)/)[1];
        lines.splice(targetLineIndex, 0, `${indent}/* eslint-disable-next-line no-console */`);

        return lines.join('\n');
    }

    replaceUnusedVariable(content, oldName, newName) {
        // Replace parameter in function signatures
        const paramRegex = new RegExp(`\\b${oldName}\\b(?=\\s*[,)])`, 'g');
        content = content.replace(paramRegex, newName);

        // Replace variable declarations
        const varRegex = new RegExp(`\\b(const|let|var)\\s+${oldName}\\b`, 'g');
        content = content.replace(varRegex, `$1 ${newName}`);

        return content;
    }

    fixConfigurationManager() {
        const filePath = './src/config/ConfigurationManager.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 424: Replace hasOwnProperty with proper call
        content = content.replace(
            /\.hasOwnProperty\s*\(/g,
            '.call(Object.prototype.hasOwnProperty, '
        );
        content = content.replace(
            /Object\.prototype\.hasOwnProperty\s*\(/g,
            'Object.prototype.hasOwnProperty.call('
        );

        // Fix line 527: Add console disable comment
        content = this.addConsoleDisable(content, 527, 'console.');

        this.writeFile(filePath, content);
    }

    fixQuantPivotEngine() {
        const filePath = './src/core/QuantPivotEngine.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 220: Change 'atrData' to '_atrData'
        content = this.replaceUnusedVariable(content, 'atrData', '_atrData');

        // Fix console statements
        content = this.addConsoleDisable(content, 388, 'console.');
        content = this.addConsoleDisable(content, 394, 'console.');
        content = this.addConsoleDisable(content, 400, 'console.');

        this.writeFile(filePath, content);
    }

    fixIndex() {
        const filePath = './src/index.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 398: Change 'config' to '_config'
        content = this.replaceUnusedVariable(content, 'config', '_config');

        // Fix console statements
        content = this.addConsoleDisable(content, 378, 'console.');
        content = this.addConsoleDisable(content, 473, 'console.');

        this.writeFile(filePath, content);
    }

    fixMathematicalModels() {
        const filePath = './src/math/MathematicalModels.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 95: Change 'atrValues' to '_atrValues'
        content = this.replaceUnusedVariable(content, 'atrValues', '_atrValues');

        // Fix line 350: Change 'config' to '_config'
        content = this.replaceUnusedVariable(content, 'config', '_config');

        // Fix lines 872-883: Change multiple unused parameters
        const unusedParams = [
            'ohlcData', 'profile', 'price', 'distribution',
            'levels', 'vol', 'percentiles'
        ];

        unusedParams.forEach(param => {
            content = this.replaceUnusedVariable(content, param, `_${param}`);
        });

        this.writeFile(filePath, content);
    }

    fixPerformanceMonitor() {
        const filePath = './src/monitoring/PerformanceMonitor.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Add console disable comments
        content = this.addConsoleDisable(content, 89, 'console.');
        content = this.addConsoleDisable(content, 458, 'console.');
        content = this.addConsoleDisable(content, 464, 'console.');
        content = this.addConsoleDisable(content, 470, 'console.');

        this.writeFile(filePath, content);
    }

    fixValidationFramework() {
        const filePath = './src/validation/ValidationFramework.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 92: Change 'options' to '_options'
        content = this.replaceUnusedVariable(content, 'options', '_options');

        // Fix line 300: Change 'optionalFields' to '_optionalFields'
        content = this.replaceUnusedVariable(content, 'optionalFields', '_optionalFields');

        // Fix line 80: Add console disable comment
        content = this.addConsoleDisable(content, 80, 'console.');

        this.writeFile(filePath, content);
    }

    fixQuantPivotEngineTest() {
        const filePath = './tests/QuantPivotEngine.test.js';

        if (!this.fileExists(filePath)) {
            this.error(`File not found: ${filePath}`);
            return;
        }

        let content = this.readFile(filePath);
        if (!content) return;

        // Fix line 362: Change 'initialCacheSize' to '_initialCacheSize'
        content = this.replaceUnusedVariable(content, 'initialCacheSize', '_initialCacheSize');

        // Fix line 517: Change 'volume' to '_volume'
        content = this.replaceUnusedVariable(content, 'volume', '_volume');

        this.writeFile(filePath, content);
    }

    run() {
        this.log('Starting ESLint error fixes...');

        // Apply all fixes
        this.fixConfigurationManager();
        this.fixQuantPivotEngine();
        this.fixIndex();
        this.fixMathematicalModels();
        this.fixPerformanceMonitor();
        this.fixValidationFramework();
        this.fixQuantPivotEngineTest();

        // Report results
        this.log(`\n=== Fix Summary ===`);
        this.log(`Files processed: ${this.fixesApplied}`);

        if (this.errors.length > 0) {
            this.log(`Errors encountered: ${this.errors.length}`);
            this.errors.forEach(error => console.error(`  - ${error}`));
        }

        if (this.fixesApplied === 0 && this.errors.length > 0) {
            this.log('No files were found to fix. Please check the file paths.');
        } else {
            this.log('ESLint error fixing complete!');
        }
    }
}

// Run the fixer
const fixer = new ESLintFixer();
fixer.run();