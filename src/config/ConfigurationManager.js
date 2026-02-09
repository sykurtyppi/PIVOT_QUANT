/**
 * ConfigurationManager - Institutional-Grade Configuration Management
 *
 * Centralized configuration management system with validation,
 * environment-specific settings, and runtime configuration updates.
 *
 * @version 2.0.0
 * @author PIVOT_QUANT Team
 */

export class ConfigurationManager {
    constructor() {
        this.configurations = new Map();
        this.environmentConfigs = new Map();
        this.defaultConfig = this._getDefaultConfiguration();
        this.configurationSchema = this._getConfigurationSchema();
        this.listeners = new Set();

        this._initializeEnvironments();
    }

    /**
     * Get merged configuration with defaults
     * @param {Object} userConfig - User-provided configuration
     * @param {string} environment - Environment name
     * @returns {Object} Merged configuration
     */
    static mergeWithDefaults(userConfig = {}, environment = 'production') {
        const instance = ConfigurationManager.getInstance();
        return instance.getMergedConfiguration(userConfig, environment);
    }

    /**
     * Get singleton instance
     * @returns {ConfigurationManager} Singleton instance
     */
    static getInstance() {
        if (!ConfigurationManager.instance) {
            ConfigurationManager.instance = new ConfigurationManager();
        }
        return ConfigurationManager.instance;
    }

    /**
     * Get merged configuration
     * @param {Object} userConfig - User configuration
     * @param {string} environment - Environment name
     * @returns {Object} Merged configuration
     */
    getMergedConfiguration(userConfig = {}, environment = 'production') {
        const envConfig = this.environmentConfigs.get(environment) || {};

        // Deep merge: defaults -> environment -> user
        return this._deepMerge(
            this._deepMerge(this.defaultConfig, envConfig),
            userConfig
        );
    }

    /**
     * Validate configuration against schema
     * @param {Object} config - Configuration to validate
     * @returns {Object} Validation result
     */
    validateConfiguration(config) {
        const errors = [];
        const warnings = [];

        try {
            this._validateAgainstSchema(config, this.configurationSchema, '', errors, warnings);

            return {
                isValid: errors.length === 0,
                errors,
                warnings
            };
        } catch (error) {
            return {
                isValid: false,
                errors: [`Configuration validation failed: ${error.message}`],
                warnings: []
            };
        }
    }

    /**
     * Update configuration at runtime
     * @param {Object} updates - Configuration updates
     * @param {string} environment - Environment to update
     */
    updateConfiguration(updates, environment = 'production') {
        const currentConfig = this.configurations.get(environment) || {};
        const newConfig = this._deepMerge(currentConfig, updates);

        const validation = this.validateConfiguration(newConfig);
        if (!validation.isValid) {
            throw new Error(`Configuration update invalid: ${validation.errors.join(', ')}`);
        }

        this.configurations.set(environment, newConfig);
        this._notifyListeners(environment, newConfig);
    }

    /**
     * Subscribe to configuration changes
     * @param {Function} callback - Callback function
     */
    subscribe(callback) {
        this.listeners.add(callback);
        return () => this.listeners.delete(callback);
    }

    /**
     * Get configuration for specific environment
     * @param {string} environment - Environment name
     * @returns {Object} Environment configuration
     */
    getEnvironmentConfiguration(environment) {
        return this.environmentConfigs.get(environment) || {};
    }

    /**
     * Get available environments
     * @returns {Array} Available environment names
     */
    getAvailableEnvironments() {
        return Array.from(this.environmentConfigs.keys());
    }

    // =================================================================================
    // PRIVATE METHODS
    // =================================================================================

    _getDefaultConfiguration() {
        return {
            // Mathematical calculation settings
            mathematical: {
                precision: 8,
                maxIterations: 1000,
                convergenceThreshold: 1e-10,
                numericalStability: true,
                cacheComputations: true
            },

            // Performance settings
            performance: {
                maxCacheSize: 100,
                cacheExpirationMs: 300000, // 5 minutes
                enableProfiling: false,
                metricsInterval: 60000, // 1 minute
                performanceThresholds: {
                    calculationTime: 1000, // ms
                    memoryUsage: 100 * 1024 * 1024, // 100MB
                    cacheHitRate: 80 // percentage
                }
            },

            // Validation settings
            validation: {
                strictMode: true,
                toleranceLevel: 1e-8,
                maxDataPoints: 10000,
                minDataPoints: 2,
                priceRange: { min: 0.01, max: 1000000 },
                volumeRange: { min: 0, max: Number.MAX_SAFE_INTEGER },
                enableOutlierDetection: true,
                enableMicrostructureValidation: true
            },

            // Logging settings
            logging: {
                level: 1, // 0: Error, 1: Warn, 2: Info, 3: Debug
                enableConsoleLogging: true,
                enableMetricsLogging: false,
                logRotation: {
                    enabled: false,
                    maxSize: 10 * 1024 * 1024, // 10MB
                    maxFiles: 5
                }
            },

            // Default calculation options
            defaultOptions: {
                type: 'standard',
                atrPeriod: 14,
                atrMethod: 'wilder',
                lookback: 100,
                methods: ['standard', 'fibonacci'],
                zoneMultipliers: [0.5, 1.0, 1.5, 2.0],
                cacheTTL: 300000, // 5 minutes
                includeGamma: true,
                includePerformance: false,
                statisticalAnalysis: false,
                gammaConfig: {
                    volumeWeighting: true,
                    densityEstimation: 'kernel',
                    confidence: 0.95
                },
                significanceConfig: {
                    alpha: 0.05,
                    testType: 'binomial',
                    minSampleSize: 20,
                    adjustmentMethod: 'benjamini_hochberg'
                }
            },

            // Risk management settings
            riskManagement: {
                enableRiskMetrics: true,
                varConfidence: [0.95, 0.99],
                stressTestScenarios: ['normal', 'crisis', 'extreme'],
                correlationThresholds: {
                    warning: 0.8,
                    critical: 0.95
                }
            },

            // API and integration settings
            api: {
                rateLimiting: {
                    enabled: true,
                    maxRequests: 1000,
                    timeWindow: 3600000 // 1 hour
                },
                authentication: {
                    enabled: false,
                    tokenExpiration: 3600000 // 1 hour
                },
                cors: {
                    enabled: true,
                    allowedOrigins: ['*']
                }
            },

            // Data source settings
            dataSources: {
                primary: 'internal',
                fallback: 'cached',
                timeout: 30000, // 30 seconds
                retryAttempts: 3,
                retryDelay: 1000 // 1 second
            }
        };
    }

    _getConfigurationSchema() {
        return {
            mathematical: {
                precision: { type: 'number', min: 1, max: 15 },
                maxIterations: { type: 'number', min: 100, max: 100000 },
                convergenceThreshold: { type: 'number', min: 1e-15, max: 1e-5 },
                numericalStability: { type: 'boolean' },
                cacheComputations: { type: 'boolean' }
            },
            performance: {
                maxCacheSize: { type: 'number', min: 10, max: 10000 },
                cacheExpirationMs: { type: 'number', min: 1000, max: 3600000 },
                enableProfiling: { type: 'boolean' },
                metricsInterval: { type: 'number', min: 1000, max: 3600000 },
                performanceThresholds: {
                    calculationTime: { type: 'number', min: 100, max: 60000 },
                    memoryUsage: { type: 'number', min: 1024 * 1024, max: 1024 * 1024 * 1024 },
                    cacheHitRate: { type: 'number', min: 0, max: 100 }
                }
            },
            validation: {
                strictMode: { type: 'boolean' },
                toleranceLevel: { type: 'number', min: 1e-15, max: 1e-5 },
                maxDataPoints: { type: 'number', min: 100, max: 100000 },
                minDataPoints: { type: 'number', min: 1, max: 100 },
                priceRange: {
                    min: { type: 'number', min: 0 },
                    max: { type: 'number', min: 1 }
                },
                enableOutlierDetection: { type: 'boolean' },
                enableMicrostructureValidation: { type: 'boolean' }
            },
            logging: {
                level: { type: 'number', min: 0, max: 3 },
                enableConsoleLogging: { type: 'boolean' },
                enableMetricsLogging: { type: 'boolean' }
            },
            defaultOptions: {
                atrPeriod: { type: 'number', min: 5, max: 100 },
                lookback: { type: 'number', min: 10, max: 1000 },
                methods: { type: 'array', items: { type: 'string', enum: ['standard', 'fibonacci', 'camarilla', 'woodie', 'demark'] } },
                zoneMultipliers: { type: 'array', items: { type: 'number', min: 0.1, max: 5.0 } },
                cacheTTL: { type: 'number', min: 1000, max: 3600000 },
                includeGamma: { type: 'boolean' },
                includePerformance: { type: 'boolean' },
                statisticalAnalysis: { type: 'boolean' }
            }
        };
    }

    _initializeEnvironments() {
        // Development environment
        this.environmentConfigs.set('development', {
            logging: {
                level: 3, // Debug level
                enableConsoleLogging: true,
                enableMetricsLogging: true
            },
            performance: {
                enableProfiling: true,
                metricsInterval: 30000 // 30 seconds
            },
            validation: {
                strictMode: true
            },
            api: {
                rateLimiting: {
                    enabled: false
                }
            }
        });

        // Testing environment
        this.environmentConfigs.set('testing', {
            logging: {
                level: 2, // Info level
                enableConsoleLogging: false,
                enableMetricsLogging: false
            },
            performance: {
                enableProfiling: false,
                maxCacheSize: 50
            },
            validation: {
                strictMode: true,
                maxDataPoints: 1000
            },
            api: {
                rateLimiting: {
                    enabled: false
                }
            }
        });

        // Staging environment
        this.environmentConfigs.set('staging', {
            logging: {
                level: 1, // Warn level
                enableConsoleLogging: true,
                enableMetricsLogging: true
            },
            performance: {
                enableProfiling: true,
                metricsInterval: 60000
            },
            validation: {
                strictMode: true
            },
            api: {
                rateLimiting: {
                    enabled: true,
                    maxRequests: 500
                }
            }
        });

        // Production environment
        this.environmentConfigs.set('production', {
            logging: {
                level: 0, // Error level only
                enableConsoleLogging: false,
                enableMetricsLogging: true
            },
            performance: {
                enableProfiling: false,
                metricsInterval: 300000, // 5 minutes
                maxCacheSize: 200
            },
            validation: {
                strictMode: true
            },
            api: {
                rateLimiting: {
                    enabled: true,
                    maxRequests: 1000
                }
            }
        });

        // High-frequency trading environment
        this.environmentConfigs.set('hft', {
            mathematical: {
                precision: 6, // Reduced precision for speed
                numericalStability: false,
                cacheComputations: true
            },
            performance: {
                enableProfiling: false,
                maxCacheSize: 500,
                cacheExpirationMs: 60000, // 1 minute
                performanceThresholds: {
                    calculationTime: 100, // Very strict
                    memoryUsage: 50 * 1024 * 1024, // 50MB
                    cacheHitRate: 95
                }
            },
            validation: {
                strictMode: false, // Relaxed for speed
                enableOutlierDetection: false,
                enableMicrostructureValidation: false
            },
            logging: {
                level: 0,
                enableConsoleLogging: false,
                enableMetricsLogging: false
            },
            defaultOptions: {
                statisticalAnalysis: false,
                includePerformance: false,
                cacheTTL: 60000 // 1 minute
            }
        });
    }

    _deepMerge(target, source) {
        const result = { ...target };

        for (const key in source) {
            if (Object.prototype.hasOwnProperty.call(source, key)) {
                if (this._isObject(source[key]) && this._isObject(target[key])) {
                    result[key] = this._deepMerge(target[key], source[key]);
                } else {
                    result[key] = source[key];
                }
            }
        }

        return result;
    }

    _isObject(obj) {
        return obj && typeof obj === 'object' && !Array.isArray(obj);
    }

    _validateAgainstSchema(config, schema, path, errors, warnings) {
        for (const [key, schemaSpec] of Object.entries(schema)) {
            const currentPath = path ? `${path}.${key}` : key;
            const value = config[key];

            if (value === undefined) {
                // Optional field, skip validation
                continue;
            }

            if (this._isObject(schemaSpec) && !schemaSpec.type && !schemaSpec.enum) {
                // Nested object schema
                if (this._isObject(value)) {
                    this._validateAgainstSchema(value, schemaSpec, currentPath, errors, warnings);
                } else {
                    errors.push(`${currentPath}: Expected object, got ${typeof value}`);
                }
                continue;
            }

            // Type validation
            if (schemaSpec.type) {
                if (!this._validateType(value, schemaSpec.type)) {
                    errors.push(`${currentPath}: Expected ${schemaSpec.type}, got ${typeof value}`);
                    continue;
                }
            }

            // Enum validation
            if (schemaSpec.enum && !schemaSpec.enum.includes(value)) {
                errors.push(`${currentPath}: Value '${value}' not in allowed values: ${schemaSpec.enum.join(', ')}`);
                continue;
            }

            // Range validation
            if (typeof value === 'number') {
                if (schemaSpec.min !== undefined && value < schemaSpec.min) {
                    errors.push(`${currentPath}: Value ${value} below minimum ${schemaSpec.min}`);
                }
                if (schemaSpec.max !== undefined && value > schemaSpec.max) {
                    errors.push(`${currentPath}: Value ${value} above maximum ${schemaSpec.max}`);
                }
            }

            // Array validation
            if (schemaSpec.type === 'array') {
                if (Array.isArray(value)) {
                    if (schemaSpec.items) {
                        value.forEach((item, index) => {
                            this._validateAgainstSchema(
                                { item },
                                { item: schemaSpec.items },
                                `${currentPath}[${index}]`,
                                errors,
                                warnings
                            );
                        });
                    }
                } else {
                    errors.push(`${currentPath}: Expected array, got ${typeof value}`);
                }
            }
        }
    }

    _validateType(value, expectedType) {
        switch (expectedType) {
            case 'string':
                return typeof value === 'string';
            case 'number':
                return typeof value === 'number' && !isNaN(value) && isFinite(value);
            case 'boolean':
                return typeof value === 'boolean';
            case 'array':
                return Array.isArray(value);
            case 'object':
                return this._isObject(value);
            default:
                return true;
        }
    }

    _notifyListeners(environment, config) {
        this.listeners.forEach(callback => {
            try {
                callback(environment, config);
            } catch (error) {
                /* eslint-disable-next-line no-console */
                console.error('[ConfigurationManager] Listener error:', error);
            }
        });
    }

    /**
     * Export configuration for backup/analysis
     * @param {string} environment - Environment to export
     * @returns {Object} Configuration export
     */
    exportConfiguration(environment = 'production') {
        const config = this.getMergedConfiguration({}, environment);
        return {
            environment,
            timestamp: new Date().toISOString(),
            configuration: config,
            metadata: {
                version: '2.0.0',
                exportedBy: 'ConfigurationManager'
            }
        };
    }

    /**
     * Import configuration from backup
     * @param {Object} exportData - Exported configuration data
     * @param {boolean} validate - Whether to validate before import
     */
    importConfiguration(exportData, validate = true) {
        if (!exportData.configuration) {
            throw new Error('Invalid export data: missing configuration');
        }

        if (validate) {
            const validation = this.validateConfiguration(exportData.configuration);
            if (!validation.isValid) {
                throw new Error(`Configuration import invalid: ${validation.errors.join(', ')}`);
            }
        }

        const environment = exportData.environment || 'imported';
        this.environmentConfigs.set(environment, exportData.configuration);
    }

    /**
     * Get configuration difference between environments
     * @param {string} env1 - First environment
     * @param {string} env2 - Second environment
     * @returns {Object} Configuration differences
     */
    compareConfigurations(env1, env2) {
        const config1 = this.getMergedConfiguration({}, env1);
        const config2 = this.getMergedConfiguration({}, env2);

        return this._findConfigDifferences(config1, config2, '', {});
    }

    _findConfigDifferences(obj1, obj2, path, differences) {
        const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);

        for (const key of allKeys) {
            const currentPath = path ? `${path}.${key}` : key;
            const value1 = obj1[key];
            const value2 = obj2[key];

            if (value1 === undefined && value2 !== undefined) {
                differences[currentPath] = { added: value2 };
            } else if (value1 !== undefined && value2 === undefined) {
                differences[currentPath] = { removed: value1 };
            } else if (this._isObject(value1) && this._isObject(value2)) {
                this._findConfigDifferences(value1, value2, currentPath, differences);
            } else if (value1 !== value2) {
                differences[currentPath] = { changed: { from: value1, to: value2 } };
            }
        }

        return differences;
    }
}

// Initialize singleton
ConfigurationManager.instance = null;