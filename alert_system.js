/* ---------- alert_system.js ----------
   Professional alert system for pivot level touches,
   price notifications, and trading signals
   -----------------------------------------*/

class ProfessionalAlertSystem {
    constructor() {
        this.alerts = new Map();
        this.activeAlerts = new Map();
        this.alertHistory = [];
        this.soundEnabled = true;
        this.browserNotifications = false;
        this.emailAlerts = false;
        this.discordWebhook = null;

        // Alert types
        this.alertTypes = {
            LEVEL_TOUCH: 'Level Touch',
            LEVEL_BREAK: 'Level Break',
            CONFLUENCE: 'Confluence Zone',
            VOLATILITY: 'Volatility Alert',
            TIME_BASED: 'Time-Based Alert',
            CUSTOM: 'Custom Alert'
        };

        // Sound library
        this.sounds = {
            success: new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdJivrJBhNjVgodDbq2EcBj+a2/LDciUFLIHO8tiJNwgZaLvt559NEAxQp+PwtmMcBjiR1/LMeSwFJHfH8N2QQAoUXrTp66hVFApGn+DyvmEaBEOY4/PEeSU'),
            warning: new Audio('data:audio/wav;base64,UklGRr4CAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YZoCAAC4uLi4QEBAQEBAQEBAQEBAQEBAuLi4uLi4uLi4QEBAQEBAQEBAuLi4uLi4uEBAQLi4uLhAQEBAQEBAQEC4uLi4uLi4uEBAQLi4uLi4uLi4uLi4uLi4QEBAuLi4uEBAQEBAQEBAQLi4uLhAQEBAQEBAQEBAuLi4'),
            error: new Audio('data:audio/wav;base64,UklGRgQDAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YOQCAAD/////////////////////////+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5')
        };

        this.init();
    }

    init() {
        this.createAlertInterface();
        this.requestNotificationPermission();
        this.loadSettings();
        this.startPriceMonitoring();

        // Integration with existing systems
        document.addEventListener('pivotUpdate', (e) => {
            this.checkPivotAlerts(e.detail);
        });
    }

    // ========= ALERT MANAGEMENT =========
    createAlert(config) {
        const alert = {
            id: `alert_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            type: config.type,
            asset: config.asset || 'SPX',
            condition: config.condition, // 'above', 'below', 'touch'
            level: config.level,
            levelName: config.levelName || `${config.level.toFixed(2)}`,
            message: config.message || `Price ${config.condition} ${config.levelName}`,
            active: true,
            created: new Date(),
            triggered: null,
            triggerCount: 0,
            persistent: config.persistent || false,
            soundAlert: config.soundAlert !== false,
            pushNotification: config.pushNotification !== false,
            discordAlert: config.discordAlert || false,
            emailAlert: config.emailAlert || false,
            metadata: config.metadata || {}
        };

        this.alerts.set(alert.id, alert);
        this.updateAlertInterface();

        return alert.id;
    }

    removeAlert(alertId) {
        this.alerts.delete(alertId);
        this.activeAlerts.delete(alertId);
        this.updateAlertInterface();
    }

    toggleAlert(alertId) {
        const alert = this.alerts.get(alertId);
        if (alert) {
            alert.active = !alert.active;
            this.updateAlertInterface();
        }
    }

    // ========= ALERT INTERFACE =========
    createAlertInterface() {
        const alertModal = document.createElement('div');
        alertModal.id = 'alert-modal';
        alertModal.className = 'alert-modal';
        alertModal.style.display = 'none';

        alertModal.innerHTML = `
            <style>
                .alert-modal {
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

                .alert-content {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 16px;
                    width: 90%;
                    max-width: 800px;
                    max-height: 80vh;
                    overflow-y: auto;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
                }

                .alert-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 24px 24px 16px;
                    border-bottom: 1px solid var(--border-soft);
                }

                .alert-title {
                    color: var(--accent-blue);
                    font-size: 1.4rem;
                    font-weight: 600;
                    margin: 0;
                }

                .alert-close {
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

                .alert-close:hover {
                    background: var(--bg-panel);
                    color: var(--text-primary);
                }

                .alert-tabs {
                    display: flex;
                    border-bottom: 1px solid var(--border-soft);
                }

                .alert-tab {
                    flex: 1;
                    background: none;
                    border: none;
                    color: var(--text-secondary);
                    padding: 16px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                    border-bottom: 2px solid transparent;
                }

                .alert-tab.active {
                    color: var(--accent-blue);
                    border-bottom-color: var(--accent-blue);
                }

                .alert-body {
                    padding: 24px;
                }

                .alert-section {
                    display: none;
                }

                .alert-section.active {
                    display: block;
                }

                .alert-form {
                    display: grid;
                    gap: 16px;
                }

                .alert-row {
                    display: grid;
                    grid-template-columns: 1fr 2fr;
                    gap: 12px;
                    align-items: center;
                }

                .alert-label {
                    color: var(--text-primary);
                    font-weight: 500;
                }

                .alert-input, .alert-select {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 6px;
                    padding: 8px 12px;
                    color: var(--text-primary);
                }

                .alert-list {
                    max-height: 400px;
                    overflow-y: auto;
                }

                .alert-item {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 12px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }

                .alert-item.inactive {
                    opacity: 0.6;
                }

                .alert-item-info {
                    flex: 1;
                }

                .alert-item-title {
                    color: var(--text-primary);
                    font-weight: 500;
                    margin-bottom: 4px;
                }

                .alert-item-details {
                    color: var(--text-secondary);
                    font-size: 13px;
                }

                .alert-item-actions {
                    display: flex;
                    gap: 8px;
                }

                .alert-btn {
                    background: var(--accent-blue);
                    color: white;
                    border: none;
                    border-radius: 6px;
                    padding: 8px 16px;
                    cursor: pointer;
                    font-size: 14px;
                    transition: background 0.2s ease;
                }

                .alert-btn:hover {
                    background: #1976d2;
                }

                .alert-btn.secondary {
                    background: var(--bg-accent);
                    color: var(--text-primary);
                    border: 1px solid var(--border-soft);
                }

                .alert-btn.danger {
                    background: var(--accent-red);
                }

                .alert-btn.small {
                    padding: 4px 8px;
                    font-size: 12px;
                }

                .alert-settings {
                    display: grid;
                    gap: 12px;
                }

                .alert-setting {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 12px 0;
                    border-bottom: 1px solid var(--border-soft);
                }

                .alert-toggle {
                    position: relative;
                    display: inline-block;
                    width: 48px;
                    height: 24px;
                }

                .alert-toggle input {
                    opacity: 0;
                    width: 0;
                    height: 0;
                }

                .alert-slider {
                    position: absolute;
                    cursor: pointer;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background-color: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    transition: .4s;
                    border-radius: 24px;
                }

                .alert-slider:before {
                    position: absolute;
                    content: "";
                    height: 16px;
                    width: 16px;
                    left: 3px;
                    bottom: 3px;
                    background-color: var(--text-secondary);
                    transition: .4s;
                    border-radius: 50%;
                }

                input:checked + .alert-slider {
                    background-color: var(--accent-blue);
                    border-color: var(--accent-blue);
                }

                input:checked + .alert-slider:before {
                    transform: translateX(24px);
                    background-color: white;
                }

                .alert-history {
                    max-height: 300px;
                    overflow-y: auto;
                }

                .alert-history-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 8px 0;
                    border-bottom: 1px solid var(--border-soft);
                }

                .alert-status-active {
                    color: var(--accent-green);
                }

                .alert-status-triggered {
                    color: var(--accent-gold);
                }

                .quick-alerts {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 12px;
                    margin-bottom: 24px;
                }

                .quick-alert-card {
                    background: var(--bg-panel);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                    padding: 16px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }

                .quick-alert-card:hover {
                    border-color: var(--accent-blue);
                    background: var(--bg-accent);
                }

                .quick-alert-title {
                    color: var(--text-primary);
                    font-weight: 500;
                    margin-bottom: 4px;
                }

                .quick-alert-desc {
                    color: var(--text-secondary);
                    font-size: 13px;
                }
            </style>

            <div class="alert-content">
                <div class="alert-header">
                    <h2 class="alert-title">🔔 Professional Alerts</h2>
                    <button class="alert-close" onclick="document.getElementById('alert-modal').style.display='none'">×</button>
                </div>

                <div class="alert-tabs">
                    <button class="alert-tab active" data-tab="create">Create Alert</button>
                    <button class="alert-tab" data-tab="manage">Manage Alerts</button>
                    <button class="alert-tab" data-tab="settings">Settings</button>
                    <button class="alert-tab" data-tab="history">History</button>
                </div>

                <div class="alert-body">
                    <!-- Create Alert Tab -->
                    <div class="alert-section active" id="alert-create">
                        <div class="quick-alerts">
                            <div class="quick-alert-card" data-preset="resistance">
                                <div class="quick-alert-title">🔴 Resistance Touch</div>
                                <div class="quick-alert-desc">Alert when price approaches R1/R2/R3 levels</div>
                            </div>
                            <div class="quick-alert-card" data-preset="support">
                                <div class="quick-alert-title">🟢 Support Touch</div>
                                <div class="quick-alert-desc">Alert when price approaches S1/S2/S3 levels</div>
                            </div>
                            <div class="quick-alert-card" data-preset="pivot">
                                <div class="quick-alert-title">🟡 Pivot Zone</div>
                                <div class="quick-alert-desc">Alert on pivot level interaction</div>
                            </div>
                            <div class="quick-alert-card" data-preset="confluence">
                                <div class="quick-alert-title">⚡ Confluence</div>
                                <div class="quick-alert-desc">Alert on EMA + Pivot confluence</div>
                            </div>
                        </div>

                        <form class="alert-form" id="alert-create-form">
                            <div class="alert-row">
                                <label class="alert-label">Alert Type:</label>
                                <select class="alert-select" id="alert-type">
                                    <option value="LEVEL_TOUCH">Level Touch</option>
                                    <option value="LEVEL_BREAK">Level Break</option>
                                    <option value="CONFLUENCE">Confluence Zone</option>
                                    <option value="VOLATILITY">Volatility Alert</option>
                                    <option value="CUSTOM">Custom Price Level</option>
                                </select>
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Asset:</label>
                                <select class="alert-select" id="alert-asset">
                                    <option value="SPX">S&P 500 (SPX)</option>
                                    <option value="NDX">NASDAQ 100 (NDX)</option>
                                    <option value="DJI">Dow Jones (DJI)</option>
                                    <option value="ES">E-mini S&P (ES)</option>
                                    <option value="NQ">E-mini NASDAQ (NQ)</option>
                                    <option value="YM">E-mini Dow (YM)</option>
                                </select>
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Condition:</label>
                                <select class="alert-select" id="alert-condition">
                                    <option value="touch">Price touches level</option>
                                    <option value="above">Price goes above</option>
                                    <option value="below">Price goes below</option>
                                    <option value="cross_up">Price crosses up through</option>
                                    <option value="cross_down">Price crosses down through</option>
                                </select>
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Price Level:</label>
                                <input type="number" class="alert-input" id="alert-level" step="0.01" placeholder="Enter price level">
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Tolerance:</label>
                                <input type="number" class="alert-input" id="alert-tolerance" step="0.01" placeholder="± tolerance (pts)" value="0.50">
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Message:</label>
                                <input type="text" class="alert-input" id="alert-message" placeholder="Custom alert message">
                            </div>

                            <div class="alert-row">
                                <label class="alert-label">Alert Methods:</label>
                                <div style="display: flex; gap: 16px; flex-wrap: wrap;">
                                    <label><input type="checkbox" checked> 🔊 Sound</label>
                                    <label><input type="checkbox" checked> 🔔 Browser</label>
                                    <label><input type="checkbox"> 📧 Email</label>
                                    <label><input type="checkbox"> 💬 Discord</label>
                                </div>
                            </div>

                            <button type="submit" class="alert-btn">Create Alert</button>
                        </form>
                    </div>

                    <!-- Manage Alerts Tab -->
                    <div class="alert-section" id="alert-manage">
                        <div class="alert-list" id="alert-list">
                            <!-- Dynamic content -->
                        </div>
                    </div>

                    <!-- Settings Tab -->
                    <div class="alert-section" id="alert-settings">
                        <div class="alert-settings">
                            <div class="alert-setting">
                                <div>
                                    <div style="color: var(--text-primary); font-weight: 500;">Sound Alerts</div>
                                    <div style="color: var(--text-secondary); font-size: 13px;">Play sound when alerts trigger</div>
                                </div>
                                <label class="alert-toggle">
                                    <input type="checkbox" id="setting-sound" checked>
                                    <span class="alert-slider"></span>
                                </label>
                            </div>

                            <div class="alert-setting">
                                <div>
                                    <div style="color: var(--text-primary); font-weight: 500;">Browser Notifications</div>
                                    <div style="color: var(--text-secondary); font-size: 13px;">Show browser popup notifications</div>
                                </div>
                                <label class="alert-toggle">
                                    <input type="checkbox" id="setting-browser">
                                    <span class="alert-slider"></span>
                                </label>
                            </div>

                            <div class="alert-setting">
                                <div>
                                    <div style="color: var(--text-primary); font-weight: 500;">Email Alerts</div>
                                    <div style="color: var(--text-secondary); font-size: 13px;">Send alerts to email (requires setup)</div>
                                </div>
                                <label class="alert-toggle">
                                    <input type="checkbox" id="setting-email">
                                    <span class="alert-slider"></span>
                                </label>
                            </div>

                            <div class="alert-setting">
                                <div>
                                    <label for="alert-volume" style="color: var(--text-primary); font-weight: 500;">Alert Volume</label>
                                    <div style="color: var(--text-secondary); font-size: 13px;">Sound alert volume level</div>
                                </div>
                                <input type="range" id="alert-volume" min="0" max="100" value="70"
                                       style="background: var(--bg-panel); accent-color: var(--accent-blue);">
                            </div>

                            <div class="alert-setting">
                                <div>
                                    <label for="discord-webhook" style="color: var(--text-primary); font-weight: 500;">Discord Webhook</label>
                                    <div style="color: var(--text-secondary); font-size: 13px;">Discord webhook URL for alerts</div>
                                </div>
                                <input type="url" class="alert-input" id="discord-webhook"
                                       placeholder="https://discord.com/api/webhooks/...">
                            </div>

                            <button class="alert-btn" onclick="window.alertSystem.saveSettings()">Save Settings</button>
                        </div>
                    </div>

                    <!-- History Tab -->
                    <div class="alert-section" id="alert-history">
                        <div class="alert-history" id="alert-history-list">
                            <!-- Dynamic content -->
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(alertModal);
        this.setupModalEvents();
    }

    setupModalEvents() {
        // Tab switching
        document.querySelectorAll('.alert-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const targetTab = tab.dataset.tab;

                // Update tab appearance
                document.querySelectorAll('.alert-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');

                // Show corresponding section
                document.querySelectorAll('.alert-section').forEach(section => {
                    section.classList.remove('active');
                });
                document.getElementById(`alert-${targetTab}`).classList.add('active');

                // Load section-specific content
                if (targetTab === 'manage') this.updateAlertList();
                if (targetTab === 'history') this.updateAlertHistory();
            });
        });

        // Quick alert presets
        document.querySelectorAll('.quick-alert-card').forEach(card => {
            card.addEventListener('click', () => {
                const preset = card.dataset.preset;
                this.applyAlertPreset(preset);
            });
        });

        // Form submission
        document.getElementById('alert-create-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.createAlertFromForm();
        });

        // Settings
        document.getElementById('setting-sound').addEventListener('change', (e) => {
            this.soundEnabled = e.target.checked;
        });

        document.getElementById('setting-browser').addEventListener('change', (e) => {
            this.browserNotifications = e.target.checked;
            if (e.target.checked) this.requestNotificationPermission();
        });

        document.getElementById('alert-volume').addEventListener('input', (e) => {
            const volume = e.target.value / 100;
            Object.values(this.sounds).forEach(sound => {
                sound.volume = volume;
            });
        });
    }

    // ========= ALERT PRESETS =========
    applyAlertPreset(preset) {
        const zones = window.pivotCore?.calculateZones?.() || {};

        switch (preset) {
            case 'resistance':
                if (zones.R1) {
                    document.getElementById('alert-type').value = 'LEVEL_TOUCH';
                    document.getElementById('alert-condition').value = 'touch';
                    document.getElementById('alert-level').value = zones.R1.value.toFixed(2);
                    document.getElementById('alert-message').value = 'Price approaching R1 resistance level';
                }
                break;

            case 'support':
                if (zones.S1) {
                    document.getElementById('alert-type').value = 'LEVEL_TOUCH';
                    document.getElementById('alert-condition').value = 'touch';
                    document.getElementById('alert-level').value = zones.S1.value.toFixed(2);
                    document.getElementById('alert-message').value = 'Price approaching S1 support level';
                }
                break;

            case 'pivot':
                if (zones.PIVOT) {
                    document.getElementById('alert-type').value = 'LEVEL_TOUCH';
                    document.getElementById('alert-condition').value = 'touch';
                    document.getElementById('alert-level').value = zones.PIVOT.value.toFixed(2);
                    document.getElementById('alert-message').value = 'Price at pivot level - key decision zone';
                }
                break;

            case 'confluence':
                document.getElementById('alert-type').value = 'CONFLUENCE';
                document.getElementById('alert-condition').value = 'touch';
                document.getElementById('alert-message').value = 'EMA and Pivot confluence detected';
                break;
        }
    }

    createAlertFromForm() {
        const formData = {
            type: document.getElementById('alert-type').value,
            asset: document.getElementById('alert-asset').value,
            condition: document.getElementById('alert-condition').value,
            level: parseFloat(document.getElementById('alert-level').value),
            tolerance: parseFloat(document.getElementById('alert-tolerance').value) || 0.5,
            message: document.getElementById('alert-message').value,
            soundAlert: document.querySelector('#alert-create-form input[type="checkbox"]:nth-of-type(1)').checked,
            pushNotification: document.querySelector('#alert-create-form input[type="checkbox"]:nth-of-type(2)').checked,
            emailAlert: document.querySelector('#alert-create-form input[type="checkbox"]:nth-of-type(3)').checked,
            discordAlert: document.querySelector('#alert-create-form input[type="checkbox"]:nth-of-type(4)').checked
        };

        if (!Number.isFinite(formData.level)) {
            if (window.professionalUI) {
                window.professionalUI.showNotification('error', 'Invalid Level', 'Please enter a valid price level');
            }
            return;
        }

        const _alertId = this.createAlert(formData);

        if (window.professionalUI) {
            window.professionalUI.showNotification('success', 'Alert Created',
                `${formData.type} alert created for ${formData.asset} at ${formData.level.toFixed(2)}`);
        }

        // Reset form
        document.getElementById('alert-create-form').reset();

        // Switch to manage tab
        document.querySelector('[data-tab="manage"]').click();
    }

    // ========= PRICE MONITORING =========
    startPriceMonitoring() {
        this.monitoringInterval = setInterval(() => {
            this.checkAllAlerts();
        }, 1000); // Check every second
    }

    checkAllAlerts() {
        if (!window.enhancedDataFetcher) return;

        const currentPrice = this.getCurrentPrice();
        if (!currentPrice) return;

        this.alerts.forEach(alert => {
            if (!alert.active) return;
            this.checkAlert(alert, currentPrice);
        });
    }

    checkAlert(alert, currentPrice) {
        const tolerance = alert.tolerance || 0.5;
        const level = alert.level;
        const condition = alert.condition;

        let triggered = false;

        switch (condition) {
            case 'touch':
                triggered = Math.abs(currentPrice - level) <= tolerance;
                break;
            case 'above':
                triggered = currentPrice > level + tolerance;
                break;
            case 'below':
                triggered = currentPrice < level - tolerance;
                break;
            case 'cross_up':
                const prevPrice = this.getPreviousPrice();
                triggered = prevPrice <= level && currentPrice > level;
                break;
            case 'cross_down':
                const prevPrice2 = this.getPreviousPrice();
                triggered = prevPrice2 >= level && currentPrice < level;
                break;
        }

        if (triggered && !this.activeAlerts.has(alert.id)) {
            this.triggerAlert(alert, currentPrice);
        }

        // Reset alert if price moves away (for touch alerts)
        if (!triggered && this.activeAlerts.has(alert.id)) {
            this.activeAlerts.delete(alert.id);
        }
    }

    triggerAlert(alert, currentPrice) {
        alert.triggered = new Date();
        alert.triggerCount++;

        this.activeAlerts.set(alert.id, {
            ...alert,
            triggerPrice: currentPrice
        });

        this.alertHistory.unshift({
            ...alert,
            triggerPrice: currentPrice,
            timestamp: new Date()
        });

        // Execute alert actions
        this.executeAlertActions(alert, currentPrice);

        // Remove non-persistent alerts
        if (!alert.persistent) {
            setTimeout(() => {
                this.removeAlert(alert.id);
            }, 5000);
        }
    }

    executeAlertActions(alert, currentPrice) {
        const message = alert.message || `${alert.levelName} level alert`;
        const title = `${alert.asset} Alert`;

        // Sound alert
        if (alert.soundAlert && this.soundEnabled) {
            this.playAlertSound(alert.type);
        }

        // Browser notification
        if (alert.pushNotification && this.browserNotifications) {
            this.showBrowserNotification(title, `${message}\nPrice: ${currentPrice.toFixed(2)}`);
        }

        // Discord webhook
        if (alert.discordAlert && this.discordWebhook) {
            this.sendDiscordAlert(alert, currentPrice);
        }

        // UI notification
        if (window.professionalUI) {
            window.professionalUI.showNotification('warning', title,
                `${message}\nCurrent Price: ${currentPrice.toFixed(2)}`, {
                    persistent: true,
                    actions: [
                        {
                            id: 'dismiss',
                            label: 'Dismiss',
                            handler: () => this.removeAlert(alert.id)
                        },
                        {
                            id: 'snooze',
                            label: 'Snooze 5m',
                            handler: () => this.snoozeAlert(alert.id, 5)
                        }
                    ]
                }
            );
        }
    }

    // ========= UTILITY FUNCTIONS =========
    getCurrentPrice() {
        // Integration with existing data systems
        const inputs = window.pivotCore?.state?.inputsDaily;
        return inputs?.close || null;
    }

    getPreviousPrice() {
        // This would need historical price tracking
        return null;
    }

    playAlertSound(type) {
        const soundMap = {
            'LEVEL_TOUCH': 'warning',
            'LEVEL_BREAK': 'error',
            'CONFLUENCE': 'success',
            'VOLATILITY': 'warning',
            'CUSTOM': 'warning'
        };

        const soundType = soundMap[type] || 'warning';
        const sound = this.sounds[soundType];

        if (sound) {
            sound.currentTime = 0;
            /* eslint-disable-next-line no-console */
            sound.play().catch(e => console.warn('Could not play sound:', e));
        }
    }

    async requestNotificationPermission() {
        if ('Notification' in window) {
            const permission = await Notification.requestPermission();
            this.browserNotifications = permission === 'granted';

            const settingEl = document.getElementById('setting-browser');
            if (settingEl) {
                settingEl.checked = this.browserNotifications;
            }
        }
    }

    showBrowserNotification(title, message) {
        if ('Notification' in window && Notification.permission === 'granted') {
            new Notification(title, {
                body: message,
                icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><circle cx="32" cy="32" r="30" fill="%23blue"/></svg>',
                requireInteraction: true
            });
        }
    }

    async sendDiscordAlert(alert, currentPrice) {
        if (!this.discordWebhook) return;

        try {
            await fetch(this.discordWebhook, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    embeds: [{
                        title: '🔔 Price Alert',
                        description: alert.message,
                        color: 0x42a5f5,
                        fields: [
                            { name: 'Asset', value: alert.asset, inline: true },
                            { name: 'Level', value: alert.level.toFixed(2), inline: true },
                            { name: 'Current Price', value: currentPrice.toFixed(2), inline: true }
                        ],
                        timestamp: new Date().toISOString()
                    }]
                })
            });
        } catch (error) {
            /* eslint-disable-next-line no-console */
            console.error('Discord alert failed:', error);
        }
    }

    updateAlertList() {
        const listEl = document.getElementById('alert-list');
        if (!listEl) return;

        const alertsArray = Array.from(this.alerts.values()).sort((a, b) => b.created - a.created);

        listEl.innerHTML = alertsArray.length ? alertsArray.map(alert => `
            <div class="alert-item ${alert.active ? '' : 'inactive'}">
                <div class="alert-item-info">
                    <div class="alert-item-title">
                        ${alert.type} - ${alert.asset} @ ${alert.level.toFixed(2)}
                    </div>
                    <div class="alert-item-details">
                        ${alert.condition} • Created: ${alert.created.toLocaleString()}
                        ${alert.triggered ? ` • Triggered: ${alert.triggered.toLocaleString()}` : ''}
                    </div>
                </div>
                <div class="alert-item-actions">
                    <button class="alert-btn small ${alert.active ? 'secondary' : ''}"
                            onclick="window.alertSystem.toggleAlert('${alert.id}')">
                        ${alert.active ? 'Pause' : 'Resume'}
                    </button>
                    <button class="alert-btn small danger"
                            onclick="window.alertSystem.removeAlert('${alert.id}')">
                        Delete
                    </button>
                </div>
            </div>
        `).join('') : '<div style="text-align: center; color: var(--text-secondary); padding: 40px;">No alerts created yet.</div>';
    }

    updateAlertHistory() {
        const historyEl = document.getElementById('alert-history-list');
        if (!historyEl) return;

        const recentHistory = this.alertHistory.slice(0, 50);

        historyEl.innerHTML = recentHistory.length ? recentHistory.map(alert => `
            <div class="alert-history-item">
                <div>
                    <div style="color: var(--text-primary); font-weight: 500;">
                        ${alert.type} - ${alert.asset} @ ${alert.level.toFixed(2)}
                    </div>
                    <div style="color: var(--text-secondary); font-size: 13px;">
                        ${alert.timestamp.toLocaleString()} • Price: ${alert.triggerPrice?.toFixed(2) || 'N/A'}
                    </div>
                </div>
                <div class="alert-status-triggered">✓ Triggered</div>
            </div>
        `).join('') : '<div style="text-align: center; color: var(--text-secondary); padding: 40px;">No alert history.</div>';
    }

    saveSettings() {
        const settings = {
            soundEnabled: this.soundEnabled,
            browserNotifications: this.browserNotifications,
            discordWebhook: document.getElementById('discord-webhook')?.value
        };

        localStorage.setItem('alertSettings', JSON.stringify(settings));

        if (window.professionalUI) {
            window.professionalUI.showNotification('success', 'Settings Saved', 'Alert preferences have been saved');
        }
    }

    loadSettings() {
        const saved = localStorage.getItem('alertSettings');
        if (saved) {
            const settings = JSON.parse(saved);
            this.soundEnabled = settings.soundEnabled !== false;
            this.browserNotifications = settings.browserNotifications || false;
            this.discordWebhook = settings.discordWebhook;

            // Update UI
            setTimeout(() => {
                const soundEl = document.getElementById('setting-sound');
                const browserEl = document.getElementById('setting-browser');
                const webhookEl = document.getElementById('discord-webhook');

                if (soundEl) soundEl.checked = this.soundEnabled;
                if (browserEl) browserEl.checked = this.browserNotifications;
                if (webhookEl && this.discordWebhook) webhookEl.value = this.discordWebhook;
            }, 100);
        }
    }

    snoozeAlert(alertId, minutes) {
        const alert = this.alerts.get(alertId);
        if (alert) {
            alert.active = false;
            setTimeout(() => {
                alert.active = true;
            }, minutes * 60 * 1000);
        }
    }

    // ========= PUBLIC API =========
    showAlertManager() {
        document.getElementById('alert-modal').style.display = 'flex';
        this.updateAlertList();
    }

    hideAlertManager() {
        document.getElementById('alert-modal').style.display = 'none';
    }
}

// Initialize alert system
document.addEventListener('DOMContentLoaded', () => {
    window.alertSystem = new ProfessionalAlertSystem();

    // Wire up the alert manager button
    const alertBtn = document.getElementById('alertManager');
    if (alertBtn) {
        alertBtn.addEventListener('click', () => {
            window.alertSystem.showAlertManager();
        });
    }
});