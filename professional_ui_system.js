/* ---------- professional_ui_system.js ----------
   Professional notification system, loading states, and UI enhancements
   for premium user experience
   ---------------------------------------------------*/

class ProfessionalUISystem {
    constructor() {
        this.notifications = new Map();
        this.loadingStates = new Map();
        this.toasts = [];
        this.shortcuts = new Map();

        this.init();
    }

    init() {
        this.createNotificationSystem();
        this.createLoadingOverlay();
        this.createKeyboardShortcuts();
        this.enhanceExistingElements();
        this.addProgressIndicators();

        // Listen for data fetcher events
        document.addEventListener('dataFetcherStatus', (e) => {
            this.handleDataFetcherStatus(e.detail);
        });
    }

    // ========= NOTIFICATION SYSTEM =========
    createNotificationSystem() {
        const container = document.createElement('div');
        container.id = 'notification-system';
        container.className = 'notification-container';

        container.innerHTML = `
            <style>
                .notification-container {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    z-index: 10000;
                    pointer-events: none;
                }

                .toast {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin-bottom: 10px;
                    min-width: 300px;
                    max-width: 450px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                    pointer-events: auto;
                    transform: translateX(100%);
                    transition: transform 0.3s ease, opacity 0.3s ease;
                    opacity: 0;
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .toast.show {
                    transform: translateX(0);
                    opacity: 1;
                }

                .toast.success {
                    border-left: 4px solid var(--accent-green);
                }

                .toast.error {
                    border-left: 4px solid var(--accent-red);
                }

                .toast.warning {
                    border-left: 4px solid var(--accent-gold);
                }

                .toast.info {
                    border-left: 4px solid var(--accent-blue);
                }

                .toast-icon {
                    font-size: 18px;
                    flex-shrink: 0;
                }

                .toast-content {
                    flex: 1;
                }

                .toast-title {
                    font-weight: 600;
                    color: var(--text-primary);
                    font-size: 14px;
                    margin-bottom: 2px;
                }

                .toast-message {
                    color: var(--text-secondary);
                    font-size: 13px;
                    line-height: 1.4;
                }

                .toast-close {
                    background: none;
                    border: none;
                    color: var(--text-secondary);
                    cursor: pointer;
                    padding: 4px;
                    border-radius: 4px;
                    transition: background 0.2s ease;
                }

                .toast-close:hover {
                    background: rgba(255,255,255,0.1);
                    color: var(--text-primary);
                }

                .toast-actions {
                    display: flex;
                    gap: 8px;
                    margin-top: 8px;
                }

                .toast-btn {
                    background: var(--accent-blue);
                    color: white;
                    border: none;
                    border-radius: 4px;
                    padding: 4px 8px;
                    font-size: 12px;
                    cursor: pointer;
                    transition: background 0.2s ease;
                }

                .toast-btn:hover {
                    background: #1976d2;
                }

                .toast-btn.secondary {
                    background: var(--bg-panel);
                    color: var(--text-primary);
                    border: 1px solid var(--border-soft);
                }
            </style>
        `;

        document.body.appendChild(container);
    }

    showNotification(type, title, message, options = {}) {
        const id = `toast_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const duration = options.duration || (type === 'error' ? 8000 : 4000);
        const persistent = options.persistent || false;

        const icons = {
            success: '✅',
            error: '❌',
            warning: '⚠️',
            info: 'ℹ️'
        };

        const toast = document.createElement('div');
        toast.id = id;
        toast.className = `toast ${type}`;

        const actionsHtml = options.actions ?
            `<div class="toast-actions">
                ${options.actions.map(action =>
                    `<button class="toast-btn ${action.type || 'primary'}" data-action="${action.id}">
                        ${action.label}
                    </button>`
                ).join('')}
            </div>` : '';

        toast.innerHTML = `
            <div class="toast-icon">${icons[type] || 'ℹ️'}</div>
            <div class="toast-content">
                <div class="toast-title">${title}</div>
                <div class="toast-message">${message}</div>
                ${actionsHtml}
            </div>
            ${!persistent ? '<button class="toast-close">×</button>' : ''}
        `;

        // Add action handlers
        if (options.actions) {
            options.actions.forEach(action => {
                const btn = toast.querySelector(`[data-action="${action.id}"]`);
                if (btn && action.handler) {
                    btn.addEventListener('click', () => {
                        action.handler();
                        if (action.closeOnAction !== false) {
                            this.hideNotification(id);
                        }
                    });
                }
            });
        }

        // Add close handler
        const closeBtn = toast.querySelector('.toast-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.hideNotification(id));
        }

        // Add to DOM
        const container = document.getElementById('notification-system');
        container.appendChild(toast);

        // Animate in
        requestAnimationFrame(() => {
            toast.classList.add('show');
        });

        // Store reference
        this.toasts.push({ id, element: toast, type, timestamp: Date.now() });

        // Auto-hide if not persistent
        if (!persistent) {
            setTimeout(() => {
                this.hideNotification(id);
            }, duration);
        }

        return id;
    }

    hideNotification(id) {
        const toast = document.getElementById(id);
        if (!toast) return;

        toast.classList.remove('show');

        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
            this.toasts = this.toasts.filter(t => t.id !== id);
        }, 300);
    }

    clearAllNotifications() {
        this.toasts.forEach(toast => {
            this.hideNotification(toast.id);
        });
    }

    // ========= LOADING SYSTEM =========
    createLoadingOverlay() {
        const overlay = document.createElement('div');
        overlay.id = 'loading-overlay';
        overlay.className = 'loading-overlay';

        overlay.innerHTML = `
            <style>
                .loading-overlay {
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(12, 17, 27, 0.85);
                    backdrop-filter: blur(4px);
                    z-index: 9999;
                    display: none;
                    align-items: center;
                    justify-content: center;
                    flex-direction: column;
                }

                .loading-content {
                    background: var(--bg-accent);
                    border: 1px solid var(--border-soft);
                    border-radius: 12px;
                    padding: 32px 24px;
                    text-align: center;
                    min-width: 280px;
                    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
                }

                .loading-spinner {
                    width: 40px;
                    height: 40px;
                    border: 3px solid var(--border-soft);
                    border-top: 3px solid var(--accent-blue);
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                    margin: 0 auto 16px;
                }

                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }

                .loading-title {
                    color: var(--text-primary);
                    font-weight: 600;
                    margin-bottom: 8px;
                }

                .loading-message {
                    color: var(--text-secondary);
                    font-size: 14px;
                    margin-bottom: 16px;
                }

                .loading-progress {
                    width: 100%;
                    height: 4px;
                    background: var(--bg-panel);
                    border-radius: 2px;
                    overflow: hidden;
                }

                .loading-progress-bar {
                    height: 100%;
                    background: linear-gradient(90deg, var(--accent-blue), var(--accent-green));
                    border-radius: 2px;
                    transition: width 0.3s ease;
                    width: 0%;
                }

                .loading-cancel {
                    margin-top: 16px;
                    background: none;
                    border: 1px solid var(--border-soft);
                    color: var(--text-secondary);
                    padding: 8px 16px;
                    border-radius: 6px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }

                .loading-cancel:hover {
                    border-color: var(--accent-red);
                    color: var(--accent-red);
                }
            </style>

            <div class="loading-content">
                <div class="loading-spinner"></div>
                <div class="loading-title" id="loading-title">Loading...</div>
                <div class="loading-message" id="loading-message">Please wait while we fetch your data.</div>
                <div class="loading-progress">
                    <div class="loading-progress-bar" id="loading-progress-bar"></div>
                </div>
                <button class="loading-cancel" id="loading-cancel" style="display: none;">Cancel</button>
            </div>
        `;

        document.body.appendChild(overlay);
    }

    showLoading(title, message, options = {}) {
        const overlay = document.getElementById('loading-overlay');
        const titleEl = document.getElementById('loading-title');
        const messageEl = document.getElementById('loading-message');
        const cancelBtn = document.getElementById('loading-cancel');
        const progressBar = document.getElementById('loading-progress-bar');

        titleEl.textContent = title;
        messageEl.textContent = message;

        if (options.progress !== undefined) {
            progressBar.style.width = `${options.progress}%`;
        }

        if (options.cancellable && options.onCancel) {
            cancelBtn.style.display = 'block';
            cancelBtn.onclick = options.onCancel;
        } else {
            cancelBtn.style.display = 'none';
        }

        overlay.style.display = 'flex';
        return overlay;
    }

    hideLoading() {
        const overlay = document.getElementById('loading-overlay');
        overlay.style.display = 'none';

        // Reset progress
        const progressBar = document.getElementById('loading-progress-bar');
        progressBar.style.width = '0%';
    }

    updateLoadingProgress(progress, message = null) {
        const progressBar = document.getElementById('loading-progress-bar');
        const messageEl = document.getElementById('loading-message');

        progressBar.style.width = `${progress}%`;

        if (message) {
            messageEl.textContent = message;
        }
    }

    // ========= KEYBOARD SHORTCUTS =========
    createKeyboardShortcuts() {
        const shortcuts = [
            { key: 'f', ctrl: true, action: () => this.focusDataFetch(), description: 'Fetch data' },
            { key: 'r', ctrl: true, action: () => this.refreshCalculations(), description: 'Refresh calculations' },
            { key: 'e', ctrl: true, action: () => this.exportData(), description: 'Export data' },
            { key: 'h', ctrl: true, action: () => this.showHelp(), description: 'Show help' },
            { key: 'Escape', action: () => this.closeModals(), description: 'Close modals' },
            { key: '1', alt: true, action: () => this.switchTimeframe('daily'), description: 'Switch to daily' },
            { key: '2', alt: true, action: () => this.switchTimeframe('weekly'), description: 'Switch to weekly' }
        ];

        shortcuts.forEach(shortcut => {
            this.shortcuts.set(this.getShortcutKey(shortcut), shortcut);
        });

        document.addEventListener('keydown', (e) => {
            const key = this.getEventKey(e);
            const shortcut = this.shortcuts.get(key);

            if (shortcut) {
                e.preventDefault();
                shortcut.action();

                this.showNotification('info', 'Shortcut Used',
                    `${shortcut.description} (${this.formatShortcut(shortcut)})`);
            }
        });

        // Create shortcut help panel
        this.createShortcutHelp();
    }

    getShortcutKey(shortcut) {
        return `${shortcut.ctrl ? 'ctrl+' : ''}${shortcut.alt ? 'alt+' : ''}${shortcut.key.toLowerCase()}`;
    }

    getEventKey(e) {
        return `${e.ctrlKey ? 'ctrl+' : ''}${e.altKey ? 'alt+' : ''}${e.key.toLowerCase()}`;
    }

    formatShortcut(shortcut) {
        const parts = [];
        if (shortcut.ctrl) parts.push('Ctrl');
        if (shortcut.alt) parts.push('Alt');
        parts.push(shortcut.key.toUpperCase());
        return parts.join(' + ');
    }

    createShortcutHelp() {
        const helpPanel = document.createElement('div');
        helpPanel.id = 'shortcut-help';
        helpPanel.style.display = 'none';
        helpPanel.innerHTML = `
            <div style="
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                background: var(--bg-accent);
                border: 1px solid var(--border-soft);
                border-radius: 12px;
                padding: 24px;
                z-index: 10001;
                max-width: 400px;
                box-shadow: 0 8px 24px rgba(0,0,0,0.4);
            ">
                <h3 style="color: var(--accent-blue); margin-top: 0;">Keyboard Shortcuts</h3>
                <div id="shortcut-list"></div>
                <button onclick="document.getElementById('shortcut-help').style.display='none'"
                        style="margin-top: 16px; width: 100%;">Close</button>
            </div>
        `;
        document.body.appendChild(helpPanel);
    }

    showHelp() {
        const helpPanel = document.getElementById('shortcut-help');
        const listEl = document.getElementById('shortcut-list');

        listEl.innerHTML = Array.from(this.shortcuts.values())
            .map(s => `
                <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                    <span style="color: var(--text-primary);">${s.description}</span>
                    <code style="color: var(--accent-blue);">${this.formatShortcut(s)}</code>
                </div>
            `).join('');

        helpPanel.style.display = 'block';
    }

    // ========= SHORTCUT ACTIONS =========
    focusDataFetch() {
        const fetchBtn = document.getElementById('fetchYahoo') || document.getElementById('fetchAlpha');
        if (fetchBtn) fetchBtn.click();
    }

    refreshCalculations() {
        if (window.pivotCore && window.pivotCore.updateInputsFromUI) {
            window.pivotCore.updateInputsFromUI();
        }
    }

    exportData() {
        const exportBtn = document.getElementById('exportCSV');
        if (exportBtn) exportBtn.click();
    }

    closeModals() {
        this.hideLoading();
        document.getElementById('shortcut-help').style.display = 'none';
        this.clearAllNotifications();
    }

    switchTimeframe(timeframe) {
        const selector = document.getElementById('timeframeMode');
        if (selector) {
            selector.value = timeframe;
            selector.dispatchEvent(new Event('change'));
        }
    }

    // ========= UI ENHANCEMENTS =========
    enhanceExistingElements() {
        this.addButtonLoadingStates();
        this.enhanceInputValidation();
        this.addTooltips();
        this.enhanceTableSorting();
    }

    addButtonLoadingStates() {
        const buttons = document.querySelectorAll('button[id*="fetch"]');

        buttons.forEach(btn => {
            const originalText = btn.textContent;

            btn.addEventListener('click', () => {
                btn.disabled = true;
                btn.innerHTML = `<span style="opacity: 0.7;">⟳</span> Loading...`;

                // Re-enable after 5 seconds (fallback)
                setTimeout(() => {
                    btn.disabled = false;
                    btn.textContent = originalText;
                }, 5000);
            });
        });
    }

    enhanceInputValidation() {
        const numericInputs = document.querySelectorAll('input[type="number"]');

        numericInputs.forEach(input => {
            // Real-time validation
            input.addEventListener('input', () => {
                const value = parseFloat(input.value);
                const isValid = Number.isFinite(value) && value > 0;

                input.style.borderColor = isValid ? 'var(--accent-green)' : 'var(--accent-red)';

                if (!isValid && input.value) {
                    input.title = 'Please enter a valid positive number';
                } else {
                    input.title = '';
                }
            });

            // Format on blur
            input.addEventListener('blur', () => {
                const value = parseFloat(input.value);
                if (Number.isFinite(value)) {
                    input.value = value.toFixed(2);
                }
            });
        });
    }

    addTooltips() {
        const tooltips = {
            'highInput': 'Previous session high price',
            'lowInput': 'Previous session low price',
            'closeInput': 'Previous session closing price',
            'atrInput': 'Average True Range (14-period)',
            'ema9Input': '9-period Exponential Moving Average',
            'ema21Input': '21-period Exponential Moving Average'
        };

        Object.entries(tooltips).forEach(([id, text]) => {
            const element = document.getElementById(id);
            if (element) {
                element.title = text;
                element.setAttribute('data-tooltip', text);
            }
        });
    }

    enhanceTableSorting() {
        const tables = document.querySelectorAll('table[id*="Table"]');

        tables.forEach(table => {
            const headers = table.querySelectorAll('th');

            headers.forEach((header, index) => {
                header.style.cursor = 'pointer';
                header.style.userSelect = 'none';
                header.addEventListener('click', () => {
                    this.sortTable(table, index);
                });
            });
        });
    }

    sortTable(table, columnIndex) {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        const isNumeric = rows.every(row => {
            const cell = row.cells[columnIndex];
            const value = cell.textContent.replace(/[^0-9.-]/g, '');
            return !isNaN(parseFloat(value));
        });

        rows.sort((a, b) => {
            const aText = a.cells[columnIndex].textContent;
            const bText = b.cells[columnIndex].textContent;

            if (isNumeric) {
                const aVal = parseFloat(aText.replace(/[^0-9.-]/g, ''));
                const bVal = parseFloat(bText.replace(/[^0-9.-]/g, ''));
                return aVal - bVal;
            } else {
                return aText.localeCompare(bText);
            }
        });

        // Re-append sorted rows
        rows.forEach(row => tbody.appendChild(row));

        this.showNotification('info', 'Table Sorted',
            `Sorted by ${table.querySelectorAll('th')[columnIndex].textContent}`);
    }

    addProgressIndicators() {
        // Add progress indicator for backtest
        const backtestBtn = document.getElementById('runBacktest');
        if (backtestBtn) {
            const originalHandler = backtestBtn.onclick;
            backtestBtn.onclick = () => {
                this.showLoading('Running Backtest', 'Analyzing historical data and pivot performance...');

                if (originalHandler) {
                    originalHandler.call(backtestBtn);
                }

                // Hide loading after reasonable time
                setTimeout(() => this.hideLoading(), 10000);
            };
        }
    }

    // ========= DATA FETCHER INTEGRATION =========
    handleDataFetcherStatus(status) {
        const { type, message } = status;

        switch (type) {
            case 'fetching':
                this.showLoading('Fetching Market Data', message, {
                    cancellable: true,
                    onCancel: () => {
                        // Stop any ongoing fetches
                        this.hideLoading();
                    }
                });
                break;

            case 'success':
                this.hideLoading();
                this.showNotification('success', 'Data Updated', message);
                break;

            case 'error':
                this.hideLoading();
                this.showNotification('error', 'Data Fetch Failed', message, {
                    persistent: false,
                    actions: [
                        {
                            id: 'retry',
                            label: 'Retry',
                            handler: () => {
                                if (window.enhancedDataFetcher) {
                                    window.enhancedDataFetcher.fetchAssetData();
                                }
                            }
                        }
                    ]
                });
                break;
        }
    }
}

// Initialize the professional UI system
document.addEventListener('DOMContentLoaded', () => {
    window.professionalUI = new ProfessionalUISystem();

    // Show welcome notification
    setTimeout(() => {
        window.professionalUI.showNotification('info',
            'Welcome to Professional Pivot Calculator',
            'Press Ctrl+H to view keyboard shortcuts',
            { duration: 6000 }
        );
    }, 1000);
});