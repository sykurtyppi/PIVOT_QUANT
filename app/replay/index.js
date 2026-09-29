(function attachReplayWorkspace(global) {
  function createReplayWorkspace(deps) {
    const {
      state,
      postJson,
      showToast,
      escapeHtml,
      formatNumber,
      formatPercent,
      renderForSession,
      saveUiPrefs,
      updateAdvancedControlsSummary,
      loadMarketData,
    } = deps;

    function setReplayStatus(message, tone = '') {
      const el = document.getElementById('replay-status');
      if (!el) return;
      el.textContent = message || 'Replay status: --';
      el.style.color = tone === 'error'
        ? 'var(--danger)'
        : tone === 'warn'
          ? 'var(--warning)'
          : '';
    }

    function renderReplayLineage(lineage) {
      const el = document.getElementById('replay-lineage');
      if (!el) return;
      const sourceDb = lineage?.source_db || '--';
      const duckdbPath = lineage?.duckdb_path || '--';
      const builtAt = lineage?.marts_built_at || lineage?.built_at_utc || '--';
      el.innerHTML = `<strong>Replay lineage</strong>DB ${escapeHtml(sourceDb)} · DuckDB ${escapeHtml(duckdbPath)} · Built ${escapeHtml(builtAt)}`;
    }

    function renderReplaySimpleTable(bodyId, rows, columns, emptyMessage) {
      const tbody = document.getElementById(bodyId);
      if (!tbody) return;
      if (!Array.isArray(rows) || !rows.length) {
        tbody.innerHTML = `<tr><td colspan="${columns.length}" class="research-empty">${escapeHtml(emptyMessage)}</td></tr>`;
        return;
      }
      tbody.innerHTML = rows
        .map((row) => {
          const cells = columns.map((column) => {
            const value = typeof column.render === 'function' ? column.render(row) : row[column.key];
            const tone = typeof column.tone === 'function' ? column.tone(row) : '';
            const style = tone === 'positive'
              ? ' style="color:var(--success)"'
              : tone === 'negative'
                ? ' style="color:var(--danger)"'
                : '';
            return `<td${style}>${escapeHtml(value ?? '--')}</td>`;
          }).join('');
          return `<tr>${cells}</tr>`;
        })
        .join('');
    }

    function renderReplayDay(payload) {
      const safe = payload || {};
      const summary = safe.summary || {};
      const avg = Number(summary.avg_reject_net_bps);
      const win = Number(summary.win_rate_reject);
      const mfe = Number(summary.avg_mfe_bps);
      const mae = Number(summary.avg_mae_bps);

      document.getElementById('replay-date').textContent = summary.event_date || '--';
      document.getElementById('replay-horizon').textContent = summary.primary_horizon != null ? `${summary.primary_horizon}m` : '--';
      document.getElementById('replay-rows').textContent = Number.isFinite(Number(summary.rows))
        ? Intl.NumberFormat('en-US').format(Number(summary.rows))
        : '--';
      document.getElementById('replay-avg').textContent = Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--';
      document.getElementById('replay-avg').style.color = Number.isFinite(avg)
        ? (avg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('replay-win').textContent = Number.isFinite(win) ? formatPercent(win * 100, 1) : '--';
      document.getElementById('replay-mfe-mae').textContent = Number.isFinite(mfe) && Number.isFinite(mae)
        ? `${formatNumber(mfe, 2)} / ${formatNumber(mae, 2)}`
        : '--';

      renderReplaySimpleTable(
        'replay-horizon-body',
        safe.by_horizon || [],
        [
          { key: 'horizon_min', render: (row) => row.horizon_min != null ? `${row.horizon_min}m` : '--' },
          { key: 'rows', render: (row) => Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--' },
          { key: 'avg_reject_net_bps', render: (row) => Number.isFinite(Number(row.avg_reject_net_bps)) ? `${formatNumber(Number(row.avg_reject_net_bps), 2)} bps` : '--', tone: (row) => Number(row.avg_reject_net_bps) >= 0 ? 'positive' : 'negative' },
          { key: 'win_rate_reject', render: (row) => Number.isFinite(Number(row.win_rate_reject)) ? formatPercent(Number(row.win_rate_reject) * 100, 1) : '--' },
          { key: 'avg_mfe_bps', render: (row) => Number.isFinite(Number(row.avg_mfe_bps)) ? `${formatNumber(Number(row.avg_mfe_bps), 2)} bps` : '--' },
          { key: 'avg_mae_bps', render: (row) => Number.isFinite(Number(row.avg_mae_bps)) ? `${formatNumber(Number(row.avg_mae_bps), 2)} bps` : '--' },
        ],
        'Replay day metrics pending.'
      );

      renderReplaySimpleTable(
        'replay-tod-body',
        safe.by_tod || [],
        [
          { key: 'tod_bucket' },
          { key: 'rows', render: (row) => Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--' },
          { key: 'avg_reject_net_bps', render: (row) => Number.isFinite(Number(row.avg_reject_net_bps)) ? `${formatNumber(Number(row.avg_reject_net_bps), 2)} bps` : '--', tone: (row) => Number(row.avg_reject_net_bps) >= 0 ? 'positive' : 'negative' },
          { key: 'win_rate_reject', render: (row) => Number.isFinite(Number(row.win_rate_reject)) ? formatPercent(Number(row.win_rate_reject) * 100, 1) : '--' },
        ],
        'Replay TOD breakdown pending.'
      );

      renderReplaySimpleTable(
        'replay-level-family-body',
        safe.by_level_family || [],
        [
          { key: 'level_family' },
          { key: 'rows', render: (row) => Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--' },
          { key: 'avg_reject_net_bps', render: (row) => Number.isFinite(Number(row.avg_reject_net_bps)) ? `${formatNumber(Number(row.avg_reject_net_bps), 2)} bps` : '--', tone: (row) => Number(row.avg_reject_net_bps) >= 0 ? 'positive' : 'negative' },
          { key: 'win_rate_reject', render: (row) => Number.isFinite(Number(row.win_rate_reject)) ? formatPercent(Number(row.win_rate_reject) * 100, 1) : '--' },
        ],
        'Replay level-family breakdown pending.'
      );

      renderReplaySimpleTable(
        'replay-top-events-body',
        safe.top_events || [],
        [
          { key: 'event_ts_et', render: (row) => row.event_ts_et ? String(row.event_ts_et).slice(11, 16) : '--' },
          { key: 'level_family', render: (row) => row.level_family || '--' },
          { key: 'reject_net_bps', render: (row) => Number.isFinite(Number(row.reject_net_bps)) ? `${formatNumber(Number(row.reject_net_bps), 2)} bps` : '--', tone: (row) => Number(row.reject_net_bps) >= 0 ? 'positive' : 'negative' },
        ],
        'Replay event leaders pending.'
      );

      renderReplaySimpleTable(
        'replay-worst-events-body',
        safe.worst_events || [],
        [
          { key: 'event_ts_et', render: (row) => row.event_ts_et ? String(row.event_ts_et).slice(11, 16) : '--' },
          { key: 'level_family', render: (row) => row.level_family || '--' },
          { key: 'reject_net_bps', render: (row) => Number.isFinite(Number(row.reject_net_bps)) ? `${formatNumber(Number(row.reject_net_bps), 2)} bps` : '--', tone: (row) => Number(row.reject_net_bps) >= 0 ? 'positive' : 'negative' },
        ],
        'Replay event misses pending.'
      );
    }

    function findSessionEntryByDate(targetDate) {
      if (!targetDate || !Array.isArray(state.pivotHistory)) return null;
      return state.pivotHistory.find((entry) => entry?.sessionDate === targetDate || entry?.baseDate === targetDate) || null;
    }

    function openResearchReplayDate(targetDate, { allowReload = true } = {}) {
      const cleanDate = String(targetDate || '').trim();
      if (!cleanDate) return false;

      const entry = findSessionEntryByDate(cleanDate);
      if (entry && state.latestData) {
        const sessionSelect = document.getElementById('session-select');
        if (sessionSelect) {
          sessionSelect.value = String(entry.sessionIndex);
        }
        renderForSession(state.latestData, entry.sessionIndex);
        document.getElementById('tab-replay')?.click();
        showToast(`Opened ${cleanDate} in replay.`, 'success', {
          title: 'Replay Ready',
          duration: 3000,
        });
        return true;
      }

      if (allowReload && state.range !== '1y') {
        state.pendingReplayDate = cleanDate;
        state.range = '1y';
        const rangeSelect = document.getElementById('range-select');
        if (rangeSelect) {
          rangeSelect.value = '1y';
        }
        saveUiPrefs();
        updateAdvancedControlsSummary();
        showToast(`Loading broader history to open ${cleanDate} in replay.`, 'info', {
          title: 'Loading Replay Context',
          duration: 5000,
        });
        loadMarketData();
        return true;
      }

      showToast(`Replay day ${cleanDate} is not available in the current chart history.`, 'warning', {
        title: 'Replay Unavailable',
        duration: 7000,
      });
      return false;
    }

    async function loadReplayDay(force = false) {
      const targetDate = state.lastSessionEntry?.sessionDate || state.pendingReplayDate || null;
      if (!targetDate) {
        setReplayStatus('Replay status: choose a session or open a day from Research.', 'warn');
        return;
      }
      if (!force && state.replay.date === targetDate && state.replay.payload) {
        renderReplayDay(state.replay.payload);
        return;
      }

      state.replay.loading = true;
      setReplayStatus(`Replay status: loading institutional day review for ${targetDate}...`);
      try {
        const primaryHorizon = Number(document.getElementById('research-horizon')?.value || 15);
        const payload = await postJson('/api/research/replay-day', {
          symbol: state.symbol || 'SPY',
          event_date: targetDate,
          primary_horizon: primaryHorizon,
          horizons: [5, 15, 30, 60],
          limit: 8,
        }, 15000);
        state.replay.date = targetDate;
        state.replay.payload = payload;
        state.replay.lastError = '';
        renderReplayDay(payload);
        renderReplayLineage(payload.lineage || {});
        const avg = Number(payload.summary?.avg_reject_net_bps);
        setReplayStatus(
          `Replay status: ${targetDate} loaded · primary ${primaryHorizon}m · avg reject net ${Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--'}`,
          Number.isFinite(avg) && avg < 0 ? 'warn' : ''
        );
      } catch (error) {
        console.error(error);
        state.replay.lastError = String(error?.detailMessage || error?.message || 'Replay day unavailable');
        setReplayStatus(`Replay status: ${state.replay.lastError}`, 'error');
        showToast(`Replay day failed: ${state.replay.lastError}`, 'error', {
          title: 'Replay Error',
          duration: 8000,
        });
      } finally {
        state.replay.loading = false;
      }
    }

    return {
      openResearchReplayDate,
      loadReplayDay,
    };
  }

  global.PQReplayWorkspace = {
    createReplayWorkspace,
  };
}(window));
