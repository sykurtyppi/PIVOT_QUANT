(function attachResearchWorkspace(global) {
  function createResearchWorkspace(deps) {
    const {
      state,
      fetchJson,
      postJson,
      beginWorkspaceRequest,
      isWorkspaceRequestCurrent,
      setResearchStatus,
      showToast,
      escapeHtml,
      formatNumber,
      formatPercent,
      fillSelectOptions,
      subtractDays,
      openResearchReplayDate,
      getActivateInsightTabByName,
    } = deps;

    function renderResearchLineage(lineage) {
      const el = document.getElementById('research-lineage');
      if (!el) return;
      const sourceDb = lineage?.source_db || '--';
      const duckdbPath = lineage?.duckdb_path || '--';
      const builtAt = lineage?.marts_built_at || lineage?.built_at_utc || '--';
      el.innerHTML = `<strong>Lineage</strong>DB ${escapeHtml(sourceDb)} · DuckDB ${escapeHtml(duckdbPath)} · Built ${escapeHtml(builtAt)}`;
    }

    function renderResearchSummary(summary) {
      const safe = summary || {};
      const avg = Number(safe.avg_reject_net_bps);
      const win = Number(safe.win_rate_reject);
      document.getElementById('research-summary-rows').textContent = Number.isFinite(Number(safe.rows))
        ? Intl.NumberFormat('en-US').format(Number(safe.rows))
        : '--';
      document.getElementById('research-summary-days').textContent = Number.isFinite(Number(safe.days))
        ? Intl.NumberFormat('en-US').format(Number(safe.days))
        : '--';
      document.getElementById('research-summary-avg').textContent = Number.isFinite(avg)
        ? `${formatNumber(avg, 2)} bps`
        : '--';
      document.getElementById('research-summary-avg').style.color = Number.isFinite(avg)
        ? (avg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('research-summary-win').textContent = Number.isFinite(win)
        ? formatPercent(win * 100, 1)
        : '--';
      document.getElementById('research-summary-mfe').textContent = Number.isFinite(Number(safe.avg_mfe_bps))
        ? `${formatNumber(Number(safe.avg_mfe_bps), 2)} bps`
        : '--';
      document.getElementById('research-summary-mae').textContent = Number.isFinite(Number(safe.avg_mae_bps))
        ? `${formatNumber(Number(safe.avg_mae_bps), 2)} bps`
        : '--';
    }

    function renderResearchGroups(groups) {
      const tbody = document.getElementById('research-groups-body');
      if (!tbody) return;
      if (!Array.isArray(groups) || !groups.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="research-empty">No grouped rows matched the current slice.</td></tr>';
        return;
      }

      tbody.innerHTML = groups
        .slice(0, 18)
        .map((row) => {
          const avg = Number(row.avg_reject_net_bps);
          const avgStyle = Number.isFinite(avg)
            ? ` style="color:${avg >= 0 ? 'var(--success)' : 'var(--danger)'}"`
            : '';
          return `
            <tr>
              <td>${escapeHtml(row.horizon_min != null ? `${row.horizon_min}m` : '--')}</td>
              <td>${escapeHtml(row.regime_bucket || '--')}</td>
              <td>${escapeHtml(row.level_family || '--')}</td>
              <td>${escapeHtml(row.tod_bucket || '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--')}</td>
              <td${avgStyle}>${escapeHtml(Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(row.win_rate_reject)) ? formatPercent(Number(row.win_rate_reject) * 100, 1) : '--')}</td>
            </tr>
          `;
        })
        .join('');
    }

    function renderResearchHeatmap(cells, metricLabel = 'avg_reject_net_bps') {
      const container = document.getElementById('research-heatmap');
      if (!container) return;
      if (!Array.isArray(cells) || !cells.length) {
        container.innerHTML = '<div class="research-empty">No expectancy-map cells matched the current slice.</div>';
        return;
      }

      const heatmapLabel = metricLabel === 'avg_reject_net_bps' ? 'Avg reject net' : metricLabel;
      const grid = cells
        .slice(0, 12)
        .map((cell) => {
          const value = Number(cell.metric_value);
          const tone = Number.isFinite(value) ? (value >= 0 ? 'good' : 'bad') : '';
          return `
            <div class="research-heatmap-cell ${tone}">
              <div class="research-heatmap-axis">${escapeHtml(cell.axis_x || '--')} · ${escapeHtml(cell.axis_y || '--')}</div>
              <div class="research-heatmap-value">${escapeHtml(Number.isFinite(value) ? `${formatNumber(value, 2)} bps` : '--')}</div>
              <div class="research-heatmap-meta">${escapeHtml(`${heatmapLabel} · ${Number.isFinite(Number(cell.rows)) ? Intl.NumberFormat('en-US').format(Number(cell.rows)) : '--'} rows`)}</div>
            </div>
          `;
        })
        .join('');

      container.innerHTML = `<div class="research-heatmap-grid">${grid}</div>`;
    }

    function formatWindowLabel(windowPayload, fieldPrefix) {
      const start = windowPayload?.[`${fieldPrefix}_start`];
      const end = windowPayload?.[`${fieldPrefix}_end`];
      if (!start || !end) return '--';
      return `${start} → ${end}`;
    }

    function renderResearchWalkforward(payload) {
      const safe = payload || {};
      const aggregate = safe.aggregate || {};
      const windows = Array.isArray(safe.windows) ? safe.windows : [];
      const meanTest = Number(aggregate.mean_test_avg_reject_net_bps);
      const meanWin = Number(aggregate.mean_test_win_rate_reject);
      const bestAvg = Number(aggregate.best_window?.test_avg_reject_net_bps);
      const worstAvg = Number(aggregate.worst_window?.test_avg_reject_net_bps);

      document.getElementById('research-wf-windows').textContent = Number.isFinite(Number(aggregate.windows))
        ? Intl.NumberFormat('en-US').format(Number(aggregate.windows))
        : '--';
      document.getElementById('research-wf-positive').textContent = Number.isFinite(Number(aggregate.positive_windows))
        ? `${Intl.NumberFormat('en-US').format(Number(aggregate.positive_windows))} (${Number.isFinite(Number(aggregate.positive_window_rate)) ? formatPercent(Number(aggregate.positive_window_rate) * 100, 1) : '--'})`
        : '--';
      document.getElementById('research-wf-mean-test').textContent = Number.isFinite(meanTest)
        ? `${formatNumber(meanTest, 2)} bps`
        : '--';
      document.getElementById('research-wf-mean-test').style.color = Number.isFinite(meanTest)
        ? (meanTest >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('research-wf-mean-win').textContent = Number.isFinite(meanWin)
        ? formatPercent(meanWin * 100, 1)
        : '--';
      document.getElementById('research-wf-best').textContent = Number.isFinite(bestAvg)
        ? `${formatNumber(bestAvg, 2)} bps`
        : '--';
      document.getElementById('research-wf-best').style.color = Number.isFinite(bestAvg)
        ? (bestAvg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('research-wf-worst').textContent = Number.isFinite(worstAvg)
        ? `${formatNumber(worstAvg, 2)} bps`
        : '--';
      document.getElementById('research-wf-worst').style.color = Number.isFinite(worstAvg)
        ? (worstAvg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';

      const tbody = document.getElementById('research-wf-body');
      if (!tbody) return;
      if (!windows.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="research-empty">Not enough slice coverage to form a full walk-forward window yet.</td></tr>';
        return;
      }

      tbody.innerHTML = windows
        .slice(0, 18)
        .map((windowPayload) => {
          const avg = Number(windowPayload.test_avg_reject_net_bps);
          const avgStyle = Number.isFinite(avg)
            ? ` style="color:${avg >= 0 ? 'var(--success)' : 'var(--danger)'}"`
            : '';
          return `
            <tr>
              <td>${escapeHtml(formatWindowLabel(windowPayload, 'train'))}</td>
              <td>${escapeHtml(formatWindowLabel(windowPayload, 'test'))}</td>
              <td>${escapeHtml(Number.isFinite(Number(windowPayload.rows_test)) ? Intl.NumberFormat('en-US').format(Number(windowPayload.rows_test)) : '--')}</td>
              <td${avgStyle}>${escapeHtml(Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(windowPayload.test_win_rate_reject)) ? formatPercent(Number(windowPayload.test_win_rate_reject) * 100, 1) : '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(windowPayload.test_positive_days)) ? String(windowPayload.test_positive_days) : '--')}</td>
              <td><button class="table-action-btn research-replay-btn" type="button" data-replay-date="${escapeHtml(windowPayload.test_start || '')}">Open Day</button></td>
            </tr>
          `;
        })
        .join('');
    }

    function renderResearchDayTable(bodyId, rows, emptyMessage) {
      const tbody = document.getElementById(bodyId);
      if (!tbody) return;
      if (!Array.isArray(rows) || !rows.length) {
        tbody.innerHTML = `<tr><td colspan="4" class="research-empty">${escapeHtml(emptyMessage)}</td></tr>`;
        return;
      }
      tbody.innerHTML = rows
        .slice(0, 8)
        .map((row) => {
          const avg = Number(row.avg_reject_net_bps);
          const avgStyle = Number.isFinite(avg)
            ? ` style="color:${avg >= 0 ? 'var(--success)' : 'var(--danger)'}"`
            : '';
          const replayDate = row.event_date_et || '';
          return `
            <tr>
              <td>${escapeHtml(replayDate || '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--')}</td>
              <td${avgStyle}>${escapeHtml(Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--')}</td>
              <td><button class="table-action-btn research-replay-btn" type="button" data-replay-date="${escapeHtml(replayDate)}">Open Day</button></td>
            </tr>
          `;
        })
        .join('');
    }

    function renderResearchDrilldown(payload) {
      const safe = payload || {};
      renderResearchDayTable('research-best-days-body', safe.best_days || [], 'Best-day cohort pending.');
      renderResearchDayTable('research-worst-days-body', safe.worst_days || [], 'Worst-day cohort pending.');
      renderResearchDayTable('research-recent-days-body', safe.recent_days || [], 'Recent-day cohort pending.');
    }

    function buildResearchRequestPayload() {
      const horizon = Number(document.getElementById('research-horizon')?.value || 15);
      const dateFrom = document.getElementById('research-date-from')?.value || null;
      const dateTo = document.getElementById('research-date-to')?.value || null;
      const regime = document.getElementById('research-regime')?.value || '';
      const levelFamily = document.getElementById('research-level-family')?.value || '';
      const tod = document.getElementById('research-tod')?.value || '';
      const confluence = document.getElementById('research-confluence')?.value || '';

      return {
        symbol: state.symbol || 'SPY',
        date_from: dateFrom,
        date_to: dateTo,
        horizons: [horizon],
        filters: {
          regime_bucket: regime ? [regime] : [],
          level_family: levelFamily ? [levelFamily] : [],
          tod_bucket: tod ? [tod] : [],
          confluence_bucket: confluence ? [confluence] : [],
        },
        group_by: ['horizon_min', 'regime_bucket', 'level_family', 'tod_bucket'],
        include_daily_curve: true,
        include_distribution: true,
      };
    }

    function buildResearchWalkforwardPayload() {
      const base = buildResearchRequestPayload();
      const trainDays = Math.max(20, Number(document.getElementById('research-train-days')?.value || 180));
      const testDays = Math.max(5, Number(document.getElementById('research-test-days')?.value || 20));
      const stepDays = Math.max(1, Number(document.getElementById('research-step-days')?.value || 20));
      return {
        symbol: base.symbol,
        horizon: base.horizons[0],
        date_from: base.date_from,
        date_to: base.date_to,
        filters: base.filters,
        train_days: trainDays,
        test_days: testDays,
        step_days: stepDays,
        baseline: 'slice_expectancy',
      };
    }

    function syncResearchControls(metadata) {
      if (!metadata) return;
      fillSelectOptions('research-regime', metadata.dimensions?.regime_bucket || [], 'All regimes');
      fillSelectOptions('research-level-family', metadata.dimensions?.level_family || [], 'All level families');
      fillSelectOptions('research-tod', metadata.dimensions?.tod_bucket || [], 'All session buckets');
      fillSelectOptions('research-confluence', metadata.dimensions?.confluence_bucket || [], 'All confluence');

      const horizonSelect = document.getElementById('research-horizon');
      if (horizonSelect && Array.isArray(metadata.horizons) && metadata.horizons.length) {
        const wanted = metadata.horizons.includes(15) ? '15' : String(metadata.horizons[0]);
        if (Array.from(horizonSelect.options).some((option) => option.value === wanted)) {
          horizonSelect.value = wanted;
        }
      }

      const fromInput = document.getElementById('research-date-from');
      const toInput = document.getElementById('research-date-to');
      const trainInput = document.getElementById('research-train-days');
      const testInput = document.getElementById('research-test-days');
      const stepInput = document.getElementById('research-step-days');
      const maxDate = metadata.date_range?.max_date || '';
      const minDate = metadata.date_range?.min_date || '';
      if (toInput && !toInput.value) {
        toInput.value = maxDate;
      }
      if (fromInput && !fromInput.value) {
        const oneYearBack = subtractDays(maxDate, 365);
        fromInput.value = minDate && oneYearBack < minDate ? minDate : oneYearBack;
      }
      if (trainInput && !trainInput.value) trainInput.value = '180';
      if (testInput && !testInput.value) testInput.value = '20';
      if (stepInput && !stepInput.value) stepInput.value = '20';

      renderResearchLineage(metadata.lineage || {});
    }

    async function loadResearchMetadata(force = false) {
      if (!force && state.research.metadata) {
        return state.research.metadata;
      }
      const requestSeq = beginWorkspaceRequest('research', 'metadataRequestSeq');
      const payload = await fetchJson('/api/research/metadata', 12000);
      if (!isWorkspaceRequestCurrent('research', requestSeq, 'metadataRequestSeq')) {
        return state.research.metadata;
      }
      state.research.metadata = payload;
      syncResearchControls(payload);
      renderResearchLineage(payload.lineage || {});
      return payload;
    }

    async function runResearchSliceQuery() {
      const requestSeq = beginWorkspaceRequest('research', 'queryRequestSeq');
      state.research.loading = true;
      setResearchStatus('Research status: running slice query, expectancy map, and walk-forward validation...');
      const payload = buildResearchRequestPayload();
      const walkforwardPayload = buildResearchWalkforwardPayload();
      try {
        const [slicePayload, mapPayload, walkforwardPayloadResult, drilldownPayload] = await Promise.all([
          postJson('/api/research/slice-query', payload, 15000),
          postJson('/api/research/expectancy-map', {
            symbol: payload.symbol,
            date_from: payload.date_from,
            date_to: payload.date_to,
            horizon: payload.horizons[0],
            axis_x: 'regime_bucket',
            axis_y: 'tod_bucket',
            metric: 'avg_reject_net_bps',
            filters: payload.filters,
          }, 15000),
          postJson('/api/research/walkforward', walkforwardPayload, 20000),
          postJson('/api/research/cohort-drilldown', {
            symbol: payload.symbol,
            date_from: payload.date_from,
            date_to: payload.date_to,
            horizons: payload.horizons,
            filters: payload.filters,
            limit: 8,
            baseline: 'reject_net_bps',
          }, 15000),
        ]);
        if (!isWorkspaceRequestCurrent('research', requestSeq, 'queryRequestSeq')) {
          return;
        }
        state.research.summary = slicePayload.summary || null;
        state.research.groups = slicePayload.groups || [];
        state.research.heatmap = mapPayload.cells || [];
        state.research.walkforward = walkforwardPayloadResult || null;
        state.research.drilldown = drilldownPayload || null;
        state.research.lastError = '';
        renderResearchSummary(slicePayload.summary || {});
        renderResearchGroups(slicePayload.groups || []);
        renderResearchHeatmap(mapPayload.cells || [], mapPayload.metric || 'avg_reject_net_bps');
        renderResearchWalkforward(walkforwardPayloadResult || {});
        renderResearchDrilldown(drilldownPayload || {});
        renderResearchLineage(slicePayload.lineage || state.research.metadata?.lineage || {});
        const rows = slicePayload.summary?.rows ?? 0;
        const avg = Number(slicePayload.summary?.avg_reject_net_bps);
        const wfMean = Number(walkforwardPayloadResult?.aggregate?.mean_test_avg_reject_net_bps);
        const tone = (Number.isFinite(avg) && avg < 0) || (Number.isFinite(wfMean) && wfMean < 0) ? 'warn' : '';
        setResearchStatus(
          `Research status: pass complete · ${Intl.NumberFormat('en-US').format(Number(rows) || 0)} rows · slice avg ${Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--'} · walk-forward mean ${Number.isFinite(wfMean) ? `${formatNumber(wfMean, 2)} bps` : '--'}`,
          tone
        );
      } catch (error) {
        if (!isWorkspaceRequestCurrent('research', requestSeq, 'queryRequestSeq')) {
          return;
        }
        console.error(error);
        state.research.lastError = String(error?.detailMessage || error?.message || 'Research query failed');
        renderResearchGroups([]);
        renderResearchHeatmap([]);
        renderResearchWalkforward({});
        renderResearchDrilldown({});
        setResearchStatus(`Research status: ${state.research.lastError}`, 'error');
        showToast(`Research query failed: ${state.research.lastError}`, 'error', {
          title: 'Research API Error',
          duration: 9000,
        });
      } finally {
        if (isWorkspaceRequestCurrent('research', requestSeq, 'queryRequestSeq')) {
          state.research.loading = false;
        }
      }
    }

    async function loadResearchWorkspace(force = false) {
      if (state.research.initialized && !force) {
        return;
      }
      const requestSeq = beginWorkspaceRequest('research', 'workspaceRequestSeq');
      try {
        await loadResearchMetadata(force);
        if (!isWorkspaceRequestCurrent('research', requestSeq, 'workspaceRequestSeq')) {
          return;
        }
        await runResearchSliceQuery();
        if (!isWorkspaceRequestCurrent('research', requestSeq, 'workspaceRequestSeq')) {
          return;
        }
        state.research.initialized = true;
      } catch (error) {
        if (!isWorkspaceRequestCurrent('research', requestSeq, 'workspaceRequestSeq')) {
          return;
        }
        console.error(error);
        const detail = String(error?.detailMessage || error?.message || 'Research workspace unavailable');
        setResearchStatus(`Research status: ${detail}`, 'error');
      }
    }

    function resetResearchFilters() {
      const selectsToClear = [
        'research-regime',
        'research-level-family',
        'research-tod',
        'research-confluence',
      ];
      selectsToClear.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
      const fromInput = document.getElementById('research-date-from');
      const toInput = document.getElementById('research-date-to');
      const trainInput = document.getElementById('research-train-days');
      const testInput = document.getElementById('research-test-days');
      const stepInput = document.getElementById('research-step-days');
      if (fromInput) fromInput.value = '';
      if (toInput) toInput.value = '';
      if (trainInput) trainInput.value = '180';
      if (testInput) testInput.value = '20';
      if (stepInput) stepInput.value = '20';
      syncResearchControls(state.research.metadata);
      runResearchSliceQuery();
    }

    function applyModelResearchContext(handoff) {
      if (!handoff) return false;
      const fromInput = document.getElementById('research-date-from');
      const toInput = document.getElementById('research-date-to');
      const horizonSelect = document.getElementById('research-horizon');
      const regimeSelect = document.getElementById('research-regime');
      if (fromInput && handoff.date_from) fromInput.value = handoff.date_from;
      if (toInput && handoff.date_to) toInput.value = handoff.date_to;
      if (horizonSelect && handoff.preferred_horizon != null) {
        const wanted = String(handoff.preferred_horizon);
        if (Array.from(horizonSelect.options).some((option) => option.value === wanted)) {
          horizonSelect.value = wanted;
        }
      }
      if (regimeSelect && handoff.suggested_regime_bucket) {
        const wanted = String(handoff.suggested_regime_bucket);
        if (Array.from(regimeSelect.options).some((option) => option.value === wanted)) {
          regimeSelect.value = wanted;
        }
      }
      return true;
    }

    async function openModelResearchContext() {
      const handoff = state.models.benchmarks?.benchmarks?.research_handoff;
      if (!handoff?.date_from || !handoff?.date_to) {
        showToast('Selected model does not expose a tune window yet.', 'warn', {
          title: 'Model Context Missing',
          duration: 5000,
        });
        return;
      }
      await loadResearchMetadata();
      applyModelResearchContext(handoff);
      getActivateInsightTabByName?.()?.('research', { triggerLoad: false });
      await runResearchSliceQuery();
      showToast(`Opened ${handoff.date_from} → ${handoff.date_to} in Research.`, 'success', {
        title: 'Research Context Ready',
        duration: 3500,
      });
    }

    async function openGovernanceResearchContext(handoff) {
      if (!handoff?.date_from || !handoff?.date_to) {
        showToast('Governance row does not expose a research handoff window yet.', 'warn', {
          title: 'Governance Context Missing',
          duration: 5000,
        });
        return;
      }
      await loadResearchMetadata();
      applyModelResearchContext({
        date_from: handoff.date_from,
        date_to: handoff.date_to,
        preferred_horizon: handoff.preferred_horizon,
        suggested_regime_bucket: handoff.suggested_regime_bucket,
      });
      getActivateInsightTabByName?.()?.('research', { triggerLoad: false });
      await runResearchSliceQuery();
      showToast(`Opened ${handoff.date_from} → ${handoff.date_to} from Governance.`, 'success', {
        title: 'Governance Research Handoff',
        duration: 3500,
      });
    }

    function bindResearchDomEvents() {
      const researchRunBtn = document.getElementById('research-run-btn');
      if (researchRunBtn) {
        researchRunBtn.addEventListener('click', () => {
          runResearchSliceQuery();
        });
      }

      const researchResetBtn = document.getElementById('research-reset-btn');
      if (researchResetBtn) {
        researchResetBtn.addEventListener('click', () => {
          resetResearchFilters();
        });
      }

      const researchPanel = document.getElementById('panel-research');
      if (researchPanel) {
        researchPanel.addEventListener('click', (event) => {
          const replayButton = event.target.closest('.research-replay-btn');
          if (!replayButton) return;
          const replayDate = replayButton.getAttribute('data-replay-date');
          if (replayDate) {
            openResearchReplayDate(replayDate);
          }
        });
      }
    }

    return {
      bindResearchDomEvents,
      loadResearchMetadata,
      runResearchSliceQuery,
      loadResearchWorkspace,
      resetResearchFilters,
      openModelResearchContext,
      openGovernanceResearchContext,
    };
  }

  global.PQResearchWorkspace = Object.freeze({
    createResearchWorkspace,
  });
})(window);
