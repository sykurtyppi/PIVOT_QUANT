(function attachModelsWorkspace(global) {
  function createModelsWorkspace(deps) {
    const {
      state,
      fetchJson,
      postJson,
      beginWorkspaceRequest,
      isWorkspaceRequestCurrent,
      setModelsStatus,
      showToast,
      escapeHtml,
      formatNumber,
      formatPercent,
      openModelResearchContext,
      openResearchReplayDate,
      requestOpsRefresh,
    } = deps;

    function renderModelsLineage(roots) {
      const el = document.getElementById('models-lineage');
      if (!el) return;
      const label = Array.isArray(roots) && roots.length ? roots.join(' · ') : '--';
      el.innerHTML = `<strong>Model registry roots</strong>${escapeHtml(label)}`;
    }

    function renderModelsRegistry(models) {
      const tbody = document.getElementById('models-registry-body');
      if (!tbody) return;
      if (!Array.isArray(models) || !models.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="research-empty">No model artifacts were found in the configured registry roots.</td></tr>';
        return;
      }
      tbody.innerHTML = models
        .slice(0, 20)
        .map((model) => {
          const meanUtility = Number(model.mean_selected_utility_avg);
          const utilityStyle = Number.isFinite(meanUtility)
            ? ` style="color:${meanUtility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
            : '';
          const selected = state.models.selectedId === model.id;
          return `
            <tr${selected ? ' style="background:rgba(34,211,238,0.08)"' : ''}>
              <td>${escapeHtml(model.version || '--')}</td>
              <td>${escapeHtml(model.family || '--')}</td>
              <td>${escapeHtml(model.trained_end_utc || '--')}</td>
              <td>${escapeHtml(model.status || '--')}</td>
              <td>${escapeHtml(`${model.guarded_pairs || 0}/${model.pair_count || 0}`)}</td>
              <td${utilityStyle}>${escapeHtml(Number.isFinite(meanUtility) ? `${formatNumber(meanUtility, 2)} bps` : '--')}</td>
              <td><button class="table-action-btn models-inspect-btn" type="button" data-model-id="${escapeHtml(model.id)}">Inspect</button></td>
            </tr>
          `;
        })
        .join('');
    }

    function renderModelDiagnostics(payload) {
      const model = payload?.model || {};
      const metadata = payload?.metadata || {};
      const details = Array.isArray(payload?.horizon_details) ? payload.horizon_details : [];

      document.getElementById('models-selected-version').textContent = model.version || '--';
      document.getElementById('models-selected-feature').textContent = metadata.feature_version || '--';
      document.getElementById('models-selected-status').textContent = model.status || '--';
      document.getElementById('models-selected-trained').textContent = metadata.trained_end_utc || '--';
      const tuneMin = metadata.tune_date_range?.min_event_date_et || '--';
      const tuneMax = metadata.tune_date_range?.max_event_date_et || '--';
      document.getElementById('models-selected-tune').textContent = `${tuneMin} → ${tuneMax}`;
      document.getElementById('models-selected-guards').textContent = `${model.guarded_pairs || 0}/${model.pair_count || 0}`;

      const tbody = document.getElementById('models-diagnostics-body');
      if (!tbody) return;
      if (!details.length) {
        tbody.innerHTML = '<tr><td colspan="8" class="research-empty">No threshold diagnostics found for the selected model.</td></tr>';
        return;
      }
      tbody.innerHTML = details
        .map((detail) => {
          const utility = Number(detail.selected_utility_avg);
          const utilityStyle = Number.isFinite(utility)
            ? ` style="color:${utility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
            : '';
          return `
            <tr>
              <td>${escapeHtml(detail.target || '--')}</td>
              <td>${escapeHtml(detail.horizon != null ? `${detail.horizon}m` : '--')}</td>
              <td>${escapeHtml(detail.calibration || '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(detail.threshold)) ? formatNumber(Number(detail.threshold), 3) : '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(detail.signals)) ? Intl.NumberFormat('en-US').format(Number(detail.signals)) : '--')}</td>
              <td>${escapeHtml(Number.isFinite(Number(detail.precision)) ? formatPercent(Number(detail.precision) * 100, 1) : '--')}</td>
              <td${utilityStyle}>${escapeHtml(Number.isFinite(utility) ? `${formatNumber(utility, 2)} bps` : '--')}</td>
              <td>${escapeHtml(detail.guard_applied ? (detail.guard_reason || 'guarded') : 'clear')}</td>
            </tr>
          `;
        })
        .join('');
    }

    function renderModelBenchmarks(payload) {
      const safe = payload?.benchmarks || {};
      const summary = safe.summary || {};
      const handoff = safe.research_handoff || {};
      const horizonRows = Array.isArray(safe.horizon_rows) ? safe.horizon_rows : [];
      const shadowRows = Array.isArray(safe.shadow_rows) ? safe.shadow_rows : [];

      const rank = Number(summary.registry_rank);
      const peerCount = Number(summary.registry_peer_count);
      const peerMedian = Number(summary.registry_median_utility_avg);
      const bestUtility = Number(summary.registry_best_utility_avg);
      const shadowCount = Number(summary.shadow_policy_count);

      document.getElementById('models-benchmark-rank').textContent = Number.isFinite(rank) && Number.isFinite(peerCount)
        ? `${rank}/${peerCount}`
        : '--';
      document.getElementById('models-benchmark-peer-median').textContent = Number.isFinite(peerMedian)
        ? `${formatNumber(peerMedian, 2)} bps`
        : '--';
      document.getElementById('models-benchmark-peer-median').style.color = Number.isFinite(peerMedian)
        ? (peerMedian >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('models-benchmark-best-peer').textContent = summary.registry_best_version
        ? `${summary.registry_best_version} (${Number.isFinite(bestUtility) ? `${formatNumber(bestUtility, 2)} bps` : '--'})`
        : '--';
      document.getElementById('models-benchmark-shadow').textContent = Number.isFinite(shadowCount)
        ? String(shadowCount)
        : '--';
      document.getElementById('models-benchmark-horizon').textContent = handoff.preferred_horizon != null
        ? `${handoff.preferred_horizon}m`
        : '--';
      document.getElementById('models-benchmark-regime').textContent = handoff.suggested_regime_bucket || '--';

      const benchBody = document.getElementById('models-benchmarks-body');
      if (benchBody) {
        if (!horizonRows.length) {
          benchBody.innerHTML = '<tr><td colspan="8" class="research-empty">No peer benchmark rows found for the selected model.</td></tr>';
        } else {
          benchBody.innerHTML = horizonRows.map((row) => {
            const selectedUtility = Number(row.selected_utility_avg);
            const medianUtility = Number(row.global_median_utility_avg);
            const baseRate = Number(row.baseline_positive_rate);
            const utilityStyle = Number.isFinite(selectedUtility)
              ? ` style="color:${selectedUtility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
              : '';
            const medianStyle = Number.isFinite(medianUtility)
              ? ` style="color:${medianUtility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
              : '';
            return `
              <tr>
                <td>${escapeHtml(row.target || '--')}</td>
                <td>${escapeHtml(row.horizon != null ? `${row.horizon}m` : '--')}</td>
                <td>${escapeHtml(Number.isFinite(Number(row.global_rank)) && Number.isFinite(Number(row.global_peer_count)) ? `${row.global_rank}/${row.global_peer_count}` : '--')}</td>
                <td${utilityStyle}>${escapeHtml(Number.isFinite(selectedUtility) ? `${formatNumber(selectedUtility, 2)} bps` : '--')}</td>
                <td${medianStyle}>${escapeHtml(Number.isFinite(medianUtility) ? `${formatNumber(medianUtility, 2)} bps` : '--')}</td>
                <td>${escapeHtml(row.global_best_version ? `${row.global_best_version} (${Number.isFinite(Number(row.global_best_utility_avg)) ? `${formatNumber(Number(row.global_best_utility_avg), 2)} bps` : '--'})` : '--')}</td>
                <td>${escapeHtml(Number.isFinite(baseRate) ? formatPercent(baseRate * 100, 1) : '--')}</td>
                <td>${escapeHtml(row.best_regime_by_separation || '--')}</td>
              </tr>
            `;
          }).join('');
        }
      }

      const shadowBody = document.getElementById('models-shadow-body');
      if (shadowBody) {
        if (!shadowRows.length) {
          shadowBody.innerHTML = '<tr><td colspan="8" class="research-empty">No shadow policy benchmarks found for the selected model.</td></tr>';
        } else {
          shadowBody.innerHTML = shadowRows.map((row) => {
            const utility = Number(row.emitted_utility_avg);
            const utilityStyle = Number.isFinite(utility)
              ? ` style="color:${utility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
              : '';
            return `
              <tr>
                <td>${escapeHtml(row.policy_name || '--')}</td>
                <td>${escapeHtml(row.side || '--')}</td>
                <td>${escapeHtml(row.horizon != null ? `${row.horizon}m` : '--')}</td>
                <td>${escapeHtml(row.status || '--')}</td>
                <td>${escapeHtml(Number.isFinite(Number(row.emitted_rows)) ? Intl.NumberFormat('en-US').format(Number(row.emitted_rows)) : '--')}</td>
                <td${utilityStyle}>${escapeHtml(Number.isFinite(utility) ? `${formatNumber(utility, 2)} bps` : '--')}</td>
                <td>${escapeHtml(Number.isFinite(Number(row.emitted_positive_rate)) ? formatPercent(Number(row.emitted_positive_rate) * 100, 1) : '--')}</td>
                <td>${escapeHtml(row.reason || '--')}</td>
              </tr>
            `;
          }).join('');
        }
      }
    }

    function renderModelBaselineCompare(payload) {
      const safe = payload?.baseline_compare || {};
      const summary = safe.summary || {};
      const rows = Array.isArray(safe.rows) ? safe.rows : [];
      const windowAvg = Number(summary.preferred_all_regimes_avg_reject_net_bps);
      const regimeAvg = Number(summary.preferred_regime_avg_reject_net_bps);
      const symbol = summary.symbol || '--';
      const preferredHorizon = summary.preferred_horizon != null ? `${summary.preferred_horizon}m` : '--';

      document.getElementById('models-baseline-window').textContent = Number.isFinite(windowAvg)
        ? `${formatNumber(windowAvg, 2)} bps`
        : '--';
      document.getElementById('models-baseline-window').style.color = Number.isFinite(windowAvg)
        ? (windowAvg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('models-baseline-regime').textContent = Number.isFinite(regimeAvg)
        ? `${formatNumber(regimeAvg, 2)} bps`
        : '--';
      document.getElementById('models-baseline-regime').style.color = Number.isFinite(regimeAvg)
        ? (regimeAvg >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';

      const preferredRegimeRow = rows.find((row) => row.baseline_name === 'best_regime_window' && row.horizon === summary.preferred_horizon);
      const preferredDelta = Number(preferredRegimeRow?.delta_vs_model_selected_avg);
      document.getElementById('models-baseline-delta').textContent = Number.isFinite(preferredDelta)
        ? `${formatNumber(preferredDelta, 2)} bps`
        : '--';
      document.getElementById('models-baseline-delta').style.color = Number.isFinite(preferredDelta)
        ? (preferredDelta >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';
      document.getElementById('models-baseline-rows').textContent = Number.isFinite(Number(summary.preferred_regime_rows))
        ? Intl.NumberFormat('en-US').format(Number(summary.preferred_regime_rows))
        : '--';
      const positiveDays = Number(summary.preferred_regime_positive_days);
      document.getElementById('models-baseline-days').textContent = Number.isFinite(positiveDays)
        ? String(positiveDays)
        : '--';
      document.getElementById('models-baseline-symbol').textContent = `${symbol} · ${preferredHorizon}`;

      const body = document.getElementById('models-baseline-body');
      if (!body) return;
      if (!rows.length) {
        body.innerHTML = '<tr><td colspan="8" class="research-empty">No research baseline rows found for the selected model.</td></tr>';
        return;
      }
      body.innerHTML = rows.map((row) => {
        const avg = Number(row.avg_reject_net_bps);
        const avgStyle = Number.isFinite(avg)
          ? ` style="color:${avg >= 0 ? 'var(--success)' : 'var(--danger)'}"`
          : '';
        const modelDelta = Number(row.delta_vs_model_selected_avg);
        const deltaStyle = Number.isFinite(modelDelta)
          ? ` style="color:${modelDelta >= 0 ? 'var(--success)' : 'var(--danger)'}"`
          : '';
        return `
          <tr>
            <td>${escapeHtml(row.target || '--')}</td>
            <td>${escapeHtml(row.horizon != null ? `${row.horizon}m` : '--')}</td>
            <td>${escapeHtml(row.baseline_name || '--')}</td>
            <td>${escapeHtml(row.regime_bucket || 'all')}</td>
            <td>${escapeHtml(Number.isFinite(Number(row.rows)) ? Intl.NumberFormat('en-US').format(Number(row.rows)) : '--')}</td>
            <td${avgStyle}>${escapeHtml(Number.isFinite(avg) ? `${formatNumber(avg, 2)} bps` : '--')}</td>
            <td>${escapeHtml(Number.isFinite(Number(row.win_rate_reject)) ? formatPercent(Number(row.win_rate_reject) * 100, 1) : '--')}</td>
            <td${deltaStyle}>${escapeHtml(Number.isFinite(modelDelta) ? `${formatNumber(modelDelta, 2)} bps` : '--')}</td>
          </tr>
        `;
      }).join('');
    }

    function renderModelDecision(payload) {
      const safe = payload?.decision || {};
      const summary = safe.summary || {};
      const rules = Array.isArray(safe.rules) ? safe.rules : [];

      const headlineEl = document.getElementById('models-decision-headline');
      if (headlineEl) {
        headlineEl.textContent = `Decision status: ${summary.headline || 'No challenger decision available.'}`;
        headlineEl.style.color = summary.verdict === 'challenger_pass'
          ? 'var(--success)'
          : summary.verdict === 'blocked'
            ? 'var(--danger)'
            : summary.verdict === 'research_only'
              ? 'var(--warning)'
              : '';
      }

      document.getElementById('models-decision-verdict').textContent = summary.verdict || '--';
      document.getElementById('models-decision-verdict').style.color = summary.verdict === 'challenger_pass'
        ? 'var(--success)'
        : summary.verdict === 'blocked'
          ? 'var(--danger)'
          : summary.verdict === 'research_only'
            ? 'var(--warning)'
            : '';
      document.getElementById('models-decision-rules').textContent = Number.isFinite(Number(summary.passed_rules)) && Number.isFinite(Number(summary.total_rules))
        ? `${summary.passed_rules}/${summary.total_rules}`
        : '--';
      document.getElementById('models-decision-rank').textContent = summary.peer_rank || '--';

      const utility = Number(summary.selected_utility_avg);
      document.getElementById('models-decision-utility').textContent = Number.isFinite(utility)
        ? `${formatNumber(utility, 2)} bps`
        : '--';
      document.getElementById('models-decision-utility').style.color = Number.isFinite(utility)
        ? (utility >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';

      const windowDelta = Number(summary.delta_vs_window_baseline);
      document.getElementById('models-decision-window-delta').textContent = Number.isFinite(windowDelta)
        ? `${formatNumber(windowDelta, 2)} bps`
        : '--';
      document.getElementById('models-decision-window-delta').style.color = Number.isFinite(windowDelta)
        ? (windowDelta >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';

      const regimeDelta = Number(summary.delta_vs_regime_baseline);
      document.getElementById('models-decision-regime-delta').textContent = Number.isFinite(regimeDelta)
        ? `${formatNumber(regimeDelta, 2)} bps`
        : '--';
      document.getElementById('models-decision-regime-delta').style.color = Number.isFinite(regimeDelta)
        ? (regimeDelta >= 0 ? 'var(--success)' : 'var(--danger)')
        : '';

      const body = document.getElementById('models-decision-body');
      if (!body) return;
      if (!rules.length) {
        body.innerHTML = '<tr><td colspan="5" class="research-empty">No challenger rules available for the selected model.</td></tr>';
        return;
      }
      body.innerHTML = rules.map((rule) => {
        const actual = typeof rule.actual === 'number'
          ? (Math.abs(rule.actual) <= 1 && !String(rule.threshold || '').includes('bps')
            ? formatNumber(rule.actual, 3)
            : formatNumber(rule.actual, 2))
          : (rule.actual ?? '--');
        const outcomeLabel = rule.passed ? 'PASS' : 'FAIL';
        const outcomeStyle = rule.passed ? 'color:var(--success)' : 'color:var(--danger)';
        return `
          <tr>
            <td>${escapeHtml(rule.label || '--')}</td>
            <td style="${outcomeStyle}">${escapeHtml(outcomeLabel)}</td>
            <td>${escapeHtml(actual)}</td>
            <td>${escapeHtml(rule.threshold || '--')}</td>
            <td>${escapeHtml(rule.note || '--')}</td>
          </tr>
        `;
      }).join('');
    }

    function renderModelReviewLog(payload) {
      const reviews = Array.isArray(payload?.reviews) ? payload.reviews : [];
      const body = document.getElementById('models-review-body');
      if (!body) return;
      if (!reviews.length) {
        body.innerHTML = '<tr><td colspan="7" class="research-empty">No review snapshots recorded for the selected model yet.</td></tr>';
        return;
      }
      body.innerHTML = reviews.map((review) => {
        const verdict = review?.decision_summary?.verdict || '--';
        const passed = review?.decision_summary?.passed_rules;
        const total = review?.decision_summary?.total_rules;
        const reviewId = String(review?.review_id || '');
        const verdictStyle = verdict === 'challenger_pass'
          ? 'color:var(--success)'
          : verdict === 'blocked'
            ? 'color:var(--danger)'
            : verdict === 'research_only'
              ? 'color:var(--warning)'
              : '';
        const compareLabel = state.models.selectedReviewA === reviewId
          ? 'A'
          : state.models.selectedReviewB === reviewId
            ? 'B'
            : 'Select';
        return `
          <tr>
            <td>${escapeHtml(review.recorded_at_utc || '--')}</td>
            <td>${escapeHtml(review?.model?.version || '--')}</td>
            <td style="${verdictStyle}">${escapeHtml(verdict)}</td>
            <td>${escapeHtml(Number.isFinite(Number(passed)) && Number.isFinite(Number(total)) ? `${passed}/${total}` : '--')}</td>
            <td>${escapeHtml(review.reviewer || '--')}</td>
            <td>${escapeHtml(review.note || '--')}</td>
            <td><button class="table-action-btn models-review-select-btn" type="button" data-review-id="${escapeHtml(reviewId)}">${escapeHtml(compareLabel)}</button></td>
          </tr>
        `;
      }).join('');
    }

    function renderModelReviewCompare(payload) {
      const safe = payload || {};
      const summary = safe.summary || {};
      const reviewA = safe.review_a || {};
      const reviewB = safe.review_b || {};
      const rows = Array.isArray(safe.rows) ? safe.rows : [];

      document.getElementById('models-compare-a').textContent = reviewA.review_id
        ? `${reviewA.recorded_at_utc || '--'}`
        : '--';
      document.getElementById('models-compare-b').textContent = reviewB.review_id
        ? `${reviewB.recorded_at_utc || '--'}`
        : '--';
      document.getElementById('models-compare-verdict').textContent = summary.verdict_changed == null
        ? '--'
        : (summary.verdict_changed ? 'changed' : 'same');
      document.getElementById('models-compare-verdict').style.color = summary.verdict_changed
        ? 'var(--warning)'
        : summary.verdict_changed === false
          ? 'var(--success)'
          : '';

      const body = document.getElementById('models-compare-body');
      if (!body) return;
      if (!rows.length) {
        body.innerHTML = '<tr><td colspan="4" class="research-empty">Choose two review snapshots to compare.</td></tr>';
        return;
      }
      body.innerHTML = rows.map((row) => {
        const delta = row.delta;
        const numericDelta = Number(delta);
        const deltaStyle = Number.isFinite(numericDelta)
          ? ` style="color:${numericDelta >= 0 ? 'var(--success)' : 'var(--danger)'}"`
          : '';
        const deltaText = Number.isFinite(numericDelta)
          ? formatNumber(numericDelta, 2)
          : (delta ?? '--');
        return `
          <tr>
            <td>${escapeHtml(row.metric || '--')}</td>
            <td>${escapeHtml(row.value_a ?? '--')}</td>
            <td>${escapeHtml(row.value_b ?? '--')}</td>
            <td${deltaStyle}>${escapeHtml(deltaText)}</td>
          </tr>
        `;
      }).join('');
    }

    async function loadModelRegistry(force = false) {
      if (!force && state.models.registry.length) {
        return state.models.registry;
      }
      const requestSeq = beginWorkspaceRequest('models', 'registryRequestSeq');
      const payload = await fetchJson('/api/models/registry', 12000);
      if (!isWorkspaceRequestCurrent('models', requestSeq, 'registryRequestSeq')) {
        return state.models.registry;
      }
      state.models.registry = payload.models || [];
      state.models.roots = payload.registry_roots || [];
      renderModelsLineage(state.models.roots);
      renderModelsRegistry(state.models.registry);
      return state.models.registry;
    }

    async function loadModelBenchmarks(modelId, force = false) {
      if (!modelId) return null;
      if (!force && state.models.selectedId === modelId && state.models.benchmarks) {
        renderModelBenchmarks(state.models.benchmarks);
        return state.models.benchmarks;
      }
      const payload = await fetchJson(`/api/models/benchmarks?id=${encodeURIComponent(modelId)}`, 12000);
      state.models.benchmarks = payload;
      renderModelBenchmarks(payload);
      return payload;
    }

    async function loadModelBaselineCompare(modelId, force = false) {
      if (!modelId) return null;
      if (!force && state.models.selectedId === modelId && state.models.baselineCompare) {
        renderModelBaselineCompare(state.models.baselineCompare);
        return state.models.baselineCompare;
      }
      const payload = await fetchJson(`/api/models/baseline-compare?id=${encodeURIComponent(modelId)}`, 12000);
      state.models.baselineCompare = payload;
      renderModelBaselineCompare(payload);
      return payload;
    }

    async function loadModelDecision(modelId, force = false) {
      if (!modelId) return null;
      if (!force && state.models.selectedId === modelId && state.models.decision) {
        renderModelDecision(state.models.decision);
        return state.models.decision;
      }
      const payload = await fetchJson(`/api/models/decision?id=${encodeURIComponent(modelId)}`, 12000);
      state.models.decision = payload;
      renderModelDecision(payload);
      return payload;
    }

    async function loadModelReviewLog(modelId, force = false) {
      if (!modelId) return null;
      if (
        !force
        && state.models.selectedId === modelId
        && Array.isArray(state.models.reviewLog)
        && state.models.reviewLog.length
      ) {
        renderModelReviewLog({ reviews: state.models.reviewLog });
        return state.models.reviewLog;
      }
      const requestSeq = beginWorkspaceRequest('models', 'reviewLogRequestSeq');
      const params = new URLSearchParams();
      params.set('id', modelId);
      params.set('limit', '12');
      if (state.models.reviewFilters.verdict) params.set('verdict', state.models.reviewFilters.verdict);
      if (state.models.reviewFilters.reviewer) params.set('reviewer', state.models.reviewFilters.reviewer);
      const payload = await fetchJson(`/api/models/review-log?${params.toString()}`, 12000);
      if (!isWorkspaceRequestCurrent('models', requestSeq, 'reviewLogRequestSeq')) {
        return state.models.reviewLog;
      }
      state.models.reviewLog = payload.reviews || [];
      renderModelReviewLog(payload);
      return state.models.reviewLog;
    }

    async function loadModelReviewCompare(force = false) {
      const reviewIdA = state.models.selectedReviewA;
      const reviewIdB = state.models.selectedReviewB;
      const requestSeq = beginWorkspaceRequest('models', 'reviewCompareRequestSeq');
      if (!reviewIdA || !reviewIdB) {
        if (isWorkspaceRequestCurrent('models', requestSeq, 'reviewCompareRequestSeq')) {
          renderModelReviewCompare({});
        }
        return null;
      }
      if (
        !force
        && state.models.reviewCompare
        && state.models.reviewCompare.summary?.review_id_a === reviewIdA
        && state.models.reviewCompare.summary?.review_id_b === reviewIdB
      ) {
        renderModelReviewCompare(state.models.reviewCompare);
        return state.models.reviewCompare;
      }
      const payload = await fetchJson(`/api/models/review-compare?review_id_a=${encodeURIComponent(reviewIdA)}&review_id_b=${encodeURIComponent(reviewIdB)}`, 12000);
      if (!isWorkspaceRequestCurrent('models', requestSeq, 'reviewCompareRequestSeq')) {
        return state.models.reviewCompare;
      }
      state.models.reviewCompare = payload;
      renderModelReviewCompare(payload);
      return payload;
    }

    function selectModelReviewSnapshot(reviewId) {
      const cleanId = String(reviewId || '').trim();
      if (!cleanId) return;
      if (!state.models.selectedReviewA || state.models.selectedReviewA === cleanId) {
        state.models.selectedReviewA = cleanId;
      } else if (!state.models.selectedReviewB || state.models.selectedReviewB === cleanId) {
        state.models.selectedReviewB = cleanId;
      } else {
        state.models.selectedReviewA = state.models.selectedReviewB;
        state.models.selectedReviewB = cleanId;
      }
      state.models.reviewCompare = null;
      renderModelReviewLog({ reviews: state.models.reviewLog });
      void loadModelReviewCompare(true).catch((error) => {
        console.error(error);
      });
    }

    async function recordModelReviewSnapshot() {
      const modelId = state.models.selectedId;
      if (!modelId) {
        showToast('Select a model before recording a review snapshot.', 'warn', {
          title: 'No Model Selected',
          duration: 4000,
        });
        return;
      }
      const noteInput = document.getElementById('models-review-note');
      const note = noteInput ? String(noteInput.value || '').trim() : '';
      try {
        setModelsStatus('Models status: recording committee snapshot to the review ledger...');
        const payload = await postJson('/api/models/review-log', {
          id: modelId,
          reviewer: 'dashboard_committee',
          note,
        }, 12000);
        state.models.reviewLog = [payload.review, ...(state.models.reviewLog || [])].slice(0, 12);
        state.models.selectedReviewA = payload.review?.review_id || state.models.selectedReviewA;
        renderModelReviewLog({ reviews: state.models.reviewLog });
        if (state.models.selectedReviewA && state.models.selectedReviewB) {
          await loadModelReviewCompare(true);
        }
        setModelsStatus(`Models status: review snapshot recorded for ${payload.review?.model?.version || '--'}.`);
        requestOpsRefresh?.();
        if (noteInput) noteInput.value = '';
        showToast('Committee snapshot recorded.', 'success', {
          title: 'Review Ledger Updated',
          duration: 3500,
        });
      } catch (error) {
        console.error(error);
        const detail = String(error?.detailMessage || error?.message || 'Review snapshot failed');
        setModelsStatus(`Models status: ${detail}`, 'error');
        showToast(`Review snapshot failed: ${detail}`, 'error', {
          title: 'Review Log Error',
          duration: 7000,
        });
      }
    }

    async function loadModelDiagnostics(modelId, force = false) {
      if (!modelId) return;
      if (
        !force
        && state.models.selectedId === modelId
        && state.models.diagnostics
        && state.models.benchmarks
        && state.models.baselineCompare
        && state.models.decision
        && Array.isArray(state.models.reviewLog)
      ) {
        renderModelDiagnostics(state.models.diagnostics);
        renderModelBenchmarks(state.models.benchmarks);
        renderModelBaselineCompare(state.models.baselineCompare);
        renderModelDecision(state.models.decision);
        renderModelReviewLog({ reviews: state.models.reviewLog });
        renderModelReviewCompare(state.models.reviewCompare || {});
        return;
      }
      const requestSeq = beginWorkspaceRequest('models', 'diagnosticsRequestSeq');
      const [payload, benchmarkPayload, baselinePayload, decisionPayload, reviewLogPayload] = await Promise.all([
        fetchJson(`/api/models/diagnostics?id=${encodeURIComponent(modelId)}`, 12000),
        fetchJson(`/api/models/benchmarks?id=${encodeURIComponent(modelId)}`, 12000),
        fetchJson(`/api/models/baseline-compare?id=${encodeURIComponent(modelId)}`, 12000),
        fetchJson(`/api/models/decision?id=${encodeURIComponent(modelId)}`, 12000),
        fetchJson(`/api/models/review-log?id=${encodeURIComponent(modelId)}&limit=12`, 12000),
      ]);
      if (!isWorkspaceRequestCurrent('models', requestSeq, 'diagnosticsRequestSeq')) {
        return;
      }
      state.models.selectedId = modelId;
      state.models.diagnostics = payload;
      state.models.benchmarks = benchmarkPayload;
      state.models.baselineCompare = baselinePayload;
      state.models.decision = decisionPayload;
      state.models.reviewLog = reviewLogPayload.reviews || [];
      state.models.selectedReviewA = '';
      state.models.selectedReviewB = '';
      state.models.reviewCompare = null;
      renderModelsRegistry(state.models.registry);
      renderModelDiagnostics(payload);
      renderModelBenchmarks(benchmarkPayload);
      renderModelBaselineCompare(baselinePayload);
      renderModelDecision(decisionPayload);
      renderModelReviewLog(reviewLogPayload);
      renderModelReviewCompare({});
    }

    async function loadModelsWorkspace(force = false) {
      const requestSeq = beginWorkspaceRequest('models', 'workspaceRequestSeq');
      state.models.loading = true;
      setModelsStatus('Models status: loading model registry, diagnostics, and benchmark context...');
      try {
        const models = await loadModelRegistry(force);
        if (!isWorkspaceRequestCurrent('models', requestSeq, 'workspaceRequestSeq')) {
          return;
        }
        const selectedId = state.models.selectedId || models[0]?.id || '';
        if (selectedId) {
          await loadModelDiagnostics(selectedId, force);
          if (!isWorkspaceRequestCurrent('models', requestSeq, 'workspaceRequestSeq')) {
            return;
          }
          const selectedModel = state.models.diagnostics?.model || {};
          setModelsStatus(
            `Models status: registry loaded · selected ${selectedModel.version || '--'} · ${selectedModel.status || '--'}`,
            selectedModel.status === 'blocked' ? 'warn' : ''
          );
        } else {
          setModelsStatus('Models status: registry loaded but no model artifacts were found.', 'warn');
        }
        state.models.initialized = true;
      } catch (error) {
        if (!isWorkspaceRequestCurrent('models', requestSeq, 'workspaceRequestSeq')) {
          return;
        }
        console.error(error);
        state.models.lastError = String(error?.detailMessage || error?.message || 'Model registry unavailable');
        setModelsStatus(`Models status: ${state.models.lastError}`, 'error');
        showToast(`Model registry failed: ${state.models.lastError}`, 'error', {
          title: 'Model API Error',
          duration: 9000,
        });
      } finally {
        if (isWorkspaceRequestCurrent('models', requestSeq, 'workspaceRequestSeq')) {
          state.models.loading = false;
        }
      }
    }

    function openModelReplayContext() {
      const replayDate = state.models.benchmarks?.benchmarks?.research_handoff?.replay_date;
      if (!replayDate) {
        showToast('Selected model does not expose a replay date yet.', 'warn', {
          title: 'Replay Context Missing',
          duration: 5000,
        });
        return;
      }
      openResearchReplayDate(replayDate);
    }

    function bindModelDomEvents() {
      const modelsPanel = document.getElementById('panel-models');
      if (modelsPanel) {
        modelsPanel.addEventListener('click', (event) => {
          const inspectButton = event.target.closest('.models-inspect-btn');
          if (inspectButton) {
            const modelId = inspectButton.getAttribute('data-model-id');
            if (modelId) {
              loadModelDiagnostics(modelId);
            }
            return;
          }
          const reviewButton = event.target.closest('.models-review-select-btn');
          if (reviewButton) {
            const reviewId = reviewButton.getAttribute('data-review-id');
            if (reviewId) {
              selectModelReviewSnapshot(reviewId);
            }
          }
        });
      }

      const modelsResearchBtn = document.getElementById('models-open-research-btn');
      if (modelsResearchBtn) {
        modelsResearchBtn.addEventListener('click', () => {
          openModelResearchContext();
        });
      }

      const modelsReplayBtn = document.getElementById('models-open-replay-btn');
      if (modelsReplayBtn) {
        modelsReplayBtn.addEventListener('click', () => {
          openModelReplayContext();
        });
      }

      const modelsRecordReviewBtn = document.getElementById('models-record-review-btn');
      if (modelsRecordReviewBtn) {
        modelsRecordReviewBtn.addEventListener('click', () => {
          recordModelReviewSnapshot();
        });
      }

      const modelsReviewRefreshBtn = document.getElementById('models-review-refresh-btn');
      if (modelsReviewRefreshBtn) {
        modelsReviewRefreshBtn.addEventListener('click', () => {
          loadModelReviewLog(state.models.selectedId, true);
        });
      }

      const modelsReviewVerdict = document.getElementById('models-review-filter-verdict');
      if (modelsReviewVerdict) {
        modelsReviewVerdict.addEventListener('change', (event) => {
          state.models.reviewFilters.verdict = String(event.target.value || '');
          loadModelReviewLog(state.models.selectedId, true);
        });
      }

      const modelsReviewReviewer = document.getElementById('models-review-filter-reviewer');
      if (modelsReviewReviewer) {
        modelsReviewReviewer.addEventListener('change', (event) => {
          state.models.reviewFilters.reviewer = String(event.target.value || '').trim();
          loadModelReviewLog(state.models.selectedId, true);
        });
      }
    }

    return {
      bindModelDomEvents,
      loadModelsWorkspace,
      loadModelDiagnostics,
      loadModelReviewLog,
      openModelReplayContext,
    };
  }

  global.PQModelsWorkspace = {
    createModelsWorkspace,
  };
}(window));
