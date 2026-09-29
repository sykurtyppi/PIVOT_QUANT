(function attachGovernanceWorkspace(global) {
  function createGovernanceWorkspace(deps) {
    const {
      state,
      fetchJson,
      beginWorkspaceRequest,
      isWorkspaceRequestCurrent,
      setGovernanceStatus,
      showToast,
      escapeHtml,
      formatNumber,
      openGovernanceResearchContext,
      openResearchReplayDate,
      getActivateInsightTabByName,
      getLoadModelsWorkspace,
    } = deps;

    function renderGovernanceLineage(payload) {
      const el = document.getElementById('governance-lineage');
      if (!el) return;
      const registryRoots = Array.isArray(payload?.registry_roots) && payload.registry_roots.length
        ? payload.registry_roots.join(' · ')
        : '--';
      const reviewLog = payload?.review_log_path || '--';
      el.innerHTML = `<strong>Governance lineage</strong>Registry ${escapeHtml(registryRoots)} · Review Log ${escapeHtml(reviewLog)}`;
    }

    function renderGovernanceSummary(payload) {
      const summary = payload?.summary || {};
      const latestVerdict = summary.latest_review_verdict || '--';
      const latestCandidate = summary.latest_registry_version || '--';
      const reviewedModels = Number(summary.reviewed_models);
      const reviewEntries = Number(summary.review_entries);

      document.getElementById('governance-pending-review').textContent = Number.isFinite(Number(summary.pending_review_count))
        ? Intl.NumberFormat('en-US').format(Number(summary.pending_review_count))
        : '--';
      document.getElementById('governance-needs-refresh').textContent = Number.isFinite(Number(summary.needs_refresh_count))
        ? Intl.NumberFormat('en-US').format(Number(summary.needs_refresh_count))
        : '--';
      document.getElementById('governance-reviewed-models').textContent = Number.isFinite(reviewedModels)
        ? Intl.NumberFormat('en-US').format(reviewedModels)
        : '--';
      document.getElementById('governance-latest-verdict').textContent = latestVerdict;
      document.getElementById('governance-latest-verdict').style.color = latestVerdict === 'challenger_pass'
        ? 'var(--success)'
        : latestVerdict === 'blocked'
          ? 'var(--danger)'
          : latestVerdict === 'research_only'
            ? 'var(--warning)'
            : '';
      document.getElementById('governance-latest-candidate').textContent = latestCandidate;
      document.getElementById('governance-coverage').textContent = Number.isFinite(reviewEntries) && Number.isFinite(reviewedModels)
        ? `${Intl.NumberFormat('en-US').format(reviewEntries)} reviews · ${Intl.NumberFormat('en-US').format(reviewedModels)} models`
        : '--';
    }

    function renderGovernanceQueue(rows) {
      const body = document.getElementById('governance-queue-body');
      if (!body) return;
      if (!Array.isArray(rows) || !rows.length) {
        body.innerHTML = '<tr><td colspan="8" class="research-empty">No governance queue rows are available yet.</td></tr>';
        return;
      }
      body.innerHTML = rows.map((row) => {
        const selectedUtility = Number(row.selected_utility_avg);
        const utilityStyle = Number.isFinite(selectedUtility)
          ? ` style="color:${selectedUtility >= 0 ? 'var(--success)' : 'var(--danger)'}"`
          : '';
        const reviewState = String(row.review_state || '--');
        const reviewStyle = reviewState === 'challenger_pass'
          ? 'color:var(--success)'
          : reviewState === 'blocked'
            ? 'color:var(--danger)'
            : reviewState === 'research_only' || reviewState === 'needs_refresh'
              ? 'color:var(--warning)'
              : '';
        const handoff = row.research_handoff || {};
        return `
          <tr>
            <td>${escapeHtml(row.version || '--')}</td>
            <td>${escapeHtml(row.status || '--')}</td>
            <td style="${reviewStyle}">${escapeHtml(reviewState)}</td>
            <td>${escapeHtml(row.preferred_horizon != null ? `${row.preferred_horizon}m · ${row.preferred_regime || 'all'}` : '--')}</td>
            <td${utilityStyle}>${escapeHtml(Number.isFinite(selectedUtility) ? `${formatNumber(selectedUtility, 2)} bps` : '--')}</td>
            <td>${escapeHtml(row.peer_rank || '--')}</td>
            <td>${escapeHtml(row.latest_reviewed_at_utc || '--')}</td>
            <td>
              <div class="control-actions-inline">
                <button class="table-action-btn governance-open-model-btn" type="button" data-model-id="${escapeHtml(row.id || '')}">Model</button>
                <button class="table-action-btn governance-open-research-btn" type="button" data-model-id="${escapeHtml(row.id || '')}" data-date-from="${escapeHtml(handoff.date_from || '')}" data-date-to="${escapeHtml(handoff.date_to || '')}" data-horizon="${escapeHtml(handoff.preferred_horizon != null ? String(handoff.preferred_horizon) : '')}" data-regime="${escapeHtml(handoff.suggested_regime_bucket || '')}">Research</button>
                <button class="table-action-btn governance-open-replay-btn" type="button" data-replay-date="${escapeHtml(handoff.replay_date || '')}">Replay</button>
              </div>
            </td>
          </tr>
        `;
      }).join('');
    }

    function renderGovernanceReviews(rows) {
      const body = document.getElementById('governance-reviews-body');
      if (!body) return;
      if (!Array.isArray(rows) || !rows.length) {
        body.innerHTML = '<tr><td colspan="7" class="research-empty">No committee activity is recorded yet.</td></tr>';
        return;
      }
      body.innerHTML = rows.map((row) => {
        const verdict = String(row.verdict || '--');
        const verdictStyle = verdict === 'challenger_pass'
          ? 'color:var(--success)'
          : verdict === 'blocked'
            ? 'color:var(--danger)'
            : verdict === 'research_only'
              ? 'color:var(--warning)'
              : '';
        return `
          <tr>
            <td>${escapeHtml(row.recorded_at_utc || '--')}</td>
            <td>${escapeHtml(row.version || '--')}</td>
            <td style="${verdictStyle}">${escapeHtml(verdict)}</td>
            <td>${escapeHtml(Number.isFinite(Number(row.passed_rules)) && Number.isFinite(Number(row.total_rules)) ? `${row.passed_rules}/${row.total_rules}` : '--')}</td>
            <td>${escapeHtml(row.reviewer || '--')}</td>
            <td>${escapeHtml(row.note || '--')}</td>
            <td><button class="table-action-btn governance-open-model-btn" type="button" data-model-id="${escapeHtml(row.model_id || '')}">Open Model</button></td>
          </tr>
        `;
      }).join('');
    }

    async function loadGovernanceWorkspace(force = false) {
      if (!force && state.governance.initialized && state.governance.summary) {
        renderGovernanceSummary({ summary: state.governance.summary });
        renderGovernanceQueue(state.governance.queue);
        renderGovernanceReviews(state.governance.latestReviews);
        renderGovernanceLineage(state.governance.lineage);
        return state.governance;
      }
      const requestSeq = beginWorkspaceRequest('governance');
      state.governance.loading = true;
      setGovernanceStatus('Governance status: loading candidate queue and committee history...');
      try {
        const payload = await fetchJson('/api/models/governance-workspace?limit=12', 12000);
        if (!isWorkspaceRequestCurrent('governance', requestSeq)) {
          return state.governance;
        }
        state.governance.summary = payload.summary || {};
        state.governance.queue = payload.queue || [];
        state.governance.latestReviews = payload.latest_reviews || [];
        state.governance.lineage = payload.lineage || {};
        renderGovernanceSummary(payload);
        renderGovernanceQueue(state.governance.queue);
        renderGovernanceReviews(state.governance.latestReviews);
        renderGovernanceLineage(state.governance.lineage);
        setGovernanceStatus(
          `Governance status: queue loaded · pending ${Number(payload.summary?.pending_review_count || 0)} · refresh ${Number(payload.summary?.needs_refresh_count || 0)}`,
          Number(payload.summary?.needs_refresh_count || 0) > 0 ? 'warn' : ''
        );
        state.governance.initialized = true;
        return state.governance;
      } catch (error) {
        if (!isWorkspaceRequestCurrent('governance', requestSeq)) {
          return state.governance;
        }
        console.error(error);
        state.governance.lastError = String(error?.detailMessage || error?.message || 'Governance workspace unavailable');
        setGovernanceStatus(`Governance status: ${state.governance.lastError}`, 'error');
        showToast(`Governance workspace failed: ${state.governance.lastError}`, 'error', {
          title: 'Governance API Error',
          duration: 9000,
        });
        throw error;
      } finally {
        if (isWorkspaceRequestCurrent('governance', requestSeq)) {
          state.governance.loading = false;
        }
      }
    }

    async function openGovernanceModelContext(modelId) {
      const cleanId = String(modelId || '').trim();
      if (!cleanId) return;
      state.models.selectedId = cleanId;
      getActivateInsightTabByName?.()?.('models', { triggerLoad: false });
      const loadModelsWorkspace = getLoadModelsWorkspace?.();
      if (typeof loadModelsWorkspace === 'function') {
        await loadModelsWorkspace(true);
      }
    }

    function bindGovernanceDomEvents() {
      const governancePanel = document.getElementById('panel-governance');
      if (governancePanel) {
        governancePanel.addEventListener('click', (event) => {
          const modelButton = event.target.closest('.governance-open-model-btn');
          if (modelButton) {
            const modelId = modelButton.getAttribute('data-model-id');
            if (modelId) {
              void openGovernanceModelContext(modelId);
            }
            return;
          }
          const researchButton = event.target.closest('.governance-open-research-btn');
          if (researchButton) {
            void openGovernanceResearchContext({
              date_from: researchButton.getAttribute('data-date-from') || '',
              date_to: researchButton.getAttribute('data-date-to') || '',
              preferred_horizon: researchButton.getAttribute('data-horizon') || '',
              suggested_regime_bucket: researchButton.getAttribute('data-regime') || '',
            });
            return;
          }
          const replayButton = event.target.closest('.governance-open-replay-btn');
          if (replayButton) {
            const replayDate = replayButton.getAttribute('data-replay-date');
            if (replayDate) {
              openResearchReplayDate(replayDate);
            }
          }
        });
      }
    }

    return {
      bindGovernanceDomEvents,
      loadGovernanceWorkspace,
      openGovernanceModelContext,
    };
  }

  global.PQGovernanceWorkspace = {
    createGovernanceWorkspace,
  };
}(window));
