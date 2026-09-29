(function attachOpsWorkspace(global) {
  function createOpsWorkspace(deps) {
    const {
      state,
      fetchJson,
      getApiOrigin,
      setLabeledText,
      clipText,
      updateMlDecisionTrace,
      updateSystemStrip,
    } = deps;

    function formatOpsAge(mins) {
      if (!Number.isFinite(mins) || mins < 0) return '--';
      if (mins < 60) return `${Math.round(mins)}m ago`;
      const hours = mins / 60;
      if (hours < 48) return `${hours.toFixed(1)}h ago`;
      return `${(hours / 24).toFixed(1)}d ago`;
    }

    function setOpsField(id, label, value) {
      setLabeledText(id, label, value ?? '--');
    }

    function updateOpsPanel(payload) {
      state.opsStatus = payload || null;
      const governancePanel = state.opsModelGovernance || null;

      if (!payload) {
        setOpsField('ops-backup-age', 'Backup Age', '--');
        setOpsField('ops-backup-status', 'Backup Status', '--');
        setOpsField('ops-drill-status', 'Restore Drill', '--');
        setOpsField('ops-host-status', 'Host Health', '--');
        setOpsField('ops-report-alerts', 'Report Alerts', '--');
        setOpsField('ops-immediate-alerts', 'Immediate Alerts', '--');
        setOpsField('ops-model-review-status', 'Model Review', '--');
        setOpsField('ops-model-reviewed-candidate', 'Top Candidate Review', '--');
        setOpsField('ops-model-review-coverage', 'Review Coverage', '--');
        const backupNote = document.getElementById('ops-backup-note');
        if (backupNote) backupNote.textContent = 'Backup note: --';
        const drillNote = document.getElementById('ops-drill-note');
        if (drillNote) drillNote.textContent = 'Drill note: --';
        const alertNote = document.getElementById('ops-alert-note');
        if (alertNote) alertNote.textContent = 'Alert note: --';
        const modelNote = document.getElementById('ops-model-note');
        if (modelNote) modelNote.textContent = 'Model governance note: --';
        updateMlDecisionTrace(state.lastMlScore);
        updateSystemStrip();
        return;
      }

      const backup = payload.backup || {};
      const drill = payload.restore_drill || {};
      const fullDrill = payload.full_restore_drill || {};
      const host = payload.host_health || {};
      const slo = payload.slo || {};
      const release = payload.release || {};
      const audit = payload.audit || {};
      const predictionLog = payload.prediction_log || {};
      const predictionAlerts = predictionLog.alerts || {};
      const alerts = payload.alerts || {};
      const reportAlerts = alerts.daily_report || {};
      const immediateAlerts = alerts.immediate || {};
      const latestReview = governancePanel?.latest_review || {};
      const latestRegistryModel = governancePanel?.latest_registry_model || {};
      const latestRegistryReview = governancePanel?.latest_registry_review || {};
      const verdictCounts = governancePanel?.verdict_counts || {};

      setOpsField('ops-backup-age', 'Backup Age', formatOpsAge(Number(backup.age_min)));
      setOpsField(
        'ops-backup-status',
        'Backup Status',
        `${String(backup.status || 'unknown').toUpperCase()}${backup.snapshot ? ` · ${backup.snapshot}` : ''}`
      );
      setOpsField(
        'ops-drill-status',
        'Restore Drill',
        `${String(drill.status || 'unknown').toUpperCase()}${drill.snapshot ? ` · ${drill.snapshot}` : ''}`
      );
      setOpsField(
        'ops-host-status',
        'Host Health',
        `${String(host.status || 'unknown').toUpperCase()} · warn ${Number(host.warn_count || 0)} · crit ${Number(host.crit_count || 0)}`
      );
      setOpsField(
        'ops-report-alerts',
        'Report Alerts',
        `${String(reportAlerts.last_status || 'unknown').toUpperCase()}${Array.isArray(reportAlerts.channels) ? ` · ${reportAlerts.channels.join(',') || 'none'}` : ''}`
      );
      setOpsField(
        'ops-immediate-alerts',
        'Immediate Alerts',
        `${String(immediateAlerts.last_status || 'unknown').toUpperCase()}${Array.isArray(immediateAlerts.channels) ? ` · ${immediateAlerts.channels.join(',') || 'none'}` : ''}`
      );
      const latestVerdict = String(latestReview?.decision_summary?.verdict || 'none');
      setOpsField(
        'ops-model-review-status',
        'Model Review',
        `${latestVerdict.toUpperCase()}${latestReview?.model?.version ? ` · ${latestReview.model.version}` : ''}`
      );
      setOpsField(
        'ops-model-reviewed-candidate',
        'Top Candidate Review',
        governancePanel?.latest_registry_reviewed
          ? `YES · ${String(latestRegistryReview?.decision_summary?.verdict || 'reviewed').toUpperCase()}`
          : `NO${latestRegistryModel?.version ? ` · ${latestRegistryModel.version}` : ''}`
      );
      setOpsField(
        'ops-model-review-coverage',
        'Review Coverage',
        `${Number(governancePanel?.review_entries || 0)} reviews · ${Number(governancePanel?.reviewed_models || 0)} models`
      );

      const backupNote = document.getElementById('ops-backup-note');
      if (backupNote) {
        const fullPart = fullDrill.status
          ? ` | Full restore: ${String(fullDrill.status || 'unknown').toUpperCase()}`
          : '';
        backupNote.textContent = `Backup note: ${backup.error || 'No backup errors recorded.'}${fullPart}`;
      }
      const drillNote = document.getElementById('ops-drill-note');
      if (drillNote) {
        const restoreMsg = drill.error || 'Restore drill healthy.';
        const fullMsg = fullDrill.error
          ? `Full restore error: ${fullDrill.error}`
          : `Full restore: ${String(fullDrill.status || 'unknown').toUpperCase()}`;
        drillNote.textContent = `Drill note: ${restoreMsg} | ${fullMsg}`;
      }
      const alertNote = document.getElementById('ops-alert-note');
      if (alertNote) {
        const reportTs = reportAlerts.last_timestamp || '--';
        const immediateTs = immediateAlerts.last_timestamp || '--';
        const sloText = String(slo.status || 'unknown').toUpperCase();
        const releaseText = release.active_commit
          ? `${release.active_commit} (${release.active_env || 'unknown'})`
          : 'none';
        const auditText = String(audit.chain_status || 'unknown').toUpperCase();
        const queueDepth = Number(predictionLog.queue_depth);
        const droppedTotal = Number(predictionLog.dropped_total || 0);
        const writeFailTotal = Number(predictionLog.write_fail_total || 0);
        let queueText = 'OK';
        if (predictionAlerts && predictionAlerts.ok === false) {
          queueText = `ALERT depth=${Number.isFinite(queueDepth) ? queueDepth : '--'} dropped=${droppedTotal} fail=${writeFailTotal}`;
        } else if (Number.isFinite(queueDepth)) {
          queueText = `depth=${queueDepth} dropped=${droppedTotal} fail=${writeFailTotal}`;
        }
        alertNote.textContent = `Alert note: report@${reportTs} | immediate@${immediateTs} | slo=${sloText} | release=${releaseText} | audit=${auditText} | prediction_log=${queueText}`;
      }
      const modelNote = document.getElementById('ops-model-note');
      if (modelNote) {
        const reviewedAt = latestReview?.recorded_at_utc || '--';
        const reviewedBy = latestReview?.reviewer || '--';
        const blockedCount = Number(verdictCounts.blocked || 0);
        const researchOnlyCount = Number(verdictCounts.research_only || 0);
        const passCount = Number(verdictCounts.challenger_pass || 0);
        modelNote.textContent = `Model governance note: latest@${reviewedAt} by ${reviewedBy} | passes=${passCount} research_only=${researchOnlyCount} blocked=${blockedCount}`;
      }
      updateMlDecisionTrace(state.lastMlScore);
      updateSystemStrip();
    }

    async function loadOpsStatus() {
      try {
        const [payload, governancePayload] = await Promise.all([
          fetchJson(`${getApiOrigin()}/api/ops/status`, 5000),
          fetchJson(`${getApiOrigin()}/api/models/governance-summary`, 5000),
        ]);
        state.opsModelGovernance = governancePayload || null;
        updateOpsPanel(payload);
      } catch (_error) {
        state.opsModelGovernance = null;
        updateOpsPanel(null);
      }
    }

    return {
      loadOpsStatus,
    };
  }

  global.PQOpsWorkspace = {
    createOpsWorkspace,
  };
}(window));
