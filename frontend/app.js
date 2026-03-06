/* ─── DataForge Frontend ────────────────────────────────────────────────────── */
'use strict';

const API = window.__DATAFORGE_API__ || window.location.origin;

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  pipelines: [],
  runs: [],
  metrics: null,
  activeWs: null,
  currentRunId: null,
  charts: {},
};

// ─── Utilities ────────────────────────────────────────────────────────────────
const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

function escHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function fmt(n) {
  if (n === null || n === undefined || n === '—') return '—';
  if (typeof n === 'number' && n >= 1000) return n.toLocaleString();
  return n;
}
function fmtMs(ms) {
  if (!ms) return '—';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}
function fmtPct(v) { return v != null ? `${v}%` : '—'; }
function fmtAmt(v) { return v != null ? `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 })}` : '—'; }
function relativeTime(isoStr) {
  if (!isoStr) return '—';
  const diff = Date.now() - new Date(isoStr).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  $('#toastContainer').appendChild(el);
  setTimeout(() => el.remove(), 4000);
}

// ─── Nav / Views ──────────────────────────────────────────────────────────────
function switchView(name) {
  $$('.view').forEach(v => v.classList.remove('active'));
  $$('.nav-link').forEach(l => l.classList.remove('active'));
  $(`#view-${name}`)?.classList.add('active');
  $(`.nav-link[data-view="${name}"]`)?.classList.add('active');
  if (name === 'dashboard') loadDashboard();
  if (name === 'pipelines') loadPipelineList();
  if (name === 'data') loadDataPreview();
  if (name === 'runs') loadRunHistory();
}

$$('.nav-link').forEach(btn => btn.addEventListener('click', () => switchView(btn.dataset.view)));
$('#refreshDashboard')?.addEventListener('click', loadDashboard);

// ─── API health check ─────────────────────────────────────────────────────────
async function checkHealth() {
  try {
    const r = await fetch(`${API}/api/health`);
    const dot = $('#apiStatus'), label = $('#apiStatusLabel');
    if (r.ok) {
      dot.className = 'status-dot ok';
      label.textContent = 'API Online';
    } else {
      dot.className = 'status-dot err';
      label.textContent = 'API Error';
    }
  } catch {
    $('#apiStatus').className = 'status-dot err';
    $('#apiStatusLabel').textContent = 'Offline';
  }
}

// ─── Dashboard ────────────────────────────────────────────────────────────────
async function loadDashboard() {
  await Promise.all([fetchMetrics(), fetchPipelines()]);
  renderKPIs();
  renderCharts();
  renderQuickLaunch();
  await renderRecentRuns();
}

async function renderRecentRuns() {
  try {
    const r = await fetch(`${API}/api/runs?limit=5`);
    const data = await r.json();
    const runs = data.runs || [];
    const el = $('#recentRuns');
    if (!runs.length) {
      el.innerHTML = '<div class="empty-state-sm">No runs yet — go to Pipelines and trigger one.</div>';
      return;
    }
    el.innerHTML = runs.map(r => {
      const q = r.quality_score || 0;
      const qClass = q >= 95 ? 'high' : q >= 85 ? 'med' : 'low';
      return `
        <div class="recent-run-row clickable" onclick="openRunDetail('${escHtml(r.run_id)}')">
          <span class="badge badge-${escHtml(r.status)}" style="min-width:64px;text-align:center">${escHtml(r.status)}</span>
          <span style="font-weight:500;color:var(--text)">${escHtml(r.pipeline_name)}</span>
          <span style="color:var(--text-dim);font-size:11px">${escHtml(r.run_id)}</span>
          <span class="run-quality ${qClass}">${q ? q.toFixed(1) + '%' : '—'}</span>
          <span style="color:var(--text-dim);font-size:11px;margin-left:auto">${relativeTime(r.started_at)}</span>
        </div>
      `;
    }).join('');
  } catch { /* ignore */ }
}

async function fetchMetrics() {
  try {
    const r = await fetch(`${API}/api/metrics`);
    state.metrics = await r.json();
  } catch { state.metrics = null; }
}

async function fetchPipelines() {
  try {
    const r = await fetch(`${API}/api/pipelines`);
    const data = await r.json();
    state.pipelines = data.pipelines || [];
  } catch { state.pipelines = []; }
}

function renderKPIs() {
  const m = state.metrics;
  if (!m) return;
  $('#kpiRecords').textContent = fmt(m.total_records_processed);
  $('#kpiRuns').textContent = fmt(m.total_pipeline_runs);
  $('#kpiSuccess').textContent = fmtPct(m.success_rate);
  $('#kpiQuality').textContent = m.avg_quality_score ? `${m.avg_quality_score}%` : '—';
}

function renderCharts() {
  const m = state.metrics;
  if (!m) return;

  // Records by day chart
  const byDay = [...(m.records_by_day || [])].reverse();
  const labels = byDay.map(d => d.day?.slice(5) || '');
  const values = byDay.map(d => d.records || 0);

  if (state.charts.records) state.charts.records.destroy();
  state.charts.records = new Chart($('#recordsChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Records',
        data: values,
        backgroundColor: 'rgba(99,102,241,0.5)',
        borderColor: '#6366f1',
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: chartOpts(false),
  });

  // Quality by pipeline doughnut chart
  const qData = m.quality_by_pipeline || [];
  if (state.charts.quality) state.charts.quality.destroy();
  state.charts.quality = new Chart($('#qualityChart'), {
    type: 'doughnut',
    data: {
      labels: qData.map(q => q.pipeline_name || ''),
      datasets: [{
        data: qData.map(q => +(q.avg_quality || 0).toFixed(1)),
        backgroundColor: ['rgba(99,102,241,0.7)', 'rgba(139,92,246,0.7)', 'rgba(236,72,153,0.7)'],
        borderColor: ['#6366f1', '#8b5cf6', '#ec4899'],
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '65%',
      plugins: {
        legend: { display: true, position: 'bottom', labels: { color: '#7a7a9a', boxWidth: 12, padding: 14 } },
        tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${ctx.raw}%` } },
      },
    },
  });

  // Pipeline performance bar chart
  const perf = m.pipeline_performance || [];
  if (state.charts.perf) state.charts.perf.destroy();
  if (perf.length) {
    state.charts.perf = new Chart($('#perfChart'), {
      type: 'bar',
      data: {
        labels: perf.map(p => p.pipeline_name || ''),
        datasets: [
          {
            label: 'Avg Duration (s)',
            data: perf.map(p => +((p.avg_duration || 0) / 1000).toFixed(1)),
            backgroundColor: 'rgba(99,102,241,0.6)',
            borderColor: '#6366f1',
            borderWidth: 1,
            borderRadius: 4,
          },
          {
            label: 'Min (s)',
            data: perf.map(p => +((p.min_duration || 0) / 1000).toFixed(1)),
            backgroundColor: 'rgba(16,185,129,0.5)',
            borderColor: '#10b981',
            borderWidth: 1,
            borderRadius: 4,
          },
          {
            label: 'Max (s)',
            data: perf.map(p => +((p.max_duration || 0) / 1000).toFixed(1)),
            backgroundColor: 'rgba(236,72,153,0.5)',
            borderColor: '#ec4899',
            borderWidth: 1,
            borderRadius: 4,
          },
        ],
      },
      options: {
        ...chartOpts(true),
        indexAxis: 'y',
        plugins: {
          ...chartOpts(true).plugins,
          legend: { display: true, position: 'top', labels: { color: '#7a7a9a', boxWidth: 10, padding: 16, font: { size: 11 } } },
        },
      },
    });
  }
}

function chartOpts(legend = true) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: legend, labels: { color: '#7a7a9a' } },
      tooltip: {
        backgroundColor: '#13131f',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
        titleColor: '#e8e8f0',
        bodyColor: '#7a7a9a',
      },
    },
    scales: {
      x: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#7a7a9a', font: { size: 11 } } },
      y: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#7a7a9a', font: { size: 11 } } },
    },
  };
}

function renderQuickLaunch() {
  const grid = $('#pipelineQuickGrid');
  if (!grid) return;
  if (!state.pipelines.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-state-icon">◈</div><h3>No pipelines</h3><p>Start the backend to load pipelines.</p></div>';
    return;
  }
  grid.innerHTML = state.pipelines.map(p => `
    <div class="pipeline-quick-card">
      <div class="pqc-header">
        <div class="pqc-name">${escHtml(p.name)}</div>
        <span class="pqc-source">${escHtml(p.source_type)}</span>
      </div>
      <div class="pqc-desc">${escHtml(p.description)}</div>
      <div class="pqc-meta">
        <span>Runs: <strong>${escHtml(p.total_runs)}</strong></span>
        <span>Success: <strong>${escHtml(p.success_rate)}%</strong></span>
        <span>Quality: <strong>${escHtml(p.avg_quality_score || '—')}%</strong></span>
        <span>Last: <strong>${relativeTime(p.last_run)}</strong></span>
      </div>
      <div class="pqc-actions">
        <button class="btn btn-primary btn-sm" onclick="triggerRun('${escHtml(p.id)}', '${escHtml(p.name)}')">▶ Run Now</button>
        <button class="btn btn-ghost btn-sm" onclick="triggerRun('${escHtml(p.id)}', '${escHtml(p.name)}', true)">Dry Run</button>
      </div>
    </div>
  `).join('');
}

// ─── Pipeline list ─────────────────────────────────────────────────────────────
async function loadPipelineList() {
  await fetchPipelines();
  const el = $('#pipelineList');
  if (!state.pipelines.length) {
    el.innerHTML = '<div class="empty-state"><div class="empty-state-icon">◈</div><h3>No pipelines available</h3><p>Start the backend server.</p></div>';
    return;
  }
  el.innerHTML = state.pipelines.map(p => {
    const statusBadge = p.last_status
      ? `<span class="badge badge-${escHtml(p.last_status)}">${escHtml(p.last_status)}</span>` : '';
    return `
      <div class="pipeline-row">
        <div class="pl-info">
          <div class="pl-name">${escHtml(p.name)} ${statusBadge}</div>
          <div class="pl-desc">${escHtml(p.description)}</div>
          <div class="pl-tags">${(p.tags || []).map(t => `<span class="pl-tag">${escHtml(t)}</span>`).join('')}</div>
          <div class="pl-actions">
            <button class="btn btn-primary btn-sm" onclick="triggerRun('${escHtml(p.id)}', '${escHtml(p.name)}')">▶ Run Now</button>
            <button class="btn btn-ghost btn-sm" onclick="triggerRun('${escHtml(p.id)}', '${escHtml(p.name)}', true)">Dry Run</button>
          </div>
        </div>
        <div class="pl-stats">
          <div class="pl-stat">
            <div class="pl-stat-val">${escHtml(p.total_runs)}</div>
            <div class="pl-stat-label">Runs</div>
          </div>
          <div class="pl-stat">
            <div class="pl-stat-val">${escHtml(p.success_rate)}%</div>
            <div class="pl-stat-label">Success</div>
          </div>
          <div class="pl-stat">
            <div class="pl-stat-val">${fmtMs(p.avg_duration_ms)}</div>
            <div class="pl-stat-label">Avg Duration</div>
          </div>
          <div class="pl-stat">
            <div class="pl-stat-val">${escHtml(p.avg_quality_score || '—')}%</div>
            <div class="pl-stat-label">Quality</div>
          </div>
        </div>
      </div>
    `;
  }).join('');
}

$('#refreshPipelines')?.addEventListener('click', loadPipelineList);

// ─── Data Preview ─────────────────────────────────────────────────────────────
let activeDataSource = 'crypto';

$$('.data-tab').forEach(btn => btn.addEventListener('click', () => {
  $$('.data-tab').forEach(t => t.classList.remove('active'));
  $$('.data-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  activeDataSource = btn.dataset.source;
  $(`#panel-${activeDataSource}`)?.classList.add('active');
  fetchDataForSource(activeDataSource);
}));

async function loadDataPreview() {
  await fetchDataForSource(activeDataSource);
}

async function fetchDataForSource(source) {
  try {
    const r = await fetch(`${API}/api/data/preview?source_type=${source}&limit=100`);
    const data = await r.json();
    const records = data.records || [];
    if (source === 'crypto') renderCryptoTable(records);
    else if (source === 'weather') renderWeatherTable(records);
    else if (source === 'github') renderGithubTable(records);
  } catch (e) {
    toast('Failed to load data preview', 'error');
  }
}

function renderCryptoTable(records) {
  const tbody = $('#tbody-crypto');
  if (!records.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Run the Crypto pipeline to see data.</td></tr>';
    return;
  }
  tbody.innerHTML = records.map(r => `
    <tr>
      <td>${escHtml(r.name || '—')}</td>
      <td><span style="color:var(--text-dim)">${escHtml(r.symbol || '—')}</span></td>
      <td>$${Number(r.current_price || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 6 })}</td>
      <td>$${Number(r.market_cap || 0).toLocaleString()}</td>
      <td class="${(r.price_change_pct_24h || 0) >= 0 ? 'val-pos' : 'val-neg'}">${r.price_change_pct_24h != null ? (r.price_change_pct_24h >= 0 ? '+' : '') + r.price_change_pct_24h.toFixed(2) + '%' : '—'}</td>
      <td>${r.volatility_pct != null ? r.volatility_pct.toFixed(2) + '%' : '—'}</td>
      <td><span class="signal-badge signal-${escHtml(r.signal || 'neutral')}">${escHtml(r.signal || '—')}</span></td>
      <td class="${r.is_anomaly ? 'anomaly-yes' : 'anomaly-no'}">${r.is_anomaly ? '⚠ Yes' : '—'}</td>
    </tr>
  `).join('');
}

function renderWeatherTable(records) {
  const tbody = $('#tbody-weather');
  if (!records.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Run the Weather pipeline to see data.</td></tr>';
    return;
  }
  tbody.innerHTML = records.map(r => `
    <tr>
      <td>${escHtml(r.city || '—')}</td>
      <td>${r.temperature_c != null ? r.temperature_c.toFixed(1) + '°' : '—'}</td>
      <td>${r.apparent_temp_c != null ? r.apparent_temp_c.toFixed(1) + '°' : '—'}</td>
      <td>${r.humidity_pct != null ? r.humidity_pct + '%' : '—'}</td>
      <td>${r.wind_speed_kmh != null ? r.wind_speed_kmh.toFixed(1) : '—'}</td>
      <td>${escHtml(r.condition || '—')}</td>
      <td>${r.heat_index_c != null ? r.heat_index_c.toFixed(1) + '°' : '—'}</td>
      <td class="${r.is_extreme ? 'anomaly-yes' : 'anomaly-no'}">${r.is_extreme ? '⚠ Extreme' : 'Normal'}</td>
    </tr>
  `).join('');
}

function renderGithubTable(records) {
  const tbody = $('#tbody-github');
  if (!records.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Run the GitHub pipeline to see data.</td></tr>';
    return;
  }
  tbody.innerHTML = records.map(r => `
    <tr>
      <td><span class="event-type">${escHtml(r.event_type || '—')}</span></td>
      <td>${escHtml(r.actor || '—')}</td>
      <td style="font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(r.repo_name || '—')}</td>
      <td>${escHtml(r.action || '—')}</td>
      <td>${r.push_size != null ? r.push_size : '—'}</td>
      <td>${r.is_trending_repo ? '<span style="color:var(--amber)">🔥 Trending</span>' : '—'}</td>
      <td class="${r.is_anomaly ? 'anomaly-yes' : 'anomaly-no'}">${r.is_anomaly ? '⚠ Yes' : '—'}</td>
      <td>${relativeTime(r.created_at)}</td>
    </tr>
  `).join('');
}

$('#refreshData')?.addEventListener('click', loadDataPreview);

// ─── Run History ──────────────────────────────────────────────────────────────
async function loadRunHistory() {
  try {
    const r = await fetch(`${API}/api/runs?limit=50`);
    const data = await r.json();
    state.runs = data.runs || [];
    renderRunHistory();
  } catch { toast('Failed to load run history', 'error'); }
}

function renderRunHistory() {
  const el = $('#runList');
  if (!state.runs.length) {
    el.innerHTML = '<div class="empty-state"><div class="empty-state-icon">◈</div><h3>No runs yet</h3><p>Trigger a pipeline to see runs here.</p></div>';
    return;
  }
  el.innerHTML = state.runs.map(r => {
    const q = r.quality_score || 0;
    const qClass = q >= 95 ? 'high' : q >= 85 ? 'med' : 'low';
    return `
      <div class="run-row clickable" onclick="openRunDetail('${escHtml(r.run_id)}')">
        <div class="run-id">${escHtml(r.run_id)}</div>
        <div class="run-pipeline">${escHtml(r.pipeline_name)}</div>
        <div class="run-stat">${fmt(r.records_loaded)} recs</div>
        <div class="run-stat">${fmtMs(r.duration_ms)}</div>
        <div class="run-quality ${qClass}">${q ? `${q.toFixed(1)}%` : '—'}</div>
        <div><span class="badge badge-${escHtml(r.status)}">${escHtml(r.status)}</span></div>
        <div class="text-dim" style="font-size:11px">${relativeTime(r.started_at)}</div>
      </div>
    `;
  }).join('');
}

$('#refreshRuns')?.addEventListener('click', loadRunHistory);

// ─── View Run Detail (click on completed run) ─────────────────────────────────
async function openRunDetail(runId) {
  try {
    const r = await fetch(`${API}/api/runs/${runId}`);
    if (!r.ok) throw new Error('Run not found');
    const run = await r.json();

    // Set modal header
    const statusIcon = run.status === 'success' ? '✓' : run.status === 'failed' ? '✗' : '◈';
    $('#modalTitle').textContent = `${statusIcon} ${run.pipeline_name || 'Pipeline Run'}`;
    $('#modalSub').textContent = `run_id: ${runId} · ${run.status} · ${fmtMs(run.duration_ms)}`;

    // Cleanup previous charts
    if (state.charts.agg) { state.charts.agg.destroy(); state.charts.agg = null; }
    if (state.charts.agg2) { state.charts.agg2.destroy(); state.charts.agg2 = null; }
    $('#aggKpis').innerHTML = '';
    $('#aggSection').style.display = 'none';
    $('#aggChartsRow').style.display = 'none';

    // Show steps
    const steps = run.steps || [];
    const track = $('#stepsTrack');
    if (steps.length) {
      track.innerHTML = steps.map((s, i) => {
        const cls = s.status === 'success' ? 'done' : s.status === 'failed' ? 'failed' : s.status === 'skipped' ? 'done' : '';
        const dot = s.status === 'success' ? '✓' : s.status === 'failed' ? '✗' : s.status === 'skipped' ? '—' : (i + 1);
        return `<div class="step-item ${cls}"><div class="step-dot">${dot}</div><div class="step-label">${escHtml(s.step_name)}</div></div>`;
      }).join('');
    } else {
      track.innerHTML = STEP_DEFS.map((s, i) => `<div class="step-item done"><div class="step-dot">✓</div><div class="step-label">${s.label}</div></div>`).join('');
    }

    // Progress bar (complete)
    $('#progressBarFill').style.width = '100%';
    $('#progressBarFill').style.background = run.status === 'failed' ? 'var(--red)' : 'var(--grad)';
    const summary = run.status === 'success'
      ? `Completed in ${fmtMs(run.duration_ms)} · ${fmt(run.records_loaded)} records · Quality: ${run.quality_score?.toFixed(1) || '—'}%`
      : `Failed: ${run.error_message || 'Unknown error'}`;
    $('#progressLabel').textContent = summary;

    // Quality checks
    const checks = run.quality_checks || [];
    if (checks.length) {
      renderQualityChecks(checks, run.quality_score);
    } else {
      $('#qualitySection').style.display = 'none';
    }

    // Aggregation KPIs from run metadata
    if (run.records_ingested || run.records_transformed || run.records_loaded) {
      $('#aggSection').style.display = 'block';
      const kpis = [
        { label: 'Ingested', value: fmt(run.records_ingested) },
        { label: 'Transformed', value: fmt(run.records_transformed) },
        { label: 'Loaded', value: fmt(run.records_loaded) },
        { label: 'Quality', value: run.quality_score ? `${run.quality_score.toFixed(1)}%` : '—', cls: run.quality_score >= 95 ? 'text-green' : run.quality_score >= 85 ? 'text-amber' : 'text-red' },
        { label: 'Duration', value: fmtMs(run.duration_ms) },
      ];
      $('#aggKpis').innerHTML = kpis.map(k => `<div class="agg-kpi"><div class="agg-kpi-label">${k.label}</div><div class="agg-kpi-value ${k.cls || ''}">${k.value}</div></div>`).join('');
    }

    // Logs
    const logs = run.logs || [];
    $('#logOutput').textContent = '';
    if (logs.length) {
      logs.forEach(line => {
        const span = document.createElement('span');
        if (line.includes('[ERROR]')) span.className = 'log-line-error';
        else if (line.includes('[DQ]') && line.includes('✗')) span.className = 'log-line-warn';
        else if (line.includes('[DONE]')) span.className = 'log-line-success';
        else if (line.includes('[STEP]') || line.includes('[DQ]')) span.className = 'log-line-info';
        span.textContent = line + '\n';
        $('#logOutput').appendChild(span);
      });
    } else {
      $('#logOutput').textContent = 'No logs available for this run.';
    }

    // Show modal
    $('#runModal').style.display = 'flex';

  } catch (e) {
    toast(`Failed to load run details: ${e.message}`, 'error');
  }
}

// ─── Trigger pipeline run ──────────────────────────────────────────────────────
async function triggerRun(pipelineId, pipelineName, dryRun = false) {
  try {
    const r = await fetch(`${API}/api/pipelines/${pipelineId}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: dryRun }),
    });
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    toast(`Pipeline started: ${data.run_id}`, 'success');
    openRunModal(data.run_id, pipelineName, dryRun);
  } catch (e) {
    toast(`Failed to trigger pipeline: ${e.message}`, 'error');
  }
}

// ─── Run Modal & WebSocket ────────────────────────────────────────────────────
const STEP_DEFS = [
  { key: 'ingestion', label: 'Ingest' },
  { key: 'quality', label: 'Quality' },
  { key: 'transformation', label: 'Transform' },
  { key: 'load', label: 'Load' },
];

function openRunModal(runId, pipelineName, dryRun) {
  state.currentRunId = runId;
  $('#modalTitle').textContent = dryRun ? `Dry Run — ${pipelineName}` : `Running — ${pipelineName}`;
  $('#modalSub').textContent = `run_id: ${runId}`;
  $('#qualitySection').style.display = 'none';
  $('#aggSection').style.display = 'none';
  $('#aggKpis').innerHTML = '';
  if (state.charts.agg) { state.charts.agg.destroy(); state.charts.agg = null; }
  if (state.charts.agg2) { state.charts.agg2.destroy(); state.charts.agg2 = null; }
  $('#logOutput').textContent = '';
  $('#progressBarFill').style.width = '0%';
  $('#progressLabel').textContent = 'Initialising…';

  // Reset steps
  const track = $('#stepsTrack');
  track.innerHTML = STEP_DEFS.map((s, i) => `
    <div class="step-item" id="step-${s.key}">
      <div class="step-dot">${i + 1}</div>
      <div class="step-label">${s.label}</div>
    </div>
  `).join('');

  $('#runModal').style.display = 'flex';

  // Close existing WS
  if (state.activeWs) {
    state.activeWs.close();
    state.activeWs = null;
  }

  // Open WebSocket — connect to the API backend (not location.host which may be a static CDN)
  const wsBase = API.replace(/^https/, 'wss').replace(/^http/, 'ws');
  const ws = new WebSocket(`${wsBase}/ws/runs/${runId}`);
  state.activeWs = ws;

  ws.onopen = () => {
    appendLog('[CONNECTED] WebSocket established');
    const hb = setInterval(() => ws.readyState === 1 && ws.send('ping'), 15000);
    ws._hb = hb;
  };

  ws.onmessage = (e) => handleWsEvent(JSON.parse(e.data));

  ws.onerror = () => appendLog('[ERROR] WebSocket error', 'error');
  ws.onclose = () => {
    clearInterval(ws._hb);
    appendLog('[DISCONNECTED] WebSocket closed');
  };

  // Keep polling as fallback for logs
  const poll = setInterval(async () => {
    if (!$('#runModal').style.display || $('#runModal').style.display === 'none') {
      clearInterval(poll);
      return;
    }
    try {
      const r = await fetch(`${API}/api/runs/${runId}/logs`);
      const data = await r.json();
      if (data.status === 'success' || data.status === 'failed') clearInterval(poll);
    } catch { }
  }, 3000);
}

let completedSteps = 0;

function handleWsEvent(msg) {
  const { event } = msg;

  if (event === 'started') {
    appendLog(`[START] Pipeline started${msg.dry_run ? ' (dry run)' : ''}`, 'info');
    completedSteps = 0;
    updateProgress(2, 'Pipeline started…');
  }

  if (event === 'step_start') {
    setStepState(msg.step, 'running');
    appendLog(`[STEP] Starting: ${msg.step}`, 'info');
  }

  if (event === 'step_done') {
    setStepState(msg.step, 'done');
    completedSteps++;
    const pct = Math.min(95, Math.round((completedSteps / 4) * 90));
    updateProgress(pct, `Completed: ${msg.step}`);

    if (msg.step === 'quality' && msg.quality_checks) {
      renderQualityChecks(msg.quality_checks, msg.quality_score);
    }
    if (msg.step === 'transformation' && msg.aggregations) {
      renderAggregations(msg.aggregations, msg.source_type);
    }
    appendLog(`[STEP] Done: ${msg.step} ${msg.records ? `(${fmt(msg.records)} records)` : ''}`, 'success');
  }

  if (event === 'progress') {
    const pct = Math.min(92, 5 + Math.round((completedSteps / 4) * 85) + (msg.pct * 0.05));
    updateProgress(pct, `${msg.label}: ${msg.done}/${msg.total}`);
  }

  if (event === 'completed') {
    updateProgress(100, `Done in ${fmtMs(msg.duration_ms)} · ${fmt(msg.records_loaded)} records`);
    appendLog(`[DONE] Pipeline completed! ${fmt(msg.records_loaded)} records loaded`, 'success');
    appendLog(`[DONE] Quality: ${msg.quality_score?.toFixed(1)}% | Duration: ${fmtMs(msg.duration_ms)}`, 'success');
    $('#modalTitle').textContent = '✓ Pipeline Completed';
    state.activeWs?.close();
    // Refresh underlying views
    if ($('#view-dashboard').classList.contains('active')) loadDashboard();
    if ($('#view-runs').classList.contains('active')) loadRunHistory();
  }

  if (event === 'failed') {
    appendLog(`[ERROR] ${msg.error}`, 'error');
    $('#modalTitle').textContent = '✗ Pipeline Failed';
    updateProgress(100, `Failed: ${msg.error}`);
    $('#progressBarFill').style.background = 'var(--red)';
    state.activeWs?.close();
  }

  if (event === 'pong') { /* heartbeat ack */ }
}

function setStepState(stepKey, stepState) {
  const el = $(`#step-${stepKey}`);
  if (!el) return;
  el.className = `step-item ${stepState}`;
  if (stepState === 'running') el.querySelector('.step-dot').textContent = '⟳';
  if (stepState === 'done') el.querySelector('.step-dot').textContent = '✓';
  if (stepState === 'failed') el.querySelector('.step-dot').textContent = '✗';
  // Mark connector line
  if (stepState === 'done') el.classList.add('done');
}

function updateProgress(pct, label) {
  $('#progressBarFill').style.width = `${pct}%`;
  $('#progressLabel').textContent = label;
}

function appendLog(line, type = '') {
  const el = $('#logOutput');
  const span = document.createElement('span');
  if (type === 'error') span.className = 'log-line-error';
  if (type === 'warn') span.className = 'log-line-warn';
  if (type === 'success') span.className = 'log-line-success';
  if (type === 'info') span.className = 'log-line-info';
  span.textContent = line + '\n';
  el.appendChild(span);
  el.scrollTop = el.scrollHeight;
}

function renderQualityChecks(checks, score) {
  const section = $('#qualitySection');
  section.style.display = 'block';
  $('#qualityGrid').innerHTML = checks.map(c => `
    <div class="quality-check qc-${escHtml(c.status)}">
      <div class="qc-name">${escHtml(c.name)} <span class="badge badge-${c.status === 'pass' ? 'success' : c.status === 'warn' ? 'pending' : 'failed'}">${escHtml(c.status)}</span></div>
      <div class="qc-bar-bg"><div class="qc-bar-fill" style="width:${c.pass_rate}%"></div></div>
      <div class="qc-stats">${c.pass_rate}% · ${fmt(c.records_failed)} failed</div>
      <div class="qc-stats text-dim" style="margin-top:2px;font-size:10px">${escHtml(c.details)}</div>
    </div>
  `).join('');
}

function renderAggregations(agg, sourceType) {
  const section = $('#aggSection');
  section.style.display = 'block';

  // Destroy previous modal charts
  if (state.charts.agg) { state.charts.agg.destroy(); state.charts.agg = null; }
  if (state.charts.agg2) { state.charts.agg2.destroy(); state.charts.agg2 = null; }
  $('#aggChart2Wrap').style.display = 'none';
  $('#aggChartsRow').style.display = 'none';

  // Show charts row — will be populated below
  const showChartsRow = () => { $('#aggChartsRow').style.display = 'grid'; };

  const doughnutOpts = {
    responsive: true, maintainAspectRatio: false, cutout: '60%',
    plugins: {
      legend: { display: true, position: 'bottom', labels: { color: '#7a7a9a', boxWidth: 10, padding: 10, font: { size: 11 } } },
      tooltip: { backgroundColor: '#13131f', borderColor: 'rgba(255,255,255,0.08)', borderWidth: 1, titleColor: '#e8e8f0', bodyColor: '#7a7a9a' },
    },
  };

  const barOpts = {
    responsive: true, maintainAspectRatio: false, indexAxis: 'y',
    plugins: {
      legend: { display: false },
      tooltip: { backgroundColor: '#13131f', borderColor: 'rgba(255,255,255,0.08)', borderWidth: 1, titleColor: '#e8e8f0', bodyColor: '#7a7a9a' },
    },
    scales: {
      x: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#7a7a9a', font: { size: 10 } } },
      y: { grid: { display: false }, ticks: { color: '#e8e8f0', font: { size: 11 } } },
    },
  };

  if (sourceType === 'crypto') {
    // KPI cards
    $('#aggKpis').innerHTML = [
      { label: 'Total Coins', value: fmt(agg.total_coins) },
      { label: 'Avg Price', value: agg.avg_price_usd != null ? `$${Number(agg.avg_price_usd).toLocaleString()}` : '—' },
      { label: 'Avg Volatility', value: agg.avg_volatility != null ? `${agg.avg_volatility}%` : '—', cls: 'text-amber' },
      { label: 'Anomalies', value: fmt(agg.anomalies_detected), cls: agg.anomalies_detected > 0 ? 'text-red' : '' },
    ].map(k => `<div class="agg-kpi"><div class="agg-kpi-label">${k.label}</div><div class="agg-kpi-value ${k.cls || ''}">${k.value}</div></div>`).join('');

    // Signals doughnut
    state.charts.agg = new Chart($('#aggChart'), {
      type: 'doughnut',
      data: {
        labels: ['Bullish', 'Bearish', 'Neutral'],
        datasets: [{
          data: [agg.bullish_signals || 0, agg.bearish_signals || 0, agg.neutral_signals || 0],
          backgroundColor: ['rgba(16,185,129,0.7)', 'rgba(239,68,68,0.7)', 'rgba(122,122,154,0.5)'],
          borderColor: ['#10b981', '#ef4444', '#7a7a9a'], borderWidth: 2
        }],
      },
      options: doughnutOpts,
    });
    showChartsRow();

  } else if (sourceType === 'weather') {
    // KPI cards
    $('#aggKpis').innerHTML = [
      { label: 'Cities', value: fmt(agg.cities_observed) },
      { label: 'Avg Temp', value: agg.avg_temp_c != null ? `${agg.avg_temp_c}°C` : '—' },
      { label: 'Min / Max', value: `${agg.min_temp_c ?? '—'}° / ${agg.max_temp_c ?? '—'}°` },
      { label: 'Extreme Events', value: fmt(agg.extreme_events), cls: agg.extreme_events > 0 ? 'text-red' : 'text-green' },
      { label: 'Anomalies', value: fmt(agg.anomalies_detected), cls: agg.anomalies_detected > 0 ? 'text-red' : '' },
    ].map(k => `<div class="agg-kpi"><div class="agg-kpi-label">${k.label}</div><div class="agg-kpi-value ${k.cls || ''}">${k.value}</div></div>`).join('');

    // Weather conditions bar chart (city → condition)
    const conditions = agg.conditions || {};
    const cities = Object.keys(conditions);
    // Map conditions to temperature values if available, otherwise use index
    state.charts.agg = new Chart($('#aggChart'), {
      type: 'bar',
      data: {
        labels: cities,
        datasets: [{
          label: 'Condition',
          data: cities.map(() => 1),
          backgroundColor: cities.map((_, i) => [
            'rgba(59,130,246,0.6)', 'rgba(99,102,241,0.6)', 'rgba(139,92,246,0.6)',
            'rgba(236,72,153,0.6)', 'rgba(16,185,129,0.6)', 'rgba(245,158,11,0.6)',
            'rgba(239,68,68,0.6)', 'rgba(59,130,246,0.4)', 'rgba(99,102,241,0.4)', 'rgba(139,92,246,0.4)',
          ][i % 10]),
          borderRadius: 4, borderWidth: 0,
        }],
      },
      options: {
        ...barOpts,
        plugins: {
          ...barOpts.plugins,
          tooltip: {
            ...barOpts.plugins.tooltip,
            callbacks: { label: ctx => ` ${conditions[ctx.label] || '—'}` },
          },
        },
      },
    });
    showChartsRow();

  } else if (sourceType === 'github') {
    // KPI cards
    $('#aggKpis').innerHTML = [
      { label: 'Total Events', value: fmt(agg.total_events) },
      { label: 'Unique Actors', value: fmt(agg.unique_actors) },
      { label: 'Unique Repos', value: fmt(agg.unique_repos) },
      { label: 'Trending Repos', value: fmt(agg.trending_repos), cls: 'text-amber' },
      { label: 'Anomalies', value: fmt(agg.anomalies_detected), cls: agg.anomalies_detected > 0 ? 'text-red' : '' },
    ].map(k => `<div class="agg-kpi"><div class="agg-kpi-label">${k.label}</div><div class="agg-kpi-value ${k.cls || ''}">${k.value}</div></div>`).join('');

    // Events by type horizontal bar
    const byType = agg.events_by_type || {};
    const eventLabels = Object.keys(byType);
    const eventValues = Object.values(byType);
    const colors = ['#6366f1', '#8b5cf6', '#ec4899', '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#7a7a9a'];
    state.charts.agg = new Chart($('#aggChart'), {
      type: 'bar',
      data: {
        labels: eventLabels,
        datasets: [{
          label: 'Events',
          data: eventValues,
          backgroundColor: eventLabels.map((_, i) => colors[i % colors.length] + 'aa'),
          borderColor: eventLabels.map((_, i) => colors[i % colors.length]),
          borderWidth: 1, borderRadius: 4,
        }],
      },
      options: barOpts,
    });
    showChartsRow();

  } else {
    // Fallback: render scalar values as KPI cards, dict values as charts
    const scalars = [];
    const dictEntries = [];
    for (const [k, v] of Object.entries(agg)) {
      if (v && typeof v === 'object' && !Array.isArray(v)) {
        dictEntries.push({ key: k, data: v });
      } else {
        scalars.push({ label: k.replace(/_/g, ' '), value: fmt(v) });
      }
    }
    $('#aggKpis').innerHTML = scalars.map(i =>
      `<div class="agg-kpi"><div class="agg-kpi-label">${i.label}</div><div class="agg-kpi-value">${i.value}</div></div>`
    ).join('');

    // Render first dict entry as a bar chart
    if (dictEntries.length > 0) {
      const first = dictEntries[0];
      const chartLabels = Object.keys(first.data).slice(0, 10);
      const chartValues = chartLabels.map(k => Number(first.data[k]) || 0);
      const colors = ['#6366f1', '#8b5cf6', '#ec4899', '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#7a7a9a', '#6366f1', '#8b5cf6'];
      state.charts.agg = new Chart($('#aggChart'), {
        type: 'bar',
        data: {
          labels: chartLabels,
          datasets: [{
            label: first.key.replace(/_/g, ' '),
            data: chartValues,
            backgroundColor: chartLabels.map((_, i) => colors[i % colors.length] + 'aa'),
            borderColor: chartLabels.map((_, i) => colors[i % colors.length]),
            borderWidth: 1, borderRadius: 4,
          }],
        },
        options: barOpts,
      });
      showChartsRow();
    }
    // Render second dict entry as a second chart
    if (dictEntries.length > 1) {
      const second = dictEntries[1];
      const chartLabels2 = Object.keys(second.data).slice(0, 10);
      const chartValues2 = chartLabels2.map(k => Number(second.data[k]) || 0);
      const colors2 = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#6366f1', '#7a7a9a', '#3b82f6', '#10b981'];
      $('#aggChart2Wrap').style.display = 'block';
      state.charts.agg2 = new Chart($('#aggChart2'), {
        type: 'bar',
        data: {
          labels: chartLabels2,
          datasets: [{
            label: second.key.replace(/_/g, ' '),
            data: chartValues2,
            backgroundColor: chartLabels2.map((_, i) => colors2[i % colors2.length] + 'aa'),
            borderColor: chartLabels2.map((_, i) => colors2[i % colors2.length]),
            borderWidth: 1, borderRadius: 4,
          }],
        },
        options: barOpts,
      });
    }
  }
}

$('#modalClose').addEventListener('click', () => {
  $('#runModal').style.display = 'none';
  state.activeWs?.close();
  state.activeWs = null;
});
$('#runModal').addEventListener('click', (e) => {
  if (e.target === e.currentTarget) $('#modalClose').click();
});

// ─── Init ─────────────────────────────────────────────────────────────────────
(async () => {
  await checkHealth();
  setInterval(checkHealth, 30000);
  switchView('dashboard');
})();
