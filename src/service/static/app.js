const app = document.querySelector('#app');
const dialog = document.querySelector('#detail-dialog');
const dialogContent = document.querySelector('#dialog-content');
const refreshTime = document.querySelector('#refresh-time');
const pageTitle = document.querySelector('#page-title');
const pageEyebrow = document.querySelector('#page-eyebrow');
const state = { view: 'overview', pages: { tasks: 1, data: 1, raw: 1, publications: 1 }, rawDate: '', rawSources: [],
  filters: { data: {dataset:'', security:'', start_date:'', end_date:'', provider:'', asset_type:'', adjustment:''}, tasks: { q: '', status: '', dataset: '' }, raw: { provider: '', endpoint: '', q: '' }, publications: { dataset: '' } } };
let renderSequence = 0;

const labels = {
  overview: ['运行总览', 'OPERATIONS OVERVIEW'],
  tasks: ['任务中心', 'TASK EXECUTION'],
  data: ['数据查询', '按类型、证券和日期查询来源记录'],
  raw: ['原始数据', 'RAW EVIDENCE'],
  publications: ['发布结果', 'CANONICAL PUBLICATIONS'],
};
const statusLabels = {
  published: '已发布', completed: '已完成', collecting: '采集中', checking: '检查中',
  committing: '发布中', running: '进行中', failed: '失败', stale: '疑似中断',
  blocked: '阻塞', partial: '部分完成', pending: '等待', unknown: '未知',
  candidate_complete: '候选完成', reused: '已复用', skipped: '已跳过',
};
const stageLabels = { raw: '原响应采集', processing: '解析与映射', publication: '检查与发布' };

function esc(value) {
  return String(value ?? '').replace(/[&<>'"]/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]));
}
function prettyNumber(value) { return value === null || value === undefined ? '—' : Number(value).toLocaleString('zh-CN'); }
function percent(done, total) { return total ? Math.min(100, Math.round(done / total * 1000) / 10) : 0; }
function formatTime(value) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN', { hour12: false });
}
function ageText(seconds) {
  if (seconds === null || seconds === undefined) return '更新时间未知';
  if (seconds < 60) return `${seconds} 秒前更新`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟前更新`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} 小时前更新`;
  return `${Math.floor(seconds / 86400)} 天前更新`;
}
function status(value) { return `<span class="status ${esc(value || 'unknown')}">${esc(statusLabels[value] || value || '未知')}</span>`; }
function showToast(message) {
  const toast = document.querySelector('#toast'); toast.textContent = message; toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2200);
}
async function api(path) {
  const response = await fetch(path, { headers: { Accept: 'application/json' } });
  if (!response.ok) {
    let message = `读取失败（${response.status}）`;
    try { message = (await response.json()).detail || message; } catch (_) {}
    throw new Error(message);
  }
  return response.json();
}
function loading() { app.innerHTML = '<div class="loading"><span></span>正在读取现有运行记录…</div>'; }
function fail(error) { app.innerHTML = `<div class="empty">${esc(error.message || error)}</div>`; }
function pager(kind, data) {
  return `<div class="pagination"><span>共 ${prettyNumber(data.total)} 条，第 ${data.page}/${Math.max(1, data.pages)} 页</span><div>
    <button data-page-kind="${kind}" data-page="${data.page - 1}" ${data.page <= 1 ? 'disabled' : ''}>上一页</button>
    <button data-page-kind="${kind}" data-page="${data.page + 1}" ${data.page >= data.pages ? 'disabled' : ''}>下一页</button>
  </div></div>`;
}
function taskRow(task) {
  const done = task.completed_units || 0, total = task.total_units || 0, value = percent(done, total);
  return `<tr class="clickable task-row" data-task-id="${esc(task.task_id)}">
    <td><strong>${esc(task.task_id)}</strong><br><span class="subtle">${esc(task.verification_mode || '—')}</span></td>
    <td>${esc(task.dataset || '—')}<br><span class="subtle">${esc(task.data_date || task.requested_date || '日期未记录')}</span></td>
    <td>${status(task.display_status)}</td>
    <td><div class="progress"><small>${prettyNumber(done)} / ${prettyNumber(total)}</small><progress max="100" value="${value}"></progress></div></td>
    <td>${prettyNumber(task.missing_count)}</td>
    <td>${formatTime(task.generated_at_utc)}<br><span class="subtle">${esc(ageText(task.age_seconds))}</span></td>
  </tr>`;
}
function publicationCard(item) {
  return `<div class="release-card"><strong>${esc(item.dataset)} · ${esc(item.asset_type)}</strong>
    <div class="release-meta"><span>${esc(item.partition_key)}</span><span>${prettyNumber(item.row_count)} 行</span></div></div>`;
}

async function renderOverview() {
  const sequence = ++renderSequence;
  const data = await api('/api/overview');
  if (sequence !== renderSequence || state.view !== 'overview') return;
  refreshTime.textContent = `刷新于 ${new Date(data.generated_at_utc).toLocaleTimeString('zh-CN', { hour12: false })}`;
  const counts = data.task_counts || {};
  const active = (counts.collecting || 0) + (counts.checking || 0) + (counts.committing || 0);
  app.innerHTML = `<section class="metric-grid">
    <article class="metric"><div class="metric-label">近期活跃任务</div><div class="metric-value">${prettyNumber(active)}</div><div class="metric-foot">最近 5 分钟仍有状态更新</div></article>
    <article class="metric"><div class="metric-label">疑似中断</div><div class="metric-value">${prettyNumber(counts.stale || 0)}</div><div class="metric-foot">采集中但超过 5 分钟未更新</div></article>
    <article class="metric"><div class="metric-label">已发布任务</div><div class="metric-value">${prettyNumber(counts.published || 0)}</div><div class="metric-foot">当前可见任务记录</div></article>
    <article class="metric"><div class="metric-label">最新原始数据日期</div><div class="metric-value">${esc(data.raw_latest_date || '—')}</div><div class="metric-foot">按来源和接口保留原始字节</div></article>
  </section>
  <section class="section-grid"><div class="panel"><div class="panel-head"><h2>最近任务</h2><small>点击查看执行详情</small></div>
    <div class="table-wrap"><table><thead><tr><th>任务</th><th>数据集</th><th>状态</th><th>采集进度</th><th>缺失</th><th>最后记录</th></tr></thead>
    <tbody>${data.recent_tasks.length ? data.recent_tasks.map(taskRow).join('') : '<tr><td colspan="6">暂无任务记录</td></tr>'}</tbody></table></div></div>
    <div class="panel"><div class="panel-head"><h2>最近发布</h2><small>Canonical</small></div><div class="panel-body release-list">
    ${data.recent_publications.length ? data.recent_publications.map(publicationCard).join('') : '<div class="subtle">暂无发布结果</div>'}</div></div></section>`;
  bindTaskRows();
}

async function renderTasks() {
  const sequence = ++renderSequence;
  const params = new URLSearchParams({ page: state.pages.tasks, page_size: 25 });
  for (const [key, value] of Object.entries(state.filters.tasks)) if (value) params.set(key, value);
  const data = await api(`/api/tasks?${params}`);
  if (sequence !== renderSequence || state.view !== 'tasks') return;
  app.innerHTML = `<form class="filters" id="task-filters">
    <input name="q" placeholder="搜索任务编号或数据集">
    <select name="status"><option value="">全部状态</option><option value="collecting">采集中</option><option value="stale">疑似中断</option><option value="failed">失败</option><option value="published">已发布</option></select>
    <input name="dataset" placeholder="数据集，例如 daily_bar">
    <span></span><button class="primary" type="submit">查询</button>
  </form><section class="panel"><div class="panel-head"><h2>任务记录</h2><small>来自现有任务摘要，不读取运行进程</small></div>
  <div class="table-wrap"><table><thead><tr><th>任务</th><th>数据集</th><th>状态</th><th>采集进度</th><th>缺失</th><th>最后记录</th></tr></thead>
  <tbody>${data.items.length ? data.items.map(taskRow).join('') : '<tr><td colspan="6">没有符合条件的任务</td></tr>'}</tbody></table></div>${pager('tasks', data)}</section>`;
  bindTaskRows();
  const form = document.querySelector('#task-filters');
  for (const [key, value] of Object.entries(state.filters.tasks)) form.elements.namedItem(key).value = value;
  form.addEventListener('submit', event => { event.preventDefault(); state.filters.tasks = Object.fromEntries(new FormData(form)); state.pages.tasks = 1; renderTasks().catch(fail); });
}

function stageList(progress) {
  const stages = progress?.stages || {};
  return Object.entries(stageLabels).map(([key, label]) => {
    const item = stages[key] || {}, value = item.percent || 0;
    return `<div><div class="stage"><strong>${label}</strong><progress max="100" value="${percent(value, 100)}"></progress><em>${prettyNumber(item.completed)} / ${prettyNumber(item.total)}</em></div>
    <p class="subtle stage-counts">${esc(statusLabels[item.status] || '等待')} · 成功 ${prettyNumber(item.success)} · 失败 ${prettyNumber(item.failed)} · 复用 ${prettyNumber(item.reused)} · 跳过 ${prettyNumber(item.skipped)} · 耗时 ${prettyNumber(item.elapsed_seconds)} 秒</p></div>`;
  }).join('');
}
async function showTask(taskId, unitPage = 1, unitQuery = '', unitStatus = '') {
  dialogContent.innerHTML = '<div class="loading"><span></span>正在读取任务详情…</div>'; if (!dialog.open) dialog.showModal();
  try {
    const [task, units] = await Promise.all([
      api(`/api/tasks/${encodeURIComponent(taskId)}`),
      api(`/api/tasks/${encodeURIComponent(taskId)}/units?${new URLSearchParams({page: unitPage, page_size: 25, q: unitQuery, status: unitStatus})}`),
    ]);
    const facts = [
      ['数据集', task.dataset], ['请求日期 / 数据日期', `${task.requested_date || '—'} / ${task.data_date || '未发布'}`], ['运行模式', task.verification_mode || '—'],
      ['状态', statusLabels[task.display_status] || task.display_status], ['发布行数', prettyNumber(task.row_count)], ['缺失单元', prettyNumber(task.missing_count)],
    ];
    dialogContent.innerHTML = `<h2 class="dialog-title">${esc(task.task_id)}</h2><p class="dialog-subtitle">${formatTime(task.generated_at_utc)} · ${esc(ageText(task.age_seconds))}</p>
      ${task.stale ? '<p class="notice">持久化状态仍在执行阶段，但已超过 5 分钟没有更新；这不代表进程仍然存活。</p>' : ''}
      <div class="stack"><section><h3>任务信息</h3><div class="facts-grid">${facts.map(([a,b]) => `<div class="fact"><span>${esc(a)}</span><strong>${esc(b)}</strong></div>`).join('')}</div></section>
      <section><h3>阶段进度</h3><div class="stage-list">${stageList(task.progress)}</div></section>
      ${task.error ? `<section><h3>任务错误</h3><div class="notice">${esc(task.error)}</div></section>` : ''}
      <section><h3>执行单元</h3><form id="unit-filters" class="filters"><input name="q" placeholder="证券或错误" value="${esc(unitQuery)}"><select name="status"><option value="">全部状态</option><option value="failed">失败</option><option value="pending">等待</option><option value="candidate_complete">候选完成</option></select><button class="primary">查询</button></form>
      <div class="table-wrap"><table><thead><tr><th>单元</th><th>状态</th><th>行数</th><th>错误</th><th>原始响应</th></tr></thead><tbody>
      ${units.items.length ? units.items.map(item => `<tr><td>${esc(item.symbol || item.key || item.input_id || '—')}</td><td>${status(item.status)}</td><td>${prettyNumber(item.row_count)}</td><td class="error-text" title="${esc(item.error || '')}">${esc(item.error || '—')}</td><td>${(item.evidence || []).map((record, index) => `<button class="secondary evidence-button" data-evidence-id="${esc(record.id)}">响应 ${index + 1}</button>`).join('') || '尚未留存'}</td></tr>`).join('') : '<tr><td colspan="5">暂无匹配执行单元</td></tr>'}</tbody></table></div>${pager('units', units)}</section>
      <section><h3>覆盖检查</h3><pre class="code-block">${esc(JSON.stringify(task.final_check, null, 2))}</pre></section>
      <section><h3>冻结参数</h3><pre class="code-block">${esc(JSON.stringify(task.definition, null, 2))}</pre></section></div>`;
    const unitForm = dialogContent.querySelector('#unit-filters'); unitForm.elements.namedItem('status').value = unitStatus;
    unitForm.addEventListener('submit', event => { event.preventDefault(); showTask(taskId, 1, unitForm.elements.namedItem('q').value, unitForm.elements.namedItem('status').value); });
    dialogContent.querySelectorAll('[data-page-kind="units"]').forEach(button => button.addEventListener('click', () => showTask(taskId, Number(button.dataset.page), unitQuery, unitStatus)));
    dialogContent.querySelectorAll('.evidence-button').forEach(button => button.addEventListener('click', () => showRaw(button.dataset.evidenceId)));
  } catch (error) { dialogContent.innerHTML = `<div class="empty">${esc(error.message)}</div>`; }
}
function bindTaskRows() { document.querySelectorAll('.task-row').forEach(row => row.addEventListener('click', () => showTask(row.dataset.taskId))); }

async function loadRawSources(date) {
  if (!date) return [];
  const data = await api(`/api/raw/sources?data_date=${encodeURIComponent(date)}`); return data.sources;
}
async function renderRaw() {
  const sequence = ++renderSequence;
  const options = await api('/api/raw/options');
  if (!state.rawDate) state.rawDate = options.latest_date || '';
  state.rawSources = await loadRawSources(state.rawDate);
  const { provider, endpoint, q } = state.filters.raw;
  const params = new URLSearchParams({ page: state.pages.raw, page_size: 25 });
  if (state.rawDate) params.set('data_date', state.rawDate); if (provider) params.set('provider', provider); if (endpoint) params.set('endpoint', endpoint); if (q) params.set('q', q);
  const data = await api(`/api/raw?${params}`);
  if (sequence !== renderSequence || state.view !== 'raw') return;
  const providerOptions = state.rawSources.map(item => `<option value="${esc(item.provider)}" ${item.provider === provider ? 'selected' : ''}>${esc(item.provider)}</option>`).join('');
  const endpoints = state.rawSources.find(item => item.provider === provider)?.endpoints || [];
  app.innerHTML = `<form class="filters" id="raw-filters">
    <select name="data_date"><option value="">无可用日期</option>${options.dates.map(item => `<option value="${esc(item)}" ${item === state.rawDate ? 'selected' : ''}>${esc(item)}</option>`).join('')}</select>
    <select name="provider"><option value="">全部来源</option>${providerOptions}</select>
    <select name="endpoint"><option value="">全部接口</option>${endpoints.map(item => `<option value="${esc(item)}" ${item === endpoint ? 'selected' : ''}>${esc(item)}</option>`).join('')}</select>
    <input name="q" value="${esc(q)}" placeholder="证券、URL 或哈希">
    <button class="primary" type="submit">查询</button>
  </form><section class="panel"><div class="panel-head"><h2>原始响应证据</h2><small>日期 ${esc(data.data_date || '—')} · 精确字节只读保留</small></div>
  <div class="table-wrap"><table><thead><tr><th>来源 / 接口</th><th>请求范围</th><th>响应</th><th>大小</th><th>采集时间</th><th>哈希</th></tr></thead><tbody>
  ${data.items.length ? data.items.map(item => `<tr class="clickable raw-row" data-evidence-id="${esc(item.id)}"><td><strong>${esc(item.provider)}</strong><br><span class="subtle">${esc(item.endpoint)}</span></td><td class="mono">${esc(JSON.stringify(item.scope || {})).slice(0,150)}</td><td>${item.status_code ?? '—'} · ${esc(item.outcome || '—')}</td><td>${prettyNumber(item.body_bytes)} B</td><td>${formatTime(item.fetched_at_utc)}</td><td class="mono">${esc((item.body_sha256 || '—').slice(0,12))}</td></tr>`).join('') : '<tr><td colspan="6">没有符合条件的原始响应</td></tr>'}</tbody></table></div>${pager('raw', data)}</section>`;
  const form = document.querySelector('#raw-filters');
  form.elements.namedItem('data_date').addEventListener('change', () => { state.rawDate = form.elements.namedItem('data_date').value; state.filters.raw = {provider:'',endpoint:'',q:''}; state.pages.raw = 1; renderRaw().catch(fail); });
  form.elements.namedItem('provider').addEventListener('change', () => { state.filters.raw.provider = form.elements.namedItem('provider').value; state.filters.raw.endpoint = ''; state.pages.raw = 1; renderRaw().catch(fail); });
  form.addEventListener('submit', event => { event.preventDefault(); state.filters.raw = {provider:form.elements.namedItem('provider').value,endpoint:form.elements.namedItem('endpoint').value,q:form.elements.namedItem('q').value}; state.pages.raw = 1; renderRaw().catch(fail); });
  document.querySelectorAll('.raw-row').forEach(row => row.addEventListener('click', () => showRaw(row.dataset.evidenceId)));
}
async function showRaw(id) {
  dialogContent.innerHTML = '<div class="loading"><span></span>正在读取原始证据…</div>'; if (!dialog.open) dialog.showModal();
  try {
    const item = await api(`/api/raw/${encodeURIComponent(id)}`);
    const facts = [['来源', item.provider], ['接口', item.endpoint], ['响应状态', item.status_code ?? item.sdk_status_code ?? '—'], ['内容类型', item.content_type || item.event_kind || '—'], ['字节数', prettyNumber(item.body_bytes)], ['SHA-256', item.body_sha256 || '—']];
    dialogContent.innerHTML = `<h2 class="dialog-title">原始响应证据</h2><p class="dialog-subtitle">${esc(item.manifest_name)} · ${formatTime(item.fetched_at_utc)}</p>
      <div class="stack"><div class="facts-grid">${facts.map(([a,b]) => `<div class="fact"><span>${a}</span><strong class="${a === 'SHA-256' ? 'mono' : ''}">${esc(b)}</strong></div>`).join('')}</div>
      ${item.error ? `<div class="notice">${esc(item.error)}</div>` : ''}
      ${item.representation ? `<div class="notice">${esc(item.representation)}</div>` : ''}
      <section><h3>请求信息（已脱敏）</h3><pre class="code-block">${esc(JSON.stringify(item.request, null, 2))}</pre></section>
      <section><h3>响应信息</h3><pre class="code-block">${esc(JSON.stringify(item.response, null, 2))}</pre></section>
      <section><h3>内容预览${item.preview.truncated ? '（已截断）' : ''}</h3>${item.preview.text !== null ? `<pre class="code-block">${esc(item.preview.text)}</pre>` : `<div class="notice">${item.preview.kind === 'binary' ? '二进制内容不进行文本渲染，请下载精确原始字节核验。' : '当前记录没有可预览的响应内容。'}</div>`}
      ${item.has_body ? `<div class="dialog-actions"><a class="primary button-link" href="/api/raw/${encodeURIComponent(id)}/download">下载原始字节</a></div>` : ''}</section></div>`;
  } catch (error) { dialogContent.innerHTML = `<div class="empty">${esc(error.message)}</div>`; }
}

async function renderPublications() {
  const sequence = ++renderSequence;
  const params = new URLSearchParams({ page: state.pages.publications, page_size: 25 });
  if (state.filters.publications.dataset) params.set('dataset', state.filters.publications.dataset);
  const data = await api(`/api/publications?${params}`);
  if (sequence !== renderSequence || state.view !== 'publications') return;
  app.innerHTML = `<form class="filters" id="publication-filters"><input name="dataset" placeholder="数据集，例如 daily_bar"><span></span><span></span><span></span><button class="primary" type="submit">查询</button></form>
  <section class="panel"><div class="panel-head"><h2>Canonical 发布分区</h2><small>来自现有 manifest，不读取 Parquet 内容</small></div><div class="table-wrap"><table><thead><tr><th>数据日期</th><th>数据集</th><th>资产类型</th><th>行数 / 预期</th><th>覆盖率</th><th>冲突 / 隔离</th><th>内容哈希</th></tr></thead><tbody>
  ${data.items.length ? data.items.map((item, index) => `<tr class="clickable publication-row" data-index="${index}"><td>${esc(item.partition_key)}</td><td><strong>${esc(item.dataset)}</strong></td><td>${esc(item.asset_type)}</td><td>${prettyNumber(item.row_count)} / ${prettyNumber(item.expected_count)}</td><td>${item.coverage_percent === null ? '—' : `${item.coverage_percent}%`}</td><td>${prettyNumber(item.conflict_count)} / ${prettyNumber(item.quarantined_count)}</td><td class="mono">${esc((item.content_hash || '—').slice(0,16))}</td></tr>`).join('') : '<tr><td colspan="7">暂无发布结果</td></tr>'}</tbody></table></div>${pager('publications', data)}</section>`;
  document.querySelectorAll('.publication-row').forEach(row => row.addEventListener('click', () => {
    const item = data.items[Number(row.dataset.index)];
    dialogContent.innerHTML = `<h2 class="dialog-title">${esc(item.dataset)} · ${esc(item.partition_key)}</h2><p class="dialog-subtitle">${esc(item.asset_type)} · ${prettyNumber(item.row_count)} 行</p>
    <div class="notice">覆盖率按清单中的行数与预期数计算；发布记录不等同于独立全市场覆盖验证。</div>
    <h3>内容哈希</h3><pre class="code-block">${esc(item.content_hash)}</pre>
    <h3>关联任务</h3>${item.tasks.map(id => `<button class="secondary linked-task" data-task-id="${esc(id)}">${esc(id)}</button>`).join('') || '<p class="subtle">没有匹配的现存任务摘要</p>'}
    <h3>来源响应</h3>${item.evidence.map((record, index) => `<button class="secondary linked-evidence" data-evidence-id="${esc(record.id)}">${esc(record.provider)} · 响应 ${index + 1}</button>`).join('') || '<p class="subtle">没有可读取的关联响应</p>'}
    <h3>发布检查</h3><pre class="code-block">${esc(JSON.stringify(item.publication_metadata, null, 2))}</pre>`;
    if (!dialog.open) dialog.showModal();
    dialogContent.querySelectorAll('.linked-task').forEach(button => button.addEventListener('click', () => showTask(button.dataset.taskId)));
    dialogContent.querySelectorAll('.linked-evidence').forEach(button => button.addEventListener('click', () => showRaw(button.dataset.evidenceId)));
  }));
  const form = document.querySelector('#publication-filters');
  form.elements.namedItem('dataset').value = state.filters.publications.dataset;
  form.addEventListener('submit', event => { event.preventDefault(); state.filters.publications = Object.fromEntries(new FormData(form)); state.pages.publications = 1; renderPublications().catch(fail); });
}

async function renderData() {
  const sequence = ++renderSequence;
  const pendingButton = document.querySelector('#data-filters button');
  if (pendingButton) { pendingButton.disabled = true; pendingButton.textContent = '查询中…'; }
  const options = await api('/api/data/options');
  if (sequence !== renderSequence || state.view !== 'data') return;
  const filters = state.filters.data;
  if (!filters.dataset && options.datasets.length) {
    filters.dataset = options.datasets.find(item => item.id === 'daily_bar')?.id || options.datasets[0].id;
    const dates = options.datasets.find(item => item.id === filters.dataset)?.dates || [];
    filters.start_date = dates.at(-1) || ''; filters.end_date = dates.at(-1) || '';
  }
  if (!filters.dataset) {
    app.innerHTML = '<div class="empty">尚无可查询的来源行文件。采集任务保存来源行后，可在这里按证券和日期查看。</div>'; return;
  }
  const params = new URLSearchParams({page:state.pages.data,page_size:25});
  for (const [key, value] of Object.entries(filters)) if (value) params.set(key, value);
  const choice = options.datasets.find(item => item.id === filters.dataset);
  let data = {items:[],columns:[],issues:[],total:0,page:1,pages:0};
  try { data = await api(`/api/data?${params}`); }
  catch (error) { data.issues = [{provider:'查询条件',reason:error.message}]; }
  if (sequence !== renderSequence || state.view !== 'data') return;
  const valueText = value => value === null || value === undefined ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value);
  app.innerHTML = `<form class="filters data-filters" id="data-filters">
    <label>数据类型<select name="dataset">${options.datasets.map(item => `<option value="${esc(item.id)}">${esc(item.label)}</option>`).join('')}</select></label>
    <label>证券（可选）<input name="security" placeholder="留空查询全部；也可输入代码或名称"></label>
    <label>开始日期<input type="date" name="start_date"></label><label>结束日期<input type="date" name="end_date"></label>
    <label>来源<select name="provider"><option value="">全部来源</option>${(choice?.providers || []).map(item => `<option>${esc(item)}</option>`).join('')}</select></label>
    <label>资产类型<select name="asset_type"><option value="">全部类型</option><option value="stock">股票</option><option value="etf">ETF</option><option value="lof">LOF</option><option value="index">指数</option></select></label>
    <label>复权口径<select name="adjustment"><option value="">全部口径</option><option value="none">不复权</option><option value="qfq">前复权</option><option value="hfq">后复权</option></select></label><button class="primary" type="submit">查询数据</button>
  </form><p class="subtle query-description">展示来源字段与原值，成交量和成交额保留来源单位。点击记录查看日期依据、完整字段和原始响应。</p>
  ${data.excluded_unknown_date_count ? `<p class="notice">${prettyNumber(data.excluded_unknown_date_count)} 条记录缺少明确业务日期，未纳入本次日期筛选。</p>` : ''}
  ${data.issues.length ? `<p class="notice">${prettyNumber(data.issues.length)} 份来源文件未通过读取或完整性检查，已排除。${esc(data.issues.map(item => `${item.provider}: ${item.reason}`).join('；'))}</p>` : ''}
  <section class="panel"><div class="panel-head"><h2>来源数据预览</h2><small>${prettyNumber(data.total)} 条匹配记录 · 点击查看详情</small></div>
  <div class="table-wrap"><table class="data-table"><thead><tr><th>查询日期</th><th>证券</th><th>来源</th>${data.columns.map(col => `<th>${esc(col.label)}</th>`).join('')}</tr></thead><tbody>
  ${data.items.length ? data.items.map((row,index) => `<tr class="clickable data-row" data-index="${index}"><td>${esc(row.record_date || '未提供')}</td><td><strong>${esc(row.symbol)}</strong><br>${esc(row.security_name)}</td><td>${esc(row.provider)}</td>${data.columns.map(col => `<td title="${esc(valueText(row.values[col.key]))}">${esc(valueText(row.values[col.key]).slice(0,180))}</td>`).join('')}</tr>`).join('') : `<tr><td colspan="${data.columns.length+3}">没有匹配的本地留存记录。可调整证券、日期或来源；查询不会触发联网采集。</td></tr>`}
  </tbody></table></div>${pager('data',data)}</section>`;
  const form = document.querySelector('#data-filters');
  for (const [key,value] of Object.entries(filters)) form.elements.namedItem(key).value = value;
  form.addEventListener('submit',event => {event.preventDefault();state.filters.data = Object.fromEntries(new FormData(form));state.pages.data=1;renderData().catch(fail);});
  form.elements.namedItem('dataset').addEventListener('change',() => {
    state.filters.data = {...Object.fromEntries(new FormData(form)),provider:'',adjustment:''};state.pages.data=1;renderData().catch(fail);
  });
  document.querySelectorAll('.data-row').forEach(element => element.addEventListener('click',() => {
    const row = data.items[Number(element.dataset.index)];
    dialogContent.innerHTML = `<h2 class="dialog-title">${esc(row.symbol)} ${esc(row.security_name)}</h2><p class="dialog-subtitle">${esc(row.provider)} · ${esc(row.endpoint)}</p>
    <div class="facts-grid"><div class="fact"><span>查询日期及依据</span><strong>${esc(row.record_date || '未提供')} · ${esc(row.date_basis)}</strong></div><div class="fact"><span>来源文件内行号（从1开始）</span><strong>${row.source_row_index+1}</strong></div><div class="fact"><span>资产类型 / 请求复权</span><strong>${esc(row.asset_type || '未提供')} / ${esc(row.adjustment || '未提供')}</strong></div></div>
    <h3>来源字段与原值</h3><pre class="code-block">${esc(JSON.stringify(row.values,null,2))}</pre>
    <h3>来源行文件 SHA-256</h3><pre class="code-block">${esc(row.source_rows_sha256)}</pre>
    <h3>关联原始响应</h3>${row.evidence.map((record,index) => `<button class="secondary query-evidence" data-id="${esc(record.id)}">${esc(record.provider)} · 响应 ${index+1} · ${esc(formatTime(record.fetched_at_utc))}</button>`).join('') || '<p class="subtle">当前没有可读取的关联原始响应。</p>'}
    <p class="subtle query-description">${esc(data.representation)}。辅助证券名称和资产类型可来自已发布证券名单，不改变上方来源字段。</p>`;
    if(!dialog.open) dialog.showModal();
    dialogContent.querySelectorAll('.query-evidence').forEach(button => button.addEventListener('click',() => showRaw(button.dataset.id)));
  }));
}

async function render() {
  loading(); const [title, eyebrow] = labels[state.view]; pageTitle.textContent = title; pageEyebrow.textContent = eyebrow;
  document.querySelectorAll('.nav-item').forEach(item => item.classList.toggle('active', item.dataset.view === state.view));
  try {
    if (state.view === 'overview') await renderOverview();
    if (state.view === 'tasks') await renderTasks();
    if (state.view === 'data') await renderData();
    if (state.view === 'raw') await renderRaw();
    if (state.view === 'publications') await renderPublications();
    refreshTime.textContent = `刷新于 ${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`;
  } catch (error) { fail(error); }
}

document.querySelectorAll('.nav-item').forEach(item => item.addEventListener('click', () => { state.view = item.dataset.view; render(); }));
document.querySelector('#refresh-button').addEventListener('click', () => { render(); showToast('已重新读取磁盘记录'); });
document.querySelector('.dialog-close').addEventListener('click', () => dialog.close());
dialog.addEventListener('click', event => { if (event.target === dialog) dialog.close(); });
document.addEventListener('click', event => {
  const button = event.target.closest('[data-page-kind]'); if (!button || button.disabled || button.dataset.pageKind === 'units') return;
  state.pages[button.dataset.pageKind] = Number(button.dataset.page); render();
});
render();
setInterval(() => { if (!document.hidden && !dialog.open && state.view === 'overview') renderOverview().catch(() => {}); }, 15000);
