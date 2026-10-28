import { mountToastContainer } from './toast.js';
import { createVirtualList } from './vlist.js';
import { renderJson } from './highlight.js';
import { mountBunnies, setBunnyMode, bunnyDash } from './bunny.js';

mountBunnies();

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;
const $ = (id) => document.getElementById(id);

// --- log: todo lo que falla en la UI va al mismo .log que el backend ----------------
const log = (level, message) => invoke('log_ui', { level, message: String(message) }).catch(() => {});
window.addEventListener('error', (ev) =>
  log('error', `${ev.message} @ ${ev.filename}:${ev.lineno}:${ev.colno}\n${ev.error?.stack ?? ''}`)
);
window.addEventListener('unhandledrejection', (ev) => log('error', `promesa rechazada: ${ev.reason?.stack ?? ev.reason}`));
const store = {
  get(k, fallback) {
    try {
      const v = localStorage.getItem(k);
      return v == null ? fallback : JSON.parse(v);
    } catch {
      return fallback;
    }
  },
  set(k, v) {
    try {
      localStorage.setItem(k, JSON.stringify(v));
    } catch {}
  },
};

const { showToast } = mountToastContainer($('toasts'));
const toast = {
  ok: (m) => showToast(m, 'success'),
  err: (m) => {
    log('warn', `toast de error: ${m}`);
    return showToast(String(m), 'error');
  },
  info: (m) => showToast(m, 'info'),
};

const ROW_H = 54;
const MAX_N = 20000;

const S = {
  queues: [],
  current: null,
  tab: 'queue',
  // Página de colas: la pide y la ordena el broker, nunca se trae el vhost entero.
  qq: { page: 1, size: store.get('qsize', 10), sort: store.get('qsort', 'messages'), name: '' },
  qpage: null, // { page, page_count, filtered_count, total_count } de la última carga
  currentInfo: null, // último dato conocido de la cola seleccionada
  qItems: [],
  localItems: [],
  view: [], // items filtrados que muestra la lista
  sel: -1, // índice en view del mensaje abierto en el detalle
  checked: new Set(), // claves seleccionadas en el respaldo local
  lastCheck: -1,
  detailView: 'payload',
  op: null, // operación en curso { label, total, start, cancellable }
  peekToken: 0,
  connected: false,
};

// --- llamadas al backend con barra de carga global ------------------------------
let inflight = 0;
async function call(cmd, args, { quiet = false } = {}) {
  if (!quiet && ++inflight === 1) {
    $('loadbar').classList.add('active');
    setBunnyMode($('brandLogo'), 'hop');
  }
  const t = performance.now();
  try {
    const r = await invoke(cmd, args);
    const ms = Math.round(performance.now() - t);
    if (ms > 3000) log('warn', `${cmd} lento: ${ms} ms`);
    return r;
  } catch (err) {
    // El backend ya dejó el detalle; acá queda qué pidió la UI y cuánto tardó.
    log('error', `${cmd} falló tras ${Math.round(performance.now() - t)} ms: ${err}`);
    throw err;
  } finally {
    if (!quiet && --inflight === 0) {
      $('loadbar').classList.remove('active');
      setBunnyMode($('brandLogo'), 'sit');
    }
  }
}

async function busy(btn, fn) {
  btn.classList.add('busy');
  btn.disabled = true;
  try {
    return await fn();
  } finally {
    btn.classList.remove('busy');
    btn.disabled = false;
  }
}

// --- perfiles de conexión --------------------------------------------------------
const DEFAULTS = { api: 'http://localhost:15672', amqp: 'amqp://localhost:5672', user: 'guest', pass: 'guest', vhost: '/' };
let profiles = store.get('profiles', null);
if (!profiles) {
  const legacy = store.get('conn', null);
  profiles = [{ name: 'local', ...DEFAULTS, ...(legacy || {}) }];
  store.set('profiles', profiles);
}
let profileName = store.get('profile', profiles[0]?.name);

const profile = () => profiles.find((p) => p.name === profileName) || profiles[0];
function conn() {
  const p = profile() || {};
  return {
    api: p.api || DEFAULTS.api,
    amqp: p.amqp || DEFAULTS.amqp,
    user: p.user || '',
    pass: p.pass || '',
    vhost: p.vhost || '/',
  };
}

function renderProfiles() {
  $('profile').innerHTML = '';
  for (const p of profiles) $('profile').add(new Option(p.name, p.name, false, p.name === profileName));
}

function openConnDialog() {
  const form = $('connDialog').querySelector('form');
  const p = profile() || { name: '', ...DEFAULTS };
  for (const f of ['name', 'api', 'amqp', 'user', 'pass', 'vhost']) form.elements[f].value = p[f] ?? '';
  $('connDelete').hidden = profiles.length < 2;
  $('connDialog').showModal();
}

$('connDialog').addEventListener('close', () => {
  if ($('connDialog').returnValue !== 'save') return;
  const f = $('connDialog').querySelector('form').elements;
  const p = Object.fromEntries(['name', 'api', 'amqp', 'user', 'pass', 'vhost'].map((k) => [k, f[k].value.trim()]));
  if (!p.name) return;
  const i = profiles.findIndex((x) => x.name === p.name);
  if (i >= 0) profiles[i] = p;
  else profiles.push(p);
  selectProfile(p.name);
});

$('connDelete').onclick = () => {
  profiles = profiles.filter((p) => p.name !== profileName);
  $('connDialog').close();
  selectProfile(profiles[0].name);
};

/** Cambiar o editar el perfil siempre desconecta: vuelve a habilitar "Conectar". */
function selectProfile(name) {
  profileName = name;
  store.set('profiles', profiles);
  store.set('profile', name);
  renderProfiles();
  log('info', `perfil "${name}" seleccionado/editado: desconectando`);
  call('disconnect', undefined, { quiet: true }).catch(() => {});
  S.queues = [];
  S.qpage = null;
  S.qq.page = 1;
  S.current = null;
  S.currentInfo = null;
  S.qItems = [];
  setConnected(false);
  setCurrent(null);
  renderQueues();
  applyFilter();
  setConn('', `Perfil ${name} — sin conectar`);
}

// --- conexión / colas -------------------------------------------------------------
function setConn(state, text) {
  $('connDot').className = `statusbar__dot ${state}`;
  $('connText').textContent = text;
}

const hostOf = (c) => c.api.replace(/^https?:\/\//, '');

/** Conectado: "Conectar" queda bloqueado y en verde hasta editar/cambiar el perfil. */
function setConnected(on) {
  S.connected = on;
  const b = $('connect');
  b.disabled = on;
  b.classList.toggle('btn--connected', on);
  b.classList.toggle('btn--primary', !on);
  b.textContent = on ? 'Conectado' : 'Conectar';
  b.title = on ? 'Para reconectar, editá la conexión o cambiá de perfil' : 'Validar credenciales y vhost';
}

/** Solo valida credenciales y vhost: no lista colas. */
async function connect() {
  if (S.connected) return;
  const c = conn();
  setConn('busy', `Conectando a ${hostOf(c)}…`);
  try {
    const r = await busy($('connect'), () => call('ping', { conn: c }));
    setConnected(true);
    setConn('ok', `${r.user}@${hostOf(c)} · vhost ${c.vhost}${r.messages != null ? ` · ${fmt(r.messages)} mensajes` : ''}`);
    toast.ok(`Conectado a ${profile().name}`);
    if (!S.qpage) $('qemptyText').innerHTML = 'Conectado.<br />Filtrá si querés y tocá <b>Cargar</b>.';
  } catch (err) {
    setConnected(false);
    setConn('bad', 'Error de conexión');
    toast.err(`No se pudo conectar: ${err}`);
  }
}

function queueQuery() {
  const raw = $('qfilter').value.trim();
  const m = raw.match(/^\/(.+)\/$/);
  return { page: S.qq.page, page_size: S.qq.size, sort: S.qq.sort, name: m ? m[1] : raw, regex: !!m };
}

let loadingQueues = false;
/** Trae una página de colas. `page` cambia de página; `silent` = refresco de fondo. */
async function loadQueues({ page = S.qq.page, silent = false } = {}) {
  if (loadingQueues) return false;
  loadingQueues = true;
  S.qq.page = page;
  if (!silent) {
    $('refresh').classList.add('spinning');
    renderQueueSkeleton();
  }
  try {
    let r;
    try {
      r = await call('list_queues', { conn: conn(), q: queueQuery() }, { quiet: silent });
    } catch (err) {
      // La página quedó fuera de rango (se borraron colas o cambió el filtro).
      if (S.qq.page === 1) throw err;
      S.qq.page = 1;
      r = await call('list_queues', { conn: conn(), q: queueQuery() }, { quiet: silent });
    }
    S.queues = r.items;
    S.qpage = r;
    if (!S.connected) {
      setConnected(true);
      const c = conn();
      setConn('ok', `${c.user}@${hostOf(c)} · vhost ${c.vhost}`);
    }
    renderQueues({ animate: !silent });
    if (S.tab === 'queue' && !S.qItems.length) applyFilter();
    return true;
  } catch (err) {
    if (!silent) {
      setConnected(false);
      setConn('bad', 'Error de conexión');
      toast.err(`No se pudo listar colas: ${err}`);
      renderQueues();
    }
    return false;
  } finally {
    loadingQueues = false;
    $('refresh').classList.remove('spinning');
  }
}

function renderQueueSkeleton() {
  $('qempty').hidden = true;
  $('queues').classList.remove('anim');
  $('queues').innerHTML = '<li class="skel"><span class="qname"></span></li>'.repeat(S.qq.size);
}

const fmt = (n) => Number(n || 0).toLocaleString('es');

function renderQueues({ animate = false } = {}) {
  const frag = document.createDocumentFragment();
  S.queues.forEach((q, i) => {
    const li = document.createElement('li');
    li.dataset.name = q.name;
    li.style.setProperty('--i', i);
    if (q.name === S.current) li.className = 'sel';
    li.title = `${q.name}\nready ${fmt(q.ready)} · unacked ${fmt(q.unacked)} · consumers ${q.consumers}${q.type ? ' · ' + q.type : ''}`;
    const dot = document.createElement('span');
    dot.className = `qdot${q.state && q.state !== 'running' ? ' bad' : q.consumers ? ' on' : ''}`;
    const name = document.createElement('span');
    name.className = 'qname';
    name.textContent = q.name;
    const cnt = document.createElement('span');
    cnt.className = `qcount${q.ready ? ' has' : ''}`;
    cnt.textContent = fmt(q.ready);
    li.append(dot, name, cnt);
    frag.appendChild(li);
  });
  $('queues').classList.toggle('anim', animate);
  $('queues').replaceChildren(frag);

  const p = S.qpage;
  $('qempty').hidden = S.queues.length > 0;
  if (p && !S.queues.length) $('qemptyText').innerHTML = $('qfilter').value.trim() ? 'Ninguna cola coincide con el filtro.' : 'Este vhost no tiene colas.';
  if (!p) $('qemptyText').innerHTML = 'Las colas no se cargan solas.<br />Filtrá si querés y tocá <b>Cargar</b>.';
  $('qsummary').textContent = !p
    ? 'Colas sin cargar'
    : p.filtered_count !== p.total_count
      ? `${fmt(p.filtered_count)} coinciden de ${fmt(p.total_count)} colas`
      : `${fmt(p.total_count)} colas en el vhost`;
  $('pager').hidden = !p || p.page_count <= 1;
  if (p) {
    $('pageInfo').textContent = `${p.page} / ${p.page_count}`;
    $('prevPage').disabled = p.page <= 1;
    $('nextPage').disabled = p.page >= p.page_count;
  }
  const info = S.queues.find((q) => q.name === S.current);
  if (info) S.currentInfo = info;
  updateCurrentPills();
}

function updateCurrentPills() {
  const q = S.currentInfo;
  $('curReady').textContent = q ? `${fmt(q.ready)} ready` : '';
  $('curUnacked').textContent = q && q.unacked ? `${fmt(q.unacked)} unacked` : '';
}

function setCurrent(name) {
  S.current = name;
  S.currentInfo = S.queues.find((q) => q.name === name) || null;
  $('current').textContent = name || 'Elegí una cola';
  for (const li of $('queues').children) li.classList.toggle('sel', li.dataset.name === name);
  updateCurrentPills();
}

// Elegir una cola no lee mensajes: eso es siempre explícito con "Ver (nack)".
$('queues').addEventListener('click', (ev) => {
  const li = ev.target.closest('li[data-name]');
  if (!li) return;
  if (li.dataset.name !== S.current) {
    setCurrent(li.dataset.name);
    S.qItems = [];
    S.peekToken++;
  }
  if (S.tab !== 'queue') setTab('queue');
  else applyFilter();
});

// Sugerencias de cola destino: búsqueda acotada en el broker mientras se escribe.
let targetTimer = 0;
$('target').addEventListener('input', () => {
  clearTimeout(targetTimer);
  const name = $('target').value.trim();
  if (!name || !S.connected) return;
  targetTimer = setTimeout(async () => {
    try {
      const r = await call('list_queues', { conn: conn(), q: { page: 1, page_size: 20, sort: 'name', name, regex: false } }, { quiet: true });
      const dl = document.createDocumentFragment();
      for (const q of r.items) dl.appendChild(new Option(q.name));
      $('queueNames').replaceChildren(dl);
    } catch {}
  }, 250);
});

// --- normalización + filtro local -------------------------------------------------
function normQueueMsg(m, i) {
  return { ...m, key: i, idx: i + 1 };
}
function normLocal(b, i) {
  return { ...b, key: `${b.batch}:${b.n}`, idx: i + 1 };
}

function hay(it) {
  if (it._hay === undefined) {
    it._hay = `${it.payload}\n${it.routing_key}\n${JSON.stringify(it.properties)}\n${it.status || ''} ${it.source || ''} ${it.target || ''} ${it.error || ''}`;
    it._low = it._hay.toLowerCase();
  }
  return it;
}

function buildMatcher(raw) {
  let f = raw.trim();
  if (!f) return null;
  let neg = false;
  if (f.startsWith('!') && f.length > 1) {
    neg = true;
    f = f.slice(1);
  }
  let test;
  const m = f.match(/^\/(.+)\/([gimsuy]*)$/);
  if (m) {
    let re;
    try {
      re = new RegExp(m[1], m[2].replace('g', '') || 'i');
    } catch (err) {
      return { error: err.message };
    }
    test = (it) => re.test(hay(it)._hay);
  } else {
    const low = f.toLowerCase();
    test = (it) => hay(it)._low.includes(low);
  }
  return { test: neg ? (it) => !test(it) : test };
}

function applyFilter({ keepScroll = false } = {}) {
  const src = S.tab === 'queue' ? S.qItems : S.localItems;
  const matcher = buildMatcher($('filter').value);
  $('filter').style.borderColor = matcher?.error ? 'var(--color-error)' : '';
  $('clearFilter').hidden = !$('filter').value;
  const status = S.tab === 'local' ? $('statusFilter').value : '';

  const prevKey = S.view[S.sel]?.key;
  S.view = src.filter((it) => (!status || it.status === status) && (!matcher?.test || matcher.test(it)));
  S.sel = prevKey === undefined ? -1 : S.view.findIndex((it) => it.key === prevKey);
  list.setItems(S.view, { keepScroll });

  $('shown').textContent = src.length ? `${fmt(S.view.length)} / ${fmt(src.length)}` : '';
  const emptyText =
    S.tab === 'local'
      ? 'No hay mensajes respaldados'
      : !S.qpage
        ? 'Cargá las colas y elegí una'
        : !S.current
          ? 'Elegí una cola de la izquierda'
          : `Tocá "Ver (nack)" para leer hasta ${fmt(count())} mensajes`;
  $('emptyText').textContent = src.length && !S.view.length ? 'Ningún mensaje coincide con el filtro' : emptyText;
  $('empty').hidden = S.view.length > 0;
  renderDetail();
  renderSelection();
}

let filterTimer = 0;
$('filter').addEventListener('input', () => {
  clearTimeout(filterTimer);
  filterTimer = setTimeout(applyFilter, 120);
});
$('clearFilter').onclick = () => {
  $('filter').value = '';
  applyFilter();
  $('filter').focus();
};
$('statusFilter').onchange = () => applyFilter();

// --- lista virtual ----------------------------------------------------------------
function createRow() {
  const row = document.createElement('div');
  row.className = 'row';
  row.innerHTML =
    '<input type="checkbox" tabindex="-1"><span class="row__idx"></span><div class="row__body"><div class="row__meta"><span class="tag t1"></span><span class="tag t2"></span><span class="row__rk"></span><span class="row__info"></span></div><div class="row__preview"></div></div>';
  row._check = row.children[0];
  row._idx = row.children[1];
  const meta = row.children[2].children[0];
  row._t1 = meta.children[0];
  row._t2 = meta.children[1];
  row._rk = meta.children[2];
  row._info = meta.children[3];
  row._preview = row.children[2].children[1];
  return row;
}

const relTime = (ts) => new Date(ts).toLocaleString('es', { dateStyle: 'short', timeStyle: 'medium' });
const fmtSize = (b) => (b < 1024 ? `${b} B` : b < 1048576 ? `${(b / 1024).toFixed(1)} KB` : `${(b / 1048576).toFixed(1)} MB`);

function updateRow(row, it, i) {
  const local = S.tab === 'local';
  row._check.hidden = !local;
  row._check.checked = local && S.checked.has(it.key);
  row.classList.toggle('sel', i === S.sel);
  row._idx.textContent = it.idx;

  if (local) {
    row._t1.className = `tag tag--${it.status}`;
    row._t1.textContent = it.status;
    row._t2.className = 'tag';
    row._t2.textContent = it.op;
    row._rk.textContent = it.target ? `${it.source} → ${it.target}` : it.source;
    row._info.textContent = `${relTime(it.ts)} · ${fmtSize(it.size)}`;
  } else {
    row._t1.className = 'tag tag--redelivered';
    row._t1.textContent = it.redelivered ? 'redelivered' : '';
    row._t2.className = 'tag';
    row._t2.textContent = it.binary ? 'binario' : '';
    row._rk.textContent = it.routing_key || '(sin routing key)';
    row._info.textContent = `${fmtSize(it.size)}${it.properties?.content_type ? ' · ' + it.properties.content_type : ''}`;
  }
  if (it._preview === undefined) it._preview = (it.payload || '').slice(0, 400).replace(/\s+/g, ' ');
  row._preview.textContent = it.error ? `⚠ ${it.error} — ${it._preview}` : it._preview;
}

const list = createVirtualList($('list'), $('listInner'), { rowHeight: ROW_H, create: createRow, update: updateRow });

$('listInner').addEventListener('click', (ev) => {
  const row = ev.target.closest('.row');
  if (!row) return;
  const i = Number(row.dataset.index);
  if (ev.target === row._check) {
    toggleCheck(i, ev.shiftKey);
    return;
  }
  select(i);
});

function select(i) {
  if (i < 0 || i >= S.view.length) return;
  S.sel = i;
  list.scrollToIndex(i);
  list.refresh();
  renderDetail();
}

function toggleCheck(i, range) {
  const it = S.view[i];
  const on = !S.checked.has(it.key);
  const [a, b] = range && S.lastCheck >= 0 ? [Math.min(S.lastCheck, i), Math.max(S.lastCheck, i)] : [i, i];
  for (let k = a; k <= b; k++) {
    if (on) S.checked.add(S.view[k].key);
    else S.checked.delete(S.view[k].key);
  }
  S.lastCheck = i;
  list.refresh();
  renderSelection();
}

function renderSelection() {
  if (S.tab !== 'local') return;
  const n = S.checked.size;
  $('selCount').textContent = n ? `${fmt(n)} seleccionados` : '';
  $('republishSel').disabled = $('deleteSel').disabled = n === 0 || !!S.op;
  const visibleChecked = S.view.length > 0 && S.view.every((it) => S.checked.has(it.key));
  $('selAll').checked = visibleChecked;
  $('selAll').indeterminate = !visibleChecked && S.view.some((it) => S.checked.has(it.key));
}

$('selAll').onchange = () => {
  const on = $('selAll').checked;
  for (const it of S.view) on ? S.checked.add(it.key) : S.checked.delete(it.key);
  list.refresh();
  renderSelection();
};

// --- detalle -----------------------------------------------------------------------
function detailText(it) {
  if (S.detailView === 'props') {
    const { headers, ...rest } = it.properties || {};
    const extra = { routing_key: it.routing_key, exchange: it.exchange, size: it.size, redelivered: it.redelivered };
    if (it.batch) Object.assign(extra, { batch: it.batch, op: it.op, source: it.source, target: it.target, status: it.status, error: it.error });
    return JSON.stringify({ ...extra, ...rest }, null, 2);
  }
  if (S.detailView === 'headers') return JSON.stringify(it.properties?.headers || {}, null, 2);
  return it.payload;
}

function renderDetail() {
  const it = S.view[S.sel];
  const code = $('code');
  if (!it) {
    code.innerHTML = '<span class="dim">Seleccioná un mensaje</span>';
    $('detailMeta').textContent = '';
    return;
  }
  $('detailMeta').textContent = `#${it.idx} · ${fmtSize(it.size)}${it.binary ? ' · base64' : ''}`;
  renderJson(code, detailText(it));
  code.scrollTop = 0;
}

$('detailTabs').addEventListener('click', (ev) => {
  const b = ev.target.closest('button[data-view]');
  if (!b) return;
  S.detailView = b.dataset.view;
  for (const x of $('detailTabs').children) x.classList.toggle('active', x === b);
  renderDetail();
});

$('copy').onclick = async () => {
  const it = S.view[S.sel];
  if (!it) return;
  try {
    await navigator.clipboard.writeText(detailText(it));
    toast.ok('Copiado al portapapeles');
  } catch (err) {
    toast.err(`No se pudo copiar: ${err}`);
  }
};

// --- barra de operación (progreso determinado) ---------------------------------------
/** `mode`: "run" mientras se descargan mensajes, "hop" para el resto. */
function startOp(label, total, { cancellable = false, mode = 'hop' } = {}) {
  S.op = { label, total, start: performance.now() };
  log('info', `operación: ${label}`);
  setBunnyMode($('opRunner'), mode);
  $('loadingFill').style.transform = 'scaleX(0)';
  $('loadingCount').textContent = '';
  $('opLabel').textContent = label;
  $('opCancel').hidden = !cancellable;
  $('opCancel').disabled = false;
  $('opbar').hidden = false;
  $('opTrack').classList.add('indeterminate');
  $('opbar').querySelector('.progress').classList.add('indeterminate');
  $('opFill').style.transform = 'scaleX(0)';
  $('opRunner').style.left = '0%';
  $('opCount').textContent = '';
  setActionsDisabled(true);
}

function updateOp(done, total) {
  if (!S.op) return;
  S.op.total = total;
  const secs = (performance.now() - S.op.start) / 1000;
  const rate = secs > 0.3 ? ` · ${fmt(Math.round(done / secs))} msg/s` : '';
  const p = total ? Math.min(1, done / total) : 0;
  $('opTrack').classList.remove('indeterminate');
  $('opbar').querySelector('.progress').classList.remove('indeterminate');
  $('opFill').style.transform = `scaleX(${p})`;
  $('opRunner').style.left = `${p * 100}%`;
  $('opCount').textContent = `${fmt(done)} / ${fmt(total)}${rate}`;
  // El overlay de descarga muestra el mismo avance bajo el conejo corriendo.
  $('loadingFill').style.transform = `scaleX(${p})`;
  $('loadingCount').textContent = `${fmt(done)} / ${fmt(total)} mensajes`;
}

function endOp() {
  S.op = null;
  $('opbar').hidden = true;
  setActionsDisabled(false);
}

function setActionsDisabled(on) {
  for (const id of ['peek', 'ack', 'move']) $(id).disabled = on;
  renderSelection();
}

listen('progress', ({ payload }) => updateOp(payload.done, payload.total));

$('opCancel').onclick = async () => {
  $('opCancel').disabled = true;
  $('opLabel').textContent = 'Cancelando al terminar el lote actual…';
  await call('cancel').catch(() => {});
};

function lastOp(text) {
  $('lastOp').textContent = `${new Date().toLocaleTimeString('es')} · ${text}`;
}

// --- confirmación -----------------------------------------------------------------
function ask({ title, body, ok = 'Aceptar', danger = false, queue = null }) {
  return new Promise((resolve) => {
    const d = $('confirmDialog');
    $('confirmTitle').textContent = title;
    $('confirmBody').textContent = body;
    $('confirmOk').textContent = ok;
    $('confirmOk').className = `btn ${danger ? 'btn--danger' : 'btn--primary'}`;
    $('confirmQueueWrap').hidden = queue === null;
    if (queue !== null) $('confirmQueue').value = queue;
    d.returnValue = '';
    d.addEventListener(
      'close',
      () => resolve(d.returnValue === 'ok' ? (queue !== null ? $('confirmQueue').value.trim() || false : true) : false),
      { once: true }
    );
    d.showModal();
    (queue !== null ? $('confirmQueue') : $('confirmOk')).focus();
  });
}

// --- acciones de cola ---------------------------------------------------------------
const count = () => Math.min(MAX_N, Math.max(1, parseInt($('count').value, 10) || 1));

async function peek() {
  if (!S.current || S.op) return;
  const queue = S.current;
  const token = ++S.peekToken;
  const n = count();
  startOp(`Descargando hasta ${fmt(n)} mensajes de ${queue}…`, n, { mode: 'run' });
  showSkeleton(true, `Descargando mensajes de ${queue}…`);
  try {
    const r = await call('peek', { conn: conn(), queue, count: n });
    if (token !== S.peekToken || queue !== S.current) return;
    S.qItems = r.map(normQueueMsg);
    S.sel = S.qItems.length ? 0 : -1;
    if (S.tab === 'queue') applyFilter();
    lastOp(`${fmt(r.length)} mensajes leídos de ${queue}`);
  } catch (err) {
    toast.err(`Error leyendo ${queue}: ${err}`);
  } finally {
    showSkeleton(false);
    endOp();
  }
}

/** Esqueleto de filas con el conejo corriendo encima mientras carga la lista. */
function showSkeleton(on, text = 'Cargando…') {
  $('skeleton').hidden = !on;
  $('loading').hidden = !on;
  if (on) {
    $('skeleton').innerHTML = '<i></i>'.repeat(Math.ceil($('list').clientHeight / ROW_H) || 10);
    $('loadingText').textContent = text;
    // Sin operación con progreso (p. ej. leer el respaldo) no hay barra que mostrar.
    $('loadingBar').hidden = $('loadingCount').hidden = !S.op;
    $('empty').hidden = true;
  }
}

async function drain(target) {
  if (!S.current || S.op) return;
  const source = S.current;
  const n = count();
  const yes = await ask(
    target
      ? { title: `Mover ${fmt(n)} mensajes`, body: `De “${source}” a “${target}”.\n\nCada lote se respalda localmente, se publica con confirmación del broker y recién ahí se ackea en origen.`, ok: 'Mover' }
      : { title: `Ack de ${fmt(n)} mensajes`, body: `Se sacan de “${source}”.\n\nQuedan respaldados localmente y se pueden re-publicar desde “Respaldo local”.`, ok: 'Sacar mensajes', danger: true }
  );
  if (!yes) return;

  startOp(target ? `Moviendo ${source} → ${target}` : `Ackeando ${source}`, n, { cancellable: true, mode: 'run' });
  showSkeleton(true, target ? `Moviendo mensajes a ${target}…` : `Sacando mensajes de ${source}…`);
  try {
    const r = await call('drain', { conn: conn(), source, target: target || null, count: n });
    const verb = target ? 'movidos' : 'ackeados';
    if (r.total === 0) toast.info(`“${source}” no tiene mensajes ready`);
    else if (r.failed.length) toast.err(`${fmt(r.ok)} ${verb}, falló: ${r.failed[0]}. Los fallidos siguen en origen.`);
    else if (r.cancelled) toast.info(`Cancelado: ${fmt(r.ok)} de ${fmt(r.total)} ${verb}`);
    else {
      toast.ok(`${fmt(r.ok)} mensajes ${verb}${target ? ` a ${target}` : ''}`);
      bunnyDash();
    }
    lastOp(`${fmt(r.ok)} ${verb} de ${source}`);
  } catch (err) {
    toast.err(`Error: ${err}. Lo procesado quedó en el respaldo local.`);
  } finally {
    showSkeleton(false);
    endOp();
  }
  // Lo leído ya no refleja la cola; solo se refresca la página de colas ya cargada.
  S.qItems = [];
  applyFilter();
  if (S.qpage) loadQueues({ silent: true });
  refreshLocalCount();
}

$('peek').onclick = peek;
$('ack').onclick = () => drain(null);
$('move').onclick = () => {
  const t = $('target').value.trim();
  if (!t) return toast.err('Elegí la cola destino');
  if (t === S.current) return toast.err('El destino no puede ser la misma cola');
  // Que exista lo valida el backend contra el broker antes de sacar nada.
  drain(t);
};

// --- respaldo local ------------------------------------------------------------------
async function loadLocal() {
  showSkeleton(!S.localItems.length);
  try {
    const r = await call('list_local');
    S.localItems = r.map(normLocal);
    const keys = new Set(S.localItems.map((it) => it.key));
    for (const k of S.checked) if (!keys.has(k)) S.checked.delete(k);
    $('localCount').textContent = r.length ? fmt(r.length) : '';
    if (S.tab === 'local') applyFilter({ keepScroll: true });
  } catch (err) {
    toast.err(`No se pudo leer el respaldo: ${err}`);
  } finally {
    showSkeleton(false);
  }
}

async function refreshLocalCount() {
  if (S.tab === 'local') return loadLocal();
  try {
    const r = await call('list_local', undefined, { quiet: true });
    S.localItems = r.map(normLocal);
    $('localCount').textContent = r.length ? fmt(r.length) : '';
  } catch {}
}

const checkedRefs = () =>
  [...S.checked].map((k) => {
    const i = k.lastIndexOf(':');
    return { batch: k.slice(0, i), n: Number(k.slice(i + 1)) };
  });

$('republishSel').onclick = async () => {
  const items = checkedRefs();
  const queue = await ask({
    title: `Re-publicar ${fmt(items.length)} mensajes`,
    body: 'Se publican con sus properties y headers originales, con confirmación del broker.',
    ok: 'Re-publicar',
    queue: S.current || '',
  });
  if (!queue) return;
  startOp(`Re-publicando en ${queue}`, items.length);
  try {
    const n = await call('republish', { conn: conn(), items, queue });
    toast.ok(`${fmt(n)} mensajes re-publicados en ${queue}`);
    lastOp(`${fmt(n)} re-publicados en ${queue}`);
    S.checked.clear();
  } catch (err) {
    toast.err(`Re-publicación interrumpida: ${err}`);
  } finally {
    endOp();
  }
  loadLocal();
  if (S.qpage) loadQueues({ silent: true });
};

$('deleteSel').onclick = async () => {
  const items = checkedRefs();
  const pending = S.localItems.filter((it) => S.checked.has(it.key) && it.status !== 'done' && it.status !== 'republished').length;
  const yes = await ask({
    title: `Borrar ${fmt(items.length)} respaldos`,
    body: pending
      ? `Atención: ${fmt(pending)} no están en estado done/republished.\nEsto no se puede deshacer.`
      : 'Esto no se puede deshacer.',
    ok: 'Borrar',
    danger: true,
  });
  if (!yes) return;
  try {
    const n = await call('delete_local', { items });
    toast.ok(`${fmt(n)} respaldos borrados`);
    S.checked.clear();
  } catch (err) {
    toast.err(`No se pudo borrar: ${err}`);
  }
  loadLocal();
};

// --- tabs -------------------------------------------------------------------------
function placeIndicator() {
  const active = document.querySelector('.tab.active');
  const ind = document.querySelector('.tab-indicator');
  ind.style.width = `${active.offsetWidth}px`;
  ind.style.transform = `translateX(${active.offsetLeft}px)`;
}

function setTab(t) {
  if (S.tab === t) return;
  S.tab = t;
  S.sel = -1;
  S.view = [];
  for (const b of document.querySelectorAll('.tab')) b.classList.toggle('active', b.dataset.tab === t);
  placeIndicator();
  $('queueActions').hidden = t !== 'queue';
  $('localActions').hidden = t !== 'local';
  applyFilter();
  if (t === 'local') loadLocal();
}
for (const b of document.querySelectorAll('.tab')) b.onclick = () => setTab(b.dataset.tab);

// --- splitter del detalle -------------------------------------------------------------
{
  const sp = $('splitter');
  const detail = $('detail');
  const saved = store.get('detailWidth', null);
  if (saved) detail.style.width = `${saved}px`;
  const clamp = (w) => Math.max(280, Math.min(w, $('detail').parentElement.clientWidth - 300));
  sp.addEventListener('pointerdown', (ev) => {
    sp.setPointerCapture(ev.pointerId);
    sp.classList.add('dragging');
    const startX = ev.clientX;
    const startW = detail.offsetWidth;
    const move = (e) => (detail.style.width = `${clamp(startW - (e.clientX - startX))}px`);
    const up = () => {
      sp.classList.remove('dragging');
      sp.removeEventListener('pointermove', move);
      store.set('detailWidth', detail.offsetWidth);
    };
    sp.addEventListener('pointermove', move);
    sp.addEventListener('pointerup', up, { once: true });
  });
  sp.addEventListener('dblclick', () => {
    detail.style.width = '';
    store.set('detailWidth', null);
  });
  sp.addEventListener('keydown', (ev) => {
    const step = ev.shiftKey ? 64 : 16;
    if (ev.key === 'ArrowLeft') detail.style.width = `${clamp(detail.offsetWidth + step)}px`;
    else if (ev.key === 'ArrowRight') detail.style.width = `${clamp(detail.offsetWidth - step)}px`;
  });
}

// --- tema ---------------------------------------------------------------------------
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  store.set('theme', t);
}
applyTheme(store.get('theme', 'dark'));
$('theme').onclick = () => applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');

// --- teclado ------------------------------------------------------------------------
document.addEventListener('keydown', (ev) => {
  const mod = ev.ctrlKey || ev.metaKey;
  const inField = ev.target.matches('input, select, textarea') && ev.target.type !== 'checkbox';
  if (document.querySelector('dialog[open]')) return;
  if (mod && ev.key.toLowerCase() === 'f') {
    ev.preventDefault();
    $('filter').focus();
    $('filter').select();
  } else if (ev.key === 'F5' || (mod && ev.key.toLowerCase() === 'r' && ev.shiftKey)) {
    ev.preventDefault();
    loadQueues();
  } else if (mod && ev.key.toLowerCase() === 'r') {
    ev.preventDefault();
    if (S.tab === 'queue') peek();
    else loadLocal();
  } else if (ev.key === 'Escape' && ev.target === $('filter')) {
    $('clearFilter').click();
  } else if (!inField && (ev.key === 'ArrowDown' || ev.key === 'j')) {
    ev.preventDefault();
    select(Math.min(S.view.length - 1, S.sel + 1));
  } else if (!inField && (ev.key === 'ArrowUp' || ev.key === 'k')) {
    ev.preventDefault();
    select(Math.max(0, S.sel - 1));
  } else if (!inField && ev.key === ' ' && S.tab === 'local' && S.sel >= 0) {
    ev.preventDefault();
    toggleCheck(S.sel, ev.shiftKey);
  } else if (!inField && mod && ev.key.toLowerCase() === 'a' && S.tab === 'local') {
    ev.preventDefault();
    $('selAll').checked = true;
    $('selAll').onchange();
  }
});

// --- arranque -----------------------------------------------------------------------
$('connect').onclick = connect;
$('editConn').onclick = openConnDialog;
$('profile').onchange = () => selectProfile($('profile').value);
// Colas: nada se trae solo. Cargar / Enter / paginar / cambiar orden o tamaño son
// acciones explícitas, y siempre piden una sola página al broker.
$('loadQueues').onclick = () => busy($('loadQueues'), () => loadQueues({ page: 1 }));
$('refresh').onclick = () => loadQueues();
$('qfilter').addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter') $('loadQueues').click();
});
$('prevPage').onclick = () => loadQueues({ page: S.qq.page - 1 });
$('nextPage').onclick = () => loadQueues({ page: S.qq.page + 1 });
$('qsize').value = String(S.qq.size);
$('qsize').onchange = () => {
  S.qq.size = Number($('qsize').value);
  store.set('qsize', S.qq.size);
  if (S.qpage) loadQueues({ page: 1 });
};
for (const b of $('qsort').children) {
  b.classList.toggle('active', b.dataset.sort === S.qq.sort);
  b.onclick = () => {
    S.qq.sort = b.dataset.sort;
    store.set('qsort', S.qq.sort);
    for (const x of $('qsort').children) x.classList.toggle('active', x === b);
    if (S.qpage) loadQueues({ page: 1 });
  };
}

let autoTimer = 0;
$('autoRefresh').checked = store.get('autoRefresh', false);
function setAuto() {
  clearInterval(autoTimer);
  store.set('autoRefresh', $('autoRefresh').checked);
  // Solo refresca la página ya cargada (5–20 colas), nunca carga por su cuenta.
  if ($('autoRefresh').checked) autoTimer = setInterval(() => S.qpage && !document.hidden && loadQueues({ silent: true }), 5000);
}
$('autoRefresh').onchange = setAuto;
setAuto();

call('backup_path', undefined, { quiet: true }).then((p) => {
  $('backupPath').textContent = `Respaldo: ${p}`;
  $('backupPath').onclick = () => navigator.clipboard.writeText(p).then(() => toast.ok('Ruta copiada'));
});
call('log_path', undefined, { quiet: true }).then((p) => ($('openLog').title = `Abrir carpeta del log: ${p}`));
$('openLog').onclick = () => call('reveal_log', undefined, { quiet: true }).catch((err) => toast.err(`No se pudo abrir el log: ${err}`));

renderProfiles();
setConnected(false);
requestAnimationFrame(placeIndicator);
window.addEventListener('resize', placeIndicator);
renderQueues();
applyFilter();
refreshLocalCount();
setConn('', `Perfil ${profile()?.name ?? ''} — sin conectar`);
