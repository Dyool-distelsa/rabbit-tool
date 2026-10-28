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
  fileItems: [], // mensajes cargados desde un .json
  fileName: '',
  backupId: null, // respaldo mostrado en la pestaña (null = el último)
  // Selección por pestaña: claves de items (índice en cola/archivo, "run:n" en respaldo).
  picks: { queue: new Set(), file: new Set(), local: new Set() },
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
  // AMQP vacío o apuntando al puerto de management: se arma con el host de la API y 5672.
  const apiHost = p.api.replace(/^https?:\/\//, '').split(/[/:]/)[0];
  if (!p.amqp || /:15672\/?$/.test(p.amqp)) {
    const fixed = `amqp://${apiHost || 'localhost'}:5672`;
    if (p.amqp) toast.info(`AMQP apuntaba al puerto de management (15672); lo cambié a ${fixed}`);
    p.amqp = fixed;
  }
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
    const info = `${r.user}@${hostOf(c)} · vhost ${c.vhost}${r.queues != null ? ` · ${fmt(r.queues)} colas` : ''}`;
    if (r.amqp_error) {
      // Management responde: se pueden listar colas, pero leer/mover necesita AMQP.
      setConn('busy', `${info} · AMQP con error`);
      toast.err(`Management OK, pero AMQP falló: ${r.amqp_error}. "Ver" y "Mover" no van a funcionar hasta corregir el campo AMQP.`);
    } else {
      setConn('ok', info);
      toast.ok(`Conectado a ${profile().name}`);
    }
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
    showDig(true);
  }
  const fetchPage = async () => {
    try {
      return await call('list_queues', { conn: conn(), q: queueQuery() }, { quiet: silent });
    } catch (err) {
      // La página quedó fuera de rango (se borraron colas o cambió el filtro).
      if (S.qq.page === 1) throw err;
      S.qq.page = 1;
      return call('list_queues', { conn: conn(), q: queueQuery() }, { quiet: silent });
    }
  };
  try {
    const r = silent ? await fetchPage() : await atLeast(fetchPage(), MIN_ANIM_MS);
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
    if (!silent) showDig(false);
  }
}

/** Conejo escarbando en el sidebar mientras se buscan colas. */
function showDig(on) {
  $('qdig').hidden = !on;
  if (on) {
    $('qempty').hidden = true;
    $('pager').hidden = true;
    $('queues').classList.remove('anim');
    $('queues').replaceChildren();
    const f = $('qfilter').value.trim();
    $('qdigText').textContent = f ? `Escarbando colas con “${f}”…` : 'Escarbando colas…';
  }
}

/**
 * Espera a `promise` pero no menos de `ms`: con un broker rápido la carga dura
 * milisegundos y el conejo ni llegaba a verse.
 */
const MIN_ANIM_MS = 900;
async function atLeast(promise, ms) {
  const [r] = await Promise.all([promise, new Promise((res) => setTimeout(res, ms))]);
  return r;
}

/**
 * Pide los ready exactos (AMQP) de las colas tocadas y los aplica en la lista:
 * las estadísticas de management llegan con ~5 s de atraso.
 */
async function refreshExact(names) {
  names = [...new Set(names.filter(Boolean))];
  if (!names.length) return;
  try {
    const ready = await call('queues_ready', { conn: conn(), names }, { quiet: true });
    for (const [name, n] of Object.entries(ready)) {
      const q = S.queues.find((x) => x.name === name);
      if (q) {
        q.messages = n + (q.unacked || 0);
        q.ready = n;
      }
      if (name === S.current) S.currentInfo = { ...(S.currentInfo || { name, unacked: 0 }), ready: n };
    }
    renderQueues();
  } catch {}
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
    S.picks.queue.clear();
    S.peekToken++;
  }
  // Desde la pestaña Archivo, elegir una cola la propone como destino.
  if (S.tab === 'file') {
    $('fileTarget').value = li.dataset.name;
    return;
  }
  if (S.tab !== 'queue') setTab('queue');
  else applyFilter();
});

// Sugerencias de cola destino: búsqueda acotada en el broker mientras se escribe.
let targetTimer = 0;
const suggestQueues = (ev) => {
  clearTimeout(targetTimer);
  const name = ev.target.value.trim();
  if (!name || !S.connected) return;
  targetTimer = setTimeout(async () => {
    try {
      const r = await call('list_queues', { conn: conn(), q: { page: 1, page_size: 20, sort: 'name', name, regex: false } }, { quiet: true });
      const dl = document.createDocumentFragment();
      for (const q of r.items) dl.appendChild(new Option(q.name));
      $('queueNames').replaceChildren(dl);
    } catch {}
  }, 250);
};
for (const id of ['target', 'fileTarget', 'confirmQueue']) $(id).addEventListener('input', suggestQueues);

// --- normalización + filtro local -------------------------------------------------
function normQueueMsg(m, i) {
  return { ...m, key: i, idx: i + 1 };
}
function normLocal(b, i) {
  return { ...b, key: `${b.batch}:${b.n}`, idx: i + 1 };
}

/**
 * Un elemento del array del archivo. Es el cuerpo del mensaje tal cual, salvo que
 * sea un "sobre" {payload, properties?, routing_key?} (como lo que muestra la app):
 * ahí se respetan sus properties.
 */
const ENVELOPE_KEYS = new Set(['payload', 'properties', 'routing_key', 'exchange']);
function normFile(el, i) {
  const envelope =
    el && typeof el === 'object' && !Array.isArray(el) && 'payload' in el && Object.keys(el).every((k) => ENVELOPE_KEYS.has(k));
  const body = envelope ? el.payload : el;
  const payload = typeof body === 'string' ? body : JSON.stringify(body);
  const properties = envelope && el.properties ? el.properties : { content_type: 'application/json', delivery_mode: 2 };
  return {
    key: i,
    idx: i + 1,
    file: true,
    payload,
    properties,
    routing_key: envelope ? el.routing_key || '' : '',
    size: new TextEncoder().encode(payload).length,
  };
}

/** Items y selección de la pestaña activa. */
const itemsOf = (tab = S.tab) => (tab === 'queue' ? S.qItems : tab === 'file' ? S.fileItems : S.localItems);
const picks = (tab = S.tab) => S.picks[tab];
const pickedItems = (tab = S.tab) => itemsOf(tab).filter((it) => picks(tab).has(it.key));

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
  const src = itemsOf();
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
      ? 'No hay respaldos: se crea uno en cada mover o ack'
      : S.tab === 'file'
        ? 'Abrí un .json con un array: cada elemento es un mensaje'
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
  row._check.checked = picks().has(it.key);
  row.classList.toggle('sel', i === S.sel);
  row.classList.toggle('picked', row._check.checked);
  row._idx.textContent = it.idx;

  if (S.tab === 'local') {
    row._t1.className = `tag tag--${it.status}`;
    row._t1.textContent = it.status;
    row._t2.className = 'tag';
    row._t2.textContent = it.op;
    row._rk.textContent = it.target ? `${it.source} → ${it.target}` : it.source;
    row._info.textContent = `${relTime(it.ts)} · ${fmtSize(it.size)}`;
  } else if (S.tab === 'file') {
    row._t1.className = 'tag';
    row._t1.textContent = 'archivo';
    row._t2.className = 'tag';
    row._t2.textContent = '';
    row._rk.textContent = it.routing_key || S.fileName;
    row._info.textContent = `${fmtSize(it.size)}${it.properties?.content_type ? ' · ' + it.properties.content_type : ''}`;
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
  // Click en la casilla, o Ctrl/Shift+click en la fila, selecciona.
  if (ev.target === row._check || ev.ctrlKey || ev.metaKey || ev.shiftKey) {
    if (ev.target !== row._check) ev.preventDefault();
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
  const set = picks();
  const on = !set.has(it.key);
  const [a, b] = range && S.lastCheck >= 0 ? [Math.min(S.lastCheck, i), Math.max(S.lastCheck, i)] : [i, i];
  for (let k = a; k <= b; k++) {
    if (on) set.add(S.view[k].key);
    else set.delete(S.view[k].key);
  }
  S.lastCheck = i;
  list.refresh();
  renderSelection();
}

/** Contador de seleccionados y textos de los botones según la pestaña. */
function renderSelection() {
  const set = picks();
  const n = set.size;
  const visibleChecked = S.view.length > 0 && S.view.every((it) => set.has(it.key));
  $('selAll').checked = visibleChecked;
  $('selAll').indeterminate = !visibleChecked && S.view.some((it) => set.has(it.key));
  $('selAll').disabled = !S.view.length;
  $('selCount').hidden = n === 0;
  $('selCount').textContent = `${fmt(n)} seleccionados ✕`;
  $('selCount').title = 'Quitar la selección';

  const busyOp = !!S.op;
  if (S.tab === 'queue') {
    $('ack').textContent = n ? `Ack ${fmt(n)} sel.` : 'Ack N';
    $('move').textContent = n ? `Mover ${fmt(n)} sel. →` : 'Mover N →';
  } else if (S.tab === 'file') {
    const visible = S.view.length;
    $('publishFile').textContent = n ? `Publicar ${fmt(n)} sel. →` : `Publicar ${fmt(visible)} →`;
    $('publishFile').disabled = busyOp || (!n && !visible);
  } else {
    // Sin selección re-publica todos los visibles; borrar exige selección explícita.
    $('republishSel').textContent = n ? `Re-publicar ${fmt(n)} sel.…` : `Re-publicar ${fmt(S.view.length)}…`;
    $('republishSel').disabled = busyOp || (!n && !S.view.length);
    $('deleteSel').disabled = busyOp || n === 0;
  }
}

$('selAll').onchange = () => {
  const on = $('selAll').checked;
  const set = picks();
  for (const it of S.view) on ? set.add(it.key) : set.delete(it.key);
  list.refresh();
  renderSelection();
};
$('selCount').onclick = () => {
  picks().clear();
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
    const r = await atLeast(call('peek', { conn: conn(), queue, count: n }), MIN_ANIM_MS);
    if (token !== S.peekToken || queue !== S.current) return;
    refreshExact([queue]);
    S.qItems = r.map(normQueueMsg);
    // Las claves son posiciones de esta lectura: una selección anterior ya no aplica.
    S.picks.queue.clear();
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

/**
 * Ready exactos (AMQP) de origen y destino. Devuelve el ready de origen o null si
 * hay que abortar (ya avisó con un toast).
 */
async function checkQueues(source, target, btn) {
  try {
    const names = target ? [source, target] : [source];
    const exact = await busy(btn, () => call('queues_ready', { conn: conn(), names }));
    if (target && !(target in exact)) return toast.err(`La cola destino “${target}” no existe o no es accesible`), null;
    if (!(source in exact)) return toast.err(`No se pudo leer “${source}”`), null;
    refreshExact(names);
    if (!exact[source]) return toast.info(`“${source}” no tiene mensajes ready`), null;
    return exact[source];
  } catch (err) {
    toast.err(`No se pudo consultar la cola: ${err}`);
    return null;
  }
}

/** Ack/Mover: con mensajes seleccionados actúa sobre esos; si no, sobre los primeros N. */
async function drain(target) {
  if (!S.current || S.op) return;
  if (S.picks.queue.size) return drainSelected(target);
  const source = S.current;
  const asked = count();

  // Cantidad real: lo pedido acotado a lo que hay ready ahora (AMQP, sin el atraso
  // de las estadísticas). Así el diálogo no promete mover 100 si hay 5.
  const ready = await checkQueues(source, target, $(target ? 'move' : 'ack'));
  if (ready == null) return;
  const n = Math.min(asked, ready);
  const note = asked > ready ? `\nPediste ${fmt(asked)}, pero la cola tiene ${fmt(ready)} ready.` : `\nLa cola tiene ${fmt(ready)} ready.`;

  const yes = await ask(
    target
      ? { title: `Mover ${fmt(n)} ${n === 1 ? 'mensaje' : 'mensajes'}`, body: `De “${source}” a “${target}”.${note}\n\nSe guarda un respaldo nuevo de este envío, se publica con confirmación del broker y recién ahí se ackea en origen.`, ok: 'Mover' }
      : { title: `Ack de ${fmt(n)} ${n === 1 ? 'mensaje' : 'mensajes'}`, body: `Se sacan de “${source}”.${note}\n\nQuedan en un respaldo nuevo y se pueden volver a cargar desde “Último respaldo”.`, ok: 'Sacar mensajes', danger: true }
  );
  if (!yes) return;

  startOp(target ? `Moviendo ${source} → ${target}` : `Ackeando ${source}`, n, { cancellable: true, mode: 'run' });
  showSkeleton(true, target ? `Moviendo mensajes a ${target}…` : `Sacando mensajes de ${source}…`);
  try {
    const r = await atLeast(call('drain', { conn: conn(), source, target: target || null, count: n }), MIN_ANIM_MS);
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
  // Lo leído ya no refleja la cola. Los contadores de origen y destino se toman
  // exactos por AMQP; un refresco completo de la página corregiría con datos viejos.
  S.qItems = [];
  S.picks.queue.clear();
  applyFilter();
  refreshExact([source, target]);
  showLatestBackup();
}

/**
 * Mueve/ackea solo los seleccionados. El backend recorre el principio de la cola,
 * los reconoce por huella y devuelve el resto a la cola en su orden.
 */
async function drainSelected(target) {
  const source = S.current;
  const chosen = pickedItems('queue');
  const k = chosen.length;
  if (!k) return;
  if ((await checkQueues(source, target, $(target ? 'move' : 'ack'))) == null) return;
  // Lo leído son los primeros mensajes de la cola: se recorre esa ventana más un margen.
  const scan = S.qItems.length + 50;
  const what = `${fmt(k)} ${k === 1 ? 'mensaje seleccionado' : 'mensajes seleccionados'}`;
  const how = `\n\nSe recorren hasta ${fmt(scan)} mensajes del principio de la cola para encontrarlos; los no seleccionados vuelven a la cola en su orden. Se guarda un respaldo nuevo antes de cualquier ack.`;
  const yes = await ask(
    target
      ? { title: `Mover ${what}`, body: `De “${source}” a “${target}”.${how}`, ok: 'Mover' }
      : { title: `Ack de ${what}`, body: `Se sacan de “${source}”.${how}`, ok: 'Sacar mensajes', danger: true }
  );
  if (!yes) return;

  startOp(target ? `Moviendo ${what} → ${target}` : `Ackeando ${what}`, k, { mode: 'run' });
  showSkeleton(true, target ? `Moviendo seleccionados a ${target}…` : `Sacando seleccionados de ${source}…`);
  try {
    const r = await atLeast(
      call('move_selected', { conn: conn(), source, target: target || null, fps: chosen.map((it) => it.fp), scan }),
      MIN_ANIM_MS
    );
    const verb = target ? 'movidos' : 'ackeados';
    if (r.failed.length) toast.err(`${fmt(r.ok)} ${verb}, falló: ${r.failed[0]}. Los fallidos siguen en origen.`);
    else if (r.not_found) toast.info(`${fmt(r.ok)} de ${fmt(r.total)} ${verb}. ${fmt(r.not_found)} ya no estaban en la cola (los consumió otro o cambiaron).`);
    else if (r.ok) {
      toast.ok(`${fmt(r.ok)} seleccionados ${verb}${target ? ` a ${target}` : ''}`);
      bunnyDash();
    }
    lastOp(`${fmt(r.ok)} seleccionados ${verb} de ${source}`);

    // Saca de la lista los que se fueron (por huella, respetando duplicados); el
    // resto sigue en la cola y queda visible.
    const gone = new Map();
    for (const fp of r.moved) gone.set(fp, (gone.get(fp) || 0) + 1);
    const remaining = S.qItems.filter((it) => {
      const c = gone.get(it.fp);
      if (!c) return true;
      gone.set(it.fp, c - 1);
      return false;
    });
    S.qItems = remaining.map((it, i) => ({ ...it, key: i, idx: i + 1 }));
    S.picks.queue.clear();
  } catch (err) {
    toast.err(`Error: ${err}. Lo procesado quedó en el respaldo local.`);
  } finally {
    showSkeleton(false);
    endOp();
  }
  applyFilter({ keepScroll: true });
  refreshExact([source, target]);
  showLatestBackup();
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

// --- archivo JSON -------------------------------------------------------------------
$('openFile').onclick = () => $('fileInput').click();
$('fileInput').onchange = async () => {
  const f = $('fileInput').files[0];
  if (!f) return;
  try {
    let data = JSON.parse(await f.text());
    if (!Array.isArray(data)) {
      // Tolerancia: {"messages": [...]} también vale.
      if (data && Array.isArray(data.messages)) data = data.messages;
      else throw new Error('tiene que ser un array JSON: [ {...}, {...} ]');
    }
    S.fileItems = data.map(normFile);
    S.fileName = f.name;
    S.picks.file.clear();
    $('fileName').textContent = `${f.name} · ${fmt(data.length)} mensajes`;
    $('fileCount').textContent = data.length ? fmt(data.length) : '';
    log('info', `archivo cargado: ${f.name} (${data.length} mensajes, ${f.size} bytes)`);
    toast.ok(`${fmt(data.length)} mensajes cargados de ${f.name}`);
    if (S.tab !== 'file') setTab('file');
    else applyFilter();
    if (!$('fileTarget').value && S.current) $('fileTarget').value = S.current;
  } catch (err) {
    toast.err(`No se pudo leer ${f.name}: ${err.message ?? err}`);
  } finally {
    $('fileInput').value = '';
  }
};

$('publishFile').onclick = async () => {
  if (S.op) return;
  const queue = $('fileTarget').value.trim();
  if (!queue) return toast.err('Elegí la cola destino');
  // Seleccionados, o todos los que deja ver el filtro.
  const chosen = S.picks.file.size ? pickedItems('file') : S.view;
  const k = chosen.length;
  if (!k) return;
  const yes = await ask({
    title: `Publicar ${fmt(k)} ${k === 1 ? 'mensaje' : 'mensajes'} en “${queue}”`,
    body: `Desde ${S.fileName}${S.picks.file.size ? ' (seleccionados)' : $('filter').value ? ' (los que pasan el filtro)' : ''}.\n\nPersistentes y con content_type application/json, salvo que el elemento traiga sus propias properties. Con confirmación del broker.`,
    ok: 'Publicar',
  });
  if (!yes) return;
  startOp(`Publicando en ${queue}`, k, { mode: 'hop' });
  try {
    const r = await atLeast(
      call('publish_messages', { conn: conn(), queue, messages: chosen.map((it) => ({ payload: it.payload, properties: it.properties })) }),
      MIN_ANIM_MS
    );
    if (r.failed.length) toast.err(`${fmt(r.ok)} de ${fmt(r.total)} publicados; falló: ${r.failed[0]}`);
    else {
      toast.ok(`${fmt(r.ok)} mensajes publicados en ${queue}`);
      bunnyDash();
    }
    lastOp(`${fmt(r.ok)} publicados desde ${S.fileName} en ${queue}`);
    S.picks.file.clear();
  } catch (err) {
    toast.err(`No se pudo publicar: ${err}`);
  } finally {
    endOp();
  }
  applyFilter({ keepScroll: true });
  refreshExact([queue]);
};

// --- respaldo local (uno por envío; la pestaña muestra el último) ---------------------
const bkpLabel = (b) =>
  `${relTime(b.ts)} · ${b.op === 'move' ? 'mover' : 'ack'} · ${b.source}${b.target ? ` → ${b.target}` : ''} · ${fmt(b.count)} msgs`;

/** Lista de respaldos para el selector; el badge muestra el tamaño del último. */
async function loadBackups() {
  const runs = await call('list_backups', undefined, { quiet: true });
  S.backups = runs;
  if (!runs.some((b) => b.id === S.backupId)) S.backupId = runs[0]?.id ?? null;
  const sel = $('bkpSelect');
  sel.replaceChildren(...runs.map((b, i) => new Option(`${i === 0 ? 'Último · ' : ''}${bkpLabel(b)}`, b.id)));
  if (!runs.length) sel.appendChild(new Option('Sin respaldos todavía', ''));
  sel.value = S.backupId ?? '';
  $('localCount').textContent = runs[0]?.count ? fmt(runs[0].count) : '';
}

async function loadLocal() {
  showSkeleton(!S.localItems.length, 'Leyendo respaldo…');
  try {
    await loadBackups();
    const r = S.backupId ? await call('load_backup', { id: S.backupId }) : { entries: [] };
    S.localItems = r.entries.map(normLocal);
    const keys = new Set(S.localItems.map((it) => it.key));
    for (const k of S.picks.local) if (!keys.has(k)) S.picks.local.delete(k);
    if (S.tab === 'local') applyFilter({ keepScroll: true });
  } catch (err) {
    toast.err(`No se pudo leer el respaldo: ${err}`);
  } finally {
    showSkeleton(false);
  }
}

/** Tras un envío nuevo, la pestaña vuelve a mostrar el último respaldo. */
function showLatestBackup() {
  S.backupId = null;
  S.picks.local.clear();
  if (S.tab === 'local') loadLocal();
  else loadBackups().catch(() => {});
}

$('bkpSelect').onchange = () => {
  S.backupId = $('bkpSelect').value || null;
  S.picks.local.clear();
  S.localItems = [];
  loadLocal();
};

const refsOf = (items) => items.map((it) => ({ batch: it.batch, n: it.n }));

$('republishSel').onclick = async () => {
  // Seleccionados, o todos los visibles del respaldo.
  const chosen = S.picks.local.size ? pickedItems('local') : S.view;
  if (!chosen.length) return;
  const queue = await ask({
    title: `Re-publicar ${fmt(chosen.length)} mensajes`,
    body: 'Se publican con sus properties y headers originales, con confirmación del broker. Por defecto, en la cola de donde salieron.',
    ok: 'Re-publicar',
    queue: chosen[0].source || S.current || '',
  });
  if (!queue) return;
  startOp(`Re-publicando en ${queue}`, chosen.length, { mode: 'hop' });
  try {
    const n = await atLeast(call('republish', { conn: conn(), items: refsOf(chosen), queue }), MIN_ANIM_MS);
    toast.ok(`${fmt(n)} mensajes re-publicados en ${queue}`);
    bunnyDash();
    lastOp(`${fmt(n)} re-publicados en ${queue}`);
    S.picks.local.clear();
  } catch (err) {
    toast.err(`Re-publicación interrumpida: ${err}`);
  } finally {
    endOp();
  }
  loadLocal();
  refreshExact([queue]);
};

$('deleteSel').onclick = async () => {
  const chosen = pickedItems('local');
  if (!chosen.length) return;
  const pending = chosen.filter((it) => it.status !== 'done' && it.status !== 'republished').length;
  const yes = await ask({
    title: `Borrar ${fmt(chosen.length)} del respaldo`,
    body: pending
      ? `Atención: ${fmt(pending)} no están en estado done/republished.\nEsto no se puede deshacer.`
      : 'Esto no se puede deshacer.',
    ok: 'Borrar',
    danger: true,
  });
  if (!yes) return;
  try {
    const n = await call('delete_local', { items: refsOf(chosen) });
    toast.ok(`${fmt(n)} mensajes borrados del respaldo`);
    S.picks.local.clear();
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
  S.lastCheck = -1;
  for (const b of document.querySelectorAll('.tab')) b.classList.toggle('active', b.dataset.tab === t);
  placeIndicator();
  $('queueActions').hidden = t !== 'queue';
  $('fileActions').hidden = t !== 'file';
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
  } else if (!inField && ev.key === ' ' && S.sel >= 0) {
    ev.preventDefault();
    toggleCheck(S.sel, ev.shiftKey);
  } else if (!inField && mod && ev.key.toLowerCase() === 'a') {
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
loadBackups().catch(() => {});
setConn('', `Perfil ${profile()?.name ?? ''} — sin conectar`);
