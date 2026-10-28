import { mountToastContainer } from './toast.js';
import { createVirtualList } from './vlist.js';
import { renderJson } from './highlight.js';
import { mountBunnies, setBunnyMode, bunnyDash, bunny, eagle, terrorScene, terrorLevel, TERROR_TEXT } from './bunny.js';
import { worstEnv, ENV_INFO } from './env.js';
import { renderHelp } from './help.js';

mountBunnies();

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

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

const S = {
  queues: [],
  current: null,
  tab: 'queue',
  // Página de colas: la pide y la ordena el broker, nunca se trae el vhost entero.
  qq: { page: 1, size: store.get('qsize', 10), sort: store.get('qsort', 'messages'), name: '' },
  qpage: null, // { page, page_count, filtered_count, total_count } de la última carga
  currentInfo: null, // último dato conocido de la cola seleccionada
  qItems: [],
  msgPage: null, // { offset, limit, ready } de la página de mensajes mostrada
  qfp: new Map(), // posición en la cola → huella, de los seleccionados (de cualquier página)
  allQueue: false, // "seleccionar toda la cola" (ack/mover todos los ready)
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

// Contraseñas: en el Administrador de credenciales del sistema, no en localStorage.
// Acá solo vive la copia en memoria de esta sesión.
const secrets = {};

/** Guarda la contraseña de un perfil en el almacén del sistema (y en memoria). */
async function saveSecret(name, pass) {
  secrets[name] = pass;
  try {
    await call('secret_set', { profile: name, pass }, { quiet: true });
    return true;
  } catch (err) {
    toast.err(`No se pudo guardar la contraseña en el Administrador de credenciales (${err}). Vale solo para esta sesión.`);
    return false;
  }
}

/**
 * Carga las contraseñas guardadas y migra las viejas que estaban en texto plano
 * en localStorage: se mueven al almacén y se borran del perfil.
 */
async function initSecrets() {
  try {
    Object.assign(secrets, await call('secret_get_all', { profiles: profiles.map((p) => p.name) }, { quiet: true }));
  } catch (err) {
    log('warn', `no se pudieron leer las contraseñas: ${err}`);
  }
  const legacy = profiles.filter((p) => p.pass);
  for (const p of legacy) {
    if (await saveSecret(p.name, p.pass)) delete p.pass;
  }
  if (legacy.length) {
    store.set('profiles', profiles);
    log('info', `contraseñas migradas al Administrador de credenciales: ${legacy.map((p) => p.name).join(', ')}`);
  }
}

const profile = () => profiles.find((p) => p.name === profileName) || profiles[0];
function conn() {
  const p = profile() || {};
  return {
    api: p.api || DEFAULTS.api,
    amqp: p.amqp || DEFAULTS.amqp,
    user: p.user || '',
    pass: secrets[p.name] ?? p.pass ?? '',
    vhost: p.vhost || '/',
  };
}

function renderProfiles() {
  $('profile').innerHTML = '';
  for (const p of profiles) $('profile').add(new Option(p.name, p.name, false, p.name === profileName));
}

const PROFILE_FIELDS = ['name', 'api', 'amqp', 'user', 'pass', 'vhost'];
// Resultado de "Probar conexión" para los valores exactos del formulario.
let lastTest = null; // { key, ok }

function openConnDialog() {
  const form = $('connDialog').querySelector('form');
  const p = profile() || { name: '', ...DEFAULTS };
  for (const f of PROFILE_FIELDS) form.elements[f].value = p[f] ?? '';
  form.elements.pass.value = secrets[p.name] ?? p.pass ?? '';
  $('connDelete').hidden = profiles.length < 2;
  $('connTest').hidden = true;
  lastTest = null;
  $('connDialog').showModal();
}

/** Valores del formulario normalizados (AMQP vacío o en 15672 → host de la API:5672). */
function formProfile({ announce = false } = {}) {
  const f = $('connDialog').querySelector('form').elements;
  const p = Object.fromEntries(PROFILE_FIELDS.map((k) => [k, f[k].value.trim()]));
  const apiHost = p.api.replace(/^https?:\/\//, '').split(/[/:]/)[0];
  if (!p.amqp || /:15672\/?$/.test(p.amqp)) {
    const fixed = `amqp://${apiHost || 'localhost'}:5672`;
    if (p.amqp && announce) toast.info(`AMQP apuntaba al puerto de management (15672); lo cambié a ${fixed}`);
    p.amqp = fixed;
    f.amqp.value = fixed;
  }
  return p;
}
const profileKey = (p) => JSON.stringify(PROFILE_FIELDS.slice(1).map((k) => p[k]));

function showConnTest(kind, html) {
  const el = $('connTest');
  el.hidden = false;
  el.className = `conn-test conn-test--${kind}`;
  el.innerHTML = html;
}

// Probar sin guardar ni cerrar: management (usuario, vhost) y AMQP con los datos del formulario.
$('connTestBtn').onclick = async () => {
  const p = formProfile({ announce: true });
  const c = { api: p.api || DEFAULTS.api, amqp: p.amqp, user: p.user, pass: p.pass, vhost: p.vhost || '/' };
  showConnTest('', `${bunny('run', 26)}<span>Probando ${esc(hostOf(c))}…</span>`);
  try {
    const r = await busy($('connTestBtn'), () => call('ping', { conn: c }));
    const head = `<b>${esc(r.user)}</b> · vhost ${esc(c.vhost)} · ${fmt(r.queues)} colas`;
    if (r.amqp_error) {
      showConnTest('warn', `<span>⚠ ${head}<br>Management OK, pero AMQP falló: ${esc(r.amqp_error)}</span>`);
      lastTest = { key: profileKey(p), ok: false };
    } else {
      showConnTest('ok', `<span>✓ ${head} · AMQP OK</span>`);
      lastTest = { key: profileKey(p), ok: true };
    }
  } catch (err) {
    showConnTest('bad', `<span>✕ ${esc(err)}</span>`);
    lastTest = { key: profileKey(p), ok: false };
  }
};

$('connDialog').addEventListener('close', () => {
  if ($('connDialog').returnValue !== 'save') return;
  const p = formProfile({ announce: true });
  if (!p.name) return;
  // Si se probó con éxito exactamente esto, queda conectado al guardar.
  const tested = lastTest?.ok && lastTest.key === profileKey(p);
  // La contraseña va al almacén del sistema; el perfil en localStorage queda sin ella.
  const { pass, ...stored } = p;
  saveSecret(p.name, pass);
  const i = profiles.findIndex((x) => x.name === p.name);
  if (i >= 0) profiles[i] = stored;
  else profiles.push(stored);
  selectProfile(p.name);
  toast.ok(`Perfil “${p.name}” guardado`);
  if (tested) connect();
});

$('connDelete').onclick = () => {
  const gone = profileName;
  profiles = profiles.filter((p) => p.name !== gone);
  delete secrets[gone];
  call('secret_delete', { profile: gone }, { quiet: true }).catch((err) => log('warn', `secret_delete ${gone}: ${err}`));
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

/**
 * Mientras se buscan colas un águila sobrevuela y el conejo corre en círculos para
 * esquivarla. Si la carga se demora, el texto lo avisa.
 */
const IMPATIENT_MS = 2500;
let impatientTimer = 0;
$('qdigEagle').innerHTML = eagle(56);
function showDig(on) {
  $('qdig').hidden = !on;
  clearTimeout(impatientTimer);
  if (on) {
    $('qempty').hidden = true;
    $('pager').hidden = true;
    $('queues').classList.remove('anim');
    $('queues').replaceChildren();
    const f = $('qfilter').value.trim();
    $('qdigText').textContent = f ? `Buscando colas con “${f}”…` : 'Buscando colas…';
    impatientTimer = setTimeout(() => {
      $('qdigText').textContent = 'Se está tardando… (el broker responde lento)';
    }, IMPATIENT_MS);
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
    const { ready } = await call('queues_ready', { conn: conn(), names }, { quiet: true });
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
    clearPicks('queue');
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
/** `pos`: posición global en la cola (offset de la página + índice). */
function normQueueMsg(m, pos) {
  return { ...m, key: pos, idx: pos + 1 };
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

/**
 * Índice de campos de un item (se arma una vez): nombre de campo → valores (en
 * cualquier nivel del JSON del payload, properties y headers) y ruta → valores.
 * Las rutas se guardan también sin índices de array (`items.sku` = `items.0.sku`).
 */
function fieldIndex(it) {
  if (it._idx) return it._idx;
  const byKey = new Map();
  const byPath = new Map();
  const push = (map, k, v) => {
    const arr = map.get(k);
    if (arr) arr.push(v);
    else map.set(k, [v]);
  };
  const add = (path, key, v) => {
    const sv = v === null ? 'null' : String(v);
    push(byKey, key.toLowerCase(), sv);
    const p = path.toLowerCase();
    push(byPath, p, sv);
    const noIdx = p.replace(/\.\d+(?=\.|$)/g, '');
    if (noIdx !== p) push(byPath, noIdx, sv);
  };
  const walk = (v, path, key, depth) => {
    if (depth > 16) return;
    if (v && typeof v === 'object') {
      const isArr = Array.isArray(v);
      for (const [k, child] of Object.entries(v)) walk(child, path ? `${path}.${k}` : k, isArr ? key : k, depth + 1);
    } else if (key != null) {
      add(path, key, v);
    }
  };
  const t = (it.payload || '').trimStart();
  if (t[0] === '{' || t[0] === '[') {
    try {
      walk(JSON.parse(it.payload), '', null, 0);
    } catch {}
  }
  const { headers, ...props } = it.properties || {};
  walk(props, 'properties', null, 0);
  walk(headers || {}, 'headers', null, 0);
  for (const k of ['routing_key', 'exchange', 'status', 'source', 'target']) if (it[k] != null && it[k] !== '') add(k, k, it[k]);
  it._idx = { byKey, byPath };
  return it._idx;
}

const isNum = (s) => s !== '' && !Number.isNaN(Number(s));

/** campo=valor / campo!=valor / campo=* sobre el índice del item. */
function fieldTest(field, op, value) {
  const f = field.toLowerCase();
  const want = value.toLowerCase();
  const eq = (sv) => want === '*' || sv.toLowerCase() === want || (isNum(sv) && isNum(want) && Number(sv) === Number(want));
  return (it) => {
    const { byKey, byPath } = fieldIndex(it);
    const vals = (f.includes('.') ? byPath.get(f) : byKey.get(f)) || [];
    const hit = vals.some(eq);
    return op === '!=' ? !hit : hit;
  };
}

// Un término del filtro: campo(op)valor | /regex/flags | "texto con espacios" | palabra.
// Cualquiera puede empezar con ! para excluir.
const TERM =
  /(!?)(?:"([^"]+)"|([A-Za-z_$@][\w.\-$@]*))\s*(!=|=|:)\s*(?:"((?:[^"\\]|\\.)*)"|([^\s"]+))|(!?)\/((?:\\.|[^/])+)\/([gimsuy]*)|(!?)"([^"]+)"|(\S+)/g;

/**
 * Filtro local. Términos separados por espacio, todos deben cumplirse:
 *  - texto suelto o "frase": busca en payload, headers, routing key…
 *  - /regex/i
 *  - campo=valor, campo:valor o "campo": valor (como en el JSON), campo!=valor,
 *    campo=* (existe). Sin punto busca el campo en cualquier nivel; con punto es
 *    una ruta (pedido.destino, headers.x-retry, properties.message_id).
 *  - ! adelante de cualquier término lo excluye: !DESTINO=64000000
 */
function buildMatcher(raw) {
  const src = raw.trim();
  if (!src) return null;
  const tests = [];
  for (const m of src.matchAll(TERM)) {
    let test;
    let neg;
    if (m[4]) {
      neg = m[1] === '!';
      const value = (m[5] ?? m[6]).replace(/,$/, '');
      test = fieldTest(m[2] ?? m[3], m[4], value);
    } else if (m[8]) {
      neg = m[7] === '!';
      let re;
      try {
        re = new RegExp(m[8], m[9].replace('g', '') || 'i');
      } catch (err) {
        return { error: err.message };
      }
      test = (it) => re.test(hay(it)._hay);
    } else {
      let text = m[11] ?? m[12];
      neg = (m[10] ?? '') === '!' || (m[12]?.startsWith('!') && m[12].length > 1);
      if (m[12] && neg) text = text.slice(1);
      const low = text.toLowerCase();
      test = (it) => hay(it)._low.includes(low);
    }
    tests.push(neg ? (it) => !test(it) : test);
  }
  return { test: (it) => tests.every((t) => t(it)) };
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
            : `Tocá "Ver (nack)" para leer la primera página (${fmt(pageSize())} mensajes)`;
  $('emptyText').textContent = src.length && !S.view.length ? 'Ningún mensaje coincide con el filtro' : emptyText;
  $('empty').hidden = S.view.length > 0;
  renderDetail();
  renderSelection();
  renderPager();
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

/** Qué significa cada estado del respaldo (tooltip de la fila). */
const STATUS_HELP = {
  pending: 'pending: respaldado, sin publicar ni sacar → sigue en la cola de origen',
  published: 'published: llegó al destino pero no se sacó del origen (se cortó entre ambos pasos) → posible duplicado',
  done: 'done: procesado completo',
  error: 'error: falló al publicar → sigue en la cola de origen',
  republished: 'republished: re-publicado desde este respaldo',
};

function updateRow(row, it, i) {
  row._check.checked = picks().has(it.key);
  row.classList.toggle('sel', i === S.sel);
  row.classList.toggle('picked', row._check.checked);
  row._idx.textContent = it.idx;

  row.title = STATUS_HELP[it.status] ?? '';
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
    row._t2.className = it.sentTo ? 'tag tag--done' : 'tag';
    row._t2.textContent = it.sentTo ? `publicado → ${it.sentTo}` : '';
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

/** Marca o desmarca un item. En la cola guarda su huella: la selección sobrevive al
 *  cambiar de página y "Mover seleccionados" la necesita aunque no esté a la vista. */
function setPick(it, on) {
  const set = picks();
  if (on) {
    set.add(it.key);
    if (S.tab === 'queue') S.qfp.set(it.key, it.fp);
  } else {
    set.delete(it.key);
    if (S.tab === 'queue') S.qfp.delete(it.key);
  }
  if (S.tab === 'queue') S.allQueue = false;
}

function clearPicks(tab = S.tab) {
  S.picks[tab].clear();
  if (tab === 'queue') {
    S.qfp.clear();
    S.allQueue = false;
  }
}

function toggleCheck(i, range) {
  const it = S.view[i];
  const on = !picks().has(it.key);
  const [a, b] = range && S.lastCheck >= 0 ? [Math.min(S.lastCheck, i), Math.max(S.lastCheck, i)] : [i, i];
  for (let k = a; k <= b; k++) setPick(S.view[k], on);
  S.lastCheck = i;
  list.refresh();
  renderSelection();
}

const queueReady = () => S.msgPage?.ready ?? S.currentInfo?.ready ?? 0;

/** Contador de seleccionados y textos de los botones según la pestaña. */
function renderSelection() {
  const set = picks();
  const all = S.tab === 'queue' && S.allQueue;
  const n = all ? queueReady() : set.size;
  const visibleChecked = S.view.length > 0 && S.view.every((it) => set.has(it.key));
  $('selAll').checked = all || visibleChecked;
  $('selAll').indeterminate = !all && !visibleChecked && S.view.some((it) => set.has(it.key));
  $('selAll').disabled = !S.view.length;
  $('selCount').hidden = n === 0;
  $('selCount').textContent = all ? `Toda la cola (${fmt(n)}) ✕` : `${fmt(n)} seleccionados ✕`;
  $('selCount').title = 'Quitar la selección';

  // Como Gmail: con la página entera marcada (y sin filtro), ofrecer toda la cola.
  const offerAll =
    S.tab === 'queue' && !all && visibleChecked && !$('filter').value.trim() && queueReady() > set.size;
  $('selQueue').hidden = !offerAll;
  $('selQueue').textContent = `Seleccionar los ${fmt(queueReady())} de la cola`;

  const busyOp = !!S.op;
  if (S.tab === 'queue') {
    $('ack').textContent = all ? `Ack todos (${fmt(n)})` : n ? `Ack ${fmt(n)} sel.` : 'Ack N';
    $('move').textContent = all ? `Mover todos (${fmt(n)}) →` : n ? `Mover ${fmt(n)} sel. →` : 'Mover N →';
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
  if (!on && S.tab === 'queue') S.allQueue = false;
  for (const it of S.view) setPick(it, on);
  list.refresh();
  renderSelection();
};
$('selCount').onclick = () => {
  clearPicks();
  list.refresh();
  renderSelection();
};
$('selQueue').onclick = () => {
  S.allQueue = true;
  log('info', `seleccionada toda la cola ${S.current} (${queueReady()} ready)`);
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
  for (const id of ['peek', 'ack', 'move', 'countAll']) $(id).disabled = on;
  renderSelection();
  renderPager();
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
/**
 * Ambiente más riesgoso entre las colas tocadas (y el perfil/vhost como señal
 * extra). `confirm` es la cola que hay que escribir en PROD.
 */
function envFor(queues, confirm) {
  const env = worstEnv([...queues, profile()?.name, conn().vhost]);
  return env && { ...env, confirm: confirm ?? queues.find(Boolean) };
}

/**
 * Diálogo de confirmación. Con `env`: PROD en rojo y hay que escribir el nombre
 * de la cola; QA en naranja con aceptar; DEV en azul.
 */
function ask({ title, body, ok = 'Aceptar', danger = false, queue = null, env = null, count = 0, check = null }) {
  return new Promise((resolve) => {
    const d = $('confirmDialog');
    d.classList.remove('modal--prod', 'modal--qa', 'modal--dev');
    $('confirmTitle').textContent = title;
    $('confirmBody').textContent = body;
    $('confirmOk').textContent = ok;
    $('confirmOk').className = `btn ${danger ? 'btn--danger' : 'btn--primary'}`;
    $('confirmQueueWrap').hidden = queue === null;
    if (queue !== null) $('confirmQueue').value = queue;

    // Casilla opcional (p. ej. "incluir los ya publicados"); el que llama lee $('confirmCheck').
    $('confirmCheckWrap').hidden = !check;
    $('confirmCheck').checked = !!check?.checked;
    $('confirmCheckLabel').textContent = check?.label ?? '';

    $('envBanner').hidden = !env;
    const mustType = env?.level === 'prod' ? env.confirm : null;
    $('confirmTypeWrap').hidden = !mustType;
    $('confirmType').value = '';
    $('confirmType').classList.remove('ok');
    $('confirmOk').disabled = !!mustType;
    if (env) {
      const info = ENV_INFO[env.level];
      d.classList.add(`modal--${env.level}`);
      // Escena del conejo rezando en todos los ambientes (el color del banner dice
      // cuál es); el terror sube cada 10k mensajes.
      const level = terrorLevel(count);
      $('envIcon').innerHTML = terrorScene(level);
      $('envTitle').textContent = info.title;
      const fear = level === 5 ? `Que Dios te acompañe con ${fmt(count)} mensajes.` : TERROR_TEXT[level];
      $('envText').innerHTML = `${esc(info.text)} Detectado por “${esc(env.name)}”.${fear ? `<em id="envFear">${esc(fear)}</em>` : ''}`;
      $('confirmTypeName').textContent = mustType ?? '';
      log('info', `confirmación ${env.level} por "${env.name}": ${title}`);
    }
    $('confirmType').oninput = () => {
      const match = $('confirmType').value.trim() === mustType;
      $('confirmOk').disabled = !match;
      $('confirmType').classList.toggle('ok', match);
    };

    d.returnValue = '';
    d.addEventListener(
      'close',
      () => resolve(d.returnValue === 'ok' ? (queue !== null ? $('confirmQueue').value.trim() || false : true) : false),
      { once: true }
    );
    d.showModal();
    (mustType ? $('confirmType') : queue !== null ? $('confirmQueue') : $('confirmOk')).focus();
  });
}

/** Contexto para la metadata del respaldo: perfil y filtro local aplicado. */
const runCtx = () => ({ profile: profile()?.name ?? '', filter: $('filter').value.trim() || null });

// --- acciones de cola ---------------------------------------------------------------
// Sin tope: para ack/mover N puede ser 200k o más (se procesa en lotes).
const count = () => Math.max(1, parseInt($('count').value, 10) || 1);
const pageSize = () => Number($('pageSize').value) || 100;

/**
 * Lee una página (`offset`, tamaño de página) y la devuelve a la cola. Para llegar
 * a una página lejana el backend recorre las anteriores sin guardarlas.
 * `fresh`: lectura nueva desde "Ver" (descarta la selección anterior).
 */
async function peek(offset = 0, { fresh = false } = {}) {
  if (!S.current || S.op) return;
  const queue = S.current;
  const token = ++S.peekToken;
  const limit = pageSize();
  const range = `${fmt(offset + 1)}–${fmt(offset + limit)}`;
  startOp(offset ? `Recorriendo ${fmt(offset)} mensajes para llegar a ${range} de ${queue}…` : `Descargando ${range} de ${queue}…`, offset + limit, { mode: 'run', cancellable: true });
  showSkeleton(true, `Descargando mensajes ${range} de ${queue}…`);
  try {
    const r = await atLeast(call('peek', { conn: conn(), queue, offset, limit }), MIN_ANIM_MS);
    if (token !== S.peekToken || queue !== S.current) return;
    refreshExact([queue]);
    // Todo lo leído vuelve a la cola igual; cancelar solo deja la página incompleta.
    if (r.cancelled) toast.info('Lectura cancelada: los mensajes volvieron a la cola');
    else if (r.stalled) toast.info(`La cola dejó de entregar antes de completar la página (¿otro consumidor activo?). Se muestran ${fmt(r.items.length)}.`);
    S.msgPage = { offset: r.offset, limit, ready: r.ready };
    S.qItems = r.items.map((m, i) => normQueueMsg(m, r.offset + i));
    if (fresh) clearPicks('queue');
    S.sel = S.qItems.length ? 0 : -1;
    if (S.tab === 'queue') applyFilter();
    lastOp(`${fmt(r.items.length)} mensajes leídos de ${queue} (${range} de ${fmt(r.ready)})`);
  } catch (err) {
    toast.err(`Error leyendo ${queue}: ${err}`);
  } finally {
    showSkeleton(false);
    endOp();
  }
}

function renderPager() {
  const p = S.msgPage;
  const show = S.tab === 'queue' && !!p && !!S.qItems.length;
  $('msgPager').hidden = !show;
  if (!show) return;
  $('msgPageInfo').textContent = `${fmt(p.offset + 1)}–${fmt(p.offset + S.qItems.length)} de ${fmt(p.ready)}`;
  $('msgPrev').disabled = p.offset === 0 || !!S.op;
  $('msgNext').disabled = p.offset + p.limit >= p.ready || !!S.op;
}
$('msgPrev').onclick = () => S.msgPage && peek(Math.max(0, S.msgPage.offset - S.msgPage.limit));
$('msgNext').onclick = () => S.msgPage && peek(S.msgPage.offset + S.msgPage.limit);
$('pageSize').value = String(store.get('pageSize', 500));
$('pageSize').onchange = () => store.set('pageSize', pageSize());
$('countAll').onclick = async () => {
  if (!S.current) return toast.err('Elegí una cola');
  try {
    const { ready, errors } = await busy($('countAll'), () => call('queues_ready', { conn: conn(), names: [S.current] }));
    if (!(S.current in ready)) return toast.err(`Cola “${S.current}”: ${errors[S.current] ?? 'no accesible'}`);
    $('count').value = ready[S.current];
    toast.info(`N = ${fmt(ready[S.current])} (todos los ready de ${S.current})`);
  } catch (err) {
    toast.err(`No se pudo leer la cola: ${err}`);
  }
};

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
    const { ready: exact, errors } = await busy(btn, () => call('queues_ready', { conn: conn(), names }));
    // El backend dice el motivo real: no existe, sin permiso, cola exclusiva…
    if (target && !(target in exact)) return toast.err(`Cola destino “${target}”: ${errors[target] ?? 'no accesible'}`), null;
    if (!(source in exact)) return toast.err(`Cola “${source}”: ${errors[source] ?? 'no accesible'}`), null;
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
  if (!S.allQueue && S.picks.queue.size) return drainSelected(target);
  const source = S.current;
  const all = S.allQueue;

  // Cantidad real: lo pedido acotado a lo que hay ready ahora (AMQP, sin el atraso
  // de las estadísticas). Así el diálogo no promete mover 100 si hay 5.
  const ready = await checkQueues(source, target, $(target ? 'move' : 'ack'));
  if (ready == null) return;
  const asked = all ? ready : count();
  const n = Math.min(asked, ready);
  const note = all
    ? `\nToda la cola: ${fmt(ready)} ready ahora.`
    : asked > ready
      ? `\nPediste ${fmt(asked)}, pero la cola tiene ${fmt(ready)} ready.`
      : `\nLa cola tiene ${fmt(ready)} ready.`;
  const qty = all ? `todos los ${fmt(n)}` : fmt(n);

  const yes = await ask(
    target
      ? { title: `Mover ${qty} ${n === 1 ? 'mensaje' : 'mensajes'}`, body: `De “${source}” a “${target}”.${note}\n\nSe guarda un respaldo nuevo de este envío, se publica con confirmación del broker y recién ahí se ackea en origen.`, ok: 'Mover', env: envFor([target, source], target), count: n }
      : { title: `Ack de ${qty} ${n === 1 ? 'mensaje' : 'mensajes'}`, body: `Se sacan de “${source}”.${note}\n\nQuedan en un respaldo nuevo y se pueden volver a cargar desde “Último respaldo”.`, ok: 'Sacar mensajes', danger: true, env: envFor([source]), count: n }
  );
  if (!yes) return;

  startOp(target ? `Moviendo ${source} → ${target}` : `Ackeando ${source}`, n, { cancellable: true, mode: 'run' });
  showSkeleton(true, target ? `Moviendo mensajes a ${target}…` : `Sacando mensajes de ${source}…`);
  try {
    const r = await atLeast(call('drain', { conn: conn(), source, target: target || null, count: n, ctx: runCtx() }), MIN_ANIM_MS);
    const verb = target ? 'movidos' : 'ackeados';
    if (r.total === 0) toast.info(`“${source}” no tiene mensajes ready`);
    else if (r.failed.length) toast.err(`${fmt(r.ok)} ${verb}, falló: ${r.failed[0]}. Los fallidos siguen en origen.`);
    else if (r.cancelled) toast.info(`Cancelado: ${fmt(r.ok)} de ${fmt(r.total)} ${verb}; el resto sigue en la cola`);
    else if (r.stalled) toast.info(`${fmt(r.ok)} de ${fmt(r.total)} ${verb}: la cola dejó de entregar (¿otro consumidor activo?). El resto sigue en la cola.`);
    else {
      toast.ok(`${fmt(r.ok)} mensajes ${verb}${target ? ` a ${target}` : ''}`);
      bunnyDash();
    }
    lastOp(`${fmt(r.ok)} ${verb} de ${source}`);
  } catch (err) {
    toast.err(`Error: ${err}. ${INTERRUPTED_HINT}`);
  } finally {
    showSkeleton(false);
    endOp();
  }
  // Lo leído ya no refleja la cola. Los contadores de origen y destino se toman
  // exactos por AMQP; un refresco completo de la página corregiría con datos viejos.
  S.qItems = [];
  S.msgPage = null;
  clearPicks('queue');
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
  // Seleccionados de cualquier página: posición en la cola → huella.
  const keys = [...S.picks.queue];
  const fps = keys.map((k) => S.qfp.get(k)).filter(Boolean);
  const k = fps.length;
  if (!k) return;
  if ((await checkQueues(source, target, $(target ? 'move' : 'ack'))) == null) return;
  // Se recorre hasta el seleccionado más lejano, más un margen por si se corrieron.
  const scan = keys.reduce((m, x) => Math.max(m, x), 0) + 1 + 50;
  const what = `${fmt(k)} ${k === 1 ? 'mensaje seleccionado' : 'mensajes seleccionados'}`;
  const how = `\n\nSe recorren hasta ${fmt(scan)} mensajes del principio de la cola para encontrarlos; los no seleccionados vuelven a la cola en su orden. Se guarda un respaldo nuevo antes de cualquier ack.`;
  const yes = await ask(
    target
      ? { title: `Mover ${what}`, body: `De “${source}” a “${target}”.${how}`, ok: 'Mover', env: envFor([target, source], target), count: k }
      : { title: `Ack de ${what}`, body: `Se sacan de “${source}”.${how}`, ok: 'Sacar mensajes', danger: true, env: envFor([source]), count: k }
  );
  if (!yes) return;

  startOp(target ? `Moviendo ${what} → ${target}` : `Ackeando ${what}`, k, { mode: 'run', cancellable: true });
  showSkeleton(true, target ? `Moviendo seleccionados a ${target}…` : `Sacando seleccionados de ${source}…`);
  try {
    const r = await atLeast(
      call('move_selected', { conn: conn(), source, target: target || null, fps, scan, ctx: runCtx() }),
      MIN_ANIM_MS
    );
    const verb = target ? 'movidos' : 'ackeados';
    if (r.failed.length) toast.err(`${fmt(r.ok)} ${verb}, falló: ${r.failed[0]}. Los fallidos siguen en origen.`);
    else if (r.cancelled) toast.info(`Cancelado: ${fmt(r.ok)} de ${fmt(r.total)} seleccionados ${verb}; el resto sigue en la cola`);
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
    // Las posiciones de toda la cola se corrieron: se renumera esta página.
    const base = S.msgPage?.offset ?? 0;
    S.qItems = remaining.map((it, i) => ({ ...it, key: base + i, idx: base + i + 1 }));
    if (S.msgPage) S.msgPage.ready = Math.max(0, S.msgPage.ready - r.ok);
    clearPicks('queue');
  } catch (err) {
    toast.err(`Error: ${err}. ${INTERRUPTED_HINT}`);
  } finally {
    showSkeleton(false);
    endOp();
  }
  applyFilter({ keepScroll: true });
  refreshExact([source, target]);
  showLatestBackup();
}

/** Qué hacer tras una interrupción: el respaldo dice dónde quedó cada mensaje. */
const INTERRUPTED_HINT =
  'Revisá “Último respaldo”: “pending” = sigue en origen; “published” = llegó al destino pero no se sacó del origen (posible duplicado).';

$('peek').onclick = () => peek(0, { fresh: true });
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
  if (!chosen.length) return;
  // Los ya publicados se excluyen salvo que se pida lo contrario (evita duplicar
  // tras un corte o un reintento).
  const already = chosen.filter((it) => it.sentTo).length;
  const k = chosen.length - already;
  const yes = await ask({
    title: `Publicar ${fmt(already && !k ? chosen.length : k)} ${k === 1 ? 'mensaje' : 'mensajes'} en “${queue}”`,
    body: `Desde ${S.fileName}${S.picks.file.size ? ' (seleccionados)' : $('filter').value.trim() ? ` (solo los que pasan el filtro: ${$('filter').value.trim()})` : ''}. Lo enviado queda en un respaldo nuevo con solo esos mensajes.\n\nPersistentes y con content_type application/json, salvo que el elemento traiga sus propias properties. Con confirmación del broker.`,
    ok: 'Publicar',
    env: envFor([queue]),
    count: k || chosen.length,
    check: already ? { label: `Incluir ${fmt(already)} que ya se publicaron (duplicarían)`, checked: !k } : null,
  });
  if (!yes) return;
  const send = already && $('confirmCheck').checked ? chosen : chosen.filter((it) => !it.sentTo);
  if (!send.length) return;
  startOp(`Publicando en ${queue}`, send.length, { mode: 'hop', cancellable: true });
  try {
    const r = await atLeast(
      call('publish_messages', { conn: conn(), queue, file: S.fileName, ctx: runCtx(), messages: send.map((it) => ({ payload: it.payload, properties: it.properties })) }),
      MIN_ANIM_MS
    );
    for (const i of r.published) send[i].sentTo = queue;
    if (r.failed.length) toast.err(`${fmt(r.ok)} de ${fmt(r.total)} publicados; falló: ${r.failed[0]}. Los publicados quedan marcados y se excluyen al reintentar.`);
    else if (r.cancelled) toast.info(`Cancelado: ${fmt(r.ok)} de ${fmt(r.total)} publicados (quedan marcados)`);
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
  // Lo publicado desde el archivo quedó en un respaldo nuevo.
  showLatestBackup();
};

// --- respaldos (uno por envío, versionados) -------------------------------------------
const OP_LABEL = { move: 'mover', ack: 'ack', publish: 'publicar', republish: 're-publicar' };
const OUTCOME_LABEL = { ok: 'completo', partial: 'parcial', cancelled: 'cancelado', stalled: 'detenido', error: 'error' };
const unresolvedOf = (b) => (b.pending || 0) + (b.published || 0);

const bkpLabel = (b) =>
  `${relTime(b.ts)} · ${OP_LABEL[b.op] ?? b.op} · ${b.source}${b.target ? ` → ${b.target}` : ''} · ${fmt(b.count)} msgs` +
  (unresolvedOf(b) ? ` · ⚠ ${fmt(unresolvedOf(b))} sin resolver` : '');

/** "file:x.json" → "archivo x.json"; "backup:<id>" → "respaldo <fecha>"; "queue:q" → "cola q". */
function originLabel(origin) {
  if (!origin) return '';
  const [kind, ...rest] = origin.split(':');
  const value = rest.join(':');
  if (kind === 'file') return `archivo ${value}`;
  if (kind === 'queue') return `cola ${value}`;
  if (kind === 'backup') {
    const p = S.backups?.find((b) => b.id === value);
    return `respaldo ${p ? relTime(p.ts) : value}`;
  }
  return origin;
}

const duration = (b) => {
  if (!b.finished_ts) return '';
  const s = Math.max(0, Math.round((b.finished_ts - b.ts) / 1000));
  return s < 60 ? `${s} s` : `${Math.floor(s / 60)} min ${s % 60} s`;
};

/** Lista de respaldos (solo resúmenes); el badge muestra el tamaño del último. */
async function loadBackups() {
  const runs = await call('list_backups', undefined, { quiet: true });
  S.backups = runs;
  if (!runs.some((b) => b.id === S.backupId)) S.backupId = runs[0]?.id ?? null;
  $('bkpBrowse').textContent = runs.length ? `Respaldos guardados (${fmt(runs.length)})…` : 'Sin respaldos todavía';
  $('bkpBrowse').disabled = !runs.length;
  $('localCount').textContent = runs[0]?.count ? fmt(runs[0].count) : '';
  if ($('bkpDialog').open) renderBkpList();
}

async function loadLocal() {
  showSkeleton(!S.localItems.length, 'Leyendo respaldo…');
  try {
    await loadBackups();
    const r = S.backupId ? await call('load_backup', { id: S.backupId }) : { entries: [], meta: null };
    S.backupMeta = r.meta;
    S.localItems = r.entries.map(normLocal);
    const keys = new Set(S.localItems.map((it) => it.key));
    for (const k of S.picks.local) if (!keys.has(k)) S.picks.local.delete(k);
    renderBkpMeta();
    if (S.tab === 'local') applyFilter({ keepScroll: true });
  } catch (err) {
    toast.err(`No se pudo leer el respaldo: ${err}`);
  } finally {
    showSkeleton(false);
  }
}

function openBackup(id) {
  S.backupId = id;
  S.picks.local.clear();
  S.localItems = [];
  if (S.tab !== 'local') setTab('local');
  else loadLocal();
}

/** Metadata del respaldo abierto: qué fue, cuándo, dónde, cómo terminó y su linaje. */
function renderBkpMeta() {
  const box = $('bkpMeta');
  const m = S.backupMeta;
  box.hidden = S.tab !== 'local' || !m;
  if (!m) return;
  const chip = (text, cls = '') => {
    const c = document.createElement('span');
    c.className = `chip ${cls}`;
    c.textContent = text;
    return c;
  };
  const text = (label, value) => {
    const s = document.createElement('span');
    s.append(`${label} `);
    const b = document.createElement('b');
    b.textContent = value;
    s.appendChild(b);
    return s;
  };
  const sep = () => Object.assign(document.createElement('span'), { className: 'sep' });
  const link = (label, onClick) => {
    const a = document.createElement('button');
    a.type = 'button';
    a.className = 'link';
    a.textContent = label;
    a.onclick = onClick;
    return a;
  };

  const parts = [chip(OP_LABEL[m.op] ?? m.op, 'chip--op')];
  const outcome = m.outcome ?? 'running';
  parts.push(chip(m.outcome ? OUTCOME_LABEL[m.outcome] ?? m.outcome : 'no terminó (se cortó)', `chip--${outcome}`));
  parts.push(text('', `${m.source}${m.target ? ` → ${m.target}` : ''}`));
  parts.push(sep(), text('', relTime(m.ts)));
  if (duration(m)) parts.push(text('duró', duration(m)));
  parts.push(sep(), text('', `${fmt(m.count)} msgs · ${fmtSize(m.bytes || 0)}`));
  for (const st of ['done', 'published', 'pending', 'error', 'republished']) if (m[st]) parts.push(chip(`${st} ${fmt(m[st])}`, `chip--${st}`));
  if (m.profile || m.host) {
    parts.push(sep(), text('perfil', `${m.profile || '—'}`));
    parts.push(text('', `${m.user ? m.user + '@' : ''}${(m.host || '').replace(/^https?:\/\//, '')} · vhost ${m.vhost || '/'}`));
  }
  if (m.origin) parts.push(sep(), text('origen', originLabel(m.origin)));
  if (m.filter) parts.push(text('filtro', `“${m.filter}”`));
  if (m.parent) {
    const p = S.backups?.find((b) => b.id === m.parent);
    parts.push(sep(), document.createTextNode('versión de '), p ? link(bkpLabel(p), () => openBackup(p.id)) : text('', `${m.parent} (ya no está)`));
  }
  const children = (S.backups ?? []).filter((b) => b.parent === m.id);
  if (children.length) {
    parts.push(sep(), document.createTextNode(`${children.length} ${children.length === 1 ? 'versión nueva' : 'versiones nuevas'}: `));
    children.forEach((c, i) => {
      if (i) parts.push(document.createTextNode(', '));
      parts.push(link(relTime(c.ts), () => openBackup(c.id)));
    });
  }
  if (m.end_error) parts.push(sep(), chip(`error: ${m.end_error}`, 'chip--error'));
  if (m.app_version) parts.push(sep(), text('app', m.app_version));
  box.replaceChildren(...parts);
}

/** Explorador: todos los respaldos guardados con su metadata; las versiones debajo de su original. */
function renderBkpList() {
  const q = $('bkpFilter').value.trim().toLowerCase();
  const runs = S.backups ?? [];
  const hay = (b) =>
    [b.op, OP_LABEL[b.op], b.source, b.target, b.profile, b.host, b.vhost, b.user, b.origin, b.filter, OUTCOME_LABEL[b.outcome]]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
  const shown = runs.filter((b) => !q || hay(b).includes(q));
  // Raíces primero (más recientes arriba) y cada versión debajo de su original.
  const ids = new Set(shown.map((b) => b.id));
  const roots = shown.filter((b) => !b.parent || !ids.has(b.parent));
  const ordered = [];
  const walk = (b, depth) => {
    ordered.push([b, depth]);
    shown.filter((c) => c.parent === b.id).forEach((c) => walk(c, depth + 1));
  };
  roots.forEach((b) => walk(b, 0));

  const frag = document.createDocumentFragment();
  for (const [b, depth] of ordered) {
    const item = document.createElement('div');
    item.className = `bkp-item${b.id === S.backupId ? ' current' : ''}${depth ? ' child' : ''}`;
    item.style.marginLeft = depth ? `${depth * 24}px` : '';
    const title = document.createElement('div');
    title.className = 'bkp-item__title';
    const op = document.createElement('span');
    op.className = 'chip chip--op';
    op.textContent = OP_LABEL[b.op] ?? b.op;
    const what = document.createElement('span');
    what.textContent = `${b.source}${b.target ? ` → ${b.target}` : ''}`;
    title.append(op, what);
    if (b.outcome && b.outcome !== 'ok') {
      const o = document.createElement('span');
      o.className = `chip chip--${b.outcome}`;
      o.textContent = OUTCOME_LABEL[b.outcome] ?? b.outcome;
      title.appendChild(o);
    } else if (!b.outcome) {
      const o = document.createElement('span');
      o.className = 'chip chip--running';
      o.textContent = 'no terminó';
      title.appendChild(o);
    }
    if (unresolvedOf(b)) {
      const u = document.createElement('span');
      u.className = 'chip chip--published';
      u.textContent = `⚠ ${fmt(unresolvedOf(b))} sin resolver`;
      title.appendChild(u);
    }
    const right = document.createElement('div');
    right.className = 'bkp-item__right';
    right.textContent = `${relTime(b.ts)}${duration(b) ? ` · ${duration(b)}` : ''}`;
    const sub = document.createElement('div');
    sub.className = 'bkp-item__sub';
    const bits = [
      `${fmt(b.count)} msgs · ${fmtSize(b.bytes || 0)}`,
      b.profile ? `perfil ${b.profile}` : '',
      b.host ? `${(b.host || '').replace(/^https?:\/\//, '')} · vhost ${b.vhost || '/'}` : '',
      b.origin ? `origen: ${originLabel(b.origin)}` : '',
      b.filter ? `filtro “${b.filter}”` : '',
      depth ? 'versión nueva' : '',
    ].filter(Boolean);
    for (const t of bits) sub.appendChild(Object.assign(document.createElement('span'), { textContent: t }));
    item.append(title, right, sub);
    item.onclick = () => {
      $('bkpDialog').close();
      openBackup(b.id);
    };
    frag.appendChild(item);
  }
  if (!ordered.length) {
    const empty = document.createElement('p');
    empty.className = 'help-text help-text--dim';
    empty.textContent = runs.length ? 'Ningún respaldo coincide con la búsqueda.' : 'Todavía no hay respaldos.';
    frag.appendChild(empty);
  }
  $('bkpList').replaceChildren(frag);
}

$('bkpBrowse').onclick = async () => {
  await loadBackups().catch(() => {});
  $('bkpFilter').value = '';
  renderBkpList();
  $('bkpDialog').showModal();
  $('bkpFilter').focus();
};
$('bkpFilter').addEventListener('input', renderBkpList);

/** Tras un envío nuevo, la pestaña vuelve a mostrar el último respaldo. */
function showLatestBackup() {
  S.backupId = null;
  S.picks.local.clear();
  if (S.tab === 'local') loadLocal();
  else loadBackups().catch(() => {});
}

const refsOf = (items) => items.map((it) => ({ batch: it.batch, n: it.n }));

$('republishSel').onclick = async () => {
  // Seleccionados, o todos los visibles del respaldo.
  const chosen = S.picks.local.size ? pickedItems('local') : S.view;
  if (!chosen.length) return;
  // Avisos que evitan duplicar: lo ya re-publicado se excluye por defecto, y
  // "published" (llegó al destino pero no se sacó del origen) puede estar repetido.
  const again = chosen.filter((it) => it.status === 'republished');
  const dupRisk = chosen.filter((it) => it.status === 'published').length;
  const fresh = chosen.length - again.length;
  const notes = [
    again.length ? `${fmt(again.length)} ya fueron re-publicados (a ${[...new Set(again.map((it) => it.republished_to || '?'))].join(', ')}).` : '',
    dupRisk ? `${fmt(dupRisk)} están “published”: llegaron al destino pero no se sacaron del origen; re-publicarlos puede duplicarlos.` : '',
  ].filter(Boolean);
  const queue = await ask({
    title: `Re-publicar ${fmt(fresh || chosen.length)} mensajes`,
    body: `Se publican con sus properties y headers originales, con confirmación del broker. Por defecto, en la cola de donde salieron.${notes.length ? `\n\n⚠ ${notes.join('\n⚠ ')}` : ''}`,
    ok: 'Re-publicar',
    queue: chosen[0].source || S.current || '',
    check: again.length ? { label: `Incluir los ${fmt(again.length)} ya re-publicados`, checked: !fresh } : null,
  });
  if (!queue) return;
  const send = again.length && !$('confirmCheck').checked ? chosen.filter((it) => it.status !== 'republished') : chosen;
  if (!send.length) return;
  // La cola recién se conoce acá: si es de un ambiente sensible, segunda confirmación.
  const env = envFor([queue]);
  if (env) {
    const sure = await ask({
      title: `Cargar ${fmt(send.length)} mensajes en “${queue}”`,
      body: 'Se re-publican desde el respaldo con sus properties y headers originales.',
      ok: 'Re-publicar',
      env,
      count: send.length,
    });
    if (!sure) return;
  }
  startOp(`Re-publicando en ${queue}`, send.length, { mode: 'hop', cancellable: true });
  try {
    const r = await atLeast(call('republish', { conn: conn(), items: refsOf(send), queue, ctx: runCtx() }), MIN_ANIM_MS);
    if (r.cancelled) toast.info(`Cancelado: ${fmt(r.ok)} de ${fmt(r.total)} re-publicados en ${queue}`);
    else {
      toast.ok(`${fmt(r.ok)} mensajes re-publicados en ${queue} · se guardó como versión nueva del respaldo`);
      bunnyDash();
    }
    lastOp(`${fmt(r.ok)} re-publicados en ${queue}`);
    S.picks.local.clear();
  } catch (err) {
    toast.err(`Re-publicación interrumpida: ${err}`);
  } finally {
    endOp();
  }
  // Re-publicar creó una versión nueva del respaldo: se abre esa.
  showLatestBackup();
  refreshExact([queue]);
};

$('deleteSel').onclick = async () => {
  const chosen = pickedItems('local');
  if (!chosen.length) return;
  const unresolved = chosen.filter((it) => it.status === 'pending' || it.status === 'published').length;
  const yes = await ask({
    title: `Borrar ${fmt(chosen.length)} del respaldo`,
    body: unresolved
      ? `Atención: ${fmt(unresolved)} están “pending” o “published” (se cortó la operación): el respaldo puede ser la única copia confiable.\nEsto no se puede deshacer.`
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
  renderBkpMeta();
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
  paths.backup = p;
  $('backupPath').textContent = `Respaldo: ${p}`;
  $('backupPath').onclick = () => navigator.clipboard.writeText(p).then(() => toast.ok('Ruta copiada'));
});
call('log_path', undefined, { quiet: true }).then((p) => {
  paths.log = p;
  $('openLog').title = `Abrir carpeta del log: ${p}`;
});
$('openLog').onclick = () => call('reveal_log', undefined, { quiet: true }).catch((err) => toast.err(`No se pudo abrir el log: ${err}`));

// --- retención y borrado de respaldos --------------------------------------------
$('retention').value = String(store.get('retentionDays', 30));
async function pruneBackups() {
  const days = Number($('retention').value);
  if (!days) return;
  try {
    const r = await call('prune_backups', { days }, { quiet: true });
    if (r.removed) log('info', `retención ${days} días: ${r.removed} respaldos borrados`);
    if (r.kept_unresolved) log('info', `retención: ${r.kept_unresolved} respaldos viejos conservados por tener pending/published`);
  } catch (err) {
    log('warn', `retención: ${err}`);
  }
}
$('retention').onchange = async () => {
  store.set('retentionDays', Number($('retention').value));
  await pruneBackups();
  S.backupId = null;
  if (S.tab === 'local') loadLocal();
  else loadBackups().catch(() => {});
};

$('deleteRun').onclick = async () => {
  const b = S.backups?.find((x) => x.id === S.backupId);
  if (!b) return;
  const unresolved = (b.pending || 0) + (b.published || 0);
  const yes = await ask({
    title: 'Borrar el respaldo completo',
    body: `${bkpLabel(b)}${unresolved ? `\n\n⚠ Tiene ${fmt(unresolved)} mensajes “pending”/“published” (la operación se cortó): puede ser la única copia confiable.` : ''}\n\nEsto no se puede deshacer.`,
    ok: 'Borrar respaldo',
    danger: true,
  });
  if (!yes) return;
  try {
    await call('delete_backup', { id: b.id });
    toast.ok('Respaldo borrado');
    S.backupId = null;
    clearPicks('local');
    S.localItems = [];
    loadLocal();
  } catch (err) {
    toast.err(`No se pudo borrar el respaldo: ${err}`);
  }
};

// --- cierre con una operación en curso ------------------------------------------------
// Cerrar a mitad de camino no pierde mensajes (el broker reencola lo no confirmado),
// pero puede dejar entradas "published" (posibles duplicados). Se pide confirmar.
{
  const win = window.__TAURI__.window?.getCurrentWindow?.();
  let closing = false;
  win?.onCloseRequested(async (ev) => {
    if (!S.op || closing) return;
    ev.preventDefault();
    const yes = await ask({
      title: 'Hay una operación en curso',
      body: `${S.op.label}\n\nSi cerrás ahora se cancela al terminar el lote actual: lo no procesado vuelve a la cola y el respaldo queda con el detalle (“published” = posible duplicado).`,
      ok: 'Cancelar y cerrar',
      danger: true,
    });
    if (!yes) return;
    closing = true;
    log('warn', `cierre con operación en curso: ${S.op?.label}`);
    await call('cancel', undefined, { quiet: true }).catch(() => {});
    // Hasta 10 s para que el lote en curso termine limpio.
    for (let i = 0; i < 50 && S.op; i++) await new Promise((r) => setTimeout(r, 200));
    await win.destroy();
  });
}

// --- ayuda --------------------------------------------------------------------------
const paths = {};
async function openHelp() {
  if ($('helpDialog').open) return;
  await renderHelp($('helpBody'), {
    invoke: (cmd, args) => call(cmd, args, { quiet: true }),
    notify: (msg, kind = 'info') => showToast(msg, kind),
    paths,
  });
  $('helpDialog').showModal();
  $('helpBody').scrollTop = 0;
}
$('help').onclick = openHelp;
document.addEventListener('keydown', (ev) => {
  if (ev.key === 'F1') {
    ev.preventDefault();
    openHelp();
  }
});

// --- el conejo se duerme si no hay actividad ------------------------------------------
// Solo los conejos en reposo (sit): los que saltan/corren/escarban indican trabajo.
// Secuencia: cava su madriguera, se acuesta al lado y se duerme.
{
  const IDLE_MS = 30_000; // inactividad hasta que empieza a cavar
  const DIG_MS = 15_000; // cuánto cava antes de meterse a dormir
  let timer = 0;
  let digTimer = 0;
  let state = 'awake'; // awake | digging | asleep
  let lastArm = 0;
  const resting = () => document.querySelectorAll('.bunny--sit');
  const sleep = () => {
    if (S.op) return arm(); // con una operación en curso nadie duerme
    state = 'digging';
    for (const b of resting()) b.classList.add('bunny--dig');
    digTimer = setTimeout(() => {
      state = 'asleep';
      for (const b of document.querySelectorAll('.bunny--dig.bunny--sit')) {
        b.classList.remove('bunny--dig');
        b.classList.add('bunny--sleep');
      }
      $('brandLogo').title = 'Zzz… (mové el mouse para despertarlo)';
    }, DIG_MS);
  };
  const arm = () => {
    clearTimeout(timer);
    timer = setTimeout(sleep, IDLE_MS);
  };
  const wake = () => {
    if (state !== 'awake') {
      clearTimeout(digTimer);
      const wasAsleep = state === 'asleep';
      state = 'awake';
      for (const b of document.querySelectorAll('.bunny--sit.bunny--dig, .bunny--sit.bunny--sleep')) {
        b.classList.remove('bunny--dig', 'bunny--sleep');
        // Se levanta de un saltito.
        if (wasAsleep) {
          b.classList.add('bunny--wake');
          setTimeout(() => b.classList.remove('bunny--wake'), 600);
        }
      }
      $('brandLogo').title = '';
      arm();
    } else if (Date.now() - lastArm > 1000) {
      // pointermove es muy frecuente: re-armar a lo sumo una vez por segundo
      lastArm = Date.now();
      arm();
    }
  };
  for (const ev of ['pointermove', 'pointerdown', 'keydown', 'wheel']) document.addEventListener(ev, wake, { passive: true, capture: true });
  arm();
}

// --- seguridad: la CSP bloquea lo que no sea de la app; queda registrado ------------
document.addEventListener('securitypolicyviolation', (ev) =>
  log('error', `CSP bloqueó ${ev.violatedDirective}: ${ev.blockedURI || '(inline)'} en ${ev.sourceFile}:${ev.lineNumber}`)
);

renderProfiles();
setConnected(false);
requestAnimationFrame(placeIndicator);
window.addEventListener('resize', placeIndicator);
renderQueues();
applyFilter();
setConn('', `Perfil ${profile()?.name ?? ''} — sin conectar`);
initSecrets().finally(() => log('info', 'UI lista'));
pruneBackups().finally(() => loadBackups().catch(() => {}));
