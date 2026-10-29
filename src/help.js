/**
 * Ayuda (como la de json-scout): acerca de, cómo funciona, estados del
 * respaldo, ambientes, atajos, archivos locales, repositorio y créditos.
 * Se arma con nodos y textContent: nada de HTML con datos de afuera.
 */
const AUTHOR = 'Dylan Yool';

const SHORTCUTS = [
  ['Ayuda', 'F1'],
  ['Buscar en los mensajes (filtro local)', 'Ctrl+F'],
  ['Limpiar el filtro', 'Esc'],
  ['Leer mensajes (Ver nack) / recargar respaldo', 'Ctrl+R'],
  ['Refrescar la página de colas', 'F5'],
  ['Mensaje anterior / siguiente', '↑ ↓  ·  k j'],
  ['Marcar / desmarcar el mensaje', 'Espacio'],
  ['Marcar un rango', 'Shift+click'],
  ['Marcar uno suelto', 'Ctrl+click'],
  ['Marcar todos los visibles', 'Ctrl+A'],
];

const STATES = [
  ['pending', 'Respaldado, todavía sin publicar ni sacar del origen. Si quedó así tras un corte: en mover/ack sigue en la cola de origen; en publicar/re-publicar no se sabe si llegó.'],
  ['published', 'Llegó al destino pero no se sacó del origen (se cortó entre los dos pasos): posible duplicado.'],
  ['done', 'Procesado completo.'],
  ['error', 'Falló al publicar: sigue en la cola de origen.'],
  ['republished', 'Se volvió a enviar desde este respaldo (eso generó una versión nueva).'],
];

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text != null) node.textContent = text;
  return node;
}

function section(title) {
  const node = el('section', 'help-section');
  node.appendChild(el('h3', 'help-section__title', title));
  return node;
}

function facts(rows, cls = '') {
  const dl = el('dl', `help-facts ${cls}`);
  for (const [term, value] of rows) dl.append(el('dt', null, term), el('dd', 'help-mono', value));
  return dl;
}

/**
 * @param {HTMLElement} container
 * @param {{ invoke: Function, notify: (msg: string, kind?: string) => void, paths: { backup?: string, log?: string } }} deps
 */
export async function renderHelp(container, { invoke, notify, paths }) {
  container.replaceChildren();
  const root = el('div', 'help-panel');

  let info = { version: 'dev', commit: '', date: '', repo: '' };
  try {
    info = await invoke('app_info');
  } catch {}
  const repo = info.repo || 'https://github.com/Dyool-distelsa/rabbit-tool';

  const link = (label, url) => {
    const a = el('a', 'help-link', label);
    a.href = url;
    a.addEventListener('click', (ev) => {
      ev.preventDefault();
      invoke('open_url', { url }).catch(() => notify('No se pudo abrir el navegador', 'error'));
    });
    return a;
  };
  const copyBtn = (text, what) => {
    const b = el('button', 'btn btn--sm', 'Copiar');
    b.type = 'button';
    b.onclick = () =>
      navigator.clipboard.writeText(text).then(
        () => notify(`${what} copiado`, 'success'),
        () => notify('No se pudo copiar', 'error')
      );
    return b;
  };
  const code = (text) => el('code', 'help-code', text);
  const steps = (items) => {
    const ol = el('ol', 'help-steps');
    for (const parts of items) {
      const li = el('li');
      li.append(...parts.map((p) => (typeof p === 'string' ? p : p)));
      ol.appendChild(li);
    }
    return ol;
  };

  // Acerca de
  const about = section('Rabbit Tool');
  about.appendChild(
    el('p', 'help-text', 'Herramienta de escritorio para ver, ackear, mover y volver a cargar mensajes de RabbitMQ sin perderlos.')
  );
  const rows = [['Versión', info.version || 'dev']];
  if (info.commit) rows.push(['Commit', info.commit]);
  if (info.date) rows.push(['Compilado', new Date(info.date).toLocaleString('es')]);
  about.appendChild(facts(rows));

  // Cómo se usa
  const how = section('Cómo se usa');
  how.appendChild(
    steps([
      ['Elegí un perfil o creá uno nuevo con ', code('+'), ' (el lápiz edita, renombra, duplica o borra el elegido), probá la conexión y ', code('Conectar'), '.'],
      ['Las colas no se cargan solas: filtrá por nombre o ', code('/regex/'), ' y tocá ', code('Cargar'), ' (de a 5–20, con mensajes primero o A–Z).'],
      [code('Ver (nack)'), ' lee una página de mensajes y los devuelve a la cola. El filtro local busca en payload, headers y routing key.'],
      ['Marcá mensajes (o “toda la cola”) y usá ', code('Ack'), ' o ', code('Mover →'), '. Sin selección actúan sobre los primeros N.'],
      [code('Descargar JSON…'), ' guarda N mensajes (o toda la cola) directo en un .json, sin ack: vuelven a la cola. ', code('Exportar JSON…'), ' (junto al filtro) guarda los seleccionados o filtrados de cualquier pestaña.'],
      ['Pestaña ', code('Archivo JSON'), ': cargá un array de mensajes (por ejemplo uno descargado y editado), filtrá y publicá en una cola.'],
      ['Pestaña ', code('Respaldos'), ': cada envío queda guardado; podés abrir cualquiera y volver a cargar sus mensajes.'],
      [code('Actividad'), ' (abajo a la izquierda): las últimas 200 operaciones con hora, la más reciente arriba; se guardan en disco y siguen ahí al volver a abrir la app. El borde marca el ambiente y tocar una abre su respaldo. El historial completo queda en el log.'],
      ['El ambiente del perfil se ve siempre: el conejo usa gorra con hélice en DEV, lentes y lupa en QA y sirena en PROD (y se tapa los ojos si pasás por Ack o Mover). También lo marcan la etiqueta junto al perfil, la franja de arriba, el color del scroll y el título de la ventana. Se elige en el lápiz (', code('Ambiente'), '); en Auto se deduce del nombre, vhost o host.'],
      ['Los paneles se ajustan arrastrando sus bordes (ancho de la barra izquierda, alto de la actividad, ancho del detalle) o con las flechas; doble clic vuelve al tamaño original. Se recuerdan entre sesiones.'],
    ])
  );

  // Respaldo
  const safety = section('Cómo no se pierden mensajes');
  safety.appendChild(
    el('p', 'help-text', 'Antes de cualquier ack, cada lote se guarda en disco. Al mover se publica con confirmación del broker y recién entonces se saca del origen. Si algo falla, lo no confirmado vuelve a la cola.')
  );
  safety.appendChild(
    el('p', 'help-text', 'Cada envío (mover, ack, publicar desde archivo o re-publicar) crea su propio respaldo. Re-publicar desde un respaldo crea una versión nueva que apunta a la original.')
  );
  safety.appendChild(facts(STATES, 'help-facts--states'));

  // Ambientes
  const envs = section('Confirmación por ambiente');
  envs.appendChild(
    facts([
      ['PROD', 'Rojo. Hay que escribir el nombre de la cola. Palabras: prod, prd, production.'],
      ['QA', 'Naranja. Palabras: qa, staging, stg, uat, test, preprod.'],
      ['DEV', 'Celeste. Palabras: dev, develop, desarrollo, local.'],
    ])
  );
  envs.appendChild(el('p', 'help-text help-text--dim', 'Se mira la cola (y el perfil/vhost). La escena del conejo se pone más oscura cada 10.000 mensajes.'));

  // Filtro
  const filt = section('Filtro de mensajes');
  filt.appendChild(el('p', 'help-text', 'Sirve en la cola, el archivo JSON y los respaldos. Varios términos separados por espacio: tienen que cumplirse todos.'));
  filt.appendChild(
    facts([
      ['texto  o  "una frase"', 'Busca en payload, headers, properties y routing key.'],
      ['/regex/i', 'Expresión regular.'],
      ['DESTINO=64000000', 'El campo con ese valor, en cualquier nivel del JSON (también DESTINO:64000000 o "DESTINO": 64000000 pegado del JSON).'],
      ['pedido.destino=64000000', 'Con punto es una ruta exacta. También headers.x-retry=2 o properties.message_id=abc.'],
      ['DESTINO!=64000000', 'El campo no tiene ese valor.'],
      ['DESTINO=*', 'El campo existe.'],
      ['!término', 'Excluye: !DESTINO=64000000 deja afuera esos mensajes.'],
    ], 'help-facts--states')
  );
  filt.appendChild(el('p', 'help-text help-text--dim', 'Al publicar o re-publicar sin selección se envían los que deja ver el filtro, y el respaldo nuevo guarda solo esos (con el filtro anotado).'));

  // Atajos
  const keys = section('Atajos de teclado');
  keys.appendChild(facts(SHORTCUTS, 'help-facts--keys'));

  // Archivos
  const files = section('Archivos locales');
  const fileRows = [];
  if (paths.backup) fileRows.push(['Respaldos', paths.backup]);
  if (paths.log) fileRows.push(['Log', paths.log]);
  fileRows.push(['Contraseñas', 'Administrador de credenciales de Windows (rabbit-tool)']);
  files.appendChild(facts(fileRows));
  const logRow = el('div', 'help-row');
  const openLog = el('button', 'btn btn--sm', 'Abrir carpeta del log');
  openLog.type = 'button';
  openLog.onclick = () => invoke('reveal_log').catch(() => notify('No se pudo abrir el log', 'error'));
  logRow.appendChild(openLog);
  files.appendChild(logRow);

  // Repositorio
  const repoSec = section('Repositorio');
  const repoRow = el('div', 'help-row');
  repoRow.append(link(repo.replace('https://', ''), repo), copyBtn(repo, 'Enlace'));
  repoSec.appendChild(repoRow);
  const issues = el('p', 'help-text');
  issues.append('¿Encontraste un error o tenés una idea? ', link('Abrí un issue', `${repo}/issues`), '.');
  repoSec.appendChild(issues);

  // Contribuir
  const contrib = section('Contribuir');
  contrib.appendChild(
    steps([
      ['Hacé un fork y una rama como ', code('feat/mi-cambio'), '.'],
      ['Instalá y corré: ', code('npm install'), ' y después ', code('npm run dev'), '.'],
      ['Verificá: ', code('cargo check'), ' en ', code('src-tauri'), ' y probá contra un broker de desarrollo.'],
      ['Commits convencionales, p. ej. ', code('fix(backup): …'), '.'],
      ['Abrí un pull request contra ', code('main'), ' contando qué cambió y cómo lo probaste.'],
    ])
  );

  // Créditos
  const credits = section('Créditos');
  credits.appendChild(el('p', 'help-text', `Creado y mantenido por ${AUTHOR}, dueño del repositorio.`));

  root.append(about, credits, how, filt, safety, envs, keys, files, repoSec, contrib);
  container.appendChild(root);
}
