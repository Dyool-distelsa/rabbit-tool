/**
 * Conejo SVG inline (no <use>, para poder animar cada parte con CSS).
 * Modos: "sit" (parpadea y mueve las orejas), "hop" (salta en loop), "run"
 * (corre: orejas hacia atrás, polvo y líneas de velocidad) y "dig" (escarba un
 * agujero tirando tierra hacia atrás). "sleep" se suma a "sit" cuando la app
 * queda inactiva: ojos cerrados, orejas caídas, respira y suelta Zzz.
 *
 * Las animaciones del conejo ignoran prefers-reduced-motion a propósito: son la
 * señal de "estoy trabajando" de la app (ver styles.css).
 */
/** Partes del conejo (viewBox 0 0 64 64), reutilizadas por la escena del aviso. */
const SHAPES = `
  <g class="b-zzz"><text x="4" y="16">z</text><text x="0" y="9">z</text><text x="-5" y="3">Z</text></g>
  <ellipse class="b-shadow" cx="34" cy="60" rx="18" ry="2.6"/>
  <path class="b-mound" d="M50 61.5 Q60 49 72 61.5 Z"/>
  <ellipse class="b-hole" cx="11" cy="61" rx="9" ry="2.8"/>
  <g class="b-lines"><path d="M61 30h9M63 38h8M60 46h7"/></g>
  <g class="b-dust"><circle cx="58" cy="58" r="2.6"/><circle cx="63" cy="55.5" r="1.8"/><circle cx="67" cy="58.5" r="1.3"/></g>
  <g class="b-dirt"><circle cx="14" cy="58" r="2.2"/><circle cx="16" cy="58" r="1.6"/><circle cx="13" cy="59" r="1.9"/><circle cx="17" cy="57" r="1.3"/><circle cx="15" cy="59" r="1.5"/></g>
  <g class="b-all">
    <g class="b-ear-b"><ellipse cx="28" cy="13" rx="3.8" ry="11.5" transform="rotate(16 28 22)"/><ellipse class="b-inner" cx="28" cy="13.5" rx="1.6" ry="8" transform="rotate(16 28 22)"/></g>
    <ellipse cx="37" cy="43" rx="17" ry="13.5"/>
    <circle class="b-tail" cx="53.5" cy="39" r="5"/>
    <ellipse class="b-foot" cx="41" cy="55" rx="10" ry="3.6"/>
    <circle cx="22" cy="31" r="10.5"/>
    <g class="b-ear-f"><ellipse cx="20" cy="12" rx="4.2" ry="12" transform="rotate(-8 20 22)"/><ellipse class="b-inner" cx="20" cy="12.5" rx="1.8" ry="8.5" transform="rotate(-8 20 22)"/></g>
    <ellipse class="b-paw" cx="22" cy="54.6" rx="5.6" ry="2.9"/>
    <ellipse class="b-eye" cx="18" cy="29" rx="1.9" ry="2.2"/>
    <circle class="b-nose" cx="12" cy="32.8" r="1.4"/>
  </g>
  <path class="b-rim" d="M-12 68 Q-9 53.8 12 54.4 Q34 53.8 42 68 Z"/>`;
const SVG = `<svg class="bunny__svg" viewBox="0 0 64 64" aria-hidden="true">${SHAPES}</svg>`;

export function bunny(mode = 'sit', size = 32) {
  return `<span class="bunny bunny--${mode}" style="--bunny-size:${size}px">${SVG}</span>`;
}

/** Rellena todos los `[data-bunny="sit|hop|run|dig"]` (con `data-size` opcional). */
export function mountBunnies(root = document) {
  for (const el of root.querySelectorAll('[data-bunny]')) {
    el.innerHTML = bunny(el.dataset.bunny, Number(el.dataset.size) || 32);
  }
}

/** Cambia el modo de un conejo ya montado dentro de `el`. */
export function setBunnyMode(el, mode) {
  const b = el.querySelector('.bunny');
  if (b) b.className = `bunny bunny--${mode}`;
}

/** Nivel de terror según la cantidad de mensajes: uno más cada 10k, hasta 5. */
export const terrorLevel = (n) => Math.max(0, Math.min(5, Math.floor((n || 0) / 10_000)));

export const TERROR_TEXT = [
  '',
  'Ya es de noche…',
  'Se escuchan truenos a lo lejos.',
  'Algo se mueve entre los relámpagos.',
  'Te está mirando.',
  'Que Dios te acompañe.',
];

// Posiciones fijas (pseudoaleatorias) para que la escena no "salte" entre renders.
const rnd = (seed) => () => ((seed = (seed * 16807) % 2147483647) - 1) / 2147483646;

/**
 * Escena del aviso de PROD/QA (viewBox 240×130): el conejo reza sosteniendo un
 * crucifijo, con una veladora y una biblia. El terror sube con `level` (0–5):
 * noche → truenos → lluvia y demonio en los relámpagos → demonio fijo, temblor y
 * sacudón → todo más cerca y en rojo. Los relámpagos marcan el ritmo (--period).
 */
export function terrorScene(level = 0) {
  const L = level;
  const period = [0, 0, 7, 5, 3.5, 2.2][L];
  const r = rnd(7);
  const out = [];

  out.push(`<defs>
    <linearGradient id="t-sky" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#03040f"/><stop offset="1" stop-color="#1b1c3a"/></linearGradient>
    <radialGradient id="t-halo"><stop offset="0" stop-color="#fff6d6" stop-opacity=".85"/><stop offset=".55" stop-color="#ffe9a8" stop-opacity=".3"/><stop offset="1" stop-color="#ffe9a8" stop-opacity="0"/></radialGradient>
    <radialGradient id="t-aura"><stop offset="0" stop-color="#ff2a1a" stop-opacity=".75"/><stop offset=".45" stop-color="#b30f0f" stop-opacity=".4"/><stop offset="1" stop-color="#5a0000" stop-opacity="0"/></radialGradient>
    <radialGradient id="t-demon-body" cx=".5" cy=".35" r=".7"><stop offset="0" stop-color="#4a0707"/><stop offset=".55" stop-color="#1c0202"/><stop offset="1" stop-color="#050000"/></radialGradient>
    <radialGradient id="t-candle"><stop offset="0" stop-color="#ffcf5a" stop-opacity=".55"/><stop offset="1" stop-color="#ffcf5a" stop-opacity="0"/></radialGradient>
  </defs>`);

  out.push('<g class="t-world">');
  if (L >= 1) {
    out.push('<rect class="t-sky" width="240" height="130" fill="url(#t-sky)"/>');
    for (let i = 0; i < 18; i++) {
      out.push(`<circle class="t-star" cx="${(r() * 240).toFixed(1)}" cy="${(r() * 70).toFixed(1)}" r="${(0.4 + r() * 0.8).toFixed(2)}" style="animation-delay:${(r() * 3).toFixed(2)}s"/>`);
    }
    out.push(`<g class="t-moon${L >= 4 ? ' t-moon--blood' : ''}"><circle cx="212" cy="24" r="10"/><circle class="t-crater" cx="208" cy="21" r="2"/><circle class="t-crater" cx="215" cy="28" r="1.4"/></g>`);
  }
  // El destello va detrás de nubes y demonio: la silueta se recorta negra.
  if (L >= 2) out.push('<rect class="t-flash" width="240" height="130"/>');
  if (L >= 2) {
    out.push(`<g class="t-clouds">
      <ellipse cx="60" cy="10" rx="40" ry="9"/><ellipse cx="95" cy="6" rx="30" ry="8"/>
      <ellipse cx="170" cy="9" rx="38" ry="9"/><ellipse cx="205" cy="5" rx="28" ry="7"/></g>`);
  }
  if (L >= 3) {
    // Silueta: hombros, cabeza, cuernos y ojos rojos, detrás del conejo.
    out.push(`<g class="t-demon">
      <ellipse class="t-aura" cx="202" cy="78" rx="66" ry="60" fill="url(#t-aura)"/>
      <path class="t-demon__body" fill="url(#t-demon-body)" d="M150 130 C152 101 168 89 182 85 C176 77 175 67 178 59 L168 30 L187 54 C191 50 196 49 202 50 C208 49 213 50 217 54 L236 30 L226 59 C229 67 228 77 222 85 C236 89 252 101 254 130 Z"/>
      <g class="t-eyes"><path d="M185 64.5 l12 2.5 l-11 2.6 z"/><path d="M219 64.5 l-12 2.5 l11 2.6 z"/></g></g>`);
  }
  if (L >= 2) {
    out.push('<path class="t-bolt" d="M196 14 l-9 24 h8 l-12 28 l21 -33 h-8 l9 -19 z"/>');
  }
  if (L >= 3) {
    out.push('<g class="t-rain">');
    for (let i = 0; i < 26; i++) {
      out.push(`<path d="M${(r() * 260).toFixed(1)} 0 l-3 9" style="animation-delay:${(r() * 0.6).toFixed(2)}s;animation-duration:${(0.45 + r() * 0.3).toFixed(2)}s"/>`);
    }
    out.push('</g>');
  }
  if (L >= 1) out.push('<rect class="t-ground" y="116" width="240" height="14"/>');

  // Veladora.
  out.push(`<g class="t-candle">
    <circle class="pray__glow" cx="50" cy="80" r="22" fill="url(#t-candle)"/>
    <path class="pray__flame" d="M50 70 C54 76 55 80 55 82.5 a5 5 0 0 1 -10 0 C45 80 46 76 50 70 Z"/>
    <path class="pray__flame-core" d="M50 76 C52 79 52.4 80.6 52.4 81.8 a2.4 2.4 0 0 1 -4.8 0 C47.6 80.6 48 79 50 76 Z"/>
    <rect class="pray__wick" x="49.4" y="85" width="1.2" height="3.5" rx=".6"/>
    <rect class="pray__wax" x="43.5" y="88" width="13" height="26" rx="2"/>
    <path class="pray__drip" d="M46 88 v5 a1.3 1.3 0 0 0 2.6 0 v-5 Z"/>
    <rect class="pray__holder" x="39" y="113" width="22" height="4.5" rx="2.2"/></g>`);

  // Biblia abierta en el piso.
  out.push(`<g class="t-bible">
    <path class="t-bible__cover" d="M66 119 L86 114.5 L106 119 L106 122 L86 118 L66 122 Z"/>
    <path class="t-bible__page" d="M67 118 L86 109 L86 116.5 L67 120.5 Z"/>
    <path class="t-bible__page t-bible__page--r" d="M105 118 L86 109 L86 116.5 L105 120.5 Z"/>
    ${L >= 3 ? '<path class="t-bible__flip" d="M86 109 L101 111 L101 118.5 L86 116.5 Z"/>' : ''}
    <path class="t-bible__mark" d="M75.5 112.6 v4.4 M73.6 114 h3.8"/>
    <path class="t-bible__ribbon" d="M86 116.5 l1.4 6"/></g>`);

  // Conejo + crucifijo sostenido (brazo levantado y pata cerrada sobre el madero).
  out.push(`<g class="t-holder">
    <circle class="t-halo" cx="150" cy="84" r="38" fill="url(#t-halo)"/>
    <g class="bunny bunny--sit"><svg x="112" y="38" width="80" height="80" viewBox="0 0 64 64" overflow="visible" fill="currentColor">${SHAPES}</svg></g>
    <g class="t-cross"><rect x="113.5" y="58" width="5" height="42" rx="1"/><rect x="105" y="68" width="22" height="5" rx="1"/></g>
    <g class="t-arm">
      <ellipse cx="128" cy="95" rx="12" ry="4.6" transform="rotate(-24 128 95)"/>
      <ellipse cx="116" cy="90.5" rx="5.2" ry="4.2"/>
    </g></g>`);

  if (L >= 5) out.push('<rect class="t-blood" width="240" height="130"/>');
  out.push('</g>');

  return `<svg class="stage pray lvl-${L}" viewBox="0 0 240 130" style="--period:${period || 6}s" aria-hidden="true">${out.join('')}</svg>`;
}

/**
 * Águila que sobrevuela (mientras se cargan colas): planea de lado a lado,
 * aletea y es la razón por la que el conejo corre en círculos.
 */
export function eagle(size = 64) {
  return `<span class="eagle" style="--eagle-size:${size}px"><svg class="eagle__svg" viewBox="0 0 64 32" aria-hidden="true">
    <path class="eagle__wing eagle__wing--back" d="M31 16 Q25 3 9 2 Q15 9 18 12 Q22 15 27 17 Z"/>
    <path class="eagle__tail" d="M22 17.5 L14 15 L15.5 18.5 L14 22 L22 19.5 Z"/>
    <ellipse class="eagle__body" cx="31" cy="18" rx="10" ry="4.2"/>
    <circle class="eagle__head" cx="42.5" cy="15.5" r="3.6"/>
    <path class="eagle__beak" d="M45.6 14.6 L49.5 15.8 L45.8 17.4 Z"/>
    <circle class="eagle__eye" cx="43.6" cy="14.8" r="0.7"/>
    <path class="eagle__wing eagle__wing--front" d="M33 16 Q41 1 59 1 Q50 8 46 11 Q41 15 37 17.5 Z"/>
    <path class="eagle__talon" d="M30 21.5 l-1.2 3 M33 21.5 l1 3"/>
  </svg></span>`;
}

/** Un conejo cruza la barra de estado de punta a punta (festejo breve). */
export function bunnyDash() {
  const el = document.createElement('div');
  el.className = 'bunny-dash';
  el.innerHTML = bunny('run', 22);
  document.getElementById('app').appendChild(el);
  el.addEventListener('animationend', () => el.remove(), { once: true });
}
