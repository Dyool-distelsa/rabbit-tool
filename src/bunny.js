/**
 * Conejo SVG inline (no <use>, para poder animar cada parte con CSS).
 * Modos: "sit" (parpadea y mueve las orejas), "hop" (salta en loop) y "run"
 * (corre: orejas hacia atrás, polvo y líneas de velocidad).
 */
const SVG = `<svg class="bunny__svg" viewBox="0 0 64 64" aria-hidden="true">
  <ellipse class="b-shadow" cx="34" cy="60" rx="18" ry="2.6"/>
  <g class="b-lines"><path d="M61 30h9M63 38h8M60 46h7"/></g>
  <g class="b-dust"><circle cx="58" cy="58" r="2.6"/><circle cx="63" cy="55.5" r="1.8"/><circle cx="67" cy="58.5" r="1.3"/></g>
  <g class="b-all">
    <g class="b-ear-b"><ellipse cx="28" cy="13" rx="3.8" ry="11.5" transform="rotate(16 28 22)"/><ellipse class="b-inner" cx="28" cy="13.5" rx="1.6" ry="8" transform="rotate(16 28 22)"/></g>
    <ellipse cx="37" cy="43" rx="17" ry="13.5"/>
    <circle class="b-tail" cx="53.5" cy="39" r="5"/>
    <ellipse cx="41" cy="55" rx="10" ry="3.6"/>
    <circle cx="22" cy="31" r="10.5"/>
    <g class="b-ear-f"><ellipse cx="20" cy="12" rx="4.2" ry="12" transform="rotate(-8 20 22)"/><ellipse class="b-inner" cx="20" cy="12.5" rx="1.8" ry="8.5" transform="rotate(-8 20 22)"/></g>
    <ellipse cx="22" cy="54.6" rx="5.6" ry="2.9"/>
    <ellipse class="b-eye" cx="18" cy="29" rx="1.9" ry="2.2"/>
    <circle class="b-nose" cx="12" cy="32.8" r="1.4"/>
  </g>
</svg>`;

export function bunny(mode = 'sit', size = 32) {
  return `<span class="bunny bunny--${mode}" style="--bunny-size:${size}px">${SVG}</span>`;
}

/** Rellena todos los `[data-bunny="sit|hop"]` (con `data-size` opcional). */
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

/** Un conejo cruza la barra de estado de punta a punta (festejo breve). */
export function bunnyDash() {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const el = document.createElement('div');
  el.className = 'bunny-dash';
  el.innerHTML = bunny('hop', 20);
  document.getElementById('app').appendChild(el);
  el.addEventListener('animationend', () => el.remove(), { once: true });
}
