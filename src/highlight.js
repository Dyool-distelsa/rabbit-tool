/** Por encima de esto se muestra texto plano: colorear payloads enormes traba la UI. */
const HIGHLIGHT_MAX = 300_000;
const TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b/g;
const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;' };
const esc = (s) => s.replace(/[&<>]/g, (c) => ESC[c]);

function pretty(text) {
  const t = text.trimStart();
  if (t[0] !== '{' && t[0] !== '[') return null;
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return null;
  }
}

/** Muestra `text` en `el`: JSON formateado y coloreado si se puede, si no texto plano. */
export function renderJson(el, text) {
  const json = pretty(text ?? '');
  if (json === null || json.length > HIGHLIGHT_MAX) {
    el.textContent = json ?? text ?? '';
    return;
  }
  let out = '';
  let last = 0;
  for (const m of json.matchAll(TOKEN)) {
    out += esc(json.slice(last, m.index));
    if (m[1]) out += m[2] ? `<span class="k">${esc(m[1])}</span>${m[2]}` : `<span class="s">${esc(m[1])}</span>`;
    else if (m[3]) out += `<span class="l">${m[0]}</span>`;
    else out += `<span class="n">${m[0]}</span>`;
    last = m.index + m[0].length;
  }
  el.innerHTML = out + esc(json.slice(last));
}
