/**
 * Lista virtual de alto fijo: solo existen en el DOM las filas visibles (+ un
 * margen), recicladas al hacer scroll. Con 20k mensajes el DOM sigue teniendo
 * ~40 nodos de fila, así que filtrar o scrollear no traba la UI.
 *
 * @param {HTMLElement} scroller  contenedor con overflow
 * @param {HTMLElement} inner     hijo que define el alto total
 * @param {{ rowHeight: number, create: () => HTMLElement, update: (el: HTMLElement, item: any, index: number) => void }} opts
 */
export function createVirtualList(scroller, inner, { rowHeight, create, update }) {
  const OVERSCAN = 8;
  let items = [];
  const pool = [];
  let frame = 0;
  let lastStart = -1;
  let lastEnd = -1;

  function paint(force = false) {
    frame = 0;
    const h = scroller.clientHeight;
    const start = Math.max(0, Math.floor(scroller.scrollTop / rowHeight) - OVERSCAN);
    const end = Math.min(items.length, Math.ceil((scroller.scrollTop + h) / rowHeight) + OVERSCAN);
    if (!force && start === lastStart && end === lastEnd) return;
    lastStart = start;
    lastEnd = end;

    const needed = end - start;
    while (pool.length < needed) {
      const el = create();
      inner.appendChild(el);
      pool.push(el);
    }
    for (let i = 0; i < pool.length; i++) {
      const el = pool[i];
      const idx = start + i;
      if (i < needed) {
        if (el.hidden) el.hidden = false;
        el.style.transform = `translateY(${idx * rowHeight}px)`;
        el.dataset.index = idx;
        update(el, items[idx], idx);
      } else if (!el.hidden) {
        el.hidden = true;
      }
    }
  }

  const schedule = () => {
    if (!frame) frame = requestAnimationFrame(() => paint());
  };
  scroller.addEventListener('scroll', schedule, { passive: true });
  new ResizeObserver(schedule).observe(scroller);

  return {
    setItems(next, { keepScroll = false } = {}) {
      items = next;
      inner.style.height = `${items.length * rowHeight}px`;
      if (!keepScroll) scroller.scrollTop = 0;
      paint(true);
    },
    /** Repinta filas visibles (p. ej. cambió la selección) sin tocar el resto. */
    refresh: () => paint(true),
    scrollToIndex(i) {
      const top = i * rowHeight;
      const bottom = top + rowHeight;
      if (top < scroller.scrollTop) scroller.scrollTop = top;
      else if (bottom > scroller.scrollTop + scroller.clientHeight) scroller.scrollTop = bottom - scroller.clientHeight;
    },
    get items() {
      return items;
    },
  };
}
