/**
 * Toast notification system.
 *
 * This module is split in two layers on purpose:
 *  - A pure, DOM-free core (TOAST_DURATIONS, getToastDuration, createToastId,
 *    ToastQueue) that owns queueing/auto-dismiss timing and can be unit
 *    tested in isolation (see toast.test.js) by injecting fake timer
 *    functions.
 *  - DOM wiring (mountToastContainer) that renders the queue's state as
 *    stacked, accessible toast elements with enter/exit transitions. This
 *    part is intentionally not unit tested — it is thin glue over the DOM.
 */

/**
 * Auto-dismiss durations (ms) per variant: success is quick, error lingers.
 * Roughly double the original (2500/3800/5500) durations — the toast is
 * now much more prominent (T1), but it still needs enough time on screen
 * to actually be read, especially for a success message that used to
 * vanish in 2.5s.
 */
export const TOAST_DURATIONS = {
  success: 5000,
  info: 7600,
  error: 11000,
};

/**
 * @param {'success'|'error'|'info'|string} variant
 * @param {typeof TOAST_DURATIONS} [durations]
 * @returns {number} auto-dismiss duration in ms, falling back to the info
 *   duration for an unrecognized variant.
 */
export function getToastDuration(variant, durations = TOAST_DURATIONS) {
  return durations[variant] ?? durations.info;
}

let idCounter = 0;
/** @returns {string} a unique, human-readable toast id. */
export function createToastId() {
  idCounter += 1;
  return `toast-${idCounter}-${Date.now().toString(36)}`;
}

/**
 * Pure queue/timeout core for toast notifications. No DOM access — timer
 * functions are injectable so behavior can be tested deterministically.
 */
export class ToastQueue {
  /**
   * @param {{ setTimeoutFn?: typeof setTimeout, clearTimeoutFn?: typeof clearTimeout, durations?: typeof TOAST_DURATIONS }} [options]
   */
  constructor({ setTimeoutFn, clearTimeoutFn, durations = TOAST_DURATIONS } = {}) {
    // The native browser timers MUST be invoked with `window` as their
    // receiver. Storing a bare `setTimeout` reference and later calling it
    // as `this._setTimeout(...)` re-binds `this` to this instance, which
    // throws "Illegal invocation" in a browser and silently killed every
    // toast. Node has no such requirement, so injected fake timers in the
    // tests never caught it — hence the default-path test in toast.test.js.
    this._setTimeout = setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms));
    this._clearTimeout = clearTimeoutFn ?? ((handle) => clearTimeout(handle));
    this._durations = durations;
    this._toasts = [];
    this._timers = new Map();
    this._listeners = new Set();
  }

  /**
   * @param {string} message
   * @param {'success'|'error'|'info'} [variant]
   * @returns {{ id: string, message: string, variant: string, duration: number }}
   */
  add(message, variant = 'info') {
    const id = createToastId();
    const duration = getToastDuration(variant, this._durations);
    const toast = { id, message, variant, duration };
    const timerHandle = this._setTimeout(() => this.dismiss(id), duration);
    this._timers.set(id, timerHandle);
    this._toasts = [...this._toasts, toast];
    this._notify();
    return toast;
  }

  /**
   * @param {string} id
   * @returns {boolean} whether a toast with that id was removed.
   */
  dismiss(id) {
    if (!this._toasts.some((toast) => toast.id === id)) return false;
    this._toasts = this._toasts.filter((toast) => toast.id !== id);
    const timerHandle = this._timers.get(id);
    if (timerHandle !== undefined) {
      this._clearTimeout(timerHandle);
      this._timers.delete(id);
    }
    this._notify();
    return true;
  }

  /** Dismiss every currently visible toast of the given variant. */
  dismissVariant(variant) {
    this._toasts.filter((toast) => toast.variant === variant).forEach((toast) => this.dismiss(toast.id));
  }

  /** Dismiss every currently visible toast. */
  dismissAll() {
    [...this._toasts].forEach((toast) => this.dismiss(toast.id));
  }

  /** @returns {{ id: string, message: string, variant: string, duration: number }[]} a snapshot of the current toasts. */
  list() {
    return [...this._toasts];
  }

  /**
   * @param {(toasts: ReturnType<ToastQueue['list']>) => void} listener
   * @returns {() => void} unsubscribe function.
   */
  subscribe(listener) {
    this._listeners.add(listener);
    return () => this._listeners.delete(listener);
  }

  _notify() {
    const snapshot = this.list();
    this._listeners.forEach((listener) => listener(snapshot));
  }
}

/**
 * Minimal per-variant glyphs, inline so a toast reads instantly without an
 * icon library or an extra asset request. Purely decorative (the toast's
 * `role`/`aria-live` already carry the semantics), so `aria-hidden`.
 */
const TOAST_ICONS = {
  success:
    '<svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true"><circle cx="10" cy="10" r="9" fill="currentColor" opacity="0.16"/><path d="M6 10.4l2.6 2.6L14.2 7" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  error:
    '<svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true"><circle cx="10" cy="10" r="9" fill="currentColor" opacity="0.16"/><path d="M7 7l6 6M13 7l-6 6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
  info:
    '<svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true"><circle cx="10" cy="10" r="9" fill="currentColor" opacity="0.16"/><path d="M10 9.2v5M10 6.3v.01" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
};

/**
 * Mount a stacked, accessible toast UI inside `containerEl`.
 * @param {HTMLElement} containerEl
 * @param {{ queue?: ToastQueue }} [options]
 * @returns {{
 *   showToast: (message: string, variant?: 'success'|'error'|'info') => { id: string },
 *   dismissToast: (id: string) => void,
 *   dismissToastsByVariant: (variant: string) => void,
 *   dismissAllToasts: () => void,
 *   destroy: () => void,
 * }}
 */
export function mountToastContainer(containerEl, { queue = new ToastQueue() } = {}) {
  const nodesById = new Map();
  const prefersReducedMotion =
    typeof window !== 'undefined' && typeof window.matchMedia === 'function'
      ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
      : false;

  function addNode(toast) {
    const el = document.createElement('div');
    el.className = `toast toast--${toast.variant}`;
    el.setAttribute('role', toast.variant === 'error' ? 'alert' : 'status');
    el.setAttribute('aria-live', toast.variant === 'error' ? 'assertive' : 'polite');
    el.style.setProperty('--toast-duration', `${toast.duration}ms`);

    const icon = document.createElement('span');
    icon.className = 'toast__icon';
    icon.innerHTML = TOAST_ICONS[toast.variant] ?? TOAST_ICONS.info;

    const message = document.createElement('span');
    message.className = 'toast__message';
    message.textContent = toast.message;

    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'toast__close';
    close.setAttribute('aria-label', 'Cerrar notificación');
    close.textContent = '✕';
    close.addEventListener('click', () => queue.dismiss(toast.id));

    const progressTrack = document.createElement('div');
    progressTrack.className = 'toast__progress-track';
    const progressBar = document.createElement('div');
    progressBar.className = 'toast__progress-bar';
    progressTrack.appendChild(progressBar);

    el.appendChild(icon);
    el.appendChild(message);
    el.appendChild(close);
    el.appendChild(progressTrack);
    containerEl.appendChild(el);
    nodesById.set(toast.id, el);

    if (prefersReducedMotion) {
      el.classList.add('toast--visible');
      return;
    }

    // Grow the stack into place rather than letting it jump: measure the
    // toast's natural size, collapse it to zero, then animate back up on
    // the next frame so sibling toasts shift smoothly instead of snapping.
    const targetHeight = el.offsetHeight;
    el.style.height = '0px';
    el.style.paddingTop = '0px';
    el.style.paddingBottom = '0px';
    void el.offsetHeight; // commit the collapsed state before animating
    requestAnimationFrame(() => {
      el.classList.add('toast--visible');
      el.style.height = `${targetHeight}px`;
      el.style.paddingTop = '';
      el.style.paddingBottom = '';
    });
    el.addEventListener('transitionend', (e) => {
      if (e.propertyName === 'height') el.style.height = '';
    });
  }

  function removeNode(id) {
    const el = nodesById.get(id);
    if (!el) return;
    nodesById.delete(id);
    if (prefersReducedMotion) {
      el.remove();
      return;
    }
    // Collapse the toast's own box (height + padding) as it fades so the
    // stack closes the gap smoothly instead of the remaining toasts
    // snapping into place the instant this node is removed from the DOM.
    const currentHeight = el.offsetHeight;
    el.style.height = `${currentHeight}px`;
    void el.offsetHeight; // commit the current height before animating down
    el.classList.remove('toast--visible');
    el.classList.add('toast--leaving');
    requestAnimationFrame(() => {
      el.style.height = '0px';
      el.style.paddingTop = '0px';
      el.style.paddingBottom = '0px';
    });
    const cleanup = () => el.remove();
    const handleTransitionEnd = (e) => {
      if (e.propertyName !== 'height') return;
      el.removeEventListener('transitionend', handleTransitionEnd);
      cleanup();
    };
    el.addEventListener('transitionend', handleTransitionEnd);
    // Safety net for when no transition event arrives (e.g. the element is
    // hidden). MUST outlast the sequenced exit in CSS: the fade runs for
    // --motion-duration-exit and only then does the height collapse run for
    // --motion-duration-base, so the total is ~620ms today.
    setTimeout(cleanup, 900);
  }

  function render(toasts) {
    const currentIds = new Set(toasts.map((toast) => toast.id));
    for (const id of nodesById.keys()) {
      if (!currentIds.has(id)) removeNode(id);
    }
    for (const toast of toasts) {
      if (!nodesById.has(toast.id)) addNode(toast);
    }
  }

  const unsubscribe = queue.subscribe(render);
  render(queue.list());

  return {
    showToast: (message, variant = 'info') => queue.add(message, variant),
    dismissToast: (id) => queue.dismiss(id),
    dismissToastsByVariant: (variant) => queue.dismissVariant(variant),
    dismissAllToasts: () => queue.dismissAll(),
    destroy: () => unsubscribe(),
  };
}
