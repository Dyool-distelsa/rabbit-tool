/**
 * Detecta el ambiente por los nombres (colas y perfil). Se parte cada nombre en
 * palabras (por guiones, puntos, guiones bajos y mayúsculas internas) y se compara
 * palabra por palabra, así "product-sync" no pasa por producción pero "orders.PRD"
 * o "PRD-billing" sí.
 */
const LEVELS = [
  ['prod', ['prod', 'prd', 'production', 'produccion', 'producción']],
  ['qa', ['qa', 'qas', 'staging', 'stg', 'uat', 'test', 'testing', 'preprod', 'pre']],
  ['dev', ['dev', 'develop', 'development', 'desarrollo', 'local', 'sandbox']],
];

export const ENV_INFO = {
  prod: { title: 'Producción', icon: '⚠', text: 'Esta operación toca una cola de producción.' },
  qa: { title: 'QA', icon: '◆', text: 'Esta operación toca una cola de QA / staging.' },
  dev: { title: 'Desarrollo', icon: '✦', text: 'Ambiente de desarrollo.' },
};

function words(name) {
  return String(name || '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .split(/[^\p{L}\p{N}]+/u)
    .map((w) => w.toLowerCase().replace(/\d+$/, ''))
    .filter(Boolean);
}

/**
 * Ambiente de un nombre: "prod" | "qa" | "dev" | null. Producción gana siempre
 * (por seguridad); entre QA y DEV decide la primera palabra que aparece, así
 * "DEV-test-raw" es DEV aunque "test" sea de QA.
 */
export function envOf(name) {
  const ws = words(name);
  if (ws.some((w) => LEVELS[0][1].includes(w))) return 'prod';
  for (const w of ws) {
    for (const [level, keys] of LEVELS.slice(1)) if (keys.includes(w)) return level;
  }
  return null;
}

/** El más riesgoso entre varios nombres, con el nombre que lo disparó. */
export function worstEnv(names) {
  let best = null;
  for (const name of names.filter(Boolean)) {
    const level = envOf(name);
    if (!level) continue;
    const rank = LEVELS.findIndex(([l]) => l === level);
    if (!best || rank < best.rank) best = { level, name, rank };
  }
  return best && { level: best.level, name: best.name };
}

/** El más riesgoso de dos resultados de `worstEnv` (cualquiera puede ser null). */
export function worseEnv(a, b) {
  if (!a || !b) return a || b || null;
  const rank = (x) => LEVELS.findIndex(([l]) => l === x.level);
  return rank(b) < rank(a) ? b : a;
}
