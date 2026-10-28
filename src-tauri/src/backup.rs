//! Respaldo local: un archivo `<id>.bkp.jsonl` por cada ejecución de mover/ack.
//!
//! Es append-only: una cabecera, las entradas (escritas antes de cualquier ack en
//! origen) y después líneas de estado o de borrado, con fsync donde importa; nunca
//! se reescribe el archivo entero por lote. Leerlo es "plegar" las líneas; una línea
//! cortada por un corte de luz al final se ignora.
//!
//! Al lado vive `<id>.meta.json`: cabecera + cantidad por estado, mantenida en forma
//! incremental. El listado y la retención leen solo eso (un envío de 200k mensajes
//! pesa cientos de MB y no se puede leer entero para mostrar un selector).
//!
//! Estados de una entrada:
//! - `pending`: respaldada, todavía sin publicar ni ackear → sigue en origen.
//! - `published`: confirmada por el broker en destino pero sin ack en origen (corte
//!   entre ambos pasos) → puede estar duplicada en destino y en origen.
//! - `done`, `error` (sigue en origen), `republished`.

use crate::{e, R};
use lapin::BasicProperties;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

const EXT: &str = ".bkp.jsonl";
const META_EXT: &str = ".meta.json";

#[derive(Serialize, Deserialize, Clone)]
pub struct Header {
    pub id: String,
    pub ts: u64,
    pub op: String, // "ack" | "move"
    pub source: String,
    pub target: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    pub n: u32,
    pub status: String,
    pub error: Option<String>,
    pub routing_key: String,
    pub exchange: String,
    pub properties: BasicProperties,
    pub payload_b64: String,
    /// Cola a la que se re-publicó por última vez.
    #[serde(default)]
    pub republished_to: Option<String>,
}

/// Resumen de un envío: cabecera + cantidad de entradas por estado.
#[derive(Serialize, Deserialize, Clone)]
pub struct Meta {
    #[serde(flatten)]
    pub header: Header,
    pub counts: BTreeMap<String, u32>,
}

impl Meta {
    pub fn count(&self, status: &str) -> u32 {
        self.counts.get(status).copied().unwrap_or(0)
    }
    pub fn total(&self) -> u32 {
        self.counts.values().sum()
    }
    /// Tiene entradas cuyo destino real no se conoce (pueden faltar o estar
    /// duplicadas): la retención nunca borra estos envíos.
    pub fn unresolved(&self) -> bool {
        self.count("pending") + self.count("published") > 0
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Line {
    Run(Header),
    Entry(Entry),
    Status {
        n: u32,
        status: String,
        error: Option<String>,
        #[serde(default)]
        to: Option<String>,
    },
    Deleted {
        n: u32,
    },
}

/// Los ids llegan desde la UI y se usan para armar rutas: solo `[0-9a-z-]`.
pub fn valid_id(id: &str) -> R<()> {
    if !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase() || b == b'-') {
        Ok(())
    } else {
        log::warn!("id de respaldo inválido rechazado: {id:?}");
        Err(format!("id de respaldo inválido: {id}"))
    }
}

pub fn path(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("{id}{EXT}"))
}

fn meta_path(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("{id}{META_EXT}"))
}

/// tmp + rename; sin fsync (el resumen se puede reconstruir desde el .bkp.jsonl).
fn save_meta(dir: &Path, meta: &Meta) {
    let tmp = dir.join(format!("{}{META_EXT}.tmp", meta.header.id));
    let res = serde_json::to_vec(meta)
        .map_err(|err| err.to_string())
        .and_then(|b| std::fs::write(&tmp, b).map_err(|err| err.to_string()))
        .and_then(|_| std::fs::rename(&tmp, meta_path(dir, &meta.header.id)).map_err(|err| err.to_string()));
    if let Err(err) = res {
        log::warn!("no se pudo guardar el resumen de {}: {err}", meta.header.id);
    }
}

/// Archivo de una ejecución abierto para agregar líneas. Lleva el estado de cada
/// entrada para mantener el resumen al día sin releer el archivo.
pub struct Writer {
    f: std::fs::File,
    dir: PathBuf,
    meta: Meta,
    status: HashMap<u32, String>,
}

impl Writer {
    pub fn create(dir: &Path, header: &Header) -> R<Self> {
        valid_id(&header.id)?;
        let f = std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path(dir, &header.id))
            .map_err(e)?;
        let meta = Meta { header: header.clone(), counts: BTreeMap::new() };
        let mut w = Writer { f, dir: dir.to_path_buf(), meta, status: HashMap::new() };
        w.write(&[Line::Run(header.clone())], true)?;
        Ok(w)
    }

    /// Reabre un envío ya leído (re-publicar, borrar entradas).
    pub fn open(dir: &Path, run: &Run) -> R<Self> {
        let f = std::fs::OpenOptions::new()
            .append(true)
            .open(path(dir, &run.header.id))
            .map_err(e)?;
        Ok(Writer {
            f,
            dir: dir.to_path_buf(),
            meta: meta_of(run),
            status: run.entries.iter().map(|en| (en.n, en.status.clone())).collect(),
        })
    }

    fn write(&mut self, lines: &[Line], sync: bool) -> R<()> {
        let mut buf = Vec::new();
        for l in lines {
            serde_json::to_writer(&mut buf, l).map_err(e)?;
            buf.push(b'\n');
        }
        self.f.write_all(&buf).map_err(e)?;
        if sync {
            self.f.sync_data().map_err(e)?;
        }
        save_meta(&self.dir, &self.meta);
        Ok(())
    }

    /// Cambia el estado de `n` en el resumen (None = la entrada se borró).
    fn bump(&mut self, n: u32, new: Option<&str>) {
        if let Some(old) = self.status.remove(&n) {
            if let Some(c) = self.meta.counts.get_mut(&old) {
                *c = c.saturating_sub(1);
            }
        }
        if let Some(s) = new {
            *self.meta.counts.entry(s.to_string()).or_default() += 1;
            self.status.insert(n, s.to_string());
        }
        self.meta.counts.retain(|_, c| *c > 0);
    }

    /// Entradas nuevas (en estado "pending"). Siempre con fsync: se llama antes de
    /// publicar o ackear nada.
    pub fn entries(&mut self, es: Vec<Entry>) -> R<()> {
        for en in &es {
            self.bump(en.n, Some(&en.status));
        }
        self.write(&es.into_iter().map(Line::Entry).collect::<Vec<_>>(), true)
    }

    /// `sync`: fsync del cambio. Un estado que solo confirma algo ya durable (p. ej.
    /// "done" después de "published") puede ir sin fsync: si se pierde, el envío
    /// queda marcado como dudoso, nunca como resuelto.
    pub fn status(&mut self, upd: Vec<(u32, &str, Option<String>)>, to: Option<&str>, sync: bool) -> R<()> {
        let mut lines = Vec::with_capacity(upd.len());
        for (n, status, error) in upd {
            self.bump(n, Some(status));
            lines.push(Line::Status { n, status: status.into(), error, to: to.map(str::to_string) });
        }
        self.write(&lines, sync)
    }

    pub fn deleted(&mut self, ns: &[u32]) -> R<()> {
        for &n in ns {
            self.bump(n, None);
        }
        self.write(&ns.iter().map(|&n| Line::Deleted { n }).collect::<Vec<_>>(), true)
    }
}

pub struct Run {
    pub header: Header,
    pub entries: Vec<Entry>,
}

pub fn meta_of(run: &Run) -> Meta {
    let mut counts = BTreeMap::new();
    for en in &run.entries {
        *counts.entry(en.status.clone()).or_default() += 1;
    }
    Meta { header: run.header.clone(), counts }
}

/// Lee y pliega un archivo de ejecución.
pub fn read(file: &Path) -> R<Run> {
    let f = std::fs::File::open(file).map_err(e)?;
    let mut header = None;
    let mut entries: BTreeMap<u32, Entry> = BTreeMap::new();
    for (i, line) in std::io::BufReader::new(f).lines().enumerate() {
        let line = line.map_err(e)?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Line>(&line) {
            Ok(Line::Run(h)) => header = Some(h),
            Ok(Line::Entry(en)) => {
                entries.insert(en.n, en);
            }
            Ok(Line::Status { n, status, error, to }) => {
                if let Some(en) = entries.get_mut(&n) {
                    en.status = status;
                    en.error = error;
                    if to.is_some() {
                        en.republished_to = to;
                    }
                }
            }
            Ok(Line::Deleted { n }) => {
                entries.remove(&n);
            }
            Err(err) => log::warn!("{}: línea {} ilegible, se ignora ({err})", file.display(), i + 1),
        }
    }
    let header = header.ok_or_else(|| format!("{}: sin cabecera", file.display()))?;
    Ok(Run { header, entries: entries.into_values().collect() })
}

pub fn read_run(dir: &Path, id: &str) -> R<Run> {
    valid_id(id)?;
    read(&path(dir, id))
}

/// Resumen de un envío. Si falta (respaldos viejos o resumen perdido) se arma
/// leyendo el archivo una vez y se guarda.
pub fn read_meta(dir: &Path, id: &str) -> R<Meta> {
    valid_id(id)?;
    if let Ok(meta) = std::fs::read(meta_path(dir, id))
        .map_err(|err| err.to_string())
        .and_then(|b| serde_json::from_slice::<Meta>(&b).map_err(|err| err.to_string()))
    {
        return Ok(meta);
    }
    let meta = meta_of(&read(&path(dir, id))?);
    save_meta(dir, &meta);
    Ok(meta)
}

/// Borra un envío completo (respaldo + resumen).
pub fn remove(dir: &Path, id: &str) -> R<()> {
    valid_id(id)?;
    std::fs::remove_file(path(dir, id)).map_err(e)?;
    let _ = std::fs::remove_file(meta_path(dir, id));
    Ok(())
}

/// Ids de ejecuciones, del más reciente al más viejo (el id empieza con el timestamp).
pub fn ids(dir: &Path) -> R<Vec<String>> {
    let mut ids: Vec<String> = std::fs::read_dir(dir)
        .map_err(e)?
        .filter_map(|d| d.ok()?.file_name().to_str()?.strip_suffix(EXT).map(str::to_string))
        .filter(|id| valid_id(id).is_ok())
        .collect();
    ids.sort_by(|a, b| b.cmp(a));
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(n: u32) -> Entry {
        Entry {
            n,
            status: "pending".into(),
            error: None,
            routing_key: format!("rk{n}"),
            exchange: String::new(),
            properties: BasicProperties::default().with_message_id(format!("m{n}").into()),
            payload_b64: "e30=".into(),
            republished_to: None,
        }
    }

    #[test]
    fn folds_entries_status_and_deletes_and_survives_torn_tail() {
        let dir = std::env::temp_dir().join(format!("rabbit-tool-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let h = Header { id: "1700000000000-move".into(), ts: 1, op: "move".into(), source: "a".into(), target: Some("b".into()) };
        let mut w = Writer::create(&dir, &h).unwrap();
        w.entries((0..3).map(entry).collect()).unwrap();
        w.status(vec![(0, "done", None), (1, "error", Some("x".into()))], None, true).unwrap();
        w.status(vec![(0, "republished", None)], Some("c"), true).unwrap();
        w.deleted(&[2]).unwrap();
        // Línea cortada a mitad de escritura.
        std::fs::OpenOptions::new().append(true).open(path(&dir, &h.id)).unwrap()
            .write_all(b"{\"kind\":\"status\",\"n\":1,\"sta").unwrap();

        let run = read(&path(&dir, &h.id)).unwrap();
        assert_eq!(run.header.source, "a");
        assert_eq!(run.entries.len(), 2);
        assert_eq!(run.entries[0].status, "republished");
        assert_eq!(run.entries[0].republished_to.as_deref(), Some("c"));
        assert_eq!(run.entries[1].status, "error");
        assert_eq!(run.entries[1].properties.message_id().as_ref().map(|s| s.as_str()), Some("m1"));
        assert_eq!(ids(&dir).unwrap(), vec![h.id.clone()]);
        let meta = read_meta(&dir, &h.id).unwrap();
        assert_eq!((meta.count("republished"), meta.count("error"), meta.total()), (1, 1, 2));
        assert!(valid_id("../x").is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
