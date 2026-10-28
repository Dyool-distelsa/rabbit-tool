//! Respaldo local: un archivo `<id>.bkp.jsonl` por cada ejecución de mover/ack.
//!
//! Es append-only: una cabecera, las entradas (escritas antes de cualquier ack en
//! origen) y después líneas de estado o de borrado. Cada append hace fsync, así que
//! nunca se reescribe el archivo entero por lote. Leerlo es "plegar" las líneas; una
//! línea cortada por un corte de luz al final se ignora.

use crate::{e, R};
use lapin::BasicProperties;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

const EXT: &str = ".bkp.jsonl";

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
    pub status: String, // "pending" | "done" | "error" | "republished"
    pub error: Option<String>,
    pub routing_key: String,
    pub exchange: String,
    pub properties: BasicProperties,
    pub payload_b64: String,
    /// Cola a la que se re-publicó por última vez.
    #[serde(default)]
    pub republished_to: Option<String>,
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

pub fn path(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("{id}{EXT}"))
}

/// Archivo de una ejecución abierto para agregar líneas.
pub struct Writer {
    f: std::fs::File,
}

impl Writer {
    pub fn create(dir: &Path, header: &Header) -> R<Self> {
        let f = std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path(dir, &header.id))
            .map_err(e)?;
        let mut w = Writer { f };
        w.write(&[Line::Run(header.clone())])?;
        Ok(w)
    }

    pub fn open(dir: &Path, id: &str) -> R<Self> {
        let f = std::fs::OpenOptions::new().append(true).open(path(dir, id)).map_err(e)?;
        Ok(Writer { f })
    }

    fn write(&mut self, lines: &[Line]) -> R<()> {
        let mut buf = Vec::new();
        for l in lines {
            serde_json::to_writer(&mut buf, l).map_err(e)?;
            buf.push(b'\n');
        }
        self.f.write_all(&buf).map_err(e)?;
        self.f.sync_data().map_err(e)
    }

    /// Entradas nuevas (en estado "pending"). Se llama antes de ackear nada.
    pub fn entries(&mut self, es: Vec<Entry>) -> R<()> {
        self.write(&es.into_iter().map(Line::Entry).collect::<Vec<_>>())
    }

    pub fn status(&mut self, upd: Vec<(u32, &str, Option<String>)>, to: Option<&str>) -> R<()> {
        let lines: Vec<Line> = upd
            .into_iter()
            .map(|(n, status, error)| Line::Status {
                n,
                status: status.into(),
                error,
                to: to.map(str::to_string),
            })
            .collect();
        self.write(&lines)
    }

    pub fn deleted(&mut self, ns: &[u32]) -> R<()> {
        self.write(&ns.iter().map(|&n| Line::Deleted { n }).collect::<Vec<_>>())
    }
}

pub struct Run {
    pub header: Header,
    pub entries: Vec<Entry>,
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

/// Ids de ejecuciones, del más reciente al más viejo (el id empieza con el timestamp).
pub fn ids(dir: &Path) -> R<Vec<String>> {
    let mut ids: Vec<String> = std::fs::read_dir(dir)
        .map_err(e)?
        .filter_map(|d| d.ok()?.file_name().to_str()?.strip_suffix(EXT).map(str::to_string))
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
        w.status(vec![(0, "done", None), (1, "error", Some("x".into()))], None).unwrap();
        w.status(vec![(0, "republished", None)], Some("c")).unwrap();
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
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
