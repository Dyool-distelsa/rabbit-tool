#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_lite::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, ConfirmSelectOptions, QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::Mutex;

type R<T> = Result<T, String>;

/// Convierte un error en mensaje para la UI con toda su cadena de causas
/// (`error sending request` solo no dice nada; la causa sí), y lo deja en el log
/// con el detalle completo (`Debug`).
#[track_caller]
fn e<E: std::error::Error>(err: E) -> String {
    let mut msg = err.to_string();
    let mut src = err.source();
    while let Some(s) = src {
        let part = s.to_string();
        if !msg.contains(&part) {
            msg.push_str(": ");
            msg.push_str(&part);
        }
        src = s.source();
    }
    let at = std::panic::Location::caller();
    log::error!("{msg} [{}:{}] {err:?}", at.file(), at.line());
    msg
}

/// Mensajes por lote: un fsync del respaldo y un ack múltiple por lote.
const CHUNK: usize = 250;
/// Si la cola deja de entregar por este tiempo, se asume vacía.
const IDLE: Duration = Duration::from_millis(1500);
/// Tope de lectura por operación (prefetch es u16 y la UI tiene que poder con ello).
const MAX_PEEK: u32 = 20_000;

#[derive(Default)]
struct AppState {
    /// Conexión AMQP reutilizada entre comandos (uri, conexión).
    amqp: Mutex<Option<(String, Connection)>>,
    cancel: AtomicBool,
}

#[derive(Deserialize, Clone)]
struct Conn {
    /// ej: http://localhost:15672
    api: String,
    /// ej: amqp://localhost:5672
    amqp: String,
    user: String,
    pass: String,
    vhost: String,
}

impl Conn {
    fn vh(&self) -> String {
        urlencoding::encode(&self.vhost).into_owned()
    }
    /// host:puerto del campo AMQP, tolerando que traiga usuario o vhost
    /// (`amqp://u:p@host:5672/vh` → `host:5672`): usuario y vhost salen de sus campos.
    fn amqp_host(&self) -> (&str, &str) {
        let (scheme, rest) = self.amqp.trim().split_once("://").unwrap_or(("amqp", self.amqp.trim()));
        let rest = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
        (scheme, rest.split('/').next().unwrap_or(rest))
    }
    fn amqp_uri(&self) -> String {
        let (scheme, host) = self.amqp_host();
        format!(
            "{scheme}://{}:{}@{host}/{}",
            urlencoding::encode(&self.user),
            urlencoding::encode(&self.pass),
            self.vh()
        )
    }
    /// Descripción para el log, sin contraseña.
    fn desc(&self) -> String {
        let (scheme, host) = self.amqp_host();
        format!("{}@{} {scheme}://{host} vhost={}", self.user, self.api, self.vhost)
    }
}

// --- respaldo local -----------------------------------------------------------

/// Un lote de mensajes sacados de una cola. Se escribe (con fsync) antes de
/// cualquier ack en origen y se reescribe al cambiar el estado de sus entradas.
#[derive(Serialize, Deserialize)]
struct Batch {
    id: String,
    ts: u64,
    op: String, // "ack" | "move"
    source: String,
    target: Option<String>,
    entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    n: u32,
    status: String, // "pending" | "done" | "error" | "republished"
    error: Option<String>,
    routing_key: String,
    exchange: String,
    properties: BasicProperties,
    payload_b64: String,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn backup_dir(app: &AppHandle) -> R<PathBuf> {
    let dir = app.path().app_data_dir().map_err(e)?.join("backup");
    std::fs::create_dir_all(&dir).map_err(e)?;
    Ok(dir)
}

fn batch_path(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("{id}.batch.json"))
}

/// tmp + fsync + rename: o queda el archivo completo o el anterior.
fn save_batch(dir: &Path, b: &Batch) -> R<()> {
    use std::io::Write;
    let tmp = dir.join(format!("{}.tmp", b.id));
    let mut f = std::fs::File::create(&tmp).map_err(e)?;
    f.write_all(&serde_json::to_vec(b).map_err(e)?).map_err(e)?;
    f.sync_all().map_err(e)?;
    std::fs::rename(&tmp, batch_path(dir, &b.id)).map_err(e)
}

fn load_batch(dir: &Path, id: &str) -> R<Batch> {
    serde_json::from_slice(&std::fs::read(batch_path(dir, id)).map_err(e)?).map_err(e)
}

/// Corre IO de disco fuera del runtime async (y del hilo principal).
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> R<T> + Send + 'static) -> R<T> {
    tokio::task::spawn_blocking(f).await.map_err(e)?
}

// --- conversión para la UI -------------------------------------------------------

fn amqp_json(v: &AMQPValue) -> Value {
    match v {
        AMQPValue::Boolean(b) => json!(b),
        AMQPValue::ShortShortInt(n) => json!(n),
        AMQPValue::ShortShortUInt(n) => json!(n),
        AMQPValue::ShortInt(n) => json!(n),
        AMQPValue::ShortUInt(n) => json!(n),
        AMQPValue::LongInt(n) => json!(n),
        AMQPValue::LongUInt(n) => json!(n),
        AMQPValue::LongLongInt(n) => json!(n),
        AMQPValue::Float(n) => json!(n),
        AMQPValue::Double(n) => json!(n),
        AMQPValue::DecimalValue(d) => json!(d.value as f64 / 10f64.powi(d.scale as i32)),
        AMQPValue::ShortString(s) => json!(s.as_str()),
        AMQPValue::LongString(s) => json!(String::from_utf8_lossy(s.as_bytes())),
        AMQPValue::FieldArray(a) => Value::Array(a.as_slice().iter().map(amqp_json).collect()),
        AMQPValue::Timestamp(t) => json!(t),
        AMQPValue::FieldTable(t) => table_json(t),
        AMQPValue::ByteArray(b) => json!(B64.encode(b.as_slice())),
        AMQPValue::Void => Value::Null,
    }
}

fn table_json(t: &FieldTable) -> Value {
    Value::Object(
        t.inner()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), amqp_json(v)))
            .collect(),
    )
}

/// Propiedades legibles (sin los tags de tipo AMQP), solo las presentes.
fn props_json(p: &BasicProperties) -> Value {
    let mut m = Map::new();
    let mut put = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            m.insert(k.into(), v);
        }
    };
    let s = |v: &Option<lapin::types::ShortString>| v.as_ref().map(|s| json!(s.as_str()));
    put("content_type", s(p.content_type()));
    put("content_encoding", s(p.content_encoding()));
    put("delivery_mode", p.delivery_mode().map(|v| json!(v)));
    put("priority", p.priority().map(|v| json!(v)));
    put("correlation_id", s(p.correlation_id()));
    put("reply_to", s(p.reply_to()));
    put("expiration", s(p.expiration()));
    put("message_id", s(p.message_id()));
    put("timestamp", p.timestamp().map(|v| json!(v)));
    put("type", s(p.kind()));
    put("user_id", s(p.user_id()));
    put("app_id", s(p.app_id()));
    put("headers", p.headers().as_ref().map(table_json));
    Value::Object(m)
}

/// Payload como texto si es UTF-8; si no, base64 marcado como binario.
fn payload_text(data: &[u8]) -> (String, bool) {
    match std::str::from_utf8(data) {
        Ok(s) => (s.to_string(), false),
        Err(_) => (B64.encode(data), true),
    }
}

fn delivery_json(d: &Delivery) -> Value {
    let (payload, binary) = payload_text(&d.data);
    json!({
        "routing_key": d.routing_key.as_str(),
        "exchange": d.exchange.as_str(),
        "redelivered": d.redelivered,
        "size": d.data.len(),
        "properties": props_json(&d.properties),
        "payload": payload,
        "binary": binary,
    })
}

// --- AMQP / HTTP ----------------------------------------------------------------

fn http_client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("cliente http")
    })
}

fn http(c: &Conn, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
    http_client()
        .request(method, format!("{}/api/{}", c.api.trim_end_matches('/'), path))
        .basic_auth(&c.user, Some(&c.pass))
}

async fn send(rb: reqwest::RequestBuilder) -> R<Value> {
    let req = rb.build().map_err(e)?;
    let (method, url) = (req.method().clone(), req.url().clone());
    let t = std::time::Instant::now();
    let res = http_client().execute(req).await.map_err(|err| {
        log::error!("HTTP {method} {url} falló tras {:?}", t.elapsed());
        e(err)
    })?;
    let status = res.status();
    let body = res.text().await.map_err(e)?;
    log::debug!("HTTP {method} {url} → {status} ({} bytes, {:?})", body.len(), t.elapsed());
    if !status.is_success() {
        let short: String = body.chars().take(500).collect();
        log::error!("HTTP {method} {url} → {status}: {short}");
        return Err(match status.as_u16() {
            401 => "HTTP 401: usuario o contraseña incorrectos".into(),
            404 => format!("HTTP 404: no existe ({short})"),
            _ => format!("HTTP {status}: {short}"),
        });
    }
    serde_json::from_str(&body).map_err(e)
}

/// Canal nuevo sobre la conexión cacheada (reconecta si cambió o se cayó).
async fn channel(state: &AppState, c: &Conn) -> R<Channel> {
    let uri = c.amqp_uri();
    let mut guard = state.amqp.lock().await;
    if let Some((u, conn)) = guard.as_ref() {
        if *u == uri && conn.status().connected() {
            match conn.create_channel().await {
                Ok(ch) => return Ok(ch),
                Err(err) => log::warn!("no se pudo abrir canal en la conexión cacheada, reconectando: {err:?}"),
            }
        } else if *u == uri {
            log::warn!("la conexión AMQP cacheada se cayó ({:?}), reconectando", conn.status().state());
        }
    }
    log::info!("conectando AMQP: {}", c.desc());
    let t = std::time::Instant::now();
    let conn = tokio::time::timeout(
        Duration::from_secs(15),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .map_err(|_| {
        log::error!("timeout (15 s) conectando AMQP: {}", c.desc());
        format!("timeout conectando por AMQP a {}", c.amqp_host().1)
    })?
    .map_err(e)?;
    log::info!("AMQP conectado en {:?}", t.elapsed());
    conn.on_error(|err| log::error!("la conexión AMQP reportó un error: {err:?}"));
    let ch = conn.create_channel().await.map_err(e)?;
    *guard = Some((uri, conn));
    Ok(ch)
}

/// Cantidad de mensajes ready (falla si la cola no existe).
async fn ready_count(c: &Conn, state: &AppState, queue: &str) -> R<u32> {
    // Canal propio: un declare pasivo fallido cierra el canal.
    let ch = channel(state, c).await?;
    let q = ch
        .queue_declare(
            queue,
            QueueDeclareOptions { passive: true, ..Default::default() },
            FieldTable::default(),
        )
        .await
        .map_err(|err| {
            log::error!("declare pasivo de \"{queue}\" falló: {err:?}");
            format!("cola \"{queue}\": {err}")
        })?;
    let _ = ch.close(200, "ok").await;
    log::debug!("cola \"{queue}\": {} ready", q.message_count());
    Ok(q.message_count())
}

/// Siguiente entrega o None si la cola quedó en silencio por IDLE.
async fn next(consumer: &mut lapin::Consumer) -> R<Option<Delivery>> {
    match tokio::time::timeout(IDLE, consumer.next()).await {
        Ok(Some(Ok(d))) => Ok(Some(d)),
        Ok(Some(Err(err))) => Err(e(err)),
        Ok(None) | Err(_) => Ok(None),
    }
}

fn progress(app: &AppHandle, op: &str, done: u32, total: u32) {
    let _ = app.emit("progress", json!({ "op": op, "done": done, "total": total }));
}

// --- comandos -------------------------------------------------------------------

/// Verifica credenciales y vhost sin listar colas.
#[tauri::command]
async fn ping(conn: Conn) -> R<Value> {
    log::info!("ping: {}", conn.desc());
    let who = send(http(&conn, reqwest::Method::GET, "whoami")).await?;
    let vh = send(http(&conn, reqwest::Method::GET, &format!("vhosts/{}", conn.vh()))).await?;
    log::info!("ping ok: usuario={} mensajes en vhost={}", who["name"], vh["messages"]);
    Ok(json!({
        "user": who["name"],
        "messages": vh["messages"].as_u64(),
        "ready": vh["messages_ready"].as_u64(),
    }))
}

#[derive(Deserialize)]
struct QueuePage {
    page: u32,
    page_size: u32,
    /// Filtro por nombre (lo aplica el broker). Vacío = todas.
    name: String,
    regex: bool,
    /// "messages" (con mensajes primero) | "name"
    sort: String,
}

/// Una página de colas, paginada, filtrada y ordenada por el broker: nunca
/// trae el vhost entero.
#[tauri::command]
async fn list_queues(conn: Conn, q: QueuePage) -> R<Value> {
    let mut path = format!(
        "queues/{}?page={}&page_size={}&pagination=true\
         &columns=name,messages,messages_ready,messages_unacknowledged,consumers,type,state",
        conn.vh(),
        q.page.max(1),
        q.page_size.clamp(1, 500),
    );
    if q.sort == "name" {
        path.push_str("&sort=name");
    } else {
        path.push_str("&sort=messages&sort_reverse=true");
    }
    if !q.name.is_empty() {
        path.push_str(&format!("&name={}&use_regex={}", urlencoding::encode(&q.name), q.regex));
    }
    log::info!(
        "list_queues página {} de {} (orden={}, filtro={:?}, regex={})",
        q.page, q.page_size, q.sort, q.name, q.regex
    );
    let res = send(http(&conn, reqwest::Method::GET, &path)).await?;
    if res.get("items").is_none() {
        // Brokers muy viejos ignoran la paginación y devuelven un array plano.
        log::warn!("respuesta de /queues sin paginación: {}", res.to_string().chars().take(300).collect::<String>());
    }
    let items: Vec<Value> = res["items"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .map(|q| {
            json!({
                "name": q["name"],
                "messages": q["messages"].as_u64().unwrap_or(0),
                "ready": q["messages_ready"].as_u64().unwrap_or(0),
                "unacked": q["messages_unacknowledged"].as_u64().unwrap_or(0),
                "consumers": q["consumers"].as_u64().unwrap_or(0),
                "type": q["type"],
                "state": q["state"],
            })
        })
        .collect();
    Ok(json!({
        "items": items,
        "page": res["page"],
        "page_count": res["page_count"],
        "filtered_count": res["filtered_count"],
        "total_count": res["total_count"],
    }))
}

/// Lee hasta `count` mensajes y los devuelve a la cola con nack+requeue.
/// Va por AMQP con prefetch: mucho más rápido que el GET de management.
#[tauri::command]
async fn peek(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    queue: String,
    count: u32,
) -> R<Vec<Value>> {
    log::info!("peek \"{queue}\" count={count} {}", conn.desc());
    let t = std::time::Instant::now();
    let total = ready_count(&conn, &state, &queue).await?.min(count).min(MAX_PEEK);
    if total == 0 {
        log::info!("peek \"{queue}\": cola vacía");
        return Ok(vec![]);
    }
    progress(&app, "peek", 0, total);
    let ch = channel(&state, &conn).await?;
    ch.basic_qos(total.min(u16::MAX as u32) as u16, BasicQosOptions::default())
        .await
        .map_err(e)?;
    let mut consumer = ch
        .basic_consume(&queue, "", BasicConsumeOptions::default(), FieldTable::default())
        .await
        .map_err(e)?;

    let mut out = Vec::with_capacity(total as usize);
    let mut last_tag = 0;
    let result = async {
        while (out.len() as u32) < total {
            let Some(d) = next(&mut consumer).await? else { break };
            last_tag = d.delivery_tag;
            out.push(delivery_json(&d));
            if out.len() % 100 == 0 {
                progress(&app, "peek", out.len() as u32, total);
            }
        }
        Ok::<_, String>(())
    }
    .await;
    if last_tag > 0 {
        if let Err(err) = ch
            .basic_nack(last_tag, BasicNackOptions { multiple: true, requeue: true })
            .await
        {
            // Igual vuelven a la cola al cerrar el canal.
            log::warn!("peek \"{queue}\": nack falló ({err:?}), se requeuean al cerrar el canal");
        }
    }
    let _ = ch.close(200, "ok").await;
    result?;
    log::info!("peek \"{queue}\": {} de {total} mensajes en {:?}", out.len(), t.elapsed());
    Ok(out)
}

/// Saca hasta `count` mensajes de `source` en lotes. Por lote:
/// 1. recibe (sin ack), 2. guarda el lote en disco con fsync,
/// 3. si hay `target`, publica con confirms + mandatory y espera los confirms,
/// 4. recién entonces ackea el origen. Ante cualquier fallo los mensajes no
///    ackeados vuelven a la cola y el lote queda en disco marcado.
#[tauri::command]
async fn drain(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    source: String,
    target: Option<String>,
    count: u32,
) -> R<Value> {
    state.cancel.store(false, Ordering::SeqCst);
    let dir = backup_dir(&app)?;
    let op = if target.is_some() { "move" } else { "ack" };
    log::info!("{op} \"{source}\" → {target:?} count={count} {}", conn.desc());
    let started = std::time::Instant::now();

    if let Some(t) = &target {
        // Publicar al default exchange una cola inexistente descarta en silencio.
        ready_count(&conn, &state, t).await?;
    }
    let total = ready_count(&conn, &state, &source).await?.min(count);
    if total == 0 {
        return Ok(json!({ "ok": 0, "total": 0, "failed": [], "cancelled": false }));
    }

    let ch = channel(&state, &conn).await?;
    if target.is_some() {
        ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(e)?;
    }
    ch.basic_qos(CHUNK as u16, BasicQosOptions::default()).await.map_err(e)?;
    let mut consumer = ch
        .basic_consume(&source, "", BasicConsumeOptions::default(), FieldTable::default())
        .await
        .map_err(e)?;

    let (mut ok, mut failed, mut seq) = (0u32, Vec::<String>::new(), 0u32);
    progress(&app, op, 0, total);

    let result: R<()> = async {
        while ok < total && failed.is_empty() && !state.cancel.load(Ordering::SeqCst) {
            let want = CHUNK.min((total - ok) as usize);
            let mut ds: Vec<Delivery> = Vec::with_capacity(want);
            while ds.len() < want {
                match next(&mut consumer).await? {
                    Some(d) => ds.push(d),
                    None => break,
                }
            }
            if ds.is_empty() {
                break;
            }

            let batch = Batch {
                id: format!("{}-{seq:04}", now_ms()),
                ts: now_ms(),
                op: op.into(),
                source: source.clone(),
                target: target.clone(),
                entries: ds
                    .iter()
                    .enumerate()
                    .map(|(i, d)| Entry {
                        n: i as u32,
                        status: "pending".into(),
                        error: None,
                        routing_key: d.routing_key.to_string(),
                        exchange: d.exchange.to_string(),
                        properties: d.properties.clone(),
                        payload_b64: B64.encode(&d.data),
                    })
                    .collect(),
            };
            seq += 1;
            // Sin respaldo en disco no se ackea nada (los mensajes vuelven a la cola).
            let (d2, batch) = (dir.clone(), batch);
            let mut batch = blocking(move || save_batch(&d2, &batch).map(|_| batch)).await?;
            log::debug!("{op}: lote {} con {} mensajes respaldado", batch.id, ds.len());

            // Publica todo el lote en vuelo y después espera los confirms en orden.
            let mut errs: Vec<Option<String>> = vec![None; ds.len()];
            if let Some(t) = &target {
                let mut pending = Vec::with_capacity(ds.len());
                for d in &ds {
                    pending.push(
                        ch.basic_publish(
                            "",
                            t,
                            BasicPublishOptions { mandatory: true, ..Default::default() },
                            &d.data,
                            d.properties.clone(),
                        )
                        .await,
                    );
                }
                for (i, p) in pending.into_iter().enumerate() {
                    errs[i] = match p {
                        Err(err) => Some(e(err)),
                        Ok(confirm) => match confirm.await {
                            Ok(Confirmation::Ack(None)) => None,
                            Ok(Confirmation::Ack(Some(r))) => {
                                Some(format!("devuelto por el broker: {}", r.reply_text.as_str()))
                            }
                            Ok(_) => Some("broker nack en publish".into()),
                            Err(err) => Some(e(err)),
                        },
                    };
                }
            }

            if errs.iter().all(Option::is_none) {
                let last = ds.last().unwrap().delivery_tag;
                ch.basic_ack(last, BasicAckOptions { multiple: true }).await.map_err(e)?;
            } else {
                let n_err = errs.iter().filter(|x| x.is_some()).count();
                log::warn!(
                    "{op}: lote {} con {n_err} fallos de publish, primero: {:?}",
                    batch.id,
                    errs.iter().flatten().next()
                );
                for (d, err) in ds.iter().zip(&errs) {
                    let _ = match err {
                        None => ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await,
                        Some(_) => {
                            ch.basic_nack(
                                d.delivery_tag,
                                BasicNackOptions { multiple: false, requeue: true },
                            )
                            .await
                        }
                    };
                }
            }

            for (entry, err) in batch.entries.iter_mut().zip(errs) {
                match err {
                    None => {
                        entry.status = "done".into();
                        ok += 1;
                    }
                    Some(msg) => {
                        entry.status = "error".into();
                        entry.error = Some(format!("{msg} (sigue en origen)"));
                        failed.push(msg);
                    }
                }
            }
            let d2 = dir.clone();
            blocking(move || save_batch(&d2, &batch)).await?;
            progress(&app, op, ok, total);
        }
        Ok(())
    }
    .await;

    // Lo prefetcheado y no procesado vuelve a la cola al cerrar el canal.
    let _ = ch.close(200, "ok").await;
    log::info!(
        "{op} \"{source}\": {ok}/{total} ok, {} fallidos, cancelado={}, {:?}{}",
        failed.len(),
        state.cancel.load(Ordering::SeqCst),
        started.elapsed(),
        result.as_ref().err().map(|x| format!(", error: {x}")).unwrap_or_default()
    );
    result?;
    Ok(json!({
        "ok": ok,
        "total": total,
        "failed": failed,
        "cancelled": state.cancel.load(Ordering::SeqCst),
    }))
}

#[tauri::command]
fn cancel(state: State<'_, AppState>) {
    state.cancel.store(true, Ordering::SeqCst);
}

#[tauri::command]
async fn list_local(app: AppHandle) -> R<Vec<Value>> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        let mut batches: Vec<Batch> = std::fs::read_dir(&dir)
            .map_err(e)?
            .filter_map(|entry| {
                let p = entry.ok()?.path();
                p.to_str()?.ends_with(".batch.json").then_some(())?;
                match std::fs::read(&p).map_err(e).and_then(|b| serde_json::from_slice(&b).map_err(e)) {
                    Ok(b) => Some(b),
                    Err(err) => {
                        log::warn!("respaldo ilegible, se ignora: {} ({err})", p.display());
                        None
                    }
                }
            })
            .collect();
        batches.sort_by(|a, b| b.id.cmp(&a.id));
        let mut out = vec![];
        for b in &batches {
            for en in &b.entries {
                let raw = B64.decode(&en.payload_b64).unwrap_or_default();
                let (payload, binary) = payload_text(&raw);
                out.push(json!({
                    "batch": b.id, "n": en.n, "ts": b.ts, "op": b.op,
                    "source": b.source, "target": b.target,
                    "status": en.status, "error": en.error,
                    "routing_key": en.routing_key, "exchange": en.exchange,
                    "size": raw.len(),
                    "properties": props_json(&en.properties),
                    "payload": payload, "binary": binary,
                }));
            }
        }
        Ok(out)
    })
    .await
}

#[derive(Deserialize)]
struct EntryRef {
    batch: String,
    n: u32,
}

/// Re-publica entradas del respaldo a una cola (recuperación manual).
#[tauri::command]
async fn republish(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    items: Vec<EntryRef>,
    queue: String,
) -> R<u32> {
    let dir = backup_dir(&app)?;
    ready_count(&conn, &state, &queue).await?;
    let ch = channel(&state, &conn).await?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(e)?;

    // Agrupa por lote para leer/escribir cada archivo una sola vez.
    let mut by_batch: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
    for it in items {
        by_batch.entry(it.batch).or_default().push(it.n);
    }
    let (total, mut done) = (by_batch.values().map(Vec::len).sum::<usize>() as u32, 0u32);
    let result: R<()> = async {
        for (id, ns) in by_batch {
            let (d2, id2) = (dir.clone(), id.clone());
            let mut b = blocking(move || load_batch(&d2, &id2)).await?;
            let mut err = None;
            for en in b.entries.iter_mut().filter(|en| ns.contains(&en.n)) {
                let data = B64.decode(&en.payload_b64).map_err(e)?;
                let conf = ch
                    .basic_publish(
                        "",
                        &queue,
                        BasicPublishOptions { mandatory: true, ..Default::default() },
                        &data,
                        en.properties.clone(),
                    )
                    .await
                    .map_err(e)?
                    .await
                    .map_err(e)?;
                if !matches!(conf, Confirmation::Ack(None)) {
                    err = Some(format!("el broker rechazó el mensaje {}/{}", b.id, en.n));
                    break;
                }
                en.status = "republished".into();
                en.error = None;
                done += 1;
            }
            b.target = Some(queue.clone());
            let d2 = dir.clone();
            blocking(move || save_batch(&d2, &b)).await?;
            progress(&app, "republish", done, total);
            if let Some(err) = err {
                return Err(err);
            }
        }
        Ok(())
    }
    .await;
    let _ = ch.close(200, "ok").await;
    log::info!(
        "republish → \"{queue}\": {done}/{total}{}",
        result.as_ref().err().map(|x| format!(", error: {x}")).unwrap_or_default()
    );
    result.map(|_| done)
}

#[tauri::command]
async fn delete_local(app: AppHandle, items: Vec<EntryRef>) -> R<u32> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        let mut by_batch: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
        for it in items {
            by_batch.entry(it.batch).or_default().push(it.n);
        }
        let mut removed = 0u32;
        for (id, ns) in by_batch {
            let mut b = load_batch(&dir, &id)?;
            let before = b.entries.len();
            b.entries.retain(|en| !ns.contains(&en.n));
            removed += (before - b.entries.len()) as u32;
            if b.entries.is_empty() {
                std::fs::remove_file(batch_path(&dir, &id)).map_err(e)?;
            } else {
                save_batch(&dir, &b)?;
            }
        }
        log::info!("delete_local: {removed} respaldos borrados");
        Ok(removed)
    })
    .await
}

#[tauri::command]
async fn backup_path(app: AppHandle) -> R<String> {
    Ok(backup_dir(&app)?.display().to_string())
}

/// Cierra la conexión AMQP cacheada (al editar o cambiar de perfil).
#[tauri::command]
async fn disconnect(state: State<'_, AppState>) -> R<()> {
    if let Some((_, conn)) = state.amqp.lock().await.take() {
        log::info!("desconectando AMQP");
        let _ = conn.close(200, "bye").await;
    }
    Ok(())
}

// --- log --------------------------------------------------------------------------

const LOG_NAME: &str = "rabbit-tool";

fn log_file(app: &AppHandle) -> R<PathBuf> {
    Ok(app.path().app_log_dir().map_err(e)?.join(format!("{LOG_NAME}.log")))
}

/// Errores y eventos de la UI al mismo archivo (target "ui").
#[tauri::command]
fn log_ui(level: String, message: String) {
    let lvl = match level.as_str() {
        "error" => log::Level::Error,
        "warn" => log::Level::Warn,
        "debug" => log::Level::Debug,
        _ => log::Level::Info,
    };
    log::log!(target: "ui", lvl, "{message}");
}

#[tauri::command]
fn log_path(app: AppHandle) -> R<String> {
    Ok(log_file(&app)?.display().to_string())
}

/// Abre el explorador con el archivo de log seleccionado.
#[tauri::command]
fn reveal_log(app: AppHandle) -> R<()> {
    let path = log_file(&app)?;
    #[cfg(target_os = "windows")]
    let r = std::process::Command::new("explorer").arg("/select,").arg(&path).spawn();
    #[cfg(target_os = "macos")]
    let r = std::process::Command::new("open").arg("-R").arg(&path).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let r = std::process::Command::new("xdg-open").arg(path.parent().unwrap_or(&path)).spawn();
    r.map(|_| ()).map_err(e)
}

fn main() {
    use tauri_plugin_log::{RotationStrategy, Target, TargetKind, TimezoneStrategy};

    tauri::Builder::default()
        .plugin(
            tauri_plugin_log::Builder::new()
                .clear_targets()
                .targets([
                    Target::new(TargetKind::LogDir { file_name: Some(LOG_NAME.into()) }),
                    Target::new(TargetKind::Stdout),
                ])
                .level(log::LevelFilter::Info)
                .level_for("rabbit_tool", log::LevelFilter::Debug)
                .level_for("ui", log::LevelFilter::Debug)
                .max_file_size(5_000_000)
                .rotation_strategy(RotationStrategy::KeepSome(5))
                .timezone_strategy(TimezoneStrategy::UseLocal)
                .build(),
        )
        .setup(|app| {
            // Un pánico en cualquier hilo queda en el archivo, con backtrace.
            let default_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                log::error!(
                    "PANIC: {info}\n{}",
                    std::backtrace::Backtrace::force_capture()
                );
                default_hook(info);
            }));
            log::info!(
                "Rabbit Tool {} iniciado — log en {}",
                app.package_info().version,
                log_file(app.handle()).unwrap_or_default().display()
            );
            Ok(())
        })
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            ping,
            list_queues,
            peek,
            drain,
            cancel,
            list_local,
            republish,
            delete_local,
            backup_path,
            disconnect,
            log_ui,
            log_path,
            reveal_log
        ])
        .run(tauri::generate_context!())
        .expect("error al iniciar tauri");
}
