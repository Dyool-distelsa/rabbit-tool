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
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::Mutex;

mod backup;

type R<T> = Result<T, String>;

/// Convierte un error en mensaje para la UI con toda su cadena de causas
/// (`error sending request` solo no dice nada; la causa sí), y lo deja en el log
/// con el detalle completo (`Debug`).
#[track_caller]
fn e<E: std::error::Error + 'static>(err: E) -> String {
    let mut msg = match (&err as &dyn std::any::Any).downcast_ref::<lapin::Error>() {
        Some(le) => amqp_msg(le),
        None => err.to_string(),
    };
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

/// Texto entendible para los errores de AMQP más comunes (el de lapin es críptico:
/// "protocol error: ACCESS_REFUSED - ..."). Se conserva el detalle del broker.
fn amqp_msg(err: &lapin::Error) -> String {
    use lapin::protocol::{AMQPErrorKind, AMQPHardError, AMQPSoftError};
    match err {
        lapin::Error::ProtocolError(pe) => {
            let detail = pe.get_message().as_str();
            let hint = match pe.kind() {
                AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED) => {
                    "sin permiso AMQP (configure/write/read) sobre ese recurso o vhost"
                }
                AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND) => "la cola o el exchange no existe",
                AMQPErrorKind::Soft(AMQPSoftError::RESOURCELOCKED) => {
                    "la cola es exclusiva de otra conexión (o tiene un consumidor exclusivo)"
                }
                AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) => {
                    "el broker rechazó la operación (p. ej. se superó su consumer_timeout o un tag de entrega no es válido)"
                }
                AMQPErrorKind::Hard(AMQPHardError::NOTALLOWED) => "el broker no permite la operación (¿vhost sin acceso?)",
                AMQPErrorKind::Hard(AMQPHardError::CONNECTIONFORCED) => "el broker cerró la conexión",
                AMQPErrorKind::Hard(AMQPHardError::RESOURCEERROR) => "el broker se quedó sin recursos",
                _ => "error de protocolo AMQP",
            };
            format!("{hint}: {detail}")
        }
        lapin::Error::IOError(io) => match io.kind() {
            std::io::ErrorKind::ConnectionRefused => format!("conexión AMQP rechazada (¿host/puerto correctos?): {io}"),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted => {
                format!("se cortó la conexión AMQP: {io}")
            }
            std::io::ErrorKind::TimedOut => format!("timeout de red AMQP: {io}"),
            _ => format!("error de red AMQP: {io}"),
        },
        lapin::Error::InvalidChannelState(st) => {
            format!("el canal AMQP ya está cerrado ({st:?}); el broker lo cerró por un error anterior, revisá el log")
        }
        lapin::Error::InvalidConnectionState(st) => format!("la conexión AMQP no está activa ({st:?})"),
        other => other.to_string(),
    }
}

/// Mensajes por lote: un fsync del respaldo y un ack múltiple por lote.
const CHUNK: usize = 250;
/// Si la cola deja de entregar por este tiempo se asume que se vació (o que otro
/// consumidor se llevó el resto). Holgado para brokers lentos o mensajes grandes;
/// cuando corta antes de lo esperado se informa como "detenido".
const IDLE: Duration = Duration::from_secs(5);
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

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn backup_dir(app: &AppHandle) -> R<PathBuf> {
    let dir = app.path().app_data_dir().map_err(e)?.join("backup");
    std::fs::create_dir_all(&dir).map_err(e)?;
    Ok(dir)
}

/// Corre IO de disco fuera del runtime async (y del hilo principal).
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> R<T> + Send + 'static) -> R<T> {
    tokio::task::spawn_blocking(f).await.map_err(e)?
}

/// fsync dentro de un comando async sin bloquear el resto del runtime.
fn sync_io<T>(f: impl FnOnce() -> R<T>) -> R<T> {
    tokio::task::block_in_place(f)
}

/// Contexto que manda la UI para la metadata del respaldo.
#[derive(Deserialize, Default, Clone)]
struct RunCtx {
    #[serde(default)]
    profile: String,
    /// Filtro local aplicado al elegir los mensajes.
    #[serde(default)]
    filter: Option<String>,
}

/// Datos de un envío nuevo: de dónde sale y su versión padre (si viene de otro respaldo).
struct RunInfo<'a> {
    op: &'a str,
    source: &'a str,
    target: &'a Option<String>,
    conn: &'a Conn,
    ctx: &'a RunCtx,
    origin: String,
    parent: Option<String>,
}

/// Abre el archivo de respaldo de una ejecución nueva. Devuelve (writer, id).
fn new_run(app: &AppHandle, info: RunInfo) -> R<(backup::Writer, String)> {
    let header = backup::Header {
        id: format!("{}-{}", now_ms(), info.op),
        ts: now_ms(),
        op: info.op.into(),
        source: info.source.into(),
        target: info.target.clone(),
        profile: info.ctx.profile.clone(),
        host: info.conn.api.clone(),
        vhost: info.conn.vhost.clone(),
        user: info.conn.user.clone(),
        app_version: app.package_info().version.to_string(),
        parent: info.parent,
        origin: Some(info.origin),
        filter: info.ctx.filter.clone().filter(|f| !f.trim().is_empty()),
    };
    log::info!("respaldo nuevo: {}", header.id);
    let dir = backup_dir(app)?;
    let w = sync_io(|| backup::Writer::create(&dir, &header))?;
    Ok((w, header.id))
}

fn entry_of(n: u32, d: &Delivery) -> backup::Entry {
    backup::Entry {
        n,
        status: "pending".into(),
        error: None,
        routing_key: d.routing_key.to_string(),
        exchange: d.exchange.to_string(),
        properties: d.properties.clone(),
        payload_b64: B64.encode(&d.data),
        republished_to: None,
    }
}

/// Huella de un mensaje para reconocerlo entre lecturas (Ver → Mover seleccionados).
/// No incluye `redelivered` ni headers que cambian al reencolar.
fn fingerprint(d: &Delivery) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let p = &d.properties;
    let s = |v: &Option<lapin::types::ShortString>| v.as_ref().map(|x| x.as_str().to_string());
    d.routing_key.as_str().hash(&mut h);
    d.data.hash(&mut h);
    s(p.message_id()).hash(&mut h);
    s(p.correlation_id()).hash(&mut h);
    s(p.content_type()).hash(&mut h);
    p.timestamp().hash(&mut h);
    format!("{:016x}", h.finish())
}

// --- JSON → AMQP (mensajes cargados desde archivo) ------------------------------

fn json_amqp(v: &Value) -> AMQPValue {
    match v {
        Value::Null => AMQPValue::Void,
        Value::Bool(b) => AMQPValue::Boolean(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => AMQPValue::LongLongInt(i),
            None => AMQPValue::Double(n.as_f64().unwrap_or_default()),
        },
        Value::String(s) => AMQPValue::LongString(s.as_str().into()),
        Value::Array(a) => AMQPValue::FieldArray(a.iter().map(json_amqp).collect::<Vec<_>>().into()),
        Value::Object(o) => AMQPValue::FieldTable(json_table(o)),
    }
}

fn json_table(o: &Map<String, Value>) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in o {
        t.insert(k.as_str().into(), json_amqp(v));
    }
    t
}

/// Properties a partir del JSON (mismos nombres que muestra la UI). Persistente por defecto.
fn props_from_json(p: &Value) -> BasicProperties {
    let s = |k: &str| p.get(k).and_then(Value::as_str).map(lapin::types::ShortString::from);
    let u = |k: &str| p.get(k).and_then(Value::as_u64);
    let mut b = BasicProperties::default().with_delivery_mode(u("delivery_mode").unwrap_or(2) as u8);
    if let Some(v) = s("content_type") {
        b = b.with_content_type(v);
    }
    if let Some(v) = s("content_encoding") {
        b = b.with_content_encoding(v);
    }
    if let Some(v) = u("priority") {
        b = b.with_priority(v as u8);
    }
    if let Some(v) = s("correlation_id") {
        b = b.with_correlation_id(v);
    }
    if let Some(v) = s("reply_to") {
        b = b.with_reply_to(v);
    }
    if let Some(v) = s("expiration") {
        b = b.with_expiration(v);
    }
    if let Some(v) = s("message_id") {
        b = b.with_message_id(v);
    }
    if let Some(v) = u("timestamp") {
        b = b.with_timestamp(v);
    }
    if let Some(v) = s("type") {
        b = b.with_type(v);
    }
    if let Some(v) = s("app_id") {
        b = b.with_app_id(v);
    }
    if let Some(Value::Object(h)) = p.get("headers") {
        b = b.with_headers(json_table(h));
    }
    b
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
        "fp": fingerprint(d),
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
        // El broker explica el motivo en {"error","reason"}: se usa tal cual, porque
        // un 401 puede ser credenciales malas o un endpoint que exige admin.
        let reason = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| v["reason"].as_str().map(str::to_string))
            .unwrap_or(short);
        let path = url.path();
        return Err(match (status.as_u16(), reason.as_str()) {
            (401, "Not_Authorized") => "HTTP 401: usuario o contraseña incorrectos".into(),
            (401, r) if r.contains("administrator") => {
                format!("HTTP 401 en {path}: el usuario no es administrador ({r})")
            }
            (401, r) => format!("HTTP 401 en {path}: sin permisos ({r})"),
            (404, r) => format!("HTTP 404 en {path}: no existe ({r})"),
            (_, r) => format!("HTTP {status} en {path}: {r}"),
        });
    }
    serde_json::from_str(&body).map_err(e)
}

/// Canal nuevo sobre la conexión cacheada (reconecta si cambió o se cayó).
async fn channel(state: &AppState, c: &Conn) -> R<Channel> {
    let (_, host) = c.amqp_host();
    if host.ends_with(":15672") {
        log::error!("el campo AMQP apunta al puerto de management: {host}");
        return Err(format!(
            "El campo AMQP apunta a {host}, que es el puerto de management. \
             AMQP suele ser el 5672 (amqp://host:5672)"
        ));
    }
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

/// Mensajes por consumer al leer sin ack. lapin bufferiza sin límite lo que el
/// broker manda hasta el prefetch: con 2 000 la memoria queda acotada a ~2 000
/// mensajes aunque se recorran 200k (65 000 × 50 KB eran ~3 GB).
const WINDOW: u32 = 2_000;

/// Lee hasta `n` mensajes en orden y sin ack, sin el techo de 65 535 del prefetch:
/// encadena consumers en el mismo canal y crea el siguiente recién cuando el
/// anterior llenó su prefetch, así el broker entrega en el orden de la cola. Solo
/// sirve cuando no se ackea durante la lectura (ackear liberaría al consumer
/// anterior y mezclaría las entregas).
struct Reader<'a> {
    ch: &'a Channel,
    queue: &'a str,
    cur: Option<lapin::Consumer>,
    in_cur: u32,
    cur_cap: u32,
    left: u32,
}

impl<'a> Reader<'a> {
    fn new(ch: &'a Channel, queue: &'a str, n: u32) -> Self {
        Reader { ch, queue, cur: None, in_cur: 0, cur_cap: 0, left: n }
    }

    async fn next(&mut self) -> R<Option<Delivery>> {
        if self.left == 0 {
            return Ok(None);
        }
        if self.cur.is_none() || self.in_cur >= self.cur_cap {
            self.cur_cap = self.left.min(WINDOW);
            // basic.qos aplica a los consumers creados después.
            self.ch
                .basic_qos(self.cur_cap as u16, BasicQosOptions::default())
                .await
                .map_err(e)?;
            let c = self
                .ch
                .basic_consume(self.queue, "", BasicConsumeOptions::default(), FieldTable::default())
                .await
                .map_err(e)?;
            self.cur = Some(c);
            self.in_cur = 0;
        }
        match next(self.cur.as_mut().unwrap()).await? {
            Some(d) => {
                self.in_cur += 1;
                self.left -= 1;
                Ok(Some(d))
            }
            None => {
                self.left = 0;
                Ok(None)
            }
        }
    }
}

/// Arranca una operación cancelable: descarta un pedido de cancelación viejo.
fn begin(state: &AppState) {
    state.cancel.store(false, Ordering::SeqCst);
}

fn cancelled(state: &AppState) -> bool {
    state.cancel.load(Ordering::SeqCst)
}

/// Origen y destino iguales: publicaría lo mismo que está consumiendo.
fn same_queue(source: &str, target: &Option<String>) -> R<()> {
    match target {
        Some(t) if t == source => Err("el destino no puede ser la misma cola que el origen".into()),
        _ => Ok(()),
    }
}

fn progress(app: &AppHandle, op: &str, done: u32, total: u32) {
    let _ = app.emit("progress", json!({ "op": op, "done": done, "total": total }));
}

// --- comandos -------------------------------------------------------------------

/// Verifica credenciales, acceso al vhost y AMQP, sin listar colas. Solo usa
/// endpoints que no exigen el tag administrator.
#[tauri::command]
async fn ping(state: State<'_, AppState>, conn: Conn) -> R<Value> {
    log::info!("ping: {}", conn.desc());
    let who = send(http(&conn, reqwest::Method::GET, "whoami")).await?;
    log::info!("whoami: usuario={} tags={}", who["name"], who["tags"]);
    // Una página de 1 cola: valida que el vhost exista y sea accesible, y da el total.
    let q = send(http(
        &conn,
        reqwest::Method::GET,
        &format!("queues/{}?page=1&page_size=1&columns=name", conn.vh()),
    ))
    .await
    .map_err(|err| format!("vhost \"{}\": {err}", conn.vhost))?;

    // AMQP se prueba ya, para que un puerto o permiso mal no aparezca recién al leer.
    let amqp_error = match channel(&state, &conn).await {
        Ok(ch) => {
            let _ = ch.close(200, "ok").await;
            None
        }
        Err(err) => Some(err),
    };
    log::info!(
        "ping ok: colas en vhost={} amqp={}",
        q["total_count"],
        amqp_error.as_deref().unwrap_or("ok")
    );
    Ok(json!({
        "user": who["name"],
        "queues": q["total_count"].as_u64(),
        "amqp_error": amqp_error,
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

/// Lee una página de mensajes (`limit` a partir de la posición `offset`) y los
/// devuelve todos a la cola con nack+requeue. Para llegar a `offset` hay que
/// recorrer los anteriores (AMQP no permite saltar): se leen sin guardarlos.
#[tauri::command]
async fn peek(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    queue: String,
    offset: u32,
    limit: u32,
) -> R<Value> {
    log::info!("peek \"{queue}\" offset={offset} limit={limit} {}", conn.desc());
    begin(&state);
    let t = std::time::Instant::now();
    let ready = ready_count(&conn, &state, &queue).await?;
    let limit = limit.min(MAX_PEEK);
    let total = ready.min(offset.saturating_add(limit));
    if total <= offset {
        log::info!("peek \"{queue}\": nada desde {offset} (ready={ready})");
        return Ok(json!({ "items": [], "offset": offset, "ready": ready, "stalled": false, "cancelled": false }));
    }
    progress(&app, "peek", 0, total);
    let ch = channel(&state, &conn).await?;
    let mut reader = Reader::new(&ch, &queue, total);

    let mut out = Vec::with_capacity((total - offset) as usize);
    let (mut last_tag, mut seen, mut stopped) = (0, 0u32, false);
    let result = async {
        while let Some(d) = reader.next().await? {
            last_tag = d.delivery_tag;
            if cancelled(&state) {
                stopped = true;
                break;
            }
            if seen >= offset {
                out.push(delivery_json(&d));
            }
            seen += 1;
            if seen % 500 == 0 {
                progress(&app, "peek", seen, total);
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
    // Se esperaba recorrer `total`: si no, la cola dejó de entregar (otro consumidor).
    let stalled = !stopped && seen < total;
    log::info!(
        "peek \"{queue}\": {} mensajes desde {offset} (recorridos {seen}/{total}, ready {ready}, cancelado={stopped}, detenido={stalled}) en {:?}",
        out.len(),
        t.elapsed()
    );
    Ok(json!({ "items": out, "offset": offset, "ready": ready, "stalled": stalled, "cancelled": stopped }))
}

/// Publica en vuelo al default exchange → `queue` (mandatory) y después espera los
/// confirms en orden. Devuelve, por mensaje, None si el broker lo confirmó.
async fn publish_all(
    ch: &Channel,
    queue: &str,
    msgs: Vec<(&[u8], BasicProperties)>,
) -> Vec<Option<String>> {
    let mut pending = Vec::with_capacity(msgs.len());
    for (data, props) in msgs {
        pending.push(
            ch.basic_publish(
                "",
                queue,
                BasicPublishOptions { mandatory: true, ..Default::default() },
                data,
                props,
            )
            .await,
        );
    }
    let mut out = Vec::with_capacity(pending.len());
    for p in pending {
        out.push(match p {
            Err(err) => Some(e(err)),
            Ok(confirm) => match confirm.await {
                Ok(Confirmation::Ack(None)) => None,
                Ok(Confirmation::Ack(Some(r))) => {
                    Some(format!("devuelto por el broker: {}", r.reply_text.as_str()))
                }
                Ok(_) => Some("broker nack en publish".into()),
                Err(err) => Some(e(err)),
            },
        });
    }
    out
}

/// Respalda (fsync) un grupo de entregas, las publica en `target` si hay, y ackea en
/// origen solo las confirmadas; las fallidas se nackean con requeue. Devuelve los
/// errores por entrega. `individual_acks` cuando las entregas no son consecutivas.
async fn process_group(
    ch: &Channel,
    w: &mut backup::Writer,
    base: u32,
    ds: &[&Delivery],
    target: &Option<String>,
    individual_acks: bool,
) -> R<Vec<Option<String>>> {
    let entries = ds.iter().enumerate().map(|(i, d)| entry_of(base + i as u32, d)).collect();
    // Sin respaldo en disco no se ackea nada.
    sync_io(|| w.entries(entries))?;

    let errs = match target {
        Some(t) => {
            let msgs = ds.iter().map(|d| (&d.data[..], d.properties.clone())).collect();
            let errs = publish_all(ch, t, msgs).await;
            // Antes del ack: si se corta acá, el respaldo dice "published" (posible
            // duplicado) y no "pending" (que significaría "sigue solo en origen").
            let published: Vec<_> = errs
                .iter()
                .enumerate()
                .filter(|(_, err)| err.is_none())
                .map(|(i, _)| (base + i as u32, "published", None))
                .collect();
            sync_io(|| w.status(published, None, true))?;
            errs
        }
        None => vec![None; ds.len()],
    };

    if !individual_acks && errs.iter().all(Option::is_none) {
        // Entregas consecutivas: un solo ack múltiple.
        let last = ds.last().map(|d| d.delivery_tag).unwrap_or_default();
        ch.basic_ack(last, BasicAckOptions { multiple: true }).await.map_err(e)?;
    } else {
        for (d, err) in ds.iter().zip(&errs) {
            let r = match err {
                None => ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await,
                Some(_) => {
                    let opts = BasicNackOptions { multiple: false, requeue: true };
                    ch.basic_nack(d.delivery_tag, opts).await
                }
            };
            r.map_err(e)?;
        }
    }

    let upd = errs
        .iter()
        .enumerate()
        .map(|(i, err)| match err {
            None => (base + i as u32, "done", None),
            Some(msg) => (base + i as u32, "error", Some(format!("{msg} (sigue en origen)"))),
        })
        .collect();
    // Al mover, "done" solo confirma un "published" ya durable: sin fsync. En un ack
    // sin destino es el único registro de que salió de la cola: con fsync.
    let sync = target.is_none();
    sync_io(|| w.status(upd, None, sync))?;
    Ok(errs)
}

/// Saca hasta `count` mensajes de `source` en lotes. Por lote: recibe (sin ack),
/// respalda con fsync, publica en `target` (si hay) con confirms + mandatory y
/// recién entonces ackea el origen. Cada ejecución crea su propio archivo de respaldo.
#[tauri::command]
async fn drain(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    source: String,
    target: Option<String>,
    count: u32,
    ctx: Option<RunCtx>,
) -> R<Value> {
    let ctx = ctx.unwrap_or_default();
    begin(&state);
    let op = if target.is_some() { "move" } else { "ack" };
    log::info!("{op} \"{source}\" → {target:?} count={count} {}", conn.desc());
    same_queue(&source, &target)?;
    let started = std::time::Instant::now();

    if let Some(t) = &target {
        // Publicar al default exchange una cola inexistente descarta en silencio.
        ready_count(&conn, &state, t).await?;
    }
    let total = ready_count(&conn, &state, &source).await?.min(count);
    if total == 0 {
        return Ok(json!({ "ok": 0, "total": 0, "failed": [], "cancelled": false, "stalled": false, "backup": null }));
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

    let (mut ok, mut failed, mut seen) = (0u32, Vec::<String>::new(), 0u32);
    let mut run: Option<(backup::Writer, String)> = None;
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
            if run.is_none() {
                run = Some(new_run(
                    &app,
                    RunInfo {
                        op,
                        source: &source,
                        target: &target,
                        conn: &conn,
                        ctx: &ctx,
                        origin: format!("queue:{source}"),
                        parent: None,
                    },
                )?);
            }
            let (w, _) = run.as_mut().unwrap();
            let refs: Vec<&Delivery> = ds.iter().collect();
            let errs = process_group(&ch, w, seen, &refs, &target, false).await?;
            seen += ds.len() as u32;
            for err in errs {
                match err {
                    None => ok += 1,
                    Some(msg) => failed.push(msg),
                }
            }
            if !failed.is_empty() {
                log::warn!("{op}: {} fallos de publish, primero: {}", failed.len(), failed[0]);
            }
            progress(&app, op, ok, total);
        }
        Ok(())
    }
    .await;

    // Lo prefetcheado y no procesado vuelve a la cola al cerrar el canal.
    let _ = ch.close(200, "ok").await;
    let was_cancelled = cancelled(&state);
    // Terminó sin llegar al total, sin cancelar ni fallar: la cola dejó de entregar.
    let stalled = result.is_ok() && !was_cancelled && failed.is_empty() && ok < total;
    log::info!(
        "{op} \"{source}\": {ok}/{total} ok, {} fallidos, cancelado={was_cancelled}, detenido={stalled}, {:?}{}",
        failed.len(),
        started.elapsed(),
        result.as_ref().err().map(|x| format!(", error: {x}")).unwrap_or_default()
    );
    if let Some((w, _)) = run.as_mut() {
        close_run(w, &result, was_cancelled, stalled, !failed.is_empty());
    }
    result?;
    Ok(json!({
        "ok": ok,
        "total": total,
        "failed": failed,
        "cancelled": was_cancelled,
        "stalled": stalled,
        "backup": run.map(|(_, id)| id),
    }))
}

/// Cierra un respaldo con su resultado (best-effort: el envío ya terminó).
fn close_run(w: &mut backup::Writer, result: &R<()>, cancelled: bool, stalled: bool, partial: bool) {
    let (outcome, error) = match result {
        Err(err) => ("error", Some(err.clone())),
        Ok(()) if cancelled => ("cancelled", None),
        Ok(()) if partial => ("partial", None),
        Ok(()) if stalled => ("stalled", None),
        Ok(()) => ("ok", None),
    };
    if let Err(err) = sync_io(|| w.finish(outcome, error, now_ms())) {
        log::warn!("no se pudo cerrar el respaldo {}: {err}", w.id());
    }
}

/// Mueve (o ackea, sin `target`) solo los mensajes elegidos en la UI. Recorre la
/// cola desde el principio hasta `scan` mensajes y los reconoce por su huella
/// (`fps`, con repeticiones si hay duplicados). Los no elegidos se quedan sin ack y
/// vuelven a la cola al cerrar el canal, en su orden.
#[tauri::command]
async fn move_selected(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    source: String,
    target: Option<String>,
    fps: Vec<String>,
    scan: u32,
    ctx: Option<RunCtx>,
) -> R<Value> {
    let ctx = ctx.unwrap_or_default();
    begin(&state);
    let op = if target.is_some() { "move" } else { "ack" };
    let wanted = fps.len() as u32;
    log::info!(
        "{op} seleccionados \"{source}\" → {target:?}: {wanted} elegidos, scan={scan} {}",
        conn.desc()
    );
    same_queue(&source, &target)?;
    if let Some(t) = &target {
        ready_count(&conn, &state, t).await?;
    }
    let scan = ready_count(&conn, &state, &source).await?.min(scan);
    if scan == 0 || wanted == 0 {
        return Ok(json!({ "ok": 0, "total": wanted, "not_found": wanted, "moved": [], "failed": [], "backup": null }));
    }

    let ch = channel(&state, &conn).await?;
    if target.is_some() {
        ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(e)?;
    }
    let mut reader = Reader::new(&ch, &source, scan);

    let mut want: std::collections::HashMap<String, u32> = Default::default();
    for fp in fps {
        *want.entry(fp).or_default() += 1;
    }
    let mut left = wanted;
    let (mut moved, mut failed) = (Vec::<String>::new(), Vec::<String>::new());
    let mut run: Option<(backup::Writer, String)> = None;
    progress(&app, op, 0, wanted);

    let result: R<()> = async {
        let (mut picked, mut picked_fp) = (Vec::<Delivery>::new(), Vec::<String>::new());
        // Lo recorrido y no elegido queda sin ack en el canal hasta cerrarlo, así
        // vuelve todo junto y en orden. No se guarda el contenido (pueden ser 200k).
        let mut seen = 0u32;
        while left > 0 {
            let Some(d) = reader.next().await? else { break };
            // Cancelar durante el recorrido no toca nada: todo vuelve a la cola.
            if cancelled(&state) {
                return Ok(());
            }
            seen += 1;
            if seen % 1000 == 0 {
                progress(&app, "scan", seen, scan);
            }
            let f = fingerprint(&d);
            match want.get_mut(&f) {
                Some(c) if *c > 0 => {
                    *c -= 1;
                    left -= 1;
                    picked.push(d);
                    picked_fp.push(f);
                }
                _ => {}
            }
        }
        log::info!(
            "{op} seleccionados: recorridos {seen}, encontrados {}/{wanted}",
            picked.len()
        );
        if picked.is_empty() {
            return Ok(());
        }
        let (w, _) = run.insert(new_run(
            &app,
            RunInfo {
                op,
                source: &source,
                target: &target,
                conn: &conn,
                ctx: &ctx,
                origin: format!("queue:{source}"),
                parent: None,
            },
        )?);
        for (i, group) in picked.chunks(CHUNK).enumerate() {
            // Entre lotes: lo ya procesado queda hecho; el resto vuelve a la cola.
            if cancelled(&state) {
                break;
            }
            let refs: Vec<&Delivery> = group.iter().collect();
            let base = i * CHUNK;
            let errs = process_group(&ch, w, base as u32, &refs, &target, true).await?;
            for (j, err) in errs.into_iter().enumerate() {
                match err {
                    None => moved.push(picked_fp[base + j].clone()),
                    Some(msg) => failed.push(msg),
                }
            }
            progress(&app, op, moved.len() as u32, wanted);
        }
        Ok(())
    }
    .await;

    // Cerrar el canal reencola todo lo que quedó sin ack (no elegidos y fallidos).
    let _ = ch.close(200, "ok").await;
    let was_cancelled = cancelled(&state);
    log::info!(
        "{op} seleccionados \"{source}\": {}/{wanted} ok, {} fallidos, {left} no encontrados, cancelado={was_cancelled}{}",
        moved.len(),
        failed.len(),
        result.as_ref().err().map(|x| format!(", error: {x}")).unwrap_or_default()
    );
    if let Some((w, _)) = run.as_mut() {
        close_run(w, &result, was_cancelled, false, !failed.is_empty() || (!was_cancelled && left > 0));
    }
    result?;
    Ok(json!({
        "ok": moved.len(),
        "total": wanted,
        "not_found": if was_cancelled { 0 } else { left },
        "moved": moved,
        "failed": failed,
        "cancelled": was_cancelled,
        "backup": run.map(|(_, id)| id),
    }))
}

/// Publica un grupo en `queue` dejando constancia en el respaldo: entradas
/// "pending" con fsync antes de publicar y "done"/"error" según el confirm.
/// Para envíos sin cola de origen (archivo, re-publicar): "pending" = no se sabe si
/// llegó (se cortó entre ambos pasos).
async fn publish_logged(
    ch: &Channel,
    w: &mut backup::Writer,
    queue: &str,
    entries: Vec<backup::Entry>,
    datas: &[Vec<u8>],
) -> R<Vec<Option<String>>> {
    let ns: Vec<u32> = entries.iter().map(|en| en.n).collect();
    let props: Vec<BasicProperties> = entries.iter().map(|en| en.properties.clone()).collect();
    sync_io(|| w.entries(entries))?;
    let msgs = datas.iter().zip(props).map(|(d, p)| (&d[..], p)).collect();
    let errs = publish_all(ch, queue, msgs).await;
    let upd = ns
        .iter()
        .zip(&errs)
        .map(|(&n, err)| match err {
            None => (n, "done", None),
            Some(msg) => (n, "error", Some(msg.clone())),
        })
        .collect();
    sync_io(|| w.status(upd, None, true))?;
    Ok(errs)
}

#[derive(Deserialize)]
struct PubMsg {
    payload: String,
    #[serde(default)]
    properties: Value,
}

/// Publica en `queue` mensajes cargados desde un archivo JSON, con confirms. Lo
/// enviado queda en un respaldo nuevo (op "publish", origen el archivo).
#[tauri::command]
async fn publish_messages(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    queue: String,
    messages: Vec<PubMsg>,
    file: Option<String>,
    ctx: Option<RunCtx>,
) -> R<Value> {
    let ctx = ctx.unwrap_or_default();
    begin(&state);
    let total = messages.len() as u32;
    let file = file.unwrap_or_else(|| "archivo".into());
    log::info!("publish_messages {file} → \"{queue}\": {total} mensajes {}", conn.desc());
    ready_count(&conn, &state, &queue).await?;
    let ch = channel(&state, &conn).await?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(e)?;
    let (mut ok, mut failed) = (0u32, Vec::<String>::new());
    // Índices publicados con confirmación: la UI los marca para no re-enviarlos.
    let mut published: Vec<u32> = Vec::new();
    let target = Some(queue.clone());
    let (mut w, backup_id) = new_run(
        &app,
        RunInfo {
            op: "publish",
            source: &file,
            target: &target,
            conn: &conn,
            ctx: &ctx,
            origin: format!("file:{file}"),
            parent: None,
        },
    )?;
    progress(&app, "publish", 0, total);
    let result: R<()> = async {
        for (gi, group) in messages.chunks(500).enumerate() {
            if cancelled(&state) {
                break;
            }
            let datas: Vec<Vec<u8>> = group.iter().map(|m| m.payload.clone().into_bytes()).collect();
            let entries = group
                .iter()
                .enumerate()
                .map(|(j, m)| backup::Entry {
                    n: (gi * 500 + j) as u32,
                    status: "pending".into(),
                    error: None,
                    routing_key: queue.clone(),
                    exchange: String::new(),
                    properties: props_from_json(&m.properties),
                    payload_b64: B64.encode(m.payload.as_bytes()),
                    republished_to: None,
                })
                .collect();
            for (j, err) in publish_logged(&ch, &mut w, &queue, entries, &datas).await?.into_iter().enumerate() {
                match err {
                    None => {
                        ok += 1;
                        published.push((gi * 500 + j) as u32);
                    }
                    Some(msg) => failed.push(msg),
                }
            }
            progress(&app, "publish", ok, total);
            if !failed.is_empty() {
                break;
            }
        }
        Ok(())
    }
    .await;
    let _ = ch.close(200, "ok").await;
    let was_cancelled = cancelled(&state);
    close_run(&mut w, &result, was_cancelled, false, !failed.is_empty());
    log::info!(
        "publish_messages → \"{queue}\": {ok}/{total} ok, {} fallidos, cancelado={was_cancelled}",
        failed.len()
    );
    result?;
    Ok(json!({ "ok": ok, "total": total, "failed": failed, "published": published, "cancelled": was_cancelled, "backup": backup_id }))
}

#[tauri::command]
fn cancel(state: State<'_, AppState>) {
    state.cancel.store(true, Ordering::SeqCst);
}

fn meta_json(m: &backup::Meta) -> Value {
    let h = &m.header;
    json!({
        "id": h.id, "ts": h.ts, "op": h.op, "source": h.source, "target": h.target,
        "count": m.total(), "done": m.count("done"), "error": m.count("error"),
        "pending": m.count("pending"), "published": m.count("published"),
        "republished": m.count("republished"),
        "bytes": m.bytes, "finished_ts": m.finished_ts, "outcome": m.outcome, "end_error": m.error,
        "profile": h.profile, "host": h.host, "vhost": h.vhost, "user": h.user,
        "app_version": h.app_version, "parent": h.parent, "origin": h.origin, "filter": h.filter,
    })
}

/// Respaldos disponibles (uno por ejecución), del más reciente al más viejo. Lee
/// solo los resúmenes: no abre los .bkp.jsonl (pueden pesar cientos de MB).
#[tauri::command]
async fn list_backups(app: AppHandle) -> R<Vec<Value>> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        let mut out = vec![];
        for id in backup::ids(&dir)?.into_iter().take(50) {
            match backup::read_meta(&dir, &id) {
                Ok(m) => out.push(meta_json(&m)),
                Err(err) => log::warn!("respaldo ilegible {id}: {err}"),
            }
        }
        Ok(out)
    })
    .await
}

/// Entradas de un respaldo (`id`), o del más reciente si no se indica.
#[tauri::command]
async fn load_backup(app: AppHandle, id: Option<String>) -> R<Value> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        let Some(id) = id.or_else(|| backup::ids(&dir).ok()?.into_iter().next()) else {
            return Ok(json!({ "id": null, "entries": [] }));
        };
        let run = backup::read_run(&dir, &id)?;
        let h = &run.header;
        let entries: Vec<Value> = run
            .entries
            .iter()
            .map(|en| {
                let raw = B64.decode(&en.payload_b64).unwrap_or_default();
                let (payload, binary) = payload_text(&raw);
                json!({
                    "batch": id, "n": en.n, "ts": h.ts, "op": h.op,
                    "source": h.source,
                    "target": en.republished_to.clone().or_else(|| h.target.clone()),
                    "republished_to": en.republished_to,
                    "status": en.status, "error": en.error,
                    "routing_key": en.routing_key, "exchange": en.exchange,
                    "size": raw.len(),
                    "properties": props_json(&en.properties),
                    "payload": payload, "binary": binary,
                })
            })
            .collect();
        Ok(json!({ "id": id, "meta": meta_json(&backup::meta_of(&run)), "entries": entries }))
    })
    .await
}

#[derive(Deserialize)]
struct EntryRef {
    batch: String,
    n: u32,
}

fn group_refs(items: Vec<EntryRef>) -> R<std::collections::BTreeMap<String, Vec<u32>>> {
    let mut by_run: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
    for it in items {
        backup::valid_id(&it.batch)?;
        by_run.entry(it.batch).or_default().push(it.n);
    }
    Ok(by_run)
}

/// Re-publica entradas de un respaldo a una cola, con sus properties originales.
/// Es una versión nueva: lo enviado queda en un respaldo propio (op "republish",
/// `parent` = el respaldo de origen). En el original se marca "republished" para
/// avisar de duplicados si se vuelve a enviar.
#[tauri::command]
async fn republish(
    app: AppHandle,
    state: State<'_, AppState>,
    conn: Conn,
    items: Vec<EntryRef>,
    queue: String,
    ctx: Option<RunCtx>,
) -> R<Value> {
    let ctx = ctx.unwrap_or_default();
    begin(&state);
    let dir = backup_dir(&app)?;
    let by_run = group_refs(items)?;
    ready_count(&conn, &state, &queue).await?;
    let ch = channel(&state, &conn).await?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(e)?;

    let total = by_run.values().map(Vec::len).sum::<usize>() as u32;
    let mut done = 0u32;
    log::info!("republish → \"{queue}\": {total} entradas");
    let mut version: Option<(backup::Writer, String)> = None;
    let result: R<()> = async {
        let mut next_n = 0u32;
        for (id, ns) in by_run {
            let (d2, id2) = (dir.clone(), id.clone());
            let run = blocking(move || backup::read_run(&d2, &id2)).await?;
            let mut orig = sync_io(|| backup::Writer::open(&dir, &run))?;
            if version.is_none() {
                let target = Some(queue.clone());
                version = Some(new_run(
                    &app,
                    RunInfo {
                        op: "republish",
                        source: &run.header.source,
                        target: &target,
                        conn: &conn,
                        ctx: &ctx,
                        origin: format!("backup:{id}"),
                        parent: Some(id.clone()),
                    },
                )?);
            }
            let (w, _) = version.as_mut().unwrap();
            let wanted: std::collections::HashSet<u32> = ns.into_iter().collect();
            let chosen: Vec<&backup::Entry> = run.entries.iter().filter(|en| wanted.contains(&en.n)).collect();
            for group in chosen.chunks(500) {
                if cancelled(&state) {
                    return Ok(());
                }
                let datas: Vec<Vec<u8>> = group
                    .iter()
                    .map(|en| B64.decode(&en.payload_b64).map_err(e))
                    .collect::<R<_>>()?;
                let copies: Vec<backup::Entry> = group
                    .iter()
                    .enumerate()
                    .map(|(j, en)| backup::Entry {
                        n: next_n + j as u32,
                        status: "pending".into(),
                        error: None,
                        republished_to: None,
                        ..(*en).clone()
                    })
                    .collect();
                next_n += group.len() as u32;
                let errs = publish_logged(&ch, w, &queue, copies, &datas).await?;
                let first_err = errs.iter().flatten().next().cloned();
                let upd: Vec<_> = group
                    .iter()
                    .zip(&errs)
                    .filter(|(_, err)| err.is_none())
                    .map(|(en, _)| (en.n, "republished", None))
                    .collect();
                done += upd.len() as u32;
                sync_io(|| orig.status(upd, Some(&queue), true))?;
                progress(&app, "republish", done, total);
                if let Some(err) = first_err {
                    return Err(err);
                }
            }
        }
        Ok(())
    }
    .await;
    let _ = ch.close(200, "ok").await;
    let was_cancelled = cancelled(&state);
    if let Some((w, _)) = version.as_mut() {
        close_run(w, &result, was_cancelled, false, false);
    }
    log::info!(
        "republish → \"{queue}\": {done}/{total}, cancelado={was_cancelled}{}",
        result.as_ref().err().map(|x| format!(", error: {x}")).unwrap_or_default()
    );
    result?;
    Ok(json!({ "ok": done, "total": total, "cancelled": was_cancelled, "backup": version.map(|(_, id)| id) }))
}

/// Borra entradas de respaldos; si un respaldo queda vacío se borra completo.
#[tauri::command]
async fn delete_local(app: AppHandle, items: Vec<EntryRef>) -> R<u32> {
    let dir = backup_dir(&app)?;
    let by_run = group_refs(items)?;
    blocking(move || {
        let mut removed = 0u32;
        for (id, ns) in by_run {
            let run = backup::read_run(&dir, &id)?;
            let ns: Vec<u32> =
                ns.into_iter().filter(|n| run.entries.iter().any(|en| en.n == *n)).collect();
            removed += ns.len() as u32;
            if ns.len() == run.entries.len() {
                backup::remove(&dir, &id)?;
            } else {
                backup::Writer::open(&dir, &run)?.deleted(&ns)?;
            }
        }
        log::info!("delete_local: {removed} entradas borradas");
        Ok(removed)
    })
    .await
}

/// Borra un respaldo completo.
#[tauri::command]
async fn delete_backup(app: AppHandle, id: String) -> R<()> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        backup::remove(&dir, &id)?;
        log::info!("respaldo {id} borrado completo");
        Ok(())
    })
    .await
}

/// Retención: borra respaldos más viejos que `days` días. Nunca borra el último
/// envío ni los que tienen entradas `pending`/`published` (su destino real no se
/// conoce: podrían ser los únicos que quedan de esos mensajes).
#[tauri::command]
async fn prune_backups(app: AppHandle, days: u32) -> R<Value> {
    let dir = backup_dir(&app)?;
    blocking(move || {
        let limit = now_ms().saturating_sub(days as u64 * 86_400_000);
        let (mut removed, mut kept_unresolved) = (0u32, 0u32);
        for (i, id) in backup::ids(&dir)?.into_iter().enumerate() {
            let Ok(meta) = backup::read_meta(&dir, &id) else { continue };
            if i == 0 || meta.header.ts >= limit {
                continue;
            }
            if meta.unresolved() {
                kept_unresolved += 1;
                continue;
            }
            match backup::remove(&dir, &id) {
                Ok(()) => removed += 1,
                Err(err) => log::warn!("retención: no se pudo borrar {id}: {err}"),
            }
        }
        if removed + kept_unresolved > 0 {
            log::info!("retención {days} días: {removed} respaldos borrados, {kept_unresolved} conservados por tener entradas sin resolver");
        }
        Ok(json!({ "removed": removed, "kept_unresolved": kept_unresolved }))
    })
    .await
}

#[tauri::command]
async fn backup_path(app: AppHandle) -> R<String> {
    Ok(backup_dir(&app)?.display().to_string())
}

/// Mensajes ready exactos por AMQP (declare pasivo). Las estadísticas de
/// management se refrescan cada ~5 s, así que justo después de mover van atrasadas.
/// `errors` dice por cola por qué no se pudo leer (no existe, sin permiso…).
#[tauri::command]
async fn queues_ready(state: State<'_, AppState>, conn: Conn, names: Vec<String>) -> R<Value> {
    let (mut ready, mut errors) = (Map::new(), Map::new());
    for name in names {
        match ready_count(&conn, &state, &name).await {
            Ok(n) => {
                ready.insert(name, json!(n));
            }
            // Una cola con problema no tiene que tirar abajo el resto.
            Err(err) => {
                log::warn!("queues_ready: {err}");
                errors.insert(name, json!(err));
            }
        }
    }
    Ok(json!({ "ready": ready, "errors": errors }))
}

// --- contraseñas en el almacén del sistema -----------------------------------------
// Windows: Administrador de credenciales. Así no quedan en texto plano en el
// localStorage del WebView.

const KEYRING_SERVICE: &str = "com.dyool.rabbit-tool";

fn keyring_entry(profile: &str) -> R<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, profile).map_err(e)
}

#[tauri::command]
async fn secret_set(profile: String, pass: String) -> R<()> {
    blocking(move || keyring_entry(&profile)?.set_password(&pass).map_err(e)).await
}

/// Contraseñas de varios perfiles; los que no tienen guardada no aparecen.
#[tauri::command]
async fn secret_get_all(profiles: Vec<String>) -> R<Map<String, Value>> {
    blocking(move || {
        let mut out = Map::new();
        for p in profiles {
            match keyring_entry(&p)?.get_password() {
                Ok(pass) => {
                    out.insert(p, json!(pass));
                }
                Err(keyring::Error::NoEntry) => {}
                Err(err) => log::warn!("no se pudo leer la contraseña de \"{p}\": {err}"),
            }
        }
        Ok(out)
    })
    .await
}

#[tauri::command]
async fn secret_delete(profile: String) -> R<()> {
    blocking(move || match keyring_entry(&profile)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(err) => Err(e(err)),
    })
    .await
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

const REPO_URL: &str = "https://github.com/Dyool-distelsa/rabbit-tool";

/// Versión y datos de compilación para "Acerca de".
#[tauri::command]
fn app_info(app: AppHandle) -> Value {
    json!({
        "version": app.package_info().version.to_string(),
        "commit": env!("BUILD_COMMIT"),
        "date": env!("BUILD_DATE"),
        "repo": REPO_URL,
    })
}

/// Abre en el navegador un enlace de la ayuda. Solo del repositorio de la app:
/// nunca una URL arbitraria que venga de la UI.
#[tauri::command]
fn open_url(url: String) -> R<()> {
    if !url.starts_with(REPO_URL) || url.contains(['"', ' ', '&', '|', '^']) {
        log::warn!("open_url rechazada: {url}");
        return Err("enlace no permitido".into());
    }
    #[cfg(target_os = "windows")]
    let r = std::process::Command::new("rundll32").args(["url.dll,FileProtocolHandler", &url]).spawn();
    #[cfg(target_os = "macos")]
    let r = std::process::Command::new("open").arg(&url).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let r = std::process::Command::new("xdg-open").arg(&url).spawn();
    r.map(|_| ()).map_err(e)
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
            list_backups,
            load_backup,
            move_selected,
            publish_messages,
            republish,
            delete_local,
            backup_path,
            delete_backup,
            prune_backups,
            secret_set,
            secret_get_all,
            secret_delete,
            queues_ready,
            disconnect,
            log_ui,
            log_path,
            reveal_log,
            app_info,
            open_url
        ])
        .run(tauri::generate_context!())
        .expect("error al iniciar tauri");
}
