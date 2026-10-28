# Rabbit Tool

Herramienta de escritorio liviana (Tauri + Rust) para administrar mensajes de RabbitMQ: ver, ackear, mover entre colas sin perderlos y volver a cargarlos.

## Qué hace

- **Colas paginadas**: se cargan a mano, de a 5/10/15/20, con las que tienen mensajes primero (o A–Z) y filtro por nombre o `/regex/` que aplica el broker. Nada se trae solo.
- **Ver (nack)**: lee páginas de mensajes por AMQP (sin tope: 200k o más) y los devuelve a la cola. Lista virtual con filtro local (texto, `/regex/i`, `!excluir`) y detalle con JSON formateado, properties y headers.
- **Ack / Mover**: los primeros N o solo los **seleccionados**. Cada lote se respalda en disco (fsync) antes de cualquier ack, se publica con confirmación del broker (`mandatory`) y recién entonces se ackea en origen. Si algo falla, el mensaje vuelve a la cola.
- **Mover seleccionados**: reconoce los mensajes elegidos por una huella de su contenido; los no elegidos vuelven a la cola en su orden.
- **Archivo JSON**: carga un array de mensajes desde un `.json`, se filtra y se publica en una cola.
- **Respaldos versionados**: cada envío (mover, ack, publicar desde archivo, re-publicar) guarda su propio respaldo (`.bkp.jsonl`) con metadata (perfil, host, vhost, usuario, origen, filtro, duración, resultado). Re-publicar desde un respaldo crea una versión nueva que apunta a la original. Retención configurable.
- **Confirmación por ambiente**: PROD (rojo, hay que escribir el nombre de la cola), QA (naranja) y DEV (celeste), detectados por el nombre de la cola.
- **Ayuda** (F1) con cómo funciona, estados del respaldo, atajos y archivos locales.
- **Log** rotativo con el detalle de cada operación y error (botón "Ver log").
- **Contraseñas** en el Administrador de credenciales del sistema, no en texto plano.

## Instalar

Bajá el instalador de la [última release](../../releases/latest):

- **Windows**: `.msi` o `-setup.exe`.
- **Linux**: `.deb`, `.rpm` o `.AppImage`.

## Conexión

| Campo | Ejemplo | Nota |
| --- | --- | --- |
| Management API | `http://host:15672` | Lista colas y valida usuario/vhost. No requiere el tag `administrator`. |
| AMQP | `amqp://host:5672` | Para leer, mover y publicar. **No** es el 15672. `amqps://` para TLS. |
| VHost | `/` | |

Los perfiles se guardan en el equipo; la contraseña va al Administrador de credenciales del sistema.

## Archivos locales

- Respaldos: `%APPDATA%\com.dyool.rabbit-tool\backup\` (un `.bkp.jsonl` por envío).
- Log: `%LOCALAPPDATA%\com.dyool.rabbit-tool\logs\rabbit-tool.log`.

## Desarrollo

Requiere Node 24+ y Rust estable.

```sh
npm install
npm run dev      # app con recarga
npm run build    # instaladores en src-tauri/target/release/bundle
```

Las releases las arma GitHub Actions al pushear un tag `vX.Y.Z` que coincida con la versión de `package.json`, `src-tauri/Cargo.toml` y `src-tauri/tauri.conf.json`.
