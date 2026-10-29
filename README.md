# Rabbit Tool

Herramienta de escritorio liviana (Tauri + Rust) para administrar mensajes de RabbitMQ: ver, ackear, mover entre colas sin perderlos y volver a cargarlos.

## Qué hace

- **Colas paginadas**: se cargan a mano, de a 5/10/15/20, con las que tienen mensajes primero (o A–Z) y filtro por nombre o `/regex/` que aplica el broker. Nada se trae solo.
- **Ver (nack)**: lee páginas de mensajes por AMQP (sin tope: 200k o más) y los devuelve a la cola. Lista virtual con detalle de JSON formateado, properties y headers.
- **Filtro local**: texto, `/regex/i`, `campo=valor` (también `"DESTINO": 64000000` pegado del JSON, en cualquier nivel o por ruta `pedido.destino`), `!excluir`. Al publicar sin selección se envían solo los filtrados.
- **Ack / Mover**: los primeros N o solo los **seleccionados**. Cada lote se respalda en disco (fsync) antes de cualquier ack, se publica con confirmación del broker (`mandatory`) y recién entonces se ackea en origen. Si algo falla, el mensaje vuelve a la cola.
- **Mover seleccionados**: reconoce los mensajes elegidos por una huella de su contenido; los no elegidos vuelven a la cola en su orden.
- **Descargar / exportar JSON**: `Descargar JSON…` guarda N mensajes (o toda la cola) directo en un `.json` editable, sin ack (vuelven a la cola), sin tope. `Exportar JSON…` guarda los seleccionados o filtrados de cualquier pestaña (cola, archivo o respaldo). El payload se copia tal cual (sin tocar números ni el orden de los campos); opcionalmente con properties y headers.
- **Archivo JSON**: carga un array de mensajes desde un `.json` (por ejemplo uno descargado y editado), se filtra y se publica en una cola.
- **Respaldos versionados**: cada envío (mover, ack, publicar desde archivo, re-publicar) guarda su propio respaldo (`.bkp.jsonl`) con metadata (perfil, host, vhost, usuario, origen, filtro, duración, resultado). Re-publicar desde un respaldo crea una versión nueva que apunta a la original. Por defecto se guardan siempre; la retención (7/30/90 días) es opcional.
- **Confirmación por ambiente**: PROD (rojo, hay que escribir el nombre de la cola), QA (naranja) y DEV (celeste), detectados por el nombre de la cola.
- **Actividad**: abajo a la izquierda, las últimas 200 operaciones (nack, ack, traslado, carga, re-publicar, descarga, exportar, borrado) con hora, cantidad, colas, perfil y resultado; el borde marca el ambiente y tocarla abre su respaldo. Se guarda en disco (`activity.json`) y persiste entre sesiones, separando las de sesiones anteriores. Las más viejas se descartan; el historial completo queda en el log rotativo.
- **Ambiente siempre a la vista**: el conejo se viste según el ambiente del perfil (DEV gorra con hélice, QA lentes y lupa, PROD sirena de policía, ojos grandes y sudor; en PROD se tapa los ojos al pasar por acciones que sacan, publican o borran). Además: aura en el logo, etiqueta junto al perfil, franja con huellitas (cinta de precaución en PROD), barras de scroll del color del ambiente, y título de ventana y punto de color en el icono de la barra de tareas. El ambiente se elige en el perfil o, en Auto, se deduce del nombre, vhost o host.
- **Paneles ajustables**: ancho de la barra izquierda, alto de la actividad y ancho del detalle se arrastran (o se mueven con las flechas); doble clic restablece. Se recuerdan entre sesiones.
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

Los perfiles se crean con **+** junto al selector; el lápiz edita el elegido (renombrar, **Duplicar** como base de uno nuevo, o borrar). Se guardan en el equipo; la contraseña va al Administrador de credenciales del sistema.

## Archivos locales

- Respaldos: `%APPDATA%\com.dyool.rabbit-tool\backup\` (un `.bkp.jsonl` por envío).
- Actividad: `%APPDATA%\com.dyool.rabbit-tool\activity.json`.
- Log: `%LOCALAPPDATA%\com.dyool.rabbit-tool\logs\rabbit-tool.log`.

## Desarrollo

Requiere Node 24+ y Rust estable.

```sh
npm install
npm run dev      # app con recarga
npm run build    # instaladores en src-tauri/target/release/bundle
```

Las releases las arma GitHub Actions al pushear un tag `vX.Y.Z` que coincida con la versión de `package.json`, `src-tauri/Cargo.toml` y `src-tauri/tauri.conf.json`.
