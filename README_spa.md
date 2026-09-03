<p align="center">
  <img src="images/HYDRA_UMC_BANNER.svg" alt="HYDRA-UMC-TELEMETRY-COLLECTOR banner" width="100%">
</p>

# 📡 HYDRA-UMC-TELEMETRY-COLLECTOR

<p align="center"><a href="README.md">🇺🇸 English</a> | 🇪🇸 <b>Español</b> | <a href="README_fra.md">🇫🇷 Français</a> | <a href="README_ita.md">🇮🇹 Italiano</a> | <a href="README_deu.md">🇩🇪 Deutsch</a> | <a href="README_zho.md">🇨🇳 简体中文</a> | <a href="README_jpn.md">🇯🇵 日本語</a></p>

### 🚀 Nodo de Ingesta de Alto Rendimiento para Logs de CAN y WebSocket

<p align="left">
  <img src="https://img.shields.io/badge/Licencia-GPL%203.0-blue.svg" alt="GPL 3.0">
  <img src="https://img.shields.io/badge/Lenguaje-Go%20%2F%20Rust-orange.svg" alt="Go/Rust">
  <img src="https://img.shields.io/badge/Protocolo-CAN%20%2F%20gRPC%20%2F%20WS-yellow.svg" alt="Protocol">
</p>

---

## 1. 🛠️ VISIÓN GENERAL TÉCNICA

**HYDRA-UMC-TELEMETRY-COLLECTOR** es la puerta de enlace de alta velocidad que captura toda la comunicación bruta dentro del ecosistema. Escucha los buses FDCAN, los flujos WebSocket y las actualizaciones gRPC, canalizando los datos hacia el Datalake.

Realiza el parseo y la normalización en tiempo real de fuentes de datos heterogéneas, asegurando que un pico de corriente de motor en un bus CAN esté correctamente correlacionado con un resultado de inferencia de IA de un Nodo Vision.

### Características Clave:
* 🚀 **Ingesta Multi-Protocolo:** Maneja telemetría CAN, WebSocket, gRPC y HTTP.
* ⚡ **Alto Rendimiento:** Optimizado para miles de mensajes por milisegundo con un overhead de CPU mínimo.
* 🧬 **Normalización de Datos:** Traduce paquetes binarios brutos a formatos estandarizados JSON/Protobuf.
* 🛡️ **Entrega Bufferizada:** Asegura la pérdida de datos cero durante fallos temporales de base de datos o picos de red.
* 🔁 **Deduplicación Segura ante Reconexión:** Un número de secuencia opcional por productor, rastreado en una ventana de reordenamiento acotada, para que un dispositivo que se reconecta y reenvía sus últimos mensajes sin confirmar nunca infle los conteos de ingesta. *(implementado)*
* 🩺 **Diagnóstico Real de Fallos:** Cada fallo de flush se clasifica como un rechazo genuino de los datos por parte del sink frente a un problema de transporte - expuesto en `GET /stats` para una visibilidad operativa real. *(implementado)*
* 🧮 **Validación de Campos Finitos:** Un nombre de campo vacío o un valor numérico `NaN`/`Infinity`/`-Infinity` se rechaza (`400`) antes de llegar al buffer o a un sink - la decodificación CAN pasa por el mismo `Sample.Validate()` que la ingesta JSON. *(implementado)*

---

## 2. 🔄 FLUJO DE TRABAJO DE INGESTA

```mermaid
flowchart LR
    CAN["Tráfico del Bus CAN"] --> COLL["TELEMETRY-COLLECTOR"]
    WS["Flujos WS / gRPC"] --> COLL
    COLL --> PARSE["Parser de Paquetes y Norm"]
    PARSE --> BUF["Buffer de Alta Velocidad"]
    BUF --> LAKE["HYDRA-UMC-DATALAKE"]
```

---

## 3. 🧱 ARQUITECTURA Y DECISIONES DE DISEÑO

* **Por qué `src/` es la raíz del módulo Go, no la raíz del repo.** Mantiene los propios archivos del módulo instalable (`main.go`, `version.go`, `go.mod`) separados de las herramientas de la raíz del repo (`bump_version.py`, `docker-compose.yml`) - `go build ./...` se ejecuta desde dentro de `src/`, no desde la raíz del repo.
* **Por qué la recolección está separada del propio HYDRA-UMC-DATALAKE.** La recolección (sondear HYDRA-UMC-SERVER, hacer buffer, agrupar escrituras) es una preocupación ligada a E/S distinta del almacenamiento/consulta - mantenerla como proceso separado significa que un reinicio del recolector o un pico de contrapresión no toca el propio camino de consulta del data lake.
* **Por qué un fallo de escritura en el sink reencola el lote en vez de descartarlo.** `src/collector` es lo que de verdad respalda la promesa de "Entrega con Buffer: cero pérdida de datos": `FlushOnce` solo elimina un lote del buffer una vez que `Sink.Write` lo confirma, y lo devuelve directamente al frente en caso de fallo - un corte real reintenta las mismas muestras, no las pierde. El buffer (`src/buffer`) sigue siendo acotado, eso sí - un corte que dure más que su capacidad SÍ descarta el excedente más antiguo, un límite real y honesto en vez de prometer memoria infinita.
* **Por qué CAN y WebSocket se parsean ambos al mismo formato `Sample`.** `src/telemetry` normaliza ambas fuentes heterogéneas en una sola estructura antes de que nada toque el buffer o el sink - el mecanismo real detrás de "un pico de corriente de motor en CAN correlacionado correctamente con un resultado de un Vision Node": ninguna etapa posterior necesita saber por qué protocolo llegó una muestra.
* **Por qué el formato de trama CAN es una convención propia v0 de este proyecto, no todavía los IDs CAN reales del ecosistema.** Las tablas reales de IDs CAN viven en la propia documentación de firmware de HYDRA-UMC y URTC - integrarse contra ellas de verdad es trabajo futuro, no algo para adivinar sin esa referencia delante.
* **Por qué `DatalakeSink` escribe una muestra por petición HTTP, y por qué un fallo parcial de lote puede duplicar filas al reintentar.** El propio `POST /ingest` de HYDRA-UMC-DATALAKE (ver `src/hydra_umc_datalake/api.py` de ese proyecto) es de una sola muestra, no por lotes - una "escritura por lotes" aquí en realidad son N peticiones reales. Si una falla a mitad de un lote, `Write` devuelve un error y la propia lógica de reintento de `collector.go` reencola el lote COMPLETO, así que las muestras ya escritas se reenvían y terminan como filas duplicadas en DATALAKE en el siguiente flush exitoso. Al-menos-una-vez con duplicados ocasionales ante un corte real - en vez de perder datos en silencio (a-lo-sumo-una-vez) - es el compromiso honesto de esta v0; la entrega real exactamente-una-vez (claves de idempotencia, upserts) es trabajo futuro, no intentada aquí. `ConsoleSink` (imprime a stdout) sigue siendo el valor por defecto cuando no se pasa `-datalake-url`, para ejecutar este collector de forma independiente.
* **Cómo encaja en el resto del ecosistema.** Un servicio hermano bajo HYDRA-UMC-DATALAKE - el componente que realmente contacta con HYDRA-UMC-SERVER para obtener telemetría por robot y la escribe en el almacén de series temporales compartido.
* **Por qué la deduplicación es un paquete `dedup` separado, indexado por un `Sequence` opcional, no un hash del propio contenido de la muestra.** Una reconexión real reenvía los mismos bytes exactos, así que el hash de contenido funcionaría para ese caso - pero también se tragaría en silencio dos muestras genuinamente distintas que por casualidad comparten todos los campos (p. ej. dos lecturas de `0.0` con un segundo de diferencia). Un número de secuencia por productor es lo que un dispositivo real ya tiene que llevar para rastrear sus propios mensajes sin confirmar, así que reutilizarlo es la señal honesta y real - no una suposición derivada de datos que en realidad no prometen unicidad. `Sequence == 0`/omitido excluye por completo a un productor, así que nada del comportamiento preexistente cambia para un dispositivo que no envía uno.
* **Por qué `sink.InvalidDataError` no cambia la política de reintentos, solo la hace diagnosticable.** El reencolado-y-reintento todo-o-nada de `collector.go` (ver arriba) se mantiene exactamente igual - una muestra permanentemente inválida se sigue reintentando como cualquier otra, lo cual es en sí mismo una limitación conocida y documentada. Lo nuevo es visibilidad real: `invalidDataErrors` frente a `transportErrors` en `GET /stats` le permite a un operador distinguir "DATALAKE está rechazando nuestros datos" de "la red hacia DATALAKE está caída" sin tener que adivinar a partir de los logs.

---

## 📂 ESTRUCTURA DE DIRECTORIOS

Servicio de software puro (nodo de ingesta) - sin hardware, firmware ni sistema operativo propios; esas carpetas se omiten por política de estructura del repositorio.

```text
HYDRA-UMC-TELEMETRY-COLLECTOR/
├── src/                  # Módulo Go
│   ├── go.mod            # Definición del módulo
│   ├── version.go        # const Version = "X.Y.Z"
│   ├── main.go           # Punto de entrada: conecta todo, arranca la API HTTP
│   ├── telemetry/        # Tipo Sample + parsers CAN/WebSocket (normalizacion)
│   ├── buffer/           # FIFO acotado con reporte de contrapresion (Ring)
│   ├── dedup/            # Deduplicación real por secuencia por productor (ventana de reordenamiento)
│   ├── collector/        # Orquesta ingesta+flush, reintenta si falla el sink, dedup
│   ├── sink/              # A donde van los lotes vaciados (ConsoleSink hoy), clasificación transporte/datos inválidos
│   └── api/                # Handlers JSON/HTTP planos que envuelven el collector
├── docs/
│   └── API.md              # Referencia real de endpoints HTTP (peticiones, respuestas, codigos de estado)
├── images/               # Medios y diagramas
├── systemd/
│   └── hydra-umc-telemetry-collector.service # Unidad systemd de la API local de ingesta de telemetría en la CM5
├── tools/
│   ├── build_test.py     # Comprobación de compilación sin versionado
│   └── ci_validate.py    # Validación de manifiesto/CHANGELOG/docs usada por CI
├── build/                # Binarios compilados (ignorado por git)
├── bump_version.py        # Incremento de versión nativa tipo cuentakilómetros (lo ejecuta el build)
├── bump_manifest_version.py # Sincroniza la versión de hydra-umc.project.json con la nativa (--sync)
├── build.sh / build.bat   # Build real: bump + go build
├── run.sh / run.bat       # Ejecución real: corre el binario compilado
└── README.md
```

Podado de la plantilla original: `hardware/`, `firmware/` y `os/` — es un
servicio de software puro (binario Go) sin hardware ni firmware propios y
sin imagen de sistema operativo que mantener. Ver [`docs/API.md`](docs/API.md)
para la referencia completa de endpoints HTTP.

---

## 4. ⚙️ BUILD Y EJECUCIÓN

Requiere Go >= 1.21. Un pipeline de ingesta real con API HTTP, no solo un
esqueleto que compila.

```bash
# Linux/macOS
./build.sh
./run.sh -addr :8092 -datalake-url http://localhost:8095

# Windows
build.bat
run.bat -addr :8092 -datalake-url http://localhost:8095
```

`build` incrementa la versión (`src/version.go`) y compila el módulo Go de
`src/` a `build/telemetry-collector(.exe)`. `run` ejecuta el binario
compilado, reenviando cualquier flag, y empieza a escuchar tráfico real.
`-datalake-url` apunta los lotes descargados a una instancia real y en
ejecución de HYDRA-UMC-DATALAKE (`POST /ingest`, `sink.DatalakeSink`) -
si se omite, imprime las muestras descargadas a stdout en su lugar
(`sink.ConsoleSink`), útil para ejecutar este collector de forma
independiente.

```bash
# Ingerir una muestra de telemetría estilo WebSocket
curl -X POST localhost:8092/ingest/ws \
  -d '{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":42.5}}'

# Ingerir una trama CAN (8 bytes, base64 - ver src/telemetry/can.go para el formato)
curl -X POST localhost:8092/ingest/can \
  -d '{"arbitrationId":7,"data":"AQAAUEEAAAA="}'

# Ver qué ha ingerido/vaciado/descartado el collector
curl localhost:8092/stats
```

```bash
cd src && go test ./...   # telemetry (round-trip CAN, validacion WS),
                           # buffer (FIFO acotado, contrapresion, requeue),
                           # collector (el comportamiento real de "no
                           # perder datos ante un corte del sink"), y api
                           # (round-trips HTTP reales via httptest)
```

---

## 🚀 HOJA DE RUTA
* **Fase 1:** Ingesta de alto rendimiento e indexación del Datalake para análisis histórico.
* **Fase 2:** Compresión en el borde del colector de telemetría y protocolos de transmisión seguros.
* **Fase 3:** Detección de anomalías mediante aprendizaje no supervisado y análisis de vibración de motores.
* **Fase 4:** Compresión en tiempo real para ingesta de logs masivos y optimización multi-protocolo.

---

## 🔗 Proyectos Relacionados

Este proyecto es parte del ecosistema de robótica HYDRA-UMC del mismo autor (JuanenRac / Electro Hobby 3D). Vale la pena conocerlo, ya que una petición podría en realidad ser sobre alguno de estos en vez de sobre este repositorio.

**Proyecto Padre**
- **[HYDRA-UMC-DATALAKE](https://github.com/JuanenRac/HYDRA-UMC-DATALAKE)** — almacén de series temporales real respaldado por sqlite3, con una API HTTP real de ingesta/consulta; el padre del que este repositorio es un servicio de analítica específico, dentro de su propia capa de datos y analítica.

**Proyectos Hermanos** — los demás servicios de analítica de la propia capa de datos y analítica de HYDRA-UMC-DATALAKE
- **[HYDRA-UMC-ANOMALY-DETECTOR](https://github.com/JuanenRac/HYDRA-UMC-ANOMALY-DETECTOR)** — detector de anomalías real basado en FFT + línea base estadística, con monitorización de deriva.
- **[HYDRA-UMC-PRODUCTION-REPORTS](https://github.com/JuanenRac/HYDRA-UMC-PRODUCTION-REPORTS)** — cálculo real de OEE/disponibilidad sobre el histórico de DATALAKE, con exportación CSV reproducible.

**Directamente Relacionados**
- **[HYDRA-UMC-SERVER](https://github.com/JuanenRac/HYDRA-UMC-SERVER)** — el backend headless real (REST/WebSocket) con el que habla de verdad cada cliente de control — la fuente de los registros que ingiere este proyecto.
- **[HYDRA-UMC](https://github.com/JuanenRac/HYDRA-UMC)** — la placa madre física del brazo robótico: host CM5 + coprocesador STM32H745 de doble núcleo, coordinando hasta 8 brazos herramienta por CAN-OTA/SPI-OTA — la tabla real de IDs CAN contra la que el propio formato CAN de este proyecto está pensado para integrarse con el tiempo; hoy usa su propia convención v0, seguida honestamente como trabajo futuro en vez de darse por hecho.
- **[URTC](https://github.com/JuanenRac/URTC)** — firmware para la placa física del Universal Robot Tool Controller, más de 25 perfiles de herramienta por bus CAN — la tabla real de IDs CAN contra la que el propio formato CAN de este proyecto está pensado para integrarse con el tiempo; hoy usa su propia convención v0, seguida honestamente como trabajo futuro en vez de darse por hecho.

**También Forma Parte del Ecosistema**

*Hardware y Plataforma Base*
- **[HYDRA-UMC-OS](https://github.com/JuanenRac/HYDRA-UMC-OS)** — capa de producto reproducible sobre Raspberry Pi OS para el CM5: agente de solo lectura, config/perfiles validados, aprovisionamiento WiFi de primer contacto.
- **[HYDRA-UMC-SDK](https://github.com/JuanenRac/HYDRA-UMC-SDK)** — el contrato JSON-Schema compartido y la barrera de seguridad contra la que cada bridge valida sus comandos.

*Backend Central y Clientes*
- **[HYDRA-UMC-STUDIO](https://github.com/JuanenRac/HYDRA-UMC-STUDIO)** — panel de control web con visualización 3D multi-robot en tiempo real.
- **[HYDRA-UMC-SUITE](https://github.com/JuanenRac/HYDRA-UMC-SUITE)** — centro de mando de enjambre de escritorio (PySide6) para varios servidores a la vez, empaquetado como ejecutable independiente.
- **[HYDRA-UMC-ANDROID-CONTROL](https://github.com/JuanenRac/HYDRA-UMC-ANDROID-CONTROL)** — app nativa de control para Android con inicio de sesión biométrico y un compañero Wear OS emparejado.
- **[HYDRA-UMC-IOS-CONTROL](https://github.com/JuanenRac/HYDRA-UMC-IOS-CONTROL)** — app de control para iOS/iPadOS (Flutter) con sincronización en tiempo real por WebSocket.
- **[HYDRA-UMC-DSI](https://github.com/JuanenRac/HYDRA-UMC-DSI)** — interfaz táctil nativa para la pantalla táctil DSI de 7" a bordo, embebida en el propio CM5.
- **[HYDRA-UMC-EDITOR-URDF](https://github.com/JuanenRac/HYDRA-UMC-EDITOR-URDF)** — creador/editor gráfico de URDF de escritorio que envía los modelos terminados al propio catálogo de STUDIO.
- **[HYDRA-UMC-BRIDGE-AMR](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-AMR)** — barrera de coordinación para flotas AGV/AMR mediante un publicador MQTT VDA 5050 real.
- **[HYDRA-UMC-BRIDGE-CNC](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-CNC)** — coordinador de alto nivel para celdas CNC con acceso real a estado/bytes de control GRBL.
- **[HYDRA-UMC-BRIDGE-DROIDS](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-DROIDS)** — barrera de coordinación para droides con patas/humanoides, con un emisor de comandos real para Boston Dynamics Spot.
- **[HYDRA-UMC-BRIDGE-LASER](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-LASER)** — coordinador de seguridad para celdas láser que lee 3 salvaguardas GPIO reales de llave/carcasa/enclavamiento.
- **[HYDRA-UMC-BRIDGE-OPENPNP](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-OPENPNP)** — coordinador de alto nivel seguro para el flujo de placas de pick-and-place OpenPnP.
- **[HYDRA-UMC-BRIDGE-PRINTER3D](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-PRINTER3D)** — barrera de coordinación segura para impresoras 3D Moonraker/Klipper, con comandos de trabajo reales y controlados.
- **[HYDRA-UMC-BRIDGE-ROS2](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-ROS2)** — coordinador de seguridad con un transporte ROS 2 rclpy real, importado de forma perezosa.
- **[HYDRA-UMC-BRIDGE-UAV](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-UAV)** — barrera de coordinación para UAV equipados con cámara, con un emisor de comandos MAVLink real.

*Plataforma de Herramientas URTC*
- **[URTC-FLASHER](https://github.com/JuanenRac/URTC-FLASHER)** — herramienta de escritorio con GUI para flashear placas URTC, CAN-OTA más SWD/JTAG de chip completo.
- **[URTC-TESTER](https://github.com/JuanenRac/URTC-TESTER)** — herramienta de escritorio de diagnóstico CAN-bus en vivo para placas URTC, un panel por perfil de herramienta.
- **[URTC-WEB-STUDIO](https://github.com/JuanenRac/URTC-WEB-STUDIO)** — alternativa basada en navegador a URTC-TESTER mediante la Web Serial API, sin instalación local.

*Nodo IA de Visión (Hailo-8)*
- **[HYDRA-UMC-VISION-NODE](https://github.com/JuanenRac/HYDRA-UMC-VISION-NODE)** — nodo de integración para el pipeline de visión Hailo-8, con una comprobación real de disponibilidad de hardware por etapa.
- **[HYDRA-UMC-DETECTION-HEF](https://github.com/JuanenRac/HYDRA-UMC-DETECTION-HEF)** — registro real de modelos compilados con verificación de carga segura por arquitectura Hailo/checksum.
- **[HYDRA-UMC-VISION-STREAMER](https://github.com/JuanenRac/HYDRA-UMC-VISION-STREAMER)** — generador real de pipeline GStreamer + config MediaMTX, con una frontera de integración HailoRT real.
- **[HYDRA-UMC-VISUAL-SERVOING-API](https://github.com/JuanenRac/HYDRA-UMC-VISUAL-SERVOING-API)** — ley de corrección real de Position-Based Visual Servoing, con puerta de seguridad según el estado de zona previo.
- **[HYDRA-UMC-SAFETY-ZONES](https://github.com/JuanenRac/HYDRA-UMC-SAFETY-ZONES)** — comprobación real de invasión de zona y solicitud de E-STOP, con exigencia de vigencia de calibración.

*Nodo IA Cognitivo (Hailo-10)*
- **[HYDRA-UMC-COGNITIVE-NODE](https://github.com/JuanenRac/HYDRA-UMC-COGNITIVE-NODE)** — nodo de integración para el pipeline cognitivo Hailo-10 (orquestación de LLM/VLA/voz).
- **[HYDRA-UMC-VLA-ENGINE](https://github.com/JuanenRac/HYDRA-UMC-VLA-ENGINE)** — codificación/decodificación real de tokens de acción y generación de trayectoria para un modelo Vision-Language-Action.
- **[HYDRA-UMC-VOICE-UI](https://github.com/JuanenRac/HYDRA-UMC-VOICE-UI)** — front-end de voz real (VAD + analizador de intención) con un relé a Watch acotado y con confirmación.
- **[HYDRA-UMC-SEMANTIC-PLANNER](https://github.com/JuanenRac/HYDRA-UMC-SEMANTIC-PLANNER)** — descomposición real de tareas basada en reglas y recuperación semántica de errores sobre códigos de error del MCU.
- **[HYDRA-UMC-DOCS-QA](https://github.com/JuanenRac/HYDRA-UMC-DOCS-QA)** — búsqueda real de documentos TF-IDF (solo librería estándar) sobre los propios documentos Markdown de este ecosistema.

*Orquestación y Enjambre*
- **[HYDRA-UMC-ORCHESTRATOR](https://github.com/JuanenRac/HYDRA-UMC-ORCHESTRATOR)** — nodo de integración con un contrato real de informe de salud gRPC/Protobuf y una máquina de estados de misión.
- **[HYDRA-UMC-JOB-DISPATCHER](https://github.com/JuanenRac/HYDRA-UMC-JOB-DISPATCHER)** — cola de trabajos real basada en prioridad con deduplicación, sobre una API HTTP real.
- **[HYDRA-UMC-NODE-HEALING](https://github.com/JuanenRac/HYDRA-UMC-NODE-HEALING)** — watchdog de salud de flota real basado en gRPC, con reintento/backoff y detección de discrepancia de identidad.
- **[HYDRA-UMC-PATH-PLANNER-3D](https://github.com/JuanenRac/HYDRA-UMC-PATH-PLANNER-3D)** — planificador de rutas 3D real basado en RRT, con validación real de colisión de obstáculos/espacio de trabajo.
- **[HYDRA-UMC-SWARM-SYNC](https://github.com/JuanenRac/HYDRA-UMC-SWARM-SYNC)** — sincronización de estado real mediante CRDT LWW-Element-Map, con pruebas de propiedades para convergencia multi-celda.

*Gemelo Digital y Simulación*
- **[HYDRA-UMC-TWIN](https://github.com/JuanenRac/HYDRA-UMC-TWIN)** — nodo de integración para el motor de gemelo digital, con un contrato real de sincronización por compatibilidad de versión.
- **[HYDRA-UMC-HIL-BRIDGE](https://github.com/JuanenRac/HYDRA-UMC-HIL-BRIDGE)** — enclavamiento de seguridad real hardware-in-the-loop que enruta comandos entre simulación y hardware real.
- **[HYDRA-UMC-PHYSICS-REPLICA](https://github.com/JuanenRac/HYDRA-UMC-PHYSICS-REPLICA)** — cinemática directa real y validación de límites articulares sobre un subconjunto real de URDF.
- **[HYDRA-UMC-SYNTHETIC-DATA-GEN](https://github.com/JuanenRac/HYDRA-UMC-SYNTHETIC-DATA-GEN)** — generador real de escenas 2D procedurales con exportación de anotaciones YOLO/COCO.

*Pasarela Industrial*
- **[HYDRA-UMC-GATEWAY-INDUSTRIAL](https://github.com/JuanenRac/HYDRA-UMC-GATEWAY-INDUSTRIAL)** — nodo de integración que retransmite a protocolos industriales, con una capa real de lista blanca de comandos/contrapresión.
- **[HYDRA-UMC-OPCUA-SERVER](https://github.com/JuanenRac/HYDRA-UMC-OPCUA-SERVER)** — espacio de direcciones OPC-UA real, verificado con una sesión de cliente real del protocolo binario.
- **[HYDRA-UMC-MQTT-BROKER](https://github.com/JuanenRac/HYDRA-UMC-MQTT-BROKER)** — broker MQTT real con autenticación por cliente opcional y ACL de tópicos.
- **[HYDRA-UMC-MTCONNECT-ADAPTER](https://github.com/JuanenRac/HYDRA-UMC-MTCONNECT-ADAPTER)** — endpoints XML reales `/probe` y `/current` de MTConnect, con salida en modo degradado.

*Herramientas Complementarias y Operaciones del Ecosistema*
- **[HYDRA-UMC-DASHBOARD-AI](https://github.com/JuanenRac/HYDRA-UMC-DASHBOARD-AI)** — paneles de Resúmenes Inteligentes y Resaltado de Anomalías sobre DATALAKE/ANOMALY-DETECTOR, con un respaldo estadístico honesto.
- **[HYDRA-UMC-TOOL-CLI](https://github.com/JuanenRac/HYDRA-UMC-TOOL-CLI)** — CLI de flota con un contrato real y estable de códigos de salida, cliente real y en vivo de la propia API de HYDRA-UMC-SERVER.
- **[HYDRA-UMC-WATCH](https://github.com/JuanenRac/HYDRA-UMC-WATCH)** — app compañera de WearOS con alertas hápticas reales y un relé de voz al teléfono emparejado.
- **[URTC-SMART-RACK](https://github.com/JuanenRac/URTC-SMART-RACK)** — firmware para un rack de montaje de placas con decodificación real de ID de herramienta y lógica de precalentamiento Smart Idle.
- **[URTC-VISION-TOOL](https://github.com/JuanenRac/URTC-VISION-TOOL)** — firmware más un compañero de visión real en Python para un cabezal de inspección térmica/RGB.
- **[HYDRA-UMC-UPDATER](https://github.com/JuanenRac/HYDRA-UMC-UPDATER)** — herramienta administrativa de escritorio que descubre, clona y actualiza cada repositorio de este ecosistema.


---

## 📚 Documentación y Comunidad

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — stack tecnológico y pautas de codificación para un pull request.
- **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)** — los estándares de comportamiento esperados en esta comunidad.
- **[SECURITY.md](SECURITY.md)** — cómo reportar una vulnerabilidad, y las áreas reales de enfoque en seguridad de este proyecto.
- **[SUPPORT.md](SUPPORT.md)** — dónde hacer preguntas y reportar errores.
- **[LICENSE.md](LICENSE.md)** — la licencia propia de este proyecto.

## 👤 AUTOR
**JuanenRac** (Electro Hobby 3D)
📧 electrohobby3d@gmail.com
📺 [youtube.com/@electrohobby3d](https://youtube.com/@electrohobby3d)

## 📜 LICENCIA
GPL-3.0 - Ver archivo LICENSE para más detalles.
