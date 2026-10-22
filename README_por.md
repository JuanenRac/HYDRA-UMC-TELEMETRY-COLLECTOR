<p align="center">
  <img src="images/HYDRA_UMC_BANNER.svg" alt="HYDRA-UMC-TELEMETRY-COLLECTOR banner" width="100%">
</p>

# 📡 HYDRA-UMC-TELEMETRY-COLLECTOR

<p align="center"><a href="README.md">🇺🇸 English</a> | <a href="README_spa.md">🇪🇸 Español</a> | <a href="README_fra.md">🇫🇷 Français</a> | <a href="README_ita.md">🇮🇹 Italiano</a> | <a href="README_deu.md">🇩🇪 Deutsch</a> | <a href="README_zho.md">🇨🇳 简体中文</a> | <a href="README_jpn.md">🇯🇵 日本語</a> | 🇵🇹 <b>Português</b></p>

### 🚀 Nó de ingestão de alto débito para registos CAN e WebSocket

<p align="center">
  <img src="https://img.shields.io/badge/License-GPL%203.0-blue.svg" alt="GPL 3.0">
  <img src="https://img.shields.io/badge/Language-Go-orange.svg" alt="Go">
  <img src="https://img.shields.io/badge/Protocol-CAN%20%2F%20WebSocket-yellow.svg" alt="Protocol">
</p>

**Verificação de honestidade - o que realmente funciona hoje:** o pipeline de ingestão real - o `telemetry` que analisa tramas CAN e JSON de WebSocket para um `Sample` normalizado, o anel limitado do `buffer` que reporta contrapressão, a orquestração de ingestão e descarga do `collector` (uma escrita falhada no destino volta a pôr em fila apenas o resto real do lote que ainda não foi escrito, quando o destino sabe indicar até onde chegou, sem nunca perder dados em nenhum dos casos), o acompanhamento de sequências por produtor do `dedup`, seguro contra religações, e o `DatalakeSink` real do `sink` (testado contra um `net/http/httptest.Server` real que implementa o próprio `POST /ingest` do DATALAKE) ou o `ConsoleSink` de recurso - são todos reais e testados (`go test ./...`). O `GET /metrics` expõe os mesmos contadores em direto de ingestão e de taxa de perda que o `GET /stats` já acompanha, no formato real de exposição de texto do Prometheus, para que um Prometheus real (ou qualquer recolhedor compatível com OpenMetrics) os consulte diretamente. Duas lacunas reais que convém conhecer antes de integrar: este collector não abre ele próprio uma ligação em direto a um barramento FDCAN físico nem aceita ligações WebSocket em direto de um robô - a ingestão faz-se pela sua própria API HTTP simples (`POST /ingest/can`, `POST /ingest/ws`), pelo que alguma outra coisa ainda tem de ler o barramento/socket real e reencaminhar as tramas para aqui. E o próprio formato de transmissão CAN de `telemetry/can.go` (byte de código de sinal + valor float32) é uma convenção v0 provisória declarada por este próprio projeto, ainda não as tabelas de IDs CAN documentadas do ecossistema, vindas do firmware do HYDRA-UMC/URTC - deliberadamente não adivinhadas. A saída é apenas JSON real; o "JSON/Protobuf" do próprio README e a ingestão gRPC continuam planeados, não implementados em lado nenhum deste código. Veja o `CHANGELOG.md` para saber exatamente o que foi entregue até agora.

---

> **Estado de segurança (safety e security):** este ecossistema não é um sistema de segurança certificado. Ainda não existe um caminho de E-STOP por hardware, nem limites ou interlocks impostos pelo firmware, nem um comando de movimento verificado; as imagens de firmware são verificadas com CRC32 e um HMAC-SHA256 de chave partilhada, não com uma assinatura assimétrica; TLS e TLS mútuo são opcionais e estão desativados por defeito; nenhuma placa própria foi ainda montada ou testada. Leia a [base de segurança](https://github.com/JuanenRac/HYDRA-UMC-DOCS/blob/main/docs/SECURITY_BASELINE.md) e o [modelo de ameaças](https://github.com/JuanenRac/HYDRA-UMC-DOCS/blob/main/docs/THREAT_MODEL.md) antes de confiar em qualquer afirmação aqui para a segurança de uma pessoa.

## 📋 Num relance

| | |
|---|---|
| Versão | `0.1.7` |
| Maturidade | Funcional |
| Tecnologia | Go / CAN/WebSocket parsers / bounded FIFO buffer / HTTP API |
| Destino | O Raspberry Pi CM5 de uma célula HYDRA-UMC |
| Em hardware real | Ainda não: não há registo de execução em hardware real para esta versão |

## 1. 🛠️ VISÃO TÉCNICA GERAL

O **HYDRA-UMC-TELEMETRY-COLLECTOR** é o gateway de ingestão que recebe a telemetria enviada por HTTP e a escreve no Datalake. Converte em amostras normalizadas as tramas CAN e as mensagens JSON de WebSocket que recebe; não escuta ele próprio nenhum barramento CAN (previsto).

Normaliza e deduplica entradas heterogéneas, para que leituras como um pico de corrente de um motor num barramento CAN possam mais tarde ser correlacionadas com outras séries (previsto: correlação com resultados de inferência de IA de um Vision Node; essa inferência ainda não existe).

### Funcionalidades principais:
* 🚀 **Ingestão multiprotocolo:** trata hoje telemetria CAN e WebSocket, através de uma API de ingestão HTTP simples. *(a ingestão gRPC está planeada)*
* ⚡ **Débito:** pensado para a telemetria da fábrica, não para capturar um barramento: com os valores por defeito o collector entrega até 250 amostras por segundo (um lote de 500 a cada 2 s, um pedido por amostra para o HYDRA-UMC-DATALAKE); acima disso o buffer enche-se e a ingestão responde `503`.
* 🧬 **Normalização de dados:** traduz pacotes binários em bruto para um formato JSON padronizado. *(a saída em Protobuf está planeada, não implementada)*
* 🛡️ **Entrega com buffer:** uma amostra que o collector aceitou não é descartada durante uma falha da base de dados ou da rede: o lote que está a ser entregue mantém o seu lugar, e um buffer cheio recusa amostras novas (`503`) em vez de largar as antigas. O buffer está em memória, pelo que reiniciar o collector perde o que ele contém.
* 🔁 **Deduplicação segura contra religações:** um número de sequência opcional por produtor, acompanhado numa janela de reordenação limitada, para que um dispositivo que volta a ligar-se e reenvia as suas últimas mensagens sem confirmação nunca infle as contagens de ingestão. *(implementado)*
* 🧭 **Entrada plausível:** um carimbo temporal em segundos em vez de milissegundos, ou mais de 5 minutos à frente do relógio (`-max-future-skew`), e um `sourceId` demasiado longo ou com caracteres invisíveis são recusados (`400`); um produtor cujo contador recomeça depois de um reinício é reconhecido (`producerResets` em `GET /stats`) em vez de ver as suas amostras novas rejeitadas como duplicadas. *(implementado)*
* 🩺 **Diagnóstico real de falhas:** cada falha de descarga é classificada como o destino ter rejeitado genuinamente os dados ou como um problema de transporte - exposto no `GET /stats` para visibilidade operacional real. *(implementado)*
* 🧮 **Validação de campos finitos:** um nome de campo vazio ou um valor numérico `NaN`/`Infinity`/`-Infinity` é rejeitado (`400`) antes de chegar à memória intermédia ou a um destino - a descodificação CAN passa exatamente pela mesma verificação `Sample.Validate()` que a ingestão JSON. *(implementado)*
* 📊 **Métricas Prometheus:** o `GET /metrics` expõe os mesmos contadores em direto de ingestão e de taxa de perda que o `GET /stats` acompanha (ingeridos, descartados, em quarentena, falhas de descarga de transporte vs. dados inválidos, ocupação da memória intermédia) no formato real de exposição de texto do Prometheus. *(implementado)*

---

## 2. 🔄 FLUXO DE INGESTÃO

```mermaid
flowchart LR
    CAN["CAN Bus Traffic"] --> COLL["TELEMETRY-COLLECTOR"]
    WS["WebSocket Streams"] --> COLL
    COLL --> PARSE["Packet Parser & Norm"]
    PARSE --> BUF["High-Speed Buffer"]
    BUF --> LAKE["HYDRA-UMC-DATALAKE"]
```

---

## 3. 🧱 ARQUITETURA E DECISÕES DE DESENHO

* **Porque é que `src/` é a raiz do módulo Go e não a raiz do repositório.** Mantém os ficheiros do próprio módulo instalável (`main.go`, `version.go`, `go.mod`) separados das ferramentas da raiz do repositório (`bump_version.py`, `docker-compose.yml`) - o `go build ./...` corre de dentro de `src/`, não da raiz do repositório.
* **Porque a recolha é separada do próprio HYDRA-UMC-DATALAKE.** A recolha (consultar o HYDRA-UMC-SERVER, pôr em memória intermédia, agrupar escritas) é uma preocupação limitada por E/S, distinta do armazenamento/consulta - mantê-la como processo separado significa que um reinício do collector ou um pico de contrapressão não toca no caminho de consulta do próprio datalake.
* **Porque uma escrita falhada no destino volta a pôr o lote em fila em vez de o descartar - exceto a única amostra que nunca terá êxito.** O `src/collector` é o que realmente merece a afirmação "Entrega com memória intermédia: zero perda de dados": o `FlushOnce` só retira da memória intermédia um lote drenado depois de o `Sink.Write` o confirmar e, numa falha ao nível do transporte, volta a colocá-lo logo à frente - uma interrupção real repete as mesmas amostras, não as perde. A única exceção é uma amostra que o próprio destino rejeitou PERMANENTEMENTE como inválida (veja `sink.InvalidDataError` abaixo) - voltar a pô-la incondicionalmente em fila colocava-a de novo à frente para sempre, bloqueando todas as amostras válidas atrás dela; agora é posta em quarentena (descartada, contada no `quarantined` do `GET /stats`), enquanto o resto do lote volta a ser posto em fila normalmente. A memória intermédia (`src/buffer`) continua a ser limitada - uma interrupção que dure mais do que a sua capacidade descarta o excesso mais antigo, um limite real e honesto e não uma promessa de memória infinita.
* **Porque CAN e WebSocket são ambos analisados para a mesma forma de `Sample`.** O `src/telemetry` normaliza as duas fontes heterogéneas numa só estrutura antes de qualquer coisa tocar na memória intermédia ou no destino - o mecanismo real por detrás de "um pico de corrente de um motor em CAN corretamente correlacionado com um resultado de um Vision Node": nenhuma das etapas seguintes precisa de saber por que protocolo uma amostra chegou.
* **Porque o formato de transmissão CAN é a convenção v0 deste projeto e ainda não os IDs CAN reais do ecossistema.** As tabelas reais de IDs CAN vivem na documentação do firmware do HYDRA-UMC e do URTC - integrar-se com elas a sério é trabalho futuro, não algo a adivinhar sem essa referência aberta.
* **Porque o `DatalakeSink` escreve uma amostra por pedido HTTP.** O `POST /ingest` do HYDRA-UMC-DATALAKE recebe uma amostra, pelo que uma "escrita em lote" são na verdade N pedidos, por ordem, que param à primeira falha. O `PartialWriteError` diz quantos foram confirmados (`202`) e só o resto é repetido; uma amostra que o sink rejeita como inválida (`400` ou `413`) é a única descartada, e o que vem depois é repetido. Uma linha duplicada só pode aparecer se o DATALAKE aceitou um pedido e a resposta se perdeu, e o DATALAKE guarda um valor por (origem, tipo, campo, carimbo temporal), pelo que um reenvio sobrescreve em vez de duplicar.
* **Como se encaixa no resto do ecossistema.** Um serviço irmão sob o HYDRA-UMC-DATALAKE: recebe a telemetria que os produtores lhe enviam por HTTP e escreve-a no armazém partilhado de séries temporais. Não consulta nada, e nada no ecossistema lhe envia dados ainda: falta escrever um produtor (uma subscrição aos deltas de robôs do Server, um leitor SocketCAN no CM5).
* **Porque a deduplicação é um pacote `dedup` separado, assente numa `Sequence` opcional e não num hash do conteúdo da própria amostra.** Uma religação real reenvia exatamente os mesmos bytes, pelo que fazer o hash do conteúdo funcionaria nesse caso - mas também engoliria em silêncio duas amostras genuinamente diferentes que por acaso partilham todos os campos (por exemplo, duas leituras de `0.0` com um segundo de intervalo). Um número de sequência por produtor é o que um dispositivo real já tem de acompanhar para as suas próprias mensagens sem confirmação, pelo que reutilizá-lo é o sinal honesto e real - não um palpite derivado de dados que na verdade não prometem unicidade. `Sequence == 0`/omitida retira um produtor por completo, pelo que nada do comportamento anterior mudou para um dispositivo que não envia nenhuma.
* **Porque o `sink.InvalidDataError` transporta o `Index` do lote da própria amostra rejeitada.** Encontrado ao auditar o código (P1): o `DatalakeSink.Write` envia uma amostra por pedido HTTP e pára na primeira falha, pelo que, quando o DATALAKE devolve um HTTP 400 real para uma amostra concreta, o `Write` carimba a posição dessa amostra no lote no erro antes de o devolver. O `FlushOnce` usa isto para pôr em quarentena (descartar de forma permanente, nunca voltar a pôr em fila) exatamente essa amostra - repetir os mesmos bytes rejeitados nunca pode ter êxito - enquanto volta a pôr em fila o resto do lote normalmente, em vez de o lote inteiro ser repetido (e voltar a ser bloqueado) para sempre atrás de uma amostra envenenada. O `quarantined` do `GET /stats` conta todas as amostras descartadas desta forma, separado do `invalidDataErrors` (tentativas de descarga, não amostras) e do `dropped` (um limite de capacidade de memória intermédia cheia, não uma rejeição permanente) - pelo que nada disto é um descarte silencioso, e o `invalidDataErrors` vs. `transportErrors` continua a permitir a um operador distinguir "o DATALAKE está a rejeitar os nossos dados" de "a rede até ao DATALAKE está em baixo" sem adivinhar a partir dos registos.

---

## 📂 ESTRUTURA DE DIRETÓRIOS

Serviço só de software (nó de ingestão) - sem hardware, firmware nem sistema operativo próprios; essas pastas são omitidas por política de estrutura do repositório.

```text
HYDRA-UMC-TELEMETRY-COLLECTOR/
├── src/                  # Módulo Go
│   ├── go.mod          # Definição do módulo
│   ├── version.go      # const Version = "X.Y.Z"
│   ├── main.go         # Ponto de entrada: liga tudo e arranca a API HTTP
│   ├── guard.go        # Guarda de arranque: só loopback, a menos que haja um token Bearer
│   ├── telemetry/      # Tipo Sample + analisadores CAN/WebSocket (normalização)
│   ├── buffer/         # FIFO limitado que reporta contrapressão (Ring)
│   ├── dedup/           # Deduplicação real de sequências por produtor (janela de reordenação)
│   ├── collector/      # Orquestra ingestão e descarga, repete em caso de falha do destino, deduplicação
│   ├── sink/           # Para onde vão os lotes descarregados (ConsoleSink hoje), classificação transporte/dados inválidos
│   └── api/             # Handlers JSON/HTTP simples que envolvem o collector
├── docs/
│   └── API.md           # Referência real dos endpoints HTTP (pedidos, respostas, códigos de estado)
├── images/              # Multimédia e diagramas
├── systemd/
│   └── hydra-umc-telemetry-collector.service # Unidade systemd da API local de ingestão de telemetria do CM5
├── tools/
│   ├── build_test.py    # Verificação de compilação sem subida de versão
│   └── ci_validate.py   # Validação de manifesto/CHANGELOG/documentação usada pelo CI
├── build/               # Binários compilados (ignorados pelo git)
├── bump_version.py      # Subida de versão ao estilo conta-quilómetros (executada só pelo comando `bump` explícito)
├── bump_manifest_version.py # Sincroniza a versão do hydra-umc.project.json com a nativa (--sync)
├── build.sh / build.bat # Valida e compila (compilar nunca altera a versão)
├── run.sh / run.bat     # Execução real: executa o binário compilado
└── README.md
```

Retirado do modelo original: `hardware/`, `firmware/` e `os/` —
este é um serviço só de software (binário Go) sem hardware nem firmware
dedicados e sem imagem de sistema operativo para manter. Veja
[`docs/API.md`](docs/API.md) para a referência completa dos
endpoints HTTP.

---

## 4. ⚙️ GUIA DE COMPILAÇÃO E EXECUÇÃO

Requer o Go >= 1.21. Um pipeline de ingestão real com uma API HTTP, não apenas um
esqueleto que compila.

```bash
# Linux/macOS
./build.sh
./run.sh -addr 127.0.0.1:8092 -datalake-url http://localhost:8095

# Windows
build.bat
run.bat -addr 127.0.0.1:8092 -datalake-url http://localhost:8095
```

`build` nunca altera a versão (só `python tools/hydra_project_tool.py bump` a sobe) e compila o módulo Go em
`src/` para `build/telemetry-collector(.exe)`. O `run` executa o
binário compilado, reencaminhando quaisquer opções, e começa a escutar tráfego real.
O `-datalake-url` aponta os lotes descarregados para o `POST /ingest` de uma
instância real e em execução do HYDRA-UMC-DATALAKE (`sink.DatalakeSink`) -
omita-o para imprimir as amostras descarregadas no stdout
(`sink.ConsoleSink`), útil para executar este collector de forma independente.

Por omissão o collector escuta apenas em loopback e as suas rotas de ingestão estão abertas aos chamadores locais. Para escutar num endereço de rede, dê-lhe um token Bearer (`-token`, ou `HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN`; pelo menos 16 caracteres): o `POST /ingest/can` e o `POST /ingest/ws` passam então a exigir `Authorization: Bearer <token>` (o `/stats` e o `/metrics` continuam abertos para que um recolhedor local não precise de nenhum segredo). Um `-addr` que não seja loopback e não tenha token é recusado no arranque, a menos que se passe `-allow-anonymous`. O corpo de um pedido está limitado a 1 MiB (`413` se o ultrapassar) e o servidor HTTP aplica tempos limite de leitura, escrita e inatividade.

```bash
# Ingerir uma amostra de telemetria ao estilo WebSocket
curl -X POST localhost:8092/ingest/ws \
  -d '{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":42.5}}'

# Ingerir uma trama CAN (8 bytes, base64 - veja src/telemetry/can.go para o formato de transmissão)
curl -X POST localhost:8092/ingest/can \
  -d '{"arbitrationId":7,"data":"AQAAUEEAAAA="}'

# Verificar o que o collector ingeriu/descarregou/descartou
curl localhost:8092/stats
```

```bash
cd src && go test ./...   # telemetry (ida e volta CAN, validação WS),
                           # buffer (FIFO limitado, contrapressão, reposição em fila),
                           # collector (o comportamento real de "não perder
                           # dados numa interrupção do destino"), e api (idas e
                           # voltas HTTP reais com httptest)
```

---

## 🚀 ROTEIRO
* **Fase 1:** ingestão de alto débito do Datalake e indexação para análise histórica.
* **Fase 2:** compressão na periferia do telemetry collector e protocolos de transmissão seguros.
* **Fase 3:** deteção de anomalias com aprendizagem não supervisionada e análise de vibração de motores.
* **Fase 4:** compressão em tempo real para ingestão massiva de registos e otimização multiprotocolo.

---

## 🔗 Projetos relacionados

Este projeto faz parte do ecossistema de robótica HYDRA-UMC do mesmo autor (JuanenRac / Electro Hobby 3D). Vale a pena conhecê-los, porque um pedido pode ser, na verdade, sobre um destes e não sobre este repositório.

**Projeto-mãe**
- **[HYDRA-UMC-DATALAKE](https://github.com/JuanenRac/HYDRA-UMC-DATALAKE)** — armazém real de séries temporais sobre sqlite3 com uma API HTTP real de ingestão/consulta; o projeto-mãe de que este repositório é um serviço de análise específico, dentro da sua própria camada de dados e análise.

**Projetos irmãos** — os outros serviços de análise da própria camada de dados e análise do HYDRA-UMC-DATALAKE
- **[HYDRA-UMC-ANOMALY-DETECTOR](https://github.com/JuanenRac/HYDRA-UMC-ANOMALY-DETECTOR)** — detetor real de anomalias por FFT e linha de base estatística, com monitorização de deriva.
- **[HYDRA-UMC-PRODUCTION-REPORTS](https://github.com/JuanenRac/HYDRA-UMC-PRODUCTION-REPORTS)** — cálculo real de OEE/disponibilidade sobre o histórico do DATALAKE, com exportação CSV reprodutível.

**Diretamente relacionados**
- **[HYDRA-UMC-SERVER](https://github.com/JuanenRac/HYDRA-UMC-SERVER)** — o back-end headless real (REST/WebSocket) com que todos os clientes de controlo realmente comunicam — a origem dos registos que este projeto ingere.
- **[HYDRA-UMC](https://github.com/JuanenRac/HYDRA-UMC)** — o desenho da placa-mãe do braço robótico: anfitrião CM5 + STM32H745 de dois núcleos, pensado para orquestrar até 8 braços de ferramenta por CAN-OTA/SPI-OTA (firmware numa fase inicial; ainda sem placa montada); hoje usa a sua própria convenção v0, assumida honestamente como trabalho futuro e não dada como concluída.
- **[URTC](https://github.com/JuanenRac/URTC)** — firmware da placa física Universal Robot Tool Controller, mais de 25 perfis de ferramenta por barramento CAN — a tabela real de IDs CAN com que o formato de transmissão CAN deste projeto deverá integrar-se um dia; hoje usa a sua própria convenção v0, assumida honestamente como trabalho futuro e não dada como concluída.

**Também fazem parte do ecossistema**

*Hardware e plataforma centrais*
- **[HYDRA-UMC-OS](https://github.com/JuanenRac/HYDRA-UMC-OS)** — camada de produto reproduzível do Raspberry Pi OS para o CM5: agente só de leitura, configuração/perfis validados, aprovisionamento Wi-Fi de primeiro contacto.
- **[HYDRA-UMC-SDK](https://github.com/JuanenRac/HYDRA-UMC-SDK)** — o contrato JSON-Schema partilhado e a fronteira de software de controlo de trabalhos contra a qual cada bridge valida os seus comandos (não é uma função de segurança certificada).
- **[HYDRA-UMC-CONNECTOR-HUB](https://github.com/JuanenRac/HYDRA-UMC-CONNECTOR-HUB)** — registo declarativo de manifestos de adaptador e validador para conectores de máquinas externas; estende a ideia de contrato do SDK a máquinas externas sem substituir os projetos de gateway industrial.

*Backend central e clientes*
- **[HYDRA-UMC-STUDIO](https://github.com/JuanenRac/HYDRA-UMC-STUDIO)** — painel web de controlo com visualização 3D em tempo real de vários robôs.
- **[HYDRA-UMC-SUITE](https://github.com/JuanenRac/HYDRA-UMC-SUITE)** — centro de comando de enxame para desktop (PySide6) para vários servidores ao mesmo tempo, empacotado como executável independente.
- **[HYDRA-UMC-ANDROID-CONTROL](https://github.com/JuanenRac/HYDRA-UMC-ANDROID-CONTROL)** — aplicação Android nativa de controlo, com início de sessão biométrico e um companheiro Wear OS emparelhado.
- **[HYDRA-UMC-IOS-CONTROL](https://github.com/JuanenRac/HYDRA-UMC-IOS-CONTROL)** — aplicação de controlo para iOS/iPadOS (Flutter) com sincronização WebSocket em tempo real.
- **[HYDRA-UMC-DSI](https://github.com/JuanenRac/HYDRA-UMC-DSI)** — interface tátil nativa para o ecrã tátil DSI de 7" integrado, executada no próprio CM5.
- **[HYDRA-UMC-EDITOR-URDF](https://github.com/JuanenRac/HYDRA-UMC-EDITOR-URDF)** — criador/editor gráfico de URDF para desktop que envia os modelos concluídos para o catálogo do STUDIO.
- **[HYDRA-UMC-EDITOR-STL](https://github.com/JuanenRac/HYDRA-UMC-EDITOR-STL)** — editor de modelos STL para desktop que transforma/substitui/remove/adiciona peças reais no mesmo catálogo de modelos que o HYDRA-UMC-EDITOR-URDF também edita.
- **[HYDRA-UMC-BRIDGE-AMR](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-AMR)** — fronteira de coordenação para frotas AGV/AMR através de um publicador MQTT VDA 5050 (testado apenas contra uma frota emulada).
- **[HYDRA-UMC-BRIDGE-CNC](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-CNC)** — coordenador de células CNC de alto nível com acesso ao estado GRBL e aos bytes de controlo (testado apenas contra um emulador).
- **[HYDRA-UMC-BRIDGE-DROIDS](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-DROIDS)** — fronteira de coordenação para droides com patas/humanoides, com um emissor de comandos para o Boston Dynamics Spot (testado apenas contra um emulador).
- **[HYDRA-UMC-BRIDGE-LASER](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-LASER)** — coordenador de células laser que lê 3 sinais GPIO (chave, caixa e interlock) (testado apenas contra um chip simulado; não pode armar nem disparar um laser).
- **[HYDRA-UMC-BRIDGE-OPENPNP](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-OPENPNP)** — coordenador seguro de fluxo de placa de alto nível para pick-and-place com OpenPnP.
- **[HYDRA-UMC-BRIDGE-PRINTER3D](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-PRINTER3D)** — fronteira de coordenação segura para impressoras 3D Moonraker/Klipper, com comandos de trabalho reais protegidos por portas.
- **[HYDRA-UMC-BRIDGE-ROS2](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-ROS2)** — coordenador com um transporte ROS 2 baseado em rclpy importado sob demanda (ainda não executado contra uma instalação ROS 2).
- **[HYDRA-UMC-BRIDGE-UAV](https://github.com/JuanenRac/HYDRA-UMC-BRIDGE-UAV)** — fronteira de coordenação para UAV com câmara, com um emissor de comandos MAVLink (testado apenas contra um emulador).

*Plataforma de ferramentas URTC*
- **[URTC-FLASHER](https://github.com/JuanenRac/URTC-FLASHER)** — ferramenta gráfica de gravação para desktop das placas URTC, por CAN-OTA e também SWD/JTAG de chip completo.
- **[URTC-TESTER](https://github.com/JuanenRac/URTC-TESTER)** — ferramenta de diagnóstico CAN em tempo real para desktop das placas URTC, com um painel por perfil de ferramenta.
- **[URTC-WEB-STUDIO](https://github.com/JuanenRac/URTC-WEB-STUDIO)** — alternativa no navegador ao URTC-TESTER através da Web Serial API, sem instalação local.

*Nó de IA de visão (Hailo-8)*
- **[HYDRA-UMC-VISION-NODE](https://github.com/JuanenRac/HYDRA-UMC-VISION-NODE)** — centro de integração do pipeline de visão Hailo-8, com uma verificação real de prontidão de hardware por fase.
- **[HYDRA-UMC-DETECTION-HEF](https://github.com/JuanenRac/HYDRA-UMC-DETECTION-HEF)** — registo real de modelos compilados com verificação segura de carga por arquitetura Hailo e soma de verificação.
- **[HYDRA-UMC-VISION-STREAMER](https://github.com/JuanenRac/HYDRA-UMC-VISION-STREAMER)** — pipeline GStreamer real e gerador de configuração MediaMTX com uma fronteira de integração HailoRT real.
- **[HYDRA-UMC-VISUAL-SERVOING-API](https://github.com/JuanenRac/HYDRA-UMC-VISUAL-SERVOING-API)** — lei de correção de servovisão baseada em posição com autorização condicionada ao estado da zona; ainda sem estimativa de pose por câmara.
- **[HYDRA-UMC-SAFETY-ZONES](https://github.com/JuanenRac/HYDRA-UMC-SAFETY-ZONES)** — lógica v0 de violação de zonas, atualidade da calibração e pedido de E-STOP que lê as posições dos objetos de um ficheiro; nenhum detetor a alimenta e ainda não há E-STOP físico ligado.

*Nó de IA cognitiva (Hailo-10)*
- **[HYDRA-UMC-COGNITIVE-NODE](https://github.com/JuanenRac/HYDRA-UMC-COGNITIVE-NODE)** — centro de integração do pipeline cognitivo do Hailo-10 (orquestração de LLM/VLA/voz).
- **[HYDRA-UMC-VLA-ENGINE](https://github.com/JuanenRac/HYDRA-UMC-VLA-ENGINE)** — codificação/descodificação real de tokens de ação e geração de trajetórias para um modelo Visão-Linguagem-Ação.
- **[HYDRA-UMC-VOICE-UI](https://github.com/JuanenRac/HYDRA-UMC-VOICE-UI)** — front-end de voz real (VAD + analisador de intenções) com um relé para o relógio limitado e protegido por confirmação.
- **[HYDRA-UMC-SEMANTIC-PLANNER](https://github.com/JuanenRac/HYDRA-UMC-SEMANTIC-PLANNER)** — decomposição real de tarefas baseada em regras e recuperação semântica de erros sobre os códigos de erro do MCU.
- **[HYDRA-UMC-DOCS-QA](https://github.com/JuanenRac/HYDRA-UMC-DOCS-QA)** — pesquisa real de documentos TF-IDF só com a biblioteca padrão sobre a documentação Markdown deste ecossistema.
- **[HYDRA-UMC-LOCAL-TECHNICIAN](https://github.com/JuanenRac/HYDRA-UMC-LOCAL-TECHNICIAN)** — técnico de manutenção de IA local, protegido por políticas, para o próprio ecossistema: observa, diagnostica e propõe correções; os dois níveis de maior risco não estão implementados de propósito.

*Orquestração e enxame*
- **[HYDRA-UMC-ORCHESTRATOR](https://github.com/JuanenRac/HYDRA-UMC-ORCHESTRATOR)** — centro de integração com um contrato real de relatório de saúde gRPC/Protobuf e uma máquina de estados de missões.
- **[HYDRA-UMC-JOB-DISPATCHER](https://github.com/JuanenRac/HYDRA-UMC-JOB-DISPATCHER)** — fila real de trabalhos por prioridade com deduplicação, sobre uma API HTTP real.
- **[HYDRA-UMC-NODE-HEALING](https://github.com/JuanenRac/HYDRA-UMC-NODE-HEALING)** — vigilante real de saúde da frota por gRPC com repetição/recuo e deteção de identidade incoerente.
- **[HYDRA-UMC-PATH-PLANNER-3D](https://github.com/JuanenRac/HYDRA-UMC-PATH-PLANNER-3D)** — planeador real de trajetórias 3D baseado em RRT com validação real de colisões com obstáculos e espaço de trabalho.
- **[HYDRA-UMC-SWARM-SYNC](https://github.com/JuanenRac/HYDRA-UMC-SWARM-SYNC)** — sincronização real de estado CRDT LWW-Element-Map, testada por propriedades para a convergência de várias células.

*Gémeo digital e simulação*
- **[HYDRA-UMC-TWIN](https://github.com/JuanenRac/HYDRA-UMC-TWIN)** — centro de integração do motor de gémeo digital, com um contrato real de sincronização de compatibilidade de versões.
- **[HYDRA-UMC-HIL-BRIDGE](https://github.com/JuanenRac/HYDRA-UMC-HIL-BRIDGE)** — encaminhador hardware-in-the-loop com um interlock de software de colisão entre a simulação e o hardware real (testado sem hardware real).
- **[HYDRA-UMC-PHYSICS-REPLICA](https://github.com/JuanenRac/HYDRA-UMC-PHYSICS-REPLICA)** — cinemática direta real e validação de limites de juntas sobre um subconjunto real de URDF.
- **[HYDRA-UMC-SYNTHETIC-DATA-GEN](https://github.com/JuanenRac/HYDRA-UMC-SYNTHETIC-DATA-GEN)** — gerador procedural real de cenas 2D com exportação de anotações YOLO/COCO.

*Gateway industrial*
- **[HYDRA-UMC-GATEWAY-INDUSTRIAL](https://github.com/JuanenRac/HYDRA-UMC-GATEWAY-INDUSTRIAL)** — centro de integração que retransmite para protocolos industriais, com uma camada real de lista de comandos permitidos e contrapressão.
- **[HYDRA-UMC-OPCUA-SERVER](https://github.com/JuanenRac/HYDRA-UMC-OPCUA-SERVER)** — espaço de endereços OPC-UA real, verificado com uma sessão de cliente real do protocolo binário.
- **[HYDRA-UMC-MQTT-BROKER](https://github.com/JuanenRac/HYDRA-UMC-MQTT-BROKER)** — broker MQTT real com autenticação opcional por cliente e ACL de tópicos.
- **[HYDRA-UMC-MTCONNECT-ADAPTER](https://github.com/JuanenRac/HYDRA-UMC-MTCONNECT-ADAPTER)** — endpoints XML MTConnect `/probe` e `/current` reais com saída em modo degradado.

*Ferramentas complementares e operações do ecossistema*
- **[HYDRA-UMC-DASHBOARD-AI](https://github.com/JuanenRac/HYDRA-UMC-DASHBOARD-AI)** — painéis de Resumos Inteligentes e Realce de Anomalias sobre o DATALAKE/ANOMALY-DETECTOR, com um recurso estatístico honesto.
- **[HYDRA-UMC-TOOL-CLI](https://github.com/JuanenRac/HYDRA-UMC-TOOL-CLI)** — CLI da frota com um contrato de códigos de saída real e estável, um cliente vivo genuíno da API do HYDRA-UMC-SERVER.
- **[HYDRA-UMC-WATCH](https://github.com/JuanenRac/HYDRA-UMC-WATCH)** — aplicação companheira WearOS com padrões de alerta tátil e um relé de voz para o telemóvel emparelhado (ainda não executada num relógio físico).
- **[URTC-SMART-RACK](https://github.com/JuanenRac/URTC-SMART-RACK)** — firmware de um rack de montagem de placas com descodificação real do ID da ferramenta e lógica de pré-aquecimento Smart Idle.
- **[URTC-VISION-TOOL](https://github.com/JuanenRac/URTC-VISION-TOOL)** — firmware mais um companheiro de visão em Python real para uma cabeça de ferramenta de inspeção térmica/RGB.
- **[HYDRA-UMC-UPDATER](https://github.com/JuanenRac/HYDRA-UMC-UPDATER)** — ferramenta administrativa de desktop que descobre, clona e atualiza todos os repositórios deste ecossistema.
- **[HYDRA-UMC-OS-REBUILDER](https://github.com/JuanenRac/HYDRA-UMC-OS-REBUILDER)** — ferramenta de desktop para Windows/Linux que constrói uma imagem do CM5 pronta a gravar com as versões mais recentes do ecossistema, com configuração de primeiro arranque de Wi-Fi/utilizador/SSH ao estilo do Raspberry Pi Imager.
- **[HYDRA-UMC-OPS-AGENT](https://github.com/JuanenRac/HYDRA-UMC-OPS-AGENT)** — coordenador de incidentes de manutenção: um papel de borda de poucos privilégios recolhe um inventário/estado higienizado, um papel de plano de controlo mostra-o só de leitura e pede a um fornecedor de IA que sugira um diagnóstico; nunca aplica um patch nem implanta nada.
- **[HYDRA-UMC-DEV-SERVER](https://github.com/JuanenRac/HYDRA-UMC-DEV-SERVER)** — anfitrião de desenvolvimento reproduzível (Raspberry Pi 5 / CM5) que guarda o código-fonte do ecossistema e executa tarefas limitadas de compilação/teste numa fila durável; um papel de desenvolvimento dedicado, explicitamente não um CM5 operacional.

---

## 📚 Documentação e comunidade

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — pilha tecnológica e diretrizes de código para um pull request.
- **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)** — as normas de comportamento esperadas nesta comunidade.
- **[SECURITY.md](SECURITY.md)** — como comunicar uma vulnerabilidade e as áreas de segurança em que este projeto se concentra.
- **[SUPPORT.md](SUPPORT.md)** — onde fazer perguntas e comunicar erros.
- **[LICENSE.md](LICENSE.md)** — a licença deste projeto.

## 👤 AUTOR
**JuanenRac** (Electro Hobby 3D)
📧 electrohobby3d@gmail.com
📺 [youtube.com/@electrohobby3d](https://youtube.com/@electrohobby3d)

## 📜 LICENÇA
GPL-3.0 - consulte LICENSE para mais detalhes.

