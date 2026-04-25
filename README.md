# Trading Alert Bot / Proyecto Omega

## 1. Descripción general

**Proyecto Omega** es un bot de monitoreo técnico para **Binance USDⓈ-M Futures** con integración de notificaciones y control por **Telegram**.

### Qué hace
- Consume datos de mercado vía REST/WebSocket.
- Escanea señales multi-timeframe (H1, M15, M5, M3, M1).
- Construye y gestiona **planes teóricos** de trading (entry/SL/TP).
- Persiste estado local en SQLite para recuperación tras reinicio.
- Notifica eventos técnicos y estado operativo por Telegram.

### Qué NO hace
- **No ejecuta órdenes reales**.
- **No abre/cierra/modifica posiciones reales** en Binance.
- **No es un sistema de trading automático**.

### Enfoque operativo actual
- Arquitectura jerárquica multi-timeframe.
- M15 como autoridad estructural.
- Lower TF (M1/M3/M5) como confirmación/trigger.
- DRY_RUN/read-only first.

---

## 2. Estado actual del proyecto

| Área | Estado | Comentario |
|---|---|---|
| Binance REST/WebSocket | Implementado | Cliente REST con guard/cache/backoff y streams de mercado/usuario. |
| Scanner multi-timeframe | Implementado | Señales y metadata propagada por timeframe con gating posterior. |
| Planner de trades | Implementado | Planes teóricos a partir de estructura y reglas de calidad. |
| Monitor de TP/SL | Implementado | `plan_monitor` evalúa avance y marca eventos de lifecycle. |
| Telegram bot | Implementado | Comandos de control, watchlist y estado; polling por `getUpdates`. |
| Watchlist | Implementado | Persistencia en SQLite y limpieza de setups armados al remover símbolo. |
| SQLite | Implementado | Tablas para estado scanner, planes y eventos; recuperación tras reinicio. |
| Idempotencia por fingerprint | Implementado | Fingerprint obligatorio en código + índice único. |
| Locks async | Implementado | Lock por `symbol+side` para serializar creación de planes. |
| M15 armed setups | Implementado | Setup M15 persistido con TTL, consumo y expiración explícita. |
| Cooldown | Implementado | Evaluado dentro del lock y después de active guard. |
| Estados activos/terminales | Implementado | Set canónico congelado; `TP3_HIT` terminal/no activo. |
| Migración segura duplicados activos | Implementado | Repara duplicados previos y luego crea índice parcial único. |
| Tests | Parcialmente validados en este entorno | Existen tests específicos nuevos; suite completa pendiente en Python 3.12 real. |
| DB migrations | Aditivas | Incluyen repair/migration para índice parcial activo por `symbol+direction`. |
| DRY_RUN / no órdenes | Implementado | Diseño explícitamente read-only. |
| Pendientes conocidos | Sí | Ver sección 20. |

---

## 3. Principios de diseño

1. **Read-only first**.
2. **No order execution**.
3. **M15 como estructura oficial**.
4. **Lower TF como triggers, no como estructura**.
5. **Velas cerradas antes de decisiones críticas**.
6. **Idempotencia obligatoria**.
7. **Máximo un plan activo por `symbol+direction`**.
8. **Persistencia local y recovery tras reinicio**.
9. **Observabilidad por logs**.
10. **Seguridad/consistencia por encima de agresividad operativa**.

---

## 4. Arquitectura multi-timeframe

| Timeframe | Rol | Puede armar setup | Puede crear plan | Puede modificar TP/SL | Uso actual |
|---|---|---:|---:|---:|---|
| H1 | Contexto macro / sesgo | No | No | No | Filtro contextual general |
| M15 | Estructura principal | Sí | Sí
aunque por default deshabilitado directo | Sí (fuente base estructural) | Define entry/SL/TP y validez base |
| M5 | Confirmación secundaria | No (solo activa setup existente) | No directo | No | Trigger/confirmación de setup M15 |
| M3 | Confirmación intermedia | No (solo activa setup existente) | No directo | No | Trigger/confirmación de setup M15 |
| M1 | Timing fino / monitoreo | No (solo activa setup existente) | No directo | No | Trigger fino y seguimiento |

> Con `ENABLE_DIRECT_M15_PLANS=false` (default), M15 arma setup pero no crea plan final directo.

---

## 5. Flujo de datos

```text
Binance REST/WebSocket
        ↓
app/scanner.py
        ↓
scanner signal
        ↓
app/main.py::handle_scanner_signal_sent
        ↓
armed_m15 state / plan gating
        ↓
app/trade_planner.py::build_trade_plan
        ↓
app/storage.py
        ↓
Telegram notifier
        ↓
app/plan_monitor.py
```

### Etapas
1. **Market data**: ingestión REST/WS.
2. **Normalización de vela**: cierre, timeframe y timestamps.
3. **Signal generation**: scanner + quality payload + estructura.
4. **Structural validation**: jerarquía M15 + reglas de gating.
5. **Plan gating**: guards de estado/cooldown/idempotencia.
6. **Plan creation**: build + persistencia.
7. **Storage**: SQLite como source-of-truth local.
8. **Notification**: alertas y estado por Telegram.
9. **Monitoring**: eventos TP/SL/expiry del plan.

---

## 6. Scanner

`app/scanner.py` monitorea símbolos de watchlist y adjunta metadata técnica usada por gating/planner.

### Timeframes en uso
- Estructura principal: `15m`.
- Confirmación/trigger: `1m`, `3m`, `5m`.
- Contexto adicional: `1h` (macro).

### Metadata relevante propagada
- `source_tf`
- `execution_tf`
- `m15_candle_open_time`
- `source_candle_closed`
- `execution_candle_closed`
- `is_closed_candle` (compatibilidad legacy)

**Regla operativa:** decisiones estructurales deben depender de velas cerradas.

---

## 7. Gating de generación de planes

Lógica principal: `app/main.py::handle_scanner_signal_sent`.

### Reglas actuales
- `execution_tf` debe ser `"15m"`.
- `execution_candle_closed` debe ser `True`.
- Si `source_tf != "15m"`:
  - `source_candle_closed` debe ser `True`.
  - debe existir `armed_m15` válido.
  - `armed_m15` no debe estar expirado.
- Si `source_tf == "15m"` y `ENABLE_DIRECT_M15_PLANS=false`:
  - arma setup M15,
  - no crea plan final,
  - log explícito: `reason="armed_m15_setup_only"`.
- Si `source_tf == "15m"` y `ENABLE_DIRECT_M15_PLANS=true`:
  - puede crear plan directo si pasa todos los guards.
- `active_plan` guard se evalúa **antes** de cooldown.
- cooldown se evalúa dentro del lock.
- `plan_fingerprint` es obligatorio.
- lock por `symbol+side`.
- SQLite refuerza con índices únicos.

### Pseudo-flujo

```text
evento scanner
  -> validar metadata mínima
  -> validar execution_tf == 15m y vela de ejecución cerrada
  -> if source_tf == 15m:
       armar setup
       if direct_m15_disabled: salir (setup-only)
     else:
       exigir source_candle_closed
       exigir armed_m15 existente y vigente
  -> exigir no duplicado por fingerprint
  -> exigir no active_plan_exists(symbol, side)
  -> evaluar cooldown
  -> build_trade_plan + INSERT
  -> registrar fingerprint state y consumir armed setup
```

---

## 8. M15 armed setup lifecycle

Un `armed_m15` representa un setup estructural M15 listo para ser activado por lower timeframe dentro de TTL.

- **Key:** `armed_m15::{symbol}:{side}:{signal_type}:{m15_candle_open_time}`
- **Persistencia:** SQLite (`scanner_alert_state`).
- **TTL default:**
  - `ARMED_M15_TTL_CANDLES=2`
  - `ARMED_M15_TF_SECONDS=900`

### Ciclo de vida
- **Creación:** señal válida de fuente M15 cerrada.
- **Consumo:** al crear un plan final.
- **Expiración:** si supera TTL sin activación.
- **Eliminación:** consumo, expiración o remoción de símbolo de watchlist.
- **Reinicio:** sobrevive reinicios por persistencia en DB.

### Logs esperados
- setup armado (`m15_setup_created`)
- setup-only (`armed_m15_setup_only`)
- setup expirado (`armed_m15_expired`)

---

## 9. Trade planner

Archivo principal: `app/trade_planner.py`.

### Responsabilidad
- `build_trade_plan`: genera un plan teórico consistente.
- La fuente estructural para `entry/SL/TP` es `structure_m15`.
- Lower TF no inyecta SL/TP estructural directamente.

### Orden esperado de niveles
- **LONG:** `SL < entry < TP1 < TP2 < TP3`
- **SHORT:** `TP3 < TP2 < TP1 < entry < SL`

### Cobertura de tests
- Hay pruebas de orden estructural LONG/SHORT y flujos de creación/monitoreo.

### Riesgos pendientes (documentados)
- RR mínimo tras refinamiento de entry (si aplica en evolución futura).
- Evitar `TP1` excesivamente cercano.
- Evitar `SL` demasiado ajustado tras refinamientos por lower TF.

---

## 10. Idempotencia y anti-duplicados

### Mecanismos actuales
- `plan_fingerprint` obligatorio en código.
- Formato conceptual: `symbol:side:signal_type:execution_tf:m15_candle_open_time`.
- Índice único: `idx_trade_plans_plan_fingerprint`.
- `sqlite3.IntegrityError` se usa como suppressor de duplicados.
- Lock async por `symbol+side`.
- Active guard + cooldown + índices SQLite.

### Riesgo original
Señales repetidas o carreras concurrentes podían crear múltiples planes del mismo par/lado.

### Mitigación actual
Fingerprint + lock + guard activo + cooldown + constraints DB.

---

## 11. Plan activo por `symbol+direction`

- Regla de negocio: **máximo 1 plan activo** por `symbol+direction`.
- Campo persistido: `direction` (equivalente operativo a side LONG/SHORT en planes).
- Índice parcial único: `idx_one_active_plan_per_symbol_side`.

### Estados canónicos

**Activos** (`ACTIVE_TRADE_PLAN_STATUSES`):
- `CREATED`
- `ENTRY_TOUCHED`
- `ACTIVE_ASSUMED`
- `TP1_HIT`
- `TP2_HIT`

**Terminales** (`TERMINAL_TRADE_PLAN_STATUSES`):
- `TP3_HIT`
- `SL_HIT`
- `EXPIRED`
- `CANCELLED`
- `INVALIDATED`
- `RESOLVED`

`TP3_HIT` cuenta como terminal (no activo).

### Migración segura
- Detecta duplicados activos preexistentes por `symbol+direction`.
- Conserva el más reciente (`created_at DESC`, `id DESC`).
- Cancela duplicados restantes (`CANCELLED`) sin borrar físicamente.
- Crea índice parcial activo.
- Si falla creación del índice: aborta `initialize` con error explícito.

---

## 12. SQLite / persistencia

SQLite es la persistencia local principal.

### Entidades relevantes
- `trade_plans`
- `trade_plan_events`
- `scanner_alert_state`
- `watchlist_symbols`
- otras tablas de runtime/historial ya existentes (alertas, trades declarados, runtime, etc.).

### Migraciones
- Aditivas y enfocadas a compatibilidad incremental.
- Se agregó columna `plan_fingerprint` y constraints asociadas.

### Índices relevantes
- `idx_trade_plans_plan_fingerprint` (UNIQUE)
- `idx_one_active_plan_per_symbol_side` (UNIQUE parcial por estados activos)

### Recovery
- Estado clave (watchlist, scanner states, planes) persiste tras reinicio.

### Limitaciones conocidas
- `plan_fingerprint` obligatorio hoy por código; `NOT NULL` físico en DB puede considerarse mejora opcional.
- Motivo de cancelación en repair de migración queda en logs; no necesariamente en metadata persistida de cada fila.

---

## 13. Cooldown

- Existe cooldown mínimo entre ciclos de planes por `symbol+direction`.
- Se evalúa dentro del lock.
- Prioridad: primero active guard, luego cooldown.
- Estados terminales habilitan nuevo ciclo (según política actual).
- Hay tests de cobertura para escenarios de bloqueo/permiso por cooldown.

---

## 14. Plan monitor

Archivo: `app/plan_monitor.py`.

### Función
- Evalúa planes abiertos contra precio actual.
- Registra eventos (`ENTRY_TOUCHED`, `TPx`, `SL`, etc.) y actualiza estado.
- Emite notificaciones por canal de alertas.

### Nota técnica importante
Con datos OHLC agregados, **no siempre se puede inferir orden intravela** cuando una vela toca SL y TP en el mismo período.

- Política conservadora intravela: **pendiente explícita** (ver sección 20).

---

## 15. Telegram bot

Archivo: `app/telegram_command_bot.py`.

### Comandos reales disponibles (extracto operativo)
- `/start`
- `/help` (`/comandos` alias)
- `/menu`
- `/status`
- `/trades`
- `/watchlist`
- `/add watchlist <SYMBOL>`
- `/remove watchlist <SYMBOL>`
- `/binance`
- `/binance_status`
- `/plan`, `/plans`, `/plan_status`, `/cancel_plan`
- `/monitor_status`
- y comandos de gestión de trade legacy (`/addtrade`, `/setsl`, `/settp`, etc.).

### Nota sobre `/quitar`
- El comando `/quitar` **no existe** actualmente.
- Equivalente operativo: `/remove watchlist <SYMBOL>`.

### Efecto de remover símbolo de watchlist
- El símbolo se elimina de watchlist.
- Se limpian setups armados (`ARMED_M15_SETUP`) asociados a ese símbolo en SQLite.

### Qué NO hace Telegram bot
- No ejecuta órdenes.
- No modifica posiciones reales de Binance.

---

## 16. Configuración

Variables principales (`.env.example`):

```env
BINANCE_API_KEY=
BINANCE_API_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
BINANCE_TESTNET=false
DRY_RUN=true
LOG_LEVEL=INFO

BINANCE_CACHE_ENABLED=true
BINANCE_GUARD_ENABLED=true
BINANCE_BACKOFF_ENABLED=true
BINANCE_RATE_LIMIT_PAUSE_SECONDS=60
MARK_PRICE_CACHE_TTL=2
KLINES_1M_CACHE_TTL=15
KLINES_3M_CACHE_TTL=30
KLINES_5M_CACHE_TTL=45
KLINES_15M_CACHE_TTL=90
POSITION_CACHE_TTL=10
EXCHANGE_INFO_CACHE_TTL=3600

SCANNER_ENABLED=true
SCANNER_ALERTS_ENABLED=true
SCANNER_INTERVAL_SECONDS=60
SCANNER_ALERT_LOW_QUALITY=false
SCANNER_ALERT_MOMENTUM_CHASE=false
SCANNER_MIN_QUALITY=MEDIA
SCANNER_REQUIRE_ADX_NOT_WEAK=true
SCANNER_REQUIRE_STRUCTURE_CONFIRMATION=false
SCANNER_BLOCK_DRY_VOLUME=true

STRUCTURE_ENABLED=true
STRUCTURE_TIMEFRAMES=15m,5m,3m
PIVOT_WINDOW=3
PULLBACK_TOLERANCE_MODE=atr
PULLBACK_ATR_MULT=0.25
PULLBACK_PCT=0.15

TRADE_PLANNER_ENABLED=true
PLAN_MONITOR_ENABLED=true
PLAN_MONITOR_INTERVAL_SECONDS=15
PLAN_EXPIRE_MINUTES=120
PLAN_USE_VOLUME_PROFILE=false
PLAN_USE_ORDER_BLOCKS=true
PLAN_USE_FIB=true
PLAN_MIN_RR_TP1=1.0
PLAN_ATR_BUFFER_MULT=0.25

ENABLE_DIRECT_M15_PLANS=false
ARMED_M15_TTL_CANDLES=2
```

### Flags clave
- `ENABLE_DIRECT_M15_PLANS=false`:
  - M15 arma setup.
  - Lower TF activa dentro del TTL.
- `ENABLE_DIRECT_M15_PLANS=true`:
  - M15 puede crear plan directo (si pasa gates).
- `ARMED_M15_TTL_CANDLES`:
  - cantidad de velas M15 de vigencia para setup armado.

---

## 17. Instalación y ejecución

### Windows (Python 3.12 recomendado)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

### Ejecución

```powershell
python -m app.main
```

### Compatibilidad de versión
- Requerido: **Python 3.11+** (`enum.StrEnum`).
- Recomendado: **Python 3.12**.
- Python 3.10: no compatible para suite completa.

---

## 18. Testing

### Comandos recomendados

```bash
python -m compileall app/main.py app/storage.py app/scanner.py app/plan_monitor.py tests/test_plan_generation_flow.py tests/test_trade_planner.py tests/test_telegram_command_bot.py tests/test_storage_trade_plan_constraints.py

python -m pytest -q tests/test_storage_trade_plan_constraints.py
python -m pytest -q tests/test_plan_generation_flow.py
python -m pytest -q tests/test_trade_planner.py
python -m pytest -q tests/test_binance_guard.py
python -m pytest -q
```

### Cobertura relevante esperada
- M1/M3/M5 no crean plan sin setup M15.
- source candle abierta no activa.
- execution candle abierta no activa.
- armed_m15 expirado no activa.
- direct M15 disabled: setup-only.
- direct M15 enabled: permite plan directo.
- carrera same fingerprint: máximo 1 plan.
- carrera different fingerprint same symbol+direction: máximo 1 activo.
- recovery tras reinicio: no duplicación por fingerprint.
- `plan_fingerprint` obligatorio.
- orden LONG/SHORT de TP/SL.
- set canónico activos/terminales.
- repair de migración de índice parcial.
- `TP3_HIT` terminal.
- logging explícito setup-only.

### Nota de validación pendiente
En este entorno de trabajo, `pytest` end-to-end no se validó por Python 3.10.19.
La validación completa debe correrse en Python 3.12 real.

---

## 19. Observabilidad / logging

Eventos y razones relevantes a observar:
- `PLAN_GATE`
- `ARMED_M15`
- `PLAN_CREATE`
- `duplicate_suppressed`
- `active_plan_exists`
- `cooldown_active`
- `missing_armed_m15_setup`
- `armed_m15_expired`
- `source_candle_not_closed`
- `execution_candle_not_closed`
- `armed_m15_setup_only`
- `duplicate_active_plans_detected`
- `trade_plan_active_index_status`

> Algunos labels pueden aparecer como texto libre en logs de implementación; mantener criterio de parseo flexible en observabilidad externa.

---

## 20. Known Issues / Pendientes técnicos

### P0
- Ejecutar suite completa en Python 3.12 real (validación final pendiente de entorno).

### P1
- Definir política conservadora intravela cuando SL y TP ocurren en la misma vela.
- Revalidar RR mínimo tras refinement de entrada (si se agrega/ajusta lógica).
- Confirmar límites Binance API/weight con watchlists grandes.
- Revisar granularidad cache por `symbol+timeframe` y TTL en escenarios de alta carga.
- Evaluar migración física `NOT NULL` para `plan_fingerprint`.
- Evaluar persistencia de reason/metadata de cancelación por migración en DB.

### P2
- Dashboard local de observabilidad.
- Mejoras de scoring multi-timeframe.
- Extensiones de volume profile/POC.
- Señales de price action adicionales.
- UX adicional en Telegram.

---

## 21. Roadmap técnico

### Prioridad alta
- Cierre de validación QA en Python 3.12.
- Política intravela conservadora.
- Hardening de constraints/migraciones DB en producción.

### Prioridad media
- Mejoras de calidad de señal multi-timeframe.
- Ajuste de cache y backoff según métricas reales.
- Mejor trazabilidad de lifecycle en DB.

### Prioridad baja
- Dashboard local.
- Mejoras de UI/UX en Telegram.
- Nuevas features de análisis cuantitativo.

---

## 22. Reglas de seguridad

- Este sistema es **read-only / DRY_RUN**.
- No está diseñado para autoejecución de órdenes.
- No usar como trading automático sin auditoría adicional.
- Las alertas no constituyen recomendación financiera.
- Validar manualmente toda decisión operativa.
- Hacer paper trading/backtesting antes de uso real.

---

## 23. Glosario

- **source_tf**: timeframe que originó la señal.
- **execution_tf**: timeframe exigido para evaluar creación de plan (actualmente 15m).
- **source_candle_closed**: indica si la vela de source_tf está cerrada.
- **execution_candle_closed**: indica si la vela de execution_tf está cerrada.
- **M15Setup**: setup estructural de referencia creado en M15.
- **LowerTFTrigger**: activación desde M1/M3/M5 sobre setup M15 vigente.
- **TradePlan**: plan teórico con entry/SL/TP y estado lifecycle.
- **plan_fingerprint**: clave de idempotencia de plan.
- **active plan**: plan en estados no terminales canónicos.
- **cooldown**: ventana mínima entre ciclos de planes.
- **armed_m15**: setup M15 persistido y vigente para activación.
- **TTL**: tiempo de vigencia del setup armado.
- **SQLite partial unique index**: índice único condicionado por estados activos.
- **TP/SL**: take profit / stop loss.
- **RR**: relación riesgo/retorno.
- **OHLC**: open/high/low/close.
- **intravela**: secuencia de movimientos dentro de una vela agregada.

---

## Disclaimer

Este repositorio es para monitoreo técnico y generación de alertas/planes teóricos.
No sustituye criterio profesional ni gestión de riesgo personal.
