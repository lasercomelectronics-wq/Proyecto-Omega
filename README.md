# trading_alert_bot

Bot de monitoreo y alertas en modo read-only para Binance USDⓈ-M Futures y Telegram. Esta primera versión solo lee precios, sincroniza posiciones abiertas, compara contra `config/trades.yaml` y envía alertas inteligentes. No ejecuta órdenes, no crea market buy/sell y no expone secretos.

## Características

- Python 3.12+ con `asyncio`.
- Cliente REST firmado solo para lectura sobre Binance USDⓈ-M Futures.
- WebSocket de `mark price` con reconexión automática.
- User Data Stream con `listenKey`, keepalive y alertas de renovación/expiración.
- Configuración por YAML para trades activos.
- Persistencia local en SQLite para historial de alertas y estado runtime.
- Notificaciones a Telegram con prioridad `INFO`, `WARNING` y `CRITICAL`.
- Cooldown anti-spam por alerta.
- Motor de estrategia con funciones puras:
  - distancia a SL y TPs
  - zona de invalidación
  - add zones
  - PnL aproximado long/short
  - alerta de break even
  - estructura con swing highs/lows
  - EMA 55 / EMA 200
  - bias bullish / bearish / neutral

## Alcance operativo

Esta versión funciona como un centinela de monitoreo. El bot no abre posiciones, no cierra posiciones, no modifica stop loss, no toma ganancias automáticamente, no promedia entradas y no ejecuta ninguna orden sobre Binance.

Su función es exclusivamente leer datos de Binance, compararlos contra `config/trades.yaml` y enviar alertas a Telegram cuando detecta condiciones relevantes de riesgo, invalidación, take profit, break even, add zone o desalineación entre configuración y posiciones reales. La decisión operativa final queda a cargo del usuario.

## Fuente de precios

- `mark price` se usa para evaluación de riesgo, PnL aproximado y distancia al stop loss.
- `klines`/velas se usan para calcular EMA 55, EMA 200 y estructura básica de mercado.
- `entry price` se sincroniza desde Binance cuando existe una posición abierta.
- En futuras versiones, las alertas de TP/SL deberían poder configurarse para usar `mark_price` o `last_price`, según el criterio operativo deseado.

## Temporalidad de estrategia

Cada trade puede definir `strategy.timeframe` como referencia operativa. Sobre ese timeframe se interpretan las EMAs, los swing highs/lows y el bias contextual del setup.

Ejemplos típicos:

- `1m` para scalping agresivo.
- `5m` para confirmación más limpia.
- `15m` para contexto superior.

Nota de implementación actual: la base del proyecto ya calcula contexto técnico con velas y mantiene un `kline_interval` global en `config/settings.yaml`. El campo `strategy.timeframe` queda documentado como extensión natural por trade para próximas iteraciones sin alterar el alcance read-only.

## Seguridad

- Creá la API Key de Binance sin permisos de retiro.
- Para esta versión, la recomendación es usar permisos de solo lectura.
- El proyecto no incluye endpoints de creación de órdenes.
- No hay lógica de `market buy`, `market sell`, `new order` ni auto-ejecución.
- El secret de Binance no se guarda en SQLite.
- Los logs aplican redacción preventiva de credenciales.
- El bot nunca envía secretos a Telegram.

## Estructura

```text
trading_alert_bot/
  app/
  config/
  data/
  tests/
  README.md
  requirements.txt
  .env.example
  .gitignore
```

## Instalación

1. Abrí una terminal en:

```powershell
cd "$env:USERPROFILE\Documents\Proyectos\trading_alert_bot"
```

2. Creá y activá un entorno virtual:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Si PowerShell bloquea la activación del entorno virtual, ejecutá:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Luego volvé a activar:

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Instalá dependencias:

```powershell
pip install -r requirements.txt
```

5. Copiá el ejemplo de entorno:

```powershell
Copy-Item .env.example .env
```

## Configuración de `.env`

Variables requeridas:

```env
BINANCE_API_KEY=
BINANCE_API_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
BINANCE_TESTNET=false
DRY_RUN=true
LOG_LEVEL=INFO
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
```

- `BINANCE_TESTNET=true` usa el entorno demo de futures si querés validar conectividad sin operar sobre la cuenta principal.
- `DRY_RUN=true` fuerza el modo de monitoreo sin ejecución. En esta versión debe permanecer en `true`.
- `LOG_LEVEL` permite controlar la verbosidad de logs.

## Cómo crear el bot de Telegram

1. Abrí Telegram y hablá con [@BotFather](https://t.me/BotFather).
2. Ejecutá `/newbot`.
3. Elegí nombre y username.
4. Copiá el token generado y pegalo en `TELEGRAM_BOT_TOKEN`.

## Cómo obtener `chat_id`

1. Escribile cualquier mensaje a tu bot, por ejemplo `/start`.
2. Abrí esta URL en el navegador reemplazando `TOKEN`:

```text
https://api.telegram.org/botTOKEN/getUpdates
```

3. Buscá el bloque `chat` y copiá el valor de `id`.
4. Pegalo en `TELEGRAM_CHAT_ID`.

## Control por Telegram

El proyecto incluye una interfaz de control por Telegram basada en polling con `getUpdates`. No usa webhooks en esta versión. Solo acepta comandos desde el `TELEGRAM_CHAT_ID` autorizado; cualquier otro chat recibe `Unauthorized` o es ignorado por la lógica operativa.

Los trades administrados desde Telegram se persisten en SQLite y pasan a ser la fuente activa del bot. `config/trades.yaml` sigue existiendo como fuente inicial opcional o respaldo: si la base está vacía al arrancar, el bot importa esos trades una sola vez.

Comandos disponibles:

- `/start`
- `/help`
- `/status`
- `/trades`
- `/addtrade`
- `/cancel`
- `/close SYMBOL`
- `/delete SYMBOL`
- `/pause SYMBOL`
- `/resume SYMBOL`
- `/setsl SYMBOL PRICE`
- `/addtp SYMBOL PRICE`
- `/settp SYMBOL PRICE1,PRICE2,PRICE3`
- `/setinvalidation SYMBOL MIN MAX`
- `/setentry SYMBOL PRICE`
- `/note SYMBOL texto`

Flujo interactivo de `/addtrade`:

1. `symbol`
2. `side` (`LONG` o `SHORT`)
3. `entry`
4. `leverage`
5. `stop_loss`
6. `take_profits` separados por coma
7. `invalidation_zone` como `MIN MAX` o `skip`
8. `timeframe`
9. `note` opcional
10. confirmación final con `yes/no`

El control por Telegram no ejecuta órdenes, no modifica posiciones en Binance y no cambia el alcance read-only del proyecto. Se usa únicamente como interfaz de configuración operativa y recepción de alertas.

## Cómo cargar operaciones en `config/trades.yaml`

Cada trade declarado puede incluir:

- `symbol`
- `side`
- `entry`
- `leverage`
- `stop_loss`
- `take_profits`
- `invalidation_zone`
- `add_zones`
- `alerts`
- `strategy`
- `note`

Ejemplo de referencia:

```yaml
trades:
  - symbol: WLDUSDT
    side: SHORT
    entry: 0.8912
    leverage: 20
    stop_loss: 0.9050

    take_profits:
      - 0.8820
      - 0.8740
      - 0.8600

    invalidation_zone:
      min: 0.9020
      max: 0.9060

    add_zones:
      - price: 0.8980
        size_usdt: 1
        note: "Micro add solo si hay rechazo y bajo volumen comprador"

    alerts:
      cooldown_seconds: 300
      sl_distance_threshold_pct: 0.35
      notify_on_tp: true
      notify_on_sl_distance: true
      notify_on_invalidation_zone: true
      notify_on_break_even: true
      notify_on_add_zone: true

    strategy:
      timeframe: 1m
      ema_fast: 55
      ema_slow: 200
      use_structure: true
      pivot_length: 5
      use_squeeze_momentum_placeholder: true

    note: "Short agresivo monitoreado manualmente."
```

Nota de compatibilidad actual: el ejemplo anterior documenta la forma objetivo de configuración por trade. En la implementación actual, el intervalo de velas efectivo se controla globalmente desde `config/settings.yaml`, y algunos overrides finos como `strategy.timeframe`, `alerts.sl_distance_threshold_pct` o `alerts.notify_on_add_zone` quedan documentados para evolución incremental del proyecto. Esto no cambia el alcance read-only ni la lógica actual de monitoreo.

## Alertas mínimas implementadas

1. Precio entra en zona de invalidación.
2. Distancia porcentual al stop loss cae por debajo del umbral configurado.
3. TP1/TP2/TP3 alcanzado.
4. Precio vuelve a entrada después de haber estado en ganancia.
5. Precio toca zona de add.
6. Posición abierta en Binance no declarada en `trades.yaml`.
7. Trade declarado en `trades.yaml` sin posición abierta en Binance.
8. WebSocket desconectado.
9. Error de API.
10. `listenKey` próximo a expirar, renovado o expirado.

## Archivos ignorados por Git

Se recomienda que `.gitignore` excluya al menos:

```text
.env
.venv/
__pycache__/
.pytest_cache/
data/*.sqlite
data/*.sqlite-shm
data/*.sqlite-wal
logs/
```

## Ejecución

Con el entorno virtual activo:

```powershell
python -m app.main
```

También queda listo para abrir en VS Code con:

```powershell
code "$env:USERPROFILE\Documents\Proyectos\trading_alert_bot"
```

## Tests

```powershell
pytest
```

## Persistencia

- `data/bot.sqlite` guarda historial de alertas, posiciones cacheadas y estado runtime.
- No almacena `BINANCE_API_SECRET`.

## Advertencia de riesgo

El trading con leverage puede generar pérdidas rápidas y liquidaciones. Este bot no constituye recomendación financiera. Usalo solo como herramienta de monitoreo y validá manualmente cualquier decisión operativa.

## Roadmap

- Paper trading con journal de entradas/salidas simuladas.
- Dashboard local para ver trades y alertas.
- Re-suscripción dinámica de símbolos al detectar nuevos instrumentos.
- Ejecución real opcional, separada y explícitamente habilitable.
- Capas extra de risk management antes de cualquier automatización de órdenes.
