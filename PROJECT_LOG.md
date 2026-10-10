# WEEX STRK Grid Bot — Project Log

_Last updated: 10 October 2026_

## Current production branch

```text
bot06.py
version: 0.6.7
symbol: STRKUSDT
prefix: strk6-
```

The bot is currently operated as a production service on a Tokyo VPS.

The public repository snapshot intentionally excludes real API credentials, runtime state, order IDs, logs, passwords, and other secrets.

## 1. Strategy snapshot

```text
SYMBOL = STRKUSDT
Direction = LONG only
QTY = 20 STRK
STEP = 0.00010
TP_DISTANCE = 0.00030
ACTIVE BUY LIMIT TARGET = 20
MAX_GRID_ORDERS = 351
MAX_STRK = 7,020 STRK
MAX_BUY_PRICE = 0.08500
LOWER PRICE LIMIT = none
PRICE_TICK = 0.00001
```

TP is exactly **3 grid steps**. `0.05000` is not a hard floor.

## 2. Grid behavior

The active ladder is anchored to the nearest exact grid price strictly below current BID.

The bot maintains **20 correct active BUY LIMIT levels**, not merely a raw count of 20 open orders.

A price level is reserved while its cycle is still OPEN / WAIT_TP / re-arming, so duplicate live cycles at the same grid price are not allowed.

## 3. TP batch re-arm

Current behavior:

```text
TP collection window = 100 ms
LONG re-arm order = highest entry -> lowest entry
repair active grid = once after full batch
trim extras = lowest first
```

This makes TP recovery deterministic and prioritizes levels nearest current price after an upward move.

## 4. Attached TP and recovery TP

Live STRK testing exposed a WEEX edge case: some filled BUY orders were accepted with attached TP parameters, but the TP order was not actually created by the exchange.

`v0.6.6` introduced recovery protection:

```text
FILLED BUY
-> fresh TP active/history re-check
-> if TP exists: adopt it
-> if TP already completed: process completion
-> if TP truly absent: place standalone recovery TP
-> if recovery cannot be safely confirmed: PAUSE
```

Recovery TP creation uses a deterministic client ID and avoids blind duplicate POST retries after ambiguous timeouts.

## 5. TP parent linkage

Primary linkage is based on:

```text
parent BUY clientOrderId
<-> TP clientAlgoId
```

Private WS may additionally supply `openTpslParentOrderId`.

Statistics use the same deterministic linkage.

## 6. Market data

Primary BID:

```text
wss://ws-contract.weex.com/v3/ws/public
channel: depth15
```

REST bookTicker is fallback only.

## 7. Private WS

Endpoint:

```text
wss://ws-contract.weex.com/v3/ws/private
```

Subscriptions:

```text
orders
fill
```

The bot requires subscription ACK before the private stream is considered healthy. A watchdog reconnects a silent/zombie stream.

## 8. REST safety / reconciliation

Important rules:

```text
WEEX = source of truth after downtime
local state = memory, not absolute truth
ambiguous POST timeout != confirmed failure
blind duplicate POST retry = forbidden
```

`v0.6.7` introduces one process-wide signed/private REST gate so trading, stats, safety and reconcile do not burst in parallel inside bot06.

## 9. Statistics

Statistics are based on real executions:

```text
GRID PROFIT = actual SELL fill - actual BUY fill - BUY fee - SELL fee
```

Tracked output includes closed cycles, grid profit, maker/taker volume, paid fees, open position, unrealized PnL and total PnL.

`v0.6.7` removes statistics from `maintenance_lock`, so a long statistics refresh cannot delay private-WS recovery or reconcile.

## 10. VPS migration

The current production deployment was moved to a Tokyo VPS after comparing several regions against the WEEX endpoint.

Production service:

```text
service: weex-bot06
status: active (running)
enabled: yes
restart policy: always
```

A real systemd restart was tested and the bot successfully performed automatic RESUME.

The exact VPS IP is deliberately omitted from this public repository.

## 11. Safe unattended startup

`v0.6.7` adds:

```text
AUTO_RESUME=1
```

This only resumes an existing strategy. Automatic first `START06` / automatic grid seeding is intentionally forbidden.

## 12. Process lock

The old `ps`-based duplicate-bot detector was removed. Current duplicate protection uses only:

```text
fcntl.flock
```

## 13. Runtime paths

State, logs, environment and lock files are based on:

```text
Path(__file__).resolve().parent
```

The bot no longer depends on shell cwd.

## 14. Crash handling

Unhandled fatal errors:

```text
log fatal error
optional Telegram alert
exit code = 1
```

systemd is configured with:

```text
Restart=always
RestartSec=5
```

## 15. Telegram alerts

`v0.6.7` includes optional alert support for:

```text
critical PAUSE
TP cancellation
fatal crash
prolonged private-WS outage
private-WS recovery
```

Telegram credentials are optional environment values and are not part of the public repository.

## 16. Log rotation

Current production policy:

```text
daily
rotate 14
maxsize 1G
compress
delaycompress
missingok
notifempty
copytruncate
```

## 17. Dependencies

Pinned:

```text
python-dotenv==1.2.4
websockets==17.2
```

## 18. Security model

Production recommendations:

```text
.env mode 600
API key: trading/read only
withdrawal permission: disabled
API key IP whitelist: production VPS only, if supported by WEEX
```

## 19. Files intentionally NOT in Git

```text
.env
state06.json
stats_state06.json
logs/
*.lock
```

A stale `state06.json` must never be blindly restored and used as truth. After recovery, reconciliation with WEEX comes first.

## 20. Future scale

For roughly **10–20 bots** on one WEEX account, move to a shared account-level layer:

```text
one private WS
one signed REST/request limiter
one account/order monitor
many strategy instances
```
