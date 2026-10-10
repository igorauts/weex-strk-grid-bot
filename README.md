# WEEX STRK Grid Bot

Current version: **bot06 v0.6.7**

Production-oriented LONG-only grid bot for **STRKUSDT** on WEEX.

The strategy logic originated from the earlier CLUSDT bot and was adapted for a much denser STRK grid, then hardened through live operation: WS/REST reconciliation, recovery after missed TP creation, deterministic TP re-arm batching, safe restart, VPS/systemd support, and log rotation.

> Real-money trading software. Use at your own risk. Verify the account, API permissions, live orders, and state before enabling unattended operation.

## Current strategy

| Parameter | Value |
|---|---:|
| Symbol | `STRKUSDT` |
| Direction | LONG only |
| Grid step | `0.00010` |
| Take Profit | `+0.00030` |
| TP distance | `3` grid steps |
| BUY size | `20 STRK` |
| Active BUY LIMIT target | `20` |
| Maximum tracked grid slots | `351` |
| Maximum potential STRK load | `7,020 STRK` |
| Maximum new BUY price | `0.08500` |
| Lower price boundary | none |
| Entry type | `LIMIT + POST_ONLY` |
| TP | exchange-side TP |

`0.05000` is **not** a hard lower boundary. It was only used during early grid-density calculations. The grid may continue lower, while the hard `351` slot / `7,020 STRK` cap limits total potential exposure.

## Grid geometry

The working ladder is anchored to the nearest exact `0.00010` level strictly below current BID.

```text
BID 0.07407 -> top BUY 0.07400
BID 0.07400 -> top BUY 0.07390
```

The bot aims to keep **20 correct unfilled BUY LIMIT levels**, not merely “20 orders somewhere”.

No duplicate live cycle is allowed on the same grid price while an earlier cycle at that price is still OPEN / waiting for TP / being re-armed.

## TP and re-arm

Normal entry BUYs are submitted with an attached exchange-side TP:

```text
TP = entry + 0.00030
```

Example:

```text
BUY 0.07400
TP  0.07430
```

### 100 ms TP batch

When multiple TPs complete almost simultaneously, re-arm events are collected for **100 ms**.

For LONG, the batch is restored **from the highest entry price to the lowest** — nearest to current price first.

```text
completed levels:
0.07370
0.07400
0.07380
0.07390

re-arm order:
0.07400
0.07390
0.07380
0.07370
```

After the whole batch is re-armed, the active 20-level grid is repaired **once**. Extra BUY LIMIT orders are trimmed from the lowest price upward.

## Hard risk limits

New BUY orders are never created above:

```text
0.08500
```

Maximum grid capacity:

```text
351 grid slots
x 20 STRK
= 7,020 STRK
```

The slot accounting includes live/potential BUY exposure such as open BUY orders, filled BUYs waiting for TP, and ambiguous/pending submits that WEEX may already have accepted.

The bot does **not** inspect, enforce, or modify account leverage.

## Missing attached TP recovery

Live testing exposed an exchange-side edge case: several filled BUY orders were accepted with attached TP parameters, but WEEX did not materialize the corresponding TP orders.

Starting with **v0.6.6**, a confirmed FILLED BUY with no real attached TP is handled as follows:

1. Re-check fresh active TP and TP history.
2. Avoid blind duplicate creation.
3. If the attached TP is truly absent, create a standalone exchange-side recovery TAKE PROFIT for the same size and target.
4. If recovery cannot be safely created or confirmed, PAUSE instead of continuing with an unprotected position.

Recovery TPs use their own deterministic client IDs and are mapped back to the opening BUY.

## Market data and execution monitoring

Primary BID source:

```text
wss://ws-contract.weex.com/v3/ws/public
channel: depth15
```

Private stream:

```text
wss://ws-contract.weex.com/v3/ws/private
channels: orders + fill
```

The bot waits for subscription acknowledgement before considering private WS healthy.

A private-WS watchdog forces reconnect on prolonged silence. REST safety can also request immediate reconnect after detecting an execution missed by WS.

## REST safety and reconciliation

The bot combines:

- event-driven private WS handling;
- REST safety checks;
- full reconciliation;
- pending-submit recovery after ambiguous POST timeouts;
- server-time synchronization.

A timed-out POST is **not** blindly repeated. WEEX is treated as the source of truth after downtime.

## Statistics

Closed grid profit is calculated from actual fills:

```text
actual SELL fill
- actual BUY fill
- BUY commission
- SELL commission
```

Tracked output includes:

- closed cycles;
- grid profit;
- maker/taker volume;
- paid fees;
- open position;
- unrealized PnL;
- total PnL.

TP-to-parent mapping uses deterministic `clientAlgoId -> parent client_id` logic rather than relying on triggered-order `actualOrderId`.

## VPS / production operation

**v0.6.7** adds production/VPS infrastructure without changing STRK strategy parameters:

- safe `AUTO_RESUME=1` for an existing live bot;
- automatic first `START06` is forbidden;
- runtime paths are relative to the `bot06.py` directory;
- duplicate process protection uses `fcntl.flock` only;
- private WS health is separated from REST reconciliation;
- REST reconcile alone cannot re-enable trading while private WS is unavailable;
- statistics do not hold the maintenance/reconcile lock;
- all signed/private REST calls inside the process use one request gate;
- fatal unhandled errors return exit code `1`;
- optional Telegram alerts for critical conditions;
- pinned Python dependencies;
- ready-to-use systemd unit and logrotate config.

The production deployment has been validated with systemd restart + automatic RESUME.

## Log rotation

Current intended production policy:

```text
daily
rotate 14
maxsize 1G
compress
delaycompress
```

Runtime logs themselves should **not** be committed to Git.

## Files

```text
bot06.py                  Current v0.6.7 bot
README.md                 Project overview
PROJECT_LOG.md            Development / production snapshot
VPS_SETUP.md              VPS deployment guide
requirements.txt          Pinned Python dependencies
.env.example              Safe environment template
.gitignore                Excludes secrets/runtime state
weex-bot06.service        systemd service
weex-bot06.logrotate      logrotate configuration
```

## Secrets and runtime state

Never commit:

```text
.env
state06.json
stats_state06.json
logs/
*.lock
```

The repository contains `.env.example` only.

## Multi-bot scaling

For **10–20 bots on the same WEEX account**, the next architectural step should be an account-level shared layer for private WS, signed REST, reconciliation and rate limiting instead of multiplying identical account polling across many independent processes.
