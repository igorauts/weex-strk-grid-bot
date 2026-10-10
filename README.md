# WEEX STRK Grid Bot

A long-only grid trading bot for **STRKUSDT perpetuals on WEEX**, built around exchange-side take-profit orders, persistent recovery logic, WebSocket monitoring, and read-only performance statistics.

> **Current version:** `bot06 v0.6.8.2`

## Strategy

Current production configuration:

| Parameter | Value |
|---|---:|
| Symbol | `STRKUSDT` |
| Direction | LONG only |
| Grid step | `0.00010` USDT |
| Order size | `20 STRK` |
| Take profit distance | `+0.00030` USDT |
| TP distance in grid steps | `3` |
| Active BUY LIMIT target | `20` |
| Maximum BUY price | `0.08500` |
| Lower bound | None |
| Maximum grid orders | `351` |
| Maximum potential position | `7020 STRK` |

Opening orders are **POST_ONLY LIMIT BUY** orders.

Normal exits are attached exchange-side **TAKE_PROFIT_MARKET** orders. This means protection remains on the exchange even if the Python process or VPS becomes temporarily unavailable.

## Core behavior

The bot maintains a moving set of 20 active BUY LIMIT orders.

When a BUY fills:

1. The position is tracked as waiting for TP.
2. The attached exchange-side TP is monitored.
3. When the TP completes, the grid level becomes eligible for re-entry.
4. TP events are collected in a short batch window.
5. For LONG positions, higher entry levels are re-armed first.
6. After the batch, the bot performs one grid repair.
7. Excess lower BUY orders are removed first.

The strategy intentionally has **no lower grid boundary**.

## TP recovery

WEEX has occasionally filled an opening BUY without successfully creating the attached TP.

The bot therefore includes recovery protection:

- checks active and historical TP state;
- confirms that the original attached TP is really absent;
- creates a standalone recovery TP only when required;
- uses deterministic `clientAlgoId` values;
- never blindly retries an ambiguous recovery POST;
- re-queries the deterministic recovery ID before retrying.

Recovery TP properties:

```text
positionSide = LONG
triggerPriceType = CONTRACT_PRICE
executePrice = 0
reduceOnly = true
qty = exact opening position size
```

## WebSocket and REST safety

The bot uses:

- public WebSocket BID updates;
- private `orders` WebSocket events;
- private `fill` WebSocket events;
- REST fallback and safety reconciliation;
- fill-ID deduplication;
- partial-fill accumulation;
- persistent raw private WebSocket logging for diagnostics.

Trading state transitions are designed to be idempotent so that WebSocket and REST recovery cannot process the same fill twice.

## Statistics

`v0.6.8.x` added persistent statistics and historical backfill.

The statistics cache stores enough information to keep completed cycles measurable even after completed parent orders disappear from the live trading state.

The bot reports:

- closed grid cycles;
- open BUY LIMIT orders;
- positions waiting for TP;
- grid profit;
- paid fees;
- total volume;
- maker volume;
- taker volume;
- open position;
- unrealized PnL;
- total PnL.

### Manual statistics refresh

On the VPS:

```bash
stats06
```

`stats06` sends `SIGUSR1` to the running bot and waits for a fresh statistics block without restarting or pausing the strategy.

Default timeout:

```text
180 seconds
```

You can override it temporarily:

```bash
STATS06_TIMEOUT=300 stats06
```

## Historical backfill fix in v0.6.8.2

WEEX `/capi/v3/order/history` was observed to return the same newest 100 records even when the `page` parameter changed.

As a result, normal pagination could silently miss older opening BUY orders.

`v0.6.8.2` no longer relies on pagination for statistics backfill.

Instead:

1. historical requests are kept below the WEEX 7-day API limit;
2. any time window returning 100 records is treated as potentially truncated;
3. that time range is recursively split into smaller non-overlapping intervals;
4. results are deduplicated by order identity;
5. backfill fails loudly rather than silently accepting incomplete history.

This restored historical parent mappings and removed false unmatched-cycle warnings.

## Observed TP execution

During production testing, exchange-side `TAKE_PROFIT_MARKET` execution was audited against actual WEEX fills.

A read-only audit verified:

- every sampled completed TP had a unique SELL trade ID;
- every sampled completed TP had a unique SELL order ID;
- no reused trade IDs;
- no partial SELL fills in the audited sample;
- WEEX algo `actualPrice` matched `/userTrades` SELL price.

In the audited production sample, the average realized TP distance was slightly **better than the nominal 3 grid steps** because fast upward moves occasionally produced positive market slippage.

These observations are historical measurements, not a guarantee of future execution quality.

## VPS / systemd

Example service commands:

```bash
systemctl status weex-bot06 --no-pager
journalctl -u weex-bot06 -f
journalctl -u weex-bot06 -n 80 --no-pager
systemctl restart weex-bot06
systemctl stop weex-bot06
systemctl start weex-bot06
```

To stop the service and disable automatic startup:

```bash
systemctl disable --now weex-bot06
```

Important: stopping the Python process does **not** automatically cancel exchange-side orders. Always check WEEX after stopping the service.

## Installation

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create your environment file from the example:

```bash
cp .env.example .env
chmod 600 .env
```

Fill in your own WEEX API credentials.

Recommended API permissions:

- trading: ON;
- read access: ON;
- withdrawals: OFF;
- IP whitelist: recommended for a fixed VPS.

Never commit `.env` or API credentials.

## Files that must stay private

Do **not** commit:

```text
.env
state06.json
stats_state06.json
logs/
```

The repository `.gitignore` should exclude runtime state, statistics state, logs, and credentials.

## Version history

### v0.6.8.2

- fixed incomplete WEEX `order/history` backfill caused by broken/ignored pagination;
- added recursive time-window splitting for dense historical ranges;
- preserves the WEEX maximum historical request window;
- restored complete parent BUY mappings for statistics;
- eliminated false unmatched-cycle warnings;
- production `stats06` timeout increased to 180 seconds.

### v0.6.8.1

- split statistics history requests into safe sub-7-day windows;
- fixed WEEX `-1170: Time range cannot exceed 7 days`.

### v0.6.8

- persistent `clientOrderId -> orderId` statistics mapping;
- historical cycle recovery after parent deletion from live state;
- statistics migration/backfill;
- manual `SIGUSR1` statistics refresh;
- `stats06` helper.

### v0.6.7

- previous production baseline.

## Risk warning

This project is experimental trading software.

It can place and cancel real orders and can create leveraged market exposure. Exchange APIs, WebSocket behavior, order semantics, liquidity, fees, slippage, outages, and rate limits can change without notice.

Review the code, test with small size, monitor exchange-side positions and orders, and use it at your own risk.

## License

No license is currently granted unless a license file is added to the repository.
