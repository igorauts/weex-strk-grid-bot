# bot06 v0.6.7 — VPS setup

## What v0.6.7 changes

- Safe `AUTO_RESUME=1`: existing bot can restart without stdin.
- Automatic first `START06` is forbidden. Missing state/cycles causes exit code 1 instead of silently creating a new grid.
- Runtime paths are relative to the location of `bot06.py`, not shell cwd.
- Only `fcntl.flock` blocks a second bot06. The old `ps` scanner is removed.
- REST reconcile cannot re-enable trading while private orders WS is down.
- Statistics do not hold `maintenance_lock`.
- All signed/private REST calls in this process share one limiter.
- Fatal crashes exit with code 1.
- Optional Telegram alerts for critical PAUSE, TP cancellation, fatal crash, long private-WS outage/recovery.
- External `logrotate` prevents logs from filling the VPS disk.
- Dependencies are pinned.

## Before migration

Copy the current live files from the Mac:

```text
state06.json
stats_state06.json
```

Never run the Mac bot and VPS bot at the same time for the same strategy/account.

## Install on Ubuntu

As root:

```bash
adduser --system --group --home /opt/weex-bot weexbot
mkdir -p /opt/weex-bot/logs
chown -R weexbot:weexbot /opt/weex-bot
```

Put these files into `/opt/weex-bot`:

```text
bot06.py
requirements.txt
weex-bot06.service
weex-bot06.logrotate
.env
state06.json
stats_state06.json
```

Create Python environment:

```bash
python3 -m venv /opt/weex-bot/.venv
/opt/weex-bot/.venv/bin/pip install --upgrade pip
/opt/weex-bot/.venv/bin/pip install -r /opt/weex-bot/requirements.txt
chown -R weexbot:weexbot /opt/weex-bot
chmod 600 /opt/weex-bot/.env
chmod 600 /opt/weex-bot/state06.json /opt/weex-bot/stats_state06.json
```

## First VPS validation — manual

Temporarily put this in `.env`:

```text
AUTO_RESUME=0
```

Run:

```bash
cd /opt/weex-bot
sudo -u weexbot /opt/weex-bot/.venv/bin/python bot06.py
```

Enter:

```text
RESUME06
```

Check that WEEX reconciliation succeeds, then stop with `Ctrl+C`.

Now set:

```text
AUTO_RESUME=1
```

## systemd

```bash
cp /opt/weex-bot/weex-bot06.service /etc/systemd/system/weex-bot06.service
systemctl daemon-reload
systemctl enable weex-bot06
systemctl start weex-bot06
systemctl status weex-bot06 --no-pager
```

Live journal:

```bash
journalctl -u weex-bot06 -f
```

Restart/stop:

```bash
systemctl restart weex-bot06
systemctl stop weex-bot06
```

## logrotate

```bash
cp /opt/weex-bot/weex-bot06.logrotate /etc/logrotate.d/weex-bot06
chmod 644 /etc/logrotate.d/weex-bot06
logrotate -d /etc/logrotate.d/weex-bot06
```

Bot logs:

```bash
tail -f /opt/weex-bot/logs/bot06.log
tail -f /opt/weex-bot/logs/private_ws_raw06.log
```

## Security

Use a WEEX API key with trading permission only, no withdrawal permission.
Whitelist the final Tokyo VPS IP on WEEX if supported.

```bash
chmod 600 /opt/weex-bot/.env
```

## Telegram alerts

Optional in `.env`:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

Leave both blank to disable.

## 10–20 bots later

The v0.6.7 REST limiter is process-wide. It prevents bot06's own stats/safety/reconcile/trading requests from bursting.

With 10–20 separate bot processes on one WEEX account, each process still has its own limiter. At that point use one shared account-level private WS / signed REST service rather than multiplying independent account polling loops.
