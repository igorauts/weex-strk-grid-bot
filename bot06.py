import os
import re
import sys
import time
import json
import atexit
import threading
import hmac
import fcntl
import base64
import hashlib
import asyncio
import urllib.request
import urllib.parse
import urllib.error

from decimal import Decimal, ROUND_FLOOR
from pathlib import Path

import websockets
from dotenv import load_dotenv


# =========================================================
# WEEX STRK GRID BOT v0.6.7
#
# Основа торговли сохранена из предыдущих версий.
# Актуальная логика v0.6.2:
# - отдельный namespace strk6-: нефтяной bot05 и другие боты игнорируются
# - отдельные state06/stats_state06/logs/bot06.log
# - размер одного входа = 20 STRK
# - TP = +0.00030 (3 шага сетки)
# - одновременно держим 20 НЕИСПОЛНЕННЫХ BUY LIMIT
# - поддерживаем 20 ПРАВИЛЬНЫХ grid-уровней без дырок возле текущего BID
# - TP rearm собирается в окно 100 мс; пачка восстанавливается СВЕРХУ ВНИЗ по цене
# - после всей TP-пачки repair/trim рабочей 20-уровневой сетки запускается ОДИН раз
# - после TP rearm исходных уровней лишняя нижняя лимитка убирается первой
# - жёсткий максимум сетки = 351 BUY-слот = 7,020 STRK потенциальной загрузки
# - новый верх сначала ставится и подтверждается; только потом удаляется лишний нижний BUY
# - краткий статус раз в минуту, статистика раз в 10 минут, reconcile раз в 30 минут
# - синхронизация часов WEEX и защита от stale state после неудачного reconcile
# - WebSocket считается активным только после подтверждения SUBSCRIBE result=true
# - REST safety-check каждые 3 сек страхует пропущенные WS-события и дырки сетки
# - оперативные строки терминала получают UTC timestamp
# - BID: public WebSocket depth15 is primary; REST bookTicker is fallback only
# - POST timeout becomes PENDING submit; bot confirms by clientOrderId before retry
# - private WS subscribes to BOTH orders + fill; fill is the primary execution signal
# - raw private WS events are written to logs/private_ws_raw06.log for diagnosis
# - private WS watchdog forces reconnect on a silent/zombie stream
# - REST-detected missed BUY fill immediately requests private WS reconnect
# - bot-initiated cancels are tracked so their WS CANCELED is not misclassified
# - v0.6.6: подтверждённый FILLED BUY без реально созданного attached TP автоматически получает RECOVERY TP
# - recovery TP ставится через V3 /placeTpSlOrder только ПОСЛЕ свежей проверки, чтобы не создать дубль
# - attached TP и recovery TP связываются с исходным BUY по clientAlgoId/client_id
# - actualOrderId НЕ используется как основной parent-link
# - private WS умеет сопоставлять standalone recovery TP даже при openTpslParentOrderId=0
# - если recovery TP не удалось надёжно создать/подтвердить, только тогда PAUSE
# - v0.6.7 VPS readiness: безопасный AUTO_RESUME без автоматического первого START
# - все runtime paths привязаны к директории bot06.py, а не к cwd
# - только flock защищает от второго bot06; ps-проверка удалена
# - private WS health отделён от REST reconcile: REST сам не может включить торговлю
# - статистика больше не держит maintenance_lock и не задерживает reconnect/reconcile
# - все signed private REST проходят через общий внутрипроцессный limiter
# - crash возвращает exit code 1; опциональные Telegram alerts для критических событий
# - bot НЕ проверяет и НЕ меняет плечо: leverage полностью контролируется пользователем на WEEX
# =========================================================


VERSION = "0.6.7"

BASE = "https://api-contract.weex.com"
PRIVATE_WS = "wss://ws-contract.weex.com/v3/ws/private"
PUBLIC_WS = "wss://ws-contract.weex.com/v3/ws/public"

SYMBOL = "STRKUSDT"

QTY = Decimal("20")
STEP = Decimal("0.00010")
TP_DISTANCE = Decimal("0.00030")
ACTIVE_LIMITS_TARGET = 20
MAX_GRID_ORDERS = 351
MAX_BUY_PRICE = Decimal("0.08500")
PRICE_TICK = Decimal("0.00001")
MAX_STRK = QTY * MAX_GRID_ORDERS

PRICE_POLL = 0.50  # legacy fallback constant; main BID path is now public WebSocket
TRADE_GAP = 0.20

# BID comes from public WEEX WebSocket. REST is only a fallback when the
# public stream is unavailable/stale.
PUBLIC_BID_STALE_SECONDS = 3.0
REST_BID_FALLBACK_INTERVAL = 1.0
REST_BID_TIMEOUT = 3
PUBLIC_WS_RECONNECT_DELAY = 2

# A POST timeout is ambiguous: WEEX may have accepted the order even when
# the HTTP response never reached us. Never blindly repeat such a POST.
PENDING_SUBMIT_CHECK_INTERVAL = 3
PENDING_SUBMIT_ABSENCE_CONFIRMATIONS = 2

# Private WS health. WEEX sends periodic private ping messages. Even if a
# TCP/WebSocket connection remains technically open, the account stream can
# become stale; REST safety also acts as an independent truth source.
PRIVATE_WS_WATCHDOG_TICK = 10.0
PRIVATE_WS_SILENCE_SECONDS = 180.0

# TP events can arrive almost simultaneously. Collect them for 100 ms,
# then re-arm LONG levels deterministically from the highest entry price
# down to the lowest, followed by ONE active-grid repair/trim pass.
REARM_BATCH_WINDOW = 0.100

# Reconcile race protection. A fast fill can happen after the initial
# openAlgo/allAlgo snapshot. Re-check fresh TP state before declaring ERROR_NO_TP.
TP_RECONCILE_GRACE_ATTEMPTS = 3
TP_RECONCILE_GRACE_DELAY = 0.35
TP_RECONCILE_HISTORY_LOOKBACK_MS = 5 * 60 * 1000
TP_RECONCILE_OLD_FILL_MS = 5_000
RECOVERY_TP_SUFFIX = "rtp"

# Внутренний контроль цены остаётся частым.
# Эти интервалы относятся только к выводу/контрольным задачам.
STATUS_PRINT_INTERVAL = 60
STATUS_RETRY_INTERVAL = 30
SAFETY_POLL_INTERVAL = 3
STATS_INTERVAL = 10 * 60
RECONCILE_INTERVAL = 30 * 60
STATS_OVERLAP_MS = 2 * 60 * 1000

PREFIX = "strk6-"

BOT_DIR = Path(__file__).resolve().parent
ENV_FILE = BOT_DIR / ".env"
STATE_FILE = BOT_DIR / "state06.json"
STATS_STATE_FILE = BOT_DIR / "stats_state06.json"
LOCK_FILE = BOT_DIR / ".strkusdt_bot06.lock"
LOG_DIR = BOT_DIR / "logs"
LOG_FILE = LOG_DIR / "bot06.log"
PRIVATE_WS_RAW_LOG_FILE = LOG_DIR / "private_ws_raw06.log"

UA = f"weex-strk-grid-bot/{VERSION}"

# Разница между временем WEEX и локальными часами Mac/VPS.
# Нужна, чтобы переживать сон/долгое отсутствие сети без 401 timestamp expired.
SERVER_TIME_OFFSET_MS = 0
SERVER_TIME_LOCK = threading.Lock()

# Serializes all signed/private REST consumers inside this bot process.
PRIVATE_REST_GATE_LOCK = threading.Lock()
PRIVATE_REST_LAST_START_MONO = 0.0


# =========================================================
# API КЛЮЧИ
# =========================================================

load_dotenv(ENV_FILE)

API_KEY = os.getenv("WEEX_API_KEY")
SECRET = os.getenv("WEEX_SECRET_KEY")
PASSPHRASE = os.getenv("WEEX_PASSPHRASE")

if not API_KEY or not SECRET or not PASSPHRASE:
    raise SystemExit("Нет ключей WEEX в .env")

AUTO_RESUME = os.getenv(
    "AUTO_RESUME",
    "0"
).strip().lower() in (
    "1",
    "true",
    "yes",
    "on"
)

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()

PRIVATE_REST_MIN_GAP = float(
    os.getenv(
        "WEEX_PRIVATE_REST_MIN_GAP",
        "0.20"
    )
)

PRIVATE_WS_ALERT_AFTER = float(
    os.getenv(
        "PRIVATE_WS_ALERT_AFTER",
        "60"
    )
)


# =========================================================
# RUNTIME LOG
# Один файл для всей жизни bot06: logs/bot06.log
# В терминале текст остаётся обычным, в файл добавляется timestamp.
# =========================================================

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)

_RUNTIME_LOG = open(
    LOG_FILE,
    "a",
    encoding="utf-8",
    buffering=1
)

_LOG_LOCK = threading.Lock()
_LOG_AT_LINE_START = True


def _write_runtime_log(data):

    global _LOG_AT_LINE_START

    if not data or _RUNTIME_LOG.closed:
        return

    with _LOG_LOCK:

        for piece in data.splitlines(keepends=True):

            # В терминале operational lines уже начинаются с [HH:MM:SS UTC].
            # В файл пишем одну более полную UTC-метку, без дублирования.
            clean_piece = re.sub(
                r"^\[\d{2}:\d{2}:\d{2} UTC\] ",
                "",
                piece
            )

            if _LOG_AT_LINE_START:
                _RUNTIME_LOG.write(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC] "
                )

            _RUNTIME_LOG.write(clean_piece)

            _LOG_AT_LINE_START = (
                clean_piece.endswith("\n")
                or clean_piece.endswith("\r")
            )


class _TeeStream:

    def __init__(self, terminal):
        self.terminal = terminal

    def write(self, data):

        result = self.terminal.write(data)
        self.terminal.flush()
        _write_runtime_log(data)

        return result

    def flush(self):
        self.terminal.flush()
        if not _RUNTIME_LOG.closed:
            _RUNTIME_LOG.flush()

    def isatty(self):
        return self.terminal.isatty()

    @property
    def encoding(self):
        return getattr(
            self.terminal,
            "encoding",
            "utf-8"
        )


def _close_runtime_log():

    try:
        _RUNTIME_LOG.flush()
        _RUNTIME_LOG.close()
    except Exception:
        pass


_RUNTIME_LOG.write(
    "\n"
    f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC] "
    f"================ BOT06 PROCESS START ================\n"
)
_RUNTIME_LOG.flush()

sys.stdout = _TeeStream(sys.__stdout__)
sys.stderr = _TeeStream(sys.__stderr__)

atexit.register(
    _close_runtime_log
)


_ALERT_LOCK = threading.Lock()
_ALERT_LAST_SENT = {}


def _telegram_alert_sync(
    message,
    key=None,
    cooldown=300
):
    """Best-effort Telegram alert; never raises into trading."""
    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):
        return False

    key = str(
        key or message
    )
    now = time.monotonic()

    with _ALERT_LOCK:

        last = _ALERT_LAST_SENT.get(
            key,
            0.0
        )

        if (
            cooldown > 0
            and now - last < cooldown
        ):
            return False

        _ALERT_LAST_SENT[key] = now

    try:

        data = urllib.parse.urlencode(
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": (
                    f"WEEX STRK bot06 v{VERSION}\n"
                    f"{message}"
                )
            }
        ).encode()

        request = urllib.request.Request(
            (
                "https://api.telegram.org/bot"
                + TELEGRAM_BOT_TOKEN
                + "/sendMessage"
            ),
            data=data,
            method="POST",
            headers={
                "Content-Type":
                    "application/x-www-form-urlencoded",
                "User-Agent": UA
            }
        )

        with urllib.request.urlopen(
            request,
            timeout=8
        ) as response:
            response.read()

        return True

    except Exception:
        return False


def notify_alert(
    message,
    key=None,
    cooldown=300
):
    """Non-blocking in asyncio; sync fallback after event loop is gone."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return _telegram_alert_sync(
            message,
            key,
            cooldown
        )

    loop.create_task(
        asyncio.to_thread(
            _telegram_alert_sync,
            message,
            key,
            cooldown
        )
    )

    return True


def utc_hms():
    return time.strftime(
        "%H:%M:%S",
        time.gmtime()
    )


def event_print(message=""):
    if message:
        print(
            f"[{utc_hms()} UTC] {message}"
        )
    else:
        print()



def _private_ws_diag(kind, payload=None):
    """
    Diagnostic-only private WS log.
    Nothing from this function is printed to the terminal.
    API keys / auth headers are never written here.
    """
    try:
        record = {
            "kind": kind,
            "payload": payload
        }
        line = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str
        )
        with _LOG_LOCK:
            with open(
                PRIVATE_WS_RAW_LOG_FILE,
                "a",
                encoding="utf-8"
            ) as raw_log:
                raw_log.write(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC] "
                    f"{line}\n"
                )
    except Exception:
        # Diagnostics must never affect trading.
        pass


def _remember_fill_id(fill_id):
    if not fill_id:
        return True

    fill_id = str(fill_id)

    if fill_id in SEEN_FILL_IDS:
        return False

    SEEN_FILL_IDS.add(fill_id)
    SEEN_FILL_ID_ORDER.append(fill_id)

    if len(SEEN_FILL_ID_ORDER) > MAX_SEEN_FILL_IDS:
        old = SEEN_FILL_ID_ORDER.pop(0)
        SEEN_FILL_IDS.discard(old)

    return True


def _remember_tp_ws_status(parent_id, status):
    """
    Remember the latest private-WS TP status for a parent BUY.
    Bounded memory; used only as reconcile race evidence.
    """
    if parent_id is None:
        return

    parent_id = str(parent_id)
    status = str(status).upper()

    TP_WS_STATUS_BY_PARENT[parent_id] = status
    TP_WS_STATUS_ORDER.append(parent_id)

    while len(TP_WS_STATUS_ORDER) > MAX_TP_WS_STATUS:
        old = TP_WS_STATUS_ORDER.pop(0)

        # Remove only if this is the last queued occurrence of that parent.
        if old not in TP_WS_STATUS_ORDER:
            TP_WS_STATUS_BY_PARENT.pop(old, None)


def _note_private_ws_rx(message=None):
    """Record activity from the private WS, including server ping."""
    global PRIVATE_WS_LAST_RX_MONO
    global PRIVATE_WS_LAST_PING_MONO

    now = time.monotonic()
    PRIVATE_WS_LAST_RX_MONO = now

    if isinstance(message, dict) and (
        message.get("type") == "ping"
        or message.get("event") == "ping"
    ):
        PRIVATE_WS_LAST_PING_MONO = now


def request_private_ws_reconnect(reason):
    """Ask order_watcher to drop and rebuild the authenticated stream."""
    global PRIVATE_WS_RECONNECT_REASON

    reason = str(reason)
    PRIVATE_WS_RECONNECT_REASON = reason

    first_request = not PRIVATE_WS_RECONNECT_EVENT.is_set()
    PRIVATE_WS_RECONNECT_EVENT.set()

    _private_ws_diag(
        "PRIVATE_WS_RECONNECT_REQUESTED",
        {"reason": reason}
    )

    if first_request:
        event_print(
            f"♻️ Private WS будет переподключён: {reason}"
        )


class PrivateWSReconnectRequested(RuntimeError):
    pass


# =========================================================
# ЗАЩИТА ОТ ДВУХ БОТОВ
# =========================================================

LOCK_HANDLE = None


def acquire_lock():

    global LOCK_HANDLE

    LOCK_HANDLE = open(
        LOCK_FILE,
        "w"
    )

    try:

        fcntl.flock(
            LOCK_HANDLE,
            fcntl.LOCK_EX | fcntl.LOCK_NB
        )

    except BlockingIOError:

        raise SystemExit(
            "❌ STRKUSDT bot06 уже запущен."
        )

    LOCK_HANDLE.write(
        str(os.getpid())
    )

    LOCK_HANDLE.flush()


acquire_lock()


# =========================================================
# STATE
# =========================================================

def load_state():

    if not STATE_FILE.exists():

        return {
            "version": VERSION,
            "top": None,
            "parents": {},
            "pending_submits": {}
        }

    try:

        state = json.loads(
            STATE_FILE.read_text()
        )

    except Exception as error:

        raise SystemExit(
            f"Не могу прочитать state06.json: {error}"
        )

    if "parents" not in state:

        raise SystemExit(
            "Непонятный формат state06.json"
        )

    state["version"] = VERSION
    state.setdefault("pending_submits", {})

    return state


STATE = load_state()

state_lock = asyncio.Lock()
strategy_lock = asyncio.Lock()
rest_lock = asyncio.Lock()
reconcile_lock = asyncio.Lock()
stats_lock = asyncio.Lock()
maintenance_lock = asyncio.Lock()

ws_ready = asyncio.Event()
private_ws_ready = asyncio.Event()
fill_ws_ready = asyncio.Event()
public_bid_ready = asyncio.Event()
bid_update_event = asyncio.Event()

# Fill-channel diagnostics/deduplication. A STRK order can in theory fill in
# several executions, so accumulate fillSize until the full 20 STRK is seen.
FILL_ACCUMULATED_QTY = {}
SEEN_FILL_IDS = set()
SEEN_FILL_ID_ORDER = []
MAX_SEEN_FILL_IDS = 5000

# Private WS health / forced reconnect signaling.
PRIVATE_WS_RECONNECT_EVENT = asyncio.Event()
PRIVATE_WS_RECONNECT_REASON = None
PRIVATE_WS_LAST_RX_MONO = 0.0
PRIVATE_WS_LAST_PING_MONO = 0.0

# Distinguish our own DELETE /order from an external/exchange cancellation.
BOT_CANCEL_IN_FLIGHT = set()
BOT_CANCEL_CONFIRMED_WS = set()

last_trade_call = 0.0

PAUSED = False
STATE_SYNCED = False
LAST_BID = None
LAST_BID_UPDATE_MONO = 0.0
LAST_BID_SOURCE = None

STATS_CACHE = None
STATS_CACHE_LOADED = False

# TP rearm batch queue. All access happens on the asyncio event loop.
REARM_BATCH_IDS = set()
REARM_BATCH_TASK = None

# Latest TP order status observed on the private orders WebSocket.
# This is especially important while reconcile holds strategy_lock:
# REST snapshots can be stale while WS has already seen the real TP.
TP_WS_STATUS_BY_PARENT = {}
TP_WS_STATUS_ORDER = []
MAX_TP_WS_STATUS = 5000


def save_state():

    STATE["version"] = VERSION

    temp = BOT_DIR / "state06.tmp"

    temp.write_text(
        json.dumps(
            STATE,
            indent=2,
            ensure_ascii=False
        )
    )

    os.replace(
        temp,
        STATE_FILE
    )


# =========================================================
# ЦЕНЫ
# =========================================================

def D(value):

    return Decimal(
        str(value)
    ).quantize(
        PRICE_TICK
    )


def P(value):

    return f"{D(value):.5f}"


def grid_below_bid(bid):

    # Сетка STRK привязана к шагу 0.00010, а минимальный tick = 0.00001.
    # Верхний рабочий уровень ВСЕГДА строго ниже текущего BID.
    #
    # 0.07407 -> 0.07400
    # 0.07400 -> 0.07390
    # 0.07411 -> 0.07410

    bid_ticks = int(
        (D(bid) / PRICE_TICK).to_integral_value(
            rounding=ROUND_FLOOR
        )
    )

    step_ticks = int(
        (STEP / PRICE_TICK).to_integral_value()
    )

    level_ticks = (
        (bid_ticks - 1) // step_ticks
    ) * step_ticks

    return D(
        Decimal(level_ticks) * PRICE_TICK
    )


def client_id(price):

    price_ticks = int(
        (D(price) / PRICE_TICK).to_integral_value()
    )

    stamp = (
        int(time.time() * 1000)
        % 10_000_000_000
    )

    return (
        f"{PREFIX}"
        f"{price_ticks}-"
        f"{stamp}"
    )


# =========================================================
# ТЕКУЩИЙ BID
# =========================================================

def get_bid():

    url = (
        BASE
        + "/capi/v3/market/ticker/bookTicker"
        + "?symbol="
        + SYMBOL
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA
        }
    )

    with urllib.request.urlopen(
        request,
        timeout=REST_BID_TIMEOUT
    ) as response:

        data = json.loads(
            response.read().decode()
        )

    return D(
        data[0]["bidPrice"]
    )


def get_24h_ticker():

    url = (
        BASE
        + "/capi/v3/market/ticker/24hr"
        + "?symbol="
        + SYMBOL
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA
        }
    )

    with urllib.request.urlopen(
        request,
        timeout=REST_BID_TIMEOUT
    ) as response:

        data = json.loads(
            response.read().decode()
        )

    if not isinstance(data, list) or not data:
        raise RuntimeError(
            f"WEEX не вернул 24h ticker для {SYMBOL}: {data}"
        )

    return data[0]


def _record_bid(bid, source):

    global LAST_BID
    global LAST_BID_UPDATE_MONO
    global LAST_BID_SOURCE

    bid = D(bid)
    LAST_BID = bid
    LAST_BID_UPDATE_MONO = time.monotonic()
    LAST_BID_SOURCE = source

    # Reader loops must never block on strategy/REST work.
    # Event coalesces many fast price updates into the newest BID.
    bid_update_event.set()

    return bid


def _is_timeout_error(error):

    text = str(error).lower()

    return (
        "timed out" in text
        or "timeout" in text
        or "time out" in text
    )


# =========================================================
# ПОДПИСЬ REST
# =========================================================

def sign_rest(
    timestamp,
    method,
    path,
    query="",
    body=""
):

    message = (
        timestamp
        + method.upper()
        + path
    )

    if query:
        message += "?" + query

    message += body

    digest = hmac.new(
        SECRET.encode(),
        message.encode(),
        hashlib.sha256
    ).digest()

    return base64.b64encode(
        digest
    ).decode()


# =========================================================
# PRIVATE REST
# =========================================================

def _normalize_server_time_ms(value):

    try:
        number = int(Decimal(str(value)))
    except Exception:
        return None

    # seconds -> milliseconds
    if number < 10_000_000_000:
        number *= 1000

    # microseconds/nanoseconds -> milliseconds
    while number > 10_000_000_000_000:
        number //= 1000

    return number


def _find_server_time_ms(data):

    if isinstance(data, (str, int, float, Decimal)):
        return _normalize_server_time_ms(data)

    if isinstance(data, dict):

        for key in (
            "serverTime",
            "server_time",
            "timestamp",
            "time",
            "ts",
            "requestTime"
        ):

            if key in data:
                value = _normalize_server_time_ms(
                    data.get(key)
                )
                if value is not None:
                    return value

        for key in ("data", "result"):
            if key in data:
                value = _find_server_time_ms(
                    data.get(key)
                )
                if value is not None:
                    return value

    if isinstance(data, list):
        for item in data:
            value = _find_server_time_ms(item)
            if value is not None:
                return value

    return None


def sync_server_time():

    global SERVER_TIME_OFFSET_MS

    request = urllib.request.Request(
        BASE + "/capi/v3/market/time",
        headers={"User-Agent": UA}
    )

    local_before = int(time.time() * 1000)

    with urllib.request.urlopen(
        request,
        timeout=10
    ) as response:
        data = json.loads(
            response.read().decode()
        )

    local_after = int(time.time() * 1000)
    server_ms = _find_server_time_ms(data)

    if server_ms is None:
        raise RuntimeError(
            f"Не понял ответ WEEX time: {data}"
        )

    local_mid = (
        local_before + local_after
    ) // 2

    offset = server_ms - local_mid

    with SERVER_TIME_LOCK:
        SERVER_TIME_OFFSET_MS = offset

    return offset


def signed_timestamp():

    with SERVER_TIME_LOCK:
        offset = SERVER_TIME_OFFSET_MS

    return str(
        int(time.time() * 1000)
        + offset
    )


def _private_request_unlocked(
    method,
    path,
    params=None,
    payload=None
):

    query = urllib.parse.urlencode(
        params or {}
    )

    if payload is None:
        body = ""
    else:
        body = json.dumps(
            payload,
            separators=(",", ":")
        )

    url = BASE + path

    if query:
        url += "?" + query

    # При -1046 обновляем смещение часов и повторяем запрос
    # с совершенно новым timestamp/signature.
    for timestamp_attempt in range(3):

        timestamp = signed_timestamp()

        headers = {
            "ACCESS-KEY": API_KEY,
            "ACCESS-SIGN": sign_rest(
                timestamp,
                method,
                path,
                query,
                body
            ),
            "ACCESS-PASSPHRASE": PASSPHRASE,
            "ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
            "User-Agent": UA
        }

        request = urllib.request.Request(
            url,
            data=(
                body.encode()
                if body
                else None
            ),
            headers=headers,
            method=method.upper()
        )

        try:

            with urllib.request.urlopen(
                request,
                timeout=15
            ) as response:

                raw = response.read().decode()

                if not raw:
                    return None

                return json.loads(raw)

        except urllib.error.HTTPError as error:

            raw = error.read().decode()

            timestamp_expired = (
                error.code == 401
                and (
                    '"code":-1046' in raw.replace(" ", "")
                    or "timestamp expired" in raw.lower()
                )
            )

            if (
                timestamp_expired
                and timestamp_attempt < 2
            ):

                try:
                    offset = sync_server_time()
                    event_print(
                        "🕒 Время WEEX пересинхронизировано "
                        f"({offset:+d} ms). Повторяю REST."
                    )
                except Exception as sync_error:
                    event_print(
                        "⚠️ Не удалось сверить время WEEX: "
                        f"{sync_error}"
                    )

                continue

            if error.code == 429:
                raise RuntimeError(
                    "429:" + raw
                )

            raise RuntimeError(
                f"WEEX HTTP {error.code}: {raw}"
            )

        except Exception as error:
            raise RuntimeError(
                f"REST ошибка: {error}"
            )

    raise RuntimeError(
        "WEEX: не удалось выполнить REST после пересинхронизации времени"
    )


def private_request(
    method,
    path,
    params=None,
    payload=None
):
    """
    Common signed/private REST gate for trading, stats, safety and reconcile.
    Requests from this process cannot burst in parallel.
    """
    global PRIVATE_REST_LAST_START_MONO

    with PRIVATE_REST_GATE_LOCK:

        passed = (
            time.monotonic()
            - PRIVATE_REST_LAST_START_MONO
        )

        wait = (
            PRIVATE_REST_MIN_GAP
            - passed
        )

        if wait > 0:
            time.sleep(
                wait
            )

        PRIVATE_REST_LAST_START_MONO = (
            time.monotonic()
        )

        return _private_request_unlocked(
            method,
            path,
            params,
            payload
        )


# =========================================================
# ОГРАНИЧИВАЕМ СКОРОСТЬ ТОРГОВЫХ ЗАПРОСОВ
# =========================================================

async def trade_request(
    method,
    path,
    params=None,
    payload=None
):

    global last_trade_call

    async with rest_lock:

        passed = (
            time.monotonic()
            - last_trade_call
        )

        wait = (
            TRADE_GAP
            - passed
        )

        if wait > 0:

            await asyncio.sleep(
                wait
            )

        for attempt in range(5):

            try:

                result = await asyncio.to_thread(
                    private_request,
                    method,
                    path,
                    params,
                    payload
                )

                last_trade_call = (
                    time.monotonic()
                )

                return result

            except RuntimeError as error:

                if str(error).startswith(
                    "429:"
                ):

                    delay = (
                        2
                        + attempt * 2
                    )

                    event_print(
                        f"⚠️ WEEX 429. "
                        f"Жду {delay} сек."
                    )

                    await asyncio.sleep(
                        delay
                    )

                    continue

                raise

        raise RuntimeError(
            "WEEX продолжает отвечать 429"
        )


# =========================================================
# СТАТИСТИКА v0.4
#
# Первый запуск без stats_state06.json:
#   -> строим статистику из истории WEEX.
#
# Дальше:
#   -> берём только новые fills с небольшим overlap;
#   -> stats_state06.json хранит накопленные итоги;
#   -> при рестарте продолжаем с сохранённой точки.
#
# ВАЖНО:
# - WEEX realizedPnl здесь НЕ используется.
# - считаются только наши циклы с PREFIX = strk6-
# - ошибка статистики НЕ ставит торговлю на паузу.
# =========================================================

STATS_SCHEMA = 1


class StatsError(RuntimeError):
    pass


def SDEC(value):

    if value is None or value == "":
        return Decimal("0")

    return Decimal(
        str(value)
    )


def is_maker_trade(trade):

    value = trade.get(
        "maker",
        False
    )

    if isinstance(value, bool):
        return value

    return str(value).lower() in (
        "true",
        "1",
        "yes"
    )


def new_stats_cache():

    return {
        "schema": STATS_SCHEMA,
        "symbol": SYMBOL,
        "prefix": PREFIX,
        "last_success_ms": 0,
        "closed_cycles": 0,
        "closed_gross": "0",
        "closed_fees": "0",
        "total_volume": "0",
        "maker_volume": "0",
        "taker_volume": "0",
        "total_fees": "0",
        "seen_trade_ids": [],
        "processed_completed": [],
        "open_buys": {}
    }


def load_stats_cache():

    if not STATS_STATE_FILE.exists():
        return None

    data = json.loads(
        STATS_STATE_FILE.read_text()
    )

    if data.get("schema") != STATS_SCHEMA:
        raise StatsError(
            "Неподдерживаемая версия stats_state06.json"
        )

    if data.get("symbol") != SYMBOL:
        raise StatsError(
            "stats_state06.json от другого символа"
        )

    if data.get("prefix") != PREFIX:
        raise StatsError(
            "stats_state06.json от другого префикса"
        )

    required = (
        "closed_cycles",
        "closed_gross",
        "closed_fees",
        "total_volume",
        "maker_volume",
        "taker_volume",
        "total_fees",
        "seen_trade_ids",
        "processed_completed",
        "open_buys"
    )

    for key in required:

        if key not in data:
            raise StatsError(
                f"В stats_state06.json нет поля {key}"
            )

    return data


def save_stats_cache(cache):

    temp = BOT_DIR / "stats_state06.tmp"

    temp.write_text(
        json.dumps(
            cache,
            indent=2,
            ensure_ascii=False
        )
    )

    os.replace(
        temp,
        STATS_STATE_FILE
    )


def fetch_all_open_orders():

    result = []
    page = 0

    while True:

        chunk = private_request(
            "GET",
            "/capi/v3/openOrders",
            {
                "symbol": SYMBOL,
                "limit": 100,
                "page": page
            },
            None
        )

        if not isinstance(chunk, list):
            raise RuntimeError(
                "openOrders вернул неожиданный формат"
            )

        result.extend(chunk)

        if len(chunk) < 100:
            break

        page += 1

        if page > 20:
            raise RuntimeError(
                "Слишком много страниц openOrders"
            )

    return result


def fetch_all_open_algos():

    result = []
    page = 1

    while True:

        chunk = private_request(
            "GET",
            "/capi/v3/openAlgoOrders",
            {
                "symbol": SYMBOL,
                "page": page,
                "limit": 100
            },
            None
        )

        if not isinstance(chunk, list):
            raise RuntimeError(
                "openAlgoOrders вернул неожиданный формат"
            )

        result.extend(chunk)

        if len(chunk) < 100:
            break

        page += 1

        if page > 20:
            raise RuntimeError(
                "Слишком много страниц openAlgoOrders"
            )

    return result


def stats_market_data():

    # В bot06 может быть до 351 нашего grid-слота; читаем все страницы истории.
    # Поэтому обязательно читаем ВСЕ страницы, а PREFIX фильтруем уже после.
    open_orders = fetch_all_open_orders()
    open_algos = fetch_all_open_algos()

    history_raw = private_request(
        "GET",
        "/capi/v3/allAlgoOrders",
        {
            "symbol": SYMBOL,
            "limit": 1000
        },
        None
    )

    if not isinstance(
        open_orders,
        list
    ):
        raise StatsError(
            "openOrders вернул неожиданный формат"
        )

    if not isinstance(
        open_algos,
        list
    ):
        raise StatsError(
            "openAlgoOrders вернул неожиданный формат"
        )

    if isinstance(
        history_raw,
        dict
    ):

        history = history_raw.get(
            "orders",
            []
        )

    elif isinstance(
        history_raw,
        list
    ):

        history = history_raw

    else:

        raise StatsError(
            "allAlgoOrders вернул неожиданный формат"
        )

    bot_open_limits = [

        order

        for order in open_orders

        if str(
            order.get(
                "clientOrderId",
                ""
            )
        ).startswith(PREFIX)
    ]

    open_tps = [

        algo

        for algo in open_algos

        if str(
            algo.get(
                "clientAlgoId",
                ""
            )
        ).startswith(PREFIX)

        and "TAKE_PROFIT"
        in str(
            algo.get(
                "orderType",
                ""
            )
        )
    ]

    completed_tps = [

        algo

        for algo in history

        if str(
            algo.get(
                "clientAlgoId",
                ""
            )
        ).startswith(PREFIX)

        and "TAKE_PROFIT"
        in str(
            algo.get(
                "orderType",
                ""
            )
        )

        and str(
            algo.get(
                "algoStatus",
                ""
            )
        ) == "FILLED"
    ]

    return {
        "bot_open_limits": bot_open_limits,
        "open_tps": open_tps,
        "completed_tps": completed_tps
    }


def stats_fetch_trades(
    start,
    end,
    depth=0
):

    if end <= start:
        return []

    if depth > 40:
        raise StatsError(
            "Слишком глубокое деление userTrades"
        )

    data = private_request(
        "GET",
        "/capi/v3/userTrades",
        {
            "symbol": SYMBOL,
            "startTime": int(start),
            "endTime": int(end),
            "limit": 100
        },
        None
    )

    if not isinstance(
        data,
        list
    ):
        raise StatsError(
            "userTrades вернул неожиданный формат"
        )

    if (
        len(data) < 100
        or end - start < 10_000
    ):

        return data

    middle = (
        start + end
    ) // 2

    left = stats_fetch_trades(
        start,
        middle,
        depth + 1
    )

    right = stats_fetch_trades(
        middle + 1,
        end,
        depth + 1
    )

    merged = {}

    for trade in left + right:

        trade_id = str(
            trade.get(
                "id",
                ""
            )
        )

        if trade_id:
            merged[trade_id] = trade

    return list(
        merged.values()
    )


def add_trade_totals(
    cache,
    trade
):

    quote = SDEC(
        trade.get(
            "quoteQty",
            "0"
        )
    )

    fee = SDEC(
        trade.get(
            "commission",
            "0"
        )
    )

    cache["total_volume"] = str(
        SDEC(cache["total_volume"])
        + quote
    )

    cache["total_fees"] = str(
        SDEC(cache["total_fees"])
        + fee
    )

    if is_maker_trade(trade):

        cache["maker_volume"] = str(
            SDEC(cache["maker_volume"])
            + quote
        )

    else:

        cache["taker_volume"] = str(
            SDEC(cache["taker_volume"])
            + quote
        )


def aggregate_buy_fills(trades):

    if not trades:
        return None

    qty = sum(
        (
            SDEC(x.get("qty", "0"))
            for x in trades
        ),
        Decimal("0")
    )

    quote = sum(
        (
            SDEC(x.get("quoteQty", "0"))
            for x in trades
        ),
        Decimal("0")
    )

    fee = sum(
        (
            SDEC(x.get("commission", "0"))
            for x in trades
        ),
        Decimal("0")
    )

    if qty <= 0:
        return None

    return {
        "qty": str(qty),
        "quote": str(quote),
        "fee": str(fee)
    }


def merge_buy_trade(
    cache,
    parent_id,
    trade
):

    current = cache[
        "open_buys"
    ].get(
        parent_id,
        {
            "qty": "0",
            "quote": "0",
            "fee": "0"
        }
    )

    current["qty"] = str(
        SDEC(current["qty"])
        + SDEC(trade.get("qty", "0"))
    )

    current["quote"] = str(
        SDEC(current["quote"])
        + SDEC(trade.get("quoteQty", "0"))
    )

    current["fee"] = str(
        SDEC(current["fee"])
        + SDEC(trade.get("commission", "0"))
    )

    cache[
        "open_buys"
    ][parent_id] = current


def algo_parent_id(algo):

    # Prefer our deterministic TP client ID -> parent BUY client ID mapping.
    # This works for both attached "...tp" and recovery "...rtp" TP orders,
    # including completed TP history where actualOrderId may be the triggered
    # SELL order rather than the opening BUY.
    algo_cid = str(
        algo.get(
            "clientAlgoId",
            ""
        )
    )

    parent_cid = None

    if algo_cid.endswith(
        RECOVERY_TP_SUFFIX
    ):
        parent_cid = algo_cid[
            :-len(RECOVERY_TP_SUFFIX)
        ]

    elif algo_cid.endswith(
        "tp"
    ):
        parent_cid = algo_cid[:-2]

    if parent_cid:

        for parent_id, info in STATE.get(
            "parents",
            {}
        ).items():

            if str(
                info.get(
                    "client_id",
                    ""
                )
            ) == parent_cid:

                return str(
                    parent_id
                )

    # Safe compatibility fallback only when actualOrderId is already one of
    # our known BUY parent IDs.
    value = algo.get(
        "actualOrderId"
    )

    if value is None:
        return None

    value = str(
        value
    )

    if value in STATE.get(
        "parents",
        {}
    ):
        return value

    return None


def algo_event_time(algo):

    return int(
        algo.get(
            "triggerTime",
            0
        )
        or
        algo.get(
            "updateTime",
            0
        )
        or 0
    )


def match_sell_for_algo(
    algo,
    sell_trades,
    unavailable_ids
):

    trigger_time = algo_event_time(
        algo
    )

    actual_price = SDEC(
        algo.get(
            "actualPrice",
            "0"
        )
    )

    quantity = SDEC(
        algo.get(
            "quantity",
            "0"
        )
    )

    candidates = []

    for trade in sell_trades:

        trade_id = str(
            trade.get(
                "id",
                ""
            )
        )

        if not trade_id:
            continue

        if trade_id in unavailable_ids:
            continue

        if str(
            trade.get(
                "side",
                ""
            )
        ) != "SELL":
            continue

        trade_qty = SDEC(
            trade.get(
                "qty",
                "0"
            )
        )

        if trade_qty != quantity:
            continue

        trade_time = int(
            trade.get(
                "time",
                0
            )
            or 0
        )

        time_diff = abs(
            trade_time
            - trigger_time
        )

        if time_diff > 30_000:
            continue

        trade_price = SDEC(
            trade.get(
                "price",
                "0"
            )
        )

        price_diff = abs(
            trade_price
            - actual_price
        )

        candidates.append(
            (
                price_diff,
                time_diff,
                trade
            )
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            item[0],
            item[1]
        )
    )

    return candidates[0][2]


def recover_buy_for_algo(
    cache,
    algo,
    seen_trade_ids
):

    parent_id = algo_parent_id(
        algo
    )

    if not parent_id:
        return None

    create_time = int(
        algo.get(
            "createTime",
            0
        )
        or 0
    )

    finish_time = algo_event_time(
        algo
    )

    if finish_time <= 0:
        finish_time = int(
            time.time() * 1000
        )

    if create_time <= 0:
        create_time = (
            finish_time
            - 24 * 60 * 60 * 1000
        )

    trades = stats_fetch_trades(
        max(
            0,
            create_time - 60_000
        ),
        finish_time + 60_000
    )

    fills = [

        trade

        for trade in trades

        if str(
            trade.get(
                "side",
                ""
            )
        ) == "BUY"

        and str(
            trade.get(
                "orderId",
                ""
            )
        ) == parent_id
    ]

    aggregate = aggregate_buy_fills(
        fills
    )

    if not aggregate:
        return None

    cache[
        "open_buys"
    ][parent_id] = aggregate

    for trade in fills:

        trade_id = str(
            trade.get(
                "id",
                ""
            )
        )

        if not trade_id:
            continue

        if trade_id in seen_trade_ids:
            continue

        add_trade_totals(
            cache,
            trade
        )

        seen_trade_ids.add(
            trade_id
        )

    return aggregate


def recover_sell_for_algo(
    algo,
    unavailable_ids
):

    trigger_time = algo_event_time(
        algo
    )

    if trigger_time <= 0:
        return None

    nearby = stats_fetch_trades(
        max(
            0,
            trigger_time - 60_000
        ),
        trigger_time + 60_000
    )

    return match_sell_for_algo(
        algo,
        nearby,
        unavailable_ids
    )


def close_cycle_in_cache(
    cache,
    parent_id,
    buy,
    sell
):

    buy_qty = SDEC(
        buy["qty"]
    )

    buy_quote = SDEC(
        buy["quote"]
    )

    buy_fee = SDEC(
        buy["fee"]
    )

    if buy_qty <= 0:
        raise StatsError(
            f"Нулевая BUY quantity для {parent_id}"
        )

    buy_avg = (
        buy_quote
        / buy_qty
    )

    sell_qty = SDEC(
        sell.get(
            "qty",
            "0"
        )
    )

    sell_price = SDEC(
        sell.get(
            "price",
            "0"
        )
    )

    sell_fee = SDEC(
        sell.get(
            "commission",
            "0"
        )
    )

    qty = min(
        buy_qty,
        sell_qty
    )

    gross = (
        sell_price
        - buy_avg
    ) * qty

    cycle_fees = (
        buy_fee
        + sell_fee
    )

    cache["closed_gross"] = str(
        SDEC(cache["closed_gross"])
        + gross
    )

    cache["closed_fees"] = str(
        SDEC(cache["closed_fees"])
        + cycle_fees
    )

    cache["closed_cycles"] = int(
        cache["closed_cycles"]
    ) + 1

    cache[
        "open_buys"
    ].pop(
        parent_id,
        None
    )


def make_stats_snapshot(
    cache,
    current_bid,
    market,
    unmatched=0,
    missing_open=0,
    history_limit_hit=False
):

    open_tps = market[
        "open_tps"
    ]

    open_parent_ids = {

        algo_parent_id(algo)

        for algo in open_tps

        if algo_parent_id(algo)
    }

    unrealized_gross = Decimal("0")
    open_entry_fees = Decimal("0")
    open_position_qty = Decimal("0")

    for parent_id in open_parent_ids:

        buy = cache[
            "open_buys"
        ].get(
            parent_id
        )

        if not buy:
            continue

        qty = SDEC(
            buy["qty"]
        )

        quote = SDEC(
            buy["quote"]
        )

        fee = SDEC(
            buy["fee"]
        )

        if qty <= 0:
            continue

        avg_price = (
            quote
            / qty
        )

        unrealized_gross += (
            current_bid
            - avg_price
        ) * qty

        open_entry_fees += fee
        open_position_qty += qty

    unrealized_pnl = (
        unrealized_gross
        - open_entry_fees
    )

    grid_profit = (
        SDEC(cache["closed_gross"])
        - SDEC(cache["closed_fees"])
    )

    total_pnl = (
        grid_profit
        + unrealized_pnl
    )

    open_limit_count = len(
        market["bot_open_limits"]
    )

    open_tp_count = len(
        open_tps
    )

    return {
        "current_bid": current_bid,
        "closed_cycles": int(
            cache["closed_cycles"]
        ),
        "open_cycles": (
            open_limit_count
            + open_tp_count
        ),
        "open_limit_count": open_limit_count,
        "open_tp_count": open_tp_count,
        "grid_profit": grid_profit,
        "total_fees": SDEC(
            cache["total_fees"]
        ),
        "total_volume": SDEC(
            cache["total_volume"]
        ),
        "maker_volume": SDEC(
            cache["maker_volume"]
        ),
        "taker_volume": SDEC(
            cache["taker_volume"]
        ),
        "open_position_qty": open_position_qty,
        "unrealized_pnl": unrealized_pnl,
        "total_pnl": total_pnl,
        "unmatched": unmatched,
        "missing_open": missing_open,
        "history_limit_hit": history_limit_hit
    }


def bootstrap_stats_sync(
    current_bid,
    known_parent_ids
):

    cache = new_stats_cache()
    market = stats_market_data()

    open_tps = market[
        "open_tps"
    ]

    completed_tps = market[
        "completed_tps"
    ]

    all_algos = (
        completed_tps
        + open_tps
    )

    parent_ids = set(
        known_parent_ids
    )

    for algo in all_algos:

        parent_id = algo_parent_id(
            algo
        )

        if parent_id:
            parent_ids.add(
                parent_id
            )

    times = [

        int(
            algo.get(
                "createTime",
                0
            )
            or 0
        )

        for algo in all_algos

        if algo.get(
            "createTime"
        )
    ]

    now_ms = int(
        time.time() * 1000
    )

    if times:

        start_time = (
            min(times)
            - 60_000
        )

    else:

        start_time = (
            now_ms
            - 24 * 60 * 60 * 1000
        )

    trades = stats_fetch_trades(
        max(0, start_time),
        now_ms
    )

    seen_trade_ids = set()

    bot_buys = [

        trade

        for trade in trades

        if str(
            trade.get(
                "side",
                ""
            )
        ) == "BUY"

        and str(
            trade.get(
                "orderId",
                ""
            )
        ) in parent_ids
    ]

    buys_by_order = {}

    for trade in bot_buys:

        order_id = str(
            trade.get(
                "orderId",
                ""
            )
        )

        buys_by_order.setdefault(
            order_id,
            []
        ).append(
            trade
        )

        trade_id = str(
            trade.get(
                "id",
                ""
            )
        )

        if trade_id and trade_id not in seen_trade_ids:

            add_trade_totals(
                cache,
                trade
            )

            seen_trade_ids.add(
                trade_id
            )

    sell_trades = [

        trade

        for trade in trades

        if str(
            trade.get(
                "side",
                ""
            )
        ) == "SELL"
    ]

    used_sell_ids = set()
    processed_completed = set()
    unmatched_algos = []

    for algo in sorted(
        completed_tps,
        key=algo_event_time
    ):

        parent_id = algo_parent_id(
            algo
        )

        if not parent_id:
            continue

        buy = aggregate_buy_fills(
            buys_by_order.get(
                parent_id,
                []
            )
        )

        if not buy:

            buy = recover_buy_for_algo(
                cache,
                algo,
                seen_trade_ids
            )

        unavailable = (
            seen_trade_ids
            | used_sell_ids
        )

        sell = match_sell_for_algo(
            algo,
            sell_trades,
            unavailable
        )

        if not sell:

            sell = recover_sell_for_algo(
                algo,
                unavailable
            )

        if not buy or not sell:

            unmatched_algos.append(
                algo
            )

            continue

        close_cycle_in_cache(
            cache,
            parent_id,
            buy,
            sell
        )

        sell_id = str(
            sell.get(
                "id",
                ""
            )
        )

        if sell_id:

            if sell_id not in seen_trade_ids:

                add_trade_totals(
                    cache,
                    sell
                )

                seen_trade_ids.add(
                    sell_id
                )

            used_sell_ids.add(
                sell_id
            )

        processed_completed.add(
            parent_id
        )

    missing_open = 0

    for algo in open_tps:

        parent_id = algo_parent_id(
            algo
        )

        if not parent_id:
            continue

        buy = aggregate_buy_fills(
            buys_by_order.get(
                parent_id,
                []
            )
        )

        if buy:

            cache[
                "open_buys"
            ][parent_id] = buy

            continue

        buy = recover_buy_for_algo(
            cache,
            algo,
            seen_trade_ids
        )

        if not buy:
            missing_open += 1

    cache["seen_trade_ids"] = sorted(
        seen_trade_ids
    )

    cache["processed_completed"] = sorted(
        processed_completed
    )

    if unmatched_algos:

        earliest = min(
            (
                algo_event_time(algo)
                for algo in unmatched_algos
                if algo_event_time(algo) > 0
            ),
            default=now_ms
        )

        cache["last_success_ms"] = max(
            0,
            earliest - 60_000
        )

    else:

        cache["last_success_ms"] = now_ms

    history_limit_hit = (
        len(completed_tps) >= 1000
    )

    save_stats_cache(
        cache
    )

    snapshot = make_stats_snapshot(
        cache,
        current_bid,
        market,
        unmatched=len(unmatched_algos),
        missing_open=missing_open,
        history_limit_hit=history_limit_hit
    )

    return cache, snapshot


def update_stats_sync(
    cache,
    current_bid,
    known_parent_ids
):

    market = stats_market_data()

    open_tps = market[
        "open_tps"
    ]

    completed_tps = market[
        "completed_tps"
    ]

    now_ms = int(
        time.time() * 1000
    )

    old_last_success = int(
        cache.get(
            "last_success_ms",
            0
        )
        or 0
    )

    if old_last_success <= 0:

        return bootstrap_stats_sync(
            current_bid,
            known_parent_ids
        )

    start_time = max(
        0,
        old_last_success
        - STATS_OVERLAP_MS
    )

    trades = stats_fetch_trades(
        start_time,
        now_ms
    )

    seen_trade_ids = set(
        str(x)
        for x in cache.get(
            "seen_trade_ids",
            []
        )
    )

    processed_completed = set(
        str(x)
        for x in cache.get(
            "processed_completed",
            []
        )
    )

    current_parent_ids = set(
        str(x)
        for x in known_parent_ids
    )

    for algo in (
        open_tps
        + completed_tps
    ):

        parent_id = algo_parent_id(
            algo
        )

        if parent_id:
            current_parent_ids.add(
                parent_id
            )

    # -----------------------------------------
    # Новые BUY fills нашего бота
    # -----------------------------------------

    for trade in sorted(
        trades,
        key=lambda x: int(
            x.get(
                "time",
                0
            )
            or 0
        )
    ):

        if str(
            trade.get(
                "side",
                ""
            )
        ) != "BUY":
            continue

        parent_id = str(
            trade.get(
                "orderId",
                ""
            )
        )

        if parent_id not in current_parent_ids:
            continue

        trade_id = str(
            trade.get(
                "id",
                ""
            )
        )

        if not trade_id:
            continue

        if trade_id in seen_trade_ids:
            continue

        add_trade_totals(
            cache,
            trade
        )

        merge_buy_trade(
            cache,
            parent_id,
            trade
        )

        seen_trade_ids.add(
            trade_id
        )

    sell_trades = [

        trade

        for trade in trades

        if str(
            trade.get(
                "side",
                ""
            )
        ) == "SELL"
    ]

    used_sell_ids = set()
    unmatched_algos = []

    # -----------------------------------------
    # Новые полностью закрытые циклы
    # -----------------------------------------

    for algo in sorted(
        completed_tps,
        key=algo_event_time
    ):

        parent_id = algo_parent_id(
            algo
        )

        if not parent_id:
            continue

        if parent_id in processed_completed:
            continue

        buy = cache[
            "open_buys"
        ].get(
            parent_id
        )

        if not buy:

            buy = recover_buy_for_algo(
                cache,
                algo,
                seen_trade_ids
            )

        unavailable = (
            seen_trade_ids
            | used_sell_ids
        )

        sell = match_sell_for_algo(
            algo,
            sell_trades,
            unavailable
        )

        if not sell:

            sell = recover_sell_for_algo(
                algo,
                unavailable
            )

        if not buy or not sell:

            unmatched_algos.append(
                algo
            )

            continue

        close_cycle_in_cache(
            cache,
            parent_id,
            buy,
            sell
        )

        sell_id = str(
            sell.get(
                "id",
                ""
            )
        )

        if sell_id:

            if sell_id not in seen_trade_ids:

                add_trade_totals(
                    cache,
                    sell
                )

                seen_trade_ids.add(
                    sell_id
                )

            used_sell_ids.add(
                sell_id
            )

        processed_completed.add(
            parent_id
        )

    # -----------------------------------------
    # Убеждаемся, что для каждого активного TP
    # у нас есть реальный BUY fill.
    # -----------------------------------------

    missing_open = 0

    for algo in open_tps:

        parent_id = algo_parent_id(
            algo
        )

        if not parent_id:
            continue

        if parent_id in cache[
            "open_buys"
        ]:
            continue

        buy = recover_buy_for_algo(
            cache,
            algo,
            seen_trade_ids
        )

        if not buy:
            missing_open += 1

    cache["seen_trade_ids"] = sorted(
        seen_trade_ids
    )

    cache["processed_completed"] = sorted(
        processed_completed
    )

    if not unmatched_algos and not missing_open:

        cache["last_success_ms"] = now_ms

    else:

        # Не двигаем watermark вперёд,
        # чтобы следующий запуск ещё раз
        # увидел сомнительный участок истории.
        cache["last_success_ms"] = (
            old_last_success
        )

    save_stats_cache(
        cache
    )

    snapshot = make_stats_snapshot(
        cache,
        current_bid,
        market,
        unmatched=len(unmatched_algos),
        missing_open=missing_open,
        history_limit_hit=False
    )

    return cache, snapshot


def print_stats(snapshot):

    print()
    event_print(
        "📊 WEEX STRK GRID STATISTICS"
    )
    print(
        "========================================"
    )
    print()

    print(
        f"Текущий BID:            "
        f"${snapshot['current_bid']:.5f}"
    )

    print()

    print(
        f"Закрытых циклов:         "
        f"{snapshot['closed_cycles']}"
    )

    print(
        f"Открыто ордеров:         "
        f"{snapshot['open_cycles']}"
    )

    print(
        f"  BUY LIMIT:             "
        f"{snapshot['open_limit_count']}"
    )

    print(
        f"  ждут TP:               "
        f"{snapshot['open_tp_count']}"
    )

    print()

    print(
        f"GRID PROFIT:           "
        f"${snapshot['grid_profit']:+.8f}"
    )

    print()

    print(
        f"Уплачено комиссий:      "
        f"${snapshot['total_fees']:.8f}"
    )

    print()

    print(
        f"Общий объём:           "
        f"${snapshot['total_volume']:.4f}"
    )

    print(
        f"  Maker:               "
        f"${snapshot['maker_volume']:.4f}"
    )

    print(
        f"  Taker:               "
        f"${snapshot['taker_volume']:.4f}"
    )

    print()

    print(
        f"Открытая позиция:       "
        f"{snapshot['open_position_qty']:.0f} STRK"
    )

    print(
        f"Нереализованный PnL:  "
        f"${snapshot['unrealized_pnl']:+.8f}"
    )

    print()
    print(
        "----------------------------------------"
    )

    print(
        f"ИТОГОВЫЙ PnL:         "
        f"${snapshot['total_pnl']:+.8f}"
    )

    print(
        "----------------------------------------"
    )
    print()

    problems = (
        snapshot["unmatched"]
        + snapshot["missing_open"]
    )

    if problems:

        if snapshot["unmatched"]:

            print(
                "⚠️ Не удалось точно "
                "сопоставить закрытых циклов: "
                f"{snapshot['unmatched']}"
            )

        if snapshot["missing_open"]:

            print(
                "⚠️ Не найдены BUY fills "
                "для активных TP: "
                f"{snapshot['missing_open']}"
            )

    else:

        print(
            "✅ Все закрытые циклы "
            "сопоставлены."
        )

    if snapshot[
        "history_limit_hit"
    ]:

        print(
            "⚠️ История TP достигла лимита 1000. "
            "Не удаляй stats_state06.json: "
            "дальше накопительная статистика "
            "продолжится из кэша."
        )

    print()


async def refresh_stats(
    show=True,
    reason="плановая статистика"
):

    global STATS_CACHE
    global STATS_CACHE_LOADED

    # Если идёт reconnect/reconcile, статистика подождёт.
    # Торговля от ошибки статистики не зависит.
    if not ws_ready.is_set():
        return None

    async with stats_lock:

        try:

            if not STATS_CACHE_LOADED:

                try:

                    STATS_CACHE = await asyncio.to_thread(
                        load_stats_cache
                    )

                except Exception as error:

                    event_print(
                        "⚠️ stats_state06.json не читается: "
                        f"{error}"
                    )

                    event_print(
                        "ℹ️ Статистика будет построена заново из WEEX."
                    )

                    STATS_CACHE = None

                STATS_CACHE_LOADED = True

            async with state_lock:

                known_parent_ids = set(
                    STATE[
                        "parents"
                    ].keys()
                )

            current_bid = LAST_BID

            if current_bid is None:

                current_bid = await asyncio.to_thread(
                    get_bid
                )

            # Statistics is observational only. It must never hold the
            # maintenance/reconcile lock and delay WS recovery.
            if STATS_CACHE is None:

                cache, snapshot = await asyncio.to_thread(
                    bootstrap_stats_sync,
                    current_bid,
                    known_parent_ids
                )

            else:

                cache, snapshot = await asyncio.to_thread(
                    update_stats_sync,
                    STATS_CACHE,
                    current_bid,
                    known_parent_ids
                )

            STATS_CACHE = cache

            if show:
                print_stats(
                    snapshot
                )

            return snapshot

        except Exception as error:

            print()
            event_print(
                "⚠️ Ошибка статистики "
                f"({reason}): {error}"
            )

            event_print(
                "ℹ️ Торговля продолжает работать. "
                "Статистика попробует снова позже."
            )
            print()

            return None


# =========================================================
# СЧЁТЧИКИ ЖИВЫХ ЦИКЛОВ / АКТИВНЫХ ЛИМИТОК
# =========================================================

def active_limits():

    return sum(

        1

        for order
        in STATE["parents"].values()

        if order["status"]
        == "OPEN"
    )


def tracked_grid_orders():

    # Жёсткий лимит риска для STRK: считаем все BUY, которые уже стоят,
    # уже исполнились и ждут TP, либо находятся в неоднозначном состоянии.
    # Завершённый parent, который только ждёт rearm, места в позиции уже
    # не занимает; новый BUY после rearm снова займёт один слот.
    closed_or_rearming = {
        "NEEDS_REARM",
        "REARMING",
        "REARM_PENDING",
        "DONE_OFFLINE"
    }

    parent_slots = sum(
        1
        for info in STATE["parents"].values()
        if info.get("status") not in closed_or_rearming
    )

    # PENDING submit тоже резервирует слот: после HTTP timeout WEEX мог
    # принять BUY, даже если ответ до нас не дошёл.
    pending_slots = len(
        STATE.get("pending_submits", {})
    )

    return parent_slots + pending_slots


def remaining_grid_slots():
    return max(
        0,
        MAX_GRID_ORDERS - tracked_grid_orders()
    )


# =========================================================
# ПОСТАВИТЬ BUY + ВШИТЫЙ TP
# =========================================================

async def place_entry(
    price,
    reason,
    rearm_parent_id=None
):

    price = D(price)

    # ЖЕЛЕЗНЫЙ потолок: bot06 никогда не создаёт новый BUY выше 0.08500.
    if price > MAX_BUY_PRICE:
        event_print(
            f"🛑 BUY {P(price)} не поставлен: "
            f"выше MAX BUY {P(MAX_BUY_PRICE)}"
        )
        return None

    # Это не нижний лимит стратегии, а базовая защита от некорректной цены.
    if price <= 0:
        event_print(
            f"🛑 BUY {P(price)} не поставлен: цена должна быть > 0"
        )
        return None

    # Второй железный предел: максимум 351 потенциальный BUY-слот,
    # то есть максимум 7,020 STRK при QTY=20.
    if tracked_grid_orders() >= MAX_GRID_ORDERS:
        event_print(
            f"🛑 BUY {P(price)} не поставлен: "
            f"лимит сетки {MAX_GRID_ORDERS} ордеров / {MAX_STRK:.0f} STRK"
        )
        return None

    tp = (
        price
        + TP_DISTANCE
    )

    cid = client_id(
        price
    )

    payload = {

        "symbol":
            SYMBOL,

        "side":
            "BUY",

        "positionSide":
            "LONG",

        "type":
            "LIMIT",

        "timeInForce":
            "POST_ONLY",

        "quantity":
            format(QTY, "f"),

        "price":
            P(price),

        "newClientOrderId":
            cid,

        "tpTriggerPrice":
            P(tp),

        "TpWorkingType":
            "CONTRACT_PRICE",

        "reduceOnly":
            False
    }

    try:

        result = await trade_request(
            "POST",
            "/capi/v3/order",
            payload=payload
        )

    except Exception as error:

        # HTTP timeout is NOT proof that WEEX rejected the POST. The request
        # may have reached the matching engine while only the response was
        # lost. Blind retry can create a duplicate order. Reserve this exact
        # clientOrderId and confirm exchange state first.
        if _is_timeout_error(error):

            async with state_lock:

                STATE.setdefault(
                    "pending_submits",
                    {}
                )[cid] = {
                    "price": P(price),
                    "tp": P(tp),
                    "reason": reason,
                    "created_ms": int(time.time() * 1000),
                    "absence_checks": 0,
                    "rearm_parent_id": rearm_parent_id
                }

                save_state()

            event_print(
                f"⏳ BUY {P(price)}: REST timeout — "
                f"НЕ повторяю вслепую, сначала проверю WEEX "
                f"| cid={cid}"
            )

            return {
                "pending": True,
                "client_id": cid
            }

        event_print(
            f"❌ Не поставлен BUY "
            f"{P(price)}: {error}"
        )

        return None

    if (
        not result
        or not result.get("success")
    ):

        event_print(
            f"❌ Не поставлен BUY "
            f"{P(price)}: "
            f"{result}"
        )

        return None

    order_id = str(
        result["orderId"]
    )

    async with state_lock:

        STATE["parents"][
            order_id
        ] = {

            "price":
                P(price),

            "tp":
                P(tp),

            "client_id":
                cid,

            "status":
                "OPEN"
        }

        save_state()

    event_print(
        f"✅ BUY {P(price)}"
        f" → TP {P(tp)}"
        f" | {reason}"
        f" | id={order_id}"
    )

    return order_id


# =========================================================
# ОТМЕНА OPEN BUY
# =========================================================

async def cancel_open_order(
    order_id,
    reason="trim lowest"
):

    order_id = str(order_id)

    async with state_lock:
        info = STATE["parents"].get(order_id)

        if (
            not info
            or info.get("status") != "OPEN"
        ):
            return False

        price = info["price"]

    # Keep this marker for the whole DELETE round-trip. If the WS CANCELED
    # event wins the race, handle_order can recognize it as our own cancel.
    BOT_CANCEL_IN_FLIGHT.add(order_id)
    BOT_CANCEL_CONFIRMED_WS.discard(order_id)

    try:
        result = await trade_request(
            "DELETE",
            "/capi/v3/order",
            params={"orderId": order_id}
        )

    except Exception as error:
        # The HTTP reply can be lost even though WEEX already confirmed the
        # cancellation over WS. In that case the cancel is still definitive.
        if order_id in BOT_CANCEL_CONFIRMED_WS:
            BOT_CANCEL_CONFIRMED_WS.discard(order_id)
            BOT_CANCEL_IN_FLIGHT.discard(order_id)
            event_print(
                f"🗑 BOT CANCEL {price}"
                f" | {reason} | подтверждено WS"
            )
            return True

        BOT_CANCEL_IN_FLIGHT.discard(order_id)
        event_print(
            f"❌ Ошибка отмены {price}: {error}"
        )
        return False

    if result and result.get("success"):
        async with state_lock:
            current = STATE["parents"].get(order_id)

            # If WS/REST changed it to WAIT_TP while DELETE was in flight,
            # do not remove the now-filled cycle from state.
            if current and current.get("status") == "OPEN":
                STATE["parents"].pop(order_id, None)
                save_state()

        BOT_CANCEL_CONFIRMED_WS.discard(order_id)
        BOT_CANCEL_IN_FLIGHT.discard(order_id)
        event_print(
            f"🗑 BOT CANCEL {price} | {reason}"
        )
        return True

    # Trust a WS cancellation acknowledgement over a late/ambiguous REST reply.
    if order_id in BOT_CANCEL_CONFIRMED_WS:
        BOT_CANCEL_CONFIRMED_WS.discard(order_id)
        BOT_CANCEL_IN_FLIGHT.discard(order_id)
        event_print(
            f"🗑 BOT CANCEL {price}"
            f" | {reason} | подтверждено WS"
        )
        return True

    BOT_CANCEL_IN_FLIGHT.discard(order_id)
    event_print(
        f"❌ Не смог удалить BUY {price}: {result}"
    )
    return False


async def cancel_lowest(
    reason="trim lowest"
):

    async with state_lock:

        opened = [

            (order_id, info)

            for order_id, info
            in STATE["parents"].items()

            if info["status"]
            == "OPEN"
        ]

        if not opened:
            return False

        order_id, _ = min(

            opened,

            key=lambda item:
                Decimal(
                    item[1]["price"]
                )
        )

    return await cancel_open_order(
        order_id,
        reason
    )


# =========================================================
# ЭТАЛОННАЯ РАБОЧАЯ СЕТКА ИЗ 20 BUY LIMIT
#
# ВАЖНО:
# недостаточно иметь просто 20 ордеров где попало.
# Нужны 20 ПРАВИЛЬНЫХ grid-уровней без дырок возле рынка.
#
# Верх рабочих 20 лимиток не ставим выше grid-уровня строго под BID.
# Поэтому если рынок уже прошёл вниз через отменённый уровень,
# бот не пытается бесконечно ставить POST_ONLY выше рынка —
# рабочие 20 лимиток автоматически смещаются ниже текущего BID.
#
# Уровни WAIT_TP / REARMING / NEEDS_REARM резервируются:
# второй живой цикл на ту же цену не создаём.
# =========================================================

async def _desired_grid_snapshot():

    global LAST_BID

    bid = LAST_BID

    if bid is None:
        bid = await asyncio.to_thread(
            get_bid
        )
        LAST_BID = bid

    market_anchor = min(
        grid_below_bid(bid),
        MAX_BUY_PRICE
    )

    async with state_lock:

        top_raw = STATE.get("top")

        top = (
            Decimal(top_raw)
            if top_raw is not None
            else market_anchor
        )

        # Никогда не строим активные POST_ONLY BUY выше рынка.
        anchor = min(
            top,
            market_anchor,
            MAX_BUY_PRICE
        )

        blocked_prices = {
            Decimal(info["price"])
            for info in STATE["parents"].values()
            if info.get("status") != "OPEN"
        }

        opened = [
            (
                order_id,
                Decimal(info["price"])
            )
            for order_id, info
            in STATE["parents"].items()
            if info.get("status") == "OPEN"
        ]

        pending_prices = {
            Decimal(info["price"])
            for info in STATE.get("pending_submits", {}).values()
            if info.get("price") is not None
        }

    desired = []
    level = anchor

    # 20 уровней, пропуская цены, где уже живёт WAIT_TP/rearm.
    while len(desired) < ACTIVE_LIMITS_TARGET:

        if level not in blocked_prices:
            desired.append(level)

        level -= STEP

    return bid, desired, opened, pending_prices


async def _remote_open_ids_for_bot():

    open_orders = await asyncio.to_thread(
        fetch_all_open_orders
    )

    return {
        str(order.get("orderId"))
        for order in open_orders
        if (
            str(order.get("clientOrderId", "")).startswith(PREFIX)
            and order.get("orderId") is not None
        )
    }


async def _repair_active_grid_locked(
    reason
):

    if PAUSED or not STATE_SYNCED:
        return

    bid, desired, opened, pending_prices = await _desired_grid_snapshot()
    desired_set = set(desired)

    open_by_price = {}
    for order_id, price in opened:
        open_by_price.setdefault(
            price,
            []
        ).append(order_id)

    missing = [
        level
        for level in desired
        if (
            level not in open_by_price
            and level not in pending_prices
        )
    ]

    # Лишние нижние ордера пока НЕ трогаем.
    # Сначала восстанавливаем дырки в верхней/торгуемой зоне.
    extras = sorted(
        [
            (order_id, price)
            for order_id, price in opened
            if price not in desired_set
        ],
        key=lambda item: item[1]
    )

    for level in missing:

        # Жёсткий лимит сетки: 351 потенциальный BUY-слот.
        # Только в этом случае разрешено сначала освободить слот.
        if tracked_grid_orders() >= MAX_GRID_ORDERS:

            if not extras:
                event_print(
                    f"ℹ️ Достигнут лимит {MAX_GRID_ORDERS} grid-слотов. "
                    f"Не могу восстановить BUY {P(level)}."
                )
                break

            extra_id, extra_price = extras.pop(0)

            ok = await cancel_open_order(
                extra_id,
                "освобождаю grid-слот для верхней дырки"
            )

            if not ok:
                break

        await place_entry(
            level,
            f"восстановление рабочей сетки | {reason}"
        )

    # Пересобираем снимок после постановок.
    # Если дырка ещё осталась, НИЧЕГО снизу не удаляем.
    bid, desired, opened, pending_prices = await _desired_grid_snapshot()
    desired_set = set(desired)

    open_by_price = {}
    for order_id, price in opened:
        open_by_price.setdefault(
            price,
            []
        ).append(order_id)

    remaining_missing = [
        level
        for level in desired
        if (
            level not in open_by_price
            and level not in pending_prices
        )
    ]

    if remaining_missing:
        return

    # A pending POST is not yet exchange-confirmed. Keep lower insurance BUYs
    # until every desired level is known to exist remotely.
    if any(level in pending_prices for level in desired):
        return

    extras = sorted(
        [
            (order_id, price)
            for order_id, price in opened
            if price not in desired_set
        ],
        key=lambda item: item[1]
    )

    if not extras:
        return

    # Критическая страховка от старого бага:
    # place_entry мог получить success, а WEEX через мгновение
    # отменить POST_ONLY. Поэтому перед удалением нижних запасных
    # ордеров подтверждаем через REST, что ВСЕ 20 нужных уровней
    # реально ещё стоят на бирже.
    try:
        remote_open_ids = await _remote_open_ids_for_bot()
    except Exception as error:
        event_print(
            f"⚠️ Не удаляю нижние BUY: "
            f"не удалось подтвердить рабочую сетку через REST: {error}"
        )
        return

    all_desired_confirmed = True

    for level in desired:

        ids = open_by_price.get(
            level,
            []
        )

        if not any(
            order_id in remote_open_ids
            for order_id in ids
        ):
            all_desired_confirmed = False
            break

    if not all_desired_confirmed:
        return

    # Теперь и только теперь можно убрать лишние самые нижние OPEN.
    for order_id, price in extras:

        if active_limits() <= ACTIVE_LIMITS_TARGET:
            break

        await cancel_open_order(
            order_id,
            "лишний нижний уровень после подтверждения 20 нужных BUY"
        )


async def maintain_active_limits(
    reason
):

    if PAUSED or not STATE_SYNCED:
        return

    async with strategy_lock:
        await _repair_active_grid_locked(
            reason
        )


# =========================================================
# СДВИГ СЕТКИ ВВЕРХ
# Логика прежняя: новый верхний уровень, лишняя нижняя лимитка уходит.
# =========================================================

async def shift_up(
    target
):

    global PAUSED

    if PAUSED or not STATE_SYNCED:
        return


    target = min(
        D(target),
        MAX_BUY_PRICE
    )

    async with strategy_lock:

        while True:

            if PAUSED:
                return

            async with state_lock:

                if STATE.get("top") is None:
                    return

                top = Decimal(
                    STATE["top"]
                )

            if target <= top:
                return

            new_level = (
                top
                + STEP
            )

            if new_level > MAX_BUY_PRICE:
                return

            # НОВАЯ ЗАЩИТА ОТ ДЫРОК:
            # сначала ставим новый верхний уровень и только потом
            # отдельный grid-repair решает, какой нижний ордер лишний.
            # Старый вариант "сначала cancel_lowest, потом place" мог
            # оставить дырку, если WEEX отменял новый POST_ONLY.
            if tracked_grid_orders() >= MAX_GRID_ORDERS:

                # Исключение только для жёсткого лимита 351 grid-слот:
                # нужно сначала освободить один слот.
                ok = await cancel_lowest(
                    "освобождаю grid-слот перед новым верхом"
                )

                if not ok:
                    return

            new_order = await place_entry(

                new_level,

                "новый верхний уровень"
            )

            if not new_order:
                return

            async with state_lock:

                STATE["top"] = P(
                    new_level
                )

                save_state()

            # Не даём быстрому скачку цены временно раздуть рабочую пачку
            # далеко выше 20 лимиток: каждый новый верх подтверждаем и
            # сразу приводим рабочую сетку обратно к правильным 20 уровням.
            await _repair_active_grid_locked(
                "после сдвига верхнего уровня"
            )


# =========================================================
# ВОССТАНОВИТЬ УРОВЕНЬ ПОСЛЕ TP
#
# КРИТИЧЕСКИЙ ПРИОРИТЕТ:
# 1) сначала ставим BUY на ИСХОДНУЮ цену закрытого цикла;
# 2) после успешного rearm восстанавливаем правильные 20 активных BUY.
# =========================================================

async def _rearm_one_locked(
    parent_id
):
    """
    Re-arm exactly one completed parent.
    Caller MUST hold strategy_lock.

    IMPORTANT:
    This function does NOT repair/trim the 20-level active grid.
    A whole TP batch is re-armed first; the grid is repaired once afterward.
    """
    global PAUSED

    parent_id = str(parent_id)

    async with state_lock:

        info = (
            STATE["parents"]
            .get(parent_id)
        )

        if not info:
            return "missing"

        if (
            info["status"]
            == "REARMING"
        ):
            return "already_rearming"

        price = Decimal(
            info["price"]
        )

        # Не допускаем второй живой цикл
        # на той же цене.
        duplicate = any(

            other_id != parent_id

            and Decimal(
                other["price"]
            ) == price

            and other["status"]
            in (
                "OPEN",
                "WAIT_TP",
                "REARMING"
            )

            for other_id, other
            in STATE["parents"].items()
        )

        if duplicate:

            event_print(
                f"ℹ️ {P(price)} уже существует. "
                f"Старый завершённый цикл удаляю из state."
            )

            STATE["parents"].pop(
                parent_id,
                None
            )

            save_state()

            return "duplicate"

        if PAUSED:

            info["status"] = (
                "NEEDS_REARM"
            )

            save_state()

            return "paused"

        info["status"] = (
            "REARMING"
        )

        save_state()

    # ВАЖНО: до rearm ничего снизу не удаляем.
    new_order = await place_entry(

        price,

        "повтор после TP",

        rearm_parent_id=parent_id
    )

    if (
        isinstance(new_order, dict)
        and new_order.get("pending")
    ):

        # Не удаляем завершённый parent, пока не узнаем, принял ли WEEX
        # повторный BUY. Pending resolver сам завершит этот переход.
        async with state_lock:
            if parent_id in STATE["parents"]:
                STATE["parents"][parent_id]["status"] = "REARM_PENDING"
                save_state()

        return "pending"

    if new_order:

        async with state_lock:

            STATE["parents"].pop(
                parent_id,
                None
            )

            save_state()

        return "rearmed"

    async with state_lock:

        if parent_id in STATE["parents"]:

            STATE["parents"][
                parent_id
            ]["status"] = "NEEDS_REARM"

            save_state()

    return "failed"


async def _process_rearm_batch(
    parent_ids,
    reason="TP batch"
):
    """
    Process one deterministic LONG rearm batch:
    highest original BUY price -> lowest.
    Only after the full batch do one active-grid repair/trim pass.
    """
    normalized = {
        str(parent_id)
        for parent_id in parent_ids
        if parent_id is not None
    }

    if not normalized:
        return

    async with strategy_lock:

        async with state_lock:

            priced = []

            for parent_id in normalized:

                info = STATE["parents"].get(
                    parent_id
                )

                if not info:
                    continue

                try:
                    price = Decimal(
                        info["price"]
                    )
                except Exception:
                    continue

                priced.append(
                    (
                        price,
                        parent_id
                    )
                )

        # LONG: ближайшие к текущей цене после движения вверх —
        # это самые дорогие закрывшиеся входы.
        priced.sort(
            key=lambda item: item[0],
            reverse=True
        )

        if not priced:
            return

        if len(priced) > 1:

            order_text = " → ".join(
                P(price)
                for price, _ in priced
            )

            event_print(
                f"⚡ TP batch {len(priced)} | "
                f"rearm сверху вниз: {order_text}"
            )

        for _, parent_id in priced:

            await _rearm_one_locked(
                parent_id
            )

        # ВАЖНО: repair/trim ОДИН раз после всей пачки.
        # Лишние OPEN внутри repair по-прежнему удаляются снизу вверх.
        await _repair_active_grid_locked(
            f"после rearm batch | {reason}"
        )


async def _rearm_batch_worker():
    """
    Fixed 100 ms collection window from the first queued TP/rearm event.
    Events that arrive while a batch is being processed are collected into
    the next 100 ms batch.
    """
    global REARM_BATCH_TASK

    try:

        while True:

            await asyncio.sleep(
                REARM_BATCH_WINDOW
            )

            batch = list(
                REARM_BATCH_IDS
            )

            REARM_BATCH_IDS.difference_update(
                batch
            )

            if not batch:
                return

            await _process_rearm_batch(
                batch,
                reason=(
                    f"{int(REARM_BATCH_WINDOW * 1000)}ms window"
                )
            )

            if not REARM_BATCH_IDS:
                return

    finally:

        REARM_BATCH_TASK = None

        # Если событие попало точно на границу завершения worker,
        # оно не должно потеряться.
        if REARM_BATCH_IDS:

            REARM_BATCH_TASK = asyncio.create_task(
                _rearm_batch_worker()
            )


def queue_rearm(
    parent_id
):
    """
    Add parent to the shared 100 ms rearm window.
    Duplicate notifications for the same parent are coalesced by the set.
    """
    global REARM_BATCH_TASK

    if parent_id is None:
        return

    REARM_BATCH_IDS.add(
        str(parent_id)
    )

    if (
        REARM_BATCH_TASK is None
        or REARM_BATCH_TASK.done()
    ):

        REARM_BATCH_TASK = asyncio.create_task(
            _rearm_batch_worker()
        )


async def rearm(
    parent_id
):
    """
    Compatibility path for places that explicitly await a rearm.
    It still uses deterministic batch processing, but without adding an
    extra 100 ms delay because the caller has already collected the item.
    """
    await _process_rearm_batch(
        [parent_id],
        reason="direct rearm"
    )



# =========================================================
# LINK ATTACHED TP -> PARENT BUY
#
# WEEX V3 REST conditional-order field actualOrderId is the
# order created AFTER the conditional order triggers. It is
# NOT a reliable opening BUY parent ID.
#
# Our attached TP has:
#   parent BUY clientOrderId:  strk6-... 
#   TP clientAlgoId:           strk6-...tp
#
# Private WS additionally gives the real opening parent via:
#   openTpslParentOrderId
# =========================================================

def _algo_parent_client_id(algo):

    cid = str(
        algo.get(
            "clientAlgoId",
            ""
        )
    )

    if not cid.startswith(
        PREFIX
    ):
        return None

    # Longest suffix first: "...rtp" also ends with "tp".
    if cid.endswith(
        RECOVERY_TP_SUFFIX
    ):
        return cid[
            :-len(RECOVERY_TP_SUFFIX)
        ]

    if cid.endswith(
        "tp"
    ):
        return cid[:-2]

    return cid


def _recovery_tp_client_id(parent_client_id):

    parent_client_id = str(
        parent_client_id
    )

    result = (
        parent_client_id
        + RECOVERY_TP_SUFFIX
    )

    if len(result) > 36:
        raise RuntimeError(
            f"Recovery clientAlgoId длиннее 36 символов: {result}"
        )

    return result


def _parent_id_from_tp_ws_client_id(tp_client_id):

    tp_client_id = str(
        tp_client_id
    )

    async_parent_cid = None

    if tp_client_id.endswith(
        RECOVERY_TP_SUFFIX
    ):
        async_parent_cid = tp_client_id[
            :-len(RECOVERY_TP_SUFFIX)
        ]

    elif tp_client_id.endswith(
        "tp"
    ):
        async_parent_cid = tp_client_id[:-2]

    if not async_parent_cid:
        return None

    for parent_id, info in STATE.get(
        "parents",
        {}
    ).items():

        if str(
            info.get(
                "client_id",
                ""
            )
        ) == async_parent_cid:

            return str(
                parent_id
            )

    return None


def _algo_parent_from_maps(
    algo,
    parent_by_client_id,
    known_parent_ids
):
    """
    Resolve an attached TP to our BUY parent.

    Priority:
    1) openTpslParentOrderId if present and already known;
    2) clientAlgoId -> parent clientOrderId (normal REST path);
    3) actualOrderId ONLY if it exactly equals a known parent ID.
       This fallback preserves compatibility with any WEEX response
       variant without ever trusting triggered-order IDs blindly.
    """

    ws_parent = algo.get(
        "openTpslParentOrderId"
    )

    if ws_parent is not None:

        ws_parent = str(
            ws_parent
        )

        if (
            ws_parent != "0"
            and ws_parent in known_parent_ids
        ):
            return ws_parent

    parent_cid = _algo_parent_client_id(
        algo
    )

    if (
        parent_cid
        and parent_cid in parent_by_client_id
    ):
        return parent_by_client_id[
            parent_cid
        ]

    actual = algo.get(
        "actualOrderId"
    )

    if actual is not None:

        actual = str(
            actual
        )

        if actual in known_parent_ids:
            return actual

    return None


async def _tp_parent_maps_snapshot():

    async with state_lock:

        parent_by_client_id = {

            str(info.get("client_id")):
                str(parent_id)

            for parent_id, info
            in STATE["parents"].items()

            if info.get("client_id")
        }

        known_parent_ids = {
            str(parent_id)
            for parent_id
            in STATE["parents"].keys()
        }

    return (
        parent_by_client_id,
        known_parent_ids
    )


# =========================================================
# СИНХРОНИЗАЦИЯ С РЕАЛЬНЫМ СОСТОЯНИЕМ WEEX
# =========================================================

def _classify_tp_algo(algo):

    status = str(
        algo.get(
            "algoStatus",
            algo.get(
                "status",
                ""
            )
        )
    ).upper()

    trigger_time = int(
        algo.get(
            "triggerTime",
            0
        ) or 0
    )

    if (
        status in (
            "FILLED",
            "TRIGGERED"
        )
        or trigger_time > 0
    ):
        return "DONE"

    if status in (
        "UNTRIGGERED",
        "PENDING",
        "NEW"
    ):
        return "ACTIVE"

    return None


async def _find_tp_for_parent_fresh(
    parent_id,
    history_start_ms=None
):
    """
    Fresh authoritative lookup for BOTH the attached TP (...tp) and the
    emergency recovery TP (...rtp) belonging to one parent BUY.
    """
    parent_id = str(
        parent_id
    )

    async with state_lock:

        info = STATE[
            "parents"
        ].get(
            parent_id
        )

        if not info:
            return None, None

        parent_cid = str(
            info.get(
                "client_id",
                ""
            )
        )

    expected_cids = {
        parent_cid + "tp",
        _recovery_tp_client_id(
            parent_cid
        )
    }

    open_algos = await asyncio.to_thread(
        fetch_all_open_algos
    )

    for algo in open_algos:

        if str(
            algo.get(
                "clientAlgoId",
                ""
            )
        ) not in expected_cids:
            continue

        kind = _classify_tp_algo(
            algo
        )

        if kind:
            return kind, algo

    now_ms = int(
        time.time() * 1000
    )

    if history_start_ms is None:
        history_start_ms = max(
            0,
            now_ms
            - TP_RECONCILE_HISTORY_LOOKBACK_MS
        )

    history_raw = await asyncio.to_thread(

        private_request,

        "GET",

        "/capi/v3/allAlgoOrders",

        {
            "symbol": SYMBOL,
            "startTime": max(
                0,
                int(history_start_ms)
            ),
            "endTime": now_ms,
            "limit": 1000
        },

        None
    )

    if isinstance(
        history_raw,
        dict
    ):
        history = history_raw.get(
            "orders",
            []
        )

    elif isinstance(
        history_raw,
        list
    ):
        history = history_raw

    else:
        history = []

    newest = None
    newest_time = -1

    for algo in history:

        if str(
            algo.get(
                "clientAlgoId",
                ""
            )
        ) not in expected_cids:
            continue

        event_time = int(
            algo.get(
                "updateTime",
                0
            )
            or
            algo.get(
                "triggerTime",
                0
            )
            or 0
        )

        if event_time > newest_time:
            newest = algo
            newest_time = event_time

    if newest is not None:

        kind = _classify_tp_algo(
            newest
        )

        if kind:
            return kind, newest

    return None, None


async def _place_recovery_tp(
    parent_id,
    history_start_ms=None
):
    """
    Last-resort safety path for a BUY that is definitely FILLED while WEEX
    never materialized its attached TP.

    Never blindly posts: a fresh open/history check happens first.
    If POST outcome is ambiguous, re-query by deterministic clientAlgoId
    instead of retrying and risking a duplicate TP.
    """
    parent_id = str(
        parent_id
    )

    async with state_lock:

        info = STATE[
            "parents"
        ].get(
            parent_id
        )

        if not info:
            return "MISSING_PARENT"

        price = str(
            info["price"]
        )

        tp = str(
            info["tp"]
        )

        parent_cid = str(
            info["client_id"]
        )

    # Final duplicate guard immediately before POST.
    existing_kind, existing_algo = await _find_tp_for_parent_fresh(
        parent_id,
        history_start_ms
    )

    if existing_kind == "ACTIVE":

        async with state_lock:
            if parent_id in STATE["parents"]:
                STATE["parents"][parent_id]["status"] = "WAIT_TP"
                save_state()

        event_print(
            f"✅ TP для входа {price} найден перед recovery POST."
        )

        return "ACTIVE"

    if existing_kind == "DONE":

        async with state_lock:
            if parent_id in STATE["parents"]:
                STATE["parents"][parent_id]["status"] = "DONE_OFFLINE"
                save_state()

        event_print(
            f"✅ TP для входа {price} уже закрыт перед recovery POST."
        )

        return "DONE"

    recovery_cid = _recovery_tp_client_id(
        parent_cid
    )

    payload = {
        "symbol": SYMBOL,
        "clientAlgoId": recovery_cid,
        "planType": "TAKE_PROFIT",
        "triggerPrice": tp,
        "executePrice": "0",
        "quantity": format(
            QTY,
            "f"
        ),
        "positionSide": "LONG",
        "triggerPriceType": "CONTRACT_PRICE",
        "reduceOnly": True
    }

    try:

        result = await trade_request(
            "POST",
            "/capi/v3/placeTpSlOrder",
            payload=payload
        )

        rows = (
            result
            if isinstance(
                result,
                list
            )
            else [result]
        )

        accepted = any(
            isinstance(row, dict)
            and row.get("success") is True
            for row in rows
        )

        if not accepted:
            raise RuntimeError(
                f"WEEX recovery TP rejected: {result}"
            )

    except Exception as error:

        event_print(
            f"⚠️ Recovery TP {tp} для входа {price}: "
            f"неоднозначный/ошибочный POST ({error}) "
            "→ НЕ повторяю вслепую, проверяю WEEX."
        )

        # A timeout may mean WEEX accepted the TP but only the reply was lost.
        for _ in range(3):

            await asyncio.sleep(
                0.50
            )

            try:

                kind, algo = await _find_tp_for_parent_fresh(
                    parent_id,
                    history_start_ms
                )

            except Exception:
                continue

            if kind == "ACTIVE":

                async with state_lock:
                    if parent_id in STATE["parents"]:
                        STATE["parents"][parent_id]["status"] = "WAIT_TP"
                        save_state()

                event_print(
                    f"✅ Recovery TP {tp} подтверждён после "
                    f"неоднозначного POST | вход {price}"
                )

                return "ACTIVE"

            if kind == "DONE":

                async with state_lock:
                    if parent_id in STATE["parents"]:
                        STATE["parents"][parent_id]["status"] = "DONE_OFFLINE"
                        save_state()

                event_print(
                    f"✅ Recovery TP {tp} уже успел исполниться "
                    f"| вход {price}"
                )

                return "DONE"

        return "FAILED"

    async with state_lock:

        if parent_id in STATE["parents"]:

            STATE["parents"][parent_id]["status"] = "WAIT_TP"
            STATE["parents"][parent_id][
                "recovery_tp_client_id"
            ] = recovery_cid

            save_state()

    event_print(
        f"🛟 RECOVERY TP поставлен | "
        f"вход {price} → TP {tp} | cid={recovery_cid}"
    )

    return "ACTIVE"


async def reconcile(
    reason
):

    global PAUSED
    global STATE_SYNCED

    async with reconcile_lock:

        print()
        event_print(
            f"🔄 Синхронизация с WEEX: "
            f"{reason}"
        )

        completed = []
        ambiguous = []
        filled_without_tp = {}

        async with strategy_lock:

            try:

                open_orders = await asyncio.to_thread(
                    fetch_all_open_orders
                )

                open_algos = await asyncio.to_thread(
                    fetch_all_open_algos
                )

                algo_history_raw = await asyncio.to_thread(

                    private_request,

                    "GET",

                    "/capi/v3/allAlgoOrders",

                    {
                        "symbol": SYMBOL,
                        "limit": 1000
                    },

                    None
                )

            except Exception as error:

                PAUSED = True
                STATE_SYNCED = False

                event_print(
                    f"❌ Не удалось синхронизироваться: "
                    f"{error}"
                )

                event_print(
                    "⏸ Новые сделки временно "
                    "не выставляются."
                )

                return False


            if not isinstance(
                open_orders,
                list
            ):

                open_orders = []


            if not isinstance(
                open_algos,
                list
            ):

                open_algos = []


            if isinstance(
                algo_history_raw,
                dict
            ):

                algo_history = (
                    algo_history_raw.get(
                        "orders",
                        []
                    )
                )

            elif isinstance(
                algo_history_raw,
                list
            ):

                algo_history = (
                    algo_history_raw
                )

            else:

                algo_history = []


            # -----------------------------------------
            # Только наши BUY
            # -----------------------------------------

            open_map = {}

            for order in open_orders:

                cid = str(
                    order.get(
                        "clientOrderId",
                        ""
                    )
                )

                if not cid.startswith(
                    PREFIX
                ):
                    continue

                order_id = str(
                    order.get(
                        "orderId"
                    )
                )

                open_map[
                    order_id
                ] = order


            # -----------------------------------------
            # Только наши активные TP
            # -----------------------------------------

            (
                parent_by_client_id,
                known_parent_ids
            ) = await _tp_parent_maps_snapshot()

            tp_map = {}

            for algo in open_algos:

                cid = str(
                    algo.get(
                        "clientAlgoId",
                        ""
                    )
                )

                if not cid.startswith(
                    PREFIX
                ):
                    continue

                if "TAKE_PROFIT" not in str(
                    algo.get(
                        "orderType",
                        ""
                    )
                ):
                    continue

                parent = _algo_parent_from_maps(
                    algo,
                    parent_by_client_id,
                    known_parent_ids
                )

                if parent is None:
                    continue

                tp_map[
                    str(parent)
                ] = algo

                _private_ws_diag(
                    "REST_TP_LINKED_TO_PARENT",
                    {
                        "parentOrderId": str(parent),
                        "clientAlgoId": algo.get("clientAlgoId"),
                        "actualOrderId": algo.get("actualOrderId"),
                        "link": "clientAlgoId/openTpslParentOrderId/known-safe-fallback"
                    }
                )


            # -----------------------------------------
            # История наших TP
            # -----------------------------------------

            history_map = {}

            for algo in algo_history:

                cid = str(
                    algo.get(
                        "clientAlgoId",
                        ""
                    )
                )

                if not cid.startswith(
                    PREFIX
                ):
                    continue

                if "TAKE_PROFIT" not in str(
                    algo.get(
                        "orderType",
                        ""
                    )
                ):
                    continue

                parent = _algo_parent_from_maps(
                    algo,
                    parent_by_client_id,
                    known_parent_ids
                )

                if parent is None:
                    continue

                parent = str(
                    parent
                )

                old = history_map.get(
                    parent
                )

                if (
                    old is None

                    or int(
                        algo.get(
                            "updateTime",
                            0
                        ) or 0
                    )
                    >
                    int(
                        old.get(
                            "updateTime",
                            0
                        ) or 0
                    )
                ):

                    history_map[
                        parent
                    ] = algo


            # -----------------------------------------
            # Восстанавливаем живые BUY,
            # которых почему-то нет в state.json
            # -----------------------------------------

            async with state_lock:

                for order_id, order in (
                    open_map.items()
                ):

                    if (
                        order_id
                        in STATE["parents"]
                    ):
                        continue

                    price = D(
                        order["price"]
                    )

                    STATE["parents"][
                        order_id
                    ] = {

                        "price":
                            P(price),

                        "tp":
                            P(
                                price
                                + TP_DISTANCE
                            ),

                        "client_id":
                            str(
                                order.get(
                                    "clientOrderId",
                                    ""
                                )
                            ),

                        "status":
                            "OPEN"
                    }

                    event_print(
                        f"🧩 Восстановлен BUY "
                        f"{P(price)} "
                        f"из WEEX"
                    )


                # -------------------------------------
                # Восстанавливаем активные TP,
                # которых нет в state.json
                # -------------------------------------

                for parent_id, algo in (
                    tp_map.items()
                ):

                    if (
                        parent_id
                        in STATE["parents"]
                    ):
                        continue

                    tp = D(
                        algo["triggerPrice"]
                    )

                    price = (
                        tp
                        - TP_DISTANCE
                    )

                    algo_cid = str(
                        algo.get(
                            "clientAlgoId",
                            ""
                        )
                    )

                    if algo_cid.endswith(
                        "tp"
                    ):

                        parent_cid = (
                            algo_cid[:-2]
                        )

                    else:

                        parent_cid = (
                            algo_cid
                        )

                    STATE["parents"][
                        parent_id
                    ] = {

                        "price":
                            P(price),

                        "tp":
                            P(tp),

                        "client_id":
                            parent_cid,

                        "status":
                            "WAIT_TP"
                    }

                    event_print(
                        f"🧩 Восстановлен TP "
                        f"{P(tp)} "
                        f"для входа "
                        f"{P(price)}"
                    )


                tracked_ids = list(
                    STATE["parents"].keys()
                )


            # -----------------------------------------
            # Проверяем каждый известный цикл
            # -----------------------------------------

            need_check = []

            async with state_lock:

                pending_rearm_parents = {
                    str(p.get("rearm_parent_id"))
                    for p in STATE.get("pending_submits", {}).values()
                    if p.get("rearm_parent_id")
                }

                for parent_id in tracked_ids:

                    info = STATE[
                        "parents"
                    ].get(
                        parent_id
                    )

                    if not info:
                        continue

                    if parent_id in pending_rearm_parents:
                        info["status"] = "REARM_PENDING"
                        continue

                    if parent_id in open_map:

                        info["status"] = (
                            "OPEN"
                        )

                        continue

                    if parent_id in tp_map:

                        info["status"] = (
                            "WAIT_TP"
                        )

                        continue

                    history = history_map.get(
                        parent_id
                    )

                    if history:

                        h_status = str(
                            history.get(
                                "algoStatus",
                                ""
                            )
                        )

                        trigger_time = int(
                            history.get(
                                "triggerTime",
                                0
                            ) or 0
                        )

                        if (
                            h_status
                            in (
                                "FILLED",
                                "TRIGGERED"
                            )

                            or trigger_time > 0
                        ):

                            info["status"] = (
                                "DONE_OFFLINE"
                            )

                            completed.append(
                                parent_id
                            )

                            continue

                    # Если ни BUY,
                    # ни активного TP,
                    # ни завершённого TP не нашли,
                    # дополнительно спрашиваем
                    # сам родительский ордер.

                    info["status"] = (
                        "CHECKING"
                    )

                    need_check.append(
                        parent_id
                    )

                save_state()


            # -----------------------------------------
            # Дополнительная проверка сомнительных
            # -----------------------------------------

            for parent_id in need_check:

                try:

                    parent = await asyncio.to_thread(

                        private_request,

                        "GET",

                        "/capi/v3/order",

                        {
                            "orderId":
                                parent_id
                        },

                        None
                    )

                    parent_status = str(
                        parent.get(
                            "status",
                            ""
                        )
                    )

                except Exception as error:

                    parent_status = (
                        "UNKNOWN"
                    )

                    event_print(
                        f"⚠️ Не смог проверить "
                        f"{parent_id}: {error}"
                    )


                async with state_lock:

                    info = STATE[
                        "parents"
                    ].get(
                        parent_id
                    )

                    if not info:
                        continue


                    if parent_status == "CANCELED":

                        event_print(
                            f"ℹ️ Старый BUY "
                            f"{info['price']} "
                            f"был отменён."
                        )

                        STATE["parents"].pop(
                            parent_id,
                            None
                        )

                        continue


                    if parent_status == "FILLED":

                        # IMPORTANT v0.6.4:
                        # This is NOT an error yet. The BUY may have filled
                        # after the initial openAlgo/allAlgo snapshot, so its
                        # attached TP can appear a moment later.
                        info["status"] = (
                            "CHECKING_TP_GRACE"
                        )

                        parent_event_ms = 0

                        if isinstance(parent, dict):
                            for key in (
                                "updateTime",
                                "time",
                                "createdTime"
                            ):
                                try:
                                    value = int(
                                        parent.get(
                                            key,
                                            0
                                        ) or 0
                                    )
                                except Exception:
                                    value = 0

                                if value > parent_event_ms:
                                    parent_event_ms = value

                        filled_without_tp[
                            parent_id
                        ] = {
                            "price": info["price"],
                            "event_ms": parent_event_ms
                        }

                        event_print(
                            f"⏳ Вход "
                            f"{info['price']} "
                            f"FILLED, но TP нет в первом snapshot "
                            f"→ свежая перепроверка."
                        )

                        continue


                    info["status"] = (
                        "ERROR_UNKNOWN"
                    )

                    ambiguous.append(
                        parent_id
                    )

                    event_print(
                        f"🚨 Непонятное состояние "
                        f"ордера "
                        f"{parent_id} "
                        f"({info['price']})"
                    )



            # -----------------------------------------
            # v0.6.4: GRACE / FRESH TP RECHECK
            #
            # Race being fixed:
            #   initial REST snapshot says no TP
            #   -> BUY fills during reconcile
            #   -> /order already says FILLED
            #   -> attached TP appears/triggers a moment later
            #
            # Never PAUSE immediately in that window.
            # -----------------------------------------

            if filled_without_tp:

                unresolved = set(
                    filled_without_tp.keys()
                )

                event_times = [
                    int(meta.get("event_ms", 0) or 0)
                    for meta in filled_without_tp.values()
                    if int(meta.get("event_ms", 0) or 0) > 0
                ]

                if event_times:
                    history_start_ms = max(
                        0,
                        min(event_times)
                        - TP_RECONCILE_HISTORY_LOOKBACK_MS
                    )
                else:
                    history_start_ms = max(
                        0,
                        int(time.time() * 1000)
                        - TP_RECONCILE_HISTORY_LOOKBACK_MS
                    )

                oldest_event_ms = min(
                    event_times,
                    default=0
                )

                fill_age_ms = (
                    int(time.time() * 1000)
                    - oldest_event_ms
                    if oldest_event_ms > 0
                    else 0
                )

                grace_attempts = (
                    1
                    if fill_age_ms > TP_RECONCILE_OLD_FILL_MS
                    else TP_RECONCILE_GRACE_ATTEMPTS
                )

                for attempt in range(
                    grace_attempts
                ):

                    if not unresolved:
                        break

                    # First trust a newer private-WS fact over the old REST snapshot.
                    ws_done = {
                        parent_id
                        for parent_id in unresolved
                        if TP_WS_STATUS_BY_PARENT.get(
                            parent_id
                        ) == "FILLED"
                    }

                    ws_active = {
                        parent_id
                        for parent_id in unresolved
                        if TP_WS_STATUS_BY_PARENT.get(
                            parent_id
                        ) in (
                            "PENDING",
                            "UNTRIGGERED",
                            "NEW"
                        )
                    }

                    if ws_done or ws_active:

                        async with state_lock:

                            for parent_id in ws_done:

                                info = STATE["parents"].get(
                                    parent_id
                                )

                                if not info:
                                    continue

                                info["status"] = (
                                    "DONE_OFFLINE"
                                )

                                if parent_id not in completed:
                                    completed.append(
                                        parent_id
                                    )

                                event_print(
                                    f"✅ TP для входа "
                                    f"{info['price']} подтверждён private WS: FILLED"
                                )

                            for parent_id in ws_active:

                                info = STATE["parents"].get(
                                    parent_id
                                )

                                if not info:
                                    continue

                                info["status"] = (
                                    "WAIT_TP"
                                )

                                event_print(
                                    f"✅ TP для входа "
                                    f"{info['price']} подтверждён private WS: "
                                    f"{TP_WS_STATUS_BY_PARENT.get(parent_id)}"
                                )

                            save_state()

                        unresolved.difference_update(
                            ws_done
                        )
                        unresolved.difference_update(
                            ws_active
                        )

                    if not unresolved:
                        break

                    if attempt > 0:
                        await asyncio.sleep(
                            TP_RECONCILE_GRACE_DELAY
                        )

                    try:

                        fresh_open_algos = await asyncio.to_thread(
                            fetch_all_open_algos
                        )

                        now_ms = int(
                            time.time() * 1000
                        )

                        fresh_history_raw = await asyncio.to_thread(

                            private_request,

                            "GET",

                            "/capi/v3/allAlgoOrders",

                            {
                                "symbol": SYMBOL,
                                "startTime": history_start_ms,
                                "endTime": now_ms,
                                "limit": 1000
                            },

                            None
                        )

                    except Exception as error:

                        event_print(
                            f"⚠️ Fresh TP recheck "
                            f"{attempt + 1}/{TP_RECONCILE_GRACE_ATTEMPTS} "
                            f"не удался: {error}"
                        )

                        continue

                    if not isinstance(
                        fresh_open_algos,
                        list
                    ):
                        fresh_open_algos = []

                    if isinstance(
                        fresh_history_raw,
                        dict
                    ):
                        fresh_history = (
                            fresh_history_raw.get(
                                "orders",
                                []
                            )
                        )
                    elif isinstance(
                        fresh_history_raw,
                        list
                    ):
                        fresh_history = (
                            fresh_history_raw
                        )
                    else:
                        fresh_history = []

                    (
                        fresh_parent_by_client_id,
                        fresh_known_parent_ids
                    ) = await _tp_parent_maps_snapshot()

                    fresh_active_parents = set()

                    for algo in fresh_open_algos:

                        cid = str(
                            algo.get(
                                "clientAlgoId",
                                ""
                            )
                        )

                        if not cid.startswith(
                            PREFIX
                        ):
                            continue

                        if "TAKE_PROFIT" not in str(
                            algo.get(
                                "orderType",
                                ""
                            )
                        ):
                            continue

                        parent = _algo_parent_from_maps(
                            algo,
                            fresh_parent_by_client_id,
                            fresh_known_parent_ids
                        )

                        if parent is None:
                            continue

                        fresh_active_parents.add(
                            str(parent)
                        )

                    fresh_done_parents = set()

                    for algo in fresh_history:

                        cid = str(
                            algo.get(
                                "clientAlgoId",
                                ""
                            )
                        )

                        if not cid.startswith(
                            PREFIX
                        ):
                            continue

                        if "TAKE_PROFIT" not in str(
                            algo.get(
                                "orderType",
                                ""
                            )
                        ):
                            continue

                        parent = _algo_parent_from_maps(
                            algo,
                            fresh_parent_by_client_id,
                            fresh_known_parent_ids
                        )

                        if parent is None:
                            continue

                        h_status = str(
                            algo.get(
                                "algoStatus",
                                ""
                            )
                        )

                        trigger_time = int(
                            algo.get(
                                "triggerTime",
                                0
                            ) or 0
                        )

                        if (
                            h_status
                            in (
                                "FILLED",
                                "TRIGGERED"
                            )
                            or trigger_time > 0
                        ):

                            fresh_done_parents.add(
                                str(parent)
                            )

                    resolved_done = (
                        unresolved
                        & fresh_done_parents
                    )

                    resolved_active = (
                        unresolved
                        & fresh_active_parents
                    ) - resolved_done

                    if resolved_done or resolved_active:

                        async with state_lock:

                            for parent_id in resolved_done:

                                info = STATE["parents"].get(
                                    parent_id
                                )

                                if not info:
                                    continue

                                info["status"] = (
                                    "DONE_OFFLINE"
                                )

                                if parent_id not in completed:
                                    completed.append(
                                        parent_id
                                    )

                                event_print(
                                    f"✅ Закрытый TP найден свежей проверкой "
                                    f"для входа {info['price']}."
                                )

                            for parent_id in resolved_active:

                                info = STATE["parents"].get(
                                    parent_id
                                )

                                if not info:
                                    continue

                                info["status"] = (
                                    "WAIT_TP"
                                )

                                event_print(
                                    f"✅ Активный TP найден свежей проверкой "
                                    f"для входа {info['price']}."
                                )

                            save_state()

                        unresolved.difference_update(
                            resolved_done
                        )
                        unresolved.difference_update(
                            resolved_active
                        )

                    if (
                        unresolved
                        and attempt
                        < grace_attempts - 1
                    ):

                        event_print(
                            f"⏳ TP ещё не виден для "
                            f"{len(unresolved)} FILLED BUY "
                            f"| retry {attempt + 2}/"
                            f"{grace_attempts}"
                        )

                # One last private-WS check after the final REST request.
                final_ws_done = {
                    parent_id
                    for parent_id in unresolved
                    if TP_WS_STATUS_BY_PARENT.get(
                        parent_id
                    ) == "FILLED"
                }

                if final_ws_done:

                    async with state_lock:

                        for parent_id in final_ws_done:

                            info = STATE["parents"].get(
                                parent_id
                            )

                            if not info:
                                continue

                            info["status"] = (
                                "DONE_OFFLINE"
                            )

                            if parent_id not in completed:
                                completed.append(
                                    parent_id
                                )

                        save_state()

                    unresolved.difference_update(
                        final_ws_done
                    )

                # If the attached TP is genuinely absent, do not leave a
                # naked filled BUY. Create a standalone exchange-side recovery
                # TAKE_PROFIT for the exact same 20 STRK / trigger.
                if unresolved:

                    async with state_lock:

                        recovery_order = sorted(
                            (
                                (
                                    Decimal(
                                        STATE["parents"][parent_id]["tp"]
                                    ),
                                    parent_id
                                )
                                for parent_id in unresolved
                                if parent_id in STATE["parents"]
                            ),
                            key=lambda item: item[0]
                        )

                    recovered_done = set()
                    recovered_active = set()
                    recovery_failed = set()

                    for _, parent_id in recovery_order:

                        event_print(
                            f"🛟 Attached TP реально отсутствует "
                            f"для parent={parent_id} "
                            "→ ставлю recovery TP."
                        )

                        outcome = await _place_recovery_tp(
                            parent_id,
                            history_start_ms
                        )

                        if outcome == "DONE":
                            recovered_done.add(
                                parent_id
                            )

                            if parent_id not in completed:
                                completed.append(
                                    parent_id
                                )

                        elif outcome == "ACTIVE":
                            recovered_active.add(
                                parent_id
                            )

                        else:
                            recovery_failed.add(
                                parent_id
                            )

                    unresolved.difference_update(
                        recovered_done
                    )
                    unresolved.difference_update(
                        recovered_active
                    )

                    if recovery_failed:

                        async with state_lock:

                            for parent_id in sorted(
                                recovery_failed
                            ):

                                info = STATE["parents"].get(
                                    parent_id
                                )

                                if not info:
                                    continue

                                info["status"] = (
                                    "ERROR_NO_TP"
                                )

                                if parent_id not in ambiguous:
                                    ambiguous.append(
                                        parent_id
                                    )

                                event_print(
                                    f"🚨 Вход {info['price']} исполнен, "
                                    "attached TP отсутствует и recovery TP "
                                    "не удалось надёжно создать."
                                )

                            save_state()


            # -----------------------------------------
            # Восстанавливаем правильный TOP
            # -----------------------------------------

            async with state_lock:

                prices = [

                    Decimal(
                        info["price"]
                    )

                    for info
                    in STATE["parents"].values()
                ]

                if prices:

                    highest = max(
                        prices
                    )

                    if (
                        STATE.get("top")
                        is None
                    ):

                        STATE["top"] = P(
                            highest
                        )

                    else:

                        old_top = Decimal(
                            STATE["top"]
                        )

                        if highest > old_top:

                            STATE["top"] = P(
                                highest
                            )


                STATE[
                    "last_reconcile"
                ] = int(
                    time.time() * 1000
                )


                limits = sum(

                    1

                    for info
                    in STATE["parents"].values()

                    if info["status"]
                    == "OPEN"
                )


                waiting = sum(

                    1

                    for info
                    in STATE["parents"].values()

                    if info["status"]
                    == "WAIT_TP"
                )


                save_state()


            if ambiguous:

                PAUSED = True
                STATE_SYNCED = False

                print()
                event_print(
                    f"🚨 Найдено непонятных "
                    f"циклов: "
                    f"{len(ambiguous)}"
                )

                event_print(
                    "⏸ Бот НЕ будет выставлять "
                    "новые ордера, пока "
                    "не разберёмся."
                )

                notify_alert(
                    f"🚨 PAUSE: reconcile нашёл непонятных циклов: "
                    f"{len(ambiguous)}.",
                    key="reconcile_ambiguous",
                    cooldown=60
                )

                return False


            STATE_SYNCED = True

            # REST reconciliation alone never grants permission to trade.
            PAUSED = not private_ws_ready.is_set()

            event_print(
                f"✅ Синхронизация завершена."
            )

            if PAUSED:
                event_print(
                    "⏸ REST state синхронизирован, но private WS не готов — "
                    "новые ордера запрещены."
                )

            print(
                f"   BUY LIMIT: {limits}"
            )

            print(
                f"   ждут TP:   {waiting}"
            )

            print(
                f"   завершились офлайн: "
                f"{len(completed)}"
            )

            print(
                f"   верх сетки: "
                f"{STATE.get('top')}"
            )


        # strategy_lock здесь уже отпущен.

        if completed:

            await _process_rearm_batch(
                completed,
                reason="reconcile completed offline"
            )

        return True


# =========================================================
# PRIVATE WS EVENT HANDLERS: orders + fill
# =========================================================

async def _mark_buy_filled(parent_id, source):
    """
    Single idempotent transition OPEN -> WAIT_TP.
    Whichever channel notices the full BUY first wins:
    orders WS, fill WS, or REST safety recovery.
    """
    parent_id = str(parent_id)

    async with state_lock:
        info = STATE["parents"].get(parent_id)

        if not info or info.get("status") != "OPEN":
            return False

        info["status"] = "WAIT_TP"
        price = info["price"]
        tp = info["tp"]
        save_state()

    suffix = (
        " | REST recovery"
        if str(source).startswith("REST")
        else ""
    )

    event_print(
        f"💥 Вход {price} исполнен "
        f"→ ждём TP {tp}{suffix}"
    )

    _private_ws_diag(
        "BUY_FILLED_STATE_TRANSITION",
        {
            "parentOrderId": parent_id,
            "price": price,
            "tp": tp,
            "source": source
        }
    )

    return True


async def handle_fill(fill):
    """
    V3 private `fill` channel: actual matched trades.
    For this LONG-only bot, BUY fills correspond to parent entry orders.
    We accumulate partial executions and mark WAIT_TP only when the full
    configured QTY has been matched.
    """
    if fill.get("symbol") != SYMBOL:
        return

    if str(fill.get("orderSide", "")).upper() != "BUY":
        return

    order_id = fill.get("orderId")
    if order_id is None:
        return

    order_id = str(order_id)

    fill_id = fill.get("id")
    if not _remember_fill_id(fill_id):
        return

    try:
        fill_size = Decimal(str(fill.get("fillSize", "0")))
    except Exception:
        fill_size = Decimal("0")

    known = False
    current_status = None

    # REST response can race the WS fill. Give place_entry() a short moment
    # to store the confirmed parent in STATE.
    for _ in range(10):
        async with state_lock:
            info = STATE["parents"].get(order_id)
            if info:
                known = True
                current_status = info.get("status")
                break
        await asyncio.sleep(0.05)

    if not known:
        _private_ws_diag(
            "FILL_UNMATCHED_PARENT",
            {
                "orderId": order_id,
                "fillId": fill_id,
                "fillSize": str(fill_size)
            }
        )
        return

    if current_status != "OPEN":
        return

    total = (
        FILL_ACCUMULATED_QTY.get(order_id, Decimal("0"))
        + fill_size
    )
    FILL_ACCUMULATED_QTY[order_id] = total

    _private_ws_diag(
        "FILL_ACCUMULATED",
        {
            "orderId": order_id,
            "fillId": fill_id,
            "thisFillSize": str(fill_size),
            "accumulated": str(total),
            "target": str(QTY)
        }
    )

    if total + Decimal("0.00000001") < QTY:
        return

    FILL_ACCUMULATED_QTY.pop(order_id, None)

    changed = await _mark_buy_filled(
        order_id,
        "private fill WS"
    )

    if changed:
        asyncio.create_task(
            maintain_active_limits(
                "после исполнения BUY (fill WS)"
            )
        )


async def handle_order(order):

    global PAUSED

    if order.get("symbol") != SYMBOL:
        return

    status = str(
        order.get(
            "status",
            ""
        )
    )

    order_type = str(
        order.get(
            "type",
            ""
        )
    )

    parent = str(
        order.get(
            "openTpslParentOrderId",
            "0"
        )
    )

    cid = str(
        order.get(
            "clientOrderId",
            ""
        )
    )

    # TP — attached or standalone recovery.
    if "TAKE_PROFIT" in order_type:

        resolved_parent = None

        if parent != "0":

            async with state_lock:
                if parent in STATE["parents"]:
                    resolved_parent = parent

        if resolved_parent is None:

            async with state_lock:
                resolved_parent = _parent_id_from_tp_ws_client_id(
                    cid
                )

        if resolved_parent is None:
            return

        parent = str(
            resolved_parent
        )

        _remember_tp_ws_status(
            parent,
            status
        )

        if status == "FILLED":

            event_print(
                f"🎯 TP исполнен | parent={parent}"
            )

            queue_rearm(
                parent
            )

            return

        if status == "CANCELED":

            async with state_lock:

                if parent in STATE["parents"]:

                    STATE["parents"][parent][
                        "status"
                    ] = "ERROR_TP_CANCELED"

                    save_state()

            PAUSED = True

            event_print(
                f"🚨 TP отменён для parent={parent}"
            )

            event_print(
                "⏸ Новые ордера остановлены."
            )

            notify_alert(
                f"🚨 PAUSE: TP отменён для parent={parent}.",
                key="tp_canceled",
                cooldown=60
            )

            return

        # PENDING / UNTRIGGERED / NEW are valid.
        return

    # BUY
    if not cid.startswith(PREFIX):
        return

    found = None

    # REST response and WebSocket event can arrive almost simultaneously.
    for _ in range(10):
        async with state_lock:
            found = next(
                (
                    (order_id, info)
                    for order_id, info in STATE["parents"].items()
                    if info["client_id"] == cid
                ),
                None
            )
        if found:
            break
        await asyncio.sleep(0.05)

    if not found:
        async with state_lock:
            pending = STATE.get("pending_submits", {}).get(cid)

        if pending:
            # V3 orders WS uses `id` for the order ID.
            # `orderId` is used by the fill channel / many REST payloads.
            remote_order_id = order.get("id")

            if remote_order_id is None:
                _private_ws_diag(
                    "PENDING_ORDER_EVENT_WITHOUT_ID",
                    order
                )
                return

            remote_order_id = str(remote_order_id)

            if status == "FILLED":
                await _adopt_pending_order(
                    cid,
                    remote_order_id,
                    "WAIT_TP",
                    "private orders WebSocket"
                )
                event_print(
                    f"💥 Вход {pending['price']} исполнен "
                    f"→ ждём TP {pending['tp']}"
                )
                _private_ws_diag(
                    "PENDING_BUY_FILLED_VIA_ORDERS",
                    {
                        "orderId": remote_order_id,
                        "clientOrderId": cid,
                        "price": pending["price"],
                        "tp": pending["tp"]
                    }
                )
                asyncio.create_task(
                    maintain_active_limits(
                        "после pending BUY fill"
                    )
                )
                return

            if status == "CANCELED":
                old_rearm = await _clear_pending_as_absent(
                    cid,
                    "private WebSocket CANCELED"
                )
                event_print(
                    f"⚠️ EXCHANGE CANCEL BUY {pending['price']} "
                    "| pending POST / возможно POST_ONLY"
                )
                if old_rearm:
                    queue_rearm(
                        str(old_rearm)
                    )
                asyncio.create_task(
                    maintain_active_limits(
                        f"exchange cancel pending {pending['price']}"
                    )
                )
                return

            await _adopt_pending_order(
                cid,
                remote_order_id,
                "OPEN",
                "private orders WebSocket"
            )
            return

        _private_ws_diag(
            "ORDER_EVENT_UNMATCHED_CLIENT_ID",
            order
        )
        return

    order_id, info = found

    if status == "FILLED":
        FILL_ACCUMULATED_QTY.pop(str(order_id), None)
        changed = await _mark_buy_filled(
            order_id,
            "private orders WS"
        )
        if changed:
            asyncio.create_task(
                maintain_active_limits(
                    "после исполнения BUY"
                )
            )

    elif status == "CANCELED":
        order_id = str(order_id)
        bot_cancel = order_id in BOT_CANCEL_IN_FLIGHT

        async with state_lock:
            old = STATE["parents"].pop(order_id, None)
            save_state()

        FILL_ACCUMULATED_QTY.pop(order_id, None)

        if bot_cancel:
            BOT_CANCEL_CONFIRMED_WS.add(order_id)
            _private_ws_diag(
                "BOT_CANCEL_CONFIRMED_BY_WS",
                {
                    "orderId": order_id,
                    "price": old.get("price") if old else None,
                    "cancelReason": order.get("cancelReason")
                }
            )
            return

        if old:
            cancel_reason = str(
                order.get("cancelReason", "UNKNOWN")
            )
            event_print(
                f"⚠️ EXCHANGE CANCEL BUY {old['price']}"
                f" | {cancel_reason}"
            )
            asyncio.create_task(
                maintain_active_limits(
                    f"exchange cancel {old['price']}"
                )
            )


# =========================================================
# PRIVATE WEBSOCKET AUTH
# =========================================================

def websocket_headers():

    # Используем то же синхронизированное с WEEX время,
    # что и для private REST.
    timestamp = signed_timestamp()

    message = (
        timestamp
        + "/v3/ws/private"
    )

    digest = hmac.new(
        SECRET.encode(),
        message.encode(),
        hashlib.sha256
    ).digest()

    signature = base64.b64encode(
        digest
    ).decode()

    return {

        "ACCESS-KEY":
            API_KEY,

        "ACCESS-PASSPHRASE":
            PASSPHRASE,

        "ACCESS-TIMESTAMP":
            timestamp,

        "ACCESS-SIGN":
            signature
    }


# =========================================================
# PRIVATE WEBSOCKET
# orders = lifecycle
# fill   = actual matched executions
# v0.6.0 keeps heartbeat diagnostics + zombie-stream watchdog from bot05 v0.5.5.
# =========================================================

async def _send_ws_pong(ws, ping_message=None):
    await ws.send(
        json.dumps({
            "method": "PONG",
            "id": 1
        })
    )
    _private_ws_diag(
        "PRIVATE_WS_PONG_SENT",
        {
            "serverTime": (
                ping_message.get("time")
                if isinstance(ping_message, dict)
                else None
            )
        }
    )


async def _handle_private_message(message):
    event_type = message.get("e")

    if event_type == "orders":
        for order in message.get("d", []):
            await handle_order(order)

    elif event_type == "fill":
        for fill in message.get("d", []):
            await handle_fill(fill)

    elif (
        message.get("id") == 9
        and "result" in message
        and message.get("result") is True
    ):
        if not fill_ws_ready.is_set():
            fill_ws_ready.set()
            event_print(
                "✅ WebSocket fill: подписка подтверждена WEEX"
            )


async def _subscribe_private_channels(ws):
    """Subscribe to orders+fill and buffer any pushes received before ACK."""
    buffered = []

    await ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": ["orders"],
        "id": 1
    }))
    await ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": ["fill"],
        "id": 9
    }))

    _private_ws_diag(
        "SUBSCRIBE_SENT",
        {"orders": 1, "fill": 9}
    )

    orders_ack = False
    fill_ack = False
    deadline = time.monotonic() + 8

    while time.monotonic() < deadline:
        if orders_ack and fill_ack:
            break

        remaining = deadline - time.monotonic()

        try:
            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=max(0.1, remaining)
            )
        except asyncio.TimeoutError:
            break

        message = json.loads(raw)
        _note_private_ws_rx(message)

        if (
            message.get("type") == "ping"
            or message.get("event") == "ping"
        ):
            _private_ws_diag("PRIVATE_WS_PING", message)
            await _send_ws_pong(ws, message)
            continue

        _private_ws_diag("PRIVATE_WS_IN", message)

        if message.get("id") == 1 and "result" in message:
            if message.get("result") is True:
                orders_ack = True
            else:
                raise RuntimeError(
                    "WEEX отклонил подписку orders: "
                    f"{message.get('msg', message)}"
                )
            continue

        if message.get("id") == 9 and "result" in message:
            if message.get("result") is True:
                fill_ack = True
                fill_ws_ready.set()
            else:
                _private_ws_diag(
                    "FILL_SUBSCRIBE_REJECTED",
                    message
                )
            continue

        if message.get("e") in ("orders", "fill"):
            buffered.append(message)

    if not orders_ack:
        raise RuntimeError(
            "WEEX не подтвердил подписку orders за 8 сек"
        )

    return buffered, fill_ack


async def order_watcher():
    global PAUSED
    global STATE_SYNCED
    global PRIVATE_WS_RECONNECT_REASON

    first_connection = True
    ws_down_since = None
    ws_down_alerted = False

    while True:
        private_ws_ready.clear()
        fill_ws_ready.clear()
        PRIVATE_WS_RECONNECT_EVENT.clear()
        PRIVATE_WS_RECONNECT_REASON = None

        try:
            _private_ws_diag(
                "PRIVATE_WS_CONNECTING",
                {"endpoint": PRIVATE_WS, "version": VERSION}
            )

            async with websockets.connect(
                PRIVATE_WS,
                additional_headers=websocket_headers(),
                user_agent_header=UA,
                ping_interval=None,
                open_timeout=15
            ) as ws:
                _note_private_ws_rx()
                _private_ws_diag(
                    "PRIVATE_WS_CONNECTED",
                    {"version": VERSION}
                )

                buffered_messages, fill_confirmed = (
                    await _subscribe_private_channels(ws)
                )

                private_ws_ready.set()

                if ws_down_since is not None:
                    down_for = (
                        time.monotonic()
                        - ws_down_since
                    )

                    if ws_down_alerted:
                        notify_alert(
                            f"✅ Private WS восстановлен после {down_for:.0f} сек.",
                            key="private_ws_restored",
                            cooldown=0
                        )

                    ws_down_since = None
                    ws_down_alerted = False

                event_print(
                    "✅ WebSocket orders: подписка подтверждена WEEX"
                )
                if fill_confirmed:
                    event_print(
                        "✅ WebSocket fill: подписка подтверждена WEEX"
                    )
                else:
                    event_print(
                        "⚠️ WebSocket fill не подтвердился. "
                        "orders + REST safety продолжают работать."
                    )

                reason = (
                    f"первый запуск v{VERSION}"
                    if first_connection
                    else "WebSocket переподключился"
                )

                async with maintenance_lock:
                    ok = await reconcile(reason)

                if not ok:
                    STATE_SYNCED = False
                    PAUSED = True
                    private_ws_ready.clear()
                    ws_ready.clear()
                    event_print(
                        "⏳ Старт не завершён: state не синхронизирован. "
                        "Повтор через 5 сек."
                    )
                    await asyncio.sleep(5)
                    continue

                async with state_lock:
                    have_cycles_after_sync = bool(STATE["parents"])

                if have_cycles_after_sync:
                    await maintain_active_limits(
                        "после синхронизации WEEX"
                    )

                event_print(
                    "🔐 Слежение за ордерами подключено"
                )
                ws_ready.set()
                first_connection = False

                for buffered_message in buffered_messages:
                    await _handle_private_message(buffered_message)

                # Bounded recv is intentional: a dead account stream can leave
                # the TCP/WebSocket session apparently open for a long time.
                while True:
                    if PRIVATE_WS_RECONNECT_EVENT.is_set():
                        reconnect_reason = (
                            PRIVATE_WS_RECONNECT_REASON
                            or "запрошен принудительный reconnect"
                        )
                        PRIVATE_WS_RECONNECT_EVENT.clear()
                        raise PrivateWSReconnectRequested(
                            reconnect_reason
                        )

                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=PRIVATE_WS_WATCHDOG_TICK
                        )
                    except asyncio.TimeoutError:
                        if PRIVATE_WS_RECONNECT_EVENT.is_set():
                            reconnect_reason = (
                                PRIVATE_WS_RECONNECT_REASON
                                or "запрошен принудительный reconnect"
                            )
                            PRIVATE_WS_RECONNECT_EVENT.clear()
                            raise PrivateWSReconnectRequested(
                                reconnect_reason
                            )

                        silence = (
                            time.monotonic()
                            - PRIVATE_WS_LAST_RX_MONO
                        )
                        if silence >= PRIVATE_WS_SILENCE_SECONDS:
                            _private_ws_diag(
                                "PRIVATE_WS_WATCHDOG_STALE",
                                {
                                    "silenceSeconds": round(silence, 1),
                                    "thresholdSeconds": PRIVATE_WS_SILENCE_SECONDS
                                }
                            )
                            raise PrivateWSReconnectRequested(
                                f"private stream молчит {silence:.0f} сек"
                            )
                        continue

                    message = json.loads(raw)
                    _note_private_ws_rx(message)

                    if (
                        message.get("type") == "ping"
                        or message.get("event") == "ping"
                    ):
                        _private_ws_diag("PRIVATE_WS_PING", message)
                        await _send_ws_pong(ws, message)
                        continue

                    # Raw event is persisted before any parser/filter logic.
                    _private_ws_diag("PRIVATE_WS_IN", message)
                    await _handle_private_message(message)

        except PrivateWSReconnectRequested as error:
            PAUSED = True
            STATE_SYNCED = False
            private_ws_ready.clear()
            ws_ready.clear()
            fill_ws_ready.clear()

            if ws_down_since is None:
                ws_down_since = time.monotonic()
            _private_ws_diag(
                "PRIVATE_WS_FORCED_RECONNECT",
                {"reason": str(error)}
            )
            event_print(
                f"♻️ Private WebSocket переподключается: {error}"
            )
            await asyncio.sleep(2)

        except Exception as error:
            PAUSED = True
            STATE_SYNCED = False
            private_ws_ready.clear()
            ws_ready.clear()
            fill_ws_ready.clear()

            if ws_down_since is None:
                ws_down_since = time.monotonic()

            down_for = (
                time.monotonic()
                - ws_down_since
            )

            if (
                down_for >= PRIVATE_WS_ALERT_AFTER
                and not ws_down_alerted
            ):
                notify_alert(
                    f"⚠️ Private WS не восстанавливается {down_for:.0f} сек. "
                    f"Последняя ошибка: {error}",
                    key="private_ws_down",
                    cooldown=300
                )
                ws_down_alerted = True

            _private_ws_diag(
                "PRIVATE_WS_DISCONNECTED",
                {"error": str(error)}
            )
            print()
            event_print(
                f"⚠️ WebSocket отключён: {error}"
            )
            event_print(
                "⏸ Новые ордера временно не ставятся."
            )
            event_print(
                "Переподключаюсь через 2 сек..."
            )
            await asyncio.sleep(2)


# =========================================================
# PENDING POST RECOVERY
#
# If POST /order times out, the result is ambiguous. We keep the exact
# newClientOrderId and NEVER resend blindly. Private WS or this REST resolver
# adopts the real order once connectivity returns. Only after two successful
# exchange checks show no trace of the clientOrderId may the grid retry it.
# =========================================================


def fetch_recent_order_history(start_ms):

    result = private_request(
        "GET",
        "/capi/v3/order/history",
        {
            "symbol": SYMBOL,
            "startTime": max(0, int(start_ms) - 60_000),
            "endTime": int(time.time() * 1000),
            "limit": 100,
            "page": 0
        },
        None
    )

    if not isinstance(result, list):
        raise RuntimeError(
            "order/history вернул неожиданный формат"
        )

    return result


def fetch_recent_algo_history(start_ms):

    result = private_request(
        "GET",
        "/capi/v3/allAlgoOrders",
        {
            "symbol": SYMBOL,
            "startTime": max(0, int(start_ms) - 60_000),
            "endTime": int(time.time() * 1000),
            "limit": 1000
        },
        None
    )

    if isinstance(result, dict):
        return result.get("orders", [])

    if isinstance(result, list):
        return result

    raise RuntimeError(
        "allAlgoOrders вернул неожиданный формат"
    )


async def _adopt_pending_order(
    cid,
    parent_id,
    status,
    source
):

    async with state_lock:

        pending = STATE.get(
            "pending_submits",
            {}
        ).get(cid)

        if not pending:
            return None

        old_rearm_parent = pending.get(
            "rearm_parent_id"
        )

        if old_rearm_parent:
            STATE["parents"].pop(
                str(old_rearm_parent),
                None
            )

        STATE["parents"][str(parent_id)] = {
            "price": pending["price"],
            "tp": pending["tp"],
            "client_id": cid,
            "status": status
        }

        STATE["pending_submits"].pop(
            cid,
            None
        )

        save_state()

    event_print(
        f"🛟 Pending BUY {pending['price']} подтверждён через {source} "
        f"→ {status} | id={parent_id}"
    )

    return old_rearm_parent


async def _clear_pending_as_absent(cid, reason):

    async with state_lock:

        pending = STATE.get(
            "pending_submits",
            {}
        ).pop(cid, None)

        if not pending:
            return None

        old_rearm_parent = pending.get(
            "rearm_parent_id"
        )

        if old_rearm_parent:
            old = STATE["parents"].get(
                str(old_rearm_parent)
            )
            if old:
                old["status"] = "NEEDS_REARM"

        save_state()

    event_print(
        f"🧹 Pending BUY {pending['price']} не найден на WEEX ({reason}). "
        "Повтор теперь разрешён."
    )

    return old_rearm_parent


async def resolve_pending_submits(open_orders=None, open_algos=None):

    async with state_lock:
        pending_snapshot = dict(
            STATE.get("pending_submits", {})
        )

    if not pending_snapshot:
        return False

    if open_orders is None:
        open_orders = await asyncio.to_thread(
            fetch_all_open_orders
        )

    if open_algos is None:
        open_algos = await asyncio.to_thread(
            fetch_all_open_algos
        )

    open_by_cid = {
        str(order.get("clientOrderId", "")): order
        for order in open_orders
        if str(order.get("clientOrderId", "")).startswith(PREFIX)
    }

    tp_by_parent_cid = {}

    for algo in open_algos:
        algo_cid = str(algo.get("clientAlgoId", ""))
        if not algo_cid.startswith(PREFIX):
            continue
        if "TAKE_PROFIT" not in str(algo.get("orderType", "")):
            continue
        if algo_cid.endswith(
            RECOVERY_TP_SUFFIX
        ):
            parent_cid = algo_cid[
                :-len(RECOVERY_TP_SUFFIX)
            ]
        elif algo_cid.endswith(
            "tp"
        ):
            parent_cid = algo_cid[:-2]
        else:
            parent_cid = algo_cid

        tp_by_parent_cid[parent_cid] = algo

    changed = False
    need_history = []

    for cid, pending in pending_snapshot.items():

        order = open_by_cid.get(cid)

        if order and order.get("orderId") is not None:
            await _adopt_pending_order(
                cid,
                str(order["orderId"]),
                "OPEN",
                "openOrders"
            )
            changed = True
            continue

        algo = tp_by_parent_cid.get(cid)

        # Do NOT use algo.actualOrderId as the opening BUY parent.
        # In WEEX V3 it identifies the order generated after TP trigger.
        # We still remember that the TP exists, but recover the true BUY
        # orderId from normal order history below.
        if algo is not None:
            _private_ws_diag(
                "PENDING_HAS_ATTACHED_TP",
                {
                    "clientOrderId": cid,
                    "clientAlgoId": algo.get("clientAlgoId"),
                    "algoStatus": algo.get("algoStatus")
                }
            )

        need_history.append((cid, pending))

    if not need_history:
        return changed

    oldest = min(
        int(p.get("created_ms", 0) or 0)
        for _, p in need_history
    )

    history = await asyncio.to_thread(
        fetch_recent_order_history,
        oldest
    )

    history_by_cid = {
        str(order.get("clientOrderId", "")): order
        for order in history
        if str(order.get("clientOrderId", "")).startswith(PREFIX)
    }

    algo_history = None

    for cid, pending in need_history:

        order = history_by_cid.get(cid)

        if order is None:

            remove_now = False

            async with state_lock:
                current = STATE.get("pending_submits", {}).get(cid)
                if not current:
                    continue
                current["absence_checks"] = int(
                    current.get("absence_checks", 0)
                ) + 1
                remove_now = (
                    current["absence_checks"]
                    >= PENDING_SUBMIT_ABSENCE_CONFIRMATIONS
                )
                save_state()

            if remove_now:
                old_rearm = await _clear_pending_as_absent(
                    cid,
                    f"{PENDING_SUBMIT_ABSENCE_CONFIRMATIONS} успешные проверки"
                )
                if old_rearm:
                    queue_rearm(
                        str(old_rearm)
                    )
                changed = True

            continue

        status = str(order.get("status", "")).upper()
        parent_id = order.get("orderId")

        if status in ("OPEN", "NEW", "PARTIALLY_FILLED") and parent_id is not None:
            await _adopt_pending_order(
                cid,
                str(parent_id),
                "OPEN",
                "order/history"
            )
            changed = True
            continue

        if status in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
            old_rearm = await _clear_pending_as_absent(
                cid,
                f"history status={status}"
            )
            if old_rearm:
                queue_rearm(
                    str(old_rearm)
                )
            changed = True
            continue

        if status == "FILLED" and parent_id is not None:

            if algo_history is None:
                algo_history = await asyncio.to_thread(
                    fetch_recent_algo_history,
                    oldest
                )

            matching_algo = None

            for algo in algo_history:
                algo_cid = str(algo.get("clientAlgoId", ""))
                if algo_cid == cid + "tp" or algo_cid == cid:
                    matching_algo = algo
                    break

            if matching_algo is None:
                continue

            algo_status = str(
                matching_algo.get("algoStatus", "")
            ).upper()

            if (
                algo_status in ("FILLED", "TRIGGERED")
                or int(matching_algo.get("triggerTime", 0) or 0) > 0
            ):
                await _adopt_pending_order(
                    cid,
                    str(parent_id),
                    "DONE_OFFLINE",
                    "order+TP history"
                )
                queue_rearm(
                    str(parent_id)
                )
                changed = True
                continue

    return changed


# =========================================================
# REST SAFETY-CHECK
#
# WebSocket остаётся главным и мгновенным каналом.
# Каждые 3 сек REST сверяет только живые open orders + active TP.
# Если WS пропустил BUY fill, восстанавливаем WAIT_TP напрямую.
# Если исчез TP / найдено другое расхождение, запускаем полный reconcile.
# =========================================================


def _remote_bot_maps(
    open_orders,
    open_algos,
    parent_by_client_id,
    known_parent_ids
):

    open_map = {}
    tp_map = {}

    for order in open_orders:
        cid = str(order.get("clientOrderId", ""))
        if not cid.startswith(PREFIX):
            continue
        order_id = order.get("orderId")
        if order_id is not None:
            open_map[str(order_id)] = order

    for algo in open_algos:
        cid = str(algo.get("clientAlgoId", ""))
        if not cid.startswith(PREFIX):
            continue
        if "TAKE_PROFIT" not in str(algo.get("orderType", "")):
            continue
        parent = _algo_parent_from_maps(
            algo,
            parent_by_client_id,
            known_parent_ids
        )

        if parent is not None:
            tp_map[str(parent)] = algo

    return open_map, tp_map


async def _state_sets():

    async with state_lock:
        local_open = {
            order_id
            for order_id, info in STATE["parents"].items()
            if info["status"] == "OPEN"
        }
        local_wait = {
            order_id
            for order_id, info in STATE["parents"].items()
            if info["status"] == "WAIT_TP"
        }
        known = set(STATE["parents"].keys())

    return local_open, local_wait, known


async def _fetch_safety_maps():

    open_orders = await asyncio.to_thread(
        fetch_all_open_orders
    )
    open_algos = await asyncio.to_thread(
        fetch_all_open_algos
    )

    (
        parent_by_client_id,
        known_parent_ids
    ) = await _tp_parent_maps_snapshot()

    open_map, tp_map = _remote_bot_maps(
        open_orders,
        open_algos,
        parent_by_client_id,
        known_parent_ids
    )

    return open_map, tp_map, open_orders, open_algos


async def safety_watcher():

    last_error_print = 0.0

    while True:

        await asyncio.sleep(
            SAFETY_POLL_INTERVAL
        )

        if (
            not ws_ready.is_set()
            or not STATE_SYNCED
            or reconcile_lock.locked()
        ):
            continue

        try:
            open_map, tp_map, open_orders, open_algos = await _fetch_safety_maps()
        except Exception as error:
            if time.monotonic() - last_error_print >= 30:
                event_print(
                    f"⚠️ REST safety-check: {error}"
                )
                last_error_print = time.monotonic()
            continue

        try:
            pending_changed = await resolve_pending_submits(
                open_orders,
                open_algos
            )
        except Exception as error:
            pending_changed = False
            if time.monotonic() - last_error_print >= 30:
                event_print(
                    f"⚠️ Pending-submit check: {error}"
                )
                last_error_print = time.monotonic()

        if pending_changed:
            await maintain_active_limits(
                "после восстановления pending POST"
            )

        local_open, local_wait, known = await _state_sets()
        remote_open = set(open_map.keys())
        remote_tp = set(tp_map.keys())

        # Самый частый и безопасно определяемый случай:
        # локально BUY ещё OPEN, на бирже его уже нет среди лимиток,
        # зато появился attached TP => BUY точно исполнился.
        missed_buy_fills = (
            local_open
            - remote_open
        ) & remote_tp

        if missed_buy_fills:

            recovered_any = False

            for parent_id in sorted(missed_buy_fills):
                changed = await _mark_buy_filled(
                    parent_id,
                    "REST safety"
                )
                recovered_any = recovered_any or changed

            if recovered_any:
                request_private_ws_reconnect(
                    "REST safety поймал пропущенный BUY fill "
                    f"({len(missed_buy_fills)} шт.)"
                )
                await maintain_active_limits(
                    "REST-страховка после пропущенного BUY fill"
                )

            # After new BUYs are placed, this REST snapshot is stale.
            continue
        unexplained_open = (
            local_open
            - remote_open
            - remote_tp
        )

        disappeared_tp = (
            local_wait
            - remote_tp
        )

        unknown_remote = (
            remote_open | remote_tp
        ) - known

        if not (
            unexplained_open
            or disappeared_tp
            or unknown_remote
        ):
            # Даже когда REST не видит рассинхронизации state/WEEX,
            # отдельно проверяем геометрию рабочей сетки: 10 правильных
            # уровней без дырок возле текущего BID.
            await maintain_active_limits(
                "REST safety-check grid integrity"
            )
            continue

        # Защита от краткой задержки API: подтверждаем расхождение повторно.
        await asyncio.sleep(0.75)

        try:
            open_map2, tp_map2, _, _ = await _fetch_safety_maps()
        except Exception:
            continue

        local_open2, local_wait2, known2 = await _state_sets()
        remote_open2 = set(open_map2.keys())
        remote_tp2 = set(tp_map2.keys())

        missed_buy_fills2 = (
            local_open2 - remote_open2
        ) & remote_tp2

        if missed_buy_fills2:
            recovered_any2 = False

            for parent_id in sorted(missed_buy_fills2):
                changed = await _mark_buy_filled(
                    parent_id,
                    "REST safety recheck"
                )
                recovered_any2 = recovered_any2 or changed

            if recovered_any2:
                request_private_ws_reconnect(
                    "REST recheck поймал пропущенный BUY fill "
                    f"({len(missed_buy_fills2)} шт.)"
                )
                await maintain_active_limits(
                    "REST recheck после пропущенного BUY fill"
                )

            continue

        still_mismatch = (
            (local_open2 - remote_open2 - remote_tp2)
            or (local_wait2 - remote_tp2)
            or ((remote_open2 | remote_tp2) - known2)
        )

        if not still_mismatch:
            continue

        event_print(
            "🛟 REST safety-check нашёл устойчивое расхождение — "
            "запускаю полную сверку WEEX"
        )

        async with maintenance_lock:
            ok = await reconcile(
                "REST safety-check после пропущенного WebSocket события"
            )

        if ok:
            await maintain_active_limits(
                "после REST safety reconcile"
            )


# =========================================================
# BID — PUBLIC WEBSOCKET PRIMARY + REST FALLBACK
#
# WEEX contract V3 public depth15 provides real-time order-book updates.
# The receive loop does no trading REST work: it only updates LAST_BID and
# sets an event. A separate strategy consumer reacts to the newest BID.
# REST bookTicker is used only if the public stream is stale/unavailable.
# =========================================================


async def _send_public_ws_pong(ws):

    await ws.send(
        json.dumps({
            "method": "PONG",
            "id": 2
        })
    )


def _apply_depth_message_to_book(message, bid_book, initialized, last_update_id):

    if message.get("e") != "depth":
        return None, initialized, last_update_id

    if str(message.get("s", "")).upper() != SYMBOL:
        return None, initialized, last_update_id

    first_id = message.get("U")
    final_id = message.get("u")

    try:
        first_id = int(first_id) if first_id is not None else None
        final_id = int(final_id) if final_id is not None else None
    except Exception:
        first_id = None
        final_id = None

    if (
        initialized
        and last_update_id is not None
        and first_id is not None
        and first_id > last_update_id + 1
    ):
        raise RuntimeError(
            f"public depth gap: last={last_update_id}, next={first_id}"
        )

    depth_type = str(message.get("d", "")).upper()
    bids = message.get("b", []) or []

    if depth_type == "SNAPSHOT":
        bid_book.clear()
        initialized = True

    elif not initialized:
        # Contract docs instruct resubscription to obtain a fresh depth view.
        # Some deployments label the first multi-level image CHANGED.
        # Accept only a multi-level first image; never infer BID from one
        # arbitrary changed level.
        if len(bids) >= 3:
            bid_book.clear()
            initialized = True
        else:
            return None, False, final_id or last_update_id

    for row in bids:

        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue

        try:
            price = D(row[0])
            size = Decimal(str(row[1]))
        except Exception:
            continue

        if size <= 0:
            bid_book.pop(price, None)
        else:
            bid_book[price] = size

    if final_id is not None:
        last_update_id = final_id

    if not bid_book:
        return None, initialized, last_update_id

    return max(bid_book.keys()), initialized, last_update_id


async def public_bid_watcher():

    last_error_print = 0.0

    while True:

        bid_book = {}
        initialized = False
        last_update_id = None

        try:

            async with websockets.connect(
                PUBLIC_WS,
                user_agent_header=UA,
                ping_interval=None,
                open_timeout=10
            ) as ws:

                await ws.send(
                    json.dumps({
                        "method": "SUBSCRIBE",
                        "params": [f"{SYMBOL}@depth15"],
                        "id": 2
                    })
                )

                ack_deadline = time.monotonic() + 8
                buffered = []

                while True:

                    remaining = ack_deadline - time.monotonic()

                    if remaining <= 0:
                        raise RuntimeError(
                            "WEEX не подтвердил public depth15 за 8 сек"
                        )

                    raw = await asyncio.wait_for(
                        ws.recv(),
                        timeout=remaining
                    )
                    message = json.loads(raw)

                    if (
                        message.get("type") == "ping"
                        or message.get("event") == "ping"
                    ):
                        await _send_public_ws_pong(ws)
                        continue

                    if message.get("id") == 2 and "result" in message:
                        if message.get("result") is True:
                            break
                        raise RuntimeError(
                            "WEEX отклонил public depth15: "
                            f"{message.get('msg', message)}"
                        )

                    if message.get("e") == "depth":
                        buffered.append(message)

                event_print(
                    "📡 Public WS BID: depth15 подписка подтверждена"
                )

                for message in buffered:
                    bid, initialized, last_update_id = (
                        _apply_depth_message_to_book(
                            message,
                            bid_book,
                            initialized,
                            last_update_id
                        )
                    )
                    if bid is not None:
                        _record_bid(bid, "PUBLIC_WS")
                        public_bid_ready.set()

                async for raw in ws:

                    message = json.loads(raw)

                    if (
                        message.get("type") == "ping"
                        or message.get("event") == "ping"
                    ):
                        await _send_public_ws_pong(ws)
                        continue

                    bid, initialized, last_update_id = (
                        _apply_depth_message_to_book(
                            message,
                            bid_book,
                            initialized,
                            last_update_id
                        )
                    )

                    if bid is not None:
                        _record_bid(bid, "PUBLIC_WS")
                        public_bid_ready.set()

        except asyncio.CancelledError:
            raise

        except Exception as error:

            public_bid_ready.clear()

            if time.monotonic() - last_error_print >= 30:
                event_print(
                    f"⚠️ Public WS BID отключён: {error}. "
                    "REST fallback подстрахует цену."
                )
                last_error_print = time.monotonic()

            await asyncio.sleep(
                PUBLIC_WS_RECONNECT_DELAY
            )


async def price_strategy_watcher():

    last_target = None

    while True:

        await bid_update_event.wait()
        bid_update_event.clear()

        bid = LAST_BID

        if bid is None:
            continue

        target = min(
            grid_below_bid(bid),
            MAX_BUY_PRICE
        )

        # Do not consume a target while trading state is paused/unsynced.
        # Otherwise startup could remember the current boundary before
        # reconciliation and never process it after STATE_SYNCED becomes true.
        if PAUSED or not STATE_SYNCED:
            continue

        # Grid geometry changes only when BID crosses a 0.00010 boundary.
        # Do not run heavy strategy/REST work for every tiny book tick.
        if target == last_target:
            continue

        last_target = target

        await shift_up(
            target
        )

        await maintain_active_limits(
            f"BID {P(bid)} ({LAST_BID_SOURCE})"
        )


async def rest_bid_fallback_watcher():

    last_error_print = 0.0
    fallback_announced = False

    while True:

        await asyncio.sleep(
            REST_BID_FALLBACK_INTERVAL
        )

        age = (
            time.monotonic() - LAST_BID_UPDATE_MONO
            if LAST_BID_UPDATE_MONO > 0
            else 10**9
        )

        if age <= PUBLIC_BID_STALE_SECONDS:
            fallback_announced = False
            continue

        try:
            bid = await asyncio.to_thread(
                get_bid
            )
        except Exception as error:
            if time.monotonic() - last_error_print >= 30:
                event_print(
                    f"⚠️ REST BID fallback: {error}"
                )
                last_error_print = time.monotonic()
            continue

        _record_bid(
            bid,
            "REST_FALLBACK"
        )

        if not fallback_announced:
            event_print(
                f"🛟 Public BID stale → REST fallback BID {P(bid)}"
            )
            fallback_announced = True


# =========================================================
# STATUS + RETRY
#
# - NEEDS_REARM проверяем каждые 30 сек, как и раньше.
# - краткий статус печатаем только раз в 60 сек.
# =========================================================

async def status_watcher():

    last_status_print = time.monotonic()

    while True:

        await asyncio.sleep(
            STATUS_RETRY_INTERVAL
        )

        async with state_lock:

            limits = sum(

                1

                for order
                in STATE["parents"].values()

                if order["status"]
                == "OPEN"
            )

            waiting = sum(

                1

                for order
                in STATE["parents"].values()

                if order["status"]
                == "WAIT_TP"
            )

            needs = [

                order_id

                for order_id, order
                in STATE["parents"].items()

                if order["status"]
                in (
                    "NEEDS_REARM",
                    "DONE_OFFLINE"
                )
            ]

            top = STATE.get(
                "top"
            )

            pending_count = len(
                STATE.get("pending_submits", {})
            )


        if (
            time.monotonic()
            - last_status_print
            >= STATUS_PRINT_INTERVAL
        ):

            bid = LAST_BID

            bid_text = (
                P(bid)
                if bid is not None
                else "?"
            )

            if not STATE_SYNCED:

                event_print(
                    f"📈 BID {bid_text} "
                    "| STATE НЕ СИНХРОНИЗИРОВАН "
                    "| ⏸ PAUSE"
                )

            else:

                event_print(
                    f"📈 BID {bid_text} "
                    f"| верх {top} "
                    f"| BUY LIMIT {limits} "
                    f"| ждут TP {waiting} "
                    f"| grid {tracked_grid_orders()}/{MAX_GRID_ORDERS}"
                    + (
                        f" | pending {pending_count}"
                        if pending_count
                        else ""
                    )
                    + (
                        " | ⏸ PAUSE"
                        if PAUSED
                        else ""
                    )
                )

            last_status_print = (
                time.monotonic()
            )


        # Это не связано с частотой печати.
        # Повтор rearm сохраняем с прежней частотой.
        if not PAUSED and STATE_SYNCED:

            for parent_id in needs:

                queue_rearm(
                    parent_id
                )

            # Rearm всегда приоритетнее обычного поддержания 20 активных BUY.
            # Если rearm сейчас не требуется, подправляем число OPEN BUY.
            if not needs:
                await maintain_active_limits(
                    "плановая проверка рабочей сетки"
                )


# =========================================================
# ПОЛНАЯ СТАТИСТИКА РАЗ В 10 МИНУТ
# =========================================================

async def stats_watcher():

    while True:

        await asyncio.sleep(
            STATS_INTERVAL
        )

        await refresh_stats(
            show=True,
            reason="10 минут"
        )


# =========================================================
# ПЛАНОВАЯ ПОЛНАЯ СВЕРКА С WEEX РАЗ В 30 МИНУТ
# Дополнительные reconcile на reconnect остаются как в bot02.
# =========================================================

async def reconcile_watcher():

    global PAUSED

    while True:

        await asyncio.sleep(
            RECONCILE_INTERVAL
            if STATE_SYNCED
            else STATUS_RETRY_INTERVAL
        )

        async with maintenance_lock:

            ok = await reconcile(
                "плановая проверка 30 минут"
                if STATE_SYNCED
                else "повтор после ошибки синхронизации"
            )

        if ok:

            if private_ws_ready.is_set():

                async with state_lock:
                    have_cycles_after_sync = bool(
                        STATE["parents"]
                    )

                if have_cycles_after_sync:
                    await maintain_active_limits(
                        "после плановой сверки WEEX"
                    )

                # ws_ready is owned exclusively by order_watcher.
            else:
                PAUSED = True
                ws_ready.clear()

        else:
            ws_ready.clear()


# =========================================================
# ЗАЩИТА ОТ ВТОРОГО ЭКЗЕМПЛЯРА BOT06
# =========================================================
# СТАРТОВАЯ СЕТКА
# Используется только если на WEEX и в state
# вообще нет наших старых циклов.
# =========================================================

async def seed_grid(
    top
):

    top = min(
        D(top),
        MAX_BUY_PRICE
    )

    # Пока ставятся первые 20, никакой параллельный replenish/rearm
    # не должен вмешаться в стартовые 20 уровней.
    async with strategy_lock:

        async with state_lock:

            STATE["top"] = P(
                top
            )

            save_state()


        event_print(
            f"Выставляю {ACTIVE_LIMITS_TARGET} стартовых BUY LIMIT..."
        )


        for i in range(
            ACTIVE_LIMITS_TARGET
        ):

            level = (
                top
                - STEP * i
            )

            await place_entry(

                level,

                f"старт "
                f"{i + 1}/"
                f"{ACTIVE_LIMITS_TARGET}"
            )


        event_print(
            "✅ Стартовые активные лимитки выставлены"
        )


# =========================================================
# MAIN
# =========================================================

async def main():

    global LAST_BID

    # Start public market-data stream before any trading logic. On a good
    # connection this removes the old REST BID poll entirely.
    public_bid_task = asyncio.create_task(
        public_bid_watcher()
    )

    try:
        await asyncio.wait_for(
            public_bid_ready.wait(),
            timeout=5
        )
        bid = LAST_BID
    except asyncio.TimeoutError:
        # Public WS did not produce a usable depth snapshot quickly enough.
        # One REST snapshot is safe as startup fallback.
        bid = await asyncio.to_thread(
            get_bid
        )
        _record_bid(
            bid,
            "REST_STARTUP"
        )

    if bid is None:
        raise RuntimeError(
            "Не удалось получить стартовый BID ни через WS, ни через REST"
        )

    try:
        offset = await asyncio.to_thread(
            sync_server_time
        )
        event_print(
            f"🕒 Время WEEX синхронизировано ({offset:+d} ms)"
        )
    except Exception as error:
        event_print(
            "⚠️ Не удалось заранее сверить время WEEX: "
            f"{error}"
        )
        event_print(
            "Продолжаю; при timestamp expired бот попробует "
            "пересинхронизироваться автоматически."
        )

    state_file_exists = STATE_FILE.exists()
    existing = state_file_exists


    print()
    print(
        f"WEEX STRK GRID BOT v{VERSION}"
    )

    print(
        f"Python: {sys.version.split()[0]} | websockets: "
        f"{getattr(websockets, '__version__', '?')}"
    )

    print()

    print(
        f"SYMBOL: {SYMBOL}"
    )
    print(
        f"BID: {P(bid)}"
    )
    print()
    print(
        f"STEP: {P(STEP)} | TP: +{P(TP_DISTANCE)} | QTY: {QTY:.0f} STRK"
    )
    print(
        f"ACTIVE BUY LIMITS: {ACTIVE_LIMITS_TARGET}"
    )
    print(
        f"MAX BUY PRICE: {P(MAX_BUY_PRICE)}"
    )
    print(
        f"MAX GRID: {MAX_GRID_ORDERS} | MAX STRK: {MAX_STRK:.0f}"
    )


    if existing:

        async with state_lock:

            limits = sum(

                1

                for order
                in STATE["parents"].values()

                if order["status"]
                == "OPEN"
            )

            waiting = sum(

                1

                for order
                in STATE["parents"].values()

                if order["status"]
                == "WAIT_TP"
            )

            top = STATE.get(
                "top"
            )


        print(
            f"{STATE_FILE.name} найден."
        )

        print(
            f"Лимиток в state: "
            f"{limits}"
        )

        print(
            f"Ждут TP в state: "
            f"{waiting}"
        )

        print(
            f"Верх в state: "
            f"{top}"
        )

        print()

        print(
            "RESUME06 не создаёт новую стартовую сетку."
        )

        print(
            "Сначала bot06 сверит "
            "state06.json с WEEX."
        )

        print()

        if AUTO_RESUME:

            confirm = "RESUME06"

            event_print(
                "🤖 AUTO_RESUME=1 → RESUME06 подтверждён автоматически."
            )

        else:

            confirm = input(
                "Напиши RESUME06: "
            ).strip()

        if confirm != "RESUME06":

            print(
                "Ничего не запущено."
            )

            return


    else:

        top = min(
            grid_below_bid(bid),
            MAX_BUY_PRICE
        )

        bottom = (
            top
            - STEP
            * (ACTIVE_LIMITS_TARGET - 1)
        )

        print(
            "state06.json нет — старт bot06 с чистого состояния."
        )

        print(
            f"Верх новой сетки: "
            f"{P(top)}"
        )

        print(
            f"Низ стартовых {ACTIVE_LIMITS_TARGET} лимиток: "
            f"{P(bottom)}"
        )

        print()

        if AUTO_RESUME:

            raise RuntimeError(
                "AUTO_RESUME=1, но state06.json отсутствует. "
                "Автоматический START06 ЗАПРЕЩЁН. "
                "Первый запуск сделай вручную с AUTO_RESUME=0."
            )

        confirm = input(
            "Напиши START06: "
        ).strip()

        if confirm != "START06":

            print(
                "Ничего не выставлено."
            )

            return


    watcher = asyncio.create_task(
        order_watcher()
    )


    # Ждём подключения WebSocket
    # и полной синхронизации с WEEX.

    await ws_ready.wait()


    # Даже если state06.json отсутствовал,
    # reconcile ищет только PREFIX strk6-.
    # Старые clg- ордера bot01–03 полностью игнорируются.

    async with state_lock:

        have_cycles = bool(
            STATE["parents"]
        )


    if not have_cycles:

        if AUTO_RESUME:
            raise RuntimeError(
                "AUTO_RESUME отказался создавать новую сетку: "
                "после reconcile не найдено существующих циклов bot06. "
                "Проверь WEEX/state и выполни ручной START06 при необходимости."
            )

        fresh_bid = LAST_BID

        if fresh_bid is None:
            fresh_bid = await asyncio.to_thread(
                get_bid
            )
            _record_bid(
                fresh_bid,
                "REST_STARTUP"
            )

        fresh_top = min(
            grid_below_bid(fresh_bid),
            MAX_BUY_PRICE
        )

        await seed_grid(
            fresh_top
        )

    # После старта/RESUME гарантируем целевые 20 активных лимиток.
    # Если лимит сетки 351 уже достигнут, функция ничего лишнего не добавит.
    await maintain_active_limits(
        "стартовая проверка рабочей сетки"
    )


    print()
    event_print(
        "🤖 bot06 работает."
    )

    event_print(
        "Диагностика private WS: logs/private_ws_raw06.log"
    )

    event_print(
        "Остановить: Ctrl+C"
    )

    print()


    # После стартовой reconciliation сразу показываем
    # полную статистику. Ошибка здесь торговлю не остановит.
    await refresh_stats(
        show=True,
        reason="старт"
    )


    await asyncio.gather(

        watcher,

        public_bid_task,

        price_strategy_watcher(),

        rest_bid_fallback_watcher(),

        safety_watcher(),

        status_watcher(),

        stats_watcher(),

        reconcile_watcher()
    )


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        event_print(
            "Бот остановлен."
        )

    except Exception as error:

        print()
        event_print(
            f"Бот аварийно остановлен: "
            f"{error}"
        )

        _telegram_alert_sync(
            f"💥 Аварийная остановка: {error}",
            key="fatal_crash",
            cooldown=0
        )

        sys.exit(1)
