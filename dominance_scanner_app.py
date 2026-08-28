import copy
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("dominance-scanner")

SHEETS_WEB_APP_URL = os.getenv("SHEETS_WEB_APP_URL", "").strip()
SHEETS_SECRET = os.getenv("SHEETS_SECRET", "").strip()
TV_WEBHOOK_TOKEN = os.getenv("TV_WEBHOOK_TOKEN", "").strip()

ALLOWED_SCANNERS = {
    "DOMINANCE_RS_M5",
    "DOMINANCE_RS_M5_V2_EARLY",
    "DOMINANCE_RS_M5_V3_STATE_MACHINE",
}
ALLOWED_BIASES = {"LONG_ONLY", "SHORT_ONLY", "NO_TRADE"}
ALLOWED_STATES = {"UP", "DOWN", "FLAT"}

executor = ThreadPoolExecutor(max_workers=2)
state_lock = threading.Lock()

last_snapshot = None
last_forward_status = {
    "ok": None,
    "message": "No snapshot received yet",
    "updated_at": None,
}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def validate_payload(payload):
    if not isinstance(payload, dict):
        return False, "Payload must be a JSON object."

    scanner = payload.get("scanner")
    if scanner not in ALLOWED_SCANNERS:
        return False, f"scanner must be one of {sorted(ALLOWED_SCANNERS)}"

    timestamp = payload.get("timestamp")
    try:
        timestamp = int(timestamp)
    except (TypeError, ValueError):
        return False, "timestamp must be epoch milliseconds."

    if timestamp <= 0:
        return False, "timestamp must be positive."

    signals = payload.get("signals")
    if not isinstance(signals, list) or not signals:
        return False, "signals must be a non-empty list."

    seen = set()

    for idx, signal in enumerate(signals):
        if not isinstance(signal, dict):
            return False, f"signals[{idx}] must be an object."

        ticker = str(signal.get("ticker", "")).strip().upper()
        if not ticker:
            return False, f"signals[{idx}].ticker is required."

        if ticker in seen:
            return False, f"Duplicate ticker: {ticker}"
        seen.add(ticker)

        bias = signal.get("bias")
        if bias not in ALLOWED_BIASES:
            return False, f"{ticker}: invalid bias {bias}"

        try:
            int(signal.get("score"))
        except (TypeError, ValueError):
            return False, f"{ticker}: score must be an integer."

        for field in (
            "dominance_m5",
            "dominance_m15",
            "dominance_h1",
            "price_m5",
            "price_m15",
            "rs_btc_m5",
            "rs_btc_m15",
        ):
            value = signal.get(field)
            if value not in ALLOWED_STATES:
                return False, f"{ticker}: {field} must be UP, DOWN or FLAT."

    return True, "ok"


def forward_to_sheets(payload):
    global last_forward_status

    if not SHEETS_WEB_APP_URL:
        msg = "SHEETS_WEB_APP_URL is not configured."
        log.error(msg)
        with state_lock:
            last_forward_status = {
                "ok": False,
                "message": msg,
                "updated_at": utc_now_iso(),
            }
        return

    if not SHEETS_SECRET:
        msg = "SHEETS_SECRET is not configured."
        log.error(msg)
        with state_lock:
            last_forward_status = {
                "ok": False,
                "message": msg,
                "updated_at": utc_now_iso(),
            }
        return

    outgoing = copy.deepcopy(payload)
    outgoing["secret"] = SHEETS_SECRET
    outgoing["render_received_at"] = utc_now_iso()

    error = None

    for attempt in range(1, 3):
        try:
            response = requests.post(
                SHEETS_WEB_APP_URL,
                json=outgoing,
                timeout=(3.0, 12.0),
                allow_redirects=True,
            )

            body = response.text[:2000]

            if 200 <= response.status_code < 300:
                # Apps Script ContentService commonly returns HTTP 200 even when
                # our doPost() reports an application-level error in JSON.
                app_ok = None
                app_message = body

                try:
                    parsed = response.json()
                    app_ok = parsed.get("ok")
                    if app_ok is False:
                        app_message = parsed.get("error") or body
                except ValueError:
                    parsed = None

                if app_ok is False:
                    error = (
                        f"Sheets application error despite HTTP "
                        f"{response.status_code}: {app_message}"
                    )
                    log.warning("SHEETS_APP_ERROR attempt=%s %s", attempt, error)
                else:
                    log.info(
                        "SHEETS_OK status=%s attempt=%s body=%s",
                        response.status_code,
                        attempt,
                        body,
                    )
                    with state_lock:
                        last_forward_status = {
                            "ok": True,
                            "message": (
                                f"Sheets accepted snapshot. "
                                f"HTTP {response.status_code}"
                            ),
                            "updated_at": utc_now_iso(),
                        }
                    return
            else:
                error = f"Sheets HTTP {response.status_code}: {body}"
                log.warning("SHEETS_REJECTED attempt=%s %s", attempt, error)

        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
            log.warning("SHEETS_ERROR attempt=%s %s", attempt, error)

    with state_lock:
        last_forward_status = {
            "ok": False,
            "message": error or "Unknown forwarding error.",
            "updated_at": utc_now_iso(),
        }


@app.get("/")
def index():
    return jsonify(
        {
            "service": "dominance-scanner",
            "status": "ok",
            "allowed_scanners": sorted(ALLOWED_SCANNERS),
            "sheets_configured": bool(SHEETS_WEB_APP_URL and SHEETS_SECRET),
        }
    )


@app.get("/health")
def health():
    with state_lock:
        forward_status = dict(last_forward_status)
        snapshot_time = None if last_snapshot is None else last_snapshot.get("timestamp")

    return jsonify(
        {
            "ok": True,
            "service": "dominance-scanner",
            "sheets_configured": bool(SHEETS_WEB_APP_URL and SHEETS_SECRET),
            "last_snapshot_timestamp": snapshot_time,
            "last_forward_status": forward_status,
        }
    )


@app.post("/webhook")
def tradingview_webhook():
    global last_snapshot

    if TV_WEBHOOK_TOKEN:
        supplied = request.args.get("token", "")
        if supplied != TV_WEBHOOK_TOKEN:
            log.warning("Rejected webhook: invalid TV token.")
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True)

    valid, message = validate_payload(payload)
    if not valid:
        log.warning("Rejected payload: %s", message)
        return jsonify({"ok": False, "error": message}), 400

    clean_payload = copy.deepcopy(payload)

    with state_lock:
        last_snapshot = clean_payload

    log.info(
        "TV_SNAPSHOT timestamp=%s signals=%s",
        clean_payload.get("timestamp"),
        len(clean_payload.get("signals", [])),
    )

    executor.submit(forward_to_sheets, clean_payload)

    return jsonify(
        {
            "ok": True,
            "accepted": True,
            "timestamp": clean_payload.get("timestamp"),
            "signals": len(clean_payload.get("signals", [])),
        }
    ), 202


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
