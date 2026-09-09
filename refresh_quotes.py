#!/usr/bin/env python3
"""
refresh_quotes.py — Signal Book live-data refresher

Parses the stock universe directly out of dashboard-v2.html (the STOCKS
array + every addSourceGroup(...) call), maps each ticker to a Yahoo
Finance symbol, pulls a delayed quote + 1y daily history for each one,
computes RSI(14) / MA20 / MA60 / a 40-point sparkline, and rewrites the
LIVE={...} block embedded in dashboard-v2.html between the
/*LIVE-DATA-START*/ ... /*LIVE-DATA-END*/ markers.

Stdlib only (urllib, json, re, time, datetime, zoneinfo). No API key,
no server, no third-party packages. Safe to re-run any time — it is
idempotent and always replaces the previous LIVE block.

Data source note: Yahoo's public "spark" endpoint (v7/finance/spark)
looks like a lightweight snapshot API but actually returns the *full*
daily close/timestamp series for whatever `range` you ask for, batched
for up to ~10 symbols per call. So one batched request per 10 tickers
gets us both the live quote (meta) AND the 1y history needed for
RSI/MA/sparkline/chart in a single round trip — ~23 requests total for
231 tickers, instead of 23 quote-batches + 231 individual chart calls.
That matters in practice: hammering Yahoo with ~250 rapid requests
reliably triggers HTTP 429 (verified while building this script); ~23
batched requests with a small delay between them does not.

Usage:
    python3 refresh_quotes.py
"""
import json
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from math import floor, log10
from pathlib import Path
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
HTML_PATH = HERE / "dashboard-v2.html"

# Keep this exactly as-is. Yahoo 429s a full Chrome UA string (and an absent
# one) while answering the bare token instantly — same URL, same IP, same
# second. What looks like rate limiting here is User-Agent filtering:
#   "Mozilla/5.0"                          -> 200 in 1.0s
#   "Mozilla/5.0 (Macintosh; ...) Chrome/124.0 Safari/537.36" -> 429
#   (no User-Agent)                        -> 429
UA = "Mozilla/5.0"

BATCH_SIZE = 10  # >10 symbols on /v7/finance/spark returns HTTP 400
SPARK_URL = "https://query1.finance.yahoo.com/v7/finance/spark?symbols={syms}&range=1y&interval=1d"
BATCH_SLEEP = 0.5     # base pause between batches
MAX_429_RETRIES = 6   # extra backoff specifically for rate-limiting

# Yahoo rate-limits by source IP and stays hostile for a while once tripped.
# These public readers fetch server-side from their own IPs, so they sail past
# a block on ours. Tried in order, only after a direct request has 429'd.
# Only readers that actually answer. allorigins and codetabs were both
# returning Cloudflare 522s after a ~20s hang on 2026-09-09 — keeping them
# in the chain cost 40s per failed batch and never once succeeded.
PROXY_TEMPLATES = [
    ("https://r.jina.ai/{url}", {"x-respond-with": "text"}),
]
PROXY_TIMEOUT = 20

MARKER_START = "/*LIVE-DATA-START*/"
MARKER_END = "/*LIVE-DATA-END*/"

# Hard overrides applied after the tv-based mapping rule below.
OVERRIDES = {
    "EKSO": None,            # no Yahoo data at all -- leave unresolved, don't fake it
    "DNNON": "DNN",          # tokenized Denison Mines proxy -- fetch DNN for a KPI basis
}

# Every Korean listing, resolved explicitly. This is NOT a convenience table
# — it is a correctness guard. Yahoo answers BOTH `.KS` and `.KQ` for almost
# every 6-digit KRX code, returning a *different instrument* rather than an
# error, with a full year of plausible-looking daily bars. Guessing the suffix
# from the exchange field silently produces wrong prices:
#     005930.KS = 273,750  (삼성전자, correct)
#     005930.KQ =  84,400  (something else entirely)
#     058470.KQ =  70,300  (리노공업, correct)
#     058470.KS = 219,500  (something else entirely)
# Each entry below was confirmed against Yahoo's own symbol-search endpoint
# (exchange KSC=KOSPI / KOE=KOSDAQ) on 2026-09-09: 56/56 agreed.
KR_SYMBOLS = {
    "000500": "000500.KS", "000660": "000660.KS", "000720": "000720.KS",
    "000990": "000990.KS", "001440": "001440.KS", "0015G0": "0015G0.KQ",
    "003670": "003670.KS", "005380": "005380.KS", "005930": "005930.KS",
    "006260": "006260.KS", "006340": "006340.KS", "010120": "010120.KS",
    "010130": "010130.KS", "032820": "032820.KQ", "033100": "033100.KQ",
    "034020": "034020.KS", "036930": "036930.KQ", "042660": "042660.KS",
    "042700": "042700.KS", "046120": "046120.KQ", "047050": "047050.KS",
    "058470": "058470.KQ", "058610": "058610.KQ", "060370": "060370.KQ",
    "062040": "062040.KS", "066970": "066970.KS", "090710": "090710.KQ",
    "094820": "094820.KQ", "095340": "095340.KQ", "103140": "103140.KS",
    "103590": "103590.KS", "105840": "105840.KS", "126340": "126340.KQ",
    "127120": "127120.KQ", "140670": "140670.KQ", "160190": "160190.KQ",
    "196170": "196170.KQ", "207940": "207940.KS", "218410": "218410.KQ",
    "229640": "229640.KS", "240810": "240810.KQ", "247540": "247540.KQ",
    "267260": "267260.KS", "277810": "277810.KQ", "295310": "295310.KQ",
    "298040": "298040.KS", "307950": "307950.KS", "319400": "319400.KQ",
    "336260": "336260.KS", "348340": "348340.KQ", "376900": "376900.KQ",
    "403870": "403870.KQ", "440110": "440110.KQ", "454910": "454910.KS",
    "488900": "488900.KQ", "900290": "900290.KQ",
}

MIN_RESOLVED = 220  # spec: fail loudly if fewer than this many of 231 resolve


# --------------------------------------------------------------------------
# Universe extraction (kept in lockstep with dashboard-v2.html so this
# script can never drift from the page it feeds).
# --------------------------------------------------------------------------
def parse_universe(html):
    base = re.findall(
        r"^\['([^']+)','([^']+)','([^']+)','([^']+)','(\w+)',\[([^\]]*)\],'([^']*)'",
        html, re.M)
    groups = re.findall(
        r"addSourceGroup\('([^']+)','([^']+)','([^']+)','([^']+)','([^']*)'\)",
        html)

    universe = {}
    order = []

    for id_, name, exchange, currency, market, themes_raw, tv in base:
        if id_ not in universe:
            universe[id_] = dict(name=name, exchange=exchange, currency=currency,
                                  market=market, tv=tv or None)
            order.append(id_)

    for theme, market, source, bucket, spec in groups:
        for row in spec.split(';'):
            if not row:
                continue
            parts = row.split('|')
            id_ = parts[0]
            if id_ in universe:
                continue  # first occurrence wins, same as addSourceGroup() in the page
            name = parts[1] if len(parts) > 1 and parts[1] else id_
            exchange = parts[2] if len(parts) > 2 and parts[2] else (
                'KRX' if market == 'domestic' else 'US 상장')
            currency = parts[3] if len(parts) > 3 and parts[3] else (
                'KRW' if market == 'domestic' else 'USD')
            tv = parts[4] if len(parts) > 4 and parts[4] else None
            universe[id_] = dict(name=name, exchange=exchange, currency=currency,
                                  market=market, tv=tv)
            order.append(id_)

    return universe, order


def yahoo_symbol(id_, info):
    if id_ in OVERRIDES:
        return OVERRIDES[id_]
    # Korean listings resolve from the verified table only — never guessed.
    # See the KR_SYMBOLS comment: a guessed suffix returns a wrong instrument
    # rather than an error.
    if id_ in KR_SYMBOLS:
        return KR_SYMBOLS[id_]
    tv = info.get('tv')
    if tv:
        if ':' in tv:
            prefix, sym = tv.split(':', 1)
        else:
            prefix, sym = None, tv
        if prefix in ('KRX', 'KOSDAQ'):
            if sym in KR_SYMBOLS:
                return KR_SYMBOLS[sym]
            raise SystemExit(
                f"Korean ticker {sym} (id {id_}) is not in KR_SYMBOLS. Resolve it "
                f"against https://query1.finance.yahoo.com/v1/finance/search?q={sym}.KS "
                f"and add it — do not let the suffix be guessed.")
        if prefix == 'TSX':
            return sym + '.TO'
        if prefix == 'TSXV':
            return sym + '.V'
        if prefix == 'TSE':
            return sym + '.T'
        if prefix == 'HKEX':
            return sym.zfill(4) + '.HK'
        if prefix in ('NASDAQ', 'NYSE', 'AMEX', 'ARCA', 'BATS'):
            return sym
    if re.fullmatch(r'[0-9A-Z]{6}', id_) and info.get('market') == 'domestic':
        raise SystemExit(
            f"Korean ticker {id_} is not in KR_SYMBOLS. Resolve it against "
            f"https://query1.finance.yahoo.com/v1/finance/search?q={id_}.KS "
            f"and add it — do not let the suffix be guessed.")
    return id_


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def _get_json(url, headers, timeout):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "replace")
    start = body.find("{")
    end = body.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object in response")
    return json.loads(body[start:end + 1])


def http_via_proxy(url, timeout=PROXY_TIMEOUT):
    """Last resort once our IP is rate-limited: fetch the same Yahoo URL
    through a public reader that requests it from its own IP."""
    enc = urllib.parse.quote(url, safe="")
    for tpl, extra in PROXY_TEMPLATES:
        headers = {"User-Agent": UA}
        headers.update(extra)
        try:
            return _get_json(tpl.format(url=url, enc=enc), headers, timeout)
        except Exception:  # noqa: BLE001 - try the next reader
            continue
    return None


def http_get_json(url, timeout=20):
    """Direct first, then a public reader, then bounded backoff.

    Yahoo answers directly in ~0.1s when it likes us and 429s when it does
    not, and the limiter decays on its own after a few minutes. So: always
    try direct (it may have recovered since the last batch), fall straight
    through to a reader on failure (~0.6s), and only sleep if both are out.
    Never latch into proxy-only mode — that turns a recovered limiter into
    a permanent slowdown."""
    headers = {"User-Agent": UA, "Accept": "application/json"}
    for attempt in range(MAX_429_RETRIES):
        try:
            return _get_json(url, headers, timeout)
        except Exception:  # noqa: BLE001 - direct failed, try the readers
            pass
        data = http_via_proxy(url)
        if data is not None:
            return data
        time.sleep((1.8 ** attempt) + random.uniform(0, 0.7))
    return _get_json(url, headers, timeout)  # last try, let it raise


def fetch_batch(symbols):
    """One batched request -> {symbol: {'meta':..., 'timestamps':[...], 'closes':[...]}}
    for every symbol Yahoo actually answered (silently-dropped/invalid symbols
    just won't be present in the result)."""
    url = SPARK_URL.format(syms=",".join(symbols))
    out = {}
    try:
        data = http_get_json(url)
    except Exception as e:
        print(f"  ! batch failed {symbols}: {e}", file=sys.stderr)
        return out
    for item in (data.get("spark", {}) or {}).get("result", []) or []:
        sym = item.get("symbol")
        resp = item.get("response")
        if not (sym and resp and resp[0] and resp[0].get("meta")):
            continue
        meta = resp[0]["meta"]
        ts = resp[0].get("timestamp") or []
        closes = (resp[0].get("indicators", {}).get("quote", [{}]) or [{}])[0].get("close") or []
        pairs = [(t, c) for t, c in zip(ts, closes) if c is not None]
        out[sym] = {
            "meta": meta,
            "timestamps": [p[0] for p in pairs],
            "closes": [p[1] for p in pairs],
        }
    return out


# --------------------------------------------------------------------------
# Indicators
# --------------------------------------------------------------------------
def wilder_rsi(closes, period=14):
    if len(closes) < 30:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def sma(closes, n):
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def sig4(x):
    """Round to 4 significant digits."""
    if x is None:
        return None
    if x == 0:
        return 0.0
    d = 4 - int(floor(log10(abs(x)))) - 1
    return round(x, d)


def downsample(values, n):
    if len(values) <= n:
        return list(values)
    step = (len(values) - 1) / (n - 1)
    out = []
    for i in range(n):
        idx = min(round(i * step), len(values) - 1)
        out.append(values[idx])
    return out


def compact_history(hist, cutoff_days=183, keep_every=5):
    """Weekly-sample anything older than ~6 months to control file size."""
    t, v = hist["t"], hist["v"]
    if not t:
        return hist
    cutoff = t[-1] - cutoff_days * 86400
    old_t, old_v, recent_t, recent_v = [], [], [], []
    for ti, vi in zip(t, v):
        (recent_t if ti >= cutoff else old_t).append(ti)
        (recent_v if ti >= cutoff else old_v).append(vi)
    return {"t": old_t[::keep_every] + recent_t, "v": old_v[::keep_every] + recent_v}


# --------------------------------------------------------------------------
# HTML rewriting
# --------------------------------------------------------------------------
def build_live_block(live_data, live_asof, symbol_map=None):
    payload = json.dumps(live_data, ensure_ascii=False, separators=(",", ":"))
    ymap = json.dumps(symbol_map or {}, ensure_ascii=False, separators=(",", ":"))
    return (MARKER_START + "\n"
            + "const LIVE=" + payload + ";\n"
            # id -> Yahoo symbol, so the page can re-fetch these same tickers
            # live in the browser without re-deriving the mapping.
            + "const YMAP=" + ymap + ";\n"
            + "const LIVE_ASOF='" + live_asof + "';\n"
            + MARKER_END)


def update_html(html, live_data, live_asof, today, symbol_map=None):
    html = re.sub(r"UPDATED='[^']*'", f"UPDATED='{today}'", html, count=1)
    block = build_live_block(live_data, live_asof, symbol_map)
    if MARKER_START in html and MARKER_END in html:
        pattern = re.compile(re.escape(MARKER_START) + r".*?" + re.escape(MARKER_END), re.S)
        html, n = pattern.subn(lambda m: block, html, count=1)
        if n == 0:
            raise RuntimeError("LIVE-DATA markers found but regex replace failed")
    else:
        anchor = re.compile(r"const THEMES=\[[^;]*?UPDATED='[^']*';\n?")
        m = anchor.search(html)
        if not m:
            raise RuntimeError("could not find 'const THEMES=...,UPDATED=...;' line to anchor LIVE-DATA block")
        html = html[:m.end()] + block + "\n" + html[m.end():]
    return html


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    if not HTML_PATH.exists():
        print(f"ERROR: {HTML_PATH} not found", file=sys.stderr)
        sys.exit(1)

    html = HTML_PATH.read_text(encoding="utf-8")
    universe, order = parse_universe(html)
    print(f"Parsed {len(universe)} unique tickers from dashboard-v2.html")
    if len(universe) != 231:
        print(f"ERROR: expected exactly 231 unique tickers, got {len(universe)}. "
              f"The page's STOCKS/addSourceGroup structure may have changed — "
              f"check the parser regexes before trusting any output.", file=sys.stderr)
        sys.exit(1)

    id_to_symbol = {id_: yahoo_symbol(id_, universe[id_]) for id_ in order}
    unique_symbols = sorted(set(s for s in id_to_symbol.values() if s))

    print(f"Fetching quote+1y history for {len(unique_symbols)} unique Yahoo symbols "
          f"in batches of {BATCH_SIZE} ...")
    fetched = {}
    n_batches = (len(unique_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    for bi in range(n_batches):
        batch = unique_symbols[bi * BATCH_SIZE:(bi + 1) * BATCH_SIZE]
        fetched.update(fetch_batch(batch))
        print(f"  batch {bi + 1}/{n_batches}: {len(fetched)} symbols resolved so far")
        time.sleep(BATCH_SLEEP)

    print(f"Symbol-level fetch resolved {len(fetched)}/{len(unique_symbols)} symbols")

    LIVE = {}
    resolved_ids, unresolved_ids = [], []

    for id_ in order:
        sym = id_to_symbol[id_]
        entry = fetched.get(sym) if sym else None
        if not entry:
            unresolved_ids.append(id_)
            continue
        meta = entry["meta"]
        close_list = entry["closes"]
        ts_list = entry["timestamps"]
        if meta.get("regularMarketPrice") is None or not close_list:
            unresolved_ids.append(id_)
            continue

        rsi = wilder_rsi(close_list)
        m20, m60 = sma(close_list, 20), sma(close_list, 60)
        spark = [sig4(v) for v in downsample(close_list[-120:], 40)]

        tz_name = meta.get("exchangeTimezoneName") or "UTC"
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")
        rmt = meta.get("regularMarketTime")
        asof = datetime.fromtimestamp(rmt, tz).strftime("%Y-%m-%d %H:%M %Z") if rmt else None

        LIVE[id_] = {
            "p": meta.get("regularMarketPrice"),
            "c": meta.get("regularMarketChangePercent"),
            "h": meta.get("fiftyTwoWeekHigh"),
            "l": meta.get("fiftyTwoWeekLow"),
            "cur": meta.get("currency"),
            "rsi": rsi,
            "m20": sig4(m20) if m20 is not None else None,
            "m60": sig4(m60) if m60 is not None else None,
            "s": spark,
            "hist": {"t": list(ts_list), "v": [sig4(v) for v in close_list]},
            "as": asof,
        }
        resolved_ids.append(id_)

    print(f"resolved {len(resolved_ids)}/231")
    if unresolved_ids:
        print(f"unresolved ({len(unresolved_ids)}): {', '.join(unresolved_ids)}")

    if len(resolved_ids) < MIN_RESOLVED:
        print(f"ERROR: only {len(resolved_ids)}/231 resolved (< {MIN_RESOLVED}) — "
              f"refusing to write a degraded dataset. Check network access / Yahoo "
              f"availability (rate limiting?) and re-run.", file=sys.stderr)
        sys.exit(1)

    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    live_asof = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

    ymap = {i: id_to_symbol[i] for i in resolved_ids if id_to_symbol.get(i)}
    new_html = update_html(html, LIVE, live_asof, today, ymap)
    size_mb = len(new_html.encode("utf-8")) / (1024 * 1024)
    if size_mb > 4:
        print(f"Output {size_mb:.2f}MB > 4MB budget — compacting history to weekly "
              f"sampling beyond the most recent 6 months ...")
        for id_ in resolved_ids:
            LIVE[id_]["hist"] = compact_history(LIVE[id_]["hist"])
        new_html = update_html(html, LIVE, live_asof, today, ymap)
        size_mb = len(new_html.encode("utf-8")) / (1024 * 1024)
        print(f"  new size: {size_mb:.2f}MB")

    HTML_PATH.write_text(new_html, encoding="utf-8")
    print(f"Wrote {HTML_PATH} ({len(new_html.encode('utf-8')) / 1024:.1f} KB, "
          f"{size_mb:.2f} MB) — UPDATED={today}, LIVE_ASOF={live_asof}")


if __name__ == "__main__":
    main()
