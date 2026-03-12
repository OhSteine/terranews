#!/usr/bin/env python3
"""
market_data.py — Yahoo Finance free data layer for TERRANEWS
Fetches 5-day/1-day OHLCV for equity, FX, and bond proxies.
Computes: ATR-normalised move, volume anomaly, cross-asset correlation break.
No API key required.
"""

import urllib.request
import json
import time
import threading
from datetime import datetime, timezone

# ── REFRESH ────────────────────────────────────────────────────────────────────
MARKET_INTERVAL = 5 * 60   # 5 minutes

# ── REGION DEFINITIONS ────────────────────────────────────────────────────────
# Each region ties to one or more TERRANEWS source cities.
# equity: main index; fx: local currency vs USD (or USD index); bond: yield proxy
# All tickers are Yahoo Finance symbols — free, no auth.
REGIONS = [
    # North America
    {
        "id": "us",
        "label": "United States",
        "cities": ["NEW YORK", "WASHINGTON D.C.", "CHICAGO", "LOS ANGELES"],
        "lat": 40.71, "lon": -74.01,
        "equity": "^GSPC",   "equity_name": "S&P 500",
        "fx":     "DX-Y.NYB","fx_name":     "US Dollar Index",
        "bond":   "^TNX",    "bond_name":   "US 10Y Yield",
        "fx_invert": False,   # DXY: higher = stronger USD (normal)
    },
    {
        "id": "ca",
        "label": "Canada",
        "cities": ["TORONTO", "CALGARY"],
        "lat": 43.65, "lon": -79.38,
        "equity": "^GSPTSE", "equity_name": "TSX",
        "fx":     "CADUSD=X","fx_name":     "CAD/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "mx",
        "label": "Mexico",
        "cities": ["MEXICO CITY"],
        "lat": 19.43, "lon": -99.13,
        "equity": "^MXX",    "equity_name": "IPC Mexico",
        "fx":     "MXNUSD=X","fx_name":     "MXN/USD",
        "bond":   None,
        "fx_invert": False,
    },
    # South America
    {
        "id": "br",
        "label": "Brazil",
        "cities": ["SAO PAULO"],
        "lat": -23.55, "lon": -46.63,
        "equity": "^BVSP",   "equity_name": "Bovespa",
        "fx":     "BRLUSD=X","fx_name":     "BRL/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "ar",
        "label": "Argentina",
        "cities": ["BUENOS AIRES"],
        "lat": -34.61, "lon": -58.38,
        "equity": "^MERV",   "equity_name": "MERVAL",
        "fx":     "ARSUSD=X","fx_name":     "ARS/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "cl",
        "label": "Chile",
        "cities": ["SANTIAGO"],
        "lat": -33.45, "lon": -70.67,
        "equity": "^IPSA",   "equity_name": "IPSA",
        "fx":     "CLPUSD=X","fx_name":     "CLP/USD",
        "bond":   None,
        "fx_invert": False,
    },
    # Europe
    {
        "id": "gb",
        "label": "United Kingdom",
        "cities": ["LONDON"],
        "lat": 51.51, "lon": -0.13,
        "equity": "^FTSE",   "equity_name": "FTSE 100",
        "fx":     "GBPUSD=X","fx_name":     "GBP/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "eu",
        "label": "Eurozone",
        "cities": ["PARIS", "FRANKFURT", "MADRID", "MILAN", "AMSTERDAM", "BRUSSELS", "LISBON", "ATHENS", "VIENNA"],
        "lat": 48.86, "lon": 2.35,
        "equity": "^STOXX50E","equity_name":"Euro Stoxx 50",
        "fx":     "EURUSD=X", "fx_name":    "EUR/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "de",
        "label": "Germany",
        "cities": ["HAMBURG", "BERLIN"],
        "lat": 52.52, "lon": 13.40,
        "equity": "^GDAXI",  "equity_name": "DAX",
        "fx":     "EURUSD=X","fx_name":     "EUR/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "ch",
        "label": "Switzerland",
        "cities": ["ZURICH"],
        "lat": 47.38, "lon": 8.54,
        "equity": "^SSMI",   "equity_name": "SMI",
        "fx":     "CHFUSD=X","fx_name":     "CHF/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "nordic",
        "label": "Nordics",
        "cities": ["OSLO", "STOCKHOLM", "HELSINKI", "COPENHAGEN"],
        "lat": 59.33, "lon": 18.07,
        "equity": "^OMX",    "equity_name": "OMX Stockholm 30",
        "fx":     "SEKUSD=X","fx_name":     "SEK/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "pl",
        "label": "Poland",
        "cities": ["WARSAW"],
        "lat": 52.23, "lon": 21.01,
        "equity": "^WIG20",  "equity_name": "WIG20",
        "fx":     "PLNUSD=X","fx_name":     "PLN/USD",
        "bond":   None,
        "fx_invert": False,
    },
    # Middle East & Africa
    {
        "id": "il",
        "label": "Israel",
        "cities": ["TEL AVIV"],
        "lat": 32.08, "lon": 34.78,
        "equity": "^TA125.TA","equity_name":"TA-125",
        "fx":     "ILSUSD=X","fx_name":     "ILS/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "za",
        "label": "South Africa",
        "cities": ["JOHANNESBURG"],
        "lat": -26.20, "lon": 28.04,
        "equity": "^J203.JO","equity_name": "JSE Top40",
        "fx":     "ZARUSD=X","fx_name":     "ZAR/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "ng",
        "label": "Egypt / N.Africa",
        "cities": ["LAGOS", "NAIROBI", "ADDIS ABABA"],
        "lat": 6.52, "lon": 3.38,
        "equity": "^CCSI",     "equity_name": "EGX 30 (Cairo)",
        "fx":     "EGPUSD=X",  "fx_name":     "EGP/USD",
        "bond":   None,
        "fx_invert": False,
    },
    # Asia
    {
        "id": "in",
        "label": "India",
        "cities": ["NEW DELHI", "MUMBAI"],
        "lat": 19.08, "lon": 72.88,
        "equity": "^BSESN",  "equity_name": "SENSEX",
        "fx":     "INRUSD=X","fx_name":     "INR/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "jp",
        "label": "Japan",
        "cities": ["TOKYO", "OSAKA"],
        "lat": 35.69, "lon": 139.69,
        "equity": "^N225",   "equity_name": "Nikkei 225",
        "fx":     "JPYUSD=X","fx_name":     "JPY/USD",
        "bond":   None,
        "fx_invert": False,  # JPY/USD: lower = weaker yen (inverted intuition but consistent)
    },
    {
        "id": "kr",
        "label": "South Korea",
        "cities": ["SEOUL"],
        "lat": 37.57, "lon": 126.98,
        "equity": "^KS11",   "equity_name": "KOSPI",
        "fx":     "KRWUSD=X","fx_name":     "KRW/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "hk",
        "label": "Hong Kong",
        "cities": ["HONG KONG"],
        "lat": 22.32, "lon": 114.17,
        "equity": "^HSI",    "equity_name": "Hang Seng",
        "fx":     "HKDUSD=X","fx_name":     "HKD/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "sg",
        "label": "Singapore",
        "cities": ["SINGAPORE"],
        "lat": 1.35, "lon": 103.82,
        "equity": "^STI",    "equity_name": "Straits Times",
        "fx":     "SGDUSD=X","fx_name":     "SGD/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "au",
        "label": "Australia",
        "cities": ["SYDNEY", "MELBOURNE"],
        "lat": -33.87, "lon": 151.21,
        "equity": "^AXJO",   "equity_name": "ASX 200",
        "fx":     "AUDUSD=X","fx_name":     "AUD/USD",
        "bond":   None,
        "fx_invert": False,
    },
    {
        "id": "cn",
        "label": "China / HK",
        "cities": ["SHANGHAI", "BEIJING"],
        "lat": 31.23, "lon": 121.47,
        "equity": "000001.SS","equity_name":"Shanghai Comp.",
        "fx":     "CNYUSD=X","fx_name":     "CNY/USD",
        "bond":   None,
        "fx_invert": False,
    },
]

# Build city → region lookup
CITY_TO_REGION = {}
for r in REGIONS:
    for city in r["cities"]:
        CITY_TO_REGION[city] = r["id"]

# ── YAHOO FINANCE FETCH ───────────────────────────────────────────────────────

def _yahoo_fetch(symbol, range_="5d", interval="1d"):
    """
    Fetch OHLCV from Yahoo Finance chart API.
    Returns list of dicts: [{open, high, low, close, volume, timestamp}, ...]
    or empty list on failure.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
        f"?interval={interval}&range={range_}&includePrePost=false"
    )
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())

        result = data.get("chart", {}).get("result", [])
        if not result:
            return []
        r0 = result[0]
        timestamps = r0.get("timestamp", [])
        q = r0.get("indicators", {}).get("quote", [{}])[0]
        opens   = q.get("open",   [])
        highs   = q.get("high",   [])
        lows    = q.get("low",    [])
        closes  = q.get("close",  [])
        volumes = q.get("volume", [])

        rows = []
        for i, ts in enumerate(timestamps):
            try:
                rows.append({
                    "ts":     ts,
                    "open":   opens[i],
                    "high":   highs[i],
                    "low":    lows[i],
                    "close":  closes[i],
                    "volume": volumes[i] if volumes else None,
                })
            except IndexError:
                pass
        # Filter out None-close rows (market closed / holiday gaps)
        rows = [r for r in rows if r["close"] is not None]
        return rows

    except Exception as e:
        print(f"  x Yahoo [{symbol}]: {e}")
        return []

# ── ANALYTICS ────────────────────────────────────────────────────────────────

def _atr(rows):
    """Average True Range over available rows (uses close-to-close as proxy when no prev)."""
    if len(rows) < 2:
        return None
    trs = []
    for i in range(1, len(rows)):
        h = rows[i]["high"]  or rows[i]["close"]
        l = rows[i]["low"]   or rows[i]["close"]
        pc = rows[i-1]["close"]
        if h is None or l is None or pc is None:
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else None

def _avg_volume(rows):
    vols = [r["volume"] for r in rows if r.get("volume") is not None]
    return sum(vols) / len(vols) if vols else None

def _pct_change(rows):
    """Today's % change: (today_close - prev_close) / prev_close * 100"""
    if len(rows) < 2:
        return None
    prev  = rows[-2]["close"]
    today = rows[-1]["close"]
    if prev and today:
        return (today - prev) / prev * 100
    return None

def _sparkline(rows, key="close"):
    """Return last N closes as list for mini chart."""
    return [r[key] for r in rows if r.get(key) is not None]

def analyse_asset(symbol, rows):
    if not rows or len(rows) < 2:
        return {"symbol": symbol, "ok": False}

    today     = rows[-1]
    atr       = _atr(rows)
    avg_vol   = _avg_volume(rows[:-1])  # exclude today from baseline
    pct       = _pct_change(rows)
    today_vol = today.get("volume")

    # ATR-normalised move (how many ATRs did we move today?)
    atr_multiple = None
    if atr and atr > 0 and today["high"] and today["low"] and today["close"] and rows[-2]["close"]:
        today_tr = max(
            today["high"] - today["low"],
            abs(today["high"] - rows[-2]["close"]),
            abs(today["low"]  - rows[-2]["close"])
        )
        atr_multiple = today_tr / atr

    # Volume ratio (today vs 5-day avg)
    vol_ratio = None
    if avg_vol and avg_vol > 0 and today_vol:
        vol_ratio = today_vol / avg_vol

    return {
        "symbol":       symbol,
        "ok":           True,
        "close":        today["close"],
        "open":         today["open"],
        "high":         today["high"],
        "low":          today["low"],
        "prev_close":   rows[-2]["close"] if len(rows) >= 2 else None,
        "pct_change":   round(pct, 3) if pct is not None else None,
        "atr":          round(atr, 4) if atr else None,
        "atr_multiple": round(atr_multiple, 2) if atr_multiple else None,
        "vol_ratio":    round(vol_ratio, 2) if vol_ratio else None,
        "sparkline":    _sparkline(rows),
    }

# ── ANOMALY SCORING ───────────────────────────────────────────────────────────

def anomaly_score(equity_data):
    """
    0–10 anomaly score for a region based on equity data.
    Drives marker size and glow on the map.
    Components:
      - ATR multiple  (0–5 pts): 1x=0, 1.5x=2, 2x=3.5, 3x=5
      - Volume ratio  (0–3 pts): 1x=0, 1.5x=1, 2x=2, 3x=3
      - Abs pct move  (0–2 pts): <0.5%=0, 1%=1, 2%=2
    """
    if not equity_data or not equity_data.get("ok"):
        return 0.0

    score = 0.0
    atr_m = equity_data.get("atr_multiple") or 0
    vol_r = equity_data.get("vol_ratio")    or 1
    pct   = abs(equity_data.get("pct_change") or 0)

    # ATR component (max 5)
    if   atr_m >= 3.0: score += 5.0
    elif atr_m >= 2.0: score += 3.5
    elif atr_m >= 1.5: score += 2.0
    elif atr_m >= 1.0: score += 0.5

    # Volume component (max 3)
    if   vol_r >= 3.0: score += 3.0
    elif vol_r >= 2.0: score += 2.0
    elif vol_r >= 1.5: score += 1.0

    # Price move component (max 2)
    if   pct >= 2.0: score += 2.0
    elif pct >= 1.0: score += 1.0
    elif pct >= 0.5: score += 0.3

    return round(min(score, 10.0), 2)

# ── CROSS-ASSET CORRELATION BREAK ─────────────────────────────────────────────

CORRELATION_BREAKS = [
    # (description, condition_fn)
    # condition_fn receives (equity_pct, fx_pct, bond_pct) — all can be None
]

def detect_correlation_break(equity, fx, bond=None):
    """
    Returns a list of detected breaks with labels.
    equity_pct: + = equity rising, fx_pct: + = local currency strengthening vs USD
    For risk-on: equity UP + currency UP = consistent.
    Breaks:
      1. Equity UP + FX DOWN (capital inflow without currency support → suspect)
      2. Equity DOWN + FX DOWN + bond yield UP (triple stress = hard risk-off)
      3. Equity UP + bond yield surging (bond market says risk-off while equities rally)
      4. Equity DOWN + FX UP (flight to local currency safety — unusual EM signal)
    """
    if not equity or not equity.get("ok"):
        return []

    eq_pct   = equity.get("pct_change") or 0
    fx_pct   = (fx.get("pct_change") if fx and fx.get("ok") else None)
    bond_pct = (bond.get("pct_change") if bond and bond.get("ok") else None)

    breaks = []
    threshold = 0.3  # minimum % move to flag a break

    if fx_pct is not None and abs(eq_pct) > threshold and abs(fx_pct) > threshold:
        # Equity UP but currency weakening
        if eq_pct > 0 and fx_pct < -threshold:
            breaks.append({
                "type": "EQ_UP_FX_DOWN",
                "label": "Equity rallying but currency weakening",
                "severity": "medium",
                "detail": f"Equity {eq_pct:+.1f}% / FX {fx_pct:+.1f}%"
            })
        # Equity DOWN and currency weakening (double stress — risk-off)
        if eq_pct < -threshold and fx_pct < -threshold:
            breaks.append({
                "type": "DOUBLE_STRESS",
                "label": "Equity + currency both selling off",
                "severity": "high",
                "detail": f"Equity {eq_pct:+.1f}% / FX {fx_pct:+.1f}%"
            })
        # Equity DOWN but currency strengthening (unusual)
        if eq_pct < -threshold and fx_pct > threshold:
            breaks.append({
                "type": "EQ_DOWN_FX_UP",
                "label": "Equity down but currency strengthening",
                "severity": "low",
                "detail": f"Equity {eq_pct:+.1f}% / FX {fx_pct:+.1f}%"
            })

    if bond_pct is not None and abs(eq_pct) > threshold and abs(bond_pct) > threshold:
        # Bond yield surging while equities also up (tension)
        if eq_pct > threshold and bond_pct > threshold:
            breaks.append({
                "type": "EQ_UP_YIELD_UP",
                "label": "Equities rising while yields surge",
                "severity": "medium",
                "detail": f"Equity {eq_pct:+.1f}% / Yield {bond_pct:+.1f}%"
            })
        # Triple stress: equity down + currency down + yield up
        if fx_pct is not None and eq_pct < -threshold and fx_pct < -threshold and bond_pct > threshold:
            breaks.append({
                "type": "TRIPLE_STRESS",
                "label": "TRIPLE STRESS — equity, currency, yields all under pressure",
                "severity": "critical",
                "detail": f"Equity {eq_pct:+.1f}% / FX {fx_pct:+.1f}% / Yield {bond_pct:+.1f}%"
            })

    return breaks

# ── MAIN FETCH + CACHE ────────────────────────────────────────────────────────

_cache = {}          # region_id → full data dict
_cache_lock = threading.Lock()
_last_fetch = 0

import urllib.parse   # needed for symbol quoting

def fetch_region(region):
    """Fetch and analyse all assets for one region. Returns region data dict."""
    out = {
        "id":       region["id"],
        "label":    region["label"],
        "lat":      region["lat"],
        "lon":      region["lon"],
        "cities":   region["cities"],
        "updated":  datetime.now(timezone.utc).isoformat(),
    }

    # Equity
    eq_rows = _yahoo_fetch(region["equity"])
    eq_data = analyse_asset(region["equity"], eq_rows)
    eq_data["name"] = region["equity_name"]
    out["equity"] = eq_data

    # FX
    fx_data = {"ok": False}
    if region.get("fx"):
        fx_rows = _yahoo_fetch(region["fx"])
        fx_data = analyse_asset(region["fx"], fx_rows)
        fx_data["name"] = region["fx_name"]
    out["fx"] = fx_data

    # Bond
    bond_data = {"ok": False}
    if region.get("bond"):
        bond_rows = _yahoo_fetch(region["bond"])
        bond_data = analyse_asset(region["bond"], bond_rows)
        bond_data["name"] = region["bond_name"]
    out["bond"] = bond_data

    # Derived signals
    out["anomaly_score"]  = anomaly_score(eq_data)
    out["corr_breaks"]    = detect_correlation_break(eq_data, fx_data, bond_data)
    out["has_break"]      = len(out["corr_breaks"]) > 0
    out["break_severity"] = (
        "critical" if any(b["severity"] == "critical" for b in out["corr_breaks"]) else
        "high"     if any(b["severity"] == "high"     for b in out["corr_breaks"]) else
        "medium"   if any(b["severity"] == "medium"   for b in out["corr_breaks"]) else
        "low"      if out["corr_breaks"] else
        "none"
    )

    return out

def fetch_all_regions():
    """Fetch all regions in parallel, update cache."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    print(f"\n  [market] Fetching {len(REGIONS)} regions...")
    t0 = time.time()
    results = {}

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_region, r): r["id"] for r in REGIONS}
        for future in as_completed(futures):
            rid = futures[future]
            try:
                data = future.result()
                results[rid] = data
            except Exception as e:
                print(f"  x region [{rid}]: {e}")

    with _cache_lock:
        _cache.clear()
        _cache.update(results)

    elapsed = time.time() - t0
    ok = sum(1 for v in results.values() if v["equity"].get("ok"))
    print(f"  [market] Done in {elapsed:.1f}s — {ok}/{len(REGIONS)} equity ok")

def get_cache():
    with _cache_lock:
        return dict(_cache)

def get_region(region_id):
    with _cache_lock:
        return _cache.get(region_id)

def background_market_fetcher():
    """Run in daemon thread. Fetches immediately, then every MARKET_INTERVAL seconds."""
    fetch_all_regions()
    while True:
        time.sleep(MARKET_INTERVAL)
        fetch_all_regions()

def city_to_region_id(city):
    return CITY_TO_REGION.get(city)
