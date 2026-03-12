#!/usr/bin/env python3
"""
TERRANEWS — zero-dependency local server
Run:  python3 server.py  →  open http://localhost:3000

News:   RSS primary, GNews fallback, every 30 min.
Market: Yahoo Finance free, every 5 min.
  GET /api/market        → all regions (anomaly scores, cross-asset breaks)
  GET /api/market/<id>   → single region full detail + sparklines
"""

import http.server
import urllib.request
import urllib.parse
import urllib.error
import json
import threading
import time
import re
import ssl
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import db
import market_data

PORT        = 3000
PUBLIC      = Path(__file__).parent / "public"
INTERVAL    = 30 * 60   # news refresh: 30 min
MAX_WORKERS = 6

SOURCES = [
    # ── NORTH AMERICA ──────────────────────────────────────────────────────────
    {"id":"us_nyt",    "country":"us","category":"general", "name":"New York Times",      "city":"NEW YORK",       "lat":40.71, "lon":-74.01, "rss":"https://rss.nytimes.com/services/xml/rss/nyt/World.xml"},
    {"id":"us_wp",     "country":"us","category":"general", "name":"Washington Post",     "city":"WASHINGTON D.C.","lat":38.89, "lon":-77.03, "rss":"https://feeds.washingtonpost.com/rss/national"},
    {"id":"us_npr",    "country":"us","category":"general", "name":"NPR News",            "city":"CHICAGO",        "lat":41.88, "lon":-87.63, "rss":"https://feeds.npr.org/1001/rss.xml"},
    {"id":"us_lat",    "country":"us","category":"general", "name":"LA Times",            "city":"LOS ANGELES",    "lat":34.05, "lon":-118.24,"rss":"https://www.latimes.com/world-nation/rss2.0.xml"},
    {"id":"ca_gm",     "country":"ca","category":"general", "name":"Globe and Mail",      "city":"TORONTO",        "lat":43.65, "lon":-79.38, "rss":"https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/world/"},
    {"id":"ca_gn",     "country":"ca","category":"general", "name":"Global News",         "city":"CALGARY",        "lat":51.04, "lon":-114.07,"rss":"https://globalnews.ca/feed/"},
    {"id":"mx_inf",    "country":"mx","category":"general", "name":"Infobae Mexico",      "city":"MEXICO CITY",    "lat":19.43, "lon":-99.13, "rss":"https://www.infobae.com/rss/"},
    # ── SOUTH AMERICA ──────────────────────────────────────────────────────────
    {"id":"br_folha",  "country":"br","category":"general", "name":"Folha de S.Paulo",    "city":"SAO PAULO",      "lat":-23.55,"lon":-46.63, "rss":"https://feeds.folha.uol.com.br/mundo/rss091.xml"},
    {"id":"ar_clarin", "country":"ar","category":"general", "name":"Clarin",              "city":"BUENOS AIRES",   "lat":-34.61,"lon":-58.38, "rss":"https://www.clarin.com/rss/lo-ultimo/"},
    {"id":"co_el",     "country":"co","category":"general", "name":"El Tiempo",           "city":"BOGOTA",         "lat":4.71,  "lon":-74.07, "rss":"https://www.eltiempo.com/rss/mundo.xml"},
    {"id":"cl_bio",    "country":"cl","category":"general", "name":"El Mostrador",        "city":"SANTIAGO",       "lat":-33.45,"lon":-70.67, "rss":"https://www.elmostrador.cl/feed/"},
    # ── EUROPE — WEST ──────────────────────────────────────────────────────────
    {"id":"gb_guardian","country":"gb","category":"general","name":"The Guardian",        "city":"LONDON",         "lat":51.51, "lon":-0.13,  "rss":"https://www.theguardian.com/world/rss"},
    {"id":"gb_bbc",    "country":"gb","category":"general", "name":"BBC News",            "city":"LONDON",         "lat":51.52, "lon":-0.14,  "rss":"http://feeds.bbci.co.uk/news/world/rss.xml"},
    {"id":"gb_ft",     "country":"gb","category":"business","name":"Financial Times",     "city":"LONDON",         "lat":51.50, "lon":-0.12,  "rss":"https://www.ft.com/world?format=rss"},
    {"id":"fr_lemonde","country":"fr","category":"general", "name":"Le Monde",            "city":"PARIS",          "lat":48.86, "lon":2.35,   "rss":"https://www.lemonde.fr/rss/une.xml"},
    {"id":"fr_f24",    "country":"fr","category":"general", "name":"France 24",           "city":"PARIS",          "lat":48.85, "lon":2.34,   "rss":"https://www.france24.com/en/rss"},
    {"id":"de_spiegel","country":"de","category":"general", "name":"Der Spiegel",         "city":"HAMBURG",        "lat":53.55, "lon":9.99,   "rss":"https://www.spiegel.de/international/index.rss"},
    {"id":"de_dw",     "country":"de","category":"general", "name":"Deutsche Welle",      "city":"BERLIN",         "lat":52.52, "lon":13.40,  "rss":"https://rss.dw.com/xml/rss-en-all"},
    {"id":"es_elpais", "country":"es","category":"general", "name":"El Pais",             "city":"MADRID",         "lat":40.42, "lon":-3.70,  "rss":"https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada"},
    {"id":"it_corriere","country":"it","category":"general","name":"Corriere della Sera", "city":"MILAN",          "lat":45.46, "lon":9.19,   "rss":"https://xml2.corriereobjects.it/rss/homepage.xml"},
    {"id":"nl_nrc",    "country":"nl","category":"general", "name":"NOS Nieuws",          "city":"AMSTERDAM",      "lat":52.37, "lon":4.90,   "rss":"https://feeds.nos.nl/nosnieuwsalgemeen"},
    {"id":"be_vrt",    "country":"be","category":"general", "name":"VRT News",            "city":"BRUSSELS",       "lat":50.85, "lon":4.35,   "rss":"https://www.vrt.be/vrtnws/en.rss.articles.xml"},
    {"id":"ch_srf",    "country":"ch","category":"general", "name":"SRF News",            "city":"ZURICH",         "lat":47.38, "lon":8.54,   "rss":"https://www.srf.ch/news/bnf/rss/1890"},
    {"id":"at_standard","country":"at","category":"general","name":"Der Standard",        "city":"VIENNA",         "lat":48.21, "lon":16.37,  "rss":"https://www.derstandard.at/rss"},
    {"id":"pt_dn",     "country":"pt","category":"general", "name":"Publico",             "city":"LISBON",         "lat":38.72, "lon":-9.14,  "rss":"https://feeds.feedburner.com/PublicoRSS"},
    # ── EUROPE — NORDIC ────────────────────────────────────────────────────────
    {"id":"no_aften",  "country":"no","category":"general", "name":"Aftenposten",         "city":"OSLO",           "lat":59.91, "lon":10.75,  "rss":"https://www.aftenposten.no/rss.xml"},
    {"id":"se_dn",     "country":"se","category":"general", "name":"Dagens Nyheter",      "city":"STOCKHOLM",      "lat":59.33, "lon":18.07,  "rss":"https://www.dn.se/rss/"},
    {"id":"fi_hs",     "country":"fi","category":"general", "name":"Helsingin Sanomat",   "city":"HELSINKI",       "lat":60.17, "lon":24.94,  "rss":"https://www.hs.fi/rss/tuoreimmat.xml"},
    {"id":"dk_cph",    "country":"dk","category":"general", "name":"Copenhagen Post",     "city":"COPENHAGEN",     "lat":55.68, "lon":12.57,  "rss":"https://cphpost.dk/?feed=rss2"},
    # ── EUROPE — EAST ──────────────────────────────────────────────────────────
    {"id":"pl_gw",     "country":"pl","category":"general", "name":"TVN24 Poland",        "city":"WARSAW",         "lat":52.23, "lon":21.01,  "rss":"https://tvn24.pl/najnowsze.xml"},
    {"id":"ua_kyiv",   "country":"ua","category":"general", "name":"Kyiv Independent",    "city":"KYIV",           "lat":50.45, "lon":30.52,  "rss":"https://kyivindependent.com/feed/"},
    {"id":"gr_ek",     "country":"gr","category":"general", "name":"Ekathimerini",        "city":"ATHENS",         "lat":37.98, "lon":23.73,  "rss":"https://www.ekathimerini.com/rss"},
    # ── MIDDLE EAST & AFRICA ───────────────────────────────────────────────────
    {"id":"eg_aj",     "country":"eg","category":"general", "name":"Al Jazeera",          "city":"DOHA",           "lat":25.29, "lon":51.53,  "rss":"https://www.aljazeera.com/xml/rss/all.xml"},
    {"id":"il_jpost",  "country":"il","category":"general", "name":"Jerusalem Post",      "city":"TEL AVIV",       "lat":32.08, "lon":34.78,  "rss":"https://www.jpost.com/rss/rssfeedsfrontpage.aspx"},
    {"id":"za_dm",     "country":"za","category":"general", "name":"Daily Maverick",      "city":"JOHANNESBURG",   "lat":-26.20,"lon":28.04,  "rss":"https://www.dailymaverick.co.za/dmrss/"},
    {"id":"ng_pt",     "country":"ng","category":"general", "name":"Premium Times",       "city":"LAGOS",          "lat":6.52,  "lon":3.38,   "rss":"https://www.premiumtimesng.com/feed"},
    {"id":"ke_dn",     "country":"ke","category":"general", "name":"Daily Nation",        "city":"NAIROBI",        "lat":-1.29, "lon":36.82,  "rss":"https://nation.africa/kenya/rss.xml"},
    {"id":"et_aa",     "country":"et","category":"general", "name":"The Reporter Eth.",   "city":"ADDIS ABABA",    "lat":9.03,  "lon":38.74,  "rss":"https://www.thereporterethiopia.com/feed"},
    # ── ASIA — SOUTH & SOUTHEAST ───────────────────────────────────────────────
    {"id":"in_hindu",  "country":"in","category":"general", "name":"The Hindu",           "city":"NEW DELHI",      "lat":28.61, "lon":77.21,  "rss":"https://www.thehindu.com/news/international/?service=rss"},
    {"id":"in_toi",    "country":"in","category":"general", "name":"Times of India",      "city":"MUMBAI",         "lat":19.08, "lon":72.88,  "rss":"https://timesofindia.indiatimes.com/rssfeeds/296589292.cms"},
    {"id":"pk_dawn",   "country":"pk","category":"general", "name":"Dawn",                "city":"KARACHI",        "lat":24.86, "lon":67.01,  "rss":"https://www.dawn.com/feeds/home"},
    {"id":"bd_ds",     "country":"bd","category":"general", "name":"Daily Star",          "city":"DHAKA",          "lat":23.81, "lon":90.41,  "rss":"https://www.thedailystar.net/frontpage/rss.xml"},
    {"id":"sg_st",     "country":"sg","category":"general", "name":"Straits Times",       "city":"SINGAPORE",      "lat":1.35,  "lon":103.82, "rss":"https://www.straitstimes.com/news/world/rss.xml"},
    {"id":"my_mm",     "country":"my","category":"general", "name":"Free Malaysia Today", "city":"KUALA LUMPUR",   "lat":3.14,  "lon":101.69, "rss":"https://www.freemalaysiatoday.com/feed/"},
    {"id":"th_bn",     "country":"th","category":"general", "name":"Bangkok Post",        "city":"BANGKOK",        "lat":13.75, "lon":100.52, "rss":"https://www.bangkokpost.com/rss/data/topstories.xml"},
    {"id":"ph_in",     "country":"ph","category":"general", "name":"Inquirer",            "city":"MANILA",         "lat":14.60, "lon":120.98, "rss":"https://newsinfo.inquirer.net/feed"},
    {"id":"id_det",    "country":"id","category":"general", "name":"Kompas",              "city":"JAKARTA",        "lat":-6.21, "lon":106.85, "rss":"https://rss.kompas.com/rss/news/nasional"},
    # ── ASIA — EAST ────────────────────────────────────────────────────────────
    {"id":"jp_jt",     "country":"jp","category":"general", "name":"Japan Times",         "city":"TOKYO",          "lat":35.69, "lon":139.69, "rss":"https://www.japantimes.co.jp/feed/"},
    {"id":"jp_nik",    "country":"jp","category":"business","name":"Nikkei Asia",          "city":"OSAKA",          "lat":34.69, "lon":135.50, "rss":"https://asia.nikkei.com/rss/feed/world"},
    {"id":"kr_kt",     "country":"kr","category":"general", "name":"Korea Times",         "city":"SEOUL",          "lat":37.57, "lon":126.98, "rss":"https://www.koreatimes.co.kr/www/rss/rss.xml"},
    {"id":"hk_scmp",   "country":"hk","category":"general", "name":"S. China Morning Post","city":"HONG KONG",     "lat":22.32, "lon":114.17, "rss":"https://www.scmp.com/rss/91/feed"},
    {"id":"tw_fp",     "country":"tw","category":"general", "name":"Focus Taiwan",        "city":"TAIPEI",         "lat":25.04, "lon":121.56, "rss":"https://focustaiwan.tw/rss/rss.xml"},
    # ── OCEANIA ────────────────────────────────────────────────────────────────
    {"id":"au_smh",    "country":"au","category":"general", "name":"Sydney Morning Herald","city":"SYDNEY",        "lat":-33.87,"lon":151.21, "rss":"https://www.smh.com.au/rss/world.xml"},
    {"id":"au_abc",    "country":"au","category":"general", "name":"ABC Australia",       "city":"MELBOURNE",      "lat":-37.81,"lon":144.96, "rss":"https://www.abc.net.au/news/feed/51120/rss.xml"},
    {"id":"nz_rnz",    "country":"nz","category":"general", "name":"RNZ News",            "city":"AUCKLAND",       "lat":-36.86,"lon":174.76, "rss":"https://www.rnz.co.nz/rss/world.xml"},
    # ── TURKEY ─────────────────────────────────────────────────────────────────
    {"id":"tr_hdn",    "country":"tr","category":"general", "name":"Hurriyet Daily News", "city":"ISTANBUL",       "lat":41.01, "lon":28.98,  "rss":"https://www.hurriyetdailynews.com/rss/"},
]

NS = {
    "media":   "http://search.yahoo.com/mrss/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc":      "http://purl.org/dc/elements/1.1/",
    "atom":    "http://www.w3.org/2005/Atom",
}

# Unverified SSL context — fallback for feeds with cert issues
_SSL_NOVERIFY = ssl.create_default_context()
_SSL_NOVERIFY.check_hostname = False
_SSL_NOVERIFY.verify_mode = ssl.CERT_NONE

def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").strip()

def fetch_rss(source):
    try:
        req = urllib.request.Request(source["rss"], headers={
            "User-Agent": "Mozilla/5.0 (compatible; TerraNews/1.0)",
            "Accept":     "application/rss+xml, application/xml, text/xml, */*"
        })
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
        except urllib.error.HTTPError as he:
            if he.code == 304:  # Not Modified — feed unchanged, not a real error
                return None
            raise
        except ssl.SSLCertVerificationError:
            # Retry without cert verification for feeds with local CA issues
            with urllib.request.urlopen(req, timeout=10, context=_SSL_NOVERIFY) as r:
                raw = r.read()
        if not raw:
            return None
        root  = ET.fromstring(raw)
        items = root.findall(".//item") or root.findall(f".//{{{NS['atom']}}}entry")
        if not items:
            return None
        item = items[0]

        def get(*tags):
            for tag in tags:
                el = item.find(tag)
                if el is not None and el.text:
                    return el.text.strip()
                for ns_uri in NS.values():
                    el = item.find(f"{{{ns_uri}}}{tag.split(':')[-1]}")
                    if el is not None and el.text:
                        return el.text.strip()
            return ""

        title       = strip_html(get("title"))
        url         = get("link", "guid")
        description = strip_html(get("description", "summary"))
        published   = get("pubDate", "published", "dc:date", "updated")
        if len(description) > 300:
            description = description[:300] + "..."

        image = ""
        for tag, attr in [("media:thumbnail","url"),("media:content","url"),("enclosure","url")]:
            ns_uri = NS.get(tag.split(":")[0], "")
            short  = tag.split(":")[-1]
            el     = item.find(f"{{{ns_uri}}}{short}") if ns_uri else item.find(short)
            if el is not None:
                val = el.get(attr, "")
                if val and any(x in val.lower() for x in ["jpg","jpeg","png","webp"]):
                    image = val; break

        if not title:
            return None
        return {"title":title, "description":description, "url":url,
                "image":image, "publishedAt":published, "source_name":source["name"]}
    except Exception as e:
        print(f"  x RSS [{source['id']}]: {e}")
        return None

def fetch_one(source):
    return fetch_rss(source)

def fetch_all_sources():
    now = datetime.now(timezone.utc).strftime("%H:%M")
    print(f"\n  [news] Fetch started {now} UTC ({len(SOURCES)} sources)")
    saved = ok = failed = 0

    def do_fetch(source):
        return source, fetch_one(source)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(do_fetch, s): s for s in SOURCES}
        for future in as_completed(futures):
            source, article = future.result()
            if article:
                if db.save_article(source["id"], source["category"], article):
                    saved += 1
                    ok += 1
            else:
                failed += 1

    stats = db.get_stats()
    print(f"  [news] Done — {ok} fetched, {saved} new, {failed} failed | total:{stats['total']} today:{stats['today']}")

def background_fetcher():
    fetch_all_sources()
    while True:
        time.sleep(INTERVAL)
        fetch_all_sources()


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        path = args[0].split()[1] if args else ""
        if path.startswith("/api"):
            print(f"  -> {path}")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        # ── NEWS ─────────────────────────────────────────────────────────────

        if parsed.path == "/api/headlines":
            sid = params.get("id", params.get("country", [None]))[0]
            row = db.get_latest_by_id(sid) if sid else None
            self._json(200 if row else 404, row or {"error": "no_data"})
            return

        if parsed.path == "/api/history":
            sid  = params.get("id", params.get("country", [None]))[0]
            rows = db.get_today(sid) if sid else []
            self._json(200, {"articles": rows, "count": len(rows)})
            return

        if parsed.path == "/api/all":
            result = {}
            for source in SOURCES:
                row = db.get_latest_by_id(source["id"])
                if row:
                    result[source["id"]] = row
            self._json(200, result)
            return

        if parsed.path == "/api/sources":
            safe = [{k: v for k, v in s.items() if k != "rss"} for s in SOURCES]
            self._json(200, safe)
            return

        if parsed.path == "/api/stats":
            self._json(200, db.get_stats())
            return

        # ── MARKET ───────────────────────────────────────────────────────────

        if parsed.path == "/api/market":
            cache   = market_data.get_cache()
            summary = {}
            for rid, data in cache.items():
                eq = data.get("equity", {})
                fx = data.get("fx", {})
                summary[rid] = {
                    "id":             rid,
                    "label":          data["label"],
                    "lat":            data["lat"],
                    "lon":            data["lon"],
                    "cities":         data["cities"],
                    "anomaly_score":  data["anomaly_score"],
                    "has_break":      data["has_break"],
                    "break_severity": data["break_severity"],
                    "corr_breaks":    data["corr_breaks"],
                    "updated":        data["updated"],
                    "eq_name":        eq.get("name"),
                    "eq_pct":         eq.get("pct_change"),
                    "eq_ok":          eq.get("ok", False),
                    "eq_close":       eq.get("close"),
                    "eq_atr_mul":     eq.get("atr_multiple"),
                    "eq_vol_rat":     eq.get("vol_ratio"),
                    "fx_name":        fx.get("name"),
                    "fx_pct":         fx.get("pct_change"),
                    "fx_ok":          fx.get("ok", False),
                }
            self._json(200, summary)
            return

        if parsed.path.startswith("/api/market/"):
            rid  = parsed.path[len("/api/market/"):]
            data = market_data.get_region(rid)
            self._json(200 if data else 404, data or {"error": "not_found", "id": rid})
            return

        # ── STATIC ───────────────────────────────────────────────────────────

        fp = (PUBLIC / "index.html" if parsed.path in ("/", "")
              else PUBLIC / parsed.path.lstrip("/"))
        if fp.exists() and fp.is_file():
            ext  = fp.suffix.lower()
            mime = {"html":"text/html","css":"text/css","js":"application/javascript"}.get(ext[1:],"text/plain")
            self._respond(200, fp.read_bytes(), mime)
        else:
            self._respond(404, b"Not found", "text/plain")

    def _json(self, code, data):
        self._respond(code, json.dumps(data, default=str).encode(), "application/json")

    def _respond(self, code, body, ct):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    db.init_db()
    threading.Thread(target=background_fetcher,                    daemon=True).start()
    threading.Thread(target=market_data.background_market_fetcher, daemon=True).start()
    print(f"\n  TERRANEWS at http://localhost:{PORT}")
    print(f"  {len(SOURCES)} news sources | RSS primary | news/{INTERVAL//60}min | market/5min\n")
    try:
        http.server.HTTPServer(("", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
