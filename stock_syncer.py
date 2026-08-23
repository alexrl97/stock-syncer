import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import time
import requests
import re
from urllib.parse import urlencode, quote

# Basisverzeichnis dieses Skripts -> State-Dateien (held_positions, Dedup) IMMER
# absolut hier ablegen. Sonst schreibt/liest ein Scheduler mit anderem Arbeits-
# verzeichnis (z.B. System32) ins Leere -> Dedup greift nie -> Signal-Spam.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# QQQM-Signal-Dedup (ausgelagert + testbar). WICHTIG: qqqm_last_signal.txt muss vom
# CI-Workflow zurueck-committet werden, sonst greift der Dedup ueber Laeufe hinweg nicht.
from qqqm_signal_state import (
    save_last_signal as _qqqm_save_last_signal,
    should_send as _qqqm_should_send,
    update_exit_state as _qqqm_update_exit_state,
    exit_cooldown_active as _qqqm_exit_cooldown_active,
    trading_days_between as _qqqm_trading_days_between,
    EXIT_COOLDOWN_TRADING_DAYS as _QQQM_EXIT_COOLDOWN,
)

# --- KONFIGURATION ---
TICKERS = [
    "NVDA", "GOOG", "GOOGL", "AAPL", "MSFT", "AMZN", "AVGO", "META", "TSLA", "TSM",
    "ASML", "COST", "PLTR", "NFLX", "MU", "AMD", "ADBE", "CSCO", "AZN", "LRCX",
    "AMAT", "INTC", "TMUS", "KLAC", "LIN", "SHOP", "PEP", "APP", "ISRG", "AMGN",
    "TXN", "QCOM", "BKNG", "GILD", "INTU", "PDD", "ADI", "HON", "PANW", "CRWD",
    "ARM", "VRTX", "CEG", "SBUX", "ADP", "MELI", "CMCSA", "SNPS", "DASH", "CDNS",
    "MAR", "PYPL", "WDAY", "CTAS", "ORLY", "FTNT", "PCAR", "MNST", "CPRT", "DXCM",
    "IDXX", "ROST", "PAYX", "MRVL", "MSTR", "ABNB", "GEHC", "ODFL", "LULU", "VRSK",
    "FAST", "DDOG", "TEAM", "BKR", "CSX", "ON", "COIN", "ZS", "MCHP", "BIIB",
    "EBAY", "MRNA", "DLTR", "KDP", "KHC", "EXC", "XEL", "MPWR", "ALGN",
    "OKTA", "MDB", "VRSN", "ROKU", "PODD", "CHTR", "UAL", "AAL", "WBD", "ILMN",
    "EUR=X",
    "SPY",      # SPDR S&P 500 (Der liquideste ETF der Welt)
    "QQQM",     # Nasdaq 100 (Günstigere "Buy & Hold" Version von QQQ)
    "IWM",      # iShares Russell 2000 (Small Caps - Wichtig für Marktbreite)
    "DIA",      # Dow Jones Industrial Average (Die "Old Economy")
    "VEU",      # Vanguard FTSE All-World ex-US (Alles außer USA)
    "SMH",      # VanEck Semiconductor (Der wichtigste Chip-Index für NVIDIA & Co.)
    "SH",       # ProShares Short S&P 500 (Direkter Invers-ETF auf den S&P)
    "GLD",      # SPDR Gold Shares (Der Gold-Standard an der Börse)
    "URTH",      # MSCI World
    "EEM",      # iShares MSCI Emerging Markets (Schwellenländer: China, Indien, etc.)
    "FEZ"       # SPDR Euro Stoxx 50 (Die 50 Top-Werte der Eurozone)
]

# Credentials kommen AUSSCHLIESSLICH aus der Umgebung (GitHub-Actions-Secrets bzw. lokal
# exportiert) — dieses Repo ist PUBLIC, hier darf nie ein Klartext-Secret stehen.
# Fail-hard bei fehlender Variable (KeyError), damit ein Fehlkonfig sofort auffaellt.
DATABASE_URL = os.environ["DATABASE_URL"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]

# --- ETF Bot Konfiguration ---
ETF_TELEGRAM_TOKEN = os.environ["ETF_TELEGRAM_TOKEN"]
ETF_CHAT_ID = os.environ.get("ETF_CHAT_ID", CHAT_ID)


# --- FUNKTIONEN ---

def load_held_positions(filepath=None):
    """Lädt die Liste der gehaltenen Ticker aus einem Textfile.
    Gibt eine Liste von Tickern zurück (z.B. ['ADBE.US', 'GLD.US']).
    Leere Zeilen und Kommentare (#) werden ignoriert.
    """
    if filepath is None:
        filepath = os.path.join(_SCRIPT_DIR, "held_positions.txt")
    if not os.path.exists(filepath):
        print(f"[!] {filepath} nicht gefunden - kein Filter aktiv.")
        return []

    with open(filepath, 'r') as f:
        tickers = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith('#')
        ]
    print(f"[*] {len(tickers)} gehaltene Positionen geladen: {tickers}")
    return tickers


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Fehler beim Telegram-Versand: {e}")


def send_telegram_document(content, filename, caption=None):
    """Sendet einen Text-String als .txt-Datei (Dokument) an Telegram.
    So lassen sich die Zertifikat-Infos bequem aus dem Chat kopieren."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    files = {"document": (filename, content.encode("utf-8"), "text/plain")}
    data = {"chat_id": CHAT_ID}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    try:
        response = requests.post(url, data=data, files=files)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Fehler beim Telegram-Dokument-Versand: {e}")


# ============================================================================
# ZERTIFIKATE-SUCHE (Long-Knock-Outs, mit und ohne SL-Funktion)
# ----------------------------------------------------------------------------
# Python-Port von gettex_search.js. Datenquelle: Hebelprodukt-Suche von
# finanzen.net (gettex-Kurse/-Spreads, Emittenten GS/HSBC/UniCredit).
# Pro Aktien-Signal werden die 5 Long-KO (Call) mit dem niedrigsten
# prozentualen Spread gesucht und als Text-Datei an Telegram gesendet.
# ============================================================================

ZERT_SEARCH_URL = "https://www.finanzen.net/ajax/DerivativesControllerSearch"
ZERT_UNDERLYING_URL = "https://www.finanzen.net/ajax/UnderlyingsByInput"

# Voller Browser-Header-Satz ist Pflicht — die Akamai-WAF blockt sonst (403).
# Zusätzlich braucht es eine Session mit dem "Bot-Information"-Cookie, das beim
# ersten Aufruf der Such-Seite gesetzt wird (siehe _zert_get_session).
ZERT_HEADERS = {
    "accept": "*/*",
    "accept-language": "de-DE,de;q=0.9",
    "accept-encoding": "gzip, deflate, br",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://www.finanzen.net/zertifikate/suche",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

ZERT_WARMUP_URL = "https://www.finanzen.net/zertifikate/suche"
_zert_session = None

# Filter (finanzen.net-Filtersprache, <feld>_<operator>=<wert>):
ZERT_FILTER = {
    "parentderivativetypeid_equals": "7",            # 7 = Knock-Outs
    "derivativesubtypeid_equals": "1",               # 1 = Long (Call)
    # hasstoploss-Filter entfernt: Scheine mit UND ohne SL-Funktion werden gesucht
    # Emittenten (Company-IDs): Goldman Sachs, HSBC, UniCredit (BNP Paribas entfernt)
    "issuercompanyid_inlistofint": "8525,14,22",
    # Hebel-Band — WICHTIG: Dezimal-KOMMA (deutsche Notation)
    "leverage_lowerbound": "3",
    "leverage_upperbound": "3,3",
    "dynamickeys": ",spreadpercent,",                 # blendet die Spread-%-Spalte ein
    # serverseitig nach Spread aufsteigend sortieren
    "orderby": "Spreadpercent:asc",
}

ZERT_MAX_TREFFER = 5     # die fünf mit dem niedrigsten Spread
ZERT_TIMEOUT = 20        # Sekunden
ZERT_SICHERHEIT = 0.003  # 0,3 % Sicherheitsabstand (auf Ask)


def _zert_get_session():
    """Liefert eine Session mit dem von der WAF gesetzten 'Bot-Information'-Cookie.
    Der Warm-up-Aufruf antwortet selbst mit 403, setzt aber das nötige Cookie."""
    global _zert_session
    if _zert_session is None:
        s = requests.Session()
        s.headers.update(ZERT_HEADERS)
        try:
            s.get(ZERT_WARMUP_URL, timeout=ZERT_TIMEOUT)  # 403 erwartet, setzt Cookie
        except Exception:
            pass
        _zert_session = s
    return _zert_session


def _zert_hole_html(url):
    res = _zert_get_session().get(url, timeout=ZERT_TIMEOUT)
    txt = res.text
    if not res.ok or re.search(r"Access Denied", txt, re.I):
        raise RuntimeError(f"Abruf fehlgeschlagen (HTTP {res.status_code}) für {url}")
    return txt


def _zert_zahl(s):
    """Deutsche Zahl ("1.234,56") -> float; None bei leer/ungültig."""
    if not s:
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def _zert_strip_tags(s):
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _zert_underlying_id(eingabe):
    """Löst einen Basiswert-Namen/ISIN in die finanzen.net-Underlying-ID auf.
    Rein numerische Eingaben werden direkt verwendet. Der Resolver ist
    case-sensitiv und mag keine Rechtsform-Zusätze ("Broadcom Inc." findet
    nichts, "Broadcom" schon), daher werden mehrere Schreibweisen UND eine
    um gängige Zusätze bereinigte Variante probiert."""
    eingabe = str(eingabe).strip()
    if re.fullmatch(r"\d+", eingabe):
        return eingabe

    # gängige Rechtsform-/Namenszusätze entfernen
    bereinigt = re.sub(
        r"\b(Corporation|Corp\.?|Inc\.?|Incorporated|Platforms?|Company|Co\.?|"
        r"Ltd\.?|PLC|Holdings?|Group|The)\b",
        "", eingabe, flags=re.I,
    )
    bereinigt = re.sub(r"\s+", " ", bereinigt).strip(" .,")

    varianten = []
    for basis in [eingabe, bereinigt]:
        if not basis:
            continue
        title_case = re.sub(r"\b\w", lambda mm: mm.group(0).upper(), basis.lower())
        varianten += [basis, title_case, basis.lower(), basis.upper()]
    varianten = list(dict.fromkeys(v for v in varianten if v))

    for v in varianten:
        html = _zert_hole_html(f"{ZERT_UNDERLYING_URL}?input={quote(v)}")
        m = re.search(r'value="(\d+)"', html)
        if m:
            return m.group(1)
    raise RuntimeError(f'Kein Basiswert für "{eingabe}" gefunden')


def _zert_parse(html):
    """Parst die Ergebnistabelle (14 Zellen je Datenzeile):
    0 Emittent · 1 WKN · 2 Bid · 3 Ask · 4 — · 5 Fälligkeit · 6 Basispreis ·
    7 Knock-Out · 8 Hebel · 9 Spread% · 10 Spread hom. · 11 Basiswert ·
    12 Art · 13 Typ"""
    scheine = []
    gesehen = set()
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
        if len(cells) < 14:
            continue  # Werbe-/Kopfzeilen überspringen
        c = [_zert_strip_tags(x) for x in cells]
        wkn = c[1]
        if not wkn or wkn in gesehen:
            continue  # Mobil-/Desktop-Duplikate filtern
        gesehen.add(wkn)

        m_emit = re.search(r'title="([^"]+)"', row_html)
        emittent = m_emit.group(1) if m_emit else "-"
        m_isin = re.search(r"DE[A-Z0-9]{10}", row_html)
        isin = m_isin.group(0) if m_isin else None

        scheine.append({
            "emittent": emittent,
            "wkn": wkn,
            "isin": isin,
            "bid": _zert_zahl(c[2]),
            "ask": _zert_zahl(c[3]),
            "faelligkeit": c[5] or "-",
            "basispreis": _zert_zahl(c[6]),
            "knockout": _zert_zahl(c[7]),
            "hebel": _zert_zahl(c[8]),
            "spread_pct": _zert_zahl(c[9]),
            "basiswert": c[11],
            "typ": c[13],
        })
    return scheine


def _zert_euro_abstand(s, tsl_pct):
    """Euro-Abstandsorder auf Ask-Basis (variabler Hebel -> keine prozentuale TSL):
        Abstand_€      = Ask × (TSL% × Hebel) + (Ask − Bid) + Ask × 0,3%
        Initialer Stop = Ask − Abstand_€
    Gibt (initialer_stop, abstand_euro) zurück (auf 3 Nachkommastellen)."""
    ask, bid, hebel = s["ask"], s["bid"], s["hebel"]
    if ask is None or bid is None or hebel is None:
        return None, None
    spread_eur = ask - bid
    abstand = ask * (tsl_pct * hebel) + spread_eur + ask * ZERT_SICHERHEIT
    init_stop = ask - abstand
    return round(init_stop, 3), round(abstand, 3)


def _zert_text(scheine, basiswert_name, tsl_pct):
    """Baut den (zuvor als Konsolen-Output gedachten) Text für die .txt-Datei."""
    def fmt(v, e=""):
        return "-" if v is None else f"{v}{e}"

    lines = [
        "=" * 50,
        f"Long-Knock-Outs (Call) — {basiswert_name}",
        f"Hebel {ZERT_FILTER['leverage_lowerbound']}-{ZERT_FILTER['leverage_upperbound']} "
        f"| verwendete TSL: {tsl_pct * 100:.1f}% | Emittenten: GS, HSBC, UniCredit",
        "=" * 50,
    ]
    for i, s in enumerate(scheine, 1):
        init_stop, abstand = _zert_euro_abstand(s, tsl_pct)
        lines += [
            "-" * 40,
            f"#{i}  Spread: {fmt(s['spread_pct'], ' %')}",
            f"Emittent:           {s['emittent']}",
            f"WKN / ISIN:         {s['wkn']} / {s['isin'] or '-'}",
            f"Typ:                {s['typ']} ({s['basiswert']})",
            f"Hebel:              {fmt(s['hebel'])}",
            f"Knock-Out / Basisp: {fmt(s['knockout'])} / {fmt(s['basispreis'])}",
            f"Fälligkeit:         {s['faelligkeit']}",
            f"Bid / Ask:          {fmt(s['bid'])} / {fmt(s['ask'])} EUR",
            f"Initialer Stop:     {fmt(init_stop, ' EUR')}",
            f"Abstand in Euro:    {fmt(abstand, ' EUR')}",
        ]
    return "\n".join(lines)


def fetch_and_send_zertifikate(basiswert_name, tsl_pct, ticker_clean, isin=None):
    """Sucht Long-KO-Zertifikate zum Basiswert und sendet die Top-5 (niedrigster
    Spread) als Text-Datei an Telegram — inkl. Euro-Abstandsorder pro Schein.

    Auflösung des Basiswerts: ISIN zuerst (eindeutig und zuverlässiger), dann
    der Name als Fallback. Schlägt beides fehl, wird das Signal NICHT still
    verschluckt, sondern per Telegram-Hinweis gemeldet."""
    try:
        underlying_id = None
        for kandidat in [isin, basiswert_name]:
            if not kandidat:
                continue
            try:
                underlying_id = _zert_underlying_id(kandidat)
                break
            except Exception:
                continue

        if underlying_id is None:
            print(f"  [Zert] Kein Basiswert auf finanzen.net für {ticker_clean} "
                  f"({basiswert_name} / {isin}) gefunden.")
            send_telegram_message(
                f"⚠️ Kein Basiswert auf finanzen.net für <b>{ticker_clean}</b> "
                f"({basiswert_name}) gefunden — keine Zertifikate."
            )
            return

        params = dict(ZERT_FILTER)
        params["underlyingids_inlistofint"] = underlying_id
        html = _zert_hole_html(f"{ZERT_SEARCH_URL}?{urlencode(params)}")

        alle = _zert_parse(html)
        auswahl = sorted(
            [s for s in alle if s["spread_pct"] is not None],
            key=lambda s: s["spread_pct"],
        )[:ZERT_MAX_TREFFER]

        if not auswahl:
            print(f"  [Zert] Keine Zertifikate für {basiswert_name} gefunden.")
            send_telegram_message(
                f"ℹ️ Keine Long-KO-Zertifikate (Hebel "
                f"{ZERT_FILTER['leverage_lowerbound']}–{ZERT_FILTER['leverage_upperbound']}) "
                f"für <b>{ticker_clean}</b> gefunden."
            )
            return

        text_content = _zert_text(auswahl, basiswert_name, tsl_pct)
        filename = f"zertifikate_{ticker_clean}.txt"
        caption = (
            f"📄 <b>{ticker_clean}</b> — {len(auswahl)} Long-KO mit niedrigstem Spread "
            f"(TSL {tsl_pct * 100:.1f}%)"
        )
        send_telegram_document(text_content, filename, caption=caption)
        print(f"  [Zert] {len(auswahl)} Zertifikate für {ticker_clean} als Datei gesendet.")

    except Exception as e:
        print(f"  [Zert] Fehler bei Zertifikate-Suche für {basiswert_name}: {e}")


def sync_ohlcv_to_neon():
    print(f"\n{'=' * 60}")
    # Basis-Zeit in UTC als Pandas Timestamp (verhindert dtype-Fehler)
    start_now_utc = pd.Timestamp.now(tz='UTC').replace(microsecond=0)
    print(f"[{start_now_utc.strftime('%H:%M:%S')}] START SYNC (UTC Mode)")

    # Ziel: Die letzte 15-Min-Kerze (z.B. 16:35 -> 16:15)
    # Wir runden ab auf das 15-Min-Intervall und gehen 15 Min zurück
    current_slot = start_now_utc.floor('15min')
    target_pd_time = (current_slot - pd.Timedelta(minutes=15)).tz_localize(None)

    print(f"[*] Suche Ziel-Timestamp: {target_pd_time}")
    print(f"{'=' * 60}")

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    remaining_tickers = list(TICKERS.copy())
    total_initial = len(remaining_tickers)

    attempt = 1
    while attempt <= 5 and remaining_tickers:
        print(f"\n--- Versuch {attempt} ({len(remaining_tickers)} Ticker verbleibend) ---")

        try:
            # Download der Daten
            data = yf.download(remaining_tickers, period="1d", interval="15m", group_by='ticker', progress=False)

            current_round_successes = []

            for ticker in list(remaining_tickers):
                try:
                    # 1. Ticker-Daten extrahieren
                    if len(remaining_tickers) > 1:
                        if ticker not in data.columns.levels[0]: continue
                        df = data[ticker].dropna(how='all')
                    else:
                        df = data.dropna(how='all')

                    if df.empty: continue

                    # 2. Index-Konvertierung (Wichtig für den Vergleich!)
                    # Wir wandeln den Index in naive Timestamps um (ohne Zeitzone)
                    df.index = pd.to_datetime(df.index).tz_convert('UTC').tz_localize(None)

                    # 3. Den exakten oder den nächstgelegenen verfügbaren Timestamp finden
                    # Da Yahoo manchmal 16:15 überspringt und 16:30 zeigt, nehmen wir die
                    # letzte Kerze, die NICHT in der Zukunft liegt.
                    valid_candles = df[df.index <= target_pd_time]

                    if valid_candles.empty:
                        if ticker == remaining_tickers[0]:
                            print(f"  [-] {ticker}: Keine Kerzen bis {target_pd_time} gefunden. Letzte: {df.index[-1]}")
                        continue

                    # Wir nehmen die aktuellste verfügbare Kerze aus den validen Daten
                    actual_time = valid_candles.index[-1]
                    candle = valid_candles.iloc[-1]

                    # 4. Einzel-Insert in die Datenbank
                    row_data = {
                        'ticker': ticker + ".US" if "=" not in ticker else ticker,
                        'market_time': actual_time,
                        'market_date': actual_time.date(),
                        'open': round(float(candle['Open']), 4),
                        'high': round(float(candle['High']), 4),
                        'low': round(float(candle['Low']), 4),
                        'close': round(float(candle['Close']), 4),
                        'volume': int(candle['Volume'])
                    }

                    # Erst der erfolgreiche DB-Commit triggert den Erfolg für diesen Ticker
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO trading.live_quotes (ticker, market_time, market_date, open, high, low, close, volume)
                            VALUES (:ticker, :market_time, :market_date, :open, :high, :low, :close, :volume)
                            ON CONFLICT (ticker, market_time) 
                            DO UPDATE SET 
                                open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
                                close=EXCLUDED.close, volume=EXCLUDED.volume;
                        """), row_data)

                    current_round_successes.append(ticker)
                    print(f"  [OK] {ticker} ({actual_time.strftime('%H:%M')})")

                except Exception as e:
                    print(f"  [ERR] {ticker}: {str(e)}")
                    continue

            # Erfolgreiche Ticker entfernen
            for t in current_round_successes:
                remaining_tickers.remove(t)

        except Exception as e:
            print(f"  [FATAL] Fehler beim Download/Processing: {e}")

        if remaining_tickers:
            time.sleep(15)
        attempt += 1

    print(f"\n{'=' * 60}")
    print(f"SYNC FERTIG: {total_initial - len(remaining_tickers)}/{total_initial} erfolgreich.")
    if remaining_tickers:
        print(f"Fehlgeschlagen: {remaining_tickers}")
    print(f"{'=' * 60}")


def fetch_and_notify():
    """Schritt 2: Analyse durchführen und Nachricht versenden"""
    engine = create_engine(DATABASE_URL)

    # Gehaltene Positionen laden (werden vom Signal ausgeschlossen)
    held_tickers = load_held_positions()

    # HIER DEINE KOMPLETTE QUERY EINFÜGEN
    query = text("""
WITH params AS (   -- dynamischer Entry-Dip (run-up-skaliert), identisch zu raw_signals_dyn_entry
    SELECT 1.2::numeric  AS dip_base,    -- Basis-Dip in % (nicht überdehnte Aktie)
           0.20::numeric AS k_ext,       -- Aufschlag pro % Überdehnung über SMA50
           1.0::numeric  AS min_dip_pct,
           8.0::numeric  AS max_dip_pct
),

source as (SELECT ticker
                     , market_time at time zone 'America/New_York'                                    as market_time
                     , market_date
                     , open
                     , high
                     , low
                     , close
                     , close - open                                                                   AS dif
                     , volume
                FROM trading.live_quotes a),

sources_rowed as (SELECT

                      *
                       , coalesce(close - LAG(close) OVER (PARTITION BY ticker, market_date ORDER BY market_time), 0)             AS dif_to_prior_close
                       , row_number() over (partition by ticker, market_date ORDER BY market_time)      as day_row
                       , row_number() over (partition by ticker, market_date ORDER BY market_time DESC) as day_row_desc
                       , AVG(volume) OVER (
        PARTITION BY ticker, to_char(market_time, 'HH24:MI')
        ORDER BY market_date
        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        )
                                                                                                        as avg_vol_specific_time_10d
                  FROM source),

daily_stats AS (
    SELECT
        ticker,
        market_date,
        MAX(close) FILTER (WHERE day_row_desc = 1) as daily_close,
        MAX(open) FILTER (WHERE day_row = 1) as daily_open,
        MAX(high)                                 as daily_high,
        MIN(low)                                  as daily_low
    FROM sources_rowed d
    GROUP BY ticker, market_date
),
daily_stats_prev as (
    SELECT *, lag(market_date) over (partition by ticker order by market_date DESC) as next_market_date
    FROM daily_stats
),

-- 50-Tage-Schnitt der Tagesschlüsse (Überdehnungsmaß für den dynamischen Entry-Dip)
daily_sma50 as (
    SELECT ticker, market_date,
           AVG(daily_close) OVER (PARTITION BY ticker ORDER BY market_date
                                  ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS sma50
    FROM daily_stats
),

-- ===== DYNAMISCHE TSL: Vola der letzten 20 Handelstage (Body + Range) =========
-- Tageskennzahlen: Body (Close-Open) und Intraday-Range (High-Low), je in %
daily_vola as (
    SELECT ticker, market_date,
           (daily_close - daily_open) / NULLIF(daily_open, 0) * 100 AS day_body_pct,
           (daily_high  - daily_low ) / NULLIF(daily_open, 0) * 100 AS day_range_pct
    FROM daily_stats
),
-- rollierendes 20-Tage-Fenster (nur abgeschlossene Vortage -> kein Lookahead)
vola_20d as (
    SELECT *,
           STDDEV_SAMP(day_body_pct) OVER w AS vola_body_20d,
           AVG(ABS(day_body_pct))    OVER w AS avg_abs_body_20d,
           AVG(day_range_pct)        OVER w AS avg_range_20d,
           COUNT(*)                  OVER w AS days_in_window
    FROM daily_vola
    WINDOW w AS (PARTITION BY ticker ORDER BY market_date
                 ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
),
-- dynamischer Trailing-Abstand (1 Nachkommastelle). Gewichte wie im Build:
-- k_atr=1.0, k_vola=2.0, k_range=0.5, Clamp [4%,25%]
dynamic_sl as (
    SELECT ticker, market_date, days_in_window,
           ROUND(LEAST(GREATEST(
                     1.0 * avg_abs_body_20d
                   + 2.0 * COALESCE(vola_body_20d, 0)
                   + 0.5 * COALESCE(avg_range_20d, 0), 4.0), 25.0)::numeric, 1) AS dyn_drop_pct
    FROM vola_20d
),

day_open as (
    SELECT a.*, b.daily_open, c.daily_close as prev_day_close
    FROM sources_rowed a
    left join daily_stats b
    ON a.ticker = b.ticker AND a.market_date = b.market_date
    LEFT JOIN daily_stats_prev c
    ON a.ticker = c.ticker AND a.market_date = c.next_market_date
),

prev_movements as (SELECT *

                        , LAG(dif, 1) OVER (partition by ticker,market_date ORDER BY market_time)                       AS close_dif_15_min_ago
                        , LAG(dif, 2) OVER (partition by ticker,market_date ORDER BY market_time)                       AS close_dif_30_min_ago
                        , LAG(close, 1) OVER (partition by ticker,market_date ORDER BY market_time)                     AS close_15_min_ago
                        , LAG(close, 2) OVER (partition by ticker,market_date ORDER BY market_time)                     AS close_30_min_ago
                        , LAG(close, 3) OVER (partition by ticker,market_date ORDER BY market_time)                     AS close_45_min_ago
                        , coalesce(LAG(open, 10) OVER (partition by ticker,market_date ORDER BY market_time), daily_open) AS open_2_30_h_ago
                        , LAG(volume, 1) OVER (partition by ticker,market_date ORDER BY market_time)                       AS volume_15_min_ago
                        , LAG(volume, 2) OVER (partition by ticker,market_date ORDER BY market_time)                       AS volume_30_min_ago
                        , LAG(avg_vol_specific_time_10d, 1) OVER (partition by ticker,market_date ORDER BY market_time)                       AS avg_vol_specific_time_10d_15_min_ago
                        , LAG(avg_vol_specific_time_10d, 2) OVER (partition by ticker,market_date ORDER BY market_time)                       AS avg_vol_specific_time_10d_30_min_ago

                   FROM day_open
                   ORDER BY market_time),

difs_to_prev as (SELECT *

                      , coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - open_2_30_h_ago as dif_prev_to_2h_before
                      , (coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - open_2_30_h_ago) / open_2_30_h_ago as dif_prev_to_2h_before_pct
                      , coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - daily_open        as dif_prev_to_day_open
                      , (coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - daily_open)/ daily_open        as dif_prev_to_day_open_pct
                                            , coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - prev_day_close        as dif_prev_to_prev_day_close
                      , (coalesce(close_45_min_ago, close_30_min_ago, close_15_min_ago, close) - prev_day_close)/ prev_day_close        as dif_prev_to_day_prev_day_close_pct
                 FROM prev_movements),

-- DYNAMISCHER ENTRY-DIP (run-up-skaliert über SMA50) statt Konstante aus entry_and_sl
entry_calc as (SELECT a.*
                    , c.sql_factor::numeric AS stop_loss_pct
                    , c.sector, c.name, c.isin
                    , GREATEST(0, (a.prev_day_close / NULLIF(s.sma50,0) - 1) * 100)                          AS ext_pct
                    , (-LEAST(GREATEST(p.dip_base + p.k_ext * GREATEST(0, (a.prev_day_close / NULLIF(s.sma50,0) - 1) * 100),
                              p.min_dip_pct), p.max_dip_pct))                                                AS dyn_entry_dip_pct
               FROM difs_to_prev a
                    LEFT JOIN trading.entry_and_sl c ON a.ticker = c.ticker
                    LEFT JOIN daily_sma50 s          ON s.ticker = a.ticker AND s.market_date = a.market_date
                    CROSS JOIN params p),

entry_points_prep as (SELECT *
                           , round((dyn_entry_dip_pct / 100)::numeric, 4)                                    as entry_dip_pct
                           , row_number() over (partition by ticker, market_date order by market_time)        as row
                      FROM entry_calc
                      WHERE (dif_prev_to_day_open_pct            <= dyn_entry_dip_pct / 100 OR
                             dif_prev_to_2h_before_pct           <= dyn_entry_dip_pct / 100 OR
                             dif_prev_to_day_prev_day_close_pct  <= dyn_entry_dip_pct / 100 * 1.5)
                        AND (dif > 0 AND close_dif_15_min_ago > 0 AND close_dif_30_min_ago > 0)),

entry_points as (

    SELECT *
    FROM entry_points_prep
    WHERE row = 1
),

start_price as (

    SELECT a.*
         , a.close as start_price
         , round((a.close * stop_loss_pct)::numeric, 2) as stop_loss
         , stop_loss_pct as trailing_stop_loss_pct_by_sector
         , (a.volume + volume_15_min_ago + volume_30_min_ago) / (a.avg_vol_specific_time_10d + avg_vol_specific_time_10d_15_min_ago + avg_vol_specific_time_10d_30_min_ago) as vol_score

    FROM entry_points a
),

rsi_steps AS (
    SELECT
        *,
        -- 2. Gewinne und Verluste trennen
        CASE WHEN dif_to_prior_close > 0 THEN dif_to_prior_close ELSE 0 END AS gain,
        CASE WHEN dif_to_prior_close < 0 THEN ABS(dif_to_prior_close) ELSE 0 END AS loss
    FROM sources_rowed
),
rsi_final AS (
    SELECT
        *,
        -- 3. Durchschnittliche Gewinne/Verluste über 14 Perioden (einfacher gleitender Durchschnitt für den Start)
        AVG(gain) OVER (PARTITION BY ticker ORDER BY market_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY ticker ORDER BY market_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM rsi_steps
),
rsi_output AS (
    SELECT
        *,
        -- 4. RSI Formel: 100 - (100 / (1 + (avg_gain / avg_loss)))
        CASE
            WHEN avg_loss = 0 THEN 100
            ELSE ROUND(100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))::numeric, 2)
        END AS rsi
    FROM rsi_final
),

daily_sma AS (
    SELECT
        ticker,
        market_date,
        daily_close,
        AVG(daily_close) OVER (
            PARTITION BY ticker
            ORDER BY market_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) as sma_200
    FROM daily_stats
),

nasdaq_source as (
        SELECT *
        FROM sources_rowed
        WHERE ticker = 'QQQM.US'
),

nasdaq_day_close as (
    SELECT market_date, close, lag(market_time) over (order by market_time DESC)::date as next_market_date
    FROM nasdaq_source
    WHERE day_row_desc = 1
),

usd_to_euro as (
    SELECT *
    --TODO: Change to live
    FROM source
        WHERE ticker = 'EUR=X'
),

start_filtered as (SELECT a.*, (b.close - c.close) / c.close as nasdaq_day_performance, sma_200, e.rsi, f.close as current_usd_to_euro
                        , g.dyn_drop_pct
                        -- dynamische TSL ersetzt die statische; Fallback auf statisch bei < 10 Tagen Historie
                        , COALESCE(
                              CASE WHEN g.days_in_window >= 10 THEN ROUND(1 - g.dyn_drop_pct / 100, 4) END,
                              a.stop_loss_pct
                          ) AS effective_stop_loss_pct
                                 FROM start_price a
                                          LEFT JOIN nasdaq_source b
                                                    ON a.market_time = b.market_time
                                          LEFT JOIN nasdaq_day_close c
                                                    ON a.market_time::date = c.next_market_date
                                          LEFT JOIN daily_sma d
                                                    ON a.market_time::date = d.market_date AND a.ticker = d.ticker
                                          LEFT JOIN rsi_output e
                                                    ON a.market_time = e.market_time AND a.ticker = e.ticker
                                          LEFT JOIN usd_to_euro f
                                                    ON a.market_time = f.market_time
                                          LEFT JOIN dynamic_sl g
                                                    ON a.ticker = g.ticker AND a.market_date = g.market_date

                                               WHERE
              rsi BETWEEN 40 AND 79
              AND (b.close - c.close) / c.close > -0.035
              AND (b.close - c.close) / c.close NOT BETWEEN -0.005 AND 0
              AND (b.close - c.close) / c.close NOT BETWEEN 0.005 AND 0.015
              AND vol_score > 0.3 AND vol_score < 1.4
              AND (start_price / sma_200 - 1) > -0.4
              AND (start_price / sma_200 - 1) NOT BETWEEN -0.09 AND 0.15
              AND (start_price / sma_200 - 1) NOT BETWEEN 0.5 AND 0.6
              AND (start_price / sma_200 - 1) < 1.5
              AND sector NOT IN (
                    'Utilities',
                    'Railroads',
                    'Platform/Travel',
                    'Communication',
                    'Industrial/Services',
                    'Biotech/High-Vola',
                    'Platform/Delivery',
                    'Business Services',
                    'Industrial/Automotive',
                    'Services',
                    'MedTech/Growth',
                    'Industrial/Logistics',
                    'E-Commerce',
                    'Industrial/Distribution',
                    'Retail',
                    'E-Commerce/High-Vola',
                    'Retail/Growth',
                    'Consumer Stable',
                    'MedTech',
                    'Energy/Services',
                    'Big Tech/Streaming'
                )

                                 ),

notification as (SELECT

                   ticker
                 , market_time::timestamp as market_time
                 , start_price as start_price_usd
                 , start_price *current_usd_to_euro as start_price_euro
                 , dyn_drop_pct
                 , 1-effective_stop_loss_pct as stop_loss_pct
                 , effective_stop_loss_pct * start_price *current_usd_to_euro as stop_loss_limit
                 , 500 + 500* (1-effective_stop_loss_pct) as minimum_free_trade
                 , entry_dip_pct
                 , isin
                 , sector
                 , name
                , nasdaq_day_performance
                , concat('https://www.google.com/search?q=https%3A%2F%2Fwww.finanzen-zero.net%2Faktien%2F',isin,'-aktie&oq=&gs_lcrp=EgZjaHJvbWUqCQgCEEUYOxjCAzIJCAAQRRg7GMIDMgkIARBFGDsYwgMyCQgCEEUYOxjCAzIJCAMQRRg7GMIDMgkIBBBFGDsYwgMyCQgFEEUYOxjCAzIJCAYQRRg7GMIDMgkIBxBFGDsYwgPSAQkzMTQ0ajBqMTWoAgiwAgE&sourceid=chrome&ie=UTF-8') as trading_link
                , concat('https://www.google.com/search?q=', replace(name, ' ', '+'), '+stock+news') as news_link
                 FROM start_filtered a
                 WHERE day_row_desc = 1 AND market_date = now()::date   -- zum Testen: "AND market_date = now()::date" entfernen
                 )

SELECT *
FROM notification
WHERE ticker != ALL(:held_tickers)
    """)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"held_tickers": held_tickers})

        if df.empty:
            print(f"[{pd.Timestamp.now()}] Analyse abgeschlossen: Keine Signale gefunden.")
            return

        message_blocks = [
            f"🎯 <b>Trading Signal ({len(df)})</b>",
            f"📅 <i>Stand: {df['market_time'].iloc[0]}</i>"
        ]

        isins_to_send = []

        for _, row in df.iterrows():
            ticker_clean = row['ticker'].split('.')[0]
            nasdaq_perf = row['nasdaq_day_performance'] * 100
            sl_pct_display = (row['stop_loss_pct']) * 100

            isins_to_send.append(row['isin'])

            # Neuer Block-Aufbau
            block = (
                f"🚀 <a href='{row['trading_link']}'><b>{ticker_clean}</b></a> ({row['name']})\n"
                f"├ {row['sector']}\n"
                f"├ Nasdaq: <code>{nasdaq_perf:.2f}%</code>\n"
                f"├ Preis: <b>${row['start_price_usd']:.2f}</b> ({row['start_price_euro']:.2f}€)\n"
                f"├ 🛡 SL Limit: <b>{row['stop_loss_limit']:.2f}€</b> (<code>-{sl_pct_display:.1f}%</code>)\n"
                f"└ 🔗 <a href='{row['news_link']}'>Google News</a>"
            )
            message_blocks.append(block)

        # Hauptnachricht senden
        full_message = "\n\n".join(message_blocks)
        send_telegram_message(full_message)

        # ISINs separat nachschicken für Kopierbarkeit
        if isins_to_send:
            isin_msg = "📑 <b>Kopierbare ISINs:</b>\n" + "\n".join([f"<code>{i}</code>" for i in isins_to_send])
            send_telegram_message(isin_msg)

        # Pro Signal Long-KO-Zertifikate suchen und als Text-Datei senden.
        # stop_loss_pct ist die TSL des Basiswerts als Dezimalwert (z.B. 0.05 = 5 %).
        for _, row in df.iterrows():
            ticker_clean = row['ticker'].split('.')[0]
            fetch_and_send_zertifikate(
                basiswert_name=row['name'],
                tsl_pct=float(row['stop_loss_pct']),
                ticker_clean=ticker_clean,
                isin=row['isin'],
            )

        print(f"Erfolg: {len(df)} Signale gesendet.")

    except Exception as e:
        print(f"Fehler bei Analyse/Versand: {e}")


# --- NEU: ETF Bot Funktionen ---

def send_etf_telegram_message(message):
    """Sendet eine Nachricht über den ETF-Bot."""
    url = f"https://api.telegram.org/bot{ETF_TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": ETF_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Fehler beim ETF-Telegram-Versand: {e}")


def fetch_etf_signals():
    """ETF-Signal-Analyse: Sucht Rebound-Einstiege in ETF-Daten und sendet Benachrichtigung."""
    engine = create_engine(DATABASE_URL)

    # Gehaltene Positionen laden (werden vom Signal ausgeschlossen)
    held_tickers = load_held_positions()

    query = text("""
/* ============================================================================
   ETF-NOTIFICATION  –  Strategie "Adaptive Trend-Rider"  (ersetzt Rebound-Logik)
   ============================================================================
   OUTPUT IDENTISCH zur alten Notification (für den Python-Austausch):
     ticker | market_time | price | entry | tsl | index | wkn | signal_day_row | ticker_short
   Bedeutung der Spalten jetzt:
     entry = dynamische Dip-Schwelle in %   (kd * Tagesrange-Schnitt, informativ)
     tsl   = dynamischer Trailing-Stop in % (m_used * Tagesrange-Schnitt + spread_pct_roundup)
             m_used = clamp(4 + 20*Trendglätte, 4, 12) -> in starken Trends BEWUSST
             weit (lässt Gewinner laufen; harte Obergrenze über m_max in params).

   SIGNAL-LOGIK (wie in 202606_etf_update.sql backgetestet):
     - Trendfilter: Kurs über dem 100-Tage-Schnitt der Tagesschlüsse (Vortagesbasis)
     - SH.US-Gate:  statt eigenem Trendfilter nur, wenn der SPY-Vortagesschluss
                    seit >= 5 Handelstagen IN FOLGE unter seinem 100-Tage-Schnitt liegt
     - Entry a) Dip-Bounce: Tief erreicht 20-Tage-Hoch*(1 - kd*Range), Schluss erobert zurück
             b) Breakout:   Schluss über dem bisherigen 20-Tage-Hoch
     - max. 1 Signal je Ticker/Tag (signal_day_row=1), Signal nur auf der
       aktuellsten Kerze des Tages (day_row_desc=1), nur heute, nicht gehalten.

   DATENBASIS: NUR trading.live_quotes. Damit alle Kennzahlen voll rechnen,
   müssen dort je ETF-Ticker mindestens ~101 Handelstage liegen (100-Tage-
   Schnitt + Trendglätte über 60 Tage). Hartes Minimum der Query: 60 Tage
   (nd >= 60), darunter feuert der Ticker nicht.

   PERFORMANCE (~2 s): CTEs sind bewusst MATERIALIZED — ohne das inlined
   Postgres sie und berechnet z.B. das SPY-Regime pro Kerze neu (gemessen 171 s).
   Tages-Kennzahlen kommen aus der vollen live_quotes-Historie (billig),
   Kerzen-Fenster nur aus den letzten 40 Tagen (mehr braucht das 20-Tage-Hoch nicht).

   RUECKWIRKEND TESTEN: im entry_points-WHERE MUESSEN die Zeilen
   "AND day_row_desc = 1", "AND market_date = now()::date" und
   "AND a.ticker != ALL(:held_tickers)" ALLE raus; stattdessen z.B.
   "AND market_date >= (now() - interval '10 days')::date".
   ACHTUNG: day_row_desc=1 ist der LIVE-Trigger ("Signal liegt auf der
   neuesten Kerze"). Bleibt er beim Testen drin, verlangt man rueckwirkend
   "erstes Tagessignal == 15:45-Schlusskerze" -> fast immer 0 Treffer.
   ============================================================================ */
WITH params AS (
    SELECT 1.5::numeric  AS kd,        -- Dip-Tiefe in Tagesranges unterm 20-Tage-Hoch
           4.0::numeric  AS m_base,    -- Stop-Breite: Basis ...
           20.0::numeric AS m_slope,   -- ... + Steigung * Trendglätte
           4.0::numeric  AS m_min,     -- Clamp unten (Seitwärtsmarkt)
           12.0::numeric AS m_max,     -- Clamp oben (starker Trend)
           16.0::numeric AS tsl_cap    -- HARTER Deckel für den Stop-Abstand in %
                                        -- (Backtest: Cap 20-25% kostet NICHTS, Cap 16%
                                        --  -0,4pp Ø-Profit; begrenzt das Anfangsrisiko)
),

-- Tages-OHLC je Ticker/Tag (nur US-Marktzeiten)
daily AS MATERIALIZED (
    SELECT ticker, market_date,
           max(open)  FILTER (WHERE (market_time at time zone 'America/New_York')::time='09:30:00') AS day_open,
           max(close) FILTER (WHERE (market_time at time zone 'America/New_York')::time='15:45:00') AS day_close,
           max(high) AS day_high, min(low) AS day_low
    FROM trading.live_quotes
    WHERE ticker IN (SELECT ticker FROM trading.etf_vola)
      AND (market_time at time zone 'America/New_York')::time BETWEEN '09:30:00' AND '15:45:00'
      AND extract(isodow FROM market_date) BETWEEN 1 AND 5   -- SPY handelt auch Sa/So -> raus (verfälscht ATR/SMA/SH-Gate)
    GROUP BY ticker, market_date
),

-- Tages-Kennzahlen, alle NUR aus Vortagen (kein Lookahead):
-- atr20 = mittlere Tages-Schwankungsbreite in %, sma100 = 100-Tage-Schnitt,
-- er60 = Trendglätte (gerichtete Bewegung / Summe aller Tagesbewegungen, 60 Tage)
daily_feat AS MATERIALIZED (
    SELECT ticker, market_date,
           avg((day_high-day_low)/nullif(day_open,0)*100) OVER w20  AS atr20,
           avg(day_close)                                 OVER w100 AS sma100,
           count(*)                                       OVER w100 AS nd,
           abs(lag(day_close,1) OVER wt - lag(day_close,61) OVER wt)
               / nullif(sum(adiff) OVER w60, 0)                     AS er60
    FROM (SELECT *, abs(day_close - lag(day_close) OVER (PARTITION BY ticker ORDER BY market_date)) AS adiff
          FROM daily) da
    WINDOW w20  AS (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN  20 PRECEDING AND 1 PRECEDING),
           w100 AS (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING),
           w60  AS (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN  60 PRECEDING AND 1 PRECEDING),
           wt   AS (PARTITION BY ticker ORDER BY market_date)
),

-- SPY-Bär-Regime mit 5-Tage-Persistenz (SH-Gate)
spy_regime AS MATERIALIZED (
    SELECT market_date,
           (sum(CASE WHEN spy_bear THEN 1 ELSE 0 END)
                OVER (ORDER BY market_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) = 5 AS spy_bear_5d
    FROM (SELECT market_date,
                 COALESCE(lag(day_close) OVER (ORDER BY market_date)
                          < avg(day_close) OVER (ORDER BY market_date ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING),
                          false) AS spy_bear
          FROM daily WHERE ticker = 'SPY.US') b
),

-- Tages-Lookup nur für die zuletzt benötigten Tage (hält den Kerzen-Join klein)
dl AS MATERIALIZED (
    SELECT f.ticker, f.market_date, f.atr20, f.sma100, f.nd, COALESCE(f.er60, 0.2) AS er60,
           COALESCE(sr.spy_bear_5d, false) AS spy_bear_5d
    FROM daily_feat f
    LEFT JOIN spy_regime sr ON sr.market_date = f.market_date
    WHERE f.market_date >= (now() - interval '40 days')::date
),

-- Kerzen der letzten 40 Tage (genug für das 520-Kerzen-/20-Tage-Hoch-Fenster)
source AS MATERIALIZED (
    SELECT ticker, market_time, market_date, open, high, low, close,
           (market_time at time zone 'America/New_York')::time AS ny_t
    FROM trading.live_quotes
    WHERE ticker IN (SELECT ticker FROM trading.etf_vola)
      AND market_time >= now() - interval '40 days'
      AND (market_time at time zone 'America/New_York')::time BETWEEN '09:30:00' AND '15:45:00'
      AND extract(isodow FROM market_date) BETWEEN 1 AND 5   -- Wochenend-Handel (SPY) raus
),

-- Kerzen + rollierende 20-Tage-Hochs + Tages-Kennzahlen + ETF-Stammdaten
feat AS (
    SELECT s.*, d.atr20, d.sma100, d.nd, d.er60, d.spy_bear_5d,
           v.index, v.wkn, v.spread_pct_roundup,
           max(s.high) OVER (PARTITION BY s.ticker ORDER BY s.market_time
                             ROWS BETWEEN 520 PRECEDING AND 1 PRECEDING)      AS prior_high,  -- exkl. aktuelle (Breakout)
           max(s.high) OVER (PARTITION BY s.ticker ORDER BY s.market_time
                             ROWS BETWEEN 519 PRECEDING AND CURRENT ROW)       AS roll_high,   -- inkl. aktuelle (Dip-Anker)
           row_number() OVER (PARTITION BY s.ticker, s.market_date ORDER BY s.market_time DESC) AS day_row_desc
    FROM source s
    JOIN dl d ON d.ticker = s.ticker AND d.market_date = s.market_date
    JOIN trading.etf_vola v ON v.ticker = s.ticker
),

-- ENTRY-Signale: Trendfilter/SH-Gate + (Dip-Bounce ODER Breakout)
signals AS (
    SELECT f.*, p.kd, p.tsl_cap,
           LEAST(GREATEST(p.m_base + p.m_slope*f.er60, p.m_min), p.m_max) AS m_used,
           row_number() OVER (PARTITION BY f.ticker, f.market_date ORDER BY f.market_time) AS signal_day_row
    FROM feat f CROSS JOIN params p
    WHERE f.nd >= 60 AND f.atr20 IS NOT NULL
      AND ( (f.ticker <> 'SH.US' AND f.close > f.sma100)                        -- Trendfilter (Longs)
            OR (f.ticker = 'SH.US' AND f.spy_bear_5d) )                        -- SH nur im persistenten Bär-Regime
      AND ( (f.low <= f.roll_high*(1 - p.kd*f.atr20/100)
             AND f.close > f.roll_high*(1 - p.kd*f.atr20/100))                  -- a) Dip-Bounce bestätigt
            OR f.close > f.prior_high )                                         -- b) Breakout 20-Tage-Hoch
),

entry_points AS (
    SELECT a.ticker
         , market_time
         , close AS price
         , round((kd*atr20)::numeric, 1)                                       AS entry
         , round((LEAST(m_used*atr20, tsl_cap) + spread_pct_roundup)::numeric, 1) AS tsl
         , index
         , wkn
         , signal_day_row
         , split_part(a.ticker, '.', 1) AS ticker_short
    FROM signals a
    WHERE signal_day_row = 1
      AND day_row_desc = 1
      AND market_date = now()::date
      AND a.ticker != ALL(:held_tickers)
      AND a.ticker <> 'QQQM.US'   -- QQQM laeuft ueber die eigene Growth-Strategie (fetch_qqqm_growth_signal)
)

SELECT *
FROM entry_points
    """)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"held_tickers": held_tickers})

        if df.empty:
            print(f"[{pd.Timestamp.now()}] ETF-Analyse abgeschlossen: Keine Signale gefunden.")
            return

        market_time = df['market_time'].iloc[0]

        message_blocks = [
            f"📊 <b>ETF Rebound Signal ({len(df)})</b>",
            f"📅 <i>Stand: {market_time}</i>"
        ]

        wkns_to_send = []

        for _, row in df.iterrows():
            ticker_display = row.get('ticker_short') or row['ticker']
            wkn = row['wkn']
            wkns_to_send.append(wkn)

            # Deeplink zu finanzen.net zero via WKN
            trading_link = f"https://www.google.com/search?q=finanzen+net+zero+{wkn}&oq=finanzen+net+zero+LYX018&gs_lcrp=EgZjaHJvbWUyBggAEEUYOTIHCAEQIRigATIHCAIQIRigATIHCAMQIRigATIHCAQQIRiPAtIBCDQxNzFqMGo3qAIAsAIA&sourceid=chrome&ie=UTF-8"

            tsl_display = row['tsl'] * 100 if row['tsl'] < 1 else row['tsl']

            block = (
                f"🚀 <a href='{trading_link}'><b>{ticker_display}</b></a>\n"
                f"├ Index: <b>{row['index']}</b>\n"
                f"├ WKN: <code>{wkn}</code>\n"
                f"├ Preis: <b>{row['price']:.2f}</b>\n"
                f"├ Einstieg (Entry %): <code>{row['entry']:.2f}%</code>\n"
                f"└ 🛡 TSL: <code>{tsl_display:.2f}%</code>"
            )
            message_blocks.append(block)

        full_message = "\n\n".join(message_blocks)
        send_etf_telegram_message(full_message)

        # WKNs separat nachschicken für Kopierbarkeit
        if wkns_to_send:
            wkn_msg = "📑 <b>Kopierbare WKNs:</b>\n" + "\n".join([f"<code>{w}</code>" for w in wkns_to_send])
            send_etf_telegram_message(wkn_msg)

        print(f"ETF Erfolg: {len(df)} Signale gesendet.")

    except Exception as e:
        print(f"Fehler bei ETF-Analyse/Versand: {e}")


# ============================================================================
# QQQM 3x GROWTH-STRATEGIE  (ersetzt QQQM in der ETF-Logik)
# ----------------------------------------------------------------------------
# Dynamischer, volatilitaets-skalierter Trailing-Stop, beim Einstieg fixiert:
#     TSL% = clamp(12 * BodyVola20, 10, 28)  (auf dem 3x-PRODUKT)
# Einstieg: Index-Tagesschluss > SMA50 UND Cooldown (>= 5 Handelstage seit dem
# letzten Stop). Der rekursive Walk auf trading.live_quotes (15-Min -> Tages-
# kerzen) repliziert die Strategie und weiss aus der Historie, OB der TSL schon
# gerissen ist (-> Cooldown) und ob wir gerade in Position sind.
#
# Es wird NUR ein KAUFSIGNAL gesendet. MASSGEBLICH fuer "in Position" ist
# held_positions.txt (NICHT das Walk-Modell) -> nach einem Verkauf verpasst du
# nie ein Re-Entry-Signal (lieber ein Signal zu viel als eines zu wenig).
# Signal-Bedingung: QQQM.US NICHT in held_positions.txt  UND  Index-Schluss > SMA50
# UND Cooldown (>= 5 Handelstage seit letztem Modell-Stop)  UND  heute noch nicht
# gesendet (Dedup-Datei). Signal erst NACH US-Close (15:45 NY).
# WORKFLOW: bei KAUF QQQM.US in held_positions.txt eintragen (mutet Signale),
#           bei VERKAUF wieder entfernen (schaltet Re-Entry-Signale frei).
# Das Walk-Modell dient nur noch fuer den dyn. TSL (BodyVola20) und den Cooldown.
# Backtest 2016-2026 (3x, netto): +2395% | maxDD -35% | Sharpe 0.94.
# ============================================================================

QQQM_PRODUKT_ISIN = "IE00BLRPRL42"   # WisdomTree Nasdaq 100 3x Daily Leveraged
# Signal-Dedup-Funktionen sind nach qqqm_signal_state.py ausgelagert (oben importiert,
# testbar via test_qqqm_signal.py).


def fetch_qqqm_growth_signal():
    """QQQM 3x Growth-Kaufsignal (dyn. Vola-TSL + SMA50 + Modell-Cooldown + Exit-Cooldown)."""
    engine = create_engine(DATABASE_URL)
    held_tickers = load_held_positions()

    # Exit-Zustand pflegen: haelst du QQQM -> Datum loeschen; frisch ausgestiegen
    # (auskommentiert) -> heutiges Datum als Ausstieg merken. Zaehlt den Exit-Cooldown
    # ab DEINEM Ausstieg (nicht ab dem Modell-Stop).
    qqqm_held = 'QQQM.US' in held_tickers
    today = datetime.date.today().isoformat()
    exit_date = _qqqm_update_exit_state(qqqm_held, today)

    query = text("""
WITH RECURSIVE
cand AS (
  SELECT market_date, market_time, open, high, low, close,
         (market_time AT TIME ZONE 'America/New_York')::time AS ny_t,
         row_number() OVER (PARTITION BY market_date ORDER BY market_time)      AS rn_asc,
         row_number() OVER (PARTITION BY market_date ORDER BY market_time DESC) AS rn_desc
  FROM trading.live_quotes
  WHERE ticker = 'QQQM.US'
    AND (market_time AT TIME ZONE 'America/New_York')::time BETWEEN '09:30:00' AND '16:00:00'
    AND extract(isodow FROM market_date) BETWEEN 1 AND 5
),
daily AS (
  SELECT market_date,
         coalesce(max(open) FILTER (WHERE ny_t='09:30:00'), max(open) FILTER (WHERE rn_asc=1)) AS d_open,
         max(close) FILTER (WHERE rn_desc=1)                                                     AS d_close,
         max(high) AS d_high, min(low) AS d_low,
         (max(ny_t) >= time '15:45:00'
          OR market_date < (now() AT TIME ZONE 'America/New_York')::date) AS complete
  FROM cand GROUP BY market_date
),
d AS (SELECT market_date, d_open::numeric AS open, d_close::numeric AS close, d_high::numeric AS high, d_low::numeric AS low
      FROM daily WHERE complete AND d_close IS NOT NULL),
per AS (SELECT market_date AS dt, close, open, high, low, lag(close) OVER (ORDER BY market_date) AS prev_close FROM d),
ind AS (SELECT dt, close, high, low, prev_close, abs(close-open)/prev_close*100 AS body_pct,
        avg(close) OVER (ORDER BY dt ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50 FROM per),
base AS (SELECT row_number() OVER (ORDER BY dt) AS rn, dt, close, high, low, prev_close, sma50, (close>sma50) AS reentry,
         avg(body_pct) OVER (ORDER BY dt ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS body_vola20,
         (0.0075 + 2*(CASE extract(year FROM dt)::int WHEN 2024 THEN 0.053 WHEN 2025 THEN 0.045 ELSE 0.041 END))/252 AS drag FROM ind),
walk AS (
  SELECT rn, dt, close, high, low, prev_close, reentry, drag, sma50, body_vola20, 0::int AS pos_after, NULL::numeric AS prod_val, NULL::numeric AS prod_wm, NULL::numeric AS tsl, NULL::date AS entry_date, NULL::numeric AS entry_index, NULL::text AS event, NULL::numeric AS prod_ret, (-1000)::bigint AS last_stop FROM base WHERE rn=1
  UNION ALL
  SELECT b.rn, b.dt, b.close, b.high, b.low, b.prev_close, b.reentry, b.drag, b.sma50, b.body_vola20,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) >  w.prod_wm*(1-w.tsl) THEN 1 WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) <= w.prod_wm*(1-w.tsl) THEN 0 WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN 1 ELSE 0 END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) >  w.prod_wm*(1-w.tsl) THEN w.prod_val*(1+3*(b.close/b.prev_close-1)-b.drag) WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN 1.0 ELSE NULL END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) >  w.prod_wm*(1-w.tsl) THEN GREATEST(w.prod_wm, w.prod_val*(1+3*(b.high/b.prev_close-1))) WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN 1.0 ELSE NULL END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) >  w.prod_wm*(1-w.tsl) THEN w.tsl WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN LEAST(GREATEST(12*b.body_vola20,10),28)/100 ELSE NULL END,
    CASE WHEN w.pos_after=1 THEN w.entry_date WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN b.dt ELSE NULL END,
    CASE WHEN w.pos_after=1 THEN w.entry_index WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN b.close ELSE NULL END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) <= w.prod_wm*(1-w.tsl) THEN 'SELL' WHEN w.pos_after=0 AND b.reentry AND b.rn>55 AND b.body_vola20 IS NOT NULL AND (b.rn-w.last_stop)>=5 THEN 'BUY' ELSE NULL END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) <= w.prod_wm*(1-w.tsl) THEN w.prod_wm*(1-w.tsl)-1 ELSE NULL END,
    CASE WHEN w.pos_after=1 AND w.prod_val*(1+3*(b.low/b.prev_close-1)) <= w.prod_wm*(1-w.tsl) THEN b.rn ELSE w.last_stop END
  FROM base b JOIN walk w ON b.rn=w.rn+1
)
SELECT dt AS letzte_kerze,
       (close > sma50)                                    AS reentry_bedingung,   -- Index > SMA50 (Einstiegs-Trigger)
       round(LEAST(GREATEST(12*body_vola20,10),28),1)     AS tsl_pct,             -- dyn. TSL, den du beim Kauf setzt
       round(close,2)                                     AS qqqm_close,
       (rn - last_stop)                                   AS tage_seit_stop,      -- fuer Cooldown
       pos_after                                          AS modell_position,     -- nur Info/Debug
       (dt >= (now() - interval '4 days')::date)          AS is_recent            -- Tag abgeschlossen & frisch
FROM walk ORDER BY rn DESC LIMIT 1
    """)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Fehler bei QQQM-Growth-Analyse: {e}")
        return

    if df.empty:
        print("[QQQM] Keine Tageshistorie -> kein Signal.")
        return

    row = df.iloc[0]
    sig_date = str(pd.to_datetime(row['letzte_kerze']).date())   # robust 'YYYY-MM-DD' (kein Timestamp-Drift)

    # 1) held_positions ist die MASSGEBLICHE Position: haelst du QQQM.US -> kein Signal.
    #    So bekommst du nach einem Verkauf (QQQM.US entfernt) zuverlaessig wieder Signale.
    if qqqm_held:
        print("[QQQM] In held_positions -> du haelst -> kein Signal.")
        return

    # 2) Exit-Cooldown: nach DEINEM Ausstieg (auskommentiert) erst nach 5 Handelstagen wieder.
    #    Zaehlt ab exit_date (dein Ausstieg), unabhaengig vom Modell-Stop.
    if _qqqm_exit_cooldown_active(exit_date, today):
        d = _qqqm_trading_days_between(exit_date, today)
        print(f"[QQQM] Exit-Cooldown aktiv ({d}/{_QQQM_EXIT_COOLDOWN} Handelstage seit Ausstieg {exit_date}).")
        return

    # 3) Einstiegsbedingung (Index-Schluss > SMA50) auf abgeschlossenem, frischem Tag
    if not bool(row['reentry_bedingung']) or not bool(row['is_recent']):
        print(f"[QQQM] Kein Einstieg (close>SMA50={row['reentry_bedingung']}, tag_fertig={row['is_recent']}).")
        return

    # 4) Modell-Cooldown: >= 5 Handelstage seit dem letzten Modell-Stop (Backtest-Heuristik)
    if int(row['tage_seit_stop']) < 5:
        print(f"[QQQM] Modell-Cooldown aktiv ({int(row['tage_seit_stop'])} Tage seit letztem Modell-Stop).")
        return

    # 5) Anti-Spam: pro Signal-Tag nur einmal senden (Dedup ueber qqqm_last_signal.txt,
    #    die der Workflow zurueck-committet)
    if not _qqqm_should_send(sig_date):
        print(f"[QQQM] Signal fuer {sig_date} bereits gesendet -> kein Doppel.")
        return

    tsl = float(row['tsl_pct'])
    price = float(row['qqqm_close'])
    msg = (
        f"🟣 <b>QQQM 3x GROWTH – KAUFSIGNAL</b>\n"
        f"📅 <i>Stand: {sig_date}</i>\n\n"
        f"├ QQQM Schlusskurs: <b>{price:.2f}</b>\n"
        f"├ 🛡 Trailing-Stop (Produkt): <b>{(tsl+1):.1f}%</b>  <i>einmal beim Kauf setzen, dann nicht mehr anfassen</i>\n"
    )
    send_etf_telegram_message(msg)
    try:
        _qqqm_save_last_signal(sig_date)
    except Exception as e:
        print(f"[QQQM] WARNUNG: Dedup-Datei nicht schreibbar ({e}) -> Doppelsignale moeglich!")
    print(f"[QQQM] Kaufsignal gesendet ({sig_date}, TSL {tsl:.1f}%).")


# --- START ---
if __name__ == "__main__":
    time.sleep(30)
    sync_ohlcv_to_neon()
    fetch_and_notify()
    fetch_etf_signals()
    fetch_qqqm_growth_signal()
