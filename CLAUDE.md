# CLAUDE.md

Projektnotizen für Claude Code. Dieses Repo ist ein **Trading-Signal-System**: Es
synct 15-Minuten-Kursdaten in eine Postgres-DB (Neon), wertet zwei Strategien per
SQL aus und verschickt Signale via Telegram. Zu jedem Aktien-Signal wird zusätzlich
ein passendes Long-Knock-Out-Zertifikat von finanzen.net gesucht und als Textdatei
geschickt.

Kommunikation und Code-Kommentare sind auf **Deutsch**.

---

## 1. Dateien & Aufbau

| Datei | Zweck |
|---|---|
| `stock_syncer.py` | **Live-Code.** Sync + beide Strategien + Zertifikate-Suche. |
| `qqqm_signal_state.py` | QQQM-Signal-Dedup (ausgelagert, testbar). |
| `test_qqqm_signal.py` | Tests der Dedup-Logik (`python test_qqqm_signal.py`, ohne schwere Deps). |
| `held_positions.txt` | Liste bereits gehaltener Ticker (`ADBE.US` etc.), werden aus Signalen ausgeschlossen. `#`-Zeilen = Kommentare. |
| `qqqm_last_signal.txt` / `qqqm_exit_date.txt` | Dedup-/Cooldown-State, wird vom Workflow zurückcommittet. |

### Zum Testen von Query-Änderungen
Die frühere Dev-Kopie (`stock_syncer_dev.py`) wurde beim Umzug ins Public-Repo
entfernt (enthielt Klartext-Secrets). Zum Testen: lokal mit exportierten Env-Vars
arbeiten und im `notification`-Block temporär `WHERE market_date = '<Testdatum>'`
+ `LIMIT n` setzen — solche Test-Anpassungen **nie committen**.

---

## 2. Daten-Pipeline

- **DB:** Postgres auf Neon. Connection-String, Telegram-Tokens und Chat-IDs kommen
  **ausschließlich aus der Umgebung** (`DATABASE_URL`, `TELEGRAM_TOKEN`, `CHAT_ID`,
  `ETF_TELEGRAM_TOKEN`, optional `ETF_CHAT_ID`) — als GitHub-Actions-Secrets gesetzt.
  **Dieses Repo ist PUBLIC: niemals Klartext-Secrets committen.** Der Code failt hart
  (KeyError), wenn eine Variable fehlt.
- **`sync_ohlcv_to_neon()`** lädt 15-Min-OHLCV via `yfinance` (`period="1d",
  interval="15m"`) für alle `TICKERS` und schreibt sie nach `trading.live_quotes`.
  - Zielkerze = letzter **abgeschlossener** 15-Min-Slot (`floor('15min') - 15min`),
    alles in **UTC**. Yahoo überspringt manchmal Slots → es wird die jüngste Kerze
    genommen, die nicht in der Zukunft liegt.
  - Bis zu 5 Versuche mit 15 s Pause; `ON CONFLICT (ticker, market_time) DO UPDATE`.
- **Ticker-Konvention:** in der DB mit Suffix `.US` (z. B. `NVDA.US`), Ausnahme
  FX-Ticker mit `=` (z. B. `EUR=X`) bleiben unverändert.
- **Zeitzonen:** Speicherung in **UTC**, in den Queries via
  `market_time at time zone 'America/New_York'` auf NY-Handelszeit umgerechnet.
- `EUR=X` liefert den USD→EUR-Kurs (für die Euro-Umrechnung der Kurse).
- `QQQM.US` (Nasdaq-100-ETF) dient als **Markt-Proxy** für die Nasdaq-Tagesperformance.

### Genutzte DB-Tabellen (Schema `trading`)
| Tabelle | Spalten (soweit verwendet) | Rolle |
|---|---|---|
| `live_quotes` | `ticker, market_time, market_date, open, high, low, close, volume` | 15-Min-OHLCV (Rohdaten). |
| `entry_and_sl` | `ticker, entry_dip_pct, sql_factor, sector, name, isin` | Pro-Aktie-Konfig für die Rebound-Strategie. `sql_factor` = statischer SL-Faktor (z. B. 0.95). |
| `etf_vola` | `ticker, manual_entry, manual_tsl_with_spread_and_puffer, index, wkn` | Pro-ETF-Konfig für die ETF-Strategie. |

---

## 3. Strategie A — NASDAQ-100 Intraday-Rebound (`fetch_and_notify`)

**Idee:** Auf einem Universum von ~100 NASDAQ-100-Aktien (+ einigen ETFs) wird ein
**Intraday-Dip mit anschließender Rebound-Bestätigung** gekauft. Die SQL-Query baut
sich in CTEs auf:

1. **Dip-Trigger** (`difs_to_prev` / `entry_points_prep`): Der Kurs ist um mindestens
   den pro Aktie konfigurierten `entry_dip_pct` gefallen — gemessen gegen **eines** von:
   - Tageseröffnung (`dif_prev_to_day_open_pct`),
   - Kurs vor ~2,5 h (`dif_prev_to_2h_before_pct`),
   - Vortagesschluss (`dif_prev_to_day_prev_day_close_pct`, hier Schwelle ×1,5).
2. **Rebound-Bestätigung:** die letzten **drei** 15-Min-Kerzen sind grün
   (`dif > 0 AND close_dif_15_min_ago > 0 AND close_dif_30_min_ago > 0`) → der
   Abwärtsimpuls dreht nach oben.
3. **Erstes Signal des Tages** je Aktie (`entry_points: row = 1`).

**Qualitäts-Filter** (`start_filtered`, WHERE):
- `rsi BETWEEN 40 AND 87` (14er-RSI auf 15-Min-Basis; weder überverkauft-extrem noch überkauft).
- **Nasdaq-Kontext** (`nasdaq_day_performance` = QQQM intraday vs. Vortagesschluss):
  `> -0.035` (kein Crash-Tag) und **nicht** im flachen Band `-0.5%..+1%` (klare Richtung gefordert).
- `vol_score` (relatives Volumen der letzten 45 Min vs. Tageszeit-Schnitt der letzten ~20 Tage):
  `> 0.4 AND < 1.8` (erhöht, aber kein Blow-off).
- **Abstand zum SMA-200** (`start_price/sma_200 - 1`): `> -0.4`, `< 1.3` und Ausschluss
  der Bänder `-5%..+15%` sowie `+50%..+70%` (meidet Zonen mit schlechter Trefferquote).
- **Sektor-Whitelist** über eine lange `sector NOT IN (...)`-Liste (viele Sektoren ausgeschlossen).

**Dynamische TSL** (`dynamic_sl`): statt eines festen Stops wird ein vola-basierter
Trailing-Abstand berechnet:
```
dyn_drop_pct = clamp( 1.0*avg_abs_body_20d + 2.0*stddev_body_20d + 0.5*avg_range_20d , 4% , 25% )
```
(20-Tage-Fenster, nur abgeschlossene Vortage). `effective_stop_loss_pct` nutzt die
dynamische TSL ab ≥10 Tagen Historie, sonst Fallback auf den statischen `sql_factor`
aus `entry_and_sl`.

**Live-Trigger:** Es wird nur benachrichtigt, wenn das Signal auf der **aktuellsten**
Kerze von **heute** liegt (`day_row_desc = 1 AND market_date = now()::date`).

**Telegram-Output** (Bot `TELEGRAM_TOKEN`): pro Signal ein Block mit Ticker/Name,
Sektor, Nasdaq-%, Preis (USD + EUR), **SL-Limit in €** und `-x.x%`, plus News-Link.
Danach die ISINs als kopierbare Liste. (Felder `minimum_free_trade`, `dyn_drop_pct`,
`entry_dip_pct` werden berechnet, aber aktuell nicht alle angezeigt.)

---

## 4. Strategie B — ETF Rebound-Entry (`fetch_etf_signals`)

**Idee:** Für eine Menge handelbarer ETFs/Index-Produkte (aus `trading.etf_vola`)
wird ein **Rebound vom rollierenden Tief** gehandelt.

1. **Rollierendes Tief:** `MIN(low)` über **546 Perioden** (15-Min-Kerzen im Fenster
   09:30–15:45 NY ≈ ~1 Handelsmonat).
2. **Trigger-Preis:** `entry_trigger_price = rollendes_Tief × (1 + manual_entry/100)`.
   `manual_entry` ist die pro ETF konfigurierte Rebound-Schwelle (z. B. 6 % über dem Tief).
3. **Signal**, wenn der Kurs den Trigger von unten durchbricht **und** intraday Stärke
   zeigt:
   - `low < trigger AND high > trigger AND high > day_start` (Kerze kreuzt den Trigger,
     Hoch über Tageseröffnung), **oder**
   - Gap-Open: um `09:30` mit `prev_close < trigger AND high > trigger`.
4. **Erstes Signal des Tages** je ETF (`signal_day_row = 1`), nur auf der aktuellsten
   Kerze von heute (`day_row_desc = 1 AND market_date = now()::date`), Held-Positionen ausgeschlossen.

**TSL:** `manual_tsl_with_spread_and_puffer` aus `etf_vola` — ein **manuell gepflegter**
TSL-Wert, der Spread und Puffer bereits enthält (für gehebelte Produkte gedacht).

**Telegram-Output** (eigener Bot `ETF_TELEGRAM_TOKEN`): pro Signal Block mit Ticker,
Index, **WKN**, Preis, Entry-% und TSL-%. Danach die WKNs als kopierbare Liste.

---

## 5. Zertifikate-Suche (Long-Knock-Outs) — pro Aktien-Signal

Python-Port von `gettex_search.js`. Wird in `fetch_and_notify()` **pro Signal**
aufgerufen und schickt die 5 Long-KO-Calls (mit SL-Funktion) mit dem niedrigsten
prozentualen Spread als **Textdatei** (`send_telegram_document`, Telegram `sendDocument`).

- **Datenquelle:** finanzen.net Hebelprodukt-Suche (`DerivativesControllerSearch`),
  gettex-Kurse. Filter in `ZERT_FILTER`: Knock-Outs, Long/Call, mit Stop-Loss,
  Emittenten GS/HSBC/BNP/UniCredit, **Hebel-Band 3–3,3** (Dezimal-**Komma**!),
  serverseitig nach Spread aufsteigend sortiert.
- **Auflösung des Basiswerts:** `_zert_underlying_id` → **ISIN zuerst** (eindeutig &
  zuverlässig), dann Name als Fallback. Schlägt beides fehl, gibt es eine
  **Telegram-Warnung** (kein stilles Verschlucken). Wird unter der ISIN nichts im
  Hebel-Band gefunden (z. B. PDD Holdings → 0 Treffer), kommt eine Info-Meldung.

### Euro-Abstandsorder (Kernlogik, `_zert_euro_abstand`)
Wegen des **variablen Hebels** (nimmt nach oben ab) eignet sich keine prozentuale TSL
auf den Schein. Stattdessen eine feste **Euro-Distanz**, kalibriert auf den Ask:
```
Abstand_€      = Ask × (TSL% × Hebel) + (Ask − Bid) + Ask × 0,3%
Initialer Stop = Ask − Abstand_€
```
- `TSL%` = `stop_loss_pct` des Basiswert-Signals (Dezimal, z. B. 0.05).
- **Warum Ask + Spread:** Du kaufst zum Ask, die Order triggert aber am Bid → der
  `(Ask − Bid)`-Term verhindert ein Ausstoppen um einen Spread zu früh. `0,3%` = Puffer.
- **Verhalten vs. %-TSL auf den Basiswert:** Ein KO-Schein bewegt sich €-für-€ mit dem
  Basiswert (× Bezugsverhältnis); der Hebel ist ein reiner %-Effekt. Die Euro-Order
  entspricht damit am Basiswert einem **eingefrorenen** Abstand `TSL% × U_entry`. Effektiv
  trailt sie bei `TSL% / (1 + g)` (g = Kursgewinn seit Einstieg) → bei neuen Hochs wird
  etwas **früher** ausgestoppt (gewollt, Gewinnmitnahme), aber moderat. Spread + 0,3 %
  machen die Order anfangs minimal weiter → schützt vor zu frühem Stop bei kleinen
  Rücksetzern direkt nach Einstieg.

---

## 6. Wichtige Learnings / Stolperfallen

- **finanzen.net WAF (Akamai):** Schlanke Requests werden mit **HTTP 403 "Access
  Denied"** geblockt. Lösung in `_zert_get_session()`:
  1. `requests.Session()` mit **vollem** Browser-Header-Satz (`sec-ch-ua`, `sec-fetch-*`,
     `accept`, voller User-Agent).
  2. **Warm-up**-GET auf `…/zertifikate/suche` — antwortet selbst mit 403, **setzt aber
     das `Bot-Information`-Cookie**. Erst danach liefert der AJAX-Endpoint 200.
- **finanzen.net Resolver (`UnderlyingsByInput`)** ist **case-sensitiv** und mag **keine
  Rechtsform-Zusätze**: „Broadcom Inc." / „Microsoft Corporation" → 0 Treffer, „Broadcom"
  / „Microsoft" → Treffer. Deshalb: ISIN zuerst, dann Name **und** eine um Zusätze
  (Inc./Corp./Corporation/Holdings/…) bereinigte Variante probieren.
- **Deutsche Zahlen** im finanzen.net-HTML: `_zert_zahl` parst `"1.234,56"` (Punkt =
  Tausender, Komma = Dezimal). Auch der Hebel-Filter braucht **Dezimalkomma** (`"3,3"`).
- **`urlencode` vs. JS `URLSearchParams`:** verhalten sich gleich (Komma → `%2C`), daher
  ist der Filter-Dict 1:1 portierbar.
- **Telegram:** zwei getrennte Bots (Aktien vs. ETF), aber derselbe Chat. Textdateien
  via `sendDocument` (UTF-8), damit Inhalte kopierbar sind.
- **Plattform:** läuft als GitHub Action (ubuntu, Workflow `main.yml`), getriggert per
  externem Cron via `repository_dispatch` (event_type `market_trigger`). Lokal (Windows/
  PowerShell) mit exportierten Env-Vars startbar.

---

## 7. Testen ohne echten Versand

Modul laden und Telegram-Funktionen mocken (kein echter Versand, keine DB nötig für die
Zertifikate-Suche):
```python
import importlib.util
spec = importlib.util.spec_from_file_location('m', 'stock_syncer.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

m.send_telegram_document = lambda content, filename, caption=None: print('DOC', filename, len(content))
m.send_telegram_message  = lambda message: print('MSG', message[:60])

m.fetch_and_send_zertifikate('Nvidia', 0.05, 'NVDA', isin='US67066G1040')
```
Syntax-Check: `python -c "import ast; ast.parse(open('stock_syncer.py', encoding='utf-8').read())"`.
