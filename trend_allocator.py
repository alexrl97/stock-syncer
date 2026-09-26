"""Monatliche Trendfolge (12 Anlageklassen, SMA10) — Order-Rechner mit Depot-Buchhaltung.

Regel (am letzten Xetra-Handelstag des Monats, nach Handelsschluss):
  * Pro ETF: Monatsschluss (ausschuettungsbereinigt) > Durchschnitt der letzten 10
    Monatsschluesse (SMA10)  -> aktiv, sonst inaktiv.
  * Aktive Klassen bekommen je min(1/n_aktiv, Obergrenze) des Eigenkapitals
    (Obergrenze 12,5 %, Quanten 7,5 %), multipliziert mit dem Hebel.
  * Hebel (Wertpapierkredit) dauerhaft, begrenzt durch den Kreditrahmen und max. 80 % des
    Beleihungswerts. Abschaltbar ueber trend_account.kredit_aktiv.
  * Rest -> Geldmarkt-ETF. Gehandelt wird am naechsten Handelstag, nur ganze Stuecke.

Der Depotstand (Stuecke, FIFO-Lots, Verrechnungskonto, Verlusttopf) liegt in der
Neon-DB (Schema trading, Tabellen trend_*), NICHT im Repo — dieses Repo ist public.
Das System nimmt an, dass jede Order zum Signal-Schlusskurs ausgefuehrt wurde.
Abweichungen (echte Ausfuehrungskurse, Stueckzahlen, Kontostand, Verlusttopf laut
Broker) werden direkt in den trend_*-Tabellen korrigiert.

Aufruf:
  python trend_allocator.py                  # regulaerer Lauf (nur am letzten Handelstag)
  python trend_allocator.py --preview        # Vorschau: rechnet + Telegram, speichert nichts
  python trend_allocator.py --force          # Lauf erzwingen (speichert!)
  python trend_allocator.py --init --mmf-value 27186 --verlusttopf 6000
"""
import os
import sys
import math
import argparse
import datetime as dt

import pandas as pd
import requests
import yfinance as yf
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
ETF_TELEGRAM_TOKEN = os.environ["ETF_TELEGRAM_TOKEN"]
ETF_CHAT_ID = os.environ.get("ETF_CHAT_ID", os.environ["CHAT_ID"])

# --- UNIVERSUM ---
# key, Name, Yahoo-Symbol (Xetra), ISIN, Obergrenze, Teilfreistellung, Beleihungssatz
# Beleihungssaetze lt. Baader-Bank-Vertrag: Fonds/ETFs 75 %, Zertifikate/ETCs 0 %.
# Immobilienfonds sind dort von der Beleihung ausgeschlossen -> REIT konservativ 0 %.
CLASSES = [
    ("US_EQ",   "S&P 500",                        "SPYL.DE", "IE000XZSV718", 0.125, 0.30, 0.75),
    ("INTL_EQ", "Industrieländer ex USA",        "EXUS.DE", "IE0006WW1TQ4", 0.125, 0.30, 0.75),
    ("EM_EQ",   "Schwellenländer",               "IS3N.DE", "IE00BKM4GZ66", 0.125, 0.30, 0.75),
    # REIT: Amundi FTSE EPRA Nareit Developed Acc (gettex A4H5, kein Xetra -> Kurse von Euronext Paris)
    ("REIT",    "Immobilien",                     "EPRA.PA", "LU1437018838", 0.125, 0.00, 0.00),
    ("COMM",    "Rohstoffe",                      "SXRS.DE", "IE00BDFL4P12", 0.125, 0.00, 0.75),
    ("GOLD",    "Gold (ETC)",                     "PPFB.DE", "IE00B4ND3602", 0.125, 0.00, 0.00),
    ("UST_L",   "US-Staatsanl. 20+J EUR-hedged",  "IUSV.DE", "IE00BD8PGZ49", 0.125, 0.00, 0.75),
    ("UST_M",   "US-Staatsanl. 7-10J EUR-hedged", "IBB1.DE", "IE00BGPP6697", 0.125, 0.00, 0.75),
    ("CORP",    "EUR-Unternehmensanleihen",       "D5BG.DE", "LU0478205379", 0.125, 0.00, 0.75),
    ("BUND",    "Bundesanleihen",                 "X03G.DE", "LU0643975161", 0.125, 0.00, 0.75),
    ("QUANT",   "Quanten-Computing",              "QUTM.DE", "IE0007Y8Y157", 0.075, 0.30, 0.75),
    ("NDX",     "Nasdaq-100",                     "XNAS.DE", "IE00BMFKG444", 0.125, 0.30, 0.75),
]
MMF = ("MMF", "Geldmarkt (Amundi Smart Overnight)", "LYOR.DE", "LU1190417599", None, 0.00, 0.75)
ALL = CLASSES + [MMF]
BY_TICKER = {c[2]: c for c in ALL}

SMA_MONTHS = 10
# Zwei Handelsrunden am Tag nach dem Signal (Berliner Zeit). Runde 1: Xetra laeuft sich ein,
# Asien/Indien teils noch offen. Runde 2: Xetra UND US-Boerse offen (nach der US-Eroeffnungsphase).
SLOTS = {1: ("10:00", "11:00"), 2: ("15:45", "17:15")}
# Startzuordnung, bis genug gettex-Spreaddaten vorliegen (dann entscheidet der Median je Runde).
SLOT_DEFAULT = {"SPYL.DE": 2, "XNAS.DE": 2, "QUTM.DE": 2, "EPRA.PA": 2}   # Rest: Runde 1
SLOT_MIN_SAMPLES = 8
WOCHENTAG = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
TAX_RATE = 0.26375          # Abgeltungsteuer + Soli (ohne Kirchensteuer)
MMF_BUFFER = 0.005          # 0,5 % des EK bleiben als Puffer fuer Kursabweichungen am Handelstag

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# ---------------------------------------------------------------- Kalender
def _easter(year):
    a = year % 19; b = year // 100; c = year % 100; d = b // 4; e = b % 4
    f = (b + 8) // 25; g = (b - f + 1) // 3; h = (19 * a + b - d - g + 15) % 30
    i = c // 4; k = c % 4; l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31; day = ((h + l - 7 * m + 114) % 31) + 1
    return dt.date(year, month, day)


def xetra_holidays(year):
    e = _easter(year)
    return {dt.date(year, 1, 1), e - dt.timedelta(days=2), e + dt.timedelta(days=1),
            dt.date(year, 5, 1), dt.date(year, 12, 24), dt.date(year, 12, 25),
            dt.date(year, 12, 26), dt.date(year, 12, 31)}


def is_trading_day(d):
    return d.weekday() < 5 and d not in xetra_holidays(d.year)


def is_last_trading_day_of_month(d):
    if not is_trading_day(d):
        return False
    n = d + dt.timedelta(days=1)
    while n.month == d.month:
        if is_trading_day(n):
            return False
        n += dt.timedelta(days=1)
    return True


def next_trading_day(d):
    n = d + dt.timedelta(days=1)
    while not is_trading_day(n):
        n += dt.timedelta(days=1)
    return n


def today_berlin():
    return pd.Timestamp.now(tz="Europe/Berlin").date()


# ---------------------------------------------------------------- Telegram
def send(msg):
    # Telegram-Limit 4096 Zeichen -> an Leerzeilen aufteilen
    chunks, cur = [], ""
    for block in msg.split("\n\n"):
        if len(cur) + len(block) + 2 > 3900:
            chunks.append(cur); cur = ""
        cur += ("\n\n" if cur else "") + block
    chunks.append(cur)
    for c in chunks:
        try:
            r = requests.post(f"https://api.telegram.org/bot{ETF_TELEGRAM_TOKEN}/sendMessage",
                              data={"chat_id": ETF_CHAT_ID, "text": c, "parse_mode": "HTML"}, timeout=15)
            if not r.ok:
                print("Telegram-Fehler:", r.text)
        except Exception as e:
            print(f"Fehler beim Telegram-Versand: {e}")


def slot_spreads():
    """Median-Spread je ETF in beiden Handelsrunden (letzte 30 Tage) aus trading.gettex_spreads."""
    q = text("""
        SELECT ticker, slot, percentile_cont(0.5) WITHIN GROUP (ORDER BY spread_pct) AS med, count(*) AS n
        FROM (SELECT ticker, spread_pct,
                     CASE WHEN (ts AT TIME ZONE 'Europe/Berlin')::time BETWEEN CAST(:a1 AS time) AND CAST(:a2 AS time) THEN 1
                          WHEN (ts AT TIME ZONE 'Europe/Berlin')::time BETWEEN CAST(:b1 AS time) AND CAST(:b2 AS time) THEN 2 END AS slot
              FROM trading.gettex_spreads
              WHERE ts > now() - interval '30 days' AND spread_pct IS NOT NULL) x
        WHERE slot IS NOT NULL GROUP BY ticker, slot""")
    try:
        with engine.connect() as c:
            rows = c.execute(q, dict(a1=SLOTS[1][0], a2=SLOTS[1][1], b1=SLOTS[2][0], b2=SLOTS[2][1])).fetchall()
    except Exception as e:   # Tabelle existiert evtl. noch nicht
        print("Spreaddaten nicht verfuegbar:", e)
        return {}
    out = {}
    for t, sl, med, n in rows:
        out.setdefault(t, {})[int(sl)] = (float(med), int(n))
    return out


def choose_slot(sym, spreads):
    d = spreads.get(sym, {})
    if all(k in d and d[k][1] >= SLOT_MIN_SAMPLES for k in (1, 2)):
        return 1 if d[1][0] <= d[2][0] else 2
    return SLOT_DEFAULT.get(sym, 1)


def eur(x):
    return f"{x:,.0f} €".replace(",", ".")


# ---------------------------------------------------------------- DB
DDL = """
CREATE TABLE IF NOT EXISTS trading.trend_account (
    id int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    cash_eur numeric NOT NULL DEFAULT 0,          -- Verrechnungskonto, negativ = Kredit
    verlusttopf_eur numeric NOT NULL,             -- geschaetzter Rest im Topf 'Sonstige'
    hebel numeric NOT NULL DEFAULT 1.63,
    kredit_rahmen_eur numeric NOT NULL DEFAULT 17000,
    kredit_zins_pa numeric NOT NULL DEFAULT 0.0542,
    kredit_aktiv boolean NOT NULL DEFAULT true,   -- manuell abschaltbar
    max_beleihung_auslastung numeric NOT NULL DEFAULT 0.80,
    min_order_eur numeric NOT NULL DEFAULT 500,
    last_booking_date date NOT NULL,              -- bis hierhin Zinsen/Ausschuettungen gebucht
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS trading.trend_lots (
    id serial PRIMARY KEY,
    ticker text NOT NULL,
    isin text NOT NULL,
    shares numeric NOT NULL,
    buy_date date NOT NULL,
    buy_price numeric NOT NULL
);
CREATE TABLE IF NOT EXISTS trading.trend_runs (
    run_date date PRIMARY KEY,
    equity_eur numeric, invested_eur numeric, debt_eur numeric, leverage numeric,
    verlusttopf_eur numeric, n_active int, created_at timestamptz DEFAULT now()
);
CREATE TABLE IF NOT EXISTS trading.trend_signals (
    run_date date, ticker text, close numeric, sma10 numeric, dist_pct numeric, active boolean,
    PRIMARY KEY (run_date, ticker)
);
CREATE TABLE IF NOT EXISTS trading.trend_orders (
    id serial PRIMARY KEY, run_date date, ticker text, isin text, side text,
    shares numeric, price_est numeric, amount_est numeric, taxable_gain_est numeric,
    status text NOT NULL DEFAULT 'pending',       -- pending | executed (/buy) | assumed (ohne /buy)
    exec_shares numeric, exec_price numeric, exec_at timestamptz,
    slot int NOT NULL DEFAULT 1                  -- Handelsrunde (1 vormittags, 2 nachmittags)
);
"""


def init(mmf_value, verlusttopf, mmf_shares=None):
    px = load_prices(period="1mo")
    p = float(px["raw"][MMF[2]].dropna().iloc[-1])
    shares = mmf_shares if mmf_shares is not None else round(mmf_value / p, 4)
    with engine.begin() as c:
        for stmt in DDL.split(";"):
            if stmt.strip():
                c.execute(text(stmt))
        if c.execute(text("SELECT count(*) FROM trading.trend_account")).scalar():
            sys.exit("trend_account existiert bereits — init abgebrochen (manuell korrigieren).")
        c.execute(text("INSERT INTO trading.trend_account (verlusttopf_eur, last_booking_date) VALUES (:v, :d)"),
                  dict(v=verlusttopf, d=today_berlin()))
        c.execute(text("INSERT INTO trading.trend_lots (ticker, isin, shares, buy_date, buy_price) "
                       "VALUES (:t, :i, :s, :d, :p)"),
                  dict(t=MMF[2], i=MMF[3], s=shares, d=today_berlin(), p=p))
    print(f"Init: {shares} Stk. {MMF[2]} à {p:.4f} € = {shares * p:,.2f} €, Verlusttopf {verlusttopf} €")


# ---------------------------------------------------------------- Kurse
def load_prices(period="2y"):
    syms = [c[2] for c in ALL]
    adj = yf.download(syms, period=period, interval="1d", auto_adjust=True, progress=False)["Close"]
    raw = yf.download(syms, period=period, interval="1d", auto_adjust=False, progress=False)["Close"]
    return {"adj": adj, "raw": raw}


def dividends_since(sym, since, until):
    try:
        d = yf.Ticker(sym).dividends
    except Exception:
        return 0.0
    if d is None or d.empty:
        return 0.0
    d.index = d.index.tz_localize(None) if d.index.tz is not None else d.index
    d = d[(d.index.date > since) & (d.index.date <= until)]
    return float(d.sum())


def signals(adj, today, warn):
    rows = []
    for key, name, sym, isin, cap, tf, bw in CLASSES:
        s = adj[sym].dropna()
        # Yahoo-Fehlticks: Sprung > 5 %, der am Folgetag zu grossen Teilen zurueckgeht -> ignorieren
        r = s.pct_change()
        bad = (r.abs() > 0.05) & (r.shift(-1).abs() > 0.04) & (r * r.shift(-1) < 0)
        s = s[~bad]
        if abs(r.iloc[-1]) > 0.05:
            warn.append(f"{sym}: Schlusskurs {r.iloc[-1] * 100:+.1f}% zum Vortag – bitte Kurs prüfen")
        m = s.resample("ME").last().dropna()
        m = m[m.index.date <= pd.Timestamp(today).to_period("M").end_time.date()]
        if len(m) < SMA_MONTHS:
            rows.append(dict(sym=sym, close=float(s.iloc[-1]), sma=None, dist=None, active=False, note="zu kurze Historie"))
            continue
        sma = float(m.iloc[-SMA_MONTHS:].mean())
        close = float(m.iloc[-1])
        rows.append(dict(sym=sym, close=close, sma=sma, dist=close / sma - 1, active=close > sma, note=""))
    return pd.DataFrame(rows).set_index("sym")


# ---------------------------------------------------------------- Buchung
def book(lots, orders, pot, cash, day):
    """Bucht Orders FIFO auf die Lots. Setzt o['taxable'] je Order, gibt (lots, pot, cash) zurueck."""
    new_lots = lots.copy()
    realized = 0.0
    for o in orders:
        tf = BY_TICKER[o["sym"]][5]
        if o["shares"] < 0:
            left = -o["shares"]; gain = 0.0
            for idx in new_lots[new_lots.ticker == o["sym"]].index:
                if left <= 1e-9:
                    break
                take = min(left, float(new_lots.at[idx, "shares"]))
                gain += take * (o["price"] - float(new_lots.at[idx, "buy_price"]))
                new_lots.at[idx, "shares"] = float(new_lots.at[idx, "shares"]) - take
                left -= take
            o["taxable"] = gain * (1 - tf)
            realized += o["taxable"]
        else:
            o["taxable"] = 0.0
            new_lots = pd.concat([new_lots, pd.DataFrame([dict(id=None, ticker=o["sym"], isin=o["isin"],
                                  shares=o["shares"], buy_date=day, buy_price=o["price"])])], ignore_index=True)
        cash -= o["shares"] * o["price"]
    tax = max(realized - max(pot, 0), 0) * TAX_RATE if realized > 0 else 0.0
    pot -= realized
    cash -= tax
    return new_lots[new_lots.shares > 1e-9], pot, cash


def price_at(sym, ts):
    """Kurs zum Zeitpunkt ts (UTC): letzte 15-Min-Kerze bis ts, sonst Eroeffnung des Tages / letzter Schluss."""
    try:
        h = yf.Ticker(sym).history(period="5d", interval="15m", auto_adjust=False)["Close"].dropna()
        h.index = h.index.tz_convert("UTC")
        before = h[h.index <= ts]
        if not before.empty and before.index[-1].date() == ts.date():
            return float(before.iloc[-1])
        same_day = h[h.index.date == ts.date()]
        if not same_day.empty:
            return float(same_day.iloc[0])
        if not before.empty:
            return float(before.iloc[-1])
    except Exception as e:
        print(f"price_at {sym}: {e}")
    return None


def parse_overrides(txt):
    """'/buy SPYL=16.80 XNAS=88@61.9' -> {'SPYL.DE': (None, 16.8), 'XNAS.DE': (88, 61.9)}"""
    out = {}
    short = {c[2].split(".")[0].upper(): c[2] for c in ALL}
    short.update({c[3]: c[2] for c in ALL})                      # auch per ISIN
    short.update({"LYX0WM": MMF[2], "A4H5": "EPRA.PA"})
    for tok in txt.split()[1:]:
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        sym = short.get(k.strip().upper())
        if not sym:
            continue
        v = v.replace(",", ".")
        if "@" in v:
            n, pr = v.split("@", 1)
            out[sym] = (float(n), float(pr))
        else:
            out[sym] = (None, float(v))
    return out


def apply_pending(fill_time=None, overrides=None, all_slots=True):
    """Bucht alle offenen Orders. fill_time=None -> zum Signalkurs (Rueckgabe: Anzahl);
    sonst zum Kurs dieses Zeitpunkts (Rueckgabe: (orders, cash, pot) bzw. [] ohne offene Orders)."""
    overrides = overrides or {}
    with engine.connect() as c:
        pend = pd.read_sql(text("SELECT * FROM trading.trend_orders WHERE status = 'pending' ORDER BY id"), c)
        if not pend.empty and not all_slots:
            pend = pend[pend.slot == pend.slot.min()]      # /buy bucht die frueheste offene Runde
        if pend.empty:
            return 0 if fill_time is None else []
        acc = c.execute(text("SELECT * FROM trading.trend_account WHERE id = 1")).mappings().first()
        lots = pd.read_sql(text("SELECT * FROM trading.trend_lots ORDER BY buy_date, id"), c)
    orders = []
    for _, r in pend.iterrows():
        sign = 1 if r.side == "BUY" else -1
        n = float(r.shares) * sign
        pr = float(r.price_est)
        if fill_time is not None:
            live = price_at(r.ticker, fill_time)
            pr = live if live else pr
        if r.ticker in overrides:
            n_o, pr_o = overrides[r.ticker]
            pr = pr_o
            if n_o is not None:
                n = n_o * sign
        orders.append(dict(id=int(r.id), sym=r.ticker, isin=r["isin"], name=BY_TICKER[r.ticker][1], shares=n, price=pr))
    orders.sort(key=lambda o: o["shares"] > 0)      # Verkaeufe zuerst
    day = fill_time.tz_convert("Europe/Berlin").date() if fill_time is not None else today_berlin()
    new_lots, pot, cash = book(lots, orders, float(acc["verlusttopf_eur"]), float(acc["cash_eur"]), day)
    with engine.begin() as c:
        c.execute(text("DELETE FROM trading.trend_lots"))
        for _, l in new_lots.iterrows():
            c.execute(text("INSERT INTO trading.trend_lots (ticker, isin, shares, buy_date, buy_price) "
                           "VALUES (:t, :i, :s, :d, :p)"),
                      dict(t=l["ticker"], i=l["isin"], s=float(l["shares"]), d=l["buy_date"], p=float(l["buy_price"])))
        c.execute(text("UPDATE trading.trend_account SET cash_eur = :c, verlusttopf_eur = :v, updated_at = now() "
                       "WHERE id = 1"), dict(c=cash, v=pot))
        for o in orders:
            c.execute(text("UPDATE trading.trend_orders SET status = :st, exec_shares = :n, exec_price = :p, "
                           "exec_at = :t, taxable_gain_est = :g WHERE id = :id"),
                      dict(st="executed" if fill_time is not None else "assumed", n=abs(o["shares"]), p=o["price"],
                           t=fill_time.to_pydatetime() if fill_time is not None else None, g=o["taxable"], id=o["id"]))
    if fill_time is None:
        return len(orders)
    return orders, cash, pot


def status_text():
    with engine.connect() as c:
        acc = c.execute(text("SELECT * FROM trading.trend_account WHERE id = 1")).mappings().first()
        lots = pd.read_sql(text("SELECT ticker, sum(shares) AS shares, sum(shares * buy_price) AS cost "
                                "FROM trading.trend_lots GROUP BY ticker"), c)
        npend = c.execute(text("SELECT count(*) FROM trading.trend_orders WHERE status = 'pending'")).scalar()
    raw = load_prices(period="10d")["raw"]
    lines = ["📊 <b>Trendfolge – Depotstand (Schätzung)</b>"]
    total = 0.0
    for _, l in lots.iterrows():
        p = float(raw[l.ticker].dropna().iloc[-1]); v = float(l.shares) * p; total += v
        lines.append(f"• {BY_TICKER[l.ticker][1]}: {float(l.shares):g} Stk ≈ {eur(v)} "
                     f"(<code>{(v / float(l.cost) - 1) * 100:+.1f}%</code>)")
    cash = float(acc["cash_eur"])
    lines += [f"├ Wertpapiere: {eur(total)}",
              f"├ Konto: {eur(cash)}" + (" (Kredit)" if cash < 0 else ""),
              f"├ Eigenkapital: {eur(total + cash)}",
              f"└ Verlusttopf (geschätzt): {eur(float(acc['verlusttopf_eur']))}"]
    if npend:
        lines.append(f"⏳ {npend} offene Order(s) – nach dem Kauf /buy schicken.")
    return "\n".join(lines)


def poll_commands():
    """Liest neue Nachrichten an den ETF-Bot (getUpdates) und verarbeitet /buy und /status."""
    with engine.begin() as c:
        c.execute(text("CREATE TABLE IF NOT EXISTS trading.trend_bot (id int PRIMARY KEY DEFAULT 1, "
                       "last_update_id bigint NOT NULL DEFAULT 0)"))
        c.execute(text("INSERT INTO trading.trend_bot (id) VALUES (1) ON CONFLICT DO NOTHING"))
        last = c.execute(text("SELECT last_update_id FROM trading.trend_bot WHERE id = 1")).scalar()
    r = requests.get(f"https://api.telegram.org/bot{ETF_TELEGRAM_TOKEN}/getUpdates",
                     params={"offset": last + 1, "timeout": 0}, timeout=20).json()
    if not r.get("ok"):
        print("getUpdates-Fehler:", r)
        return
    max_id = last
    for u in r["result"]:
        max_id = max(max_id, u["update_id"])
        m = u.get("message") or {}
        if str(m.get("chat", {}).get("id")) != str(ETF_CHAT_ID):
            continue   # nur der eigene Chat darf buchen
        txt = (m.get("text") or "").strip()
        cmd = txt.split()[0].split("@")[0].lower() if txt else ""
        ts = pd.Timestamp(m["date"], unit="s", tz="UTC")
        try:
            if cmd == "/buy":
                everything = any(w.lower() in ("alle", "all") for w in txt.split()[1:])
                res = apply_pending(fill_time=ts, overrides=parse_overrides(txt), all_slots=everything)
                if not res:
                    send("ℹ️ Keine offenen Orders – nichts gebucht.")
                    continue
                orders, cash, pot = res
                ol = [f"✅ <b>Gebucht</b> ({ts.tz_convert('Europe/Berlin'):%d.%m. %H:%M})"]
                for o in orders:
                    ol.append(f"{'🔴' if o['shares'] < 0 else '🟢'} {abs(o['shares']):g} × {o['name']} "
                              f"à {o['price']:.2f} € = {eur(abs(o['shares']) * o['price'])}")
                ol.append(f"Konto: {eur(cash)}{' (Kredit)' if cash < 0 else ''} · Verlusttopf: {eur(pot)}")
                send("\n".join(ol))
            elif cmd == "/status":
                send(status_text())
        except Exception as e:
            send(f"⚠️ Fehler bei {cmd}: {e}")
    if max_id != last:
        with engine.begin() as c:
            c.execute(text("UPDATE trading.trend_bot SET last_update_id = :u WHERE id = 1"), dict(u=max_id))


# ---------------------------------------------------------------- Kern
def run(preview=False, force=False):
    today = today_berlin()
    if not (force or preview) and not is_last_trading_day_of_month(today):
        print(f"{today}: nicht der letzte Xetra-Handelstag des Monats — nichts zu tun.")
        return
    with engine.connect() as c:
        acc = c.execute(text("SELECT * FROM trading.trend_account WHERE id = 1")).mappings().first()
        if acc is None:
            sys.exit("trend_account fehlt — zuerst --init ausfuehren.")
        done = c.execute(text("SELECT run_date FROM trading.trend_runs WHERE date_trunc('month', run_date) = "
                              "date_trunc('month', CAST(:d AS date))"), dict(d=today)).scalar()
        lots = pd.read_sql(text("SELECT * FROM trading.trend_lots ORDER BY buy_date, id"), c)
    if done and not (force or preview):
        print(f"Fuer {today:%m/%Y} gibt es schon einen Lauf ({done}) — nichts zu tun.")
        return

    stale = 0 if preview else apply_pending(fill_time=None)
    if stale:
        send(f"⚠️ {stale} Order(s) vom letzten Signal waren ohne /buy – zum Signalkurs gebucht.")
        with engine.connect() as c:
            acc = c.execute(text("SELECT * FROM trading.trend_account WHERE id = 1")).mappings().first()
            lots = pd.read_sql(text("SELECT * FROM trading.trend_lots ORDER BY buy_date, id"), c)

    px = load_prices()
    adj, raw = px["adj"], px["raw"]
    last_bar = raw.dropna(how="all").index[-1].date()
    if last_bar != today and not (force or preview):
        msg = f"⚠️ Trendfolge: Yahoo hat noch keinen Schlusskurs für {today} (letzter: {last_bar})."
        print(msg)
        if pd.Timestamp.now(tz="UTC").hour >= 20:   # letzter Versuch des Tages -> melden
            send(msg + " Lauf manuell mit --force wiederholen.")
        return
    price = {s: float(raw[s].dropna().iloc[-1]) for s in raw.columns}
    warn = []
    sig = signals(adj, today, warn)

    cash = float(acc["cash_eur"]); pot = float(acc["verlusttopf_eur"])
    notes = []
    # Kreditzinsen seit der letzten Buchung (Schaetzung, zero bucht quartalsweise)
    days = (today - acc["last_booking_date"]).days
    if cash < 0 and days > 0:
        interest = -cash * float(acc["kredit_zins_pa"]) * days / 365
        cash -= interest
        notes.append(f"Kreditzinsen ~{interest:.2f} € gebucht")
    # Ausschuettungen der gehaltenen Stuecke (Dist-ETFs) seit der letzten Buchung
    holdings = lots.groupby("ticker")["shares"].sum().to_dict() if not lots.empty else {}
    for sym, sh in holdings.items():
        dps = dividends_since(sym, acc["last_booking_date"], today)
        if dps > 0 and sh > 0:
            gross = dps * float(sh); tf = BY_TICKER[sym][5]
            taxable = gross * (1 - tf)
            use = min(max(pot, 0), taxable); pot -= taxable
            tax = (taxable - use) * TAX_RATE
            cash += gross - tax
            notes.append(f"Ausschüttung {sym.replace('.DE', '')}: {gross:.2f} €")

    value = {s: float(holdings.get(s, 0)) * price[s] for s in price}
    equity = sum(value.values()) + cash

    # --- Zielgewichte
    active = [c for c in CLASSES if sig.loc[c[2], "active"]]
    n = len(active)
    w = {c[2]: min(1 / n, c[4]) for c in active} if n else {}
    lev = float(acc["hebel"]) if acc["kredit_aktiv"] else 1.0
    tgt_val = {s: equity * wi * lev for s, wi in w.items()}
    total = sum(tgt_val.values())
    debt = total - equity
    if debt > 0:
        bw_sum = sum(tgt_val[s] * BY_TICKER[s][6] for s in tgt_val)
        scale = 1.0
        rahmen = float(acc["kredit_rahmen_eur"])
        if debt > rahmen:
            scale = min(scale, (equity + rahmen) / total)
        ausl = float(acc["max_beleihung_auslastung"])
        denom = total - ausl * bw_sum
        if denom > 0 and total - equity > ausl * bw_sum:
            scale = min(scale, equity / denom)
        if scale < 1:
            notes.append(f"Hebel begrenzt (Kreditrahmen/Beleihung): Faktor {scale:.2f}")
            tgt_val = {s: v * scale for s, v in tgt_val.items()}

    # --- Orders fuer die Klassen-ETFs (ganze Stuecke)
    orders = []
    for key, name, sym, isin, cap, tf, bw in CLASSES:
        cur = float(holdings.get(sym, 0))
        t = math.floor(tgt_val.get(sym, 0) / price[sym])
        d = t - cur
        if d == 0:
            continue
        switch = (cur == 0) or (t == 0)
        if not switch and abs(d) * price[sym] < float(acc["min_order_eur"]):
            continue   # kleine Rebalancings sparen (Ordergebuehr < 500 €, Spread)
        orders.append(dict(sym=sym, isin=isin, name=name, shares=d, price=price[sym]))

    # --- Geldmarkt als Rest
    liquid = cash + value[MMF[2]] - sum(o["shares"] * o["price"] for o in orders)
    pm = price[MMF[2]]; cur_m = float(holdings.get(MMF[2], 0))
    if liquid > 0:
        t_m = math.floor(max(liquid - MMF_BUFFER * equity, 0) / pm)
    else:
        t_m = 0
    d_m = t_m - cur_m
    if abs(d_m) * pm >= float(acc["min_order_eur"]) or (t_m == 0 and cur_m > 0):
        orders.append(dict(sym=MMF[2], isin=MMF[3], name=MMF[1], shares=d_m, price=pm))

    # --- Buchung simulieren (fuer die Vorschau-Zahlen; echt gebucht wird erst bei /buy)
    new_lots, pot_after, cash_after = book(lots, orders, pot, cash, today)

    new_hold = new_lots.groupby("ticker")["shares"].sum().to_dict()
    invested = sum(float(new_hold.get(c[2], 0)) * price[c[2]] for c in CLASSES)
    lev_eff = invested / equity if equity > 0 else 0

    # --- Nachricht
    head = "🔎 <b>VORSCHAU</b> (nicht gespeichert)\n" if preview else ""
    lines = [head + f"📈 <b>Trendfolge – Signal {today:%d.%m.%Y}</b>",
             f"Gehandelt wird am <b>{WOCHENTAG[next_trading_day(today).weekday()]} "
             f"{next_trading_day(today):%d.%m.}</b> in zwei Runden (siehe unten). Limit-Order knapp über Ask "
             "(Kauf) bzw. unter Bid (Verkauf), ganze Stücke."]
    sl = ["<b>Signale (Kurs vs. SMA10)</b>"]
    for key, name, sym, isin, cap, tf, bw in CLASSES:
        r = sig.loc[sym]
        dist = "n/a" if r["dist"] is None or pd.isna(r["dist"]) else f"{r['dist'] * 100:+.1f}%"
        sl.append(f"{'🟢' if r['active'] else '⚪'} {name}: <code>{dist}</code>")
    lines.append("\n".join(sl))
    if orders:
        spreads = slot_spreads()
        for o in orders:
            if o["sym"] == MMF[2]:
                # Geldmarkt-Verkauf finanziert alles -> Runde 1; ein Geldmarkt-Kauf (Rest) -> zuletzt in Runde 2
                o["slot"] = 1 if o["shares"] < 0 else 2
            else:
                o["slot"] = choose_slot(o["sym"], spreads)
            sp = spreads.get(o["sym"], {}).get(o["slot"])
            o["spread"] = sp[0] if sp and sp[1] >= SLOT_MIN_SAMPLES else None
        for k in (1, 2):
            grp = [o for o in orders if o["slot"] == k]
            if not grp:
                continue
            # Reihenfolge: Geldmarkt-Verkauf, andere Verkaeufe, Kaeufe, Geldmarkt-Kauf
            grp.sort(key=lambda o: (0 if (o["sym"] == MMF[2] and o["shares"] < 0) else
                                    1 if o["shares"] < 0 else 3 if o["sym"] == MMF[2] else 2))
            ol = [f"{'🕙' if k == 1 else '🕓'} <b>Runde {k}: {SLOTS[k][0]}–{SLOTS[k][1]} Uhr</b>"]
            for o in grp:
                side = "🔴 VERKAUF" if o["shares"] < 0 else "🟢 KAUF"
                amt = abs(o["shares"]) * o["price"]
                sp = (f" · Spread üblich {o['spread'] * 100:.2f}% ≈ {amt * o['spread'] / 2:.0f} €"
                      if o["spread"] is not None else "")
                ol.append(f"{side} {abs(o['shares']):g} × {o['name']}\n"
                          f"├ ISIN: <code>{o['isin']}</code>\n"
                          f"└ ~{o['price']:.2f} € = {eur(amt)}{sp}")
            lines.append("\n".join(ol))
        if not any(o["spread"] is not None for o in orders):
            lines.append("ℹ️ Runden-Zuordnung noch nach Voreinstellung (US-Werte + Immobilien in Runde 2). "
                         "Sobald genug gettex-Spreads gemessen sind, wählt das System je ETF die günstigere Runde. "
                         "Ist der Spread in der App deutlich größer als üblich: warten, nicht kaufen.")
    else:
        lines.append("<b>Keine Orders</b> – alles bleibt wie es ist.")
    summ = [f"<b>Depot nach den Orders (Schätzung)</b>",
            f"├ Eigenkapital: {eur(equity)}",
            f"├ Investiert: {eur(invested)} ({lev_eff * 100:.0f} % des EK, {n}/12 Klassen aktiv)",
            f"├ Kredit genutzt: {eur(max(-cash_after, 0))} von {eur(float(acc['kredit_rahmen_eur']))}",
            f"└ Verlusttopf (geschätzt): {eur(pot_after)}"]
    if notes:
        summ.append("ℹ️ " + "; ".join(notes))
    for w_ in warn:
        summ.append("⚠️ " + w_)
    if pot_after <= 0 < pot:
        summ.append("ℹ️ Verlusttopf aufgebraucht – ab jetzt fällt auf Gewinne Abgeltungsteuer an.")
    lines.append("\n".join(summ))
    msg = "\n\n".join(lines)
    if orders and not preview:
        msg += ("\n\nNach <b>jeder Runde /buy</b> schicken – gebucht wird die jeweils offene Runde zu den Kursen "
                "zum Zeitpunkt deiner Nachricht (<code>/buy alle</code> bucht alles auf einmal). Abweichungen: "
                "<code>/buy SPYL=16.80 XNAS=88@61.9</code> (Preis bzw. Stück@Preis).")
    isins = [o["isin"] for o in orders]
    print(msg)
    send(msg)
    if isins:
        send("📑 <b>Kopierbare ISINs:</b>\n" + "\n".join(f"<code>{i}</code>" for i in isins))

    if preview:
        return
    with engine.begin() as c:
        # nur Zinsen/Ausschuettungen buchen; die Orders selbst bucht erst /buy
        c.execute(text("UPDATE trading.trend_account SET cash_eur = :c, verlusttopf_eur = :v, "
                       "last_booking_date = :d, updated_at = now() WHERE id = 1"),
                  dict(c=cash, v=pot, d=today))
        c.execute(text("INSERT INTO trading.trend_runs VALUES (:d, :e, :i, :db, :l, :v, :n) "
                       "ON CONFLICT (run_date) DO UPDATE SET equity_eur = EXCLUDED.equity_eur, "
                       "invested_eur = EXCLUDED.invested_eur, debt_eur = EXCLUDED.debt_eur, "
                       "leverage = EXCLUDED.leverage, verlusttopf_eur = EXCLUDED.verlusttopf_eur, "
                       "n_active = EXCLUDED.n_active"),
                  dict(d=today, e=equity, i=invested, db=max(-cash_after, 0), l=lev_eff, v=pot_after, n=n))
        for sym, r in sig.iterrows():
            c.execute(text("INSERT INTO trading.trend_signals VALUES (:d, :t, :c, :s, :x, :a) "
                           "ON CONFLICT (run_date, ticker) DO NOTHING"),
                      dict(d=today, t=sym, c=r["close"], s=r["sma"],
                           x=None if r["dist"] is None or pd.isna(r["dist"]) else r["dist"], a=bool(r["active"])))
        for o in orders:
            c.execute(text("INSERT INTO trading.trend_orders (run_date, ticker, isin, side, shares, price_est, "
                           "amount_est, taxable_gain_est, status, slot) VALUES (:d, :t, :i, :s, :n, :p, :a, :g, 'pending', :sl)"),
                      dict(d=today, t=o["sym"], i=o["isin"], s="BUY" if o["shares"] > 0 else "SELL",
                           n=abs(o["shares"]), p=o["price"], a=abs(o["shares"]) * o["price"], g=o["taxable"],
                           sl=o.get("slot", 1)))
    print("Gespeichert.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--preview", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--init", action="store_true")
    ap.add_argument("--poll", action="store_true", help="Telegram-Befehle (/buy, /status) verarbeiten")
    ap.add_argument("--mmf-value", type=float)
    ap.add_argument("--mmf-shares", type=float)
    ap.add_argument("--verlusttopf", type=float, default=6000)
    a = ap.parse_args()
    if a.init:
        init(a.mmf_value, a.verlusttopf, a.mmf_shares)
    elif a.poll:
        poll_commands()
    else:
        run(preview=a.preview, force=a.force)
