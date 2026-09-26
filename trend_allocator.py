"""Monatliche Trendfolge (12 Anlageklassen, SMA10) — Order-Rechner mit Depot-Buchhaltung.

Regel (am letzten Xetra-Handelstag des Monats, nach Handelsschluss):
  * Pro ETF: Monatsschluss (ausschuettungsbereinigt) > Durchschnitt der letzten 10
    Monatsschluesse (SMA10)  -> aktiv, sonst inaktiv.
  * Aktive Klassen bekommen je min(1/n_aktiv, Obergrenze) des Eigenkapitals
    (Obergrenze 12,5 %, Quanten 7,5 %), multipliziert mit dem Hebel.
  * Hebel (Wertpapierkredit) nur solange der geschaetzte Verlusttopf > 0 ist, begrenzt
    durch den Kreditrahmen und max. 80 % des Beleihungswerts. Danach 1x.
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
    ("REIT",    "Immobilien",                     "IQQ6.DE", "IE00B1FZS350", 0.125, 0.30, 0.00),
    ("COMM",    "Rohstoffe",                      "SXRS.DE", "IE00BDFL4P12", 0.125, 0.00, 0.75),
    ("GOLD",    "Gold (ETC)",                     "PPFB.DE", "IE00B4ND3602", 0.125, 0.00, 0.00),
    ("UST_L",   "US-Staatsanl. 20+J EUR-hedged",  "IUSV.DE", "IE00BD8PGZ49", 0.125, 0.00, 0.75),
    ("UST_M",   "US-Staatsanl. 7-10J EUR-hedged", "IBB1.DE", "IE00BGPP6697", 0.125, 0.00, 0.75),
    ("CORP",    "EUR-Unternehmensanleihen",       "D5BG.DE", "LU0478205379", 0.125, 0.00, 0.75),
    ("BUND",    "Bundesanleihen",                 "X03G.DE", "LU0468896575", 0.125, 0.00, 0.75),
    ("QUANT",   "Quanten-Computing",              "QUTM.DE", "IE0007Y8Y157", 0.075, 0.30, 0.75),
    ("NDX",     "Nasdaq-100",                     "XNAS.DE", "IE00BMFKG444", 0.125, 0.30, 0.75),
]
MMF = ("MMF", "Geldmarkt (Amundi Smart Overnight)", "LYOR.DE", "LU1190417599", None, 0.00, 0.75)
ALL = CLASSES + [MMF]
BY_TICKER = {c[2]: c for c in ALL}

SMA_MONTHS = 10
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
    shares numeric, price_est numeric, amount_est numeric, taxable_gain_est numeric
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


def signals(adj, today):
    rows = []
    for key, name, sym, isin, cap, tf, bw in CLASSES:
        s = adj[sym].dropna()
        # Tagesausreisser (> 25 % gegen Vortag und Folgetag zurueck) ignorieren
        r = s.pct_change()
        bad = (r.abs() > 0.25) & (r.shift(-1).abs() > 0.2)
        s = s[~bad]
        m = s.resample("ME").last().dropna()
        m = m[m.index.date <= pd.Timestamp(today).to_period("M").end_time.date()]
        if len(m) < SMA_MONTHS:
            rows.append(dict(sym=sym, close=float(s.iloc[-1]), sma=None, dist=None, active=False, note="zu kurze Historie"))
            continue
        sma = float(m.iloc[-SMA_MONTHS:].mean())
        close = float(m.iloc[-1])
        rows.append(dict(sym=sym, close=close, sma=sma, dist=close / sma - 1, active=close > sma, note=""))
    return pd.DataFrame(rows).set_index("sym")


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
    sig = signals(adj, today)

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
    lev = float(acc["hebel"]) if (acc["kredit_aktiv"] and pot > 0) else 1.0
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

    # --- Buchung (FIFO) und steuerpflichtige Gewinne
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
                                  shares=o["shares"], buy_date=today, buy_price=o["price"])])], ignore_index=True)
        cash -= o["shares"] * o["price"]
    tax = max(realized - max(pot, 0), 0) * TAX_RATE if realized > 0 else 0.0
    pot -= realized
    cash -= tax
    new_lots = new_lots[new_lots.shares > 1e-9]

    new_hold = new_lots.groupby("ticker")["shares"].sum().to_dict()
    invested = sum(float(new_hold.get(c[2], 0)) * price[c[2]] for c in CLASSES)
    lev_eff = invested / equity if equity > 0 else 0

    # --- Nachricht
    head = "🔎 <b>VORSCHAU</b> (nicht gespeichert)\n" if preview else ""
    lines = [head + f"📈 <b>Trendfolge – Signal {today:%d.%m.%Y}</b>",
             "Handeln am nächsten Handelstag, ganze Stücke, Limit ≈ Kurs."]
    sl = ["<b>Signale (Kurs vs. SMA10)</b>"]
    for key, name, sym, isin, cap, tf, bw in CLASSES:
        r = sig.loc[sym]
        dist = "n/a" if r["dist"] is None or pd.isna(r["dist"]) else f"{r['dist'] * 100:+.1f}%"
        sl.append(f"{'🟢' if r['active'] else '⚪'} {name}: <code>{dist}</code>")
    lines.append("\n".join(sl))
    if orders:
        ol = ["<b>Orders</b> (erst verkaufen, dann kaufen)"]
        for o in sorted(orders, key=lambda o: o["shares"] > 0):
            side = "🔴 VERKAUF" if o["shares"] < 0 else "🟢 KAUF"
            ol.append(f"{side} {abs(o['shares']):g} × {o['name']}\n"
                      f"├ ISIN: <code>{o['isin']}</code>\n"
                      f"└ ~{o['price']:.2f} € = {eur(abs(o['shares']) * o['price'])}")
        lines.append("\n".join(ol))
    else:
        lines.append("<b>Keine Orders</b> – alles bleibt wie es ist.")
    summ = [f"<b>Depot nach den Orders (Schätzung)</b>",
            f"├ Eigenkapital: {eur(equity)}",
            f"├ Investiert: {eur(invested)} ({lev_eff * 100:.0f} % des EK, {n}/12 Klassen aktiv)",
            f"├ Kredit genutzt: {eur(max(-cash, 0))} von {eur(float(acc['kredit_rahmen_eur']))}",
            f"└ Verlusttopf (geschätzt): {eur(pot)}"]
    if notes:
        summ.append("ℹ️ " + "; ".join(notes))
    if pot <= 0 and acc["kredit_aktiv"]:
        summ.append("⚠️ Verlusttopf aufgebraucht → ab nächstem Monat 1x, der Kredit wird abgebaut.")
    lines.append("\n".join(summ))
    msg = "\n\n".join(lines)
    isins = [o["isin"] for o in orders]
    print(msg)
    send(msg)
    if isins:
        send("📑 <b>Kopierbare ISINs:</b>\n" + "\n".join(f"<code>{i}</code>" for i in isins))

    if preview:
        return
    with engine.begin() as c:
        c.execute(text("DELETE FROM trading.trend_lots"))
        for _, l in new_lots.iterrows():
            c.execute(text("INSERT INTO trading.trend_lots (ticker, isin, shares, buy_date, buy_price) "
                           "VALUES (:t, :i, :s, :d, :p)"),
                      dict(t=l.ticker, i=l.isin, s=float(l.shares), d=l.buy_date, p=float(l.buy_price)))
        c.execute(text("UPDATE trading.trend_account SET cash_eur = :c, verlusttopf_eur = :v, "
                       "last_booking_date = :d, updated_at = now() WHERE id = 1"),
                  dict(c=cash, v=pot, d=today))
        c.execute(text("INSERT INTO trading.trend_runs VALUES (:d, :e, :i, :db, :l, :v, :n) "
                       "ON CONFLICT (run_date) DO UPDATE SET equity_eur = EXCLUDED.equity_eur, "
                       "invested_eur = EXCLUDED.invested_eur, debt_eur = EXCLUDED.debt_eur, "
                       "leverage = EXCLUDED.leverage, verlusttopf_eur = EXCLUDED.verlusttopf_eur, "
                       "n_active = EXCLUDED.n_active"),
                  dict(d=today, e=equity, i=invested, db=max(-cash, 0), l=lev_eff, v=pot, n=n))
        for sym, r in sig.iterrows():
            c.execute(text("INSERT INTO trading.trend_signals VALUES (:d, :t, :c, :s, :x, :a) "
                           "ON CONFLICT (run_date, ticker) DO NOTHING"),
                      dict(d=today, t=sym, c=r["close"], s=r["sma"],
                           x=None if r["dist"] is None or pd.isna(r["dist"]) else r["dist"], a=bool(r["active"])))
        for o in orders:
            c.execute(text("INSERT INTO trading.trend_orders (run_date, ticker, isin, side, shares, price_est, "
                           "amount_est, taxable_gain_est) VALUES (:d, :t, :i, :s, :n, :p, :a, :g)"),
                      dict(d=today, t=o["sym"], i=o["isin"], s="BUY" if o["shares"] > 0 else "SELL",
                           n=abs(o["shares"]), p=o["price"], a=abs(o["shares"]) * o["price"], g=o["taxable"]))
    print("Gespeichert.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--preview", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--init", action="store_true")
    ap.add_argument("--mmf-value", type=float)
    ap.add_argument("--mmf-shares", type=float)
    ap.add_argument("--verlusttopf", type=float, default=6000)
    a = ap.parse_args()
    if a.init:
        init(a.mmf_value, a.verlusttopf, a.mmf_shares)
    else:
        run(preview=a.preview, force=a.force)
