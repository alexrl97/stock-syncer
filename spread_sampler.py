"""Misst Bid/Ask-Spreads der Trendfolge-ETFs auf gettex und speichert sie in Neon.

Zweck: herausfinden, zu welcher Uhrzeit die Spreads am engsten sind (Handelsfenster fuer
die monatlichen Orders). Laeuft im 15-Minuten-Takt ueber den trend.yml-Workflow.

Datenquelle: das Kurs-Widget auf gettex.de (LSEG-Widget-API). Ablauf wie im Browser:
SAML-Request aus der gettex-Startseite -> Session + JWT -> quote/info mit q._BID/q._ASK.
Ports der Logik aus github.com/escalate/gettex-exchange.

Aufruf: python spread_sampler.py           # eine Messung aller ETFs
"""
import os
import re
import base64
import datetime as dt

import requests
from sqlalchemy import create_engine, text

from trend_allocator import ALL, is_trading_day

DATABASE_URL = os.environ["DATABASE_URL"]
AUTH = "https://lseg-widgets.financial.com/auth/api/v1"
REST = "https://lseg-widgets.financial.com/rest/api"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def gettex_token():
    html = requests.get("https://www.gettex.de/", headers={"User-Agent": UA}, timeout=15).text
    m = re.search(r"const samlRequest=`([\S\s.]+?)`;", html)
    if not m:
        raise RuntimeError("SAML-Request auf gettex.de nicht gefunden (Seite geaendert?)")
    r = requests.post(f"{AUTH}/sessions/samllogin?fetchToken=true",
                      headers={"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"},
                      data={"SAMLResponse": base64.b64encode(m.group(1).encode()).decode()}, timeout=15)
    r.raise_for_status()
    return r.json()["token"]


def ric_for(isin, token):
    r = requests.get(f"{REST}/find/securities", timeout=15,
                     params={"fids": "x.RIC", "search": isin, "searchFor": "ISIN", "exchanges": "GTX", "isNF": "false"},
                     headers={"Accept": "application/json", "User-Agent": UA, "jwt": token, "x-component-id": "GettexInit"})
    r.raise_for_status()
    d = r.json()
    return d["data"][0]["x.RIC"] if d.get("totalCount") else None


def quotes(rics, token):
    r = requests.get(f"{REST}/quote/info", timeout=15,
                     params={"rics": ",".join(rics), "fids": "q._BID,q._ASK,q.BIDSIZE,q.ASKSIZE,x.RIC"},
                     headers={"Accept": "application/json", "User-Agent": UA, "jwt": token, "x-component-id": "InfoMatrix"})
    r.raise_for_status()
    return r.json()["data"]


def sample():
    import pandas as pd
    now = pd.Timestamp.now(tz="Europe/Berlin")
    if not is_trading_day(now.date()) or not (8 <= now.hour < 22):   # gettex handelt 8-22 Uhr
        print(f"{now:%a %H:%M}: gettex geschlossen – keine Messung."); return
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE IF NOT EXISTS trading.gettex_spreads (
            ts timestamptz NOT NULL, isin text NOT NULL, ticker text, bid numeric, ask numeric,
            bid_size numeric, ask_size numeric, spread_pct numeric, PRIMARY KEY (ts, isin))"""))
    token = gettex_token()
    rics = {}
    for key, name, sym, isin, *_ in ALL:
        ric = ric_for(isin, token)
        if ric:
            rics[ric] = (isin, sym)
        else:
            print(f"Kein gettex-RIC fuer {isin} ({sym})")
    ts = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for q in quotes(list(rics), token):
        isin, sym = rics.get(q.get("x.RIC"), (None, None))
        bid, ask = q.get("q._BID"), q.get("q._ASK")
        if not isin or not bid or not ask:
            continue
        bid, ask = float(bid), float(ask)
        sp = (ask - bid) / ((ask + bid) / 2) if ask > 0 and bid > 0 else None
        rows.append(dict(ts=ts, isin=isin, ticker=sym, bid=bid, ask=ask, bs=q.get("q.BIDSIZE"), as_=q.get("q.ASKSIZE"), sp=sp))
    with engine.begin() as c:
        for r in rows:
            c.execute(text("INSERT INTO trading.gettex_spreads VALUES (:ts, :isin, :ticker, :bid, :ask, :bs, :as_, :sp) "
                           "ON CONFLICT DO NOTHING"), r)
    for r in rows:
        print(f"{r['ticker']:8s} bid {r['bid']:>10.4f} ask {r['ask']:>10.4f} spread {r['sp'] * 100:6.3f}%")
    print(f"{len(rows)} Quotes gespeichert ({ts:%Y-%m-%d %H:%M} UTC)")


def report(send_telegram=True):
    """Median-Spread je halbe Stunde (Berliner Zeit) ueber alle bisherigen Messungen."""
    import pandas as pd
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as c:
        df = pd.read_sql(text("SELECT ts, ticker, spread_pct FROM trading.gettex_spreads WHERE spread_pct IS NOT NULL"), c)
    if df.empty:
        print("Noch keine Messungen."); return
    df["ts"] = pd.to_datetime(df.ts, utc=True).dt.tz_convert("Europe/Berlin")
    df["slot"] = df.ts.dt.floor("30min").dt.strftime("%H:%M")
    df["tag"] = df.ts.dt.date
    piv = df.pivot_table(index="slot", columns="ticker", values="spread_pct", aggfunc="median") * 100
    piv["Ø alle"] = piv.mean(axis=1)
    print(piv.round(3).to_string())
    days = df.tag.nunique()
    best = piv["Ø alle"].nsmallest(4)
    lines = [f"📏 <b>gettex-Spreads</b> ({days} Handelstag{'e' if days != 1 else ''}, Median je halbe Stunde, Ø über 13 ETFs)"]
    for slot, v in piv["Ø alle"].items():
        mark = " ✅" if slot in best.index else ""
        lines.append(f"<code>{slot}</code> {v:5.2f}%{mark}")
    lines.append("\n<b>Je ETF im besten und schlechtesten Slot</b>")
    for t in [c for c in piv.columns if c != "Ø alle"]:
        s = piv[t].dropna()
        if len(s):
            lines.append(f"{t.split('.')[0]}: min {s.min():.2f}% ({s.idxmin()}), max {s.max():.2f}% ({s.idxmax()})")
    msg = "\n".join(lines)
    if send_telegram:
        from trend_allocator import send
        send(msg)
    return piv


if __name__ == "__main__":
    import sys
    if "--report" in sys.argv:
        report()
    else:
        sample()
