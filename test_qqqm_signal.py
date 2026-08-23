"""
Test der QQQM-Signal-Dedup-Logik (Anti-Spam). Laeuft ohne die schweren Deps des
Hauptskripts (nur qqqm_signal_state). Start: python test_qqqm_signal.py
"""
import os
import tempfile
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import qqqm_signal_state as q

passed = failed = 0


def check(name, cond):
    global passed, failed
    if cond:
        print("  ok  ", name)
        passed += 1
    else:
        print("  FAIL", name)
        failed += 1


tmp = os.path.join(tempfile.gettempdir(), "qqqm_test_signal.txt")
if os.path.exists(tmp):
    os.remove(tmp)

# 1) Ohne Datei: kein letztes Signal -> senden erlaubt
check("kein File -> load None", q.load_last_signal(tmp) is None)
check("kein File -> should_send True", q.should_send("2026-08-20", tmp) is True)

# 2) Speichern + Laden (das FILE-Mechanismus, den der User verdaechtigt)
q.save_last_signal("2026-08-20", tmp)
check("roundtrip: load == gespeichert", q.load_last_signal(tmp) == "2026-08-20")

# 3) ANTI-SPAM: derselbe Signal-Tag -> KEIN zweites Signal (das war der Bug: alle 15 min)
check("selber Tag -> should_send False (kein Spam)", q.should_send("2026-08-20", tmp) is False)

# 4) MAX 1/TAG: neuer Signal-Tag -> genau ein neues Signal erlaubt
check("neuer Tag -> should_send True", q.should_send("2026-08-21", tmp) is True)
q.save_last_signal("2026-08-21", tmp)
check("nach Senden am neuen Tag -> selber Tag wieder False", q.should_send("2026-08-21", tmp) is False)

# 5) date-Objekt statt String wird sauber verglichen (fetch_qqqm nutzt str(...), hier robust)
q.save_last_signal("2026-08-22", tmp)
check("Vergleich mit str() konsistent", q.should_send("2026-08-22", tmp) is False)

os.remove(tmp)

# ------------------------------------------------------------------
# Exit-Cooldown (5 Handelstage ab DEINEM Ausstieg)
print("\n--- Exit-Cooldown ---")

# trading_days_between: nur Mo-Fr, start exklusiv, end inklusiv
check("Mi->Do = 1 Handelstag", q.trading_days_between("2026-08-19", "2026-08-20") == 1)
check("Fr->Mo = 1 Handelstag (WE uebersprungen)", q.trading_days_between("2026-08-21", "2026-08-24") == 1)
check("Do 20.08 -> Do 27.08 = 5 Handelstage", q.trading_days_between("2026-08-20", "2026-08-27") == 5)
check("gleicher Tag = 0", q.trading_days_between("2026-08-20", "2026-08-20") == 0)

# exit_cooldown_active
check("kein Exit-Datum -> kein Cooldown", q.exit_cooldown_active(None, "2026-08-25") is False)
check("2 Handelstage nach Ausstieg -> Cooldown aktiv", q.exit_cooldown_active("2026-08-20", "2026-08-24") is True)  # 2 < 5
check("5 Handelstage nach Ausstieg -> Cooldown vorbei", q.exit_cooldown_active("2026-08-20", "2026-08-27") is False)  # 5 >= 5

# update_exit_state: File-Verhalten
extmp = os.path.join(tempfile.gettempdir(), "qqqm_test_exit.txt")
if os.path.exists(extmp):
    os.remove(extmp)
check("nicht gehalten + kein Datum -> setzt heute", q.update_exit_state(False, "2026-08-20", extmp) == "2026-08-20")
check("nicht gehalten + Datum da -> behaelt (kein Reset auf heute)", q.update_exit_state(False, "2026-08-25", extmp) == "2026-08-20")
check("gehalten -> Datum geloescht (None)", q.update_exit_state(True, "2026-08-25", extmp) is None)
check("nach Reset: Datei weg", not os.path.exists(extmp))
check("wieder ausgestiegen -> neues Datum", q.update_exit_state(False, "2026-08-26", extmp) == "2026-08-26")
os.remove(extmp)

print(f"\n{passed} ok, {failed} fehlgeschlagen")
sys.exit(1 if failed else 0)
