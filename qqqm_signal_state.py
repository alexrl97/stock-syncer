"""
QQQM-Signal-Dedup-Status (ausgelagert aus stock_syncer.py, damit testbar ohne die
schweren Abhaengigkeiten des Hauptskripts).

Zweck: Anti-Spam. Es wird pro Signal-Datum (= letzte abgeschlossene Tageskerze) hoechstens
EIN Kaufsignal gesendet. Das zuletzt gesendete Datum wird in qqqm_last_signal.txt gespeichert.

WICHTIG: Diese Datei muss vom CI-Workflow nach jedem Lauf zurueck-committet werden, sonst
liest jeder Lauf den alten (eingecheckten) Stand und sendet erneut -> Spam alle 15 min.
"""
import os
from datetime import datetime, timedelta

_DIR = os.path.dirname(os.path.abspath(__file__))
QQQM_SIGNAL_FILE = os.path.join(_DIR, "qqqm_last_signal.txt")
QQQM_EXIT_FILE = os.path.join(_DIR, "qqqm_exit_date.txt")
EXIT_COOLDOWN_TRADING_DAYS = 5  # nach dem Ausstieg (Auskommentieren) so lange keine Signale


def load_last_signal(filepath=QQQM_SIGNAL_FILE):
    """Datum (YYYY-MM-DD) des zuletzt gesendeten QQQM-Kaufsignals oder None."""
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return f.read().strip()
    return None


def save_last_signal(datestr, filepath=QQQM_SIGNAL_FILE):
    """Speichert das zuletzt gesendete Signal-Datum."""
    with open(filepath, "w") as f:
        f.write(str(datestr))


def should_send(sig_date, filepath=QQQM_SIGNAL_FILE):
    """True, wenn fuer sig_date noch KEIN Signal gesendet wurde (max. 1 pro Signal-Tag)."""
    return load_last_signal(filepath) != str(sig_date)


# --- Exit-Cooldown: nach dem Ausstieg (QQQM.US aus held_positions auskommentiert) erst nach
#     EXIT_COOLDOWN_TRADING_DAYS Handelstagen wieder Signale. Zaehlt ab DEINEM Ausstieg,
#     nicht ab dem Modell-Stop. Auch diese Datei muss der Workflow zurueck-committen.

def load_exit_date(filepath=QQQM_EXIT_FILE):
    if os.path.exists(filepath):
        s = open(filepath, "r").read().strip()
        return s or None
    return None


def save_exit_date(datestr, filepath=QQQM_EXIT_FILE):
    with open(filepath, "w") as f:
        f.write(str(datestr))


def clear_exit_date(filepath=QQQM_EXIT_FILE):
    if os.path.exists(filepath):
        os.remove(filepath)


def _to_date(s):
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


def trading_days_between(start, end):
    """Handelstage (Mo-Fr, ohne Feiertage) zwischen start (exkl.) und end (inkl.)."""
    s, e = _to_date(start), _to_date(end)
    if e <= s:
        return 0
    n, d = 0, s + timedelta(days=1)
    while d <= e:
        if d.weekday() < 5:  # 0-4 = Mo-Fr
            n += 1
        d += timedelta(days=1)
    return n


def update_exit_state(qqqm_held, today, filepath=QQQM_EXIT_FILE):
    """
    Haelt die Exit-Datei aktuell:
      - gehalten            -> Exit-Datum loeschen (du bist wieder drin)
      - NICHT gehalten,
        noch kein Datum     -> `today` als Ausstiegs-Datum setzen
        Datum vorhanden     -> unveraendert lassen
    Gibt das aktuelle Exit-Datum zurueck (oder None).
    """
    if qqqm_held:
        clear_exit_date(filepath)
        return None
    ex = load_exit_date(filepath)
    if ex is None:
        save_exit_date(today, filepath)
        return str(today)
    return ex


def exit_cooldown_active(exit_date, today, cooldown=EXIT_COOLDOWN_TRADING_DAYS):
    """True = noch im Cooldown (weniger als `cooldown` Handelstage seit dem Ausstieg)."""
    if not exit_date:
        return False
    return trading_days_between(exit_date, today) < cooldown
