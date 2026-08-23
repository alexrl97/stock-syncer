# stock-syncer

Nasdaq/ETF 15-Minuten-Sync: zieht Kursdaten (yfinance), schreibt in Postgres und sendet
Telegram-Signale (inkl. QQQM-Dedup-Logik, Status wird zurueckcommittet).

Getriggert per externem Cron via `repository_dispatch` (event_type: `market_trigger`).

Benoetigte Actions-Secrets: `DATABASE_URL`, `TELEGRAM_TOKEN`, `ETF_TELEGRAM_TOKEN`, `CHAT_ID`.
