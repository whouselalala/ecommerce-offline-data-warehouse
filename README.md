# Ecommerce Offline Warehouse (Spark Local MVP)

**Scope:** deterministic local demonstration of ODS → DWD → DWS → ADS. This is **not** a deployed Hive/HDFS/DolphinScheduler cluster. SQL under `sql/` and scheduling under `dags/` are integration references.

## Run
Requirements: Docker Engine + Compose, or Java 17 + Python 3.10+ for local PySpark.

```bash
docker compose up --build --abort-on-container-exit
# or (with Java installed):
python -m pip install -r requirements.txt
python -m warehouse.pipeline --date 2026-10-01
python -m pytest -q
```

View `output/ads_trade_daily/dt=2026-10-01/` for CSV results.

## Metric definitions
- Paid orders: unique order_ids with successful payment on UTC business date.
- Paid GMV: sum of those payments, in **integer cents** (not a placed-order GMV).
- Refunded amount: successful refund transactions **completed on that UTC date**, independent of the payment date.
- Net daily cash movement: same-day paid GMV minus same-day refund amount. Not a cohort net GMV.

Fixture checks on 2026-10-01: 3 paid orders, 25000 cents paid, 2000 cents refunded, 23000 cents net.

## Limits / next stages
CSV sources represent full snapshots. `mode(overwrite)` rewrites ODS/DWD/DWS on every run; a real warehouse must use incremental ingestion, partition-scoped overwrite, validation of duplicate/change events, schema evolution, run metadata, quality gates and scheduler backfills. The demo rejects multiple successful payments per order; production would need an explicit split-payment business rule.
