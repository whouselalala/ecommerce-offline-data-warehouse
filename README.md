# Ecommerce Offline Data Warehouse

Local Spark 3.5 demo with CSV fixtures, ODS/DWD/DWS/ADS Parquet layers, daily paid order totals, refunds and net receipts. The local demo does not deploy HDFS, Hive Metastore or DolphinScheduler.

Run: `docker compose up --build --abort-on-container-exit`; output in `output/ads_trade_daily/`.

Or: `pip install -r requirements.txt && python -m warehouse.pipeline --date 2026-10-01 && python -m pytest -q`.

Amounts are integer cents; paid GMV excludes refunds; completed refunds are recognized on their completion date. UTC timestamps.
