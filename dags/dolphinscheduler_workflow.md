# DolphinScheduler workflow example
1. ingest-orders: import incremental source data.
2. spark-dwd-dws-ads: spark-submit warehouse/pipeline.py --date ${system.biz.date}.
3. quality-check: compare source and calculated control totals.
4. publish: mark partition ready after quality passes.
Configure task dependencies, retries, SLA alerts and backfills.
