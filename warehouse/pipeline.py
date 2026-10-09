"""Deterministic local Spark warehouse (paid-order and refund-day metrics)."""
import argparse
from datetime import date as Date
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parents[1]

def build(spark, base: Path, date: str):
    Date.fromisoformat(date)
    source = Path(base) / "data"
    output = Path(base) / "output"
    output.mkdir(parents=True, exist_ok=True)
    read = lambda name: spark.read.option("header", True).csv(str(source / (name + ".csv")))
    orders, payments, refunds = read("orders"), read("payments"), read("refunds")
    # ODS files are unmodified source snapshots, not partition snapshots for all dates.
    for name, df in (("orders", orders), ("payments", payments), ("refunds", refunds)):
        df.write.mode("overwrite").parquet(str(output / "ods" / name))
    order_dim = orders.dropDuplicates(["order_id"]).select("order_id", "user_id")
    successful = (
        payments.filter(F.col("status") == "SUCCESS")
        .withColumn("amount_cent", F.col("amount_cent").cast("long"))
        .withColumn("paid_ts", F.to_timestamp("paid_at"))
        .filter("payment_id IS NOT NULL AND order_id IS NOT NULL AND amount_cent >= 0 AND paid_ts IS NOT NULL")
        .dropDuplicates(["payment_id"])
    )
    # Fixture contract: one successful payment per order; reject bad source rather than silently lose revenue.
    duplicate_order = successful.groupBy("order_id").count().filter("count > 1").limit(1).count()
    if duplicate_order:
        raise ValueError("Multiple successful payments for one order; source payment model requires reconciliation")
    dwd = (successful.join(order_dim, "order_id", "inner")
           .withColumn("dt", F.to_date("paid_ts"))
           .select("payment_id", "order_id", "user_id", "amount_cent", "paid_ts", "dt"))
    dwd.write.mode("overwrite").partitionBy("dt").parquet(str(output / "dwd" / "paid_orders"))
    dws = (dwd.groupBy("dt", "user_id")
           .agg(F.countDistinct("order_id").alias("order_count"), F.sum("amount_cent").alias("paid_cent")))
    dws.write.mode("overwrite").partitionBy("dt").parquet(str(output / "dws" / "user_trade_daily"))
    totals = (dws.filter(F.col("dt") == F.lit(date)).groupBy("dt")
              .agg(F.sum("order_count").alias("paid_orders"), F.sum("paid_cent").alias("paid_gmv_cent")))
    valid_refunds = (refunds.filter("status = 'SUCCESS'")
        .withColumn("amount_cent", F.col("amount_cent").cast("long"))
        .withColumn("dt", F.to_date(F.to_timestamp("refunded_at")))
        .filter("refund_id IS NOT NULL AND amount_cent >= 0 AND dt IS NOT NULL")
        .dropDuplicates(["refund_id"]))
    valid_refunds.write.mode("overwrite").partitionBy("dt").parquet(str(output / "dwd" / "refunds"))
    refunds_by_day = (valid_refunds.filter(F.col("dt") == F.lit(date))
                      .groupBy("dt").agg(F.sum("amount_cent").alias("refunded_cent")))
    # Seed desired date ensures 0-row days still produce a report.
    report_day = spark.range(1).select(F.to_date(F.lit(date)).alias("dt"))
    result = (report_day.join(totals, "dt", "left").join(refunds_by_day, "dt", "left")
              .na.fill({"paid_orders": 0, "paid_gmv_cent": 0, "refunded_cent": 0})
              .withColumn("net_receipts_cent", F.col("paid_gmv_cent") - F.col("refunded_cent")))
    # Output by date avoids one day's backfill overwriting another date.
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / "ads_trade_daily" / ("dt=" + date)))
    result.show(truncate=False)
    return result

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    args = p.parse_args()
    spark = (SparkSession.builder.master("local[2]").appName("ecommerce-warehouse")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        build(spark, ROOT, args.date)
    finally:
        spark.stop()
