"""Spark local warehouse; integer cents and UTC date semantics."""
import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window

ROOT = Path(__file__).resolve().parents[1]

def build(spark, base: Path, date: str):
    src = base / 'data'
    dst = base / 'output'
    dst.mkdir(parents=True, exist_ok=True)
    orders = spark.read.option('header', True).csv(str(src / 'orders.csv'))
    payments = spark.read.option('header', True).csv(str(src / 'payments.csv'))
    refunds = spark.read.option('header', True).csv(str(src / 'refunds.csv'))
    for name, frame in [('orders', orders), ('payments', payments), ('refunds', refunds)]:
        frame.withColumn('ingest_dt', F.lit(date)).write.mode('overwrite').partitionBy('ingest_dt').parquet(str(dst / 'ods' / name))
    paid = (payments.filter(F.col('status') == 'SUCCESS')
        .withColumn('amount_cent', F.col('amount_cent').cast('long'))
        .withColumn('paid_ts', F.to_timestamp('paid_at')))
    paid = paid.withColumn('rn', F.row_number().over(Window.partitionBy('payment_id').orderBy(F.col('paid_ts').desc()))).filter('rn = 1').drop('rn')
    paid = paid.withColumn('rn', F.row_number().over(Window.partitionBy('order_id').orderBy(F.col('paid_ts').asc(), 'payment_id'))).filter('rn = 1').drop('rn')
    dwd = (paid.join(orders.select('order_id', 'user_id'), 'order_id', 'inner')
          .withColumn('dt', F.to_date('paid_ts'))
          .select('payment_id', 'order_id', 'user_id', 'amount_cent', 'paid_ts', 'dt'))
    dwd.write.mode('overwrite').partitionBy('dt').parquet(str(dst / 'dwd' / 'paid_orders'))
    daily = dwd.filter(F.col('dt') == F.lit(date)).groupBy('user_id', 'dt').agg(F.count('*').alias('order_count'), F.sum('amount_cent').alias('paid_cent'))
    daily.write.mode('overwrite').partitionBy('dt').parquet(str(dst / 'dws' / 'user_trade_daily'))
    totals = daily.groupBy('dt').agg(F.sum('order_count').alias('paid_orders'), F.sum('paid_cent').alias('paid_gmv_cent'))
    refund = (refunds.filter(F.col('status') == 'SUCCESS')
      .withColumn('amount_cent', F.col('amount_cent').cast('long'))
      .withColumn('dt', F.to_date(F.to_timestamp('refunded_at')))
      .filter(F.col('dt') == date).dropDuplicates(['refund_id'])
      .groupBy('dt').agg(F.sum('amount_cent').alias('refunded_cent')))
    result = (totals.join(refund, 'dt', 'full')
      .na.fill({'paid_orders': 0, 'paid_gmv_cent': 0, 'refunded_cent': 0})
      .withColumn('net_receipts_cent', F.col('paid_gmv_cent') - F.col('refunded_cent')))
    result.coalesce(1).write.mode('overwrite').option('header', True).csv(str(dst / 'ads_trade_daily'))
    result.show(truncate=False)
    return result

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--date', required=True)
    args = p.parse_args()
    import runpy
    runpy.run_path(str(ROOT / 'scripts' / 'generate_data.py'))
    spark = (SparkSession.builder.master('local[2]').appName('ecommerce-warehouse')
        .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        build(spark, ROOT, args.date)
    finally:
        spark.stop()
