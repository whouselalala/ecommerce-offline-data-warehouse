-- Sample Hive external table for configured Hive/HDFS environment.
CREATE EXTERNAL TABLE IF NOT EXISTS ads_trade_daily (paid_orders BIGINT, paid_gmv_cent BIGINT, refunded_cent BIGINT, net_receipts_cent BIGINT) PARTITIONED BY (dt STRING) STORED AS PARQUET LOCATION 'hdfs:///warehouse/ads_trade_daily';
