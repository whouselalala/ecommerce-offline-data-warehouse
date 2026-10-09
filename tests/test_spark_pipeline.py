import pytest
pytest.importorskip("pyspark")
from pathlib import Path
from pyspark.sql import SparkSession
from warehouse.pipeline import build

@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test-warehouse")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()

def test_paid_and_refund_metrics(spark, tmp_path):
    src = Path(__file__).resolve().parents[1] / "data"
    (tmp_path / "data").symlink_to(src, target_is_directory=True)
    actual = build(spark, tmp_path, "2026-10-01").first().asDict()
    assert (actual["paid_orders"], actual["paid_gmv_cent"], actual["refunded_cent"], actual["net_receipts_cent"]) == (3,25000,2000,23000)
    assert build(spark, tmp_path, "2026-10-02").first()["paid_gmv_cent"] == 0
    assert (tmp_path / "output" / "ads_trade_daily" / "dt=2026-10-01").exists()

def test_invalid_date(spark, tmp_path):
    with pytest.raises(ValueError):
        build(spark, tmp_path, "not-a-date")
