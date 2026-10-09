import csv
from pathlib import Path

def test_fixture_payment_contract():
    base = Path(__file__).resolve().parents[1] / 'data'
    with (base / 'payments.csv').open() as f:
        paid = [r for r in csv.DictReader(f) if r['status'] == 'SUCCESS']
    with (base / 'refunds.csv').open() as f:
        refunds = [r for r in csv.DictReader(f) if r['status'] == 'SUCCESS']
    assert len(paid) == 3
    assert sum(int(r['amount_cent']) for r in paid) == 25000
    assert sum(int(r['amount_cent']) for r in refunds) == 2000
