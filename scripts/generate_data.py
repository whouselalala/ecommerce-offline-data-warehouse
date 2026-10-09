from pathlib import Path
import csv

base = Path(__file__).resolve().parents[1] / 'data'
base.mkdir(exist_ok=True)
fixtures = {
 'orders.csv': (['order_id', 'user_id', 'created_at'], [
  ['o1','u1','2026-10-01T08:00:00Z'], ['o2','u2','2026-10-01T08:30:00Z'],
  ['o3','u1','2026-10-01T09:00:00Z'], ['o4','u3','2026-10-01T10:00:00Z']]),
 'payments.csv': (['payment_id','order_id','amount_cent','status','paid_at'], [
  ['p1','o1','12000','SUCCESS','2026-10-01T08:03:00Z'],
  ['p2','o2','8000','SUCCESS','2026-10-01T08:33:00Z'],
  ['p3','o3','10000','FAILED','2026-10-01T09:05:00Z'],
  ['p4','o4','5000','SUCCESS','2026-10-01T10:05:00Z']]),
 'refunds.csv': (['refund_id','order_id','amount_cent','status','refunded_at'], [
  ['r1','o2','2000','SUCCESS','2026-10-01T11:00:00Z'],
  ['r2','o4','1000','PENDING','2026-10-01T11:30:00Z']])
}
for name, (header, rows) in fixtures.items():
 with (base/name).open('w', newline='', encoding='utf8') as f:
  w = csv.writer(f)
  w.writerow(header)
  w.writerows(rows)
print('Generated fixtures in', base)
