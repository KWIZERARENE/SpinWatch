"""Trim SQLite mirror to WM_0001..WM_1000 only."""
import sqlite3, os

db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "laundry_ops.sqlite3")
conn = sqlite3.connect(db)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM machine_status")
print(f"Before: {cur.fetchone()[0]}")

cur.execute("""
    DELETE FROM machine_status
    WHERE CAST(SUBSTR(machine_id, 4) AS INTEGER) > 1000
    OR CAST(SUBSTR(machine_id, 4) AS INTEGER) = 0
    OR machine_id NOT LIKE 'WM_%'
""")
print(f"Deleted: {cur.rowcount}")
conn.commit()

cur.execute("SELECT COUNT(*) FROM machine_status")
print(f"After: {cur.fetchone()[0]}")
conn.close()
print("Done.")
