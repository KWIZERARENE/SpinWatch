"""Trim MySQL machine_status and readings_log down to WM_0001..WM_1000 only."""
import mysql.connector

conn = mysql.connector.connect(host="127.0.0.1", user="root", password="", database="laundry_ops")
cur = conn.cursor()

# Delete any machine outside WM_0001 to WM_1000
cur.execute("""
    DELETE FROM machine_status
    WHERE machine_id NOT REGEXP '^WM_[0-9]{4}$'
    OR CAST(SUBSTRING(machine_id, 4) AS UNSIGNED) > 1000
""")
deleted_status = cur.rowcount
print(f"Deleted {deleted_status} rows from machine_status (kept WM_0001..WM_1000)")

cur.execute("""
    DELETE FROM readings_log
    WHERE machine_id NOT REGEXP '^WM_[0-9]{4}$'
    OR CAST(SUBSTRING(machine_id, 4) AS UNSIGNED) > 1000
""")
deleted_log = cur.rowcount
print(f"Deleted {deleted_log} rows from readings_log (kept WM_0001..WM_1000)")

conn.commit()

cur.execute("SELECT COUNT(*) FROM machine_status")
print(f"machine_status count now: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM readings_log")
print(f"readings_log count now: {cur.fetchone()[0]}")

cur.close()
conn.close()
print("Done.")
