"""Delete WM_1000 padding extra row if it exists alongside WM_0001-WM_0999."""
import mysql.connector

conn = mysql.connector.connect(host="127.0.0.1", user="root", password="", database="laundry_ops")
cur = conn.cursor()

# Check what IDs we have
cur.execute("SELECT COUNT(*) FROM machine_status")
print(f"Total before: {cur.fetchone()[0]}")

# machine_status has 1001 because WM_1000 is valid (WM 1 through 1000)
# But let's verify the count is exactly 1000 (WM_0001..WM_1000)
cur.execute("""
    SELECT COUNT(*) FROM machine_status
    WHERE CAST(SUBSTRING(machine_id, 4) AS UNSIGNED) BETWEEN 1 AND 1000
""")
in_range = cur.fetchone()[0]
print(f"Rows with machine_id WM_0001-WM_1000: {in_range}")

# Delete any stray rows outside 1-1000
cur.execute("""
    DELETE FROM machine_status
    WHERE CAST(SUBSTRING(machine_id, 4) AS UNSIGNED) = 0
    OR CAST(SUBSTRING(machine_id, 4) AS UNSIGNED) > 1000
""")
print(f"Deleted extra rows: {cur.rowcount}")
conn.commit()

cur.execute("SELECT COUNT(*) FROM machine_status")
print(f"Total after: {cur.fetchone()[0]}")
cur.close()
conn.close()
