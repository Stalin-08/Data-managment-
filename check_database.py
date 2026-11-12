import sqlite3

conn = sqlite3.connect("database/crypto_data.db")
cursor = conn.cursor()

# Check total trades
cursor.execute("SELECT COUNT(*) FROM trades")
total_trades = cursor.fetchone()[0]

# Check total anomalies
cursor.execute("SELECT COUNT(*) FROM anomalies")
total_anomalies = cursor.fetchone()[0]

print("\n===== DATABASE REPORT =====")
print("Total Trades:", total_trades)
print("Total Anomalies:", total_anomalies)

# Show latest trades
cursor.execute("""
    SELECT * FROM trades
    ORDER BY id DESC
    LIMIT 5
""")

print("\n===== LATEST TRADES =====")
for row in cursor.fetchall():
    print(row)

# Anomaly distribution
cursor.execute("""
    SELECT is_anomaly, COUNT(*)
    FROM trades
    GROUP BY is_anomaly
""")

print("\n===== ANOMALY DISTRIBUTION =====")

for row in cursor.fetchall():
    status = "Anomaly" if row[0] == 1 else "Normal"
    print(status, ":", row[1])
print("\n===== DATA CONSISTENCY CHECK =====")

cursor.execute("SELECT COUNT(*) FROM trades")
trade_count = cursor.fetchone()[0]

cursor.execute(
    "SELECT COUNT(*) FROM trades WHERE is_anomaly = 1"
)
flagged_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM anomalies")
anomaly_table_count = cursor.fetchone()[0]

print("Total Trades:", trade_count)
print("Anomalies in Trades:", flagged_count)
print("Records in Anomalies Table:", anomaly_table_count)

if flagged_count == anomaly_table_count:
    print("SUCCESS: Database counts match!")
else:
    print("WARNING: Anomaly counts do not match!")
conn.close()