import sqlite3

conn = sqlite3.connect('marketplace_analysis.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print('Existing tables:', [table[0] for table in tables])

# Check if we have data
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f'{table_name}: {count} rows')

conn.close()





