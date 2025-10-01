import sqlite3
import pandas as pd

conn = sqlite3.connect('marketplace_analysis.db')

# Test different boolean queries
print("Testing boolean queries:")

# Query 1: Check what the actual values are
print("\n1. Raw is_bot values:")
raw_bot = pd.read_sql_query("SELECT DISTINCT is_bot FROM search_events LIMIT 10", conn)
print(raw_bot)

# Query 2: Try with boolean False
print("\n2. Query with is_bot = 0:")
query1 = """
SELECT COUNT(*) as count 
FROM search_events 
WHERE search_sort IS NOT NULL 
AND is_bot = 0
"""
result1 = pd.read_sql_query(query1, conn)
print(f"Count with is_bot = 0: {result1['count'].iloc[0]}")

# Query 3: Try with boolean False (string)
print("\n3. Query with is_bot = 'False':")
query2 = """
SELECT COUNT(*) as count 
FROM search_events 
WHERE search_sort IS NOT NULL 
AND is_bot = 'False'
"""
result2 = pd.read_sql_query(query2, conn)
print(f"Count with is_bot = 'False': {result2['count'].iloc[0]}")

# Query 4: Try with boolean False (boolean)
print("\n4. Query with is_bot = 0:")
query3 = """
SELECT COUNT(*) as count 
FROM search_events 
WHERE search_sort IS NOT NULL 
AND is_bot = 0
"""
result3 = pd.read_sql_query(query3, conn)
print(f"Count with is_bot = 0: {result3['count'].iloc[0]}")

# Query 5: Try without bot filter
print("\n5. Query without bot filter:")
query4 = """
SELECT COUNT(*) as count 
FROM search_events 
WHERE search_sort IS NOT NULL
"""
result4 = pd.read_sql_query(query4, conn)
print(f"Count without bot filter: {result4['count'].iloc[0]}")

conn.close()
