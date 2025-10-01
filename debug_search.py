import sqlite3
import pandas as pd

conn = sqlite3.connect('marketplace_analysis.db')

# Check search_events data
print("SEARCH_EVENTS DATA:")
search_events = pd.read_sql_query("SELECT COUNT(*) as total FROM search_events", conn)
print(f"Total search events: {search_events['total'].iloc[0]}")

# Check search_sort data specifically
search_sort_data = pd.read_sql_query("""
    SELECT search_sort, COUNT(*) as count 
    FROM search_events 
    WHERE search_sort IS NOT NULL 
    GROUP BY search_sort 
    ORDER BY count DESC
""", conn)
print("\nSearch sort data:")
print(search_sort_data)

# Check if there are any non-bot events
non_bot_events = pd.read_sql_query("""
    SELECT COUNT(*) as count 
    FROM search_events 
    WHERE is_bot = 0
""", conn)
print(f"\nNon-bot events: {non_bot_events['count'].iloc[0]}")

# Check bot status distribution
bot_dist = pd.read_sql_query("""
    SELECT is_bot, COUNT(*) as count 
    FROM search_events 
    GROUP BY is_bot
""", conn)
print("\nBot status distribution:")
print(bot_dist)

conn.close()
