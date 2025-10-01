import sqlite3
import pandas as pd

conn = sqlite3.connect('marketplace_analysis.db')

# Check reservations table structure
print("RESERVATIONS TABLE:")
reservations = pd.read_sql_query("SELECT * FROM reservations LIMIT 1", conn)
print("Columns:", list(reservations.columns))

# Check listing_views table structure  
print("\nLISTING_VIEWS TABLE:")
listing_views = pd.read_sql_query("SELECT * FROM listing_views LIMIT 1", conn)
print("Columns:", list(listing_views.columns))

# Check search_events table structure
print("\nSEARCH_EVENTS TABLE:")
search_events = pd.read_sql_query("SELECT * FROM search_events LIMIT 1", conn)
print("Columns:", list(search_events.columns))

conn.close()
