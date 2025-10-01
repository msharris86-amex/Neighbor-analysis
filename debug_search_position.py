import pandas as pd
import sqlite3

conn = sqlite3.connect('marketplace_analysis.db')

# Check the data structure
print("=== SEARCH EVENTS ===")
search_events = pd.read_sql_query("SELECT * FROM search_events WHERE is_bot = 'False' LIMIT 5", conn)
print("Search events columns:", list(search_events.columns))
print("Search events sample:")
print(search_events[['merged_amplitude_id', 'search_sort', 'event_time']].head())

print("\n=== LISTING VIEWS ===")
click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False' LIMIT 5", conn)
print("Click events columns:", list(click_events.columns))
print("Click events sample:")
print(click_events[['merged_amplitude_id', 'search_position', 'event_time']].head())

print("\n=== RESERVATIONS ===")
reservations = pd.read_sql_query("SELECT * FROM reservations LIMIT 5", conn)
print("Reservations columns:", list(reservations.columns))
print("Reservations sample:")
print(reservations[['renter_user_id', 'created_at']].head())

# Check if we can link the data
print("\n=== DATA LINKING TEST ===")
# Get a sample user from search events
sample_user = search_events['merged_amplitude_id'].iloc[0]
print(f"Sample user: {sample_user}")

# Check if this user has clicks
user_clicks = click_events[click_events['merged_amplitude_id'] == sample_user]
print(f"User clicks: {len(user_clicks)}")
if len(user_clicks) > 0:
    print("Click positions:", user_clicks['search_position'].tolist())

# Check if this user has reservations
user_reservations = reservations[reservations['renter_user_id'] == sample_user]
print(f"User reservations: {len(user_reservations)}")

conn.close()
