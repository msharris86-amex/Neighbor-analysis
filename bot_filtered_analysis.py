import pandas as pd
import sqlite3

def analyze_conversion_without_bots():
    """
    Re-analyze conversion rates with bots filtered out.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data with bot filtering...")
    
    # Load data filtering out bots
    search_events = pd.read_sql_query("SELECT * FROM search_events WHERE is_bot = 'False'", conn)
    click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False'", conn)
    reservations = pd.read_sql_query("SELECT * FROM reservations", conn)
    
    print(f"Non-bot search events: {len(search_events):,}")
    print(f"Non-bot listing views: {len(click_events):,}")
    print(f"Total reservations: {len(reservations):,}")
    
    # Convert timestamps
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get users who completed the full funnel (non-bot only)
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    
    # Users who did the full funnel
    funnel_users = searchers.intersection(clickers).intersection(reservers)
    
    print(f"\n=== NON-BOT CONVERSION ANALYSIS ===")
    print(f"Total non-bot searchers: {len(searchers):,}")
    print(f"Total non-bot clickers: {len(clickers):,}")
    print(f"Total reservers: {len(reservers):,}")
    print(f"Users who completed full funnel: {len(funnel_users):,}")
    
    # Calculate conversion rates
    search_to_click_rate = len(searchers.intersection(clickers)) / len(searchers) * 100
    click_to_reserve_rate = len(searchers.intersection(clickers).intersection(reservers)) / len(searchers.intersection(clickers)) * 100
    search_to_reserve_rate = len(funnel_users) / len(searchers) * 100
    
    print(f"\n=== CONVERSION RATES (NO BOTS) ===")
    print(f"Search to Click rate: {search_to_click_rate:.2f}%")
    print(f"Click to Reserve rate: {click_to_reserve_rate:.2f}%")
    print(f"Search to Reserve rate: {search_to_reserve_rate:.2f}%")
    
    # Compare with original analysis (including bots)
    print(f"\n=== COMPARISON WITH BOT-INCLUDED ANALYSIS ===")
    print("Original analysis (with bots):")
    print("- Total searchers: 7,941")
    print("- Funnel users: 597") 
    print("- Search to Reserve rate: 7.52%")
    print("\nFiltered analysis (no bots):")
    print(f"- Total searchers: {len(searchers):,}")
    print(f"- Funnel users: {len(funnel_users):,}")
    print(f"- Search to Reserve rate: {search_to_reserve_rate:.2f}%")
    
    # Calculate the impact of bot filtering
    original_searchers = 7941
    original_funnel = 597
    original_rate = 7.52
    
    searcher_change = (len(searchers) - original_searchers) / original_searchers * 100
    funnel_change = (len(funnel_users) - original_funnel) / original_funnel * 100
    rate_change = search_to_reserve_rate - original_rate
    
    print(f"\n=== BOT FILTERING IMPACT ===")
    print(f"Searcher count change: {searcher_change:+.2f}%")
    print(f"Funnel user count change: {funnel_change:+.2f}%")
    print(f"Conversion rate change: {rate_change:+.2f} percentage points")
    
    conn.close()
    
    return {
        'non_bot_searchers': len(searchers),
        'non_bot_clickers': len(clickers),
        'funnel_users': len(funnel_users),
        'search_to_click_rate': search_to_click_rate,
        'click_to_reserve_rate': click_to_reserve_rate,
        'search_to_reserve_rate': search_to_reserve_rate
    }

if __name__ == "__main__":
    results = analyze_conversion_without_bots()
