import pandas as pd
import sqlite3
from datetime import datetime

def analyze_search_to_reservation_conversion():
    """
    Analyze the conversion funnel from search events to listing clicks to reservations.
    """
    
    # Read the data files
    print("Loading data files...")
    search_events = pd.read_csv('all_search_events (1).csv')
    click_events = pd.read_csv('view_listing_detail_events (1).csv')
    reservations = pd.read_csv('reservations (1).csv')
    
    print(f"Loaded {len(search_events)} search events")
    print(f"Loaded {len(click_events)} click events")
    print(f"Loaded {len(reservations)} reservations")
    
    # Convert timestamps to datetime for proper analysis
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get unique users who searched
    unique_searchers = search_events['merged_amplitude_id'].nunique()
    print(f"\n=== CONVERSION FUNNEL ANALYSIS ===")
    print(f"Total unique users who searched: {unique_searchers:,}")
    
    # Get unique users who clicked on listings
    unique_clickers = click_events['merged_amplitude_id'].nunique()
    print(f"Total unique users who clicked on listings: {unique_clickers:,}")
    
    # Get unique users who made reservations
    unique_reservers = reservations['renter_user_id'].nunique()
    print(f"Total unique users who made reservations: {unique_reservers:,}")
    
    # Find users who did the full funnel: searched → clicked → reserved
    # First, get users who both searched and clicked
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    
    # Users who searched and clicked
    searched_and_clicked = searchers.intersection(clickers)
    print(f"Users who both searched and clicked: {len(searched_and_clicked):,}")
    
    # Users who searched, clicked, and reserved
    # Note: We need to match amplitude_id from search/click events with renter_user_id from reservations
    # This assumes they are the same user identifier system
    searched_clicked_reserved = searched_and_clicked.intersection(reservers)
    print(f"Users who searched, clicked, AND reserved: {len(searched_clicked_reserved):,}")
    
    # Calculate conversion rates
    search_to_click_rate = len(searched_and_clicked) / unique_searchers * 100
    click_to_reserve_rate = len(searched_clicked_reserved) / len(searched_and_clicked) * 100 if len(searched_and_clicked) > 0 else 0
    search_to_reserve_rate = len(searched_clicked_reserved) / unique_searchers * 100
    
    print(f"\n=== CONVERSION RATES ===")
    print(f"Search to Click conversion rate: {search_to_click_rate:.2f}%")
    print(f"Click to Reserve conversion rate: {click_to_reserve_rate:.2f}%")
    print(f"Search to Reserve conversion rate: {search_to_reserve_rate:.2f}%")
    
    # More detailed analysis: Track the actual funnel with timestamps
    print(f"\n=== DETAILED FUNNEL ANALYSIS ===")
    
    # Create a more sophisticated analysis that tracks the sequence
    funnel_users = []
    
    # For each user who made a reservation, check if they searched and clicked before reserving
    for _, reservation in reservations.iterrows():
        user_id = reservation['renter_user_id']
        reservation_time = reservation['created_at']
        
        # Check if this user searched before the reservation
        user_searches = search_events[
            (search_events['merged_amplitude_id'] == user_id) & 
            (search_events['event_time'] <= reservation_time)
        ]
        
        # Check if this user clicked on listings before the reservation
        user_clicks = click_events[
            (click_events['merged_amplitude_id'] == user_id) & 
            (click_events['event_time'] <= reservation_time)
        ]
        
        if len(user_searches) > 0 and len(user_clicks) > 0:
            funnel_users.append({
                'user_id': user_id,
                'searches': len(user_searches),
                'clicks': len(user_clicks),
                'reservation_time': reservation_time,
                'listing_id': reservation['listing_id']
            })
    
    print(f"Users who completed the full funnel (searched -> clicked -> reserved): {len(funnel_users):,}")
    
    # Additional insights
    if funnel_users:
        avg_searches_per_funnel_user = sum(f['searches'] for f in funnel_users) / len(funnel_users)
        avg_clicks_per_funnel_user = sum(f['clicks'] for f in funnel_users) / len(funnel_users)
        
        print(f"\n=== FUNNEL USER INSIGHTS ===")
        print(f"Average searches per funnel user: {avg_searches_per_funnel_user:.2f}")
        print(f"Average clicks per funnel user: {avg_clicks_per_funnel_user:.2f}")
    
    return {
        'total_searchers': unique_searchers,
        'total_clickers': unique_clickers,
        'total_reservers': unique_reservers,
        'searched_and_clicked': len(searched_and_clicked),
        'searched_clicked_reserved': len(searched_clicked_reserved),
        'funnel_users': len(funnel_users),
        'search_to_click_rate': search_to_click_rate,
        'click_to_reserve_rate': click_to_reserve_rate,
        'search_to_reserve_rate': search_to_reserve_rate
    }

if __name__ == "__main__":
    results = analyze_search_to_reservation_conversion()
