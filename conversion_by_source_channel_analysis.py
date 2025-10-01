import pandas as pd
import numpy as np

def analyze_conversion_by_source_channel():
    """
    Analyze conversion funnel rates by first_attribution_source and first_attribution_channel combination.
    """
    
    # Read the data files
    print("Loading data files...")
    search_events = pd.read_csv('all_search_events (1).csv')
    click_events = pd.read_csv('view_listing_detail_events (1).csv')
    reservations = pd.read_csv('reservations (1).csv')
    
    print(f"Loaded {len(search_events)} search events")
    print(f"Loaded {len(click_events)} click events")
    print(f"Loaded {len(reservations)} reservations")
    
    # Convert timestamps to datetime
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Create source-channel combinations
    search_events['source_channel'] = search_events['first_attribution_source'] + ' - ' + search_events['first_attribution_channel']
    
    # Get unique source-channel combinations
    source_channels = search_events['source_channel'].unique()
    print(f"\nFound {len(source_channels)} source-channel combinations:")
    for sc in sorted(source_channels):
        count = len(search_events[search_events['source_channel'] == sc])
        print(f"  {sc}: {count:,} searches")
    
    # Create results dataframe
    results = []
    
    for source_channel in source_channels:
        print(f"\n=== Analyzing: {source_channel} ===")
        
        # Filter data for this source-channel combination
        sc_searches = search_events[search_events['source_channel'] == source_channel]
        sc_search_users = set(sc_searches['merged_amplitude_id'].unique())
        
        # Get clicks for users who came from this source-channel
        sc_clicks = click_events[click_events['merged_amplitude_id'].isin(sc_search_users)]
        sc_click_users = set(sc_clicks['merged_amplitude_id'].unique())
        
        # Get reservations for users who came from this source-channel
        sc_reservations = reservations[reservations['renter_user_id'].isin(sc_search_users)]
        sc_reserve_users = set(sc_reservations['renter_user_id'].unique())
        
        # Calculate metrics
        total_searchers = len(sc_search_users)
        total_clickers = len(sc_click_users)
        total_reservers = len(sc_reserve_users)
        
        # Users who did both searched and clicked
        searched_and_clicked = len(sc_search_users.intersection(sc_click_users))
        
        # Users who did searched, clicked, and reserved
        searched_clicked_reserved = len(sc_search_users.intersection(sc_click_users).intersection(sc_reserve_users))
        
        # Calculate conversion rates
        search_to_click_rate = (searched_and_clicked / total_searchers * 100) if total_searchers > 0 else 0
        click_to_reserve_rate = (searched_clicked_reserved / searched_and_clicked * 100) if searched_and_clicked > 0 else 0
        search_to_reserve_rate = (searched_clicked_reserved / total_searchers * 100) if total_searchers > 0 else 0
        
        # Store results
        results.append({
            'source_channel': source_channel,
            'total_searchers': total_searchers,
            'total_clickers': total_clickers,
            'total_reservers': total_reservers,
            'searched_and_clicked': searched_and_clicked,
            'searched_clicked_reserved': searched_clicked_reserved,
            'search_to_click_rate': search_to_click_rate,
            'click_to_reserve_rate': click_to_reserve_rate,
            'search_to_reserve_rate': search_to_reserve_rate
        })
        
        print(f"Total searchers: {total_searchers:,}")
        print(f"Total clickers: {total_clickers:,}")
        print(f"Total reservers: {total_reservers:,}")
        print(f"Searched and clicked: {searched_and_clicked:,}")
        print(f"Searched, clicked, and reserved: {searched_clicked_reserved:,}")
        print(f"Search to Click rate: {search_to_click_rate:.2f}%")
        print(f"Click to Reserve rate: {click_to_reserve_rate:.2f}%")
        print(f"Search to Reserve rate: {search_to_reserve_rate:.2f}%")
    
    # Create summary dataframe
    results_df = pd.DataFrame(results)
    
    # Sort by search to reserve rate (overall conversion)
    results_df = results_df.sort_values('search_to_reserve_rate', ascending=False)
    
    print(f"\n=== SUMMARY BY SOURCE-CHANNEL (Sorted by Search to Reserve Rate) ===")
    print(results_df[['source_channel', 'total_searchers', 'searched_clicked_reserved', 
                     'search_to_click_rate', 'click_to_reserve_rate', 'search_to_reserve_rate']].to_string(index=False))
    
    # Filter for meaningful volume (50+ searchers)
    meaningful_volume = results_df[results_df['total_searchers'] >= 50]
    print(f"\n=== MEANINGFUL VOLUME ANALYSIS (50+ searchers) ===")
    print(meaningful_volume[['source_channel', 'total_searchers', 'searched_clicked_reserved', 
                           'search_to_reserve_rate']].to_string(index=False))
    
    # Top performers by different metrics
    print(f"\n=== TOP PERFORMERS ===")
    
    # Best conversion rates
    top_conversion = results_df[results_df['total_searchers'] >= 50].head(5)
    print("Top 5 by Search to Reserve rate (50+ searchers):")
    for _, row in top_conversion.iterrows():
        print(f"{row['source_channel']}: {row['search_to_reserve_rate']:.2f}% ({row['searched_clicked_reserved']:,} conversions from {row['total_searchers']:,} searchers)")
    
    # Highest volume
    top_volume = results_df.nlargest(5, 'total_searchers')
    print(f"\nTop 5 by volume:")
    for _, row in top_volume.iterrows():
        print(f"{row['source_channel']}: {row['total_searchers']:,} searchers, {row['search_to_reserve_rate']:.2f}% conversion")
    
    # Best click-to-reserve rates
    top_click_reserve = results_df[results_df['searched_and_clicked'] >= 20].nlargest(5, 'click_to_reserve_rate')
    print(f"\nTop 5 by Click to Reserve rate (20+ clickers):")
    for _, row in top_click_reserve.iterrows():
        print(f"{row['source_channel']}: {row['click_to_reserve_rate']:.2f}% ({row['searched_clicked_reserved']:,} reservations from {row['searched_and_clicked']:,} clickers)")
    
    return results_df

if __name__ == "__main__":
    results = analyze_conversion_by_source_channel()
