import pandas as pd
import numpy as np

def analyze_conversion_by_channel():
    """
    Analyze conversion funnel rates by first attribution channel.
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
    
    # Get unique channels from search events
    channels = search_events['first_attribution_channel'].unique()
    print(f"\nFound {len(channels)} attribution channels: {list(channels)}")
    
    # Create results dataframe
    results = []
    
    for channel in channels:
        print(f"\n=== Analyzing channel: {channel} ===")
        
        # Filter data for this channel
        channel_searches = search_events[search_events['first_attribution_channel'] == channel]
        channel_search_users = set(channel_searches['merged_amplitude_id'].unique())
        
        # Get clicks for users who came from this channel
        channel_clicks = click_events[click_events['merged_amplitude_id'].isin(channel_search_users)]
        channel_click_users = set(channel_clicks['merged_amplitude_id'].unique())
        
        # Get reservations for users who came from this channel
        channel_reservations = reservations[reservations['renter_user_id'].isin(channel_search_users)]
        channel_reserve_users = set(channel_reservations['renter_user_id'].unique())
        
        # Calculate metrics
        total_searchers = len(channel_search_users)
        total_clickers = len(channel_click_users)
        total_reservers = len(channel_reserve_users)
        
        # Users who did both searched and clicked
        searched_and_clicked = len(channel_search_users.intersection(channel_click_users))
        
        # Users who did searched, clicked, and reserved
        searched_clicked_reserved = len(channel_search_users.intersection(channel_click_users).intersection(channel_reserve_users))
        
        # Calculate conversion rates
        search_to_click_rate = (searched_and_clicked / total_searchers * 100) if total_searchers > 0 else 0
        click_to_reserve_rate = (searched_clicked_reserved / searched_and_clicked * 100) if searched_and_clicked > 0 else 0
        search_to_reserve_rate = (searched_clicked_reserved / total_searchers * 100) if total_searchers > 0 else 0
        
        # Store results
        results.append({
            'channel': channel,
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
    
    print(f"\n=== SUMMARY BY CHANNEL (Sorted by Search to Reserve Rate) ===")
    print(results_df[['channel', 'total_searchers', 'searched_clicked_reserved', 
                     'search_to_click_rate', 'click_to_reserve_rate', 'search_to_reserve_rate']].to_string(index=False))
    
    # Additional analysis: Volume vs Conversion
    print(f"\n=== VOLUME vs CONVERSION ANALYSIS ===")
    high_volume = results_df[results_df['total_searchers'] >= 100]
    print(f"Channels with 100+ searchers:")
    print(high_volume[['channel', 'total_searchers', 'search_to_reserve_rate']].to_string(index=False))
    
    # Best performing channels
    print(f"\n=== TOP PERFORMING CHANNELS ===")
    top_5 = results_df.head(5)
    print("Top 5 channels by Search to Reserve rate:")
    for _, row in top_5.iterrows():
        print(f"{row['channel']}: {row['search_to_reserve_rate']:.2f}% ({row['searched_clicked_reserved']:,} conversions from {row['total_searchers']:,} searchers)")
    
    return results_df

if __name__ == "__main__":
    results = analyze_conversion_by_channel()
