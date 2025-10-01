import pandas as pd
import numpy as np
import sqlite3

def analyze_listing_conversion_characteristics():
    """
    Analyze which listing characteristics lead to better conversion rates.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data from database...")
    
    # Load data
    listing_views = pd.read_sql_query("SELECT * FROM listing_views", conn)
    reservations = pd.read_sql_query("SELECT * FROM reservations", conn)
    
    print(f"Loaded {len(listing_views)} listing views")
    print(f"Loaded {len(reservations)} reservations")
    
    # Convert timestamps
    listing_views['event_time'] = pd.to_datetime(listing_views['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get listings that were reserved
    reserved_listings = set(reservations['listing_id'].unique())
    print(f"Total unique listings that were reserved: {len(reserved_listings):,}")
    
    # Get all listings that were viewed
    all_viewed_listings = set(listing_views['listing_id'].unique())
    print(f"Total unique listings that were viewed: {len(all_viewed_listings):,}")
    
    # Calculate conversion rate by listing
    listing_conversion_stats = []
    
    for listing_id in all_viewed_listings:
        # Get views for this listing
        listing_views_data = listing_views[listing_views['listing_id'] == listing_id]
        total_views = len(listing_views_data)
        
        # Check if this listing was ever reserved
        was_reserved = listing_id in reserved_listings
        
        # Get reservations for this listing
        listing_reservations = reservations[reservations['listing_id'] == listing_id]
        total_reservations = len(listing_reservations)
        
        # Calculate conversion rate
        conversion_rate = (total_reservations / total_views * 100) if total_views > 0 else 0
        
        listing_conversion_stats.append({
            'listing_id': listing_id,
            'total_views': total_views,
            'total_reservations': total_reservations,
            'was_reserved': was_reserved,
            'conversion_rate': conversion_rate
        })
    
    listing_stats_df = pd.DataFrame(listing_conversion_stats)
    
    # Create a function to analyze conversion by listing characteristic
    def analyze_listing_characteristic_by_category(df, category_col, min_views=10):
        results = []
        
        for category in df[category_col].unique():
            if pd.isna(category):
                continue
                
            # Get listings for this category
            category_listings = df[df[category_col] == category]
            category_listing_ids = set(category_listings['listing_id'].unique())
            
            # Calculate metrics for this category
            total_views = len(category_listings)
            total_reservations = len(reservations[reservations['listing_id'].isin(category_listing_ids)])
            conversion_rate = (total_reservations / total_views * 100) if total_views > 0 else 0
            
            # Get unique listings in this category
            unique_listings = len(category_listing_ids)
            reserved_listings_in_category = len(category_listing_ids.intersection(reserved_listings))
            listing_reservation_rate = (reserved_listings_in_category / unique_listings * 100) if unique_listings > 0 else 0
            
            if total_views >= min_views:
                results.append({
                    'category': category,
                    'total_views': total_views,
                    'total_reservations': total_reservations,
                    'conversion_rate': conversion_rate,
                    'unique_listings': unique_listings,
                    'reserved_listings': reserved_listings_in_category,
                    'listing_reservation_rate': listing_reservation_rate
                })
        
        return pd.DataFrame(results).sort_values('conversion_rate', ascending=False)
    
    # Analyze by different listing characteristics
    print("\n" + "="*80)
    print("LISTING CONVERSION BY SEARCH POSITION")
    print("="*80)
    
    # Convert search_position to numeric for analysis
    listing_views['search_position_numeric'] = pd.to_numeric(listing_views['search_position'], errors='coerce')
    listing_views['position_bin'] = pd.cut(listing_views['search_position_numeric'], 
                                          bins=[0, 5, 10, 15, 20, 100], 
                                          labels=['1-5', '6-10', '11-15', '16-20', '20+'])
    
    position_analysis = analyze_listing_characteristic_by_category(listing_views, 'position_bin', min_views=50)
    print(position_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY SOURCE SCREEN")
    print("="*80)
    source_screen_analysis = analyze_listing_characteristic_by_category(listing_views, 'source_screen', min_views=50)
    print(source_screen_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY DMA (MARKET)")
    print("="*80)
    dma_analysis = analyze_listing_characteristic_by_category(listing_views, 'click_dma', min_views=50)
    print(dma_analysis.head(10).to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY ATTRIBUTION SOURCE")
    print("="*80)
    source_analysis = analyze_listing_characteristic_by_category(listing_views, 'first_attribution_source', min_views=50)
    print(source_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY ATTRIBUTION CHANNEL")
    print("="*80)
    channel_analysis = analyze_listing_characteristic_by_category(listing_views, 'first_attribution_channel', min_views=50)
    print(channel_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY USER TYPE")
    print("="*80)
    
    # Bot analysis
    bot_analysis = analyze_listing_characteristic_by_category(listing_views, 'is_bot', min_views=50)
    print("Bot vs Non-Bot:")
    print(bot_analysis.to_string(index=False))
    
    # Host analysis
    host_analysis = analyze_listing_characteristic_by_category(listing_views, 'is_host', min_views=50)
    print("\nHost vs Non-Host:")
    print(host_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("LISTING CONVERSION BY TIME PATTERNS")
    print("="*80)
    
    # Month analysis
    month_analysis = analyze_listing_characteristic_by_category(listing_views, 'month', min_views=50)
    print("Conversion by Month:")
    print(month_analysis.to_string(index=False))
    
    # Day of week analysis
    listing_views['day_of_week'] = listing_views['event_time'].dt.day_name()
    dow_analysis = analyze_listing_characteristic_by_category(listing_views, 'day_of_week', min_views=50)
    print("\nConversion by Day of Week:")
    print(dow_analysis.to_string(index=False))
    
    # Hour analysis
    listing_views['hour'] = listing_views['event_time'].dt.hour
    listing_views['hour_bin'] = pd.cut(listing_views['hour'], 
                                      bins=[0, 6, 12, 18, 24], 
                                      labels=['Night (0-6)', 'Morning (6-12)', 'Afternoon (12-18)', 'Evening (18-24)'])
    
    hour_analysis = analyze_listing_characteristic_by_category(listing_views, 'hour_bin', min_views=50)
    print("\nConversion by Time of Day:")
    print(hour_analysis.to_string(index=False))
    
    # Analyze by listing ID patterns (high-volume listings)
    print("\n" + "="*80)
    print("HIGH-VOLUME LISTING ANALYSIS")
    print("="*80)
    
    # Get listings with high view counts
    listing_view_counts = listing_views.groupby('listing_id').size()
    high_volume_listings = listing_view_counts[listing_view_counts >= 100].index
    
    print(f"Listings with 100+ views: {len(high_volume_listings)}")
    
    # Analyze conversion for high-volume listings
    high_volume_analysis = []
    for listing_id in high_volume_listings:
        views = listing_view_counts[listing_id]
        listing_reservations = len(reservations[reservations['listing_id'] == listing_id])
        conversion_rate = (listing_reservations / views * 100) if views > 0 else 0
        
        high_volume_analysis.append({
            'listing_id': listing_id,
            'total_views': views,
            'total_reservations': listing_reservations,
            'conversion_rate': conversion_rate
        })
    
    high_volume_df = pd.DataFrame(high_volume_analysis).sort_values('conversion_rate', ascending=False)
    print("\nTop 10 high-volume listings by conversion rate:")
    print(high_volume_df.head(10).to_string(index=False))
    
    # Analyze listing performance by geographic patterns
    print("\n" + "="*80)
    print("GEOGRAPHIC LISTING PERFORMANCE")
    print("="*80)
    
    # Get hex resolution analysis
    hex_analysis = analyze_listing_characteristic_by_category(listing_views, 'hex_1_resolution', min_views=50)
    print("Conversion by Hex Resolution (Level 1):")
    print(hex_analysis.head(10).to_string(index=False))
    
    # Analyze by listing reservation status
    print("\n" + "="*80)
    print("LISTING RESERVATION STATUS ANALYSIS")
    print("="*80)
    
    # Check if listing was already reserved when viewed
    reserved_status_analysis = analyze_listing_characteristic_by_category(listing_views, 'is_listing_reserved', min_views=50)
    print("Conversion by Listing Reservation Status:")
    print(reserved_status_analysis.to_string(index=False))
    
    # Analyze listing conversion by listing ID ranges (to identify patterns)
    print("\n" + "="*80)
    print("LISTING ID PATTERN ANALYSIS")
    print("="*80)
    
    # Convert listing_id to numeric for analysis
    listing_views['listing_id_numeric'] = pd.to_numeric(listing_views['listing_id'], errors='coerce')
    listing_views['listing_id_bin'] = pd.cut(listing_views['listing_id_numeric'], 
                                           bins=[0, 100, 500, 1000, 2000, 10000], 
                                           labels=['1-100', '101-500', '501-1000', '1001-2000', '2000+'])
    
    listing_id_analysis = analyze_listing_characteristic_by_category(listing_views, 'listing_id_bin', min_views=50)
    print("Conversion by Listing ID Range:")
    print(listing_id_analysis.to_string(index=False))
    
    # Summary statistics
    print("\n" + "="*80)
    print("LISTING CONVERSION SUMMARY")
    print("="*80)
    
    total_listing_views = len(listing_views)
    total_reservations = len(reservations)
    overall_conversion_rate = (total_reservations / total_listing_views * 100) if total_listing_views > 0 else 0
    
    print(f"Total listing views: {total_listing_views:,}")
    print(f"Total reservations: {total_reservations:,}")
    print(f"Overall conversion rate: {overall_conversion_rate:.2f}%")
    print(f"Unique listings viewed: {len(all_viewed_listings):,}")
    print(f"Unique listings reserved: {len(reserved_listings):,}")
    print(f"Listing reservation rate: {len(reserved_listings) / len(all_viewed_listings) * 100:.2f}%")
    
    # Top performing listings
    top_listings = listing_stats_df[listing_stats_df['total_views'] >= 10].nlargest(10, 'conversion_rate')
    print(f"\nTop 10 listings by conversion rate (10+ views):")
    print(top_listings.to_string(index=False))
    
    conn.close()
    
    return {
        'position_analysis': position_analysis,
        'source_screen_analysis': source_screen_analysis,
        'dma_analysis': dma_analysis,
        'source_analysis': source_analysis,
        'channel_analysis': channel_analysis,
        'bot_analysis': bot_analysis,
        'host_analysis': host_analysis,
        'month_analysis': month_analysis,
        'dow_analysis': dow_analysis,
        'hour_analysis': hour_analysis,
        'high_volume_analysis': high_volume_df,
        'hex_analysis': hex_analysis,
        'reserved_status_analysis': reserved_status_analysis,
        'listing_id_analysis': listing_id_analysis,
        'listing_stats': listing_stats_df
    }

if __name__ == "__main__":
    results = analyze_listing_conversion_characteristics()
