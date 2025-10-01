import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def create_search_sort_preference_chart():
    """
    Create a chart showing conversion rates by search sort preferences.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for search sort preference analysis...")
    
    # Load data
    search_events = pd.read_sql_query("SELECT * FROM search_events WHERE is_bot = 'False'", conn)
    click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False'", conn)
    reservations = pd.read_sql_query("SELECT * FROM reservations", conn)
    
    # Convert timestamps
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get users who completed the full funnel
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    funnel_users = searchers.intersection(clickers).intersection(reservers)
    
    # Analyze by search sort preference
    sort_preferences = search_events['search_sort'].unique()
    sort_data = []
    
    for sort_type in sort_preferences:
        if pd.isna(sort_type):
            continue
            
        # Get searches for this sort type
        sort_searches = search_events[search_events['search_sort'] == sort_type]
        sort_users = set(sort_searches['merged_amplitude_id'].unique())
        
        # Calculate metrics
        total_searchers = len(sort_users)
        funnel_users_in_sort = len(sort_users.intersection(funnel_users))
        conversion_rate = (funnel_users_in_sort / total_searchers * 100) if total_searchers > 0 else 0
        
        sort_data.append({
            'sort_preference': sort_type,
            'total_searchers': total_searchers,
            'funnel_users': funnel_users_in_sort,
            'conversion_rate': conversion_rate
        })
    
    # Create DataFrame and sort by conversion rate
    sort_df = pd.DataFrame(sort_data).sort_values('conversion_rate', ascending=True)
    
    # Create the chart
    plt.figure(figsize=(12, 8))
    
    # Create horizontal bar chart
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    bars = plt.barh(sort_df['sort_preference'], sort_df['conversion_rate'], 
                    color=colors[:len(sort_df)])
    
    # Customize the chart
    plt.xlabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.ylabel('Search Sort Preference', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate by Search Sort Preference\n(Search → Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for i, (bar, rate) in enumerate(zip(bars, sort_df['conversion_rate'])):
        plt.text(rate + 0.05, bar.get_y() + bar.get_height()/2, 
                f'{rate:.2f}%', 
                va='center', ha='left', fontweight='bold')
    
    # Add searcher count labels
    for i, (bar, count) in enumerate(zip(bars, sort_df['total_searchers'])):
        plt.text(rate + 0.3, bar.get_y() + bar.get_height()/2, 
                f'({count:,} searchers)', 
                va='center', ha='left', fontsize=10, alpha=0.7)
    
    # Customize appearance
    plt.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('search_sort_preference_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'search_sort_preference_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("SEARCH SORT PREFERENCE CONVERSION SUMMARY")
    print("="*60)
    print(sort_df.to_string(index=False))
    
    # Calculate and display insights
    best_sort = sort_df.iloc[-1]
    worst_sort = sort_df.iloc[0]
    
    print(f"\nKEY INSIGHTS:")
    print(f"Best performing sort: {best_sort['sort_preference']} ({best_sort['conversion_rate']:.2f}%)")
    print(f"Lowest performing sort: {worst_sort['sort_preference']} ({worst_sort['conversion_rate']:.2f}%)")
    print(f"Performance gap: {best_sort['conversion_rate'] - worst_sort['conversion_rate']:.2f} percentage points")
    
    # Additional analysis
    print(f"\nDETAILED BREAKDOWN:")
    for _, row in sort_df.iterrows():
        print(f"{row['sort_preference'].upper()}:")
        print(f"  - Total Searchers: {row['total_searchers']:,}")
        print(f"  - Converting Users: {row['funnel_users']:,}")
        print(f"  - Conversion Rate: {row['conversion_rate']:.2f}%")
        print()
    
    conn.close()
    
    return sort_df

if __name__ == "__main__":
    results = create_search_sort_preference_chart()
