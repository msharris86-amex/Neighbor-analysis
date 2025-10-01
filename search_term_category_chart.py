import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def create_search_term_category_chart():
    """
    Create a chart showing conversion rates by search term categories.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for search term category analysis...")
    
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
    
    # Analyze by search term category
    categories = search_events['search_term_category'].unique()
    category_data = []
    
    for category in categories:
        if pd.isna(category):
            continue
            
        # Get searches for this category
        category_searches = search_events[search_events['search_term_category'] == category]
        category_users = set(category_searches['merged_amplitude_id'].unique())
        
        # Calculate metrics
        total_searchers = len(category_users)
        funnel_users_in_category = len(category_users.intersection(funnel_users))
        conversion_rate = (funnel_users_in_category / total_searchers * 100) if total_searchers > 0 else 0
        
        category_data.append({
            'category': category,
            'total_searchers': total_searchers,
            'funnel_users': funnel_users_in_category,
            'conversion_rate': conversion_rate
        })
    
    # Create DataFrame and sort by conversion rate
    category_df = pd.DataFrame(category_data).sort_values('conversion_rate', ascending=True)
    
    # Create the chart
    plt.figure(figsize=(12, 8))
    
    # Create horizontal bar chart
    bars = plt.barh(category_df['category'], category_df['conversion_rate'], 
                    color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
    
    # Customize the chart
    plt.xlabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.ylabel('Search Term Category', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate by Search Term Category\n(Search → Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for i, (bar, rate) in enumerate(zip(bars, category_df['conversion_rate'])):
        plt.text(rate + 0.05, bar.get_y() + bar.get_height()/2, 
                f'{rate:.2f}%', 
                va='center', ha='left', fontweight='bold')
    
    # Add searcher count labels
    for i, (bar, count) in enumerate(zip(bars, category_df['total_searchers'])):
        plt.text(rate + 0.3, bar.get_y() + bar.get_height()/2, 
                f'({count:,} searchers)', 
                va='center', ha='left', fontsize=10, alpha=0.7)
    
    # Customize appearance
    plt.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('search_term_category_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'search_term_category_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("SEARCH TERM CATEGORY CONVERSION SUMMARY")
    print("="*60)
    print(category_df.to_string(index=False))
    
    # Calculate and display insights
    best_category = category_df.iloc[-1]
    worst_category = category_df.iloc[0]
    
    print(f"\nKEY INSIGHTS:")
    print(f"Best performing category: {best_category['category']} ({best_category['conversion_rate']:.2f}%)")
    print(f"Lowest performing category: {worst_category['category']} ({worst_category['conversion_rate']:.2f}%)")
    print(f"Performance gap: {best_category['conversion_rate'] - worst_category['conversion_rate']:.2f} percentage points")
    
    conn.close()
    
    return category_df

if __name__ == "__main__":
    results = create_search_term_category_chart()
