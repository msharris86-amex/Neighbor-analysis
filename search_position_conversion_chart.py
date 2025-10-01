import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def create_search_position_conversion_chart():
    """
    Create a chart showing conversion rates by search position.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for search position conversion analysis...")
    
    # Load data
    search_events = pd.read_sql_query("SELECT * FROM search_events WHERE is_bot = 'False'", conn)
    click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False'", conn)
    reservations = pd.read_sql_query("SELECT * FROM reservations", conn)
    
    # Convert timestamps
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get users who completed the full funnel
    # Note: reservations uses renter_user_id, others use merged_amplitude_id
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    
    # For this analysis, we'll focus on users who clicked and then converted
    # We'll use users who have both clicks and reservations
    funnel_users = clickers.intersection(reservers)
    
    # Analyze by search position
    # First, let's see what search positions are available
    print("Available search positions:")
    position_counts = click_events['search_position'].value_counts().sort_index()
    print(position_counts.head(20))
    
    # Focus on positions 1-20 for the main analysis
    position_data = []
    
    for position in range(1, 21):  # Positions 1-20
        # Get clicks for this position
        position_clicks = click_events[click_events['search_position'] == position]
        position_users = set(position_clicks['merged_amplitude_id'].unique())
        
        # Calculate metrics - users who clicked on this position AND converted
        total_clickers = len(position_users)
        funnel_users_in_position = len(position_users.intersection(funnel_users))
        conversion_rate = (funnel_users_in_position / total_clickers * 100) if total_clickers > 0 else 0
        
        position_data.append({
            'search_position': position,
            'total_clickers': total_clickers,
            'funnel_users': funnel_users_in_position,
            'conversion_rate': conversion_rate
        })
    
    # Create DataFrame
    position_df = pd.DataFrame(position_data)
    
    # Create the chart
    plt.figure(figsize=(16, 10))
    
    # Create bar chart
    colors = plt.cm.viridis(np.linspace(0, 1, len(position_df)))
    bars = plt.bar(position_df['search_position'], position_df['conversion_rate'], 
                   color=colors, alpha=0.8)
    
    # Customize the chart
    plt.xlabel('Search Position', fontsize=12, fontweight='bold')
    plt.ylabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate by Search Position\n(Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for bar, rate in zip(bars, position_df['conversion_rate']):
        if rate > 0:  # Only show labels for positions with data
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                    f'{rate:.1f}%', 
                    ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # Customize appearance
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.xticks(range(1, 21))
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('search_position_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'search_position_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("SEARCH POSITION CONVERSION SUMMARY")
    print("="*60)
    print(position_df.to_string(index=False))
    
    # Calculate and display insights
    # Filter out positions with no data
    position_df_filtered = position_df[position_df['total_clickers'] > 0]
    
    if len(position_df_filtered) > 0:
        best_position = position_df_filtered.loc[position_df_filtered['conversion_rate'].idxmax()]
        worst_position = position_df_filtered.loc[position_df_filtered['conversion_rate'].idxmin()]
        
        print(f"\nKEY INSIGHTS:")
        print(f"Best performing position: {int(best_position['search_position'])} ({best_position['conversion_rate']:.2f}%)")
        print(f"Lowest performing position: {int(worst_position['search_position'])} ({worst_position['conversion_rate']:.2f}%)")
        print(f"Performance gap: {best_position['conversion_rate'] - worst_position['conversion_rate']:.2f} percentage points")
        
        # Calculate average conversion rate for positions with data
        avg_conversion = position_df_filtered['conversion_rate'].mean()
        print(f"Average conversion rate: {avg_conversion:.2f}%")
        
        # Analyze top positions vs lower positions
        top_positions = position_df_filtered[position_df_filtered['search_position'] <= 5]
        lower_positions = position_df_filtered[position_df_filtered['search_position'] > 5]
        
        if len(top_positions) > 0 and len(lower_positions) > 0:
            top_avg = top_positions['conversion_rate'].mean()
            lower_avg = lower_positions['conversion_rate'].mean()
            
            print(f"\nTOP 5 vs LOWER POSITIONS:")
            print(f"Top 5 positions average: {top_avg:.2f}%")
            print(f"Positions 6+ average: {lower_avg:.2f}%")
            print(f"Top positions perform {top_avg - lower_avg:.2f} percentage points better")
        
        # Additional analysis
        print(f"\nDETAILED BREAKDOWN (Top 10 positions):")
        top_10 = position_df_filtered.head(10)
        for _, row in top_10.iterrows():
            print(f"Position {int(row['search_position'])}:")
            print(f"  - Total Clickers: {row['total_clickers']:,}")
            print(f"  - Converting Users: {row['funnel_users']:,}")
            print(f"  - Conversion Rate: {row['conversion_rate']:.2f}%")
            print()
    
    conn.close()
    
    return position_df

if __name__ == "__main__":
    results = create_search_position_conversion_chart()
