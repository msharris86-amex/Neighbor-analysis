import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def create_search_position_simple_chart():
    """
    Create a chart showing click distribution by search position.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for search position analysis...")
    
    # Load click events data
    click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False'", conn)
    
    # Analyze by search position
    print("Available search positions:")
    position_counts = click_events['search_position'].value_counts().sort_index()
    print(position_counts.head(20))
    
    # Focus on positions 1-20 for the main analysis
    position_data = []
    
    for position in range(1, 21):  # Positions 1-20
        # Get clicks for this position (search_position is stored as string)
        position_clicks = click_events[click_events['search_position'] == str(position)]
        total_clicks = len(position_clicks)
        
        # Calculate click rate as percentage of total clicks
        total_all_clicks = len(click_events)
        click_percentage = (total_clicks / total_all_clicks * 100) if total_all_clicks > 0 else 0
        
        position_data.append({
            'search_position': position,
            'total_clicks': total_clicks,
            'click_percentage': click_percentage
        })
    
    # Create DataFrame
    position_df = pd.DataFrame(position_data)
    
    # Create the chart
    plt.figure(figsize=(16, 10))
    
    # Create bar chart
    colors = plt.cm.viridis(np.linspace(0, 1, len(position_df)))
    bars = plt.bar(position_df['search_position'], position_df['click_percentage'], 
                   color=colors, alpha=0.8)
    
    # Customize the chart
    plt.xlabel('Search Position', fontsize=12, fontweight='bold')
    plt.ylabel('Click Percentage (%)', fontsize=12, fontweight='bold')
    plt.title('Click Distribution by Search Position\n(Percentage of Total Clicks)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for bar, percentage in zip(bars, position_df['click_percentage']):
        if percentage > 0:  # Only show labels for positions with data
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                    f'{percentage:.1f}%', 
                    ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # Customize appearance
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.xticks(range(1, 21))
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('search_position_click_distribution_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'search_position_click_distribution_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("SEARCH POSITION CLICK DISTRIBUTION")
    print("="*60)
    print(position_df.to_string(index=False))
    
    # Calculate and display insights
    # Filter out positions with no data
    position_df_filtered = position_df[position_df['total_clicks'] > 0]
    
    if len(position_df_filtered) > 0:
        best_position = position_df_filtered.loc[position_df_filtered['click_percentage'].idxmax()]
        worst_position = position_df_filtered.loc[position_df_filtered['click_percentage'].idxmin()]
        
        print(f"\nKEY INSIGHTS:")
        print(f"Most clicked position: {int(best_position['search_position'])} ({best_position['click_percentage']:.2f}%)")
        print(f"Least clicked position: {int(worst_position['search_position'])} ({worst_position['click_percentage']:.2f}%)")
        print(f"Click distribution gap: {best_position['click_percentage'] - worst_position['click_percentage']:.2f} percentage points")
        
        # Calculate average click percentage
        avg_clicks = position_df_filtered['click_percentage'].mean()
        print(f"Average click percentage: {avg_clicks:.2f}%")
        
        # Analyze top positions vs lower positions
        top_positions = position_df_filtered[position_df_filtered['search_position'] <= 5]
        lower_positions = position_df_filtered[position_df_filtered['search_position'] > 5]
        
        if len(top_positions) > 0 and len(lower_positions) > 0:
            top_avg = top_positions['click_percentage'].mean()
            lower_avg = lower_positions['click_percentage'].mean()
            
            print(f"\nTOP 5 vs LOWER POSITIONS:")
            print(f"Top 5 positions average: {top_avg:.2f}%")
            print(f"Positions 6+ average: {lower_avg:.2f}%")
            print(f"Top positions get {top_avg - lower_avg:.2f} percentage points more clicks")
        
        # Calculate cumulative distribution
        position_df_filtered_sorted = position_df_filtered.sort_values('search_position')
        cumulative_percentage = position_df_filtered_sorted['click_percentage'].cumsum()
        
        print(f"\nCUMULATIVE CLICK DISTRIBUTION:")
        for i, (_, row) in enumerate(position_df_filtered_sorted.iterrows()):
            if i < 10:  # Show first 10 positions
                print(f"Position {int(row['search_position'])}: {cumulative_percentage.iloc[i]:.1f}% cumulative")
        
        # Additional analysis
        print(f"\nDETAILED BREAKDOWN (Top 10 positions):")
        top_10 = position_df_filtered.head(10)
        for _, row in top_10.iterrows():
            print(f"Position {int(row['search_position'])}:")
            print(f"  - Total Clicks: {row['total_clicks']:,}")
            print(f"  - Click Percentage: {row['click_percentage']:.2f}%")
            print()
    
    conn.close()
    
    return position_df

if __name__ == "__main__":
    results = create_search_position_simple_chart()
