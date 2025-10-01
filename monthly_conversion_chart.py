import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
from datetime import datetime

def create_monthly_conversion_chart():
    """
    Create a chart showing conversion rates by month.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for monthly conversion analysis...")
    
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
    
    # Analyze by month
    months = search_events['month'].unique()
    month_data = []
    
    for month in sorted(months):
        if pd.isna(month):
            continue
            
        # Get searches for this month
        month_searches = search_events[search_events['month'] == month]
        month_users = set(month_searches['merged_amplitude_id'].unique())
        
        # Calculate metrics
        total_searchers = len(month_users)
        funnel_users_in_month = len(month_users.intersection(funnel_users))
        conversion_rate = (funnel_users_in_month / total_searchers * 100) if total_searchers > 0 else 0
        
        month_data.append({
            'month': month,
            'total_searchers': total_searchers,
            'funnel_users': funnel_users_in_month,
            'conversion_rate': conversion_rate
        })
    
    # Create DataFrame and sort by month (1-12)
    month_df = pd.DataFrame(month_data)
    month_df['month'] = month_df['month'].astype(int)
    month_df = month_df.sort_values('month')
    
    # Create the chart
    plt.figure(figsize=(14, 8))
    
    # Create bar chart
    colors = plt.cm.viridis(np.linspace(0, 1, len(month_df)))
    bars = plt.bar(month_df['month'], month_df['conversion_rate'], 
                   color=colors, alpha=0.8)
    
    # Customize the chart
    plt.xlabel('Month', fontsize=12, fontweight='bold')
    plt.ylabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate by Month\n(Search → Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for bar, rate in zip(bars, month_df['conversion_rate']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05, 
                f'{rate:.2f}%', 
                ha='center', va='bottom', fontweight='bold')
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Customize appearance
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('monthly_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'monthly_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("MONTHLY CONVERSION SUMMARY")
    print("="*60)
    print(month_df.to_string(index=False))
    
    # Calculate and display insights
    best_month = month_df.loc[month_df['conversion_rate'].idxmax()]
    worst_month = month_df.loc[month_df['conversion_rate'].idxmin()]
    
    print(f"\nKEY INSIGHTS:")
    print(f"Best performing month: {best_month['month']} ({best_month['conversion_rate']:.2f}%)")
    print(f"Lowest performing month: {worst_month['month']} ({worst_month['conversion_rate']:.2f}%)")
    print(f"Performance gap: {best_month['conversion_rate'] - worst_month['conversion_rate']:.2f} percentage points")
    
    # Calculate average conversion rate
    avg_conversion = month_df['conversion_rate'].mean()
    print(f"Average conversion rate: {avg_conversion:.2f}%")
    
    # Identify months above and below average
    above_avg = month_df[month_df['conversion_rate'] > avg_conversion]
    below_avg = month_df[month_df['conversion_rate'] < avg_conversion]
    
    print(f"\nMonths above average: {', '.join(above_avg['month'].astype(str))}")
    print(f"Months below average: {', '.join(below_avg['month'].astype(str))}")
    
    conn.close()
    
    return month_df

if __name__ == "__main__":
    results = create_monthly_conversion_chart()
