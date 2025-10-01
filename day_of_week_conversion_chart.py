import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
from datetime import datetime

def create_day_of_week_conversion_chart():
    """
    Create a chart showing conversion rates by day of the week.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for day of week conversion analysis...")
    
    # Load data
    search_events = pd.read_sql_query("SELECT * FROM search_events WHERE is_bot = 'False'", conn)
    click_events = pd.read_sql_query("SELECT * FROM listing_views WHERE is_bot = 'False'", conn)
    reservations = pd.read_sql_query("SELECT * FROM reservations", conn)
    
    # Convert timestamps
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Extract day of week
    search_events['day_of_week'] = search_events['event_time'].dt.day_name()
    
    # Get users who completed the full funnel
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    funnel_users = searchers.intersection(clickers).intersection(reservers)
    
    # Analyze by day of week
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_data = []
    
    for day in days:
        # Get searches for this day
        day_searches = search_events[search_events['day_of_week'] == day]
        day_users = set(day_searches['merged_amplitude_id'].unique())
        
        # Calculate metrics
        total_searchers = len(day_users)
        funnel_users_in_day = len(day_users.intersection(funnel_users))
        conversion_rate = (funnel_users_in_day / total_searchers * 100) if total_searchers > 0 else 0
        
        day_data.append({
            'day_of_week': day,
            'total_searchers': total_searchers,
            'funnel_users': funnel_users_in_day,
            'conversion_rate': conversion_rate
        })
    
    # Create DataFrame
    day_df = pd.DataFrame(day_data)
    
    # Create the chart
    plt.figure(figsize=(12, 8))
    
    # Create bar chart
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8']
    bars = plt.bar(day_df['day_of_week'], day_df['conversion_rate'], 
                   color=colors, alpha=0.8)
    
    # Customize the chart
    plt.xlabel('Day of Week', fontsize=12, fontweight='bold')
    plt.ylabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate by Day of Week\n(Search → Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for bar, rate in zip(bars, day_df['conversion_rate']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05, 
                f'{rate:.2f}%', 
                ha='center', va='bottom', fontweight='bold')
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Customize appearance
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('day_of_week_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'day_of_week_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("DAY OF WEEK CONVERSION SUMMARY")
    print("="*60)
    print(day_df.to_string(index=False))
    
    # Calculate and display insights
    best_day = day_df.loc[day_df['conversion_rate'].idxmax()]
    worst_day = day_df.loc[day_df['conversion_rate'].idxmin()]
    
    print(f"\nKEY INSIGHTS:")
    print(f"Best performing day: {best_day['day_of_week']} ({best_day['conversion_rate']:.2f}%)")
    print(f"Lowest performing day: {worst_day['day_of_week']} ({worst_day['conversion_rate']:.2f}%)")
    print(f"Performance gap: {best_day['conversion_rate'] - worst_day['conversion_rate']:.2f} percentage points")
    
    # Calculate average conversion rate
    avg_conversion = day_df['conversion_rate'].mean()
    print(f"Average conversion rate: {avg_conversion:.2f}%")
    
    # Identify weekdays vs weekends
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekends = ['Saturday', 'Sunday']
    
    weekday_avg = day_df[day_df['day_of_week'].isin(weekdays)]['conversion_rate'].mean()
    weekend_avg = day_df[day_df['day_of_week'].isin(weekends)]['conversion_rate'].mean()
    
    print(f"\nWEEKDAY vs WEEKEND ANALYSIS:")
    print(f"Weekday average conversion: {weekday_avg:.2f}%")
    print(f"Weekend average conversion: {weekend_avg:.2f}%")
    
    if weekday_avg > weekend_avg:
        print(f"Weekdays perform {weekday_avg - weekend_avg:.2f} percentage points better than weekends")
    else:
        print(f"Weekends perform {weekend_avg - weekday_avg:.2f} percentage points better than weekdays")
    
    # Additional analysis
    print(f"\nDETAILED BREAKDOWN:")
    for _, row in day_df.iterrows():
        print(f"{row['day_of_week'].upper()}:")
        print(f"  - Total Searchers: {row['total_searchers']:,}")
        print(f"  - Converting Users: {row['funnel_users']:,}")
        print(f"  - Conversion Rate: {row['conversion_rate']:.2f}%")
        print()
    
    conn.close()
    
    return day_df

if __name__ == "__main__":
    results = create_day_of_week_conversion_chart()
