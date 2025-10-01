import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

def create_host_vs_nonhost_conversion_chart():
    """
    Create a chart showing conversion rates for hosts vs non-hosts.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for host vs non-host conversion analysis...")
    
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
    
    # Analyze by host status
    host_data = []
    
    # Non-hosts (is_host = 'False')
    non_host_searches = search_events[search_events['is_host'] == 'False']
    non_host_users = set(non_host_searches['merged_amplitude_id'].unique())
    non_host_funnel_users = len(non_host_users.intersection(funnel_users))
    non_host_conversion_rate = (non_host_funnel_users / len(non_host_users) * 100) if len(non_host_users) > 0 else 0
    
    host_data.append({
        'user_type': 'Non-Hosts',
        'total_searchers': len(non_host_users),
        'funnel_users': non_host_funnel_users,
        'conversion_rate': non_host_conversion_rate
    })
    
    # Hosts (is_host = 'True')
    host_searches = search_events[search_events['is_host'] == 'True']
    host_users = set(host_searches['merged_amplitude_id'].unique())
    host_funnel_users = len(host_users.intersection(funnel_users))
    host_conversion_rate = (host_funnel_users / len(host_users) * 100) if len(host_users) > 0 else 0
    
    host_data.append({
        'user_type': 'Hosts',
        'total_searchers': len(host_users),
        'funnel_users': host_funnel_users,
        'conversion_rate': host_conversion_rate
    })
    
    # Create DataFrame
    host_df = pd.DataFrame(host_data)
    
    # Create the chart
    plt.figure(figsize=(12, 8))
    
    # Create horizontal bar chart
    colors = ['#2E86AB', '#A23B72']
    bars = plt.barh(host_df['user_type'], host_df['conversion_rate'], 
                    color=colors)
    
    # Customize the chart
    plt.xlabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
    plt.ylabel('User Type', fontsize=12, fontweight='bold')
    plt.title('Conversion Rate: Hosts vs Non-Hosts\n(Search → Click → Reserve)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for i, (bar, rate) in enumerate(zip(bars, host_df['conversion_rate'])):
        plt.text(rate + 0.05, bar.get_y() + bar.get_height()/2, 
                f'{rate:.2f}%', 
                va='center', ha='left', fontweight='bold', fontsize=12)
    
    # Add searcher count labels
    for i, (bar, count) in enumerate(zip(bars, host_df['total_searchers'])):
        plt.text(rate + 0.3, bar.get_y() + bar.get_height()/2, 
                f'({count:,} searchers)', 
                va='center', ha='left', fontsize=10, alpha=0.7)
    
    # Customize appearance
    plt.grid(axis='x', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('host_vs_nonhost_conversion_chart.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'host_vs_nonhost_conversion_chart.png'")
    
    # Show the chart
    plt.show()
    
    # Print summary table
    print("\n" + "="*60)
    print("HOST VS NON-HOST CONVERSION SUMMARY")
    print("="*60)
    print(host_df.to_string(index=False))
    
    # Calculate and display insights
    non_host_row = host_df[host_df['user_type'] == 'Non-Hosts'].iloc[0]
    host_row = host_df[host_df['user_type'] == 'Hosts'].iloc[0]
    
    print(f"\nKEY INSIGHTS:")
    print(f"Non-Hosts conversion rate: {non_host_row['conversion_rate']:.2f}%")
    print(f"Hosts conversion rate: {host_row['conversion_rate']:.2f}%")
    
    if host_row['conversion_rate'] > non_host_row['conversion_rate']:
        difference = host_row['conversion_rate'] - non_host_row['conversion_rate']
        print(f"Hosts convert {difference:.2f} percentage points HIGHER than non-hosts")
    else:
        difference = non_host_row['conversion_rate'] - host_row['conversion_rate']
        print(f"Non-hosts convert {difference:.2f} percentage points HIGHER than hosts")
    
    # Calculate relative performance
    if non_host_row['conversion_rate'] > 0:
        relative_performance = (host_row['conversion_rate'] / non_host_row['conversion_rate'] - 1) * 100
        print(f"Hosts perform {relative_performance:.1f}% {'better' if relative_performance > 0 else 'worse'} than non-hosts")
    
    # Additional analysis
    print(f"\nDETAILED BREAKDOWN:")
    for _, row in host_df.iterrows():
        print(f"{row['user_type'].upper()}:")
        print(f"  - Total Searchers: {row['total_searchers']:,}")
        print(f"  - Converting Users: {row['funnel_users']:,}")
        print(f"  - Conversion Rate: {row['conversion_rate']:.2f}%")
        print()
    
    # Calculate market share
    total_searchers = host_df['total_searchers'].sum()
    print(f"MARKET SHARE:")
    for _, row in host_df.iterrows():
        market_share = (row['total_searchers'] / total_searchers) * 100
        print(f"  - {row['user_type']}: {market_share:.1f}% of total searchers")
    
    conn.close()
    
    return host_df

if __name__ == "__main__":
    results = create_host_vs_nonhost_conversion_chart()
