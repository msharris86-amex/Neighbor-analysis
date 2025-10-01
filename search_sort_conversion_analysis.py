import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def analyze_conversion_by_search_sort():
    """
    Analyze conversion rates by search sort preference
    """
    print("="*80)
    print("CONVERSION RATE BY SEARCH SORT PREFERENCE ANALYSIS")
    print("="*80)
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    # Get search events with conversion data
    query = """
    SELECT 
        merged_amplitude_id,
        search_sort,
        event_date,
        search_term,
        count_results,
        search_type,
        is_bot,
        is_host
    FROM search_events 
    WHERE search_sort IS NOT NULL 
    AND is_bot = 0
    """
    
    search_events = pd.read_sql_query(query, conn)
    
    # Get conversion data (users who completed the full funnel)
    # We'll use users who have both search events and listing views as our conversion metric
    conversion_query = """
    SELECT DISTINCT se.merged_amplitude_id
    FROM search_events se
    WHERE se.is_bot = 0
    AND EXISTS (
        SELECT 1 FROM listing_views lv 
        WHERE lv.merged_amplitude_id = se.merged_amplitude_id
    )
    """
    
    converting_users = pd.read_sql_query(conversion_query, conn)
    converting_user_set = set(converting_users['merged_amplitude_id'].unique())
    
    print(f"Total search events analyzed: {len(search_events):,}")
    print(f"Total converting users: {len(converting_user_set):,}")
    
    # Analyze conversion by search sort
    def analyze_by_search_sort():
        results = []
        
        for sort_type in search_events['search_sort'].unique():
            if pd.isna(sort_type):
                continue
                
            # Get users who used this sort type
            sort_searches = search_events[search_events['search_sort'] == sort_type]
            sort_users = set(sort_searches['merged_amplitude_id'].unique())
            
            # Calculate conversion metrics
            total_users = len(sort_users)
            converting_users_count = len(sort_users.intersection(converting_user_set))
            conversion_rate = (converting_users_count / total_users * 100) if total_users > 0 else 0
            
            # Additional metrics
            total_searches = len(sort_searches)
            avg_searches_per_user = total_searches / total_users if total_users > 0 else 0
            avg_results_per_search = sort_searches['count_results'].mean()
            
            results.append({
                'search_sort': sort_type,
                'total_users': total_users,
                'converting_users': converting_users_count,
                'conversion_rate': conversion_rate,
                'total_searches': total_searches,
                'avg_searches_per_user': avg_searches_per_user,
                'avg_results_per_search': avg_results_per_search
            })
        
        return pd.DataFrame(results).sort_values('conversion_rate', ascending=False)
    
    # Run analysis
    sort_analysis = analyze_by_search_sort()
    
    print("\nCONVERSION RATE BY SEARCH SORT:")
    print("="*50)
    print(sort_analysis.to_string(index=False, float_format='%.2f'))
    
    # Create visualization
    plt.figure(figsize=(12, 8))
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Conversion Rate by Search Sort
    colors = ['#2E8B57', '#FF6B6B', '#4ECDC4', '#45B7D1']
    bars1 = ax1.bar(sort_analysis['search_sort'], sort_analysis['conversion_rate'], 
                   color=colors[:len(sort_analysis)], alpha=0.8)
    ax1.set_title('Conversion Rate by Search Sort Preference', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Conversion Rate (%)')
    ax1.set_xlabel('Search Sort Type')
    ax1.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars1, sort_analysis['conversion_rate']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    # 2. Total Users by Search Sort
    bars2 = ax2.bar(sort_analysis['search_sort'], sort_analysis['total_users'], 
                   color=colors[:len(sort_analysis)], alpha=0.8)
    ax2.set_title('Total Users by Search Sort Preference', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Number of Users')
    ax2.set_xlabel('Search Sort Type')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars2, sort_analysis['total_users']):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(sort_analysis['total_users'])*0.01, 
                f'{value:,}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Average Searches per User by Search Sort
    bars3 = ax3.bar(sort_analysis['search_sort'], sort_analysis['avg_searches_per_user'], 
                   color=colors[:len(sort_analysis)], alpha=0.8)
    ax3.set_title('Average Searches per User by Search Sort', fontsize=14, fontweight='bold')
    ax3.set_ylabel('Average Searches per User')
    ax3.set_xlabel('Search Sort Type')
    ax3.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars3, sort_analysis['avg_searches_per_user']):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(sort_analysis['avg_searches_per_user'])*0.01, 
                f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
    
    # 4. Average Results per Search by Search Sort
    bars4 = ax4.bar(sort_analysis['search_sort'], sort_analysis['avg_results_per_search'], 
                   color=colors[:len(sort_analysis)], alpha=0.8)
    ax4.set_title('Average Results per Search by Search Sort', fontsize=14, fontweight='bold')
    ax4.set_ylabel('Average Results per Search')
    ax4.set_xlabel('Search Sort Type')
    ax4.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars4, sort_analysis['avg_results_per_search']):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(sort_analysis['avg_results_per_search'])*0.01, 
                f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    # Save the chart
    chart_filename = 'search_sort_conversion_analysis.png'
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"\nChart saved as: {chart_filename}")
    
    # Show the chart
    plt.show()
    
    # Additional detailed analysis
    print("\n" + "="*80)
    print("DETAILED ANALYSIS BY SEARCH SORT")
    print("="*80)
    
    for _, row in sort_analysis.iterrows():
        sort_type = row['search_sort']
        print(f"\n{sort_type.upper()}:")
        print(f"  - Total Users: {row['total_users']:,}")
        print(f"  - Converting Users: {row['converting_users']:,}")
        print(f"  - Conversion Rate: {row['conversion_rate']:.2f}%")
        print(f"  - Total Searches: {row['total_searches']:,}")
        print(f"  - Avg Searches per User: {row['avg_searches_per_user']:.2f}")
        print(f"  - Avg Results per Search: {row['avg_results_per_search']:.1f}")
    
    # Calculate overall conversion rate for comparison
    total_users = len(set(search_events['merged_amplitude_id'].unique()))
    overall_conversion_rate = (len(converting_user_set) / total_users * 100) if total_users > 0 else 0
    
    print(f"\nOVERALL CONVERSION RATE: {overall_conversion_rate:.2f}%")
    
    # Identify best and worst performing sort types
    best_sort = sort_analysis.iloc[0]
    worst_sort = sort_analysis.iloc[-1]
    
    print(f"\nBEST PERFORMING SORT: {best_sort['search_sort']} ({best_sort['conversion_rate']:.2f}%)")
    print(f"WORST PERFORMING SORT: {worst_sort['search_sort']} ({worst_sort['conversion_rate']:.2f}%)")
    
    conn.close()
    
    return sort_analysis

if __name__ == "__main__":
    results = analyze_conversion_by_search_sort()
