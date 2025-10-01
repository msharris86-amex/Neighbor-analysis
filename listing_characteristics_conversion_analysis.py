import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
import seaborn as sns

def analyze_listing_characteristics_conversion():
    """
    Analyze which listing characteristics lead to higher conversion rates.
    """
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    print("Loading data for listing characteristics conversion analysis...")
    
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
    
    print(f"Total funnel users: {len(funnel_users):,}")
    
    # Analyze by different listing characteristics
    analyses = {}
    
    # 1. Search Position Analysis
    print("\n1. SEARCH POSITION ANALYSIS")
    print("="*50)
    position_analysis = analyze_by_search_position(click_events, funnel_users)
    analyses['search_position'] = position_analysis
    
    # 2. Search Sort Analysis
    print("\n2. SEARCH SORT ANALYSIS")
    print("="*50)
    sort_analysis = analyze_by_search_sort(search_events, funnel_users)
    analyses['search_sort'] = sort_analysis
    
    # 3. Search Term Category Analysis
    print("\n3. SEARCH TERM CATEGORY ANALYSIS")
    print("="*50)
    category_analysis = analyze_by_search_term_category(search_events, funnel_users)
    analyses['search_term_category'] = category_analysis
    
    # 4. Geographic Analysis
    print("\n4. GEOGRAPHIC ANALYSIS")
    print("="*50)
    geo_analysis = analyze_by_geography(search_events, funnel_users)
    analyses['geography'] = geo_analysis
    
    # 5. User Type Analysis
    print("\n5. USER TYPE ANALYSIS")
    print("="*50)
    user_type_analysis = analyze_by_user_type(search_events, funnel_users)
    analyses['user_type'] = user_type_analysis
    
    # 6. Time-based Analysis
    print("\n6. TIME-BASED ANALYSIS")
    print("="*50)
    time_analysis = analyze_by_time(search_events, funnel_users)
    analyses['time'] = time_analysis
    
    # Create summary visualization
    create_summary_chart(analyses)
    
    # Generate insights
    generate_insights(analyses)
    
    conn.close()
    
    return analyses

def analyze_by_search_position(click_events, funnel_users):
    """Analyze conversion by search position"""
    position_data = []
    
    for position in range(1, 11):  # Top 10 positions
        position_clicks = click_events[click_events['search_position'] == str(position)]
        position_users = set(position_clicks['merged_amplitude_id'].unique())
        
        total_users = len(position_users)
        converting_users = len(position_users.intersection(funnel_users))
        conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
        
        position_data.append({
            'characteristic': f'Position {position}',
            'total_users': total_users,
            'converting_users': converting_users,
            'conversion_rate': conversion_rate
        })
    
    df = pd.DataFrame(position_data)
    print("Search Position Conversion Rates:")
    print(df.to_string(index=False))
    return df

def analyze_by_search_sort(search_events, funnel_users):
    """Analyze conversion by search sort preference"""
    sort_data = []
    
    for sort_type in search_events['search_sort'].unique():
        if pd.isna(sort_type):
            continue
            
        sort_searches = search_events[search_events['search_sort'] == sort_type]
        sort_users = set(sort_searches['merged_amplitude_id'].unique())
        
        total_users = len(sort_users)
        converting_users = len(sort_users.intersection(funnel_users))
        conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
        
        sort_data.append({
            'characteristic': sort_type,
            'total_users': total_users,
            'converting_users': converting_users,
            'conversion_rate': conversion_rate
        })
    
    df = pd.DataFrame(sort_data).sort_values('conversion_rate', ascending=False)
    print("Search Sort Conversion Rates:")
    print(df.to_string(index=False))
    return df

def analyze_by_search_term_category(search_events, funnel_users):
    """Analyze conversion by search term category"""
    category_data = []
    
    for category in search_events['search_term_category'].unique():
        if pd.isna(category):
            continue
            
        category_searches = search_events[search_events['search_term_category'] == category]
        category_users = set(category_searches['merged_amplitude_id'].unique())
        
        total_users = len(category_users)
        converting_users = len(category_users.intersection(funnel_users))
        conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
        
        category_data.append({
            'characteristic': category,
            'total_users': total_users,
            'converting_users': converting_users,
            'conversion_rate': conversion_rate
        })
    
    df = pd.DataFrame(category_data).sort_values('conversion_rate', ascending=False)
    print("Search Term Category Conversion Rates:")
    print(df.to_string(index=False))
    return df

def analyze_by_geography(search_events, funnel_users):
    """Analyze conversion by geographic characteristics"""
    geo_data = []
    
    # USA/Canada vs International
    usa_canada_searches = search_events[search_events['is_usa_canada'] == 'True']
    usa_canada_users = set(usa_canada_searches['merged_amplitude_id'].unique())
    
    total_users = len(usa_canada_users)
    converting_users = len(usa_canada_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    geo_data.append({
        'characteristic': 'USA/Canada',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    # International
    intl_searches = search_events[search_events['is_usa_canada'] == 'False']
    intl_users = set(intl_searches['merged_amplitude_id'].unique())
    
    total_users = len(intl_users)
    converting_users = len(intl_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    geo_data.append({
        'characteristic': 'International',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    df = pd.DataFrame(geo_data).sort_values('conversion_rate', ascending=False)
    print("Geographic Conversion Rates:")
    print(df.to_string(index=False))
    return df

def analyze_by_user_type(search_events, funnel_users):
    """Analyze conversion by user type (host vs non-host)"""
    user_type_data = []
    
    # Non-hosts
    non_host_searches = search_events[search_events['is_host'] == 'False']
    non_host_users = set(non_host_searches['merged_amplitude_id'].unique())
    
    total_users = len(non_host_users)
    converting_users = len(non_host_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    user_type_data.append({
        'characteristic': 'Non-Hosts',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    # Hosts
    host_searches = search_events[search_events['is_host'] == 'True']
    host_users = set(host_searches['merged_amplitude_id'].unique())
    
    total_users = len(host_users)
    converting_users = len(host_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    user_type_data.append({
        'characteristic': 'Hosts',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    df = pd.DataFrame(user_type_data).sort_values('conversion_rate', ascending=False)
    print("User Type Conversion Rates:")
    print(df.to_string(index=False))
    return df

def analyze_by_time(search_events, funnel_users):
    """Analyze conversion by time characteristics"""
    time_data = []
    
    # Extract hour of day
    search_events['hour'] = search_events['event_time'].dt.hour
    
    # Peak hours (9-17) vs Off-peak hours
    peak_searches = search_events[(search_events['hour'] >= 9) & (search_events['hour'] <= 17)]
    peak_users = set(peak_searches['merged_amplitude_id'].unique())
    
    total_users = len(peak_users)
    converting_users = len(peak_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    time_data.append({
        'characteristic': 'Peak Hours (9-17)',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    # Off-peak hours
    off_peak_searches = search_events[(search_events['hour'] < 9) | (search_events['hour'] > 17)]
    off_peak_users = set(off_peak_searches['merged_amplitude_id'].unique())
    
    total_users = len(off_peak_users)
    converting_users = len(off_peak_users.intersection(funnel_users))
    conversion_rate = (converting_users / total_users * 100) if total_users > 0 else 0
    
    time_data.append({
        'characteristic': 'Off-Peak Hours',
        'total_users': total_users,
        'converting_users': converting_users,
        'conversion_rate': conversion_rate
    })
    
    df = pd.DataFrame(time_data).sort_values('conversion_rate', ascending=False)
    print("Time-based Conversion Rates:")
    print(df.to_string(index=False))
    return df

def create_summary_chart(analyses):
    """Create a summary chart of all analyses"""
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    
    analysis_names = ['search_position', 'search_sort', 'search_term_category', 'geography', 'user_type', 'time']
    
    for i, (name, df) in enumerate(analyses.items()):
        if i < len(axes):
            ax = axes[i]
            
            # Create bar chart
            bars = ax.bar(df['characteristic'], df['conversion_rate'], 
                         color=plt.cm.viridis(np.linspace(0, 1, len(df))), alpha=0.8)
            
            ax.set_title(f'{name.replace("_", " ").title()} Conversion Rates', fontweight='bold')
            ax.set_ylabel('Conversion Rate (%)')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value labels
            for bar, rate in zip(bars, df['conversion_rate']):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                       f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=8)
    
    plt.tight_layout()
    plt.savefig('listing_characteristics_conversion_summary.png', dpi=300, bbox_inches='tight')
    print("\nSummary chart saved as 'listing_characteristics_conversion_summary.png'")
    plt.show()

def generate_insights(analyses):
    """Generate key insights from all analyses"""
    print("\n" + "="*80)
    print("KEY INSIGHTS: LISTING CHARACTERISTICS THAT DRIVE CONVERSION")
    print("="*80)
    
    # Find best performing characteristics in each category
    for name, df in analyses.items():
        if len(df) > 0:
            best = df.loc[df['conversion_rate'].idxmax()]
            worst = df.loc[df['conversion_rate'].idxmin()]
            
            print(f"\n{name.upper().replace('_', ' ')}:")
            print(f"  Best: {best['characteristic']} ({best['conversion_rate']:.2f}%)")
            print(f"  Worst: {worst['characteristic']} ({worst['conversion_rate']:.2f}%)")
            print(f"  Gap: {best['conversion_rate'] - worst['conversion_rate']:.2f} percentage points")
    
    # Overall recommendations
    print(f"\n" + "="*80)
    print("STRATEGIC RECOMMENDATIONS:")
    print("="*80)
    
    # Get the best performing characteristic overall
    all_best = []
    for name, df in analyses.items():
        if len(df) > 0:
            best = df.loc[df['conversion_rate'].idxmax()]
            all_best.append((name, best['characteristic'], best['conversion_rate']))
    
    all_best.sort(key=lambda x: x[2], reverse=True)
    
    print("Top 5 Highest Converting Characteristics:")
    for i, (category, characteristic, rate) in enumerate(all_best[:5], 1):
        print(f"{i}. {category}: {characteristic} ({rate:.2f}%)")
    
    print(f"\nFocus on optimizing for these high-converting characteristics to improve overall conversion rates.")

if __name__ == "__main__":
    results = analyze_listing_characteristics_conversion()
