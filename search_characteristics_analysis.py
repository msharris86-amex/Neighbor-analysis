import pandas as pd
import numpy as np

def analyze_search_characteristics():
    """
    Analyze which search characteristics lead to better conversion rates.
    """
    
    # Read the data files
    print("Loading data files...")
    search_events = pd.read_csv('all_search_events (1).csv')
    click_events = pd.read_csv('view_listing_detail_events (1).csv')
    reservations = pd.read_csv('reservations (1).csv')
    
    print(f"Loaded {len(search_events)} search events")
    print(f"Loaded {len(click_events)} click events")
    print(f"Loaded {len(reservations)} reservations")
    
    # Convert timestamps to datetime
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    click_events['event_time'] = pd.to_datetime(click_events['event_time'])
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    
    # Get users who completed the full funnel
    searchers = set(search_events['merged_amplitude_id'].unique())
    clickers = set(click_events['merged_amplitude_id'].unique())
    reservers = set(reservations['renter_user_id'].unique())
    
    # Users who did the full funnel
    funnel_users = searchers.intersection(clickers).intersection(reservers)
    
    print(f"Total funnel users (searched -> clicked -> reserved): {len(funnel_users):,}")
    
    # Create a function to analyze conversion by category
    def analyze_by_category(df, category_col, min_count=100):
        results = []
        
        for category in df[category_col].unique():
            if pd.isna(category):
                continue
                
            # Get searches for this category
            category_searches = df[df[category_col] == category]
            category_users = set(category_searches['merged_amplitude_id'].unique())
            
            # Calculate conversion metrics
            total_searchers = len(category_users)
            funnel_users_in_category = len(category_users.intersection(funnel_users))
            conversion_rate = (funnel_users_in_category / total_searchers * 100) if total_searchers > 0 else 0
            
            if total_searchers >= min_count:
                results.append({
                    'category': category,
                    'total_searchers': total_searchers,
                    'funnel_users': funnel_users_in_category,
                    'conversion_rate': conversion_rate
                })
        
        return pd.DataFrame(results).sort_values('conversion_rate', ascending=False)
    
    # Analyze by different search characteristics
    print("\n" + "="*80)
    print("SEARCH TYPE ANALYSIS")
    print("="*80)
    search_type_analysis = analyze_by_category(search_events, 'search_type', min_count=50)
    print(search_type_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("SEARCH TERM CATEGORY ANALYSIS")
    print("="*80)
    search_term_analysis = analyze_by_category(search_events, 'search_term_category', min_count=50)
    print(search_term_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("SEARCH SORT ANALYSIS")
    print("="*80)
    search_sort_analysis = analyze_by_category(search_events, 'search_sort', min_count=50)
    print(search_sort_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("DMA (MARKET) ANALYSIS")
    print("="*80)
    dma_analysis = analyze_by_category(search_events, 'search_dma', min_count=50)
    print(dma_analysis.head(10).to_string(index=False))
    
    # Analyze by search result count ranges
    print("\n" + "="*80)
    print("SEARCH RESULT COUNT ANALYSIS")
    print("="*80)
    
    # Create result count bins
    search_events['result_count_bin'] = pd.cut(search_events['count_results'], 
                                               bins=[0, 10, 50, 100, 200, 1000], 
                                               labels=['1-10', '11-50', '51-100', '101-200', '200+'])
    
    result_count_analysis = analyze_by_category(search_events, 'result_count_bin', min_count=50)
    print(result_count_analysis.to_string(index=False))
    
    # Analyze by search term length
    print("\n" + "="*80)
    print("SEARCH TERM LENGTH ANALYSIS")
    print("="*80)
    
    search_events['search_term_length'] = search_events['search_term'].str.len()
    search_events['term_length_bin'] = pd.cut(search_events['search_term_length'], 
                                             bins=[0, 5, 10, 15, 30, 100], 
                                             labels=['1-5 chars', '6-10 chars', '11-15 chars', '16-30 chars', '30+ chars'])
    
    term_length_analysis = analyze_by_category(search_events, 'term_length_bin', min_count=50)
    print(term_length_analysis.to_string(index=False))
    
    # Analyze by geographic characteristics
    print("\n" + "="*80)
    print("GEOGRAPHIC ANALYSIS")
    print("="*80)
    
    # USA/Canada vs International
    geo_analysis = analyze_by_category(search_events, 'is_usa_canada', min_count=50)
    print(geo_analysis.to_string(index=False))
    
    # Analyze by bot/host status
    print("\n" + "="*80)
    print("USER TYPE ANALYSIS")
    print("="*80)
    
    # Bot analysis
    bot_analysis = analyze_by_category(search_events, 'is_bot', min_count=50)
    print("Bot vs Non-Bot:")
    print(bot_analysis.to_string(index=False))
    
    # Host analysis
    host_analysis = analyze_by_category(search_events, 'is_host', min_count=50)
    print("\nHost vs Non-Host:")
    print(host_analysis.to_string(index=False))
    
    # Analyze by time patterns
    print("\n" + "="*80)
    print("TIME PATTERN ANALYSIS")
    print("="*80)
    
    # Month analysis
    month_analysis = analyze_by_category(search_events, 'month', min_count=50)
    print("Conversion by Month:")
    print(month_analysis.to_string(index=False))
    
    # Day of week analysis
    search_events['day_of_week'] = search_events['event_time'].dt.day_name()
    dow_analysis = analyze_by_category(search_events, 'day_of_week', min_count=50)
    print("\nConversion by Day of Week:")
    print(dow_analysis.to_string(index=False))
    
    # Hour analysis
    search_events['hour'] = search_events['event_time'].dt.hour
    search_events['hour_bin'] = pd.cut(search_events['hour'], 
                                      bins=[0, 6, 12, 18, 24], 
                                      labels=['Night (0-6)', 'Morning (6-12)', 'Afternoon (12-18)', 'Evening (18-24)'])
    
    hour_analysis = analyze_by_category(search_events, 'hour_bin', min_count=50)
    print("\nConversion by Time of Day:")
    print(hour_analysis.to_string(index=False))
    
    # Advanced analysis: Top performing search terms
    print("\n" + "="*80)
    print("TOP PERFORMING SEARCH TERMS")
    print("="*80)
    
    # Get search terms with high conversion rates
    term_analysis = analyze_by_category(search_events, 'search_term', min_count=20)
    top_terms = term_analysis.head(10)
    print("Top 10 search terms by conversion rate:")
    print(top_terms.to_string(index=False))
    
    # Analyze by search behavior patterns
    print("\n" + "="*80)
    print("SEARCH BEHAVIOR ANALYSIS")
    print("="*80)
    
    # Users with multiple searches vs single searches
    user_search_counts = search_events.groupby('merged_amplitude_id').size()
    search_events['user_search_count'] = search_events['merged_amplitude_id'].map(user_search_counts)
    search_events['search_frequency'] = pd.cut(search_events['user_search_count'], 
                                              bins=[0, 1, 3, 10, 100], 
                                              labels=['1 search', '2-3 searches', '4-10 searches', '10+ searches'])
    
    frequency_analysis = analyze_by_category(search_events, 'search_frequency', min_count=50)
    print("Conversion by Search Frequency:")
    print(frequency_analysis.to_string(index=False))
    
    return {
        'search_type': search_type_analysis,
        'search_term_category': search_term_analysis,
        'search_sort': search_sort_analysis,
        'dma': dma_analysis,
        'result_count': result_count_analysis,
        'term_length': term_length_analysis,
        'geo': geo_analysis,
        'bot': bot_analysis,
        'host': host_analysis,
        'month': month_analysis,
        'day_of_week': dow_analysis,
        'hour': hour_analysis,
        'top_terms': top_terms,
        'search_frequency': frequency_analysis
    }

if __name__ == "__main__":
    results = analyze_search_characteristics()
