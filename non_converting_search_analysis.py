import pandas as pd
import numpy as np

def analyze_non_converting_searches():
    """
    Analyze characteristics of searches that do NOT convert to understand barriers.
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
    
    # Users who searched but did NOT convert
    non_converting_searchers = searchers - funnel_users
    
    print(f"Total searchers: {len(searchers):,}")
    print(f"Funnel users (converted): {len(funnel_users):,}")
    print(f"Non-converting searchers: {len(non_converting_searchers):,}")
    print(f"Non-conversion rate: {len(non_converting_searchers) / len(searchers) * 100:.2f}%")
    
    # Create a function to analyze non-conversion by category
    def analyze_non_conversion_by_category(df, category_col, min_count=100):
        results = []
        
        for category in df[category_col].unique():
            if pd.isna(category):
                continue
                
            # Get searches for this category
            category_searches = df[df[category_col] == category]
            category_users = set(category_searches['merged_amplitude_id'].unique())
            
            # Calculate non-conversion metrics
            total_searchers = len(category_users)
            non_converting_in_category = len(category_users.intersection(non_converting_searchers))
            non_conversion_rate = (non_converting_in_category / total_searchers * 100) if total_searchers > 0 else 0
            
            if total_searchers >= min_count:
                results.append({
                    'category': category,
                    'total_searchers': total_searchers,
                    'non_converting_users': non_converting_in_category,
                    'non_conversion_rate': non_conversion_rate
                })
        
        return pd.DataFrame(results).sort_values('non_conversion_rate', ascending=False)
    
    # Analyze non-conversion by different search characteristics
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH TYPE")
    print("="*80)
    search_type_analysis = analyze_non_conversion_by_category(search_events, 'search_type', min_count=50)
    print(search_type_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH TERM CATEGORY")
    print("="*80)
    search_term_analysis = analyze_non_conversion_by_category(search_events, 'search_term_category', min_count=50)
    print(search_term_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH SORT")
    print("="*80)
    search_sort_analysis = analyze_non_conversion_by_category(search_events, 'search_sort', min_count=50)
    print(search_sort_analysis.to_string(index=False))
    
    print("\n" + "="*80)
    print("NON-CONVERSION BY DMA (MARKET)")
    print("="*80)
    dma_analysis = analyze_non_conversion_by_category(search_events, 'search_dma', min_count=50)
    print(dma_analysis.head(10).to_string(index=False))
    
    # Analyze by search result count ranges
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH RESULT COUNT")
    print("="*80)
    
    search_events['result_count_bin'] = pd.cut(search_events['count_results'], 
                                               bins=[0, 10, 50, 100, 200, 1000], 
                                               labels=['1-10', '11-50', '51-100', '101-200', '200+'])
    
    result_count_analysis = analyze_non_conversion_by_category(search_events, 'result_count_bin', min_count=50)
    print(result_count_analysis.to_string(index=False))
    
    # Analyze by search term length
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH TERM LENGTH")
    print("="*80)
    
    search_events['search_term_length'] = search_events['search_term'].str.len()
    search_events['term_length_bin'] = pd.cut(search_events['search_term_length'], 
                                             bins=[0, 5, 10, 15, 30, 100], 
                                             labels=['1-5 chars', '6-10 chars', '11-15 chars', '16-30 chars', '30+ chars'])
    
    term_length_analysis = analyze_non_conversion_by_category(search_events, 'term_length_bin', min_count=50)
    print(term_length_analysis.to_string(index=False))
    
    # Analyze by geographic characteristics
    print("\n" + "="*80)
    print("NON-CONVERSION BY GEOGRAPHIC REGION")
    print("="*80)
    
    geo_analysis = analyze_non_conversion_by_category(search_events, 'is_usa_canada', min_count=50)
    print(geo_analysis.to_string(index=False))
    
    # Analyze by bot/host status
    print("\n" + "="*80)
    print("NON-CONVERSION BY USER TYPE")
    print("="*80)
    
    # Bot analysis
    bot_analysis = analyze_non_conversion_by_category(search_events, 'is_bot', min_count=50)
    print("Bot vs Non-Bot:")
    print(bot_analysis.to_string(index=False))
    
    # Host analysis
    host_analysis = analyze_non_conversion_by_category(search_events, 'is_host', min_count=50)
    print("\nHost vs Non-Host:")
    print(host_analysis.to_string(index=False))
    
    # Analyze by time patterns
    print("\n" + "="*80)
    print("NON-CONVERSION BY TIME PATTERNS")
    print("="*80)
    
    # Month analysis
    month_analysis = analyze_non_conversion_by_category(search_events, 'month', min_count=50)
    print("Non-conversion by Month:")
    print(month_analysis.to_string(index=False))
    
    # Day of week analysis
    search_events['day_of_week'] = search_events['event_time'].dt.day_name()
    dow_analysis = analyze_non_conversion_by_category(search_events, 'day_of_week', min_count=50)
    print("\nNon-conversion by Day of Week:")
    print(dow_analysis.to_string(index=False))
    
    # Hour analysis
    search_events['hour'] = search_events['event_time'].dt.hour
    search_events['hour_bin'] = pd.cut(search_events['hour'], 
                                      bins=[0, 6, 12, 18, 24], 
                                      labels=['Night (0-6)', 'Morning (6-12)', 'Afternoon (12-18)', 'Evening (18-24)'])
    
    hour_analysis = analyze_non_conversion_by_category(search_events, 'hour_bin', min_count=50)
    print("\nNon-conversion by Time of Day:")
    print(hour_analysis.to_string(index=False))
    
    # Analyze by attribution source/channel
    print("\n" + "="*80)
    print("NON-CONVERSION BY ATTRIBUTION")
    print("="*80)
    
    # Source analysis
    source_analysis = analyze_non_conversion_by_category(search_events, 'first_attribution_source', min_count=50)
    print("Non-conversion by Source:")
    print(source_analysis.to_string(index=False))
    
    # Channel analysis
    channel_analysis = analyze_non_conversion_by_category(search_events, 'first_attribution_channel', min_count=50)
    print("\nNon-conversion by Channel:")
    print(channel_analysis.to_string(index=False))
    
    # Analyze worst performing search terms
    print("\n" + "="*80)
    print("WORST PERFORMING SEARCH TERMS")
    print("="*80)
    
    # Get search terms with high non-conversion rates
    term_analysis = analyze_non_conversion_by_category(search_events, 'search_term', min_count=20)
    worst_terms = term_analysis.head(10)
    print("Top 10 search terms by non-conversion rate:")
    print(worst_terms.to_string(index=False))
    
    # Analyze by search behavior patterns
    print("\n" + "="*80)
    print("NON-CONVERSION BY SEARCH BEHAVIOR")
    print("="*80)
    
    # Users with multiple searches vs single searches
    user_search_counts = search_events.groupby('merged_amplitude_id').size()
    search_events['user_search_count'] = search_events['merged_amplitude_id'].map(user_search_counts)
    search_events['search_frequency'] = pd.cut(search_events['user_search_count'], 
                                              bins=[0, 1, 3, 10, 100], 
                                              labels=['1 search', '2-3 searches', '4-10 searches', '10+ searches'])
    
    frequency_analysis = analyze_non_conversion_by_category(search_events, 'search_frequency', min_count=50)
    print("Non-conversion by Search Frequency:")
    print(frequency_analysis.to_string(index=False))
    
    # Analyze search-to-click drop-off
    print("\n" + "="*80)
    print("SEARCH-TO-CLICK DROP-OFF ANALYSIS")
    print("="*80)
    
    # Users who searched but never clicked
    searchers_who_never_clicked = searchers - clickers
    print(f"Users who searched but never clicked: {len(searchers_who_never_clicked):,}")
    print(f"Search-to-click drop-off rate: {len(searchers_who_never_clicked) / len(searchers) * 100:.2f}%")
    
    # Users who clicked but never reserved
    clickers_who_never_reserved = clickers - reservers
    print(f"Users who clicked but never reserved: {len(clickers_who_never_reserved):,}")
    print(f"Click-to-reserve drop-off rate: {len(clickers_who_never_reserved) / len(clickers) * 100:.2f}%")
    
    # Analyze characteristics of users who never clicked
    def analyze_never_clicked_by_category(df, category_col, min_count=100):
        results = []
        
        for category in df[category_col].unique():
            if pd.isna(category):
                continue
                
            # Get searches for this category
            category_searches = df[df[category_col] == category]
            category_users = set(category_searches['merged_amplitude_id'].unique())
            
            # Calculate never-clicked metrics
            total_searchers = len(category_users)
            never_clicked_in_category = len(category_users.intersection(searchers_who_never_clicked))
            never_clicked_rate = (never_clicked_in_category / total_searchers * 100) if total_searchers > 0 else 0
            
            if total_searchers >= min_count:
                results.append({
                    'category': category,
                    'total_searchers': total_searchers,
                    'never_clicked_users': never_clicked_in_category,
                    'never_clicked_rate': never_clicked_rate
                })
        
        return pd.DataFrame(results).sort_values('never_clicked_rate', ascending=False)
    
    print("\nSearch types with highest never-clicked rates:")
    never_clicked_by_type = analyze_never_clicked_by_category(search_events, 'search_type', min_count=50)
    print(never_clicked_by_type.to_string(index=False))
    
    print("\nSearch term categories with highest never-clicked rates:")
    never_clicked_by_term = analyze_never_clicked_by_category(search_events, 'search_term_category', min_count=50)
    print(never_clicked_by_term.to_string(index=False))
    
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
        'source': source_analysis,
        'channel': channel_analysis,
        'worst_terms': worst_terms,
        'search_frequency': frequency_analysis,
        'never_clicked_by_type': never_clicked_by_type,
        'never_clicked_by_term': never_clicked_by_term
    }

if __name__ == "__main__":
    results = analyze_non_converting_searches()
