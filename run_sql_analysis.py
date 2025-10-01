#!/usr/bin/env python3
"""
Run SQL Analysis for Marketplace Conversion Funnel
==================================================

This script executes the SQL analysis queries and displays the results.
"""

import pandas as pd
import sqlite3
import os

def create_database():
    """Create SQLite database from CSV files"""
    print("Creating SQLite database from CSV files...")
    
    # Load CSV files
    search_events = pd.read_csv('all_search_events (1).csv')
    listing_views = pd.read_csv('view_listing_detail_events (1).csv')
    reservations = pd.read_csv('reservations (1).csv')
    user_ids = pd.read_csv('amplitude_user_ids (1).csv')
    
    # Convert date columns
    search_events['event_time'] = pd.to_datetime(search_events['event_time'])
    search_events['event_date'] = pd.to_datetime(search_events['event_date'])
    
    listing_views['event_time'] = pd.to_datetime(listing_views['event_time'])
    listing_views['event_date'] = pd.to_datetime(listing_views['event_date'])
    
    reservations['created_at'] = pd.to_datetime(reservations['created_at'])
    reservations['approved_at'] = pd.to_datetime(reservations['approved_at'])
    reservations['successful_payment_collected_at'] = pd.to_datetime(reservations['successful_payment_collected_at'])
    
    # Create database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    # Write data to database
    search_events.to_sql('search_events', conn, if_exists='replace', index=False)
    listing_views.to_sql('listing_views', conn, if_exists='replace', index=False)
    reservations.to_sql('reservations', conn, if_exists='replace', index=False)
    user_ids.to_sql('amplitude_user_ids', conn, if_exists='replace', index=False)
    
    print(f"Database created with {len(search_events):,} search events, {len(listing_views):,} listing views, {len(reservations):,} reservations")
    return conn

def run_sql_queries(conn):
    """Run the SQL analysis queries"""
    print("\n" + "="*60)
    print("RUNNING SQL ANALYSIS QUERIES")
    print("="*60)
    
    # Query 1: Basic Funnel Metrics
    print("\n1. BASIC FUNNEL METRICS")
    print("-" * 30)
    query1 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    ),
    cleaned_reservations AS (
        SELECT * FROM reservations WHERE renter_user_id IS NOT NULL
    )
    SELECT 
        COUNT(DISTINCT s.merged_amplitude_id) AS total_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS total_viewers,
        COUNT(DISTINCT r.renter_user_id) AS total_reservers,
        COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                           THEN r.renter_user_id END) AS total_payers
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.merged_amplitude_id = l.merged_amplitude_id
    LEFT JOIN cleaned_reservations r ON s.merged_amplitude_id = r.renter_user_id
    """
    
    result1 = pd.read_sql_query(query1, conn)
    print(result1.to_string(index=False))
    
    # Calculate conversion rates
    if not result1.empty:
        searchers = result1['total_searchers'].iloc[0]
        viewers = result1['total_viewers'].iloc[0]
        reservers = result1['total_reservers'].iloc[0]
        payers = result1['total_payers'].iloc[0]
        
        print(f"\nConversion Rates:")
        print(f"Search → View: {(viewers/searchers*100):.2f}%")
        print(f"View → Reserve: {(reservers/viewers*100):.2f}%" if viewers > 0 else "View → Reserve: N/A")
        print(f"Reserve → Pay: {(payers/reservers*100):.2f}%" if reservers > 0 else "Reserve → Pay: N/A")
        print(f"Overall: {(payers/searchers*100):.2f}%")
    
    # Query 2: Search Type Analysis
    print("\n\n2. SEARCH TYPE PERFORMANCE")
    print("-" * 30)
    query2 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    )
    SELECT 
        s.search_type,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(AVG(s.count_results), 2) AS avg_results_per_search,
        ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
              NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS conversion_rate
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.search_type
    ORDER BY conversion_rate DESC
    """
    
    result2 = pd.read_sql_query(query2, conn)
    print(result2.to_string(index=False))
    
    # Query 3: Attribution Analysis
    print("\n\n3. ATTRIBUTION SOURCE PERFORMANCE")
    print("-" * 30)
    query3 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    )
    SELECT 
        s.first_attribution_source,
        s.first_attribution_channel,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
              NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS conversion_rate
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.first_attribution_source, s.first_attribution_channel
    HAVING COUNT(DISTINCT s.merged_amplitude_id) >= 10
    ORDER BY unique_searchers DESC
    LIMIT 10
    """
    
    result3 = pd.read_sql_query(query3, conn)
    print(result3.to_string(index=False))
    
    # Query 4: Geographic Analysis (DMA)
    print("\n\n4. GEOGRAPHIC PERFORMANCE (DMA)")
    print("-" * 30)
    query4 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    ),
    cleaned_reservations AS (
        SELECT * FROM reservations WHERE renter_user_id IS NOT NULL
    )
    SELECT 
        s.search_dma,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT r.renter_user_id) AS unique_reservers,
        COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                           THEN r.renter_user_id END) AS unique_payers,
        ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
              NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS search_to_view_rate,
        ROUND(COUNT(DISTINCT r.renter_user_id) * 100.0 / 
              NULLIF(COUNT(DISTINCT l.merged_amplitude_id), 0), 2) AS view_to_reserve_rate,
        ROUND(COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                              THEN r.renter_user_id END) * 100.0 / 
              NULLIF(COUNT(DISTINCT r.renter_user_id), 0), 2) AS reserve_to_pay_rate
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_dma = l.click_dma
    LEFT JOIN cleaned_reservations r ON l.listing_id = r.listing_id
    GROUP BY s.search_dma
    HAVING COUNT(DISTINCT s.merged_amplitude_id) >= 50
    ORDER BY unique_searchers DESC
    LIMIT 10
    """
    
    result4 = pd.read_sql_query(query4, conn)
    print(result4.to_string(index=False))
    
    # Query 5: Monthly Trends
    print("\n\n5. MONTHLY TRENDS")
    print("-" * 30)
    query5 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    ),
    cleaned_reservations AS (
        SELECT * FROM reservations WHERE renter_user_id IS NOT NULL
    )
    SELECT 
        s.month,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT r.renter_user_id) AS unique_reservers,
        COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                           THEN r.renter_user_id END) AS unique_payers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(AVG(s.count_results), 2) AS avg_search_results
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.month = l.month
    LEFT JOIN cleaned_reservations r ON CAST(strftime('%m', r.created_at) AS INTEGER) = s.month
    GROUP BY s.month
    ORDER BY s.month
    """
    
    result5 = pd.read_sql_query(query5, conn)
    print(result5.to_string(index=False))
    
    # Query 6: Payment Analysis
    print("\n\n6. PAYMENT ANALYSIS")
    print("-" * 30)
    query6 = """
    SELECT 
        COUNT(*) AS total_reservations,
        COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) AS successful_payments,
        COUNT(CASE WHEN approved_at IS NOT NULL AND successful_payment_collected_at IS NULL THEN 1 END) AS pending_payments,
        COUNT(CASE WHEN approved_at IS NULL THEN 1 END) AS rejected_reservations,
        ROUND(COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) * 100.0 / 
              COUNT(*), 2) AS payment_completion_rate
    FROM reservations
    """
    
    result6 = pd.read_sql_query(query6, conn)
    print(result6.to_string(index=False))
    
    # Query 7: Search Term Category Analysis
    print("\n\n7. SEARCH TERM CATEGORY ANALYSIS")
    print("-" * 30)
    query7 = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 0
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 0
    )
    SELECT 
        s.search_term_category,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(AVG(s.count_results), 2) AS avg_results,
        ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
              NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS conversion_rate
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.search_term_category
    ORDER BY unique_searchers DESC
    """
    
    result7 = pd.read_sql_query(query7, conn)
    print(result7.to_string(index=False))
    
    return {
        'funnel_metrics': result1,
        'search_type': result2,
        'attribution': result3,
        'geographic': result4,
        'monthly': result5,
        'payment': result6,
        'categories': result7
    }

def main():
    """Main function to run the analysis"""
    print("=== MARKETPLACE CONVERSION FUNNEL SQL ANALYSIS ===")
    
    # Create database
    conn = create_database()
    
    # Run SQL queries
    results = run_sql_queries(conn)
    
    # Close connection
    conn.close()
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("All SQL queries executed successfully!")
    print("Results are displayed above for each analysis section.")

if __name__ == "__main__":
    main()







