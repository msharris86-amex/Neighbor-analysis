#!/usr/bin/env python3
"""
Marketplace Conversion Funnel Analysis
======================================

This script performs comprehensive analysis of the marketplace conversion funnel
from search to payment, using SQL for data processing and Python for visualization.

Author: Data Analysis Team
Date: 2024
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import sqlite3
import os
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Configure display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def create_database_from_csvs():
    """Create SQLite database from CSV files for SQL analysis"""
    print("Creating SQLite database from CSV files...")
    
    # Create database connection
    conn = sqlite3.connect('marketplace_analysis.db')
    
    # Load CSV files
    print("Loading CSV files...")
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
    
    # Write to database
    search_events.to_sql('search_events', conn, if_exists='replace', index=False)
    listing_views.to_sql('listing_views', conn, if_exists='replace', index=False)
    reservations.to_sql('reservations', conn, if_exists='replace', index=False)
    user_ids.to_sql('amplitude_user_ids', conn, if_exists='replace', index=False)
    
    print(f"Database created with {len(search_events):,} search events, {len(listing_views):,} listing views, {len(reservations):,} reservations")
    
    return conn

def run_sql_analysis(conn):
    """Run the comprehensive SQL analysis"""
    print("Running SQL analysis...")
    
    # Read SQL file
    with open('sql_analysis.sql', 'r') as f:
        sql_script = f.read()
    
    # Split into individual queries (simplified for SQLite)
    queries = [
        # Basic funnel metrics
        """
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
            'FUNNEL_METRICS' AS analysis_type,
            COUNT(DISTINCT s.merged_amplitude_id) AS total_searchers,
            COUNT(DISTINCT l.merged_amplitude_id) AS total_viewers,
            COUNT(DISTINCT r.renter_user_id) AS total_reservers,
            COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                               THEN r.renter_user_id END) AS total_payers
        FROM cleaned_search_events s
        LEFT JOIN cleaned_listing_views l ON s.merged_amplitude_id = l.merged_amplitude_id
        LEFT JOIN cleaned_reservations r ON s.merged_amplitude_id = r.renter_user_id
        """,
        
        # Search type analysis
        """
        WITH cleaned_search_events AS (
            SELECT * FROM search_events WHERE is_bot = 0
        ),
        cleaned_listing_views AS (
            SELECT * FROM listing_views WHERE is_bot = 0
        )
        SELECT 
            'SEARCH_TYPE' AS analysis_type,
            s.search_type,
            COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
            COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
            ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
                  NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS conversion_rate
        FROM cleaned_search_events s
        LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
        GROUP BY s.search_type
        ORDER BY conversion_rate DESC
        """,
        
        # Attribution analysis
        """
        WITH cleaned_search_events AS (
            SELECT * FROM search_events WHERE is_bot = 0
        ),
        cleaned_listing_views AS (
            SELECT * FROM listing_views WHERE is_bot = 0
        )
        SELECT 
            'ATTRIBUTION' AS analysis_type,
            s.first_attribution_source,
            s.first_attribution_channel,
            COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
            COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
            ROUND(COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
                  NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2) AS conversion_rate
        FROM cleaned_search_events s
        LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
        GROUP BY s.first_attribution_source, s.first_attribution_channel
        HAVING COUNT(DISTINCT s.merged_amplitude_id) >= 10
        ORDER BY unique_searchers DESC
        LIMIT 10
        """,
        
        # Monthly trends
        """
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
            'MONTHLY' AS analysis_type,
            s.month,
            COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
            COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
            COUNT(DISTINCT r.renter_user_id) AS unique_reservers,
            COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                               THEN r.renter_user_id END) AS unique_payers
        FROM cleaned_search_events s
        LEFT JOIN cleaned_listing_views l ON s.month = l.month
        LEFT JOIN cleaned_reservations r ON CAST(strftime('%m', r.created_at) AS INTEGER) = s.month
        GROUP BY s.month
        ORDER BY s.month
        """,
        
        # Payment analysis
        """
        SELECT 
            'PAYMENT' AS analysis_type,
            COUNT(*) AS total_reservations,
            COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) AS successful_payments,
            COUNT(CASE WHEN approved_at IS NOT NULL AND successful_payment_collected_at IS NULL THEN 1 END) AS pending_payments,
            COUNT(CASE WHEN approved_at IS NULL THEN 1 END) AS rejected_reservations,
            ROUND(COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) * 100.0 / 
                  COUNT(*), 2) AS payment_completion_rate
        FROM reservations
        """
    ]
    
    # Execute queries and collect results
    results = {}
    for i, query in enumerate(queries):
        try:
            df = pd.read_sql_query(query, conn)
            results[f'query_{i}'] = df
            print(f"Query {i+1} completed: {len(df)} rows")
        except Exception as e:
            print(f"Error in query {i+1}: {e}")
    
    return results

def create_visualizations(results):
    """Create comprehensive visualizations from SQL results"""
    print("Creating visualizations...")
    
    # Set up the plotting environment
    plt.rcParams['figure.figsize'] = (15, 10)
    plt.rcParams['font.size'] = 12
    
    # 1. Funnel Metrics Visualization
    if 'query_0' in results and not results['query_0'].empty:
        funnel_data = results['query_0'].iloc[0]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Funnel bar chart
        stages = ['Searchers', 'Viewers', 'Reservers', 'Payers']
        values = [funnel_data['total_searchers'], funnel_data['total_viewers'], 
                 funnel_data['total_reservers'], funnel_data['total_payers']]
        
        bars = ax1.barh(stages, values, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax1.set_xlabel('Number of Users')
        ax1.set_title('Conversion Funnel - User Counts')
        ax1.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for bar, value in zip(bars, values):
            ax1.text(bar.get_width() + max(values)*0.01, bar.get_y() + bar.get_height()/2, 
                    f'{value:,}', va='center', fontweight='bold')
        
        # Conversion rates
        conversion_rates = [
            100,  # Search to search (baseline)
            (funnel_data['total_viewers'] / funnel_data['total_searchers'] * 100) if funnel_data['total_searchers'] > 0 else 0,
            (funnel_data['total_reservers'] / funnel_data['total_viewers'] * 100) if funnel_data['total_viewers'] > 0 else 0,
            (funnel_data['total_payers'] / funnel_data['total_reservers'] * 100) if funnel_data['total_reservers'] > 0 else 0
        ]
        
        ax2.bar(stages, conversion_rates, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax2.set_ylabel('Conversion Rate (%)')
        ax2.set_title('Conversion Rates by Stage')
        ax2.set_ylim(0, 100)
        ax2.grid(axis='y', alpha=0.3)
        
        # Add percentage labels
        for i, rate in enumerate(conversion_rates):
            ax2.text(i, rate + 1, f'{rate:.1f}%', ha='center', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('funnel_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    # 2. Search Type Performance
    if 'query_1' in results and not results['query_1'].empty:
        search_type_data = results['query_1']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Search volume by type
        ax1.bar(search_type_data['search_type'], search_type_data['unique_searchers'], color='skyblue')
        ax1.set_title('Search Volume by Type')
        ax1.set_ylabel('Number of Searchers')
        ax1.tick_params(axis='x', rotation=45)
        
        # Conversion rate by type
        ax2.bar(search_type_data['search_type'], search_type_data['conversion_rate'], color='lightcoral')
        ax2.set_title('Conversion Rate by Search Type')
        ax2.set_ylabel('Conversion Rate (%)')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('search_type_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    # 3. Attribution Performance
    if 'query_2' in results and not results['query_2'].empty:
        attribution_data = results['query_2']
        
        plt.figure(figsize=(12, 8))
        attribution_data.set_index(['first_attribution_source', 'first_attribution_channel'])['unique_searchers'].plot(kind='barh')
        plt.title('Top Attribution Sources by User Count')
        plt.xlabel('Number of Unique Users')
        plt.tight_layout()
        plt.savefig('attribution_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    # 4. Monthly Trends
    if 'query_3' in results and not results['query_3'].empty:
        monthly_data = results['query_3']
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
        
        # Monthly search volume
        ax1.plot(monthly_data['month'], monthly_data['unique_searchers'], marker='o', linewidth=2, markersize=8)
        ax1.set_title('Monthly Search Volume')
        ax1.set_ylabel('Number of Searchers')
        ax1.grid(True, alpha=0.3)
        ax1.set_xticks(monthly_data['month'])
        
        # Monthly conversion rates
        conversion_rates = (monthly_data['unique_viewers'] / monthly_data['unique_searchers'] * 100).fillna(0)
        ax2.plot(monthly_data['month'], conversion_rates, marker='s', color='red', linewidth=2, markersize=8)
        ax2.set_title('Monthly Conversion Rate (Search to View)')
        ax2.set_ylabel('Conversion Rate (%)')
        ax2.set_xlabel('Month')
        ax2.grid(True, alpha=0.3)
        ax2.set_xticks(monthly_data['month'])
        
        plt.tight_layout()
        plt.savefig('monthly_trends.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    # 5. Payment Analysis
    if 'query_4' in results and not results['query_4'].empty:
        payment_data = results['query_4'].iloc[0]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Payment status pie chart
        payment_status = ['Successful', 'Pending', 'Rejected']
        payment_counts = [payment_data['successful_payments'], 
                         payment_data['pending_payments'], 
                         payment_data['rejected_reservations']]
        colors = ['#2ca02c', '#ff7f0e', '#d62728']
        
        ax1.pie(payment_counts, labels=payment_status, autopct='%1.1f%%', colors=colors, startangle=90)
        ax1.set_title('Payment Status Distribution')
        
        # Payment completion rate
        ax2.bar(['Payment Completion Rate'], [payment_data['payment_completion_rate']], color='lightgreen')
        ax2.set_ylabel('Completion Rate (%)')
        ax2.set_title('Overall Payment Completion Rate')
        ax2.set_ylim(0, 100)
        
        # Add percentage label
        ax2.text(0, payment_data['payment_completion_rate'] + 1, 
                f'{payment_data["payment_completion_rate"]:.1f}%', ha='center', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('payment_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

def generate_business_recommendations(results):
    """Generate data-driven business recommendations"""
    print("Generating business recommendations...")
    
    recommendations = []
    
    # Analyze funnel metrics
    if 'query_0' in results and not results['query_0'].empty:
        funnel_data = results['query_0'].iloc[0]
        
        search_to_view = (funnel_data['total_viewers'] / funnel_data['total_searchers'] * 100) if funnel_data['total_searchers'] > 0 else 0
        view_to_reserve = (funnel_data['total_reservers'] / funnel_data['total_viewers'] * 100) if funnel_data['total_viewers'] > 0 else 0
        reserve_to_pay = (funnel_data['total_payers'] / funnel_data['total_reservers'] * 100) if funnel_data['total_reservers'] > 0 else 0
        
        recommendations.append({
            'priority': 'HIGH',
            'area': 'Search-to-View Conversion',
            'current_rate': f"{search_to_view:.1f}%",
            'recommendation': 'Optimize search result relevance and improve listing previews',
            'impact': f"Potential to recover {funnel_data['total_searchers'] - funnel_data['total_viewers']:,} users"
        })
        
        recommendations.append({
            'priority': 'MEDIUM',
            'area': 'View-to-Reserve Conversion',
            'current_rate': f"{view_to_reserve:.1f}%",
            'recommendation': 'Enhance listing detail pages and add social proof',
            'impact': f"Potential to recover {funnel_data['total_viewers'] - funnel_data['total_reservers']:,} users"
        })
    
    # Analyze search type performance
    if 'query_1' in results and not results['query_1'].empty:
        search_type_data = results['query_1']
        best_type = search_type_data.loc[search_type_data['conversion_rate'].idxmax()]
        worst_type = search_type_data.loc[search_type_data['conversion_rate'].idxmin()]
        
        recommendations.append({
            'priority': 'MEDIUM',
            'area': f'Search Type Optimization',
            'current_rate': f"Best: {best_type['conversion_rate']:.1f}%, Worst: {worst_type['conversion_rate']:.1f}%",
            'recommendation': f'Focus on improving {worst_type["search_type"]} search experience',
            'impact': f"Learn from {best_type['search_type']} success factors"
        })
    
    # Analyze payment completion
    if 'query_4' in results and not results['query_4'].empty:
        payment_data = results['query_4'].iloc[0]
        
        recommendations.append({
            'priority': 'HIGH',
            'area': 'Payment Completion',
            'current_rate': f"{payment_data['payment_completion_rate']:.1f}%",
            'recommendation': 'Streamline payment process and add multiple payment options',
            'impact': f"Potential to recover {payment_data['pending_payments']:,} pending payments"
        })
    
    return recommendations

def main():
    """Main analysis function"""
    print("=== MARKETPLACE CONVERSION FUNNEL ANALYSIS ===")
    print("Starting comprehensive analysis...")
    
    # Create database and run SQL analysis
    conn = create_database_from_csvs()
    results = run_sql_analysis(conn)
    
    # Create visualizations
    create_visualizations(results)
    
    # Generate recommendations
    recommendations = generate_business_recommendations(results)
    
    # Print summary
    print("\n=== ANALYSIS SUMMARY ===")
    for result_name, result_df in results.items():
        if not result_df.empty:
            print(f"\n{result_name}: {len(result_df)} rows")
            print(result_df.head())
    
    print("\n=== BUSINESS RECOMMENDATIONS ===")
    for rec in recommendations:
        print(f"\n🎯 {rec['priority']} PRIORITY: {rec['area']}")
        print(f"   Current Rate: {rec['current_rate']}")
        print(f"   Recommendation: {rec['recommendation']}")
        print(f"   Impact: {rec['impact']}")
    
    # Close database connection
    conn.close()
    
    print("\n=== ANALYSIS COMPLETE ===")
    print("Visualizations saved as PNG files")
    print("SQL analysis completed successfully")

if __name__ == "__main__":
    main()

