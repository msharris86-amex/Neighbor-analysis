#!/usr/bin/env python3
"""
Marketplace Data Visualization
=============================

This script creates various charts and visualizations to analyze the marketplace data.
"""

import sqlite3
import csv
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
import os

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

def create_database():
    """Create database from CSV files if it doesn't exist"""
    if not os.path.exists('marketplace_analysis.db'):
        print("Creating database from CSV files...")
        
        conn = sqlite3.connect('marketplace_analysis.db')
        
        # Load search events
        with open('all_search_events (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS search_events')
            conn.execute('CREATE TABLE search_events (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO search_events VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load listing views
        with open('view_listing_detail_events (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS listing_views')
            conn.execute('CREATE TABLE listing_views (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO listing_views VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load reservations
        with open('reservations (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS reservations')
            conn.execute('CREATE TABLE reservations (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO reservations VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load user IDs
        with open('amplitude_user_ids (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS amplitude_user_ids')
            conn.execute('CREATE TABLE amplitude_user_ids (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO amplitude_user_ids VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        conn.commit()
        conn.close()
        print("Database created successfully!")
    else:
        print("Database already exists!")

def get_data(conn, query):
    """Execute query and return results"""
    cursor = conn.execute(query)
    return cursor.fetchall()

def create_conversion_funnel_chart(conn):
    """Create conversion funnel visualization"""
    print("Creating conversion funnel chart...")
    
    # Get funnel metrics
    query = """
    WITH cleaned_search_events AS (
        SELECT * FROM search_events WHERE is_bot = 'False'
    ),
    cleaned_listing_views AS (
        SELECT * FROM listing_views WHERE is_bot = 'False'
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
    
    result = get_data(conn, query)[0]
    searchers, viewers, reservers, payers = result
    
    # Create funnel chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Funnel bar chart
    stages = ['Searchers', 'Viewers', 'Reservers', 'Payers']
    values = [searchers, viewers, reservers, payers]
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    bars = ax1.barh(stages, values, color=colors)
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
        (viewers / searchers * 100) if searchers > 0 else 0,
        (reservers / viewers * 100) if viewers > 0 else 0,
        (payers / reservers * 100) if reservers > 0 else 0
    ]
    
    ax2.bar(stages, conversion_rates, color=colors)
    ax2.set_ylabel('Conversion Rate (%)')
    ax2.set_title('Conversion Rates by Stage')
    ax2.set_ylim(0, 100)
    ax2.grid(axis='y', alpha=0.3)
    
    # Add percentage labels
    for i, rate in enumerate(conversion_rates):
        ax2.text(i, rate + 1, f'{rate:.1f}%', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('conversion_funnel.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"Conversion Funnel Results:")
    print(f"• Searchers: {searchers:,}")
    print(f"• Viewers: {viewers:,} ({conversion_rates[1]:.1f}% conversion)")
    print(f"• Reservers: {reservers:,} ({conversion_rates[2]:.1f}% conversion)")
    print(f"• Payers: {payers:,} ({conversion_rates[3]:.1f}% conversion)")
    print(f"• Overall conversion: {payers/searchers*100:.2f}%")

def create_search_type_chart(conn):
    """Create search type analysis chart"""
    print("Creating search type analysis chart...")
    
    query = """
    SELECT 
        search_type,
        COUNT(*) as count,
        COUNT(DISTINCT merged_amplitude_id) as unique_users
    FROM search_events 
    WHERE is_bot = 'False'
    GROUP BY search_type
    ORDER BY count DESC
    """
    
    result = get_data(conn, query)
    
    search_types = [row[0] for row in result]
    counts = [row[1] for row in result]
    unique_users = [row[2] for row in result]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Search volume by type
    ax1.bar(search_types, counts, color='skyblue')
    ax1.set_title('Search Volume by Type')
    ax1.set_ylabel('Number of Searches')
    ax1.tick_params(axis='x', rotation=45)
    
    # Add value labels
    for i, count in enumerate(counts):
        ax1.text(i, count + max(counts)*0.01, f'{count:,}', ha='center', fontweight='bold')
    
    # Unique users by type
    ax2.bar(search_types, unique_users, color='lightcoral')
    ax2.set_title('Unique Users by Search Type')
    ax2.set_ylabel('Number of Unique Users')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add value labels
    for i, users in enumerate(unique_users):
        ax2.text(i, users + max(unique_users)*0.01, f'{users:,}', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('search_type_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_geographic_chart(conn):
    """Create geographic analysis chart"""
    print("Creating geographic analysis chart...")
    
    query = """
    SELECT 
        search_dma,
        COUNT(DISTINCT merged_amplitude_id) as unique_searchers
    FROM search_events 
    WHERE is_bot = 'False' AND search_dma IS NOT NULL
    GROUP BY search_dma
    ORDER BY unique_searchers DESC
    LIMIT 15
    """
    
    result = get_data(conn, query)
    
    dmas = [row[0] for row in result]
    searchers = [row[1] for row in result]
    
    plt.figure(figsize=(12, 8))
    bars = plt.barh(dmas, searchers, color='lightgreen')
    plt.title('Top 15 DMAs by Search Volume')
    plt.xlabel('Number of Unique Searchers')
    
    # Add value labels
    for bar, value in zip(bars, searchers):
        plt.text(bar.get_width() + max(searchers)*0.01, bar.get_y() + bar.get_height()/2, 
                f'{value:,}', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('geographic_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_attribution_chart(conn):
    """Create attribution source analysis chart"""
    print("Creating attribution analysis chart...")
    
    query = """
    SELECT 
        first_attribution_source,
        first_attribution_channel,
        COUNT(DISTINCT merged_amplitude_id) as unique_users
    FROM search_events 
    WHERE is_bot = 'False' AND first_attribution_source IS NOT NULL
    GROUP BY first_attribution_source, first_attribution_channel
    HAVING COUNT(DISTINCT merged_amplitude_id) >= 50
    ORDER BY unique_users DESC
    LIMIT 10
    """
    
    result = get_data(conn, query)
    
    labels = [f"{row[0]} - {row[1]}" for row in result]
    users = [row[2] for row in result]
    
    plt.figure(figsize=(12, 8))
    bars = plt.barh(labels, users, color='lightblue')
    plt.title('Top Attribution Sources by User Count')
    plt.xlabel('Number of Unique Users')
    
    # Add value labels
    for bar, value in zip(bars, users):
        plt.text(bar.get_width() + max(users)*0.01, bar.get_y() + bar.get_height()/2, 
                f'{value:,}', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('attribution_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_monthly_trends_chart(conn):
    """Create monthly trends chart"""
    print("Creating monthly trends chart...")
    
    query = """
    SELECT 
        month,
        COUNT(DISTINCT merged_amplitude_id) as unique_searchers,
        COUNT(*) as total_searches
    FROM search_events 
    WHERE is_bot = 'False'
    GROUP BY month
    ORDER BY month
    """
    
    result = get_data(conn, query)
    
    months = [row[0] for row in result]
    searchers = [row[1] for row in result]
    searches = [row[2] for row in result]
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Monthly search volume
    ax1.plot(months, searchers, marker='o', linewidth=2, markersize=8, color='blue')
    ax1.set_title('Monthly Search Volume')
    ax1.set_ylabel('Number of Unique Searchers')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(months)
    
    # Add value labels
    for i, value in enumerate(searchers):
        ax1.text(months[i], value + max(searchers)*0.02, f'{value:,}', ha='center', fontweight='bold')
    
    # Monthly total searches
    ax2.plot(months, searches, marker='s', linewidth=2, markersize=8, color='red')
    ax2.set_title('Monthly Total Searches')
    ax2.set_ylabel('Number of Total Searches')
    ax2.set_xlabel('Month')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(months)
    
    # Add value labels
    for i, value in enumerate(searches):
        ax2.text(months[i], value + max(searches)*0.02, f'{value:,}', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('monthly_trends.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_payment_analysis_chart(conn):
    """Create payment analysis chart"""
    print("Creating payment analysis chart...")
    
    query = """
    SELECT 
        COUNT(*) as total_reservations,
        COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) as successful_payments,
        COUNT(CASE WHEN approved_at IS NOT NULL AND successful_payment_collected_at IS NULL THEN 1 END) as pending_payments,
        COUNT(CASE WHEN approved_at IS NULL THEN 1 END) as rejected_reservations
    FROM reservations
    """
    
    result = get_data(conn, query)[0]
    total, successful, pending, rejected = result
    
    # Payment status pie chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    payment_status = ['Successful', 'Pending', 'Rejected']
    payment_counts = [successful, pending, rejected]
    colors = ['#2ca02c', '#ff7f0e', '#d62728']
    
    ax1.pie(payment_counts, labels=payment_status, autopct='%1.1f%%', colors=colors, startangle=90)
    ax1.set_title('Payment Status Distribution')
    
    # Payment completion rate
    completion_rate = (successful / total * 100) if total > 0 else 0
    ax2.bar(['Payment Completion Rate'], [completion_rate], color='lightgreen')
    ax2.set_ylabel('Completion Rate (%)')
    ax2.set_title('Overall Payment Completion Rate')
    ax2.set_ylim(0, 100)
    
    # Add percentage label
    ax2.text(0, completion_rate + 1, f'{completion_rate:.1f}%', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('payment_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"Payment Analysis Results:")
    print(f"• Total reservations: {total:,}")
    print(f"• Successful payments: {successful:,}")
    print(f"• Pending payments: {pending:,}")
    print(f"• Rejected reservations: {rejected:,}")
    print(f"• Payment completion rate: {completion_rate:.1f}%")

def create_search_terms_chart(conn):
    """Create search terms analysis chart"""
    print("Creating search terms analysis chart...")
    
    query = """
    SELECT 
        search_term_category,
        COUNT(*) as count,
        COUNT(DISTINCT merged_amplitude_id) as unique_users
    FROM search_events 
    WHERE is_bot = 'False' AND search_term_category IS NOT NULL
    GROUP BY search_term_category
    ORDER BY count DESC
    """
    
    result = get_data(conn, query)
    
    categories = [row[0] for row in result]
    counts = [row[1] for row in result]
    unique_users = [row[2] for row in result]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Search volume by category
    ax1.bar(categories, counts, color='lightblue')
    ax1.set_title('Search Volume by Category')
    ax1.set_ylabel('Number of Searches')
    ax1.tick_params(axis='x', rotation=45)
    
    # Add value labels
    for i, count in enumerate(counts):
        ax1.text(i, count + max(counts)*0.01, f'{count:,}', ha='center', fontweight='bold')
    
    # Unique users by category
    ax2.bar(categories, unique_users, color='lightcoral')
    ax2.set_title('Unique Users by Search Category')
    ax2.set_ylabel('Number of Unique Users')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add value labels
    for i, users in enumerate(unique_users):
        ax2.text(i, users + max(unique_users)*0.01, f'{users:,}', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('search_terms_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """Main function to create all charts"""
    print("=" * 60)
    print("📊 CREATING MARKETPLACE DATA VISUALIZATIONS")
    print("=" * 60)
    
    # Create database if needed
    create_database()
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    try:
        # Create all charts
        create_conversion_funnel_chart(conn)
        create_search_type_chart(conn)
        create_geographic_chart(conn)
        create_attribution_chart(conn)
        create_monthly_trends_chart(conn)
        create_payment_analysis_chart(conn)
        create_search_terms_chart(conn)
        
        print("\n" + "=" * 60)
        print("✅ ALL CHARTS CREATED SUCCESSFULLY!")
        print("=" * 60)
        print("Charts saved as PNG files:")
        print("• conversion_funnel.png")
        print("• search_type_analysis.png")
        print("• geographic_analysis.png")
        print("• attribution_analysis.png")
        print("• monthly_trends.png")
        print("• payment_analysis.png")
        print("• search_terms_analysis.png")
        
    except Exception as e:
        print(f"❌ Error creating charts: {str(e)}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()







