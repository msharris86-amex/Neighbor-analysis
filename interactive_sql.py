#!/usr/bin/env python3
"""
Interactive SQL Interface for Marketplace Analysis
=================================================

This script provides an interactive SQL interface to query the marketplace database.
You can write SQL queries and see the results immediately.
"""

import sqlite3
import pandas as pd
import sys
import os

def create_database():
    """Create the database if it doesn't exist"""
    if not os.path.exists('marketplace_analysis.db'):
        print("Creating database from CSV files...")
        
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
        
        conn.close()
        print("Database created successfully!")
    else:
        print("Database already exists!")

def show_tables(conn):
    """Show available tables and their schemas"""
    print("\n=== AVAILABLE TABLES ===")
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f"\n📊 Table: {table_name}")
        
        # Get column info
        cursor = conn.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        print("   Columns:")
        for col in columns:
            print(f"     - {col[1]} ({col[2]})")
        
        # Get row count
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        print(f"   Rows: {count:,}")

def execute_query(conn, query):
    """Execute a SQL query and return results"""
    try:
        # Use pandas for better formatting
        result = pd.read_sql_query(query, conn)
        return result
    except Exception as e:
        return f"Error: {str(e)}"

def interactive_sql():
    """Main interactive SQL interface"""
    print("=" * 60)
    print("🔍 INTERACTIVE SQL INTERFACE FOR MARKETPLACE ANALYSIS")
    print("=" * 60)
    print("Type your SQL queries below. Type 'help' for commands, 'quit' to exit.")
    print()
    
    # Create database if needed
    create_database()
    
    # Connect to database
    conn = sqlite3.connect('marketplace_analysis.db')
    
    # Show available tables
    show_tables(conn)
    
    print("\n" + "=" * 60)
    print("💡 QUICK START QUERIES:")
    print("=" * 60)
    print("• SELECT * FROM search_events LIMIT 10;")
    print("• SELECT search_type, COUNT(*) FROM search_events GROUP BY search_type;")
    print("• SELECT search_dma, COUNT(*) FROM search_events GROUP BY search_dma ORDER BY COUNT(*) DESC LIMIT 10;")
    print("• SELECT * FROM reservations WHERE successful_payment_collected_at IS NOT NULL LIMIT 10;")
    print()
    
    while True:
        try:
            # Get user input
            query = input("SQL> ").strip()
            
            # Handle special commands
            if query.lower() in ['quit', 'exit', 'q']:
                print("Goodbye! 👋")
                break
            elif query.lower() == 'help':
                print("\n📚 AVAILABLE COMMANDS:")
                print("• help - Show this help message")
                print("• tables - Show available tables")
                print("• quit/exit/q - Exit the interface")
                print("• Any valid SQL query")
                print("\n💡 TIP: End your queries with semicolon (;)")
                continue
            elif query.lower() == 'tables':
                show_tables(conn)
                continue
            elif not query:
                continue
            
            # Execute query
            result = execute_query(conn, query)
            
            if isinstance(result, str) and result.startswith("Error:"):
                print(f"❌ {result}")
            else:
                print("\n📊 RESULTS:")
                print("-" * 40)
                if len(result) == 0:
                    print("No results found.")
                else:
                    # Display results with better formatting
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    pd.set_option('display.max_colwidth', None)
                    print(result.to_string(index=False))
                    print(f"\n📈 {len(result)} rows returned")
                print("-" * 40)
            
            print()  # Add spacing
            
        except KeyboardInterrupt:
            print("\n\nGoodbye! 👋")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    interactive_sql()







