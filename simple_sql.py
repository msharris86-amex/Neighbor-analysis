#!/usr/bin/env python3
"""
Simple Interactive SQL Interface
================================

A lightweight SQL interface that doesn't require pandas.
"""

import sqlite3
import csv
import os

def create_database():
    """Create database from CSV files"""
    if not os.path.exists('marketplace_analysis.db'):
        print("Creating database from CSV files...")
        
        conn = sqlite3.connect('marketplace_analysis.db')
        
        # Load and create search_events table
        with open('all_search_events (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS search_events')
            conn.execute('CREATE TABLE search_events (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO search_events VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load and create listing_views table
        with open('view_listing_detail_events (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS listing_views')
            conn.execute('CREATE TABLE listing_views (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO listing_views VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load and create reservations table
        with open('reservations (1).csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            conn.execute('DROP TABLE IF EXISTS reservations')
            conn.execute('CREATE TABLE reservations (' + ','.join([f'{h} TEXT' for h in headers]) + ')')
            
            for row in reader:
                conn.execute('INSERT INTO reservations VALUES (' + ','.join(['?' for _ in headers]) + ')', row)
        
        # Load and create amplitude_user_ids table
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

def show_tables(conn):
    """Show available tables"""
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
    """Execute SQL query and return results"""
    try:
        cursor = conn.execute(query)
        rows = cursor.fetchall()
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        return column_names, rows
    except Exception as e:
        return None, str(e)

def format_results(column_names, rows, max_rows=20):
    """Format results for display"""
    if not column_names:
        return "No results"
    
    # Limit rows for display
    display_rows = rows[:max_rows]
    
    # Calculate column widths
    widths = []
    for i, col in enumerate(column_names):
        max_width = len(col)
        for row in display_rows:
            if i < len(row) and row[i] is not None:
                max_width = max(max_width, len(str(row[i])))
        widths.append(min(max_width, 30))  # Cap at 30 characters
    
    # Print header
    header = " | ".join([col.ljust(widths[i]) for i, col in enumerate(column_names)])
    print(header)
    print("-" * len(header))
    
    # Print rows
    for row in display_rows:
        row_str = " | ".join([str(row[i] if i < len(row) and row[i] is not None else "").ljust(widths[i]) for i in range(len(column_names))])
        print(row_str)
    
    if len(rows) > max_rows:
        print(f"\n... and {len(rows) - max_rows} more rows")
    
    return len(rows)

def interactive_sql():
    """Main interactive SQL interface"""
    print("=" * 60)
    print("🔍 SIMPLE INTERACTIVE SQL INTERFACE")
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
            column_names, result = execute_query(conn, query)
            
            if column_names is None:
                print(f"❌ Error: {result}")
            else:
                print("\n📊 RESULTS:")
                print("-" * 40)
                if len(result) == 0:
                    print("No results found.")
                else:
                    row_count = format_results(column_names, result)
                    print(f"\n📈 {row_count} rows returned")
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






