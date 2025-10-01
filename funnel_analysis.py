import sqlite3
import csv
import matplotlib.pyplot as plt
import pandas as pd

# Create database if needed
conn = sqlite3.connect('marketplace_analysis.db')

# Always reload data to ensure we have all tables
print('Loading data into database...')

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
print('Data loaded successfully!')

# Get funnel metrics
print('\n=== CALCULATING FUNNEL METRICS ===')
query = '''
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
'''

result = conn.execute(query).fetchone()
searchers, viewers, reservers, payers = result

print('\n=== CONVERSION FUNNEL RESULTS ===')
print(f'Searchers: {searchers:,}')
print(f'Viewers: {viewers:,}')
print(f'Reservers: {reservers:,}')
print(f'Payers: {payers:,}')

# Calculate conversion rates
search_to_view = (viewers / searchers * 100) if searchers > 0 else 0
view_to_reserve = (reservers / viewers * 100) if viewers > 0 else 0
reserve_to_pay = (payers / reservers * 100) if reservers > 0 else 0
overall = (payers / searchers * 100) if searchers > 0 else 0

print(f'\n=== CONVERSION RATES ===')
print(f'Search to View: {search_to_view:.2f}%')
print(f'View to Reserve: {view_to_reserve:.2f}%')
print(f'Reserve to Pay: {reserve_to_pay:.2f}%')
print(f'Overall (Search to Pay): {overall:.2f}%')

# Create funnel chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# Funnel bar chart
stages = ['Searchers', 'Viewers', 'Reservers', 'Payers']
values = [searchers, viewers, reservers, payers]
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

bars = ax1.barh(stages, values, color=colors, alpha=0.8)
ax1.set_xlabel('Number of Users', fontsize=12, fontweight='bold')
ax1.set_title('Conversion Funnel - User Counts', fontsize=14, fontweight='bold')
ax1.grid(axis='x', alpha=0.3)

# Add value labels
for bar, value in zip(bars, values):
    ax1.text(bar.get_width() + max(values)*0.01, bar.get_y() + bar.get_height()/2, 
            f'{value:,}', va='center', fontweight='bold', fontsize=11)

# Conversion rates
conversion_rates = [100, search_to_view, view_to_reserve, reserve_to_pay]

bars2 = ax2.bar(stages, conversion_rates, color=colors, alpha=0.8)
ax2.set_ylabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
ax2.set_title('Conversion Rates by Stage', fontsize=14, fontweight='bold')
ax2.set_ylim(0, 100)
ax2.grid(axis='y', alpha=0.3)

# Add percentage labels
for i, rate in enumerate(conversion_rates):
    ax2.text(i, rate + 1, f'{rate:.1f}%', ha='center', fontweight='bold', fontsize=11)

plt.tight_layout()
plt.savefig('funnel_metrics.png', dpi=300, bbox_inches='tight')
plt.show()

print(f'\n=== KEY INSIGHTS ===')
print(f'- {searchers - viewers:,} users ({100-search_to_view:.1f}%) dropped off after searching')
print(f'- {viewers - reservers:,} users ({100-view_to_reserve:.1f}%) dropped off after viewing')
print(f'- {reservers - payers:,} users ({100-reserve_to_pay:.1f}%) dropped off after reserving')
print(f'- Only {overall:.2f}% of searchers complete the full journey to payment')

conn.close()
