-- =====================================================
-- MARKETPLACE CONVERSION FUNNEL ANALYSIS - SQL PIPELINE
-- =====================================================
-- This SQL script performs comprehensive data cleaning, joining, and analysis
-- for the marketplace conversion funnel from search to payment.

-- =====================================================
-- 1. DATA CLEANING AND PREPARATION
-- =====================================================

-- Clean search events (remove bots, handle missing values)
WITH cleaned_search_events AS (
    SELECT 
        event_uuid,
        event_type,
        merged_amplitude_id,
        ip_address,
        search_id,
        search_type,
        search_term,
        prior_search_id,
        latitude,
        longitude,
        is_bot,
        is_host,
        is_neighbor_office,
        is_lehi_centerpoint_latlng,
        is_usa_canada,
        count_results,
        top20_listing_id,
        search_sort,
        search_term_category,
        hex_1_resolution,
        hex_2_resolution,
        hex_3_resolution,
        hex_4_resolution,
        hex_5_resolution,
        hex_6_resolution,
        hex_7_resolution,
        hex_8_resolution,
        hex_9_resolution,
        search_dma,
        first_attribution_source,
        first_attribution_channel,
        event_time,
        event_date,
        month,
        -- Add derived fields
        CASE 
            WHEN count_results = 0 THEN 'No Results'
            WHEN count_results BETWEEN 1 AND 10 THEN '1-10 Results'
            WHEN count_results BETWEEN 11 AND 50 THEN '11-50 Results'
            WHEN count_results BETWEEN 51 AND 100 THEN '51-100 Results'
            WHEN count_results BETWEEN 101 AND 200 THEN '101-200 Results'
            ELSE '200+ Results'
        END AS result_count_category,
        
        -- Extract hour of day for time analysis
        EXTRACT(HOUR FROM event_time) AS search_hour,
        
        -- Create search session identifier
        CONCAT(merged_amplitude_id, '_', DATE(event_time)) AS search_session_id
        
    FROM search_events
    WHERE is_bot = FALSE  -- Remove bot traffic
        AND merged_amplitude_id IS NOT NULL
        AND search_id IS NOT NULL
        AND event_time IS NOT NULL
),

-- Clean listing view events
cleaned_listing_views AS (
    SELECT 
        event_uuid,
        event_type,
        merged_amplitude_id,
        ip_address,
        listing_id,
        latitude,
        longitude,
        is_bot,
        is_host,
        is_listing_reserved,
        source_screen,
        search_position,
        search_id,
        hex_1_resolution,
        hex_2_resolution,
        hex_3_resolution,
        hex_4_resolution,
        hex_5_resolution,
        hex_6_resolution,
        hex_7_resolution,
        hex_8_resolution,
        hex_9_resolution,
        click_dma,
        first_attribution_source,
        first_attribution_channel,
        event_time,
        event_date,
        month,
        
        -- Add derived fields
        CASE 
            WHEN search_position BETWEEN 1 AND 5 THEN 'Top 5'
            WHEN search_position BETWEEN 6 AND 10 THEN '6-10'
            WHEN search_position BETWEEN 11 AND 20 THEN '11-20'
            ELSE '20+'
        END AS position_category,
        
        EXTRACT(HOUR FROM event_time) AS view_hour
        
    FROM listing_views
    WHERE is_bot = FALSE  -- Remove bot traffic
        AND merged_amplitude_id IS NOT NULL
        AND listing_id IS NOT NULL
        AND event_time IS NOT NULL
),

-- Clean reservations data
cleaned_reservations AS (
    SELECT 
        reservation_id,
        renter_user_id,
        host_user_id,
        listing_id,
        created_at,
        approved_at,
        successful_payment_collected_at,
        hex_08_id,
        dma,
        
        -- Add derived fields
        CASE 
            WHEN successful_payment_collected_at IS NOT NULL THEN 'Completed'
            WHEN approved_at IS NOT NULL AND successful_payment_collected_at IS NULL THEN 'Pending Payment'
            WHEN approved_at IS NULL THEN 'Pending Approval'
            ELSE 'Unknown'
        END AS reservation_status,
        
        -- Calculate time to approval
        CASE 
            WHEN approved_at IS NOT NULL THEN 
                EXTRACT(EPOCH FROM (approved_at - created_at)) / 3600  -- Hours
            ELSE NULL
        END AS hours_to_approval,
        
        -- Calculate time to payment
        CASE 
            WHEN successful_payment_collected_at IS NOT NULL THEN 
                EXTRACT(EPOCH FROM (successful_payment_collected_at - created_at)) / 3600  -- Hours
            ELSE NULL
        END AS hours_to_payment,
        
        EXTRACT(MONTH FROM created_at) AS reservation_month
        
    FROM reservations
    WHERE renter_user_id IS NOT NULL
        AND listing_id IS NOT NULL
        AND created_at IS NOT NULL
),

-- =====================================================
-- 2. USER JOURNEY ANALYSIS
-- =====================================================

-- Create user journey mapping
user_journey AS (
    SELECT 
        u.merged_amplitude_id,
        u.user_id,
        
        -- Search activity
        COUNT(DISTINCT s.search_id) AS total_searches,
        COUNT(DISTINCT s.event_uuid) AS total_search_events,
        MIN(s.event_time) AS first_search_time,
        MAX(s.event_time) AS last_search_time,
        
        -- View activity
        COUNT(DISTINCT l.listing_id) AS unique_listings_viewed,
        COUNT(DISTINCT l.event_uuid) AS total_view_events,
        MIN(l.event_time) AS first_view_time,
        MAX(l.event_time) AS last_view_time,
        
        -- Reservation activity
        COUNT(DISTINCT r.reservation_id) AS total_reservations,
        COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                           THEN r.reservation_id END) AS successful_payments,
        
        -- Conversion flags
        CASE WHEN COUNT(DISTINCT l.listing_id) > 0 THEN 1 ELSE 0 END AS has_viewed_listing,
        CASE WHEN COUNT(DISTINCT r.reservation_id) > 0 THEN 1 ELSE 0 END AS has_made_reservation,
        CASE WHEN COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                                   THEN r.reservation_id END) > 0 THEN 1 ELSE 0 END AS has_completed_payment
        
    FROM amplitude_user_ids u
    LEFT JOIN cleaned_search_events s ON u.merged_amplitude_id = s.merged_amplitude_id
    LEFT JOIN cleaned_listing_views l ON u.merged_amplitude_id = l.merged_amplitude_id
    LEFT JOIN cleaned_reservations r ON u.user_id = r.renter_user_id
    GROUP BY u.merged_amplitude_id, u.user_id
),

-- =====================================================
-- 3. CONVERSION FUNNEL METRICS
-- =====================================================

-- Calculate overall funnel metrics
funnel_metrics AS (
    SELECT 
        COUNT(DISTINCT merged_amplitude_id) AS total_users,
        COUNT(DISTINCT CASE WHEN total_searches > 0 THEN merged_amplitude_id END) AS users_who_searched,
        COUNT(DISTINCT CASE WHEN has_viewed_listing = 1 THEN merged_amplitude_id END) AS users_who_viewed,
        COUNT(DISTINCT CASE WHEN has_made_reservation = 1 THEN merged_amplitude_id END) AS users_who_reserved,
        COUNT(DISTINCT CASE WHEN has_completed_payment = 1 THEN merged_amplitude_id END) AS users_who_paid,
        
        -- Calculate conversion rates
        ROUND(
            COUNT(DISTINCT CASE WHEN has_viewed_listing = 1 THEN merged_amplitude_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN total_searches > 0 THEN merged_amplitude_id END), 0), 2
        ) AS search_to_view_rate,
        
        ROUND(
            COUNT(DISTINCT CASE WHEN has_made_reservation = 1 THEN merged_amplitude_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN has_viewed_listing = 1 THEN merged_amplitude_id END), 0), 2
        ) AS view_to_reserve_rate,
        
        ROUND(
            COUNT(DISTINCT CASE WHEN has_completed_payment = 1 THEN merged_amplitude_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN has_made_reservation = 1 THEN merged_amplitude_id END), 0), 2
        ) AS reserve_to_pay_rate,
        
        ROUND(
            COUNT(DISTINCT CASE WHEN has_completed_payment = 1 THEN merged_amplitude_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN total_searches > 0 THEN merged_amplitude_id END), 0), 2
        ) AS overall_conversion_rate
        
    FROM user_journey
),

-- =====================================================
-- 4. SEGMENT ANALYSIS
-- =====================================================

-- Search type performance
search_type_analysis AS (
    SELECT 
        s.search_type,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(AVG(s.count_results), 2) AS avg_results_per_search,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS conversion_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.search_type
    ORDER BY conversion_rate DESC
),

-- Attribution source performance
attribution_analysis AS (
    SELECT 
        s.first_attribution_source,
        s.first_attribution_channel,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS conversion_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.first_attribution_source, s.first_attribution_channel
    HAVING COUNT(DISTINCT s.merged_amplitude_id) >= 10  -- Minimum threshold
    ORDER BY unique_searchers DESC
),

-- Geographic analysis (DMA performance)
dma_analysis AS (
    SELECT 
        s.search_dma,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT r.renter_user_id) AS unique_reservers,
        COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                           THEN r.renter_user_id END) AS unique_payers,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS search_to_view_rate,
        
        ROUND(
            COUNT(DISTINCT r.renter_user_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT l.merged_amplitude_id), 0), 2
        ) AS view_to_reserve_rate,
        
        ROUND(
            COUNT(DISTINCT CASE WHEN r.successful_payment_collected_at IS NOT NULL 
                              THEN r.renter_user_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT r.renter_user_id), 0), 2
        ) AS reserve_to_pay_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_dma = l.click_dma
    LEFT JOIN cleaned_reservations r ON l.listing_id = r.listing_id
    GROUP BY s.search_dma
    HAVING COUNT(DISTINCT s.merged_amplitude_id) >= 50  -- Minimum threshold
    ORDER BY unique_searchers DESC
),

-- =====================================================
-- 5. TEMPORAL ANALYSIS
-- =====================================================

-- Monthly trends
monthly_trends AS (
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
    LEFT JOIN cleaned_reservations r ON EXTRACT(MONTH FROM r.created_at) = s.month
    GROUP BY s.month
    ORDER BY s.month
),

-- Hourly patterns
hourly_patterns AS (
    SELECT 
        s.search_hour,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS conversion_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_hour = l.view_hour
    GROUP BY s.search_hour
    ORDER BY s.search_hour
),

-- =====================================================
-- 6. SEARCH BEHAVIOR ANALYSIS
-- =====================================================

-- Search term category performance
category_analysis AS (
    SELECT 
        s.search_term_category,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        ROUND(AVG(s.count_results), 2) AS avg_results,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS conversion_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.search_term_category
    ORDER BY unique_searchers DESC
),

-- Search result count analysis
result_count_analysis AS (
    SELECT 
        s.result_count_category,
        COUNT(DISTINCT s.merged_amplitude_id) AS unique_searchers,
        COUNT(DISTINCT l.merged_amplitude_id) AS unique_viewers,
        COUNT(DISTINCT s.search_id) AS total_searches,
        
        ROUND(
            COUNT(DISTINCT l.merged_amplitude_id) * 100.0 / 
            NULLIF(COUNT(DISTINCT s.merged_amplitude_id), 0), 2
        ) AS conversion_rate
        
    FROM cleaned_search_events s
    LEFT JOIN cleaned_listing_views l ON s.search_id = l.search_id
    GROUP BY s.result_count_category
    ORDER BY 
        CASE s.result_count_category
            WHEN 'No Results' THEN 1
            WHEN '1-10 Results' THEN 2
            WHEN '11-50 Results' THEN 3
            WHEN '51-100 Results' THEN 4
            WHEN '101-200 Results' THEN 5
            WHEN '200+ Results' THEN 6
        END
),

-- =====================================================
-- 7. PAYMENT ANALYSIS
-- =====================================================

-- Payment completion analysis
payment_analysis AS (
    SELECT 
        COUNT(*) AS total_reservations,
        COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) AS successful_payments,
        COUNT(CASE WHEN approved_at IS NOT NULL AND successful_payment_collected_at IS NULL THEN 1 END) AS pending_payments,
        COUNT(CASE WHEN approved_at IS NULL THEN 1 END) AS rejected_reservations,
        
        ROUND(
            COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) * 100.0 / 
            COUNT(*), 2
        ) AS payment_completion_rate,
        
        ROUND(AVG(hours_to_approval), 2) AS avg_hours_to_approval,
        ROUND(AVG(hours_to_payment), 2) AS avg_hours_to_payment
        
    FROM cleaned_reservations
),

-- Monthly payment trends
monthly_payment_trends AS (
    SELECT 
        reservation_month,
        COUNT(*) AS total_reservations,
        COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) AS successful_payments,
        
        ROUND(
            COUNT(CASE WHEN successful_payment_collected_at IS NOT NULL THEN 1 END) * 100.0 / 
            COUNT(*), 2
        ) AS payment_completion_rate
        
    FROM cleaned_reservations
    GROUP BY reservation_month
    ORDER BY reservation_month
)

-- =====================================================
-- 8. FINAL RESULTS QUERIES
-- =====================================================

-- Main funnel metrics
SELECT 'FUNNEL_METRICS' AS analysis_type, * FROM funnel_metrics

UNION ALL

-- Search type performance
SELECT 'SEARCH_TYPE_ANALYSIS' AS analysis_type, 
       search_type, unique_searchers, unique_viewers, total_searches, 
       avg_results_per_search, conversion_rate
FROM search_type_analysis

UNION ALL

-- Top attribution sources
SELECT 'ATTRIBUTION_ANALYSIS' AS analysis_type,
       first_attribution_source, first_attribution_channel, unique_searchers, 
       unique_viewers, total_searches, conversion_rate
FROM attribution_analysis
LIMIT 10

UNION ALL

-- Top DMAs
SELECT 'DMA_ANALYSIS' AS analysis_type,
       search_dma, unique_searchers, unique_viewers, unique_reservers, 
       unique_payers, search_to_view_rate, view_to_reserve_rate, reserve_to_pay_rate
FROM dma_analysis
LIMIT 10

UNION ALL

-- Monthly trends
SELECT 'MONTHLY_TRENDS' AS analysis_type,
       month, unique_searchers, unique_viewers, unique_reservers, 
       unique_payers, total_searches, avg_search_results
FROM monthly_trends

UNION ALL

-- Payment analysis
SELECT 'PAYMENT_ANALYSIS' AS analysis_type,
       total_reservations, successful_payments, pending_payments, 
       rejected_reservations, payment_completion_rate, 
       avg_hours_to_approval, avg_hours_to_payment
FROM payment_analysis;

