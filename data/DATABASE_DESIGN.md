# rs_analytics Database Design

**Database:** DuckDB (Local Analytics Warehouse)  
**Location:** `data/warehouse.duckdb`  
**Total Tables:** 25 (6 GA4 + 10 GSC + 9 Google Ads)  
**Total Rows:** 165,145+ (63,480 GA4 + 94,270 GSC + 7,395 Google Ads)  
**Data Range:** GA4: 2020-2026 | GSC: 2024-09-16 to 2026-01-29 | Google Ads: 2020-2026

---

## Table of Contents

1. [Overview](#overview)
2. [GA4 Tables](#ga4-tables-google-analytics-4)
3. [GSC Tables](#gsc-tables-google-search-console)
4. [Google Ads Tables](#google-ads-tables)
5. [Data Dictionary](#data-dictionary)
6. [Query Examples](#query-examples)
7. [ETL Process](#etl-process)
8. [Performance Optimization](#performance-optimization)

---

## Overview

The database contains 25 tables organized by data source:

### Summary by Data Source

| Source | Tables | Rows | Primary Use |
|--------|--------|------|-------------|
| GA4 (Google Analytics 4) | 6 | 63,480+ | Website traffic, user behavior, conversions |
| GSC (Google Search Console) | 10 | 94,270+ | Organic search, SEO keywords, rankings |
| Google Ads | 9 | 7,395+ | Paid advertising, campaigns, keywords |

---

## GA4 Tables (Google Analytics 4)

| Table Name | Rows | Purpose | ETL Source |
|------------|------|---------|------------|
| `ga4_sessions` | 2,220 | Basic session metrics (legacy) | `run_etl.py` |
| `ga4_traffic_overview` | 12,438 | Traffic source analysis | `run_etl_comprehensive.py` |
| `ga4_page_performance` | 12,530 | Content & page performance | `run_etl_comprehensive.py` |
| `ga4_geographic_data` | 14,921 | Geographic breakdown | `run_etl_comprehensive.py` |
| `ga4_technology_data` | 16,081 | Device & browser data | `run_etl_comprehensive.py` |
| `ga4_event_data` | 5,290 | Custom event tracking | `run_etl_comprehensive.py` |

### ga4_sessions (Legacy)

**Purpose:** Basic session-level metrics for quick daily monitoring.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Session date |
| `session_source` | VARCHAR | Traffic source |
| `session_medium` | VARCHAR | Traffic medium |
| `device_category` | VARCHAR | Device type |
| `country` | VARCHAR | Country code |
| `sessions` | INTEGER | Session count |
| `active_users` | INTEGER | Active users |
| `new_users` | INTEGER | New users |
| `screen_page_views` | INTEGER | Page views |
| `avg_session_duration` | DOUBLE | Avg duration (seconds) |
| `bounce_rate` | DOUBLE | Bounce rate (0-1) |
| `engagement_rate` | DOUBLE | Engagement rate (0-1) |

### ga4_traffic_overview

**Purpose:** Detailed traffic analysis with campaign tracking and revenue.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD) |
| `sessionSource` | VARCHAR | Traffic source |
| `sessionMedium` | VARCHAR | Traffic medium |
| `sessionCampaignName` | VARCHAR | Campaign name |
| `sessionDefaultChannelGroup` | VARCHAR | Channel grouping |
| `deviceCategory` | VARCHAR | Device type |
| `country` | VARCHAR | Country |
| `activeUsers` | VARCHAR | Active users |
| `newUsers` | VARCHAR | New users |
| `sessions` | VARCHAR | Sessions |
| `engagedSessions` | VARCHAR | Engaged sessions |
| `engagementRate` | VARCHAR | Engagement rate |
| `screenPageViews` | VARCHAR | Page views |
| `averageSessionDuration` | VARCHAR | Avg duration |
| `bounceRate` | VARCHAR | Bounce rate |
| `conversions` | VARCHAR | Conversions |
| `totalRevenue` | VARCHAR | Total revenue |

### ga4_page_performance

**Purpose:** Page-level content analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD) |
| `pageTitle` | VARCHAR | Page title |
| `pagePath` | VARCHAR | URL path |
| `landingPage` | VARCHAR | Landing page |
| `deviceCategory` | VARCHAR | Device type |
| `screenPageViews` | VARCHAR | Page views |
| `activeUsers` | VARCHAR | Users |
| `sessions` | VARCHAR | Sessions |
| `engagementRate` | VARCHAR | Engagement rate |
| `bounceRate` | VARCHAR | Bounce rate |
| `conversions` | VARCHAR | Conversions |

### ga4_geographic_data

**Purpose:** Geographic breakdown by country, region, city.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD) |
| `country` | VARCHAR | Country name |
| `region` | VARCHAR | State/province |
| `city` | VARCHAR | City name |
| `deviceCategory` | VARCHAR | Device type |
| `activeUsers` | VARCHAR | Active users |
| `sessions` | VARCHAR | Sessions |
| `conversions` | VARCHAR | Conversions |
| `totalRevenue` | VARCHAR | Revenue |

### ga4_technology_data

**Purpose:** Device, browser, and OS analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD) |
| `deviceCategory` | VARCHAR | Device type |
| `operatingSystem` | VARCHAR | OS name |
| `browser` | VARCHAR | Browser name |
| `screenResolution` | VARCHAR | Screen resolution |
| `activeUsers` | VARCHAR | Users |
| `sessions` | VARCHAR | Sessions |
| `engagementRate` | VARCHAR | Engagement rate |
| `bounceRate` | VARCHAR | Bounce rate |

### ga4_event_data

**Purpose:** Custom event tracking and user interactions.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD) |
| `eventName` | VARCHAR | Event name |
| `deviceCategory` | VARCHAR | Device type |
| `eventCount` | VARCHAR | Event count |
| `activeUsers` | VARCHAR | Users |
| `sessions` | VARCHAR | Sessions |

---

## GSC Tables (Google Search Console)

| Table Name | Rows | Purpose | ETL Source |
|------------|------|---------|------------|
| `gsc_queries` | 44,020 | Search keywords over time | `run_etl_gsc.py` |
| `gsc_pages` | 8,694 | Page-level SEO performance | `run_etl_gsc.py` |
| `gsc_countries` | 15,107 | Geographic search performance | `run_etl_gsc.py` |
| `gsc_devices` | 659 | Device-specific SEO metrics | `run_etl_gsc.py` |
| `gsc_query_page` | 5,507 | Query to page mapping | `run_etl_gsc.py` |
| `gsc_query_country` | 13,065 | Query by country | `run_etl_gsc.py` |
| `gsc_query_device` | 3,300 | Query by device | `run_etl_gsc.py` |
| `gsc_page_country` | 3,298 | Page by country | `run_etl_gsc.py` |
| `gsc_page_device` | 366 | Page by device | `run_etl_gsc.py` |
| `gsc_daily_totals` | 254 | Daily aggregate totals | `run_etl_gsc.py` |

### Common GSC Columns

All GSC tables share these core metrics:

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Query date |
| `clicks` | INTEGER | Total clicks from search |
| `impressions` | INTEGER | Times shown in search |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average search position |

### gsc_queries

**Purpose:** Keyword/search query performance.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `query` | VARCHAR | Search query/keyword |
| `clicks` | INTEGER | Clicks |
| `impressions` | INTEGER | Impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Avg position |

### gsc_pages

**Purpose:** Page-level organic search performance.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `page` | VARCHAR | Page URL |
| `clicks` | INTEGER | Clicks |
| `impressions` | INTEGER | Impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Avg position |

### gsc_countries

**Purpose:** Search performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `country` | VARCHAR | Country code (3-letter) |
| `clicks` | INTEGER | Clicks |
| `impressions` | INTEGER | Impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Avg position |

---

## Google Ads Tables

| Table Name | Rows | Purpose | ETL Source |
|------------|------|---------|------------|
| `gads_daily_summary` | 225 | Daily account-level totals | `run_etl_gads.py` |
| `gads_campaigns` | 225 | Campaign performance | `run_etl_gads.py` |
| `gads_ad_groups` | 722 | Ad group performance | `run_etl_gads.py` |
| `gads_keywords` | 746 | Keyword performance | `run_etl_gads.py` |
| `gads_ads` | 722 | Individual ad metrics | `run_etl_gads.py` |
| `gads_devices` | 543 | Performance by device | `run_etl_gads.py` |
| `gads_geographic` | 225 | Geographic performance | `run_etl_gads.py` |
| `gads_hourly` | 3,707 | Hour-by-hour performance | `run_etl_gads.py` |
| `gads_conversions` | 280 | Conversion action data | `run_etl_gads.py` |

### Common Google Ads Metrics

All Google Ads tables share these core metrics:

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `impressions` | BIGINT | Ad impressions |
| `clicks` | BIGINT | Ad clicks |
| `cost_micros` | BIGINT | Cost in micros (÷1,000,000 for actual cost) |
| `ctr` | DOUBLE | Click-through rate |
| `average_cpc` | DOUBLE | Average cost per click (micros) |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions (inc. cross-device) |

### gads_daily_summary

**Purpose:** Daily account-level performance totals.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Total impressions |
| `clicks` | BIGINT | Total clicks |
| `cost_micros` | BIGINT | Total cost (micros) |
| `conversions` | DOUBLE | Total conversions |
| `conversions_value` | DOUBLE | Total conversion value |
| `all_conversions` | DOUBLE | All conversions |

### gads_campaigns

**Purpose:** Campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | VARCHAR | Status (ENABLED, PAUSED, etc.) |
| `advertising_channel_type` | VARCHAR | Channel type (SEARCH, DISPLAY, etc.) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `ctr` | DOUBLE | Click-through rate |
| `average_cpc` | DOUBLE | Avg CPC (micros) |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `search_impression_share` | DOUBLE | Search impression share |

### gads_ad_groups

**Purpose:** Ad group-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `campaign_id` | BIGINT | Parent campaign ID |
| `campaign_name` | VARCHAR | Parent campaign name |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | VARCHAR | Status |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |

### gads_keywords

**Purpose:** Keyword-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `campaign_id` | BIGINT | Campaign ID |
| `ad_group_id` | BIGINT | Ad group ID |
| `keyword_id` | BIGINT | Keyword criterion ID |
| `keyword_text` | VARCHAR | Keyword text |
| `keyword_match_type` | VARCHAR | Match type (BROAD, PHRASE, EXACT) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |
| `quality_score` | INTEGER | Quality score (1-10) |

### gads_ads

**Purpose:** Individual ad creative performance.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `campaign_id` | BIGINT | Campaign ID |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_id` | BIGINT | Ad ID |
| `ad_type` | VARCHAR | Ad type |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |

### gads_devices

**Purpose:** Performance segmented by device type.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `device` | VARCHAR | Device type (MOBILE, DESKTOP, TABLET) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |

### gads_geographic

**Purpose:** Geographic performance by country/region.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `country_criterion_id` | BIGINT | Country ID |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |

### gads_hourly

**Purpose:** Hour-by-hour performance for optimization.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `hour` | INTEGER | Hour of day (0-23) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `conversions` | DOUBLE | Conversions |

### gads_conversions

**Purpose:** Conversion action-level data.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date |
| `conversion_action` | VARCHAR | Conversion action resource name |
| `conversion_action_name` | VARCHAR | Conversion action name |
| `conversion_action_category` | VARCHAR | Category |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions |

---

## Data Dictionary

### Cost Conversion

Google Ads costs are stored in **micros** (1/1,000,000 of the currency unit):

```sql
-- Convert cost_micros to actual cost
SELECT 
    campaign_name,
    cost_micros / 1000000.0 as cost_dollars,
    clicks,
    (cost_micros / 1000000.0) / NULLIF(clicks, 0) as actual_cpc
FROM gads_campaigns;
```

### Common Dimension Values

| Dimension | Example Values |
|-----------|----------------|
| `device` / `deviceCategory` | MOBILE, DESKTOP, TABLET |
| `campaign_status` | ENABLED, PAUSED, REMOVED |
| `keyword_match_type` | BROAD, PHRASE, EXACT |
| `advertising_channel_type` | SEARCH, DISPLAY, VIDEO, SHOPPING |

---

## Query Examples

### Cross-Platform Analysis

**Total Marketing Spend vs Organic Traffic:**
```sql
-- Paid traffic (Google Ads)
SELECT 
    'Paid (Google Ads)' as channel,
    SUM(clicks) as total_clicks,
    SUM(cost_micros) / 1000000.0 as total_cost,
    SUM(conversions) as conversions
FROM gads_daily_summary

UNION ALL

-- Organic traffic (Search Console)
SELECT 
    'Organic (SEO)' as channel,
    SUM(clicks) as total_clicks,
    0 as total_cost,  -- Organic is "free"
    NULL as conversions
FROM gsc_daily_totals;
```

### Google Ads ROI Analysis

**Campaign ROI:**
```sql
SELECT 
    campaign_name,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(cost_micros) / 1000000.0 as spend,
    SUM(conversions) as conversions,
    SUM(conversions_value) as revenue,
    CASE 
        WHEN SUM(cost_micros) > 0 
        THEN (SUM(conversions_value) - SUM(cost_micros) / 1000000.0) / (SUM(cost_micros) / 1000000.0) * 100
        ELSE 0 
    END as roi_percent
FROM gads_campaigns
WHERE campaign_status = 'ENABLED'
GROUP BY campaign_name
ORDER BY spend DESC;
```

**Best Performing Keywords:**
```sql
SELECT 
    keyword_text,
    keyword_match_type,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(cost_micros) / 1000000.0 as spend,
    SUM(conversions) as conversions,
    CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) * 100 ELSE 0 END as conv_rate
FROM gads_keywords
WHERE clicks > 0
GROUP BY keyword_text, keyword_match_type
ORDER BY conversions DESC
LIMIT 20;
```

**Hourly Performance Heatmap:**
```sql
SELECT 
    hour,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(conversions) as conversions,
    AVG(CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END) as avg_ctr
FROM gads_hourly
GROUP BY hour
ORDER BY hour;
```

### SEO vs SEM Keyword Comparison

```sql
-- Keywords appearing in both organic and paid
SELECT 
    g.keyword_text,
    g.clicks as paid_clicks,
    g.cost_micros / 1000000.0 as paid_cost,
    s.clicks as organic_clicks,
    s.position as organic_position
FROM (
    SELECT keyword_text, SUM(clicks) as clicks, SUM(cost_micros) as cost_micros
    FROM gads_keywords
    GROUP BY keyword_text
) g
JOIN (
    SELECT query, SUM(clicks) as clicks, AVG(position) as position
    FROM gsc_queries
    GROUP BY query
) s ON LOWER(g.keyword_text) = LOWER(s.query)
ORDER BY g.clicks DESC
LIMIT 20;
```

---

## ETL Process

### ETL Commands Summary

| Data Source | Script | Purpose |
|-------------|--------|---------|
| GA4 (basic) | `python scripts/run_etl.py` | Quick daily update |
| GA4 (full) | `python scripts/run_etl_comprehensive.py --lifetime` | All metrics |
| Search Console | `python scripts/run_etl_gsc.py --lifetime` | SEO data |
| Google Ads | `python scripts/run_etl_gads.py --lifetime` | PPC data |

### Data Freshness

| Source | Delay | Recommended Update |
|--------|-------|-------------------|
| GA4 | 24-48 hours | Daily at 6 AM |
| Search Console | 2-3 days | Daily at 6:30 AM |
| Google Ads | Same day | Daily at 7 AM |

---

## Performance Optimization

### Recommended Indexes

```sql
-- Google Ads indexes
CREATE INDEX idx_gads_campaigns_date ON gads_campaigns(date);
CREATE INDEX idx_gads_keywords_text ON gads_keywords(keyword_text);
CREATE INDEX idx_gads_hourly_date ON gads_hourly(date, hour);

-- GSC indexes
CREATE INDEX idx_gsc_queries_query ON gsc_queries(query);
CREATE INDEX idx_gsc_pages_page ON gsc_pages(page);

-- GA4 indexes
CREATE INDEX idx_ga4_traffic_date ON ga4_traffic_overview(date);
```

### Storage

Current database size: ~75-150 MB

To optimize:
```sql
VACUUM;
ANALYZE;
```

---

## Maintenance

### Daily
- Run ETL scripts for fresh data
- Monitor `logs/` for errors

### Weekly
- Review query performance
- Check database size

### Monthly
- Archive old data if needed
- Update documentation

### Backup

```bash
# Backup
cp data/warehouse.duckdb data/backups/warehouse_$(date +%Y%m%d).duckdb

# Restore
cp data/backups/warehouse_20260130.duckdb data/warehouse.duckdb
```

---

**Last Updated:** 2026-01-30  
**Database Version:** 2.0  
**DuckDB Version:** 0.9.0+
