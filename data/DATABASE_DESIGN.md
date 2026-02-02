# rs_analytics Database Design

**Database:** DuckDB (Local Analytics Warehouse)  
**Location:** `data/warehouse.duckdb`  
**Total Tables:** 35 (6 GA4 + 10 GSC + 9 Google Ads + 10 Meta Ads)  
**Total Rows:** 165,000+ (63,480 GA4 + 94,270 GSC + 7,395 Google Ads + 500+ Meta Ads)  
**Data Range:** GA4: 2020-2026 | GSC: 2024-2026 | Google Ads: 2020-2026 | Meta Ads: Up to 37 months

---

## Table of Contents

1. [Overview](#overview)
2. [GA4 Tables](#ga4-tables-google-analytics-4)
3. [GSC Tables](#gsc-tables-google-search-console)
4. [Google Ads Tables](#google-ads-tables)
5. [Meta Ads Tables](#meta-ads-tables)
6. [Data Dictionary](#data-dictionary)
7. [Query Examples](#query-examples)
8. [ETL Process](#etl-process)
9. [Performance Optimization](#performance-optimization)

---

## Overview

The database contains 35 tables organized by data source:

### Summary by Data Source

| Source | Tables | Rows | Primary Use |
|--------|--------|------|-------------|
| GA4 (Google Analytics 4) | 6 | 63,480+ | Website traffic, user behavior, conversions |
| GSC (Google Search Console) | 10 | 94,270+ | Organic search, SEO keywords, rankings |
| Google Ads | 9 | 7,395+ | Google paid advertising, campaigns, keywords |
| Meta Ads | 10 | 500+ | Facebook/Instagram advertising, demographics |

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

---

## Meta Ads Tables

| Table Name | Rows | Purpose | ETL Source |
|------------|------|---------|------------|
| `meta_daily_account` | 41+ | Daily account-level metrics | `run_etl_meta.py` |
| `meta_campaigns` | 15+ | Campaign metadata | `run_etl_meta.py` |
| `meta_campaign_insights` | 140+ | Daily campaign performance | `run_etl_meta.py` |
| `meta_adsets` | 15+ | Ad set (targeting) metadata | `run_etl_meta.py` |
| `meta_adset_insights` | 140+ | Daily ad set performance | `run_etl_meta.py` |
| `meta_ads` | 159+ | Individual ad metadata | `run_etl_meta.py` |
| `meta_ad_insights` | varies | Daily ad-level performance | `run_etl_meta.py` |
| `meta_geographic` | 3+ | Country-level breakdown | `run_etl_meta.py` |
| `meta_devices` | 6+ | Device/platform breakdown | `run_etl_meta.py` |
| `meta_demographics` | 18+ | Age/gender breakdown | `run_etl_meta.py` |

### Common Meta Ads Metrics

All Meta Ads insight tables share these core metrics:

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Performance date |
| `ad_account_id` | VARCHAR | Ad account ID (act_XXXX) |
| `impressions` | BIGINT | Ad impressions |
| `reach` | BIGINT | Unique people reached |
| `clicks` | BIGINT | Ad clicks |
| `spend` | DOUBLE | Amount spent (account currency) |
| `ctr` | DOUBLE | Click-through rate (%) |
| `cpc` | DOUBLE | Cost per click |
| `cpm` | DOUBLE | Cost per 1000 impressions |
| `frequency` | DOUBLE | Avg times each person saw the ad |
| `app_installs` | BIGINT | Mobile app installs |
| `purchases` | BIGINT | Purchase conversions |
| `purchase_value` | DOUBLE | Revenue from purchases |

### meta_daily_account

**Purpose:** Daily account-level performance totals.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `impressions` | BIGINT | Total impressions |
| `reach` | BIGINT | Unique reach |
| `clicks` | BIGINT | Total clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Total spend |
| `ctr` | DOUBLE | Click-through rate |
| `cpc` | DOUBLE | Cost per click |
| `cpm` | DOUBLE | Cost per mille |
| `frequency` | DOUBLE | Average frequency |
| `cost_per_unique_click` | DOUBLE | Cost per unique click |
| `link_clicks` | BIGINT | Link clicks |
| `page_engagement` | BIGINT | Page engagement |
| `post_engagement` | BIGINT | Post engagement |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `video_p25` | BIGINT | Video 25% watched |
| `video_p50` | BIGINT | Video 50% watched |
| `video_p75` | BIGINT | Video 75% watched |
| `video_p100` | BIGINT | Video 100% watched |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_campaigns

**Purpose:** Campaign metadata and configuration.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | VARCHAR | Campaign ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_name` | VARCHAR | Campaign name |
| `status` | VARCHAR | Status (ACTIVE, PAUSED) |
| `effective_status` | VARCHAR | Effective status |
| `objective` | VARCHAR | Campaign objective |
| `buying_type` | VARCHAR | Buying type |
| `daily_budget` | DOUBLE | Daily budget |
| `lifetime_budget` | DOUBLE | Lifetime budget |
| `budget_remaining` | DOUBLE | Remaining budget |
| `created_time` | TIMESTAMP | Creation time |
| `start_time` | TIMESTAMP | Start time |
| `stop_time` | TIMESTAMP | Stop time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_campaign_insights

**Purpose:** Daily campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID (PK) |
| `campaign_name` | VARCHAR | Campaign name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `frequency` | DOUBLE | Frequency |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_adsets

**Purpose:** Ad set metadata with targeting information.

| Column | Type | Description |
|--------|------|-------------|
| `adset_id` | VARCHAR | Ad set ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Parent campaign ID |
| `adset_name` | VARCHAR | Ad set name |
| `status` | VARCHAR | Status |
| `effective_status` | VARCHAR | Effective status |
| `optimization_goal` | VARCHAR | Optimization goal |
| `billing_event` | VARCHAR | Billing event |
| `bid_strategy` | VARCHAR | Bid strategy |
| `daily_budget` | DOUBLE | Daily budget |
| `lifetime_budget` | DOUBLE | Lifetime budget |
| `budget_remaining` | DOUBLE | Remaining budget |
| `target_countries` | VARCHAR | Target countries (comma-separated) |
| `created_time` | TIMESTAMP | Creation time |
| `start_time` | TIMESTAMP | Start time |
| `end_time` | TIMESTAMP | End time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_adset_insights

**Purpose:** Daily ad set-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `adset_id` | VARCHAR | Ad set ID (PK) |
| `adset_name` | VARCHAR | Ad set name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `unique_clicks` | BIGINT | Unique clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `frequency` | DOUBLE | Frequency |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `leads` | BIGINT | Leads |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_geographic

**Purpose:** Performance breakdown by country.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start (PK) |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID (PK) |
| `country` | VARCHAR | Country code (PK) |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_devices

**Purpose:** Performance breakdown by device and publisher platform.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start (PK) |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID (PK) |
| `device_platform` | VARCHAR | Device (mobile, desktop) (PK) |
| `publisher_platform` | VARCHAR | Platform (facebook, instagram, etc.) (PK) |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `extracted_at` | TIMESTAMP | ETL timestamp |

### meta_demographics

**Purpose:** Performance breakdown by age and gender.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start (PK) |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID (PK) |
| `age` | VARCHAR | Age bracket (18-24, 25-34, etc.) (PK) |
| `gender` | VARCHAR | Gender (male, female, unknown) (PK) |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

## Data Dictionary

### Cost Conversion

**Google Ads** costs are stored in **micros** (1/1,000,000 of the currency unit):

```sql
-- Convert cost_micros to actual cost
SELECT 
    campaign_name,
    cost_micros / 1000000.0 as cost_dollars,
    clicks,
    (cost_micros / 1000000.0) / NULLIF(clicks, 0) as actual_cpc
FROM gads_campaigns;
```

**Meta Ads** costs are stored in actual currency values (no conversion needed):

```sql
-- Meta costs are already in currency units
SELECT 
    campaign_name,
    spend as cost,
    clicks,
    spend / NULLIF(clicks, 0) as actual_cpc
FROM meta_campaign_insights;
```

### Common Dimension Values

| Dimension | Platform | Example Values |
|-----------|----------|----------------|
| `device` / `deviceCategory` | Google/GA4 | MOBILE, DESKTOP, TABLET |
| `device_platform` | Meta | mobile, desktop |
| `publisher_platform` | Meta | facebook, instagram, audience_network, messenger |
| `campaign_status` / `status` | Both | ENABLED/ACTIVE, PAUSED, REMOVED |
| `age` | Meta | 13-17, 18-24, 25-34, 35-44, 45-54, 55-64, 65+ |
| `gender` | Meta | male, female, unknown |

---

## Query Examples

### Cross-Platform Analysis

**Total Marketing Spend Comparison (All Platforms):**
```sql
-- Google Ads spend
SELECT 
    'Google Ads' as platform,
    SUM(cost_micros) / 1000000.0 as total_spend,
    SUM(clicks) as total_clicks,
    SUM(conversions) as conversions
FROM gads_daily_summary

UNION ALL

-- Meta Ads spend
SELECT 
    'Meta Ads' as platform,
    SUM(spend) as total_spend,
    SUM(clicks) as total_clicks,
    SUM(app_installs) as conversions
FROM meta_daily_account

UNION ALL

-- Organic (SEO) - no cost
SELECT 
    'Organic (SEO)' as platform,
    0 as total_spend,
    SUM(clicks) as total_clicks,
    NULL as conversions
FROM gsc_daily_totals;
```

### Meta Ads ROI Analysis

**Campaign Performance with CPI:**
```sql
SELECT 
    campaign_name,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(spend) as spend,
    SUM(app_installs) as installs,
    CASE WHEN SUM(app_installs) > 0 
         THEN SUM(spend) / SUM(app_installs) 
         ELSE 0 
    END as cost_per_install,
    CASE WHEN SUM(impressions) > 0 
         THEN SUM(clicks) * 100.0 / SUM(impressions) 
         ELSE 0 
    END as ctr_percent
FROM meta_campaign_insights
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY campaign_name
ORDER BY spend DESC;
```

**Geographic Performance:**
```sql
SELECT 
    country,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(spend) as spend,
    SUM(app_installs) as installs,
    CASE WHEN SUM(app_installs) > 0 
         THEN SUM(spend) / SUM(app_installs) 
         ELSE 0 
    END as cpi
FROM meta_geographic
GROUP BY country
ORDER BY spend DESC;
```

**Demographics Matrix:**
```sql
SELECT 
    age,
    gender,
    SUM(spend) as spend,
    SUM(clicks) as clicks,
    SUM(app_installs) as installs
FROM meta_demographics
GROUP BY age, gender
ORDER BY age, gender;
```

**Platform Efficiency Comparison:**
```sql
SELECT 
    device_platform,
    publisher_platform,
    SUM(spend) as spend,
    SUM(clicks) as clicks,
    CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
    SUM(app_installs) as installs
FROM meta_devices
GROUP BY device_platform, publisher_platform
ORDER BY spend DESC;
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

---

## ETL Process

### ETL Commands Summary

| Data Source | Script | Purpose |
|-------------|--------|---------|
| GA4 (basic) | `python scripts/run_etl.py` | Quick daily update |
| GA4 (full) | `python scripts/run_etl_comprehensive.py --lifetime` | All metrics |
| Search Console | `python scripts/run_etl_gsc.py --lifetime` | SEO data |
| Google Ads | `python scripts/run_etl_gads.py --lifetime` | Google PPC data |
| Meta Ads | `python scripts/run_etl_meta.py --lifetime` | Meta/Facebook PPC data |

### Data Freshness

| Source | Delay | Recommended Update | Max History |
|--------|-------|-------------------|-------------|
| GA4 | 24-48 hours | Daily at 6 AM | Unlimited |
| Search Console | 2-3 days | Daily at 6:30 AM | 16 months |
| Google Ads | Same day | Daily at 7 AM | Unlimited |
| Meta Ads | Same day | Daily at 7:30 AM | 37 months |

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

-- Meta Ads indexes
CREATE INDEX idx_meta_daily_date ON meta_daily_account(date);
CREATE INDEX idx_meta_campaign_insights_date ON meta_campaign_insights(date);
CREATE INDEX idx_meta_adset_insights_date ON meta_adset_insights(date);
```

### Storage

Current database size: ~75-200 MB

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

**Last Updated:** 2026-02-02  
**Database Version:** 3.0 (Added Meta Ads)  
**DuckDB Version:** 0.9.0+
