# rs_analytics Database Design

**Database:** DuckDB (Local Analytics Warehouse)  
**Location:** `data/warehouse.duckdb`  
**Total Tables:** 35 (6 GA4 + 10 GSC + 9 Google Ads + 10 Meta Ads)  
**Total Rows:** ~132,000+ rows  
**Last Updated:** 2026-02-03

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

---

## Overview

### Summary by Data Source

| Source | Tables | Rows | Primary Use |
|--------|--------|------|-------------|
| GA4 (Google Analytics 4) | 6 | 26,668 | Website traffic, user behavior, events |
| GSC (Google Search Console) | 10 | 96,873 | Organic search, SEO keywords, rankings |
| Google Ads | 9 | 9,010 | Google paid advertising, campaigns, keywords |
| Meta Ads | 10 | 537 | Facebook/Instagram advertising, demographics |
| **Total** | **35** | **~133,088** | |

---

## GA4 Tables (Google Analytics 4)

| Table Name | Rows | Purpose |
|------------|------|---------|
| `ga4_sessions` | 278 | Basic daily session metrics |
| `ga4_traffic_overview` | 4,153 | Traffic by source/medium/campaign |
| `ga4_page_performance` | 6,138 | Page-level content performance |
| `ga4_geographic_data` | 10,000 | Geographic breakdown (country/region/city) |
| `ga4_technology_data` | 3,741 | Device, OS, browser breakdown |
| `ga4_event_data` | 2,358 | Custom event tracking |

---

### ga4_sessions

**Purpose:** Basic daily session totals for quick monitoring.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `sessions` | VARCHAR | Total sessions |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |
| `bounceRate` | VARCHAR | Bounce rate (0-1) |

---

### ga4_traffic_overview

**Purpose:** Traffic analysis by source, medium, and campaign.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `sessionSource` | VARCHAR | Traffic source (google, facebook, direct, etc.) |
| `sessionMedium` | VARCHAR | Traffic medium (organic, cpc, referral, etc.) |
| `sessionCampaignName` | VARCHAR | Campaign name |
| `sessions` | VARCHAR | Session count |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |
| `bounceRate` | VARCHAR | Bounce rate |
| `screenPageViews` | VARCHAR | Page views |

---

### ga4_page_performance

**Purpose:** Page-level content analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `pagePath` | VARCHAR | URL path (/page/subpage) |
| `pageTitle` | VARCHAR | Page title |
| `screenPageViews` | VARCHAR | Page views |
| `sessions` | VARCHAR | Sessions |
| `bounceRate` | VARCHAR | Bounce rate |
| `averageSessionDuration` | VARCHAR | Avg session duration (seconds) |

---

### ga4_geographic_data

**Purpose:** Geographic breakdown by country, region, and city.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `country` | VARCHAR | Country name |
| `region` | VARCHAR | State/province/region |
| `city` | VARCHAR | City name |
| `sessions` | VARCHAR | Sessions |
| `totalUsers` | VARCHAR | Total users |
| `newUsers` | VARCHAR | New users |

---

### ga4_technology_data

**Purpose:** Device, browser, and OS analysis.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `deviceCategory` | VARCHAR | Device type (desktop, mobile, tablet) |
| `operatingSystem` | VARCHAR | OS name (Windows, iOS, Android, etc.) |
| `browser` | VARCHAR | Browser name (Chrome, Safari, etc.) |
| `sessions` | VARCHAR | Sessions |
| `totalUsers` | VARCHAR | Total users |
| `screenPageViews` | VARCHAR | Page views |

---

### ga4_event_data

**Purpose:** Custom event tracking and user interactions.

| Column | Type | Description |
|--------|------|-------------|
| `date` | VARCHAR | Date (YYYYMMDD format) |
| `eventName` | VARCHAR | Event name (page_view, click, scroll, etc.) |
| `eventCount` | VARCHAR | Number of event occurrences |
| `totalUsers` | VARCHAR | Users who triggered the event |

---

## GSC Tables (Google Search Console)

| Table Name | Rows | Purpose |
|------------|------|---------|
| `gsc_queries` | 45,218 | Search keywords over time |
| `gsc_pages` | 8,992 | Page-level SEO performance |
| `gsc_countries` | 15,513 | Geographic search performance |
| `gsc_devices` | 671 | Device-specific SEO metrics |
| `gsc_query_page` | 5,680 | Query to page mapping |
| `gsc_query_country` | 13,353 | Query by country |
| `gsc_query_device` | 3,419 | Query by device |
| `gsc_page_country` | 3,397 | Page by country |
| `gsc_page_device` | 372 | Page by device |
| `gsc_daily_totals` | 258 | Daily aggregate totals |

---

### gsc_queries

**Purpose:** Search query/keyword performance over time.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `query` | VARCHAR | Search query/keyword |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate (0-1) |
| `position` | DOUBLE | Average search position |

---

### gsc_pages

**Purpose:** Page-level organic search performance.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `page` | VARCHAR | Page URL |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_countries

**Purpose:** Search performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `country` | VARCHAR | Country code (3-letter: usa, sgp, etc.) |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_devices

**Purpose:** Search performance by device type.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `device` | VARCHAR | Device type (DESKTOP, MOBILE, TABLET) |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_page

**Purpose:** Query to page mapping (which queries drive which pages).

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `page` | VARCHAR | Page URL |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_country

**Purpose:** Query performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `country` | VARCHAR | Country code |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_query_device

**Purpose:** Query performance by device.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `query` | VARCHAR | Search query |
| `device` | VARCHAR | Device type |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_page_country

**Purpose:** Page performance by country.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `page` | VARCHAR | Page URL |
| `country` | VARCHAR | Country code |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_page_device

**Purpose:** Page performance by device.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `page` | VARCHAR | Page URL |
| `device` | VARCHAR | Device type |
| `clicks` | BIGINT | Total clicks |
| `impressions` | BIGINT | Total impressions |
| `ctr` | DOUBLE | Click-through rate |
| `position` | DOUBLE | Average position |

---

### gsc_daily_totals

**Purpose:** Daily aggregate totals across all queries/pages.

| Column | Type | Description |
|--------|------|-------------|
| `_dataset` | VARCHAR | Dataset identifier |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `clicks` | BIGINT | Total daily clicks |
| `impressions` | BIGINT | Total daily impressions |
| `ctr` | DOUBLE | Daily click-through rate |
| `position` | DOUBLE | Daily average position |

---

## Google Ads Tables

| Table Name | Rows | Purpose |
|------------|------|---------|
| `gads_daily_summary` | 273 | Daily account-level totals |
| `gads_campaigns` | 273 | Campaign performance |
| `gads_ad_groups` | 857 | Ad group performance |
| `gads_keywords` | 930 | Keyword performance |
| `gads_ads` | 857 | Individual ad metrics |
| `gads_devices` | 655 | Performance by device |
| `gads_geographic` | 273 | Geographic performance |
| `gads_hourly` | 4,543 | Hour-by-hour performance |
| `gads_conversions` | 349 | Conversion action data |

---

### gads_campaigns

**Purpose:** Campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID (PK) |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | VARCHAR | Status (ENABLED, PAUSED, REMOVED) |
| `campaign_type` | VARCHAR | Type (SEARCH, DISPLAY, VIDEO, etc.) |
| `date` | VARCHAR | Date (YYYY-MM-DD) |
| `impressions` | BIGINT | Ad impressions |
| `clicks` | BIGINT | Ad clicks |
| `cost_micros` | BIGINT | Cost in micros (÷1,000,000 for actual) |
| `cost` | DOUBLE | Cost in currency units |
| `ctr` | DOUBLE | Click-through rate |
| `average_cpc_micros` | DOUBLE | Avg CPC in micros |
| `average_cpc` | DOUBLE | Avg CPC in currency |
| `average_cpm_micros` | DOUBLE | Avg CPM in micros |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions (inc. cross-device) |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Total interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% watched rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% watched rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% watched rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% watched rate |
| `search_impression_share` | DOUBLE | Search impression share |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Average position (deprecated) |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top impression % |

---

### gads_ad_groups

**Purpose:** Ad group-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Parent campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status code |
| `campaign_type` | INTEGER | Campaign type code |
| `ad_group_id` | BIGINT | Ad group ID (PK) |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | VARCHAR | Status |
| `ad_group_type` | VARCHAR | Type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| `average_cpc_micros` | DOUBLE | Avg CPC (micros) |
| `average_cpc` | DOUBLE | Avg CPC |
| `average_cpm_micros` | DOUBLE | Avg CPM (micros) |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% rate |
| `search_impression_share` | DOUBLE | Search IS |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Avg position |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top % |

---

### gads_keywords

**Purpose:** Keyword-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | INTEGER | Ad group status |
| `ad_group_type` | INTEGER | Ad group type |
| `keyword_id` | BIGINT | Keyword criterion ID (PK) |
| `keyword_text` | VARCHAR | Keyword text |
| `keyword_match_type` | VARCHAR | Match type (BROAD, PHRASE, EXACT) |
| `keyword_status` | VARCHAR | Keyword status |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| `average_cpc_micros` | DOUBLE | Avg CPC (micros) |
| `average_cpc` | DOUBLE | Avg CPC |
| `average_cpm_micros` | DOUBLE | Avg CPM (micros) |
| `average_cpm` | BIGINT | Avg CPM |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| `all_conversions` | DOUBLE | All conversions |
| `all_conversions_value` | DOUBLE | All conversions value |
| `conversion_rate` | DOUBLE | Conversion rate |
| `view_through_conversions` | BIGINT | View-through conversions |
| `interactions` | BIGINT | Interactions |
| `interaction_rate` | DOUBLE | Interaction rate |
| `engagement_rate` | DOUBLE | Engagement rate |
| `video_views` | BIGINT | Video views |
| `video_quartile_p25_rate` | DOUBLE | Video 25% rate |
| `video_quartile_p50_rate` | DOUBLE | Video 50% rate |
| `video_quartile_p75_rate` | DOUBLE | Video 75% rate |
| `video_quartile_p100_rate` | DOUBLE | Video 100% rate |
| `search_impression_share` | DOUBLE | Search IS |
| `search_rank_lost_impression_share` | DOUBLE | Lost IS (rank) |
| `search_budget_lost_impression_share` | DOUBLE | Lost IS (budget) |
| `average_position` | INTEGER | Avg position |
| `top_impression_percentage` | DOUBLE | Top impression % |
| `absolute_top_impression_percentage` | DOUBLE | Absolute top % |

---

### gads_ads

**Purpose:** Individual ad performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `ad_group_id` | BIGINT | Ad group ID |
| `ad_group_name` | VARCHAR | Ad group name |
| `ad_group_status` | INTEGER | Ad group status |
| `ad_group_type` | INTEGER | Ad group type |
| `ad_id` | BIGINT | Ad ID (PK) |
| `ad_status` | VARCHAR | Ad status |
| `ad_type` | VARCHAR | Ad type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| `ctr` | DOUBLE | CTR |
| *(+ all standard metrics)* | | |

---

### gads_devices

**Purpose:** Performance breakdown by device type.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `device` | VARCHAR | Device (DESKTOP, MOBILE, TABLET) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_geographic

**Purpose:** Performance breakdown by geographic location.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `country_criterion_id` | BIGINT | Country criterion ID |
| `location_type` | VARCHAR | Location type |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_hourly

**Purpose:** Hour-by-hour performance breakdown.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `hour` | DOUBLE | Hour of day (0-23) |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `cost_micros` | BIGINT | Cost (micros) |
| `cost` | DOUBLE | Cost |
| *(+ all standard metrics)* | | |

---

### gads_conversions

**Purpose:** Conversion action performance.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `conversion_action` | VARCHAR | Conversion action resource |
| `conversion_action_name` | VARCHAR | Conversion action name |
| `impressions` | BIGINT | Impressions |
| `clicks` | BIGINT | Clicks |
| `conversions` | DOUBLE | Conversions |
| `conversions_value` | DOUBLE | Conversion value |
| *(+ all standard metrics)* | | |

---

### gads_daily_summary

**Purpose:** Daily account-level totals.

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | BIGINT | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `campaign_status` | INTEGER | Campaign status |
| `campaign_type` | INTEGER | Campaign type |
| `date` | VARCHAR | Date |
| `impressions` | BIGINT | Total impressions |
| `clicks` | BIGINT | Total clicks |
| `cost_micros` | BIGINT | Total cost (micros) |
| `cost` | DOUBLE | Total cost |
| *(+ all standard metrics)* | | |

---

## Meta Ads Tables

| Table Name | Rows | Purpose |
|------------|------|---------|
| `meta_daily_account` | 41 | Daily account-level metrics |
| `meta_campaigns` | 15 | Campaign metadata |
| `meta_campaign_insights` | 140 | Daily campaign performance |
| `meta_adsets` | 15 | Ad set (targeting) metadata |
| `meta_adset_insights` | 140 | Daily ad set performance |
| `meta_ads` | 159 | Individual ad metadata |
| `meta_ad_insights` | 0 | Daily ad-level performance |
| `meta_geographic` | 3 | Country-level breakdown |
| `meta_devices` | 6 | Device/platform breakdown |
| `meta_demographics` | 18 | Age/gender breakdown |

---

### meta_daily_account

**Purpose:** Daily account-level performance totals.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID (act_XXXX) |
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

---

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

---

### meta_campaign_insights

**Purpose:** Daily campaign-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
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

---

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

---

### meta_adset_insights

**Purpose:** Daily ad set-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `adset_id` | VARCHAR | Ad set ID |
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

---

### meta_ads

**Purpose:** Individual ad metadata.

| Column | Type | Description |
|--------|------|-------------|
| `ad_id` | VARCHAR | Ad ID (PK) |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `adset_id` | VARCHAR | Ad set ID |
| `ad_name` | VARCHAR | Ad name |
| `status` | VARCHAR | Status |
| `effective_status` | VARCHAR | Effective status |
| `creative_id` | VARCHAR | Creative ID |
| `created_time` | TIMESTAMP | Creation time |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_ad_insights

**Purpose:** Daily ad-level performance metrics.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Date |
| `ad_account_id` | VARCHAR | Ad account ID |
| `campaign_id` | VARCHAR | Campaign ID |
| `campaign_name` | VARCHAR | Campaign name |
| `adset_id` | VARCHAR | Ad set ID |
| `adset_name` | VARCHAR | Ad set name |
| `ad_id` | VARCHAR | Ad ID |
| `ad_name` | VARCHAR | Ad name |
| `impressions` | BIGINT | Impressions |
| `reach` | BIGINT | Reach |
| `clicks` | BIGINT | Clicks |
| `spend` | DOUBLE | Spend |
| `ctr` | DOUBLE | CTR |
| `cpc` | DOUBLE | CPC |
| `cpm` | DOUBLE | CPM |
| `link_clicks` | BIGINT | Link clicks |
| `app_installs` | BIGINT | App installs |
| `purchases` | BIGINT | Purchases |
| `purchase_value` | DOUBLE | Purchase value |
| `extracted_at` | TIMESTAMP | ETL timestamp |

---

### meta_geographic

**Purpose:** Performance breakdown by country.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `country` | VARCHAR | Country code |
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

---

### meta_devices

**Purpose:** Performance breakdown by device and publisher platform.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `device_platform` | VARCHAR | Device (mobile, desktop) |
| `publisher_platform` | VARCHAR | Platform (facebook, instagram, etc.) |
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

### meta_demographics

**Purpose:** Performance breakdown by age and gender.

| Column | Type | Description |
|--------|------|-------------|
| `date_start` | DATE | Period start |
| `date_stop` | DATE | Period end |
| `ad_account_id` | VARCHAR | Ad account ID |
| `age` | VARCHAR | Age bracket (18-24, 25-34, etc.) |
| `gender` | VARCHAR | Gender (male, female, unknown) |
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
    cost_micros / 1000000.0 as cost_sgd,
    cost as cost_direct,  -- Already converted
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

### Cross-Platform Spend Comparison

```sql
-- Total spend across all platforms
SELECT 'Google Ads' as platform, SUM(cost) as total_spend
FROM gads_daily_summary
UNION ALL
SELECT 'Meta Ads', SUM(spend) FROM meta_daily_account
UNION ALL
SELECT 'Organic (SEO)', 0 FROM gsc_daily_totals LIMIT 1;
```

### Top Keywords (Google Ads)

```sql
SELECT 
    keyword_text,
    keyword_match_type,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(cost) as spend,
    SUM(conversions) as conversions
FROM gads_keywords
WHERE clicks > 0
GROUP BY keyword_text, keyword_match_type
ORDER BY conversions DESC
LIMIT 20;
```

### Top SEO Queries

```sql
SELECT 
    query,
    SUM(clicks) as clicks,
    SUM(impressions) as impressions,
    AVG(position) as avg_position
FROM gsc_queries
GROUP BY query
ORDER BY clicks DESC
LIMIT 20;
```

### Meta Campaign Performance

```sql
SELECT 
    campaign_name,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(spend) as spend,
    SUM(app_installs) as installs,
    CASE WHEN SUM(app_installs) > 0 
         THEN SUM(spend) / SUM(app_installs) 
         ELSE 0 END as cost_per_install
FROM meta_campaign_insights
GROUP BY campaign_name
ORDER BY spend DESC;
```

---

## View Layer Architecture

The database implements a **Bronze/Silver/Gold** data architecture using DuckDB views for cleaner querying and cross-platform analysis.

### Layer Overview

| Layer | Purpose | Naming Convention | Example |
|-------|---------|-------------------|---------|
| **Bronze** | Raw tables from ETL | `{platform}_{entity}` | `gads_campaigns` |
| **Silver** | Typed views with standardized columns | `{table}_v` | `gads_campaigns_v` |
| **Gold** | Unified reporting facts | `fact_{domain}_{grain}` | `fact_paid_daily` |
| **Dimensions** | De-duplicated metadata | `dim_{platform}_{entity}` | `dim_gads_campaign` |

### Initializing Views

```bash
# Create all views
python scripts/init_views.py

# Validate grains only
python scripts/init_views.py --validate-only

# Recreate views (drop and recreate)
python scripts/init_views.py --drop
```

### Silver Views

Silver views provide:
- Consistent `date_day` column (DATE type) across all platforms
- Proper numeric types for metrics
- Standardized column names (e.g., `ad_group_id` instead of `adset_id`)
- Platform identifier column

| Platform | Raw Table | Silver View | Key Changes |
|----------|-----------|-------------|-------------|
| GA4 | `ga4_sessions` | `ga4_sessions_v` | `date` → `date_day` (DATE), metrics → numeric |
| GSC | `gsc_queries` | `gsc_queries_v` | `date` → `date_day` (DATE), `position` → `avg_position` |
| Google Ads | `gads_campaigns` | `gads_campaigns_v` | `date` → `date_day` (DATE), cost conversion included |
| Meta Ads | `meta_campaign_insights` | `meta_campaign_insights_v` | `date` → `date_day`, `purchase_value` → `revenue` |

### Gold Fact Views

Unified views for cross-platform reporting:

| View | Description | Use Case |
|------|-------------|----------|
| `fact_paid_daily` | Unified Google Ads + Meta Ads daily metrics | Cross-platform paid spend analysis |
| `fact_paid_adgroup_daily` | Ad group/adset level metrics | Campaign structure analysis |
| `fact_organic_daily` | GSC daily totals | SEO performance tracking |
| `fact_organic_queries` | Query-level organic data | Keyword research |
| `fact_web_daily` | GA4 session aggregates | Website traffic analysis |
| `fact_web_traffic` | Traffic by source/medium | Channel attribution |

### Dimension Views

| View | Description |
|------|-------------|
| `dim_gads_campaign` | Google Ads campaign metadata |
| `dim_gads_ad_group` | Google Ads ad group metadata |
| `dim_gads_keyword` | Google Ads keyword metadata |
| `dim_meta_campaign` | Meta Ads campaign metadata |
| `dim_meta_adset` | Meta Ads ad set metadata |
| `dim_meta_ad` | Meta Ads ad metadata |

### Query Examples with Views

**Cross-Platform Daily Spend:**
```sql
SELECT 
    date_day,
    platform,
    SUM(spend) as total_spend,
    SUM(clicks) as total_clicks
FROM fact_paid_daily
WHERE date_day >= '2025-01-01'
GROUP BY date_day, platform
ORDER BY date_day;
```

**Unified Summary:**
```sql
SELECT * FROM summary_platform_totals;
```

### Grain Definitions

Each table has defined uniqueness constraints (grains):

| Table | Grain (Unique Key) |
|-------|--------------------|
| `gads_daily_summary` | `date`, `campaign_id` |
| `gads_campaigns` | `date`, `campaign_id` |
| `gsc_queries` | `_dataset`, `date`, `query` |
| `meta_campaign_insights` | `date`, `ad_account_id`, `campaign_id` |
| `ga4_traffic_overview` | `date`, `sessionSource`, `sessionMedium`, `sessionCampaignName` |

### Data Quality

Run grain validation to check for duplicates:

```bash
python scripts/init_views.py --validate-only
```

---

## ETL Process

### ETL Commands

| Data Source | Command | Purpose |
|-------------|---------|---------|
| All Sources | `python scripts/run_etl_unified.py --source all --lifetime` | Full historical data |
| All Sources | `python scripts/run_etl_unified.py --source all --lookback-days 3` | Daily incremental |
| GA4 (comprehensive) | `python scripts/run_etl_unified.py --source ga4 --lifetime --comprehensive` | All GA4 metrics |
| Google Ads | `python scripts/run_etl_unified.py --source gads --lifetime` | Google PPC data |
| Search Console | `python scripts/run_etl_unified.py --source gsc --lifetime` | SEO data |
| Meta Ads | `python scripts/run_etl_unified.py --source meta --lifetime` | Meta/Facebook data |

### Data Loading Modes

- **Lifetime Mode** (`--lifetime`): Full table replace - use for initial load or rebuilding
- **Incremental Mode** (default): Upsert - updates existing records, preserves history

### Data Freshness

| Source | Delay | Recommended Update | Max History |
|--------|-------|-------------------|-------------|
| GA4 | 24-48 hours | Daily | Unlimited |
| Search Console | 2-3 days | Daily | 16 months |
| Google Ads | Same day | Daily | Unlimited |
| Meta Ads | Same day | Daily | 37 months |

### Extracted At Timestamps

All tables now include an `extracted_at` timestamp for data lineage:
- **GA4, GSC, Google Ads**: Added via ETL extractors
- **Meta Ads, Twitter**: Already included

---

**Last Updated:** 2026-02-03  
**Database Version:** 5.0 (Added view layer architecture)  
**DuckDB Version:** 0.9.0+
