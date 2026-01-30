# GA4 Comprehensive Metrics Guide

## Quick Start

### Pull ALL Lifetime Data
```bash
python scripts/run_etl_comprehensive.py --lifetime
```

### Pull Last 90 Days with All Metrics
```bash
python scripts/run_etl_comprehensive.py --lookback-days 90
```

### Pull Specific Date Range
```bash
python scripts/run_etl_comprehensive.py --start-date 2023-01-01 --end-date 2024-12-31
```

---

## What Gets Extracted

The comprehensive ETL pulls **6 different datasets** with optimized dimension/metric combinations:

### 1. Traffic Overview (`ga4_traffic_overview`)
**Dimensions:**
- date, sessionSource, sessionMedium, sessionCampaignName
- sessionDefaultChannelGroup, deviceCategory, country

**Metrics:**
- activeUsers, newUsers, sessions, engagedSessions
- engagementRate, screenPageViews, averageSessionDuration
- bounceRate, conversions, totalRevenue

**Use for:** Overall traffic analysis, channel performance, campaign ROI

---

### 2. Page Performance (`ga4_page_performance`)
**Dimensions:**
- date, pageTitle, pagePath, landingPage, deviceCategory

**Metrics:**
- screenPageViews, activeUsers, sessions
- engagementRate, averageSessionDuration, bounceRate, conversions

**Use for:** Content performance, landing page optimization, user journey analysis

---

### 3. Geographic Data (`ga4_geographic_data`)
**Dimensions:**
- date, country, region, city, deviceCategory

**Metrics:**
- activeUsers, newUsers, sessions, screenPageViews
- engagementRate, conversions, totalRevenue

**Use for:** Geographic targeting, regional performance, localization decisions

---

### 4. Technology Data (`ga4_technology_data`)
**Dimensions:**
- date, deviceCategory, operatingSystem, browser, screenResolution

**Metrics:**
- activeUsers, sessions, screenPageViews, engagementRate, bounceRate

**Use for:** Technical optimization, device compatibility, browser testing

---

### 5. Event Data (`ga4_event_data`)
**Dimensions:**
- date, eventName, deviceCategory

**Metrics:**
- eventCount, activeUsers, sessions, engagementRate

**Use for:** Custom event tracking, user interaction analysis, feature usage

---

### 6. Ecommerce Data (`ga4_ecommerce_data`)
**Dimensions:**
- date, sessionSource, sessionMedium, deviceCategory

**Metrics:**
- transactions, ecommercePurchases, purchaseRevenue
- itemsPurchased, itemRevenue, shippingAmount, taxAmount

**Use for:** Sales analysis, revenue attribution, ecommerce optimization

---

## Complete List of Available GA4 Metrics

### User Metrics
- `activeUsers` - Users who engaged with your site/app
- `newUsers` - First-time users
- `totalUsers` - Total unique users
- `userEngagementDuration` - Total engagement time
- `engagedSessions` - Sessions lasting >10s or with conversion/2+ page views
- `engagementRate` - Percentage of engaged sessions

### Session Metrics
- `sessions` - Total number of sessions
- `sessionsPerUser` - Average sessions per user
- `averageSessionDuration` - Average session length
- `bounceRate` - Percentage of non-engaged sessions

### Page/Screen Metrics
- `screenPageViews` - Total page/screen views
- `screenPageViewsPerSession` - Average views per session
- `screenPageViewsPerUser` - Average views per user

### Event Metrics
- `eventCount` - Total number of events
- `eventCountPerUser` - Average events per user
- `eventsPerSession` - Average events per session

### Conversion Metrics
- `conversions` - Total conversion events
- `totalRevenue` - Total revenue (purchase + ad + subscription)
- `purchaseRevenue` - Revenue from purchases
- `adRevenue` - Revenue from ads
- `totalAdRevenue` - Total ad revenue

### Ecommerce Metrics
- `transactions` - Number of purchase transactions
- `transactionsPerPurchaser` - Average transactions per buyer
- `purchaseToViewRate` - Purchase rate
- `itemsViewed` - Product views
- `itemsAddedToCart` - Items added to cart
- `itemsPurchased` - Items purchased
- `itemRevenue` - Revenue from items
- `cartToViewRate` - Add-to-cart rate
- `checkouts` - Checkout initiations
- `ecommercePurchases` - Completed purchases
- `firstTimePurchasers` - New customers
- `shippingAmount` - Total shipping fees
- `taxAmount` - Total tax collected
- `refundAmount` - Total refunds

### Engagement Metrics
- `averageEngagementTime` - Average time users engaged
- `averageEngagementTimePerSession` - Engagement time per session
- `userEngagementDuration` - Total engagement duration

### Publisher Metrics (AdSense)
- `publisherAdClicks` - Ad clicks
- `publisherAdImpressions` - Ad impressions
- `totalAdRevenue` - Ad revenue

### Video Metrics
- `videoStart` - Video starts
- `videoProgress` - Video progress events
- `videoComplete` - Video completions

---

## Complete List of Available GA4 Dimensions

### Date & Time
- `date` - Date (YYYYMMDD)
- `dateHour` - Date and hour
- `year`, `month`, `week`, `day`

### Traffic Source
- `sessionSource` - Traffic source
- `sessionMedium` - Traffic medium
- `sessionCampaignName` - Campaign name
- `sessionDefaultChannelGroup` - Default channel grouping
- `sessionSourceMedium` - Source/medium combination
- `sessionManualAdContent` - Ad content
- `sessionManualTerm` - Search term
- `sessionGoogleAdsAccountName` - Google Ads account
- `sessionGoogleAdsCampaignName` - Google Ads campaign
- `sessionGoogleAdsAdGroupName` - Google Ads ad group
- `sessionGoogleAdsKeyword` - Google Ads keyword
- `sessionGoogleAdsQuery` - Google Ads search query

### Geography
- `country` - Country
- `region` - Region/state
- `city` - City
- `continent` - Continent
- `subContinent` - Sub-continent

### Technology
- `deviceCategory` - Device type (desktop/mobile/tablet)
- `operatingSystem` - OS name
- `operatingSystemVersion` - OS version
- `browser` - Browser name
- `browserVersion` - Browser version
- `mobileDeviceBranding` - Device brand
- `mobileDeviceModel` - Device model
- `screenResolution` - Screen resolution

### Page & Content
- `pageTitle` - Page title
- `pagePath` - Page path
- `pagePathPlusQueryString` - Page path with query
- `hostName` - Hostname
- `landingPage` - Landing page
- `landingPagePlusQueryString` - Landing page with query

### Events
- `eventName` - Event name
- `linkUrl` - Link URL
- `linkDomain` - Link domain
- `linkText` - Link text
- `outbound` - Outbound link indicator

### User
- `newVsReturning` - New vs returning user
- `userAgeBracket` - Age range
- `userGender` - Gender
- `language` - Language
- `languageCode` - Language code

### Platform
- `platform` - Platform (web/iOS/Android)
- `platformDeviceCategory` - Platform + device
- `appVersion` - App version
- `streamId` - Data stream ID
- `streamName` - Data stream name

---

## Data Limits & Best Practices

### GA4 API Limits
- **Max dimensions per request:** 9
- **Max metrics per request:** 10
- **Max rows per request:** 100,000
- **Daily quota:** 200,000 requests

### Recommendations
1. **For lifetime data:** Run during off-peak hours (takes longer)
2. **For daily updates:** Use standard ETL with 30-day lookback
3. **For analysis:** Query DuckDB tables (much faster than re-pulling from GA4)

### Historical Data Availability
- GA4 stores data from when you first set up the property
- No retroactive data before GA4 implementation
- Universal Analytics data is separate (not accessible via GA4 API)

---

## Querying Your Data

After running the comprehensive ETL, query your DuckDB database:

```python
import duckdb

# Connect to database
conn = duckdb.connect('data/warehouse.duckdb')

# Example: Top traffic sources by revenue
result = conn.execute("""
    SELECT 
        sessionSource,
        sessionMedium,
        SUM(sessions) as total_sessions,
        SUM(totalRevenue) as revenue
    FROM ga4_traffic_overview
    WHERE date >= '20240101'
    GROUP BY sessionSource, sessionMedium
    ORDER BY revenue DESC
    LIMIT 10
""").fetchdf()

print(result)
```

---

## Troubleshooting

### "Metric X is not available"
Some metrics only work with specific dimensions or require ecommerce setup:
- Ecommerce metrics require ecommerce events to be tracked
- Ad revenue metrics require AdSense integration
- Video metrics require video tracking setup

### "Too many rows"
If you hit the 100,000 row limit, the script automatically paginates.
For very large datasets, consider:
- Breaking into smaller date ranges
- Using fewer dimensions
- Aggregating data differently

### "API quota exceeded"
If you hit daily limits:
- Wait 24 hours for quota reset
- Reduce the number of datasets
- Use incremental updates instead of full refreshes

---

## Next Steps

1. **Run comprehensive ETL:**
   ```bash
   python scripts/run_etl_comprehensive.py --lifetime
   ```

2. **Update dashboard** to query new tables (see `app/main.py`)

3. **Set up daily automation:**
   ```bash
   # Add to crontab for daily updates
   0 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_comprehensive.py --lookback-days 7
   ```

4. **Build custom reports** using DuckDB SQL queries

---

## Support

For GA4 API documentation:
- [GA4 Data API Reference](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [Available Dimensions & Metrics](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
