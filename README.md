# rs_analytics

**Unified Analytics Pipeline** for extracting data from Google Analytics 4 (GA4), Google Search Console (GSC), and Google Ads into a local DuckDB warehouse with a Streamlit dashboard for visualization.

## Features

- **GA4 Data Extraction**: Pull website analytics using the Google Analytics Data API
- **Google Search Console (SEO)**: Comprehensive organic search data and keyword performance
- **Google Ads (PPC)**: Campaign, ad group, keyword, and conversion data from Google Ads
- **Local Data Warehouse**: Store 165,000+ rows in DuckDB for fast local queries
- **Streamlit Dashboard**: Interactive 4-tab visualization (GA4, SEO, PPC, Settings)
- **Secure Credentials**: Service account and OAuth authentication with no secrets in code
- **Lifetime Data**: Pull complete historical data from all sources

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Credentials

See the detailed sections below for each data source:
- [GA4 Credentials](#ga4-google-analytics-4)
- [Search Console Credentials](#google-search-console-seo)
- [Google Ads Credentials](#google-ads-ppc)

### 3. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your actual values
# Use absolute paths for all file references
```

### 4. Test Your Connections

```bash
# Test GA4
python scripts/test_ga4_connection.py

# Test Search Console
python scripts/test_gsc_connection.py

# Test Google Ads
python scripts/test_gads_connection.py
```

### 5. Run the ETL Pipelines

```bash
# GA4 - Comprehensive extraction (all metrics, lifetime data)
python scripts/run_etl_comprehensive.py --lifetime

# Search Console - Full SEO data extraction
python scripts/run_etl_gsc.py --lifetime

# Google Ads - Complete advertising data
python scripts/run_etl_gads.py --lifetime
```

### 6. Launch the Dashboard

```bash
streamlit run app/main.py --server.port 3000
```

Open **http://localhost:3000** to view your analytics dashboard with 4 tabs:
- 📊 **GA4 Analytics** - Website traffic and user behavior
- 🔍 **Search Console (SEO)** - Organic search performance
- 💰 **Google Ads (PPC)** - Paid advertising metrics
- ⚙️ **Settings** - Configuration and connection status

---

## Data Sources

### GA4 (Google Analytics 4)

**Authentication:** Service Account  
**Tables Created:** 6  
**Data Coverage:** Traffic, pages, geography, technology, events

#### Setup Steps

1. **Create Service Account** in [Google Cloud Console](https://console.cloud.google.com/)
2. **Download JSON key** and save to `secrets/ga4_service_account.json`
3. **Enable** the [Google Analytics Data API](https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com)
4. **Add service account** to GA4 property (Admin → Property Access Management)
5. **Configure** `.env`:
   ```env
   GA4_PROPERTY_ID=123456789
   GOOGLE_APPLICATION_CREDENTIALS=C:/path/to/secrets/ga4_service_account.json
   ```

#### ETL Commands

```bash
# Standard daily ETL (last 30 days)
python scripts/run_etl.py

# Comprehensive ETL (all metrics, lifetime)
python scripts/run_etl_comprehensive.py --lifetime

# Custom date range
python scripts/run_etl_comprehensive.py --start-date 2024-01-01 --end-date 2024-12-31
```

---

### Google Search Console (SEO)

**Authentication:** Service Account  
**Tables Created:** 10  
**Data Coverage:** Queries, pages, countries, devices, daily totals

#### Setup Steps

1. **Create Service Account** (can reuse GA4 service account or create new)
2. **Download JSON key** and save to `secrets/gsc_service_account.json`
3. **Enable** the [Search Console API](https://console.cloud.google.com/apis/library/searchconsole.googleapis.com)
4. **Add service account** to Search Console (Settings → Users and permissions)
5. **Configure** `.env`:
   ```env
   # For domain property (recommended)
   GSC_SITE_URL=sc-domain:yourdomain.com
   
   # OR for URL prefix property
   GSC_SITE_URL=https://www.yourdomain.com/
   
   GOOGLE_SEARCH_CONSOLE_CREDENTIALS=C:/path/to/secrets/gsc_service_account.json
   ```

#### ETL Commands

```bash
# Full lifetime extraction
python scripts/run_etl_gsc.py --lifetime

# Last 90 days
python scripts/run_etl_gsc.py --lookback-days 90

# Custom date range
python scripts/run_etl_gsc.py --start-date 2024-06-01 --end-date 2024-12-31
```

---

### Google Ads (PPC)

**Authentication:** OAuth 2.0 (Desktop App)  
**Tables Created:** 9  
**Data Coverage:** Campaigns, ad groups, keywords, ads, devices, geographic, hourly, conversions

#### Setup Steps

1. **Apply for Google Ads API access** at [Google Ads API Center](https://ads.google.com/home/tools/manager-accounts/)
2. **Create OAuth 2.0 Client** in Google Cloud Console:
   - Go to APIs & Services → Credentials
   - Create OAuth 2.0 Client ID → **Desktop app**
   - Download client ID and client secret
3. **Create `secrets/google_ads.yaml`**:
   ```yaml
   developer_token: YOUR_DEVELOPER_TOKEN
   client_id: YOUR_OAUTH_CLIENT_ID
   client_secret: YOUR_OAUTH_CLIENT_SECRET
   refresh_token: YOUR_REFRESH_TOKEN
   login_customer_id: YOUR_MANAGER_ACCOUNT_ID
   use_proto_plus: True
   ```
4. **Generate Refresh Token**:
   ```bash
   python scripts/generate_gads_refresh_token.py
   ```
5. **Configure** `.env`:
   ```env
   GOOGLE_ADS_YAML_PATH=C:/path/to/secrets/google_ads.yaml
   # Use CLIENT account ID (not manager) for metrics
   GOOGLE_ADS_CUSTOMER_ID=1234567890
   ```

#### Finding Your Customer IDs

```bash
# List all accounts under your manager account
python scripts/list_gads_accounts.py
```

This will show:
- **Manager Account ID** (use for `login_customer_id` in YAML)
- **Client Account IDs** (use for `GOOGLE_ADS_CUSTOMER_ID` in .env)

#### ETL Commands

```bash
# Full lifetime extraction
python scripts/run_etl_gads.py --lifetime

# Last 30 days
python scripts/run_etl_gads.py --lookback-days 30

# Custom date range
python scripts/run_etl_gads.py --start-date 2024-01-01 --end-date 2024-12-31
```

---

## Project Structure

```
rs_analytics/
├── .env.example              # Template for environment variables
├── .env                      # Your local configuration (NOT committed)
├── .gitignore                # Git ignore rules
├── README.md                 # This file
├── requirements.txt          # Python dependencies
│
├── secrets/                  # Credential files (NOT committed)
│   ├── ga4_service_account.json
│   ├── gsc_service_account.json
│   └── google_ads.yaml
│
├── data/                     # DuckDB database & documentation
│   ├── warehouse.duckdb      # Local analytics warehouse
│   └── DATABASE_DESIGN.md    # Complete schema documentation
│
├── logs/                     # Application logs (NOT committed)
│   └── *.log
│
├── etl/                      # ETL configuration modules
│   ├── __init__.py
│   ├── config.py             # GA4 configuration
│   ├── gsc_config.py         # Search Console configuration
│   ├── gads_config.py        # Google Ads configuration
│   ├── gsc_extractor.py      # Search Console data extractor
│   └── gads_extractor.py     # Google Ads data extractor
│
├── scripts/                  # Runnable scripts
│   ├── run_etl.py            # Standard GA4 ETL
│   ├── run_etl_comprehensive.py  # Full GA4 extraction
│   ├── run_etl_gsc.py        # Search Console ETL
│   ├── run_etl_gads.py       # Google Ads ETL
│   ├── test_ga4_connection.py
│   ├── test_gsc_connection.py
│   ├── test_gads_connection.py
│   ├── list_gads_accounts.py  # List Google Ads accounts
│   └── generate_gads_refresh_token.py
│
└── app/                      # Streamlit dashboard
    ├── __init__.py
    └── main.py               # Multi-tab dashboard app
```

---

## Database Overview

**Location:** `data/warehouse.duckdb`  
**Total Tables:** 25  
**Total Rows:** 165,000+

| Source | Tables | Rows | Data |
|--------|--------|------|------|
| GA4 | 6 | 63,480+ | Traffic, pages, geography, technology, events |
| Search Console | 10 | 94,270+ | Queries, pages, countries, devices |
| Google Ads | 9 | 7,395+ | Campaigns, keywords, ads, conversions |

See `data/DATABASE_DESIGN.md` for complete schema documentation.

---

## Common Errors and Fixes

### GA4 Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "Missing GOOGLE_APPLICATION_CREDENTIALS" | Env var not set | Set absolute path in `.env` |
| "Service account file not found" | Wrong path | Verify file exists, use absolute path |
| "Permission denied (403)" | No GA4 access | Add service account to GA4 property |
| "API has not been used in project" | API not enabled | Enable Analytics Data API in GCP |

### Search Console Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "Configured site not found" | Wrong URL format | Use `sc-domain:` prefix for domain properties |
| "Permission denied" | No access | Add service account in Search Console settings |

### Google Ads Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "unauthorized_client" | Invalid refresh token | Run `generate_gads_refresh_token.py` |
| "Metrics cannot be requested for manager account" | Using MCA ID | Use client account ID in `.env` |
| "DEVELOPER_TOKEN_NOT_APPROVED" | Test token | Apply for basic or standard access |

---

## Running in Production

### Cron Job Setup

```bash
# Edit crontab
crontab -e

# Daily GA4 update at 6 AM
0 6 * * * cd /path/to/rs_analytics && python scripts/run_etl.py >> logs/cron.log 2>&1

# Daily GSC update at 6:30 AM
30 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_gsc.py >> logs/cron.log 2>&1

# Daily Google Ads update at 7 AM
0 7 * * * cd /path/to/rs_analytics && python scripts/run_etl_gads.py >> logs/cron.log 2>&1
```

### Windows Task Scheduler

Create scheduled tasks for each ETL script with appropriate triggers (daily, specific times).

---

## Security Best Practices

1. **Never commit secrets**: `.env`, `secrets/`, and `*.yaml` are in `.gitignore`
2. **Restrict file permissions**: `chmod 600` on credential files
3. **Use absolute paths**: Prevents working directory issues
4. **Fail fast**: Invalid config stops execution with clear errors
5. **No secrets in logs**: Only paths and status are logged
6. **Separate credentials**: Use different service accounts per service when possible

---

## Troubleshooting

If you encounter issues:

1. **Run connection tests** for the problematic service
2. **Check logs** in `logs/` directory
3. **Verify environment** variables in `.env`
4. **Check permissions** for service accounts
5. **Review API quotas** in Google Cloud Console

For Streamlit dashboard issues:
- Configuration validated at startup
- Errors displayed with fix instructions
- Use Settings tab to test connections

---

## License

[Add your license here]
