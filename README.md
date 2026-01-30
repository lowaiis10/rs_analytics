# rs_analytics

Analytics pipeline for extracting Google Analytics 4 (GA4) data into a local DuckDB warehouse with a Streamlit dashboard for visualization.

## Features

- **GA4 Data Extraction**: Pull analytics data using the Google Analytics Data API
- **Local Data Warehouse**: Store data in DuckDB for fast local queries
- **Streamlit Dashboard**: Interactive visualization of analytics data
- **Secure Credentials**: Service account authentication with no secrets in code
- **Extensible**: Ready for BigQuery mirror, Google Ads, Meta Ads integration

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Credentials

See the [Credentials & Security](#credentials--security) section below for detailed instructions.

### 3. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your actual values
# Use absolute paths for all file references
```

### 4. Test Your Setup

```bash
# Verify credentials are working
python scripts/test_ga4_connection.py
```

### 5. Run the ETL Pipeline

```bash
# Extract GA4 data and load into DuckDB
python scripts/run_etl.py
```

### 6. Launch the Dashboard

```bash
streamlit run app/main.py
```

---

## Credentials & Security

This section explains how to securely configure authentication for GA4 and other integrations.

### Overview

- **Authentication Method**: Service Account (not OAuth)
- **Credential Storage**: JSON key file in `secrets/` directory
- **Environment Variable**: `GOOGLE_APPLICATION_CREDENTIALS`
- **What NOT to use**: OAuth, refresh tokens, client secrets

### Step 1: Create a Google Cloud Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select or create a project
3. Navigate to **IAM & Admin → Service Accounts**
4. Click **+ Create Service Account**
5. Enter a name (e.g., `rs-analytics-ga4`)
6. Click **Create and Continue**
7. Skip the optional role assignment (GA4 permissions are set separately)
8. Click **Done**

### Step 2: Generate a JSON Key

1. In the Service Accounts list, find your new service account
2. Click the three-dot menu → **Manage keys**
3. Click **Add Key → Create new key**
4. Select **JSON** format
5. Click **Create** - the key file will download automatically
6. **Move the downloaded file to**: `secrets/ga4_service_account.json`

### Step 3: Secure the Credentials File

**Linux/macOS:**
```bash
# Restrict file permissions (owner read-only)
chmod 600 secrets/ga4_service_account.json
```

**Windows:**
Right-click the file → Properties → Security → Edit → Remove all users except your account.

### Step 4: Enable the Google Analytics Data API

1. Go to [Google Analytics Data API](https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com)
2. Select your project
3. Click **Enable**
4. Wait 2-5 minutes for the change to propagate

### Step 5: Grant GA4 Property Access

1. Open [Google Analytics](https://analytics.google.com/)
2. Go to **Admin** (gear icon)
3. In the Property column, click **Property Access Management**
4. Click **+** to add a new user
5. Enter the service account email (found in your JSON file, looks like `name@project.iam.gserviceaccount.com`)
6. Select role: **Viewer** (minimum required)
7. Uncheck "Notify new users by email" (service accounts can't receive email)
8. Click **Add**

### Step 6: Configure Environment Variables

Edit your `.env` file with the correct values:

```env
# GA4 Property ID (found in GA4 → Admin → Property Settings)
GA4_PROPERTY_ID=123456789

# ABSOLUTE path to your service account JSON
# Windows example:
GOOGLE_APPLICATION_CREDENTIALS=C:/Users/username/projects/rs_analytics/secrets/ga4_service_account.json

# Linux/macOS example:
# GOOGLE_APPLICATION_CREDENTIALS=/home/username/rs_analytics/secrets/ga4_service_account.json
```

**Important**: Use forward slashes (`/`) even on Windows, or escape backslashes (`\\`).

### Step 7: Test Your Credentials

```bash
python scripts/test_ga4_connection.py
```

Expected output:
```
============================================================
  rs_analytics - GA4 Connection Test
============================================================

Step 1: Loading configuration...
  [OK] Configuration loaded successfully
  [i] GA4 Property ID: 123456789
  [i] Credentials Path: C:/Users/.../secrets/ga4_service_account.json

Step 2: Verifying credentials file...
  [OK] Credentials file exists
  [OK] Credentials file has appropriate permissions

Step 3: Verifying service account JSON structure...
  [OK] Service account JSON structure is valid
  [i] Service Account Type: service_account
  [i] GCP Project: your-project-id
  [i] Service Account Email: name@project.iam.gserviceaccount.com

Step 4: Testing GA4 API connection...
  [i] Creating GA4 client (using GOOGLE_APPLICATION_CREDENTIALS)...
  [OK] GA4 client created successfully

Step 5: Running test query...
  [i] Querying property 123456789 for date: 2024-01-15
  [OK] GA4 API query executed successfully!

  --- Query Results ---
  Sessions: 1234
  Active Users: 567
  Page Views: 8901

============================================================
  All Tests Passed!
============================================================
```

---

## Common Errors and Fixes

### Error: "Missing GOOGLE_APPLICATION_CREDENTIALS"

**Cause**: The environment variable is not set.

**Fix**:
1. Ensure `.env` file exists in the project root
2. Verify `GOOGLE_APPLICATION_CREDENTIALS` is set with an absolute path
3. Check the path uses forward slashes or escaped backslashes

### Error: "Service account file not found"

**Cause**: The path in `GOOGLE_APPLICATION_CREDENTIALS` doesn't point to an existing file.

**Fix**:
1. Verify the JSON file exists at the specified path
2. Check for typos in the path
3. Use an absolute path, not relative

### Error: "Permission denied (403)"

**Cause**: Service account doesn't have access to the GA4 property.

**Fix**:
1. Go to GA4 → Admin → Property Access Management
2. Add the service account email as a user
3. Grant at least "Viewer" role
4. Wait 2-5 minutes for permissions to propagate

### Error: "API has not been used in project"

**Cause**: The Google Analytics Data API is not enabled.

**Fix**:
1. Go to [Google Analytics Data API](https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com)
2. Select your GCP project
3. Click "Enable"
4. Wait 2-5 minutes

### Error: "Property not found"

**Cause**: Invalid GA4 Property ID.

**Fix**:
1. Go to GA4 → Admin → Property Settings
2. Find your Property ID (numeric value)
3. Update `GA4_PROPERTY_ID` in `.env`

### Error: "Invalid JSON in credentials file"

**Cause**: The JSON file is corrupted or incomplete.

**Fix**:
1. Re-download the service account key from GCP Console
2. Replace `secrets/ga4_service_account.json` with the new file
3. Ensure the file wasn't truncated during download

---

## Project Structure

```
rs_analytics/
├── .env.example          # Template for environment variables (committed)
├── .env                  # Your local configuration (NOT committed)
├── .gitignore            # Git ignore rules
├── README.md             # This file
├── requirements.txt      # Python dependencies
│
├── secrets/              # Credential files (NOT committed)
│   ├── .gitkeep
│   └── ga4_service_account.json
│
├── data/                 # DuckDB database (NOT committed)
│   └── warehouse.duckdb
│
├── logs/                 # Application logs (NOT committed)
│   └── *.log
│
├── etl/                  # ETL modules
│   ├── __init__.py
│   └── config.py         # Central configuration loader
│
├── scripts/              # Runnable scripts
│   ├── __init__.py
│   ├── test_ga4_connection.py  # Credential testing
│   └── run_etl.py        # ETL pipeline runner
│
└── app/                  # Streamlit dashboard
    ├── __init__.py
    └── main.py           # Main dashboard app
```

---

## Running in Production (VPS/Server)

### Cron Job Setup

```bash
# Edit crontab
crontab -e

# Add daily ETL run at 6 AM
0 6 * * * cd /path/to/rs_analytics && /path/to/python scripts/run_etl.py >> logs/cron.log 2>&1
```

**Note**: The script loads `.env` automatically, so no environment variable exports are needed in the cron command.

### Systemd Service (Alternative)

Create `/etc/systemd/system/rs-analytics-etl.service`:

```ini
[Unit]
Description=rs_analytics ETL Service
After=network.target

[Service]
Type=oneshot
User=your-username
WorkingDirectory=/path/to/rs_analytics
ExecStart=/path/to/python scripts/run_etl.py

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/rs-analytics-etl.timer`:

```ini
[Unit]
Description=Run rs_analytics ETL daily

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```bash
sudo systemctl enable rs-analytics-etl.timer
sudo systemctl start rs-analytics-etl.timer
```

---

## Security Best Practices

1. **Never commit secrets**: `.env` and `secrets/` are in `.gitignore`
2. **Restrict file permissions**: `chmod 600` on credential files
3. **Use absolute paths**: Prevents issues with working directory changes
4. **Fail fast**: Invalid config stops execution immediately with clear errors
5. **No secrets in logs**: Only paths and status are logged, never credential contents
6. **Service accounts only**: OAuth refresh tokens are explicitly not used

---

## Future Integrations

The configuration system is designed to be extensible:

- **BigQuery**: Set `ENABLE_BQ_MIRROR=1` and configure BQ credentials
- **Google Ads**: Add `GOOGLE_ADS_*` variables (planned)
- **Meta/Facebook Ads**: Add `META_*` variables (planned)

Each integration follows the same pattern:
1. Add environment variables to `.env.example`
2. Add validation logic to `etl/config.py`
3. Create extraction module under `etl/`

---

## Troubleshooting

If you encounter issues:

1. **Run the connection test**: `python scripts/test_ga4_connection.py`
2. **Check logs**: Look in `logs/` directory for detailed error messages
3. **Verify environment**: Ensure `.env` exists with correct values
4. **Check permissions**: Service account needs GA4 access

For Streamlit issues:
- The app validates configuration at startup
- Errors are displayed with clear fix instructions
- Use the "Test GA4 Connection" button in the sidebar

---

## License

[Add your license here]
