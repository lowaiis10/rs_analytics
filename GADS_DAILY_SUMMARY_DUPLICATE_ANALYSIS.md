# gads_daily_summary Grain Violation Analysis

## Problem Summary

**Issue:** 231 duplicates found on `['date', 'campaign_id']` where `campaign_id = 0`
- Sample: `{'date': '2026-02-02', 'campaign_id': 0, '_duplicate_count': 12}`

## Root Cause

### 1. Data Extraction Issue

**File:** `etl/gads_extractor.py` (lines 499-524)

The `extract_daily_account_summary()` method queries from the `campaign` resource:
```python
query = f"""
    SELECT
        segments.date,
        metrics.impressions,
        metrics.clicks,
        ...
    FROM campaign
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""
```

**Problem:** Even though the query doesn't SELECT `campaign.id`, the `_row_to_dict()` method (lines 114-119) automatically populates `campaign_id` from the row's campaign object:
```python
if hasattr(row, 'campaign') and row.campaign:
    campaign = row.campaign
    result['campaign_id'] = campaign.id  # This gets populated!
```

This means the extraction returns **one row per campaign per date**, not one row per date.

### 2. Table Key Mismatch

**File:** `scripts/utils/db.py` (line 49)

The table key is defined as:
```python
'gads_daily_summary': ['date'],  # ❌ Only date, missing campaign_id
```

**File:** `scripts/utils/data_quality.py` (line 33)

But the grain validation expects:
```python
'gads_daily_summary': ['date', 'campaign_id'],  # ✅ Includes campaign_id
```

### 3. Upsert Logic Problem

When upserting data:
1. The upsert function uses `['date']` as the key (from `db.py`)
2. It deletes all rows matching the date: `DELETE FROM gads_daily_summary WHERE date IN (...)`
3. Then inserts all new rows, including multiple campaigns for the same date
4. If the same date is processed multiple times, or if `campaign_id = 0` appears multiple times, duplicates accumulate

### 4. Why campaign_id = 0?

When querying from the `campaign` resource:
- Some rows may have `campaign.id = 0` (invalid/missing campaign ID)
- Or the campaign object may be None/empty, resulting in `campaign_id = 0`
- All rows with `campaign_id = 0` and the same date create duplicate key violations

## Recommended Fix

### Option 1: Update Table Key (RECOMMENDED)

**Change:** Update the table key definition in `scripts/utils/db.py` to include `campaign_id`:

```python
'gads_daily_summary': ['date', 'campaign_id'],  # Match grain definition
```

**Pros:**
- Aligns with grain validation expectations
- Allows proper upsert behavior (deletes by date + campaign_id)
- Matches the actual data structure (campaign-level, not account-level)
- No changes needed to extraction logic

**Cons:**
- May require data cleanup for existing duplicates

### Option 2: Aggregate at Account Level

**Change:** Modify `extract_daily_account_summary()` to aggregate metrics and remove campaign_id:

```python
def extract_daily_account_summary(self, start_date: str, end_date: str):
    # Query customer-level data instead of campaign-level
    query = f"""
        SELECT
            segments.date,
            SUM(metrics.impressions) as impressions,
            SUM(metrics.clicks) as clicks,
            ...
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY segments.date
    """
    # Then manually set campaign_id to NULL or remove it
```

**Pros:**
- Matches the "account-level" description in DATABASE_DESIGN.md
- True one row per date grain

**Cons:**
- Requires changing extraction logic
- Loses campaign-level granularity
- May break existing queries that expect campaign_id

## Implementation Plan

### Step 1: Fix Table Key Definition
- Update `scripts/utils/db.py` line 49 to include `campaign_id`

### Step 2: Clean Existing Duplicates
- Run cleanup query to remove duplicates, keeping the most recent `extracted_at` timestamp

### Step 3: Re-run ETL
- Re-extract data with corrected upsert logic

### Step 4: Verify
- Run grain validation to confirm no duplicates remain

## Cleanup Query

```sql
-- Remove duplicates, keeping the most recent extracted_at
DELETE FROM gads_daily_summary
WHERE (date, campaign_id) IN (
    SELECT date, campaign_id
    FROM gads_daily_summary
    GROUP BY date, campaign_id
    HAVING COUNT(*) > 1
)
AND rowid NOT IN (
    SELECT MAX(rowid)
    FROM gads_daily_summary
    GROUP BY date, campaign_id
    HAVING COUNT(*) > 1
);
```

Or using DuckDB's window functions:
```sql
-- Keep only one row per (date, campaign_id), preferring most recent extracted_at
DELETE FROM gads_daily_summary
WHERE (date, campaign_id, extracted_at) NOT IN (
    SELECT date, campaign_id, MAX(extracted_at) as extracted_at
    FROM gads_daily_summary
    GROUP BY date, campaign_id
);
```
