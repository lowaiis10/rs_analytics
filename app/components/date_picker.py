"""
Date Range Picker Component for RS Analytics

This module provides a reusable calendar-based date range selector
for all dashboard pages, replacing the dropdown-based date selection.

Usage:
    from app.components.date_picker import render_date_range_picker
    
    start_date, end_date = render_date_range_picker(
        key="my_page_dates",
        default_days=30
    )
"""

from datetime import datetime, timedelta, date
from typing import Tuple, Optional
import streamlit as st


def render_date_range_picker(
    key: str = "date_range",
    default_days: int = 30,
    max_days: int = 365,
    show_comparison: bool = False
) -> Tuple[date, date, Optional[date], Optional[date]]:
    """
    Render a calendar-based date range picker with preset options.
    
    Args:
        key: Unique key for this date picker instance (required for multiple pickers on same page)
        default_days: Default number of days to select (default: 30)
        max_days: Maximum allowed date range in days (default: 365)
        show_comparison: If True, also returns comparison period dates
    
    Returns:
        If show_comparison is False:
            (start_date, end_date, None, None)
        If show_comparison is True:
            (start_date, end_date, prev_start_date, prev_end_date)
    
    Example:
        # Simple date range
        start, end, _, _ = render_date_range_picker(key="ga4_dates")
        
        # With comparison period
        start, end, prev_start, prev_end = render_date_range_picker(
            key="exec_dates",
            show_comparison=True
        )
    """
    
    # Initialize session state for this picker if not exists
    if f"{key}_preset" not in st.session_state:
        st.session_state[f"{key}_preset"] = f"Last {default_days} days"
    
    # Calculate default date range
    today = datetime.now().date()
    default_start = today - timedelta(days=default_days)
    default_end = today
    
    # Date range preset options
    preset_options = {
        "Last 7 days": 7,
        "Last 14 days": 14,
        "Last 30 days": 30,
        "Last 60 days": 60,
        "Last 90 days": 90,
        "Last 180 days": 180,
        "Last 365 days": 365,
        "Custom Range": None
    }
    
    # ========================================
    # Row 1: Preset Selector
    # ========================================
    st.markdown("##### 📅 Select Date Range")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Preset selector as horizontal radio buttons
        selected_preset = st.radio(
            "Quick Select",
            options=list(preset_options.keys()),
            index=2,  # Default to "Last 30 days"
            key=f"{key}_preset_selector",
            horizontal=True,
            label_visibility="collapsed"
        )
    
    with col2:
        # Reset to today button
        if st.button("📍 Today", key=f"{key}_reset", use_container_width=True):
            st.session_state[f"{key}_preset"] = "Last 30 days"
            st.rerun()
    
    # ========================================
    # Row 2: Calendar Inputs (if Custom Range selected)
    # ========================================
    
    # Determine start and end dates based on preset selection
    if selected_preset == "Custom Range":
        st.markdown("**Select Custom Date Range:**")
        
        # Initialize custom date session state
        if f"{key}_custom_start" not in st.session_state:
            st.session_state[f"{key}_custom_start"] = default_start
        if f"{key}_custom_end" not in st.session_state:
            st.session_state[f"{key}_custom_end"] = default_end
        
        date_col1, date_col2 = st.columns(2)
        
        with date_col1:
            # Start date picker - calendar widget
            start_date = st.date_input(
                "From Date",
                value=st.session_state[f"{key}_custom_start"],
                max_value=today,
                key=f"{key}_start_date",
                help="Select the start date for your date range"
            )
            st.session_state[f"{key}_custom_start"] = start_date
        
        with date_col2:
            # End date picker - calendar widget
            end_date = st.date_input(
                "To Date",
                value=st.session_state[f"{key}_custom_end"],
                min_value=start_date,
                max_value=today,
                key=f"{key}_end_date",
                help="Select the end date for your date range"
            )
            st.session_state[f"{key}_custom_end"] = end_date
        
        # Validate date range duration
        days_diff = (end_date - start_date).days
        
        if days_diff > max_days:
            st.error(f"⚠️ Date range cannot exceed {max_days} days. Please select a shorter range.")
            # Fallback to max allowed range
            end_date = start_date + timedelta(days=max_days)
        
        if days_diff < 0:
            st.error("⚠️ Start date must be before end date.")
            # Swap dates
            start_date, end_date = end_date, start_date
        
    else:
        # Use preset date range
        days = preset_options[selected_preset]
        end_date = today
        start_date = today - timedelta(days=days)
    
    # ========================================
    # Row 3: Comparison Period Selector (optional)
    # ========================================
    
    prev_start_date = None
    prev_end_date = None
    
    if show_comparison:
        st.markdown("**Compare To:**")
        
        comparison_col1, comparison_col2 = st.columns(2)
        
        with comparison_col1:
            comparison_type = st.radio(
                "Comparison Period",
                options=["Previous Period", "Week over Week", "Month over Month", "Year over Year", "No Comparison"],
                index=0,
                key=f"{key}_comparison_type",
                horizontal=False,
                label_visibility="collapsed",
                help="""
                - Previous Period: Compare to the period immediately before (e.g., if current is Jan 15-30, compare to Dec 31-Jan 14)
                - Week over Week: Compare to same period 7 days earlier
                - Month over Month: Compare to same period 30 days earlier
                - Year over Year: Compare to same period 365 days earlier
                - No Comparison: Don't show comparison metrics
                """
            )
        
        with comparison_col2:
            # Calculate comparison period based on selection
            period_length = (end_date - start_date).days
            
            if comparison_type == "Previous Period":
                prev_end_date = start_date - timedelta(days=1)
                prev_start_date = prev_end_date - timedelta(days=period_length)
            elif comparison_type == "Week over Week":
                prev_start_date = start_date - timedelta(days=7)
                prev_end_date = end_date - timedelta(days=7)
            elif comparison_type == "Month over Month":
                prev_start_date = start_date - timedelta(days=30)
                prev_end_date = end_date - timedelta(days=30)
            elif comparison_type == "Year over Year":
                prev_start_date = start_date - timedelta(days=365)
                prev_end_date = end_date - timedelta(days=365)
            else:  # No Comparison
                prev_start_date = None
                prev_end_date = None
            
            # Display comparison period info
            if prev_start_date and prev_end_date:
                st.info(
                    f"**Comparison Period:**\n\n{prev_start_date.strftime('%Y-%m-%d')} → {prev_end_date.strftime('%Y-%m-%d')}",
                    icon="📊"
                )
    
    # ========================================
    # Row 4: Summary Information
    # ========================================
    
    # Calculate and display date range summary
    days_selected = (end_date - start_date).days + 1  # Include both start and end dates
    
    summary_col1, summary_col2 = st.columns(2)
    
    with summary_col1:
        st.info(
            f"**Selected Period:** {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')} ({days_selected} days)",
            icon="📅"
        )
    
    with summary_col2:
        if show_comparison and prev_start_date and prev_end_date:
            comparison_days = (prev_end_date - prev_start_date).days + 1
            st.info(
                f"**Comparing To:** {prev_start_date.strftime('%Y-%m-%d')} → {prev_end_date.strftime('%Y-%m-%d')} ({comparison_days} days)",
                icon="📊"
            )
    
    # Return dates
    return start_date, end_date, prev_start_date, prev_end_date


def get_date_range_sql_filter(
    start_date: date,
    end_date: date,
    date_column: str = "date",
    date_format: str = "YYYY-MM-DD"
) -> str:
    """
    Generate SQL WHERE clause for date filtering.
    
    Args:
        start_date: Start date
        end_date: End date
        date_column: Name of the date column in SQL (default: "date")
        date_format: Expected date format in the column (default: "YYYY-MM-DD")
    
    Returns:
        SQL WHERE clause string (without the WHERE keyword)
    
    Example:
        filter_sql = get_date_range_sql_filter(start, end, "date_day")
        query = f"SELECT * FROM table WHERE {filter_sql}"
    
    Note:
        - Handles different date formats (YYYYMMDD, YYYY-MM-DD)
        - Ensures both start and end dates are inclusive
    """
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    if date_format == "YYYYMMDD":
        # Convert date to YYYYMMDD format (8-digit string)
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
    
    return f"{date_column} >= '{start_str}' AND {date_column} <= '{end_str}'"


def format_date_range_label(start_date: date, end_date: date) -> str:
    """
    Format a date range as a readable label.
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        Formatted string like "Jan 1 - Jan 31, 2024" or "Dec 25, 2023 - Jan 5, 2024"
    
    Example:
        label = format_date_range_label(date(2024, 1, 1), date(2024, 1, 31))
        # Returns: "Jan 1 - 31, 2024"
    """
    
    # Same month and year
    if start_date.month == end_date.month and start_date.year == end_date.year:
        return f"{start_date.strftime('%b %d')} - {end_date.strftime('%d, %Y')}"
    
    # Same year, different month
    elif start_date.year == end_date.year:
        return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    
    # Different year
    else:
        return f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"


def calculate_delta_percentage(current: float, previous: float) -> Optional[float]:
    """
    Calculate percentage change between two values.
    
    Args:
        current: Current period value
        previous: Previous period value
    
    Returns:
        Percentage change (e.g., 15.5 for 15.5% increase), or None if cannot calculate
    
    Example:
        delta = calculate_delta_percentage(150, 100)  # Returns: 50.0 (50% increase)
        delta = calculate_delta_percentage(80, 100)   # Returns: -20.0 (20% decrease)
    """
    
    if previous is None or previous == 0:
        return None
    
    if current is None:
        return None
    
    return ((current - previous) / abs(previous)) * 100
