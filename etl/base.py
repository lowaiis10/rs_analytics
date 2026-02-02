"""
Base Extractor Class for rs_analytics

This module provides the base class and common functionality for all data extractors.
All platform-specific extractors (GA4, GSC, Google Ads, Meta, Twitter) should extend
this base class to ensure consistent behavior and reduce code duplication.

Features:
- Standardized logger initialization
- Common test_connection() interface
- Extraction summary logging
- Timestamp handling for extracted records
- Error handling patterns

Usage:
    from etl.base import BaseExtractor
    
    class MyExtractor(BaseExtractor):
        def test_connection(self) -> tuple[bool, str]:
            # Platform-specific implementation
            pass
        
        def extract_all(self, start_date: str, end_date: str) -> dict:
            # Platform-specific implementation
            pass
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.
    
    Provides common functionality:
    - Logger initialization with optional custom logger
    - Standardized test_connection() interface
    - Extraction summary logging
    - Timestamp addition to records
    
    Subclasses must implement:
    - test_connection(): Verify API connectivity
    - extract_all(): Extract all available data
    """
    
    def __init__(
        self,
        source_name: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the base extractor.
        
        Args:
            source_name: Name of the data source (e.g., 'ga4', 'gads', 'meta')
            logger: Optional custom logger instance. If not provided,
                   creates a logger with the class name.
        """
        self.source_name = source_name
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._extraction_start_time: Optional[datetime] = None
    
    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to the data source.
        
        This method should perform a minimal API call to verify:
        1. Credentials are valid
        2. API is accessible
        3. Account/property access is granted
        
        Returns:
            Tuple of (success: bool, message: str)
            - success: True if connection test passed
            - message: Descriptive message about the connection status
        """
        raise NotImplementedError("Subclasses must implement test_connection()")
    
    @abstractmethod
    def extract_all(
        self,
        start_date: str,
        end_date: str,
        **kwargs
    ) -> Union[Dict[str, List[Dict[str, Any]]], Dict[str, pd.DataFrame]]:
        """
        Extract all available data from the source.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            **kwargs: Additional platform-specific parameters
            
        Returns:
            Dictionary mapping dataset names to data (list of dicts or DataFrame)
        """
        raise NotImplementedError("Subclasses must implement extract_all()")
    
    def _start_extraction(self) -> None:
        """Mark the start of an extraction run."""
        self._extraction_start_time = datetime.now()
        self.logger.info(f"Starting extraction from {self.source_name}")
    
    def _get_extracted_at(self) -> str:
        """
        Get the current timestamp for the extracted_at field.
        
        Returns:
            ISO format timestamp string
        """
        return datetime.now().isoformat()
    
    def _add_extracted_at_to_records(
        self,
        records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Add extracted_at timestamp to a list of records.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Records with extracted_at field added
        """
        timestamp = self._get_extracted_at()
        for record in records:
            record['extracted_at'] = timestamp
        return records
    
    def _add_extracted_at_to_dataframe(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Add extracted_at timestamp to a DataFrame.
        
        Args:
            df: DataFrame to modify
            
        Returns:
            DataFrame with extracted_at column added
        """
        df['extracted_at'] = self._get_extracted_at()
        return df
    
    def _log_extraction_summary(
        self,
        data: Union[Dict[str, List], Dict[str, pd.DataFrame]],
        dataset_names: Optional[List[str]] = None
    ) -> None:
        """
        Log a summary of the extraction results.
        
        Args:
            data: Dictionary of dataset_name -> data (list or DataFrame)
            dataset_names: Optional list of dataset names to include in summary
        """
        # Calculate totals
        if dataset_names is None:
            dataset_names = list(data.keys())
        
        total_rows = 0
        total_datasets = len(dataset_names)
        
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info(f"{self.source_name.upper()} EXTRACTION SUMMARY")
        self.logger.info("=" * 60)
        
        for name in dataset_names:
            if name in data:
                dataset = data[name]
                row_count = len(dataset) if isinstance(dataset, (list, pd.DataFrame)) else 0
                total_rows += row_count
                self.logger.info(f"  {name}: {row_count:,} rows")
        
        self.logger.info("-" * 60)
        self.logger.info(f"Total datasets: {total_datasets}")
        self.logger.info(f"Total rows: {total_rows:,}")
        
        # Log duration if available
        if self._extraction_start_time:
            duration = datetime.now() - self._extraction_start_time
            self.logger.info(f"Duration: {duration}")
        
        self.logger.info("=" * 60)
    
    def _log_dataset_start(self, dataset_name: str, description: str = "") -> None:
        """
        Log the start of a dataset extraction.
        
        Args:
            dataset_name: Name of the dataset being extracted
            description: Optional description of the dataset
        """
        self.logger.info("")
        self.logger.info("=" * 50)
        self.logger.info(f"Extracting: {dataset_name}")
        if description:
            self.logger.info(f"Description: {description}")
        self.logger.info("=" * 50)
    
    def _log_dataset_complete(
        self,
        dataset_name: str,
        row_count: int,
        success: bool = True
    ) -> None:
        """
        Log the completion of a dataset extraction.
        
        Args:
            dataset_name: Name of the dataset
            row_count: Number of rows extracted
            success: Whether extraction was successful
        """
        if success:
            self.logger.info(f"Successfully extracted {row_count:,} rows for {dataset_name}")
        else:
            self.logger.warning(f"No data returned for {dataset_name}")
    
    def _handle_extraction_error(
        self,
        dataset_name: str,
        error: Exception,
        continue_on_error: bool = True
    ) -> None:
        """
        Handle an extraction error consistently.
        
        Args:
            dataset_name: Name of the dataset that failed
            error: The exception that occurred
            continue_on_error: If True, log error and continue; if False, re-raise
        """
        self.logger.error(f"Failed to extract {dataset_name}: {error}")
        
        if not continue_on_error:
            raise


class ExtractionResult:
    """
    Container for extraction results with metadata.
    
    Provides a standardized way to return extraction results with
    additional metadata like row counts and extraction timestamps.
    """
    
    def __init__(
        self,
        source_name: str,
        start_date: str,
        end_date: str
    ):
        """
        Initialize an extraction result container.
        
        Args:
            source_name: Name of the data source
            start_date: Start date of the extraction
            end_date: End date of the extraction
        """
        self.source_name = source_name
        self.start_date = start_date
        self.end_date = end_date
        self.extracted_at = datetime.now().isoformat()
        self.datasets: Dict[str, Union[List[Dict], pd.DataFrame]] = {}
        self.row_counts: Dict[str, int] = {}
        self.errors: Dict[str, str] = {}
    
    def add_dataset(
        self,
        name: str,
        data: Union[List[Dict], pd.DataFrame]
    ) -> None:
        """
        Add a dataset to the results.
        
        Args:
            name: Dataset name
            data: Dataset data (list of dicts or DataFrame)
        """
        self.datasets[name] = data
        self.row_counts[name] = len(data) if isinstance(data, (list, pd.DataFrame)) else 0
    
    def add_error(self, dataset_name: str, error_message: str) -> None:
        """
        Record an error for a dataset.
        
        Args:
            dataset_name: Name of the dataset that failed
            error_message: Error message
        """
        self.errors[dataset_name] = error_message
    
    @property
    def total_rows(self) -> int:
        """Get total row count across all datasets."""
        return sum(self.row_counts.values())
    
    @property
    def success_count(self) -> int:
        """Get count of successfully extracted datasets."""
        return len(self.datasets)
    
    @property
    def error_count(self) -> int:
        """Get count of failed datasets."""
        return len(self.errors)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert results to a dictionary.
        
        Returns:
            Dictionary with metadata and datasets
        """
        return {
            'source_name': self.source_name,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'extracted_at': self.extracted_at,
            'total_rows': self.total_rows,
            'datasets': list(self.datasets.keys()),
            'row_counts': self.row_counts,
            'errors': self.errors,
        }
