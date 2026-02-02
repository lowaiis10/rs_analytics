"""
Base Classes for ML Models in rs_analytics

This module provides abstract base classes for all ML models,
ensuring consistent interfaces and behavior.

Classes:
- BaseModel: Abstract base for all models
- TimeSeriesModel: Base for time series models
- ClassificationModel: Base for classification models
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


logger = logging.getLogger(__name__)


class BaseModel(ABC):
    """
    Abstract base class for all ML models.
    
    Provides common functionality:
    - Model metadata tracking
    - Save/load functionality
    - Logging integration
    """
    
    def __init__(self, name: str, version: str = "1.0.0"):
        """
        Initialize the base model.
        
        Args:
            name: Model name identifier
            version: Model version string
        """
        self.name = name
        self.version = version
        self.created_at = datetime.now()
        self.trained_at: Optional[datetime] = None
        self.logger = logging.getLogger(f"model.{name}")
        self._is_fitted = False
    
    @property
    def is_fitted(self) -> bool:
        """Check if model has been fitted/trained."""
        return self._is_fitted
    
    @abstractmethod
    def fit(self, data: pd.DataFrame, **kwargs) -> "BaseModel":
        """
        Train/fit the model on data.
        
        Args:
            data: Training data
            **kwargs: Additional training parameters
            
        Returns:
            Self for method chaining
        """
        raise NotImplementedError
    
    @abstractmethod
    def predict(self, data: pd.DataFrame, **kwargs) -> Any:
        """
        Make predictions using the trained model.
        
        Args:
            data: Input data for prediction
            **kwargs: Additional prediction parameters
            
        Returns:
            Model predictions
        """
        raise NotImplementedError
    
    def save(self, path: Union[str, Path]) -> None:
        """
        Save model to disk.
        
        Args:
            path: Path to save the model
        """
        import pickle
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'wb') as f:
            pickle.dump(self, f)
        
        self.logger.info(f"Model saved to {path}")
    
    @classmethod
    def load(cls, path: Union[str, Path]) -> "BaseModel":
        """
        Load model from disk.
        
        Args:
            path: Path to the saved model
            
        Returns:
            Loaded model instance
        """
        import pickle
        
        with open(path, 'rb') as f:
            model = pickle.load(f)
        
        return model
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get model metadata.
        
        Returns:
            Dictionary with model metadata
        """
        return {
            'name': self.name,
            'version': self.version,
            'created_at': self.created_at.isoformat(),
            'trained_at': self.trained_at.isoformat() if self.trained_at else None,
            'is_fitted': self._is_fitted,
        }


class TimeSeriesModel(BaseModel):
    """
    Base class for time series models.
    
    Adds time series specific functionality:
    - Date column handling
    - Forecasting interface
    - Seasonality detection
    """
    
    def __init__(
        self,
        name: str,
        date_column: str = "date",
        value_column: str = "value",
        version: str = "1.0.0"
    ):
        """
        Initialize time series model.
        
        Args:
            name: Model name
            date_column: Name of date column in data
            value_column: Name of value column to forecast
            version: Model version
        """
        super().__init__(name, version)
        self.date_column = date_column
        self.value_column = value_column
    
    @abstractmethod
    def forecast(
        self,
        periods: int,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate forecast for future periods.
        
        Args:
            periods: Number of periods to forecast
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with forecasted values
        """
        raise NotImplementedError
    
    def _validate_time_series_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and prepare time series data.
        
        Args:
            data: Input data
            
        Returns:
            Validated DataFrame
            
        Raises:
            ValueError: If required columns missing
        """
        if self.date_column not in data.columns:
            raise ValueError(f"Date column '{self.date_column}' not found")
        
        if self.value_column not in data.columns:
            raise ValueError(f"Value column '{self.value_column}' not found")
        
        # Ensure date column is datetime
        data = data.copy()
        data[self.date_column] = pd.to_datetime(data[self.date_column])
        
        # Sort by date
        data = data.sort_values(self.date_column)
        
        return data


class AnomalyDetectionModel(BaseModel):
    """
    Base class for anomaly detection models.
    
    Adds anomaly detection specific functionality:
    - Threshold configuration
    - Anomaly scoring
    - Explanation generation
    """
    
    def __init__(
        self,
        name: str,
        threshold: float = 0.95,
        version: str = "1.0.0"
    ):
        """
        Initialize anomaly detection model.
        
        Args:
            name: Model name
            threshold: Anomaly threshold (0-1)
            version: Model version
        """
        super().__init__(name, version)
        self.threshold = threshold
    
    @abstractmethod
    def detect(
        self,
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Detect anomalies in data.
        
        Args:
            data: Input data
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with anomaly indicators
        """
        raise NotImplementedError
    
    @abstractmethod
    def score(
        self,
        data: pd.DataFrame,
        **kwargs
    ) -> pd.Series:
        """
        Generate anomaly scores for data.
        
        Args:
            data: Input data
            **kwargs: Additional parameters
            
        Returns:
            Series of anomaly scores (higher = more anomalous)
        """
        raise NotImplementedError
