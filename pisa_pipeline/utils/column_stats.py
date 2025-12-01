"""
Column statistics analyzer for PISA pipeline.
Handles computation of statistics for individual columns.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Union
from collections import Counter


class ColumnStats:
    """Compute statistics for a single column"""
    
    @staticmethod
    def analyze_column(column_data: pd.Series, column_name: str) -> Dict[str, Any]:
        """
        Analyze a column and return comprehensive statistics.
        
        Args:
            column_data: pandas Series containing the column data
            column_name: name of the column
            
        Returns:
            Dictionary with statistics and metadata
        """
        stats = {
            "name": column_name,
            "dtype": str(column_data.dtype),
            "total_count": len(column_data),
            "missing_count": int(column_data.isna().sum()),
            "missing_percentage": float(column_data.isna().sum() / len(column_data) * 100),
            "non_missing_count": int(column_data.notna().sum()),
        }
        
        # Determine column type and compute appropriate statistics
        if pd.api.types.is_numeric_dtype(column_data):
            stats.update(ColumnStats._analyze_numeric(column_data))
        else:
            stats.update(ColumnStats._analyze_categorical(column_data))
        
        return stats
    
    @staticmethod
    def _analyze_numeric(column_data: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column (int/float)"""
        clean_data = column_data.dropna()
        
        if len(clean_data) == 0:
            return {
                "type": "numeric",
                "mean": None,
                "std": None,
                "variance": None,
                "min": None,
                "max": None,
                "q25": None,
                "median": None,
                "q75": None,
                "unique_count": 0,
            }
        
        return {
            "type": "numeric",
            "mean": float(clean_data.mean()),
            "std": float(clean_data.std()),
            "variance": float(clean_data.var()),
            "min": float(clean_data.min()),
            "max": float(clean_data.max()),
            "q25": float(clean_data.quantile(0.25)),
            "median": float(clean_data.median()),
            "q75": float(clean_data.quantile(0.75)),
            "unique_count": int(clean_data.nunique()),
            "histogram_data": ColumnStats._compute_histogram(clean_data),
        }
    
    @staticmethod
    def _analyze_categorical(column_data: pd.Series) -> Dict[str, Any]:
        """Analyze categorical/string column"""
        clean_data = column_data.dropna()
        
        if len(clean_data) == 0:
            return {
                "type": "categorical",
                "unique_count": 0,
                "value_counts": {},
            }
        
        # Get value counts
        value_counts = clean_data.value_counts()
        
        # Limit to top 20 values if too many
        if len(value_counts) > 20:
            top_values = value_counts.head(20).to_dict()
            other_count = value_counts.iloc[20:].sum()
            top_values["__OTHER__"] = int(other_count)
        else:
            top_values = value_counts.to_dict()
        
        # Convert to regular Python types for JSON serialization
        top_values = {str(k): int(v) for k, v in top_values.items()}
        
        return {
            "type": "categorical",
            "unique_count": int(clean_data.nunique()),
            "most_common": str(value_counts.index[0]) if len(value_counts) > 0 else None,
            "most_common_count": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
            "value_counts": top_values,
        }
    
    @staticmethod
    def _compute_histogram(data: pd.Series, bins: int = 30) -> Dict[str, Any]:
        """Compute histogram data for numeric columns"""
        try:
            counts, bin_edges = np.histogram(data, bins=bins)
            return {
                "counts": counts.tolist(),
                "bin_edges": bin_edges.tolist(),
                "bins": bins,
            }
        except Exception:
            return {
                "counts": [],
                "bin_edges": [],
                "bins": 0,
            }


class ColumnStatsLoader:
    """Load specific columns from files efficiently"""
    
    @staticmethod
    def load_column(file_path: str, column_name: str, encoding: str = "cp1252") -> Optional[pd.Series]:
        """
        Load a single column from a CSV file efficiently.
        
        Args:
            file_path: path to CSV file
            column_name: name of column to load
            encoding: file encoding
            
        Returns:
            pandas Series or None if column not found
        """
        try:
            # Read only the specific column
            df = pd.read_csv(file_path, usecols=[column_name], encoding=encoding)
            return df[column_name]
        except ValueError:
            # Column not found
            print(f"Column '{column_name}' not found in {file_path}")
            return None
        except Exception as e:
            print(f"Error loading column '{column_name}': {e}")
            return None
    
    @staticmethod
    def load_column_from_dataframe(df: pd.DataFrame, column_name: str) -> Optional[pd.Series]:
        """
        Extract a column from an existing DataFrame.
        
        Args:
            df: pandas DataFrame
            column_name: name of column to extract
            
        Returns:
            pandas Series or None if column not found
        """
        if column_name in df.columns:
            return df[column_name]
        else:
            print(f"Column '{column_name}' not found in DataFrame")
            return None
    
    @staticmethod
    def get_column_list(file_path: str, encoding: str = "cp1252") -> list:
        """
        Get list of column names without loading full dataset.
        
        Args:
            file_path: path to CSV file
            encoding: file encoding
            
        Returns:
            List of column names
        """
        try:
            # Read only first row to get column names
            df = pd.read_csv(file_path, nrows=0, encoding=encoding)
            return df.columns.tolist()
        except Exception as e:
            print(f"Error reading columns from {file_path}: {e}")
            return []


class ColumnStatsFactory:
    """Factory to create column statistics from various sources"""
    
    @staticmethod
    def from_file(file_path: str, column_name: str, encoding: str = "cp1252") -> Optional[Dict[str, Any]]:
        """
        Load column from file and compute statistics.
        
        Args:
            file_path: path to CSV file
            column_name: name of column to analyze
            encoding: file encoding
            
        Returns:
            Statistics dictionary or None if error
        """
        column_data = ColumnStatsLoader.load_column(file_path, column_name, encoding)
        if column_data is not None:
            return ColumnStats.analyze_column(column_data, column_name)
        return None
    
    @staticmethod
    def from_dataframe(df: pd.DataFrame, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Extract column from DataFrame and compute statistics.
        
        Args:
            df: pandas DataFrame
            column_name: name of column to analyze
            
        Returns:
            Statistics dictionary or None if error
        """
        column_data = ColumnStatsLoader.load_column_from_dataframe(df, column_name)
        if column_data is not None:
            return ColumnStats.analyze_column(column_data, column_name)
        return None
    
    @staticmethod
    def from_series(column_data: pd.Series, column_name: str) -> Dict[str, Any]:
        """
        Compute statistics from pandas Series directly.
        
        Args:
            column_data: pandas Series
            column_name: name of column
            
        Returns:
            Statistics dictionary
        """
        return ColumnStats.analyze_column(column_data, column_name)