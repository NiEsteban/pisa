from typing import Optional
import pandas as pd
import numpy as np
import re
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns
# -------------------------
# Cleaning functions
# -------------------------
# -------------------------
# Cleaning functions
# -------------------------
def clean_all_names_and_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans all string values and column headers in the DataFrame.
    - Removes quotes, commas, backslashes, tabs, and newlines.
    - Preserves existing NaN values.
    
    Args:
        dataframe (pd.DataFrame): The input pandas DataFrame.
        
    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    
    # Clean column names
    dataframe.columns = (
        dataframe.columns
        .str.replace("'", " ", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("\"", "", regex=False)
        .str.replace("\\", "", regex=False)
        .str.replace('\n', ' ', regex=False)
        .str.replace('\r', ' ', regex=False)
        .str.replace('\t', ' ', regex=False)
        .str.strip()
    )
    
    # Clean string/object values in all columns
    for col in dataframe.select_dtypes(include=['object', 'category']):
        dataframe[col] = dataframe[col].apply(lambda x: (
            str(x)
            .replace("\\", "")
            .replace("'", " ")
            .replace(",", "")
            .replace("\"", "")
            .replace('\n', ' ')
            .replace('\r', ' ')
            .replace('\t', ' ')
            .strip()
        ) if pd.notna(x) else np.nan)
    
    return dataframe


def clean_large_values(dataframe: pd.DataFrame, keep_ids_list=None) -> pd.DataFrame:
    """
    Replaces values greater than 9990.0 with NaN (Missing).
    These are typically error codes in SPSS/PISA data (e.g., 9999).
    
    Args:
        dataframe (pd.DataFrame): Input data.
        keep_ids_list (list): List of column names (IDs) to exclude from this check.
        
    Returns:
        pd.DataFrame: Data with outliers set to NaN.
    """
    if keep_ids_list is None:
        keep_ids_list = []
        
    return dataframe.apply(
        lambda col: col.mask(col > 9990.0)
        if pd.api.types.is_numeric_dtype(col) and col.name not in keep_ids_list
        else col
    )

def drop_highly_uniform_columns(dataframe: pd.DataFrame, threshold: float = 1.0) -> list:
    """
    Identifies and drops columns that have too little variation (too uniform).
    
    Args:
        dataframe (pd.DataFrame): Input data.
        threshold (float): Proportion (0.0 to 1.0). If the most frequent value occurs
                           more than this ratio, the column is dropped.
                           
    Returns:
        list: List of dropped column names.
    """
    dropped_cols = []

    for col in dataframe.columns:
        # Count only non-NaN values
        valid_count = dataframe[col].notna().sum()
        if valid_count == 0:
            continue
            
        top_freq_count = dataframe[col].value_counts(dropna=True).max()
        freq_ratio = top_freq_count / valid_count
        
        if freq_ratio >= threshold:
            dropped_cols.append(col)

    dataframe.drop(columns=dropped_cols, inplace=True)
    return dropped_cols


def replace_missing_invalid(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces common string representations of missing data ('Missing', 'Invalid', 'N/A')
    with the actual NumPy NaN value.
    """
    for col in dataframe.select_dtypes(['category']):
        # Replace "Missing", "Invalid", "N/A" with np.nan, preserving categories
        mask_invalid = dataframe[col].isin(["Missing", "Invalid", "N/A"])
        dataframe.loc[mask_invalid, col] = np.nan
        
        # Remove unused categories
        dataframe[col] = dataframe[col].cat.remove_unused_categories()
    return dataframe

def sanitize_newlines(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Escapes newline characters in string columns to prevent CSV formatting issues.
    Replaces '\n' with '\\n'.
    """
    for col in dataframe.select_dtypes(include=['object', 'category']):
        dataframe[col] = dataframe[col].apply(
            lambda x: x.replace('\n', '\\n').replace('\r', '\\n').strip() if pd.notna(x) else np.nan
        )
    return dataframe

def enforce_numeric_or_category(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Attempts to convert object columns to numeric types.
    If conversion fails (non-numeric characters present), converts to Category type.
    This reduces memory usage and standardizes types.
    """
    for col in dataframe.columns:
        # Try to convert to numeric
        numeric_series = pd.to_numeric(dataframe[col], errors='coerce')
        
        # If any non-NaN value failed conversion (meaning it was a real string), treat as category
        # But if the original was NOT NaN, and the numeric result IS NaN, then it wasn't a number.
        
        is_mixed = (dataframe[col].notna() & numeric_series.isna()).any()
        
        if is_mixed:
            dataframe[col] = dataframe[col].astype('category')
        else:
            dataframe[col] = numeric_series
            
    return dataframe


def drop_columns_with_missing_threshold(dataframe: pd.DataFrame, threshold: float) -> list:
    """
    Drops columns that have too many missing (NaN) values.
    
    Args:
        dataframe (pd.DataFrame): Input data.
        threshold (float): Max allowed ratio of missing values (e.g., 0.9 = 90% missing).
        
    Returns:
        list: List of dropped column names.
    """
    missing_ratio = dataframe.isna().mean()
    cols_to_drop = missing_ratio[missing_ratio > threshold].index.tolist()
    dataframe.drop(columns=cols_to_drop, inplace=True)
    return cols_to_drop


def drop_correlated_columns(dataframe: pd.DataFrame, threshold: float = 1.0, target_column: Optional[str] = None) -> tuple[pd.DataFrame, list]:
    """
    Identifies and drops one column from each pair of highly correlated features.
    
    Args:
        dataframe (pd.DataFrame): Input data.
        threshold (float): Pearson correlation coefficient threshold (e.g., 0.95).
        target_column (str, optional): Name of the target variable.
                                       If provided, the feature less correlated with the target
                                       will be dropped in a correlated pair.
                                       
    Returns:
        tuple: (Cleaned DataFrame, List of dropped column names)
    """
    try:
        # Encode string/object columns as numeric for correlation calculation
        df_encoded, _ = encode_nominal_features(dataframe)

        # Select only numeric columns
        numeric_dataframe = select_numeric_features(df_encoded, target_column)

        if numeric_dataframe.empty:
            return dataframe, []

        # Compute correlation matrix
        corr_matrix = compute_correlation_matrix(numeric_dataframe)

        # Find highly correlated pairs
        correlated_pairs_list = find_highly_correlated_pairs(corr_matrix, threshold)

        # Drop one column from each pair
        dropped_columns_set = set()
        for col_a, col_b, _ in correlated_pairs_list:
            if target_column and target_column in numeric_dataframe.columns:
                # Drop the column with lower correlation to the target
                correlation_a = abs(numeric_dataframe[col_a].corr(numeric_dataframe[target_column]))
                correlation_b = abs(numeric_dataframe[col_b].corr(numeric_dataframe[target_column]))
                if correlation_a < correlation_b:
                    dropped_columns_set.add(col_a)
                else:
                    dropped_columns_set.add(col_b)
            else:
                # Default: Drop the second column in the pair
                dropped_columns_set.add(col_b)

        # Drop the selected columns from the original dataframe
        dataframe_dropped = dataframe.drop(columns=dropped_columns_set, errors="ignore")

        return dataframe_dropped, list(dropped_columns_set)
    except Exception as error:
        print(f"Error in drop_correlated_columns: {error}")
        return dataframe, []


def encode_nominal_features(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, LabelEncoder]]:
    """Convert string/object columns to numeric labels for analysis."""
    df_encoded = dataframe.copy()
    label_encoders_map = {}
    for col_name in df_encoded.select_dtypes(include=["object", "category"]).columns:
        encoder = LabelEncoder()
        df_encoded[col_name] = encoder.fit_transform(df_encoded[col_name].astype(str))
        label_encoders_map[col_name] = encoder
    return df_encoded, label_encoders_map

def select_numeric_features(dataframe: pd.DataFrame, target_to_exclude: Optional[str] = None) -> pd.DataFrame:
    """Return numeric columns, optionally excluding the target variable."""
    numeric_df = dataframe.select_dtypes(include=[np.number])
    if target_to_exclude and target_to_exclude in numeric_df.columns:
        numeric_df = numeric_df.drop(columns=[target_to_exclude])
    return numeric_df

def compute_correlation_matrix(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Compute Pearson correlation matrix (absolute values)."""
    return dataframe.corr().abs()

def find_highly_correlated_pairs(corr_matrix: pd.DataFrame, threshold: float = 0.8) -> list[tuple[str, str, float]]:
    """Return list of (feature1, feature2, correlation) tuples where |corr| > threshold."""
    pairs_list = []
    # Iterate over lower triangle of the matrix to avoid duplicates
    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            correlation_value = corr_matrix.iloc[i, j]
            if abs(correlation_value) > threshold:
                pairs_list.append(
                    (corr_matrix.columns[i], corr_matrix.columns[j], correlation_value)
                )
    return pairs_list

# -------------------------
# Main class
# -------------------------
class CSVCleaner:
    """
    Main class for cleaning PISA data files.
    Applies a sequence of cleaning steps: 
    Basic cleanup -> Large value removal -> Correlation filter -> Uniformity filter -> Missing filter.
    """
    def __init__(self):
        """Empty constructor; the file to clean is passed to run()"""
        pass 
        
    def run(
        self,
        dataframe: pd.DataFrame,
        filename_label: str,
        protected_ids_list: Optional[list] = None,
        missing_threshold: float = 1.0,
        uniform_threshold: float = 1.0,
        correlation_threshold: float = 1.0,
        target_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Clean a single CSV file and save the result in Weka-safe format.
        
        Args:
            dataframe (pd.DataFrame): The raw data.
            filename_label (str): Label for logging (usually filename).
            protected_ids_list (list, optional): Columns that should NOT be touched/dropped.
            missing_threshold (float): Threshold to drop sparse columns.
            uniform_threshold (float): Threshold to drop uniform columns.
            correlation_threshold (float): Threshold to drop duplicate columns.
            target_column (str, optional): Target variable to preserve during correlation filtering.
            
        Returns:
            pd.DataFrame: The fully cleaned dataframe.
        """
        print(f"[CLEANER] Cleaning {filename_label}")

        try:
            # Clean formatting
            dataframe = clean_all_names_and_values(dataframe)
            dataframe = replace_missing_invalid(dataframe)
            dataframe = clean_large_values(dataframe, protected_ids_list)
            dataframe = sanitize_newlines(dataframe)
            dataframe = enforce_numeric_or_category(dataframe)

            # Drop duplicates based on correlation
            dataframe, dropped_correlated_list = drop_correlated_columns(dataframe, correlation_threshold, target_column)
            if dropped_correlated_list:
                print(f"[CLEANER] Dropped {len(dropped_correlated_list)} correlated columns (Corr > {correlation_threshold})")

            # Drop uniform (little variation)
            dropped_uniform_list = drop_highly_uniform_columns(dataframe, uniform_threshold)
            if dropped_uniform_list:
                print(f"[CLEANER] Dropped {len(dropped_uniform_list)} uniform columns (Uniform > {uniform_threshold*100}%)")

            # Drop missing (sparse)
            dropped_missing_list = drop_columns_with_missing_threshold(dataframe, missing_threshold)
            if dropped_missing_list:
                print(f"[CLEANER] Dropped {len(dropped_missing_list)} sparse columns (Missing > {missing_threshold*100}%)")

        except Exception as error:
            print(f"[ERROR] Cleaning failed: {error}")

        return dataframe

