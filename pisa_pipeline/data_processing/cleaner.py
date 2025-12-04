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
def clean_all_names_and_values(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names and all string values in the DataFrame, preserving NaNs."""
    
    # Clean column names
    df.columns = (
        df.columns
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
    for col in df.select_dtypes(include=['object', 'category']):
        df[col] = df[col].apply(lambda x: (
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
    
    return df


def clean_large_values(df: pd.DataFrame, keep_ids=None) -> pd.DataFrame:
    """Replace values > 9990 with NaN, except for ID columns."""
    if keep_ids is None:
        keep_ids = []
    return df.apply(
        lambda col: col.mask(col > 9990.0)
        if pd.api.types.is_numeric_dtype(col) and col.name not in keep_ids
        else col
    )

def drop_highly_uniform_columns(df: pd.DataFrame, threshold: float = 1.0) -> list:
    """
    Drop columns where the most frequent value (ignoring NaNs) represents
    more than `threshold` proportion of non-missing values.
    """
    dropped_cols = []

    for col in df.columns:
        # Count only non-NaN values
        top_freq = df[col].value_counts(dropna=True).max() / df[col].notna().sum()
        if top_freq >= threshold:
            dropped_cols.append(col)

    df.drop(columns=dropped_cols, inplace=True)
    return dropped_cols


def replace_missing_invalid(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(['category']):
        # Replace "Missing", "Invalid", "N/A" with np.nan, preserving categories
        mask_invalid = df[col].isin(["Missing", "Invalid", "N/A"])
        df.loc[mask_invalid, col] = np.nan
        
        # Remove unused categories
        df[col] = df[col].cat.remove_unused_categories()
    return df

def sanitize_newlines(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object', 'category']):
        df[col] = df[col].apply(
            lambda x: x.replace('\n', '\\n').replace('\r', '\\n').strip() if pd.notna(x) else np.nan
        )
    return df

def enforce_numeric_or_category(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        # Try to convert to numeric
        numeric_col = pd.to_numeric(df[col], errors='coerce')
        # If any non-NaN value failed conversion, treat as category
        if (df[col].notna() & numeric_col.isna()).any():
            df[col] = df[col].astype('category')
        else:
            df[col] = numeric_col
    return df




def drop_columns_with_missing_threshold(df: pd.DataFrame, threshold: float) -> list:
    missing_ratio = df.isna().mean()
    cols_to_drop = missing_ratio[missing_ratio > threshold].index.tolist()
    df.drop(columns=cols_to_drop, inplace=True)
    return cols_to_drop


def drop_correlated_columns(df: pd.DataFrame, threshold: float = 1.0, target: Optional[str] = None) -> tuple[pd.DataFrame, list]:
    """
    Drop one column from each highly correlated pair.
    - Only numeric columns are considered.
    - If target is provided, the column with lower correlation to the target is dropped.
    - Otherwise, the second column in the pair is dropped.
    """
    try:
        # Encode string/object columns as numeric
        df_encoded, _ = encode_nominal_features(df)

        # Select only numeric columns
        numeric_df = select_numeric_features(df_encoded, target)

        if numeric_df.empty:
            return df, []

        # Compute correlation matrix
        corr_matrix = compute_correlation_matrix(numeric_df)

        # Find highly correlated pairs
        correlated_pairs = find_highly_correlated_pairs(corr_matrix, threshold)

        # Drop one column from each pair
        cols_to_drop = set()
        for a, b, _ in correlated_pairs:
            if target and target in numeric_df.columns:
                # Drop the column with lower correlation to the target
                corr_a = abs(numeric_df[a].corr(numeric_df[target]))
                corr_b = abs(numeric_df[b].corr(numeric_df[target]))
                if corr_a < corr_b:
                    cols_to_drop.add(a)
                else:
                    cols_to_drop.add(b)
            else:
                # Drop the second column in the pair
                cols_to_drop.add(b)

        # Drop the selected columns
        df_dropped = df.drop(columns=cols_to_drop, errors="ignore")

        return df_dropped, list(cols_to_drop)
    except Exception as e:
        print(f"Error in drop_correlated_columns: {e}")
        return df, []


def encode_nominal_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, LabelEncoder]]:
    """Convert string/object columns to numeric labels."""
    df_encoded = df.copy()
    label_encoders = {}
    for col in df_encoded.select_dtypes(include=["object", "category"]).columns:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
        label_encoders[col] = le
    return df_encoded, label_encoders

def select_numeric_features(df: pd.DataFrame, target: Optional[str] = None) -> pd.DataFrame:
    """Return numeric columns, optionally excluding the target."""
    numeric_df = df.select_dtypes(include=[np.number])
    if target and target in numeric_df.columns:
        numeric_df = numeric_df.drop(columns=[target])
    return numeric_df

def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Pearson correlation matrix."""
    return df.corr().abs()

def find_highly_correlated_pairs(corr_matrix: pd.DataFrame, threshold: float = 0.8) -> list[tuple[str, str, float]]:
    """Return list of (feature1, feature2, correlation) with |corr| > threshold."""
    correlated_pairs = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            corr_value = corr_matrix.iloc[i, j]
            if abs(corr_value) > threshold:
                correlated_pairs.append(
                    (corr_matrix.columns[i], corr_matrix.columns[j], corr_value)
                )
    return correlated_pairs

# -------------------------
# Main class
# -------------------------
class CSVCleaner:
    def __init__(self):
        """Empty constructor; the file to clean is passed to run()"""
        pass 
    def run(
        self,
        df: pd.DataFrame,
        filename: str,
        ids: Optional[list] = None,
        missing_threshold: float = 1.0,
        uniform_threshold: float = 1.0,
        correlation_threshold: float = 1.0,
        target: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Clean a single CSV file and save the result in Weka-safe format.
        - Drops columns with missing values above `missing_threshold`.
        - Drops columns with uniform values above `uniform_threshold`.
        - Drops duplicated columns (correlation_threshold = 1.0).
        - Optionally, uses `target` for correlation-based dropping.
        """
        print(f"[CLEANER] Cleaning {filename}")

        try:
            # Clean column names and values
            df = clean_all_names_and_values(df)
            df = replace_missing_invalid(df)
            df = clean_large_values(df, ids)
            df = sanitize_newlines(df)
            df = enforce_numeric_or_category(df)

            # Drop duplicated columns (correlation_threshold = 1.0)
            df, dropped_correlated = drop_correlated_columns(df, correlation_threshold, target)
            if dropped_correlated:
                print(f"[CLEANER] Dropped {len(dropped_correlated)} correlated columns: {dropped_correlated}")

            # Drop uniform columns
            dropped_uniform = drop_highly_uniform_columns(df, uniform_threshold)
            if dropped_uniform:
                print(f"[CLEANER] Dropped {len(dropped_uniform)} columns for uniform values: {dropped_uniform}")

            # Drop columns with missing values
            dropped_missing = drop_columns_with_missing_threshold(df, missing_threshold)
            if dropped_missing:
                print(f"[CLEANER] Dropped {len(dropped_missing)} columns for missing values: {dropped_missing}")

        except Exception as e:
            print(f"[ERROR] Error during cleaning process: {e}")

        return df

