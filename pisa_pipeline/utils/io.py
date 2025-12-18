import os
import csv
import pyreadstat
import numpy as np
import pandas as pd
from tkinter import messagebox
import os
import pandas as pd
from tkinter import messagebox
from typing import Dict, Any, List, Union

def save_top_x_to_excel(
    results: Dict[str, Any],
    output_dir: str,
    top_x: int,
    year: str = None,
    dataset_name: str = None,
    ids_col: List[str] = ["ID"],
    math_col: List[str] = ["MATH"]
) -> None:
    """
    Save top X results and a copy of the dataset (with only IDs and math level) to an Excel file.
    If the file exists, save to a new sheet named after the year.
    If the file doesn't exist, create it.

    Args:
        results: Dictionary containing the results and dataset.
        output_dir: Directory to save the Excel file.
        top_x: Number of top results to save.
        year: Year to use as sheet name (optional).
        dataset_name: Name of the dataset (optional).
        ids_col: List of ID column names.
        math_col: List of math level column names.
    """
    try:
        # Check if "overall_summary" exists and is a DataFrame
        if "overall_summary" not in results or results["overall_summary"].empty:
            raise ValueError("No valid overall summary found in results.")

        # Extract top X results
        top_results = results["overall_summary"].head(top_x)

        # Extract dataset with only IDs and math level
        dataset = results["dataset"]

        # Ensure all columns exist in the dataset
        all_cols = ids_col + math_col
        missing_cols = [col for col in all_cols if col not in dataset.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in dataset: {missing_cols}")

        # Select only the ID and math columns
        dataset_shrunk = dataset[all_cols].copy()

        # Determine output file path
        output_file = os.path.join(output_dir, "results.xlsx")

        # If year is not provided, try to extract it from the dataset name or folder
        if not year:
            if dataset_name:
                year = dataset_name.split("_")[-1]
            else:
                year = "unknown_year"

        # Save to Excel
        with pd.ExcelWriter(output_file, engine="openpyxl", mode="a" if os.path.exists(output_file) else "w") as writer:
            top_results.to_excel(writer, sheet_name=f"Top_{top_x}_{year}", index=False)
            dataset_shrunk.to_excel(writer, sheet_name=f"IDs_Math_{year}", index=False)

        messagebox.showinfo("Success", f"Results saved to {output_file}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to save results: {e}")



def get_path(relative_path: str, script_file: str = __file__) -> str:
    """
    Convert a relative path (from project root) into an absolute path
    based on the location of a script file.
    
    Parameters:
        relative_path: str
            Path relative to the project or desired base folder
        script_file: str
            Reference script file, defaults to the caller file (__file__)
    
    Returns:
        str: absolute path
    """
    base_dir = os.path.dirname(os.path.abspath(script_file))
    abs_path = os.path.abspath(os.path.join(base_dir, relative_path))
    return abs_path

def ensure_folder(folder_path):
    """Create folder if it doesn't exist."""
    os.makedirs(folder_path, exist_ok=True)


# -------------------------
# CSV save/load utilities
# -------------------------
def load_sav_metadata(file_path):
        """Load only metadata from a SPSS .sav file."""
        _, meta = pyreadstat.read_sav(file_path, metadataonly=True, user_missing=True)
        return meta

def load_csvs_from_folder(input_folder: str):
    """Load all CSV files from a folder and return (dfs, filenames)."""
    dfs_dict = {}
    for filename in os.listdir(input_folder):
        if not filename.endswith(".csv"):
            continue
        infile = os.path.join(input_folder, filename)
        print(f"[IO] Loading {filename}...")
        dfs_dict[filename] = read_csv(infile)
    return dfs_dict

def save_dataframe_to_csv(df, output_path, verbose=True):
    """Save a DataFrame to CSV, creating directories if needed."""
    ensure_folder(os.path.dirname(output_path))
    df.to_csv(output_path, index=False,quoting=csv.QUOTE_ALL,escapechar='\\', encoding='cp1252')
    if verbose:
        print(f"[INFO] Saved CSV → {output_path}")
    return output_path

def save_csv_weka_safe(df: pd.DataFrame, filepath: str):
    """Save CSV in Weka-safe format, quoting all strings, preserving NaNs."""
    ensure_folder(os.path.dirname(filepath))
    df.to_csv(
        filepath,
        index=False,
        na_rep=np.nan,
        quoting=csv.QUOTE_ALL,
        escapechar='\\'
    )

def read_csv(csv_path, encoding='cp1252'):
    """Read a CSV file into a DataFrame."""
    return pd.read_csv(csv_path, encoding=encoding)

def save_results_with_mapping(
    all_results: Dict[str, Any],
    dataset: pd.DataFrame,
    output_dir: str,
    top_x: int,
    ids_col: List[str] = None
) -> str:
    """
    Save top X ranking summary and a filtered dataset to Excel.
    Performs robust column name cleaning and fuzzy matching between 
    ranking attributes and dataset columns.
    
    Args:
        all_results: Dictionary of results per dataset key.
        dataset: The full dataset dataframe.
        output_dir: Target directory.
        top_x: Number of top features to select.
        ids_col: List of ID columns to always include (optional).
    
    Returns:
        path to the saved file
    """
    import re
    from difflib import get_close_matches
    from pisa_pipeline.utils.algo_utils import detect_columns

    output_file = os.path.join(output_dir, "top_selection_results.xlsx")
    
    # helper: clean column name
    def clean_column_name(name: str) -> str:
        name = re.sub(r"(\\v|[\x00-\x1F]|\\)+", "", str(name))
        return name.strip()

    # Work on a copy with CLEANED headers
    dataset = dataset.copy()
    dataset.columns = [clean_column_name(c) for c in dataset.columns]
    
    # Detect ID/Math columns if not provided fully
    score_col, school_col, student_col, leveled_score_col = detect_columns(
        dataset, detect_math_level=True
    )
    
    # Essentials to always keep
    essentials = []
    if ids_col:
        essentials.extend([c for c in ids_col if c in dataset.columns])
    else:
        # Fallback detection
        found_ids = [c for c in [school_col, student_col] if c and c in dataset.columns]
        if not found_ids:
             found_ids = [col for col in dataset.columns if "id" in col.lower()]
        essentials.extend(found_ids)
        
    if leveled_score_col and leveled_score_col in dataset.columns:
        if leveled_score_col not in essentials:
            essentials.append(leveled_score_col)

    print(f"[IO] Essentials: {essentials}")

    dataset_clean_cols = list(dataset.columns)

    def map_attr_name_to_col(attr_name, use_fuzzy=True, cutoff=0.96):
        if not attr_name: return None, "none"
        cleaned = clean_column_name(attr_name)
        if not cleaned: return None, "none"
        
        # 1. Exact
        if cleaned in dataset_clean_cols:
            return cleaned, "exact"
        
        # 2. Fuzzy
        if use_fuzzy:
            matches = get_close_matches(cleaned, dataset_clean_cols, n=1, cutoff=cutoff)
            if matches:
                 return matches[0], "fuzzy"
        return None, "none"

    with pd.ExcelWriter(output_file, engine="openpyxl", mode="w") as writer:
        for dataset_key, result_data in all_results.items():
            full_summary = result_data.get("summary")
            if full_summary is None or full_summary.empty:
                continue

            # Identify Attribute Name column
            attr_name_col = None
            for col in full_summary.columns:
                 if "attribute" in col.lower() and "name" in col.lower():
                     attr_name_col = col; break
            if not attr_name_col:
                for col in full_summary.columns:
                    if col.lower() in ("attribute", "variable", "feature"):
                         attr_name_col = col; break
            if not attr_name_col:
                 # Fallback: take first column that isn't rank/score
                 for col in full_summary.columns:
                     if "rank" not in col.lower() and "score" not in col.lower():
                         attr_name_col = col; break
            if not attr_name_col:
                attr_name_col = full_summary.columns[0]

            top_summary = full_summary.head(top_x).copy()
            
            # Map attributes
            mapped_cols = []
            match_types = []
            for _, row in top_summary.iterrows():
                cname, mtype = map_attr_name_to_col(row[attr_name_col])
                mapped_cols.append(cname)
                match_types.append(mtype)

            top_summary["mapped_dataset_column"] = mapped_cols
            top_summary["match_type"] = match_types
            
            # Filter dataset
            selected_cols = [c for c in mapped_cols if c]
            # Unique ordered
            final_cols = []
            for c in selected_cols + essentials:
                if c not in final_cols and c in dataset.columns:
                    final_cols.append(c)
            
            if not final_cols:
                # Save only rank if no columns match
                top_summary.to_excel(writer, sheet_name=f"Rank_{dataset_key}"[:31], index=False)
                continue
                
            subset_df = dataset[final_cols]
            
            # Save
            top_summary.to_excel(writer, sheet_name=f"Rank_{dataset_key}"[:31], index=False)
            subset_df.to_excel(writer, sheet_name=f"Data_{dataset_key}"[:31], index=False)
            
    return output_file
