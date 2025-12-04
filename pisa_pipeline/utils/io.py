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

def save_csv_weka_safe(self, df: pd.DataFrame, filepath: str):
    """Save CSV in Weka-safe format, quoting all strings, preserving NaNs."""
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