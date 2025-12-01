import os
import csv
import pyreadstat
import numpy as np
import pandas as pd
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