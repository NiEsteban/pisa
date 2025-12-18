import os
import pandas as pd
import numpy as np
from pisa_pipeline.utils.file_utils import is_file
from pisa_pipeline.utils.io import save_dataframe_to_csv, read_csv, load_csvs_from_folder


# -------------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------------
# def find_column_in_dfs(dfs, column_name):
#     indices = [i for i, df in enumerate(dfs) if column_name in df.columns]
#     return indices

def find_score_dataframe(dfs: dict, score_col: str):
    """
    Return (score_name, score_df) if found, else (None, None).

    Parameters:
        dfs: dict of {name: DataFrame}
        score_col: column name to find

    Returns:
        tuple: (name_str, DataFrame copy)
    """
    for name, df in dfs.items():
        if score_col in df.columns:
            print(f"[Transformer] Found score DataFrame: '{name}'")
            return name, df.copy()
    
    print(f"[Warning] No DataFrame contains column '{score_col}'")
    return None, None


def determine_merge_keys(df1: pd.DataFrame, df2: pd.DataFrame, merge_keys=None):
    """Find which keys to use for merging two DataFrames."""
    if merge_keys:
        # Use the first user-provided key that exists in both DataFrames
        for key in merge_keys:
            if key in df1.columns and key in df2.columns:
                return key

    # Otherwise, discover automatically (take the first intersection)
    intersection = list(set(df1.columns).intersection(df2.columns))
    if intersection:
        return intersection[0]

    return None

def assign_math_level(score: float) -> str:
    """Convert numerical math score into PISA proficiency level."""
    if pd.isna(score):
        return np.nan
    if score >= 669.30:
        return "Level 6"
    elif score >= 607.04:
        return "Level 5"
    elif score >= 544.68:
        return "Level 4"
    elif score >= 482.38:
        return "Level 3"
    elif score >= 420.07:
        return "Level 2"
    elif score >= 358.77:
        return "Level 1"
    else:
        return "Below Level 1"
    
def merge_each_with_score(score_df: pd.DataFrame, other_df: pd.DataFrame, key: str, score_col: str,keep_ids :str) -> pd.DataFrame:
    """
    Merge score_df with other_df using kepping the id and score only.
    Returns the merged DataFrame.
    """
    # Revert: Do not subset score_df. Keep all columns.
    dfs_dict = {
        "score_df": score_df,
        "other_df": other_df
    }
    merged = merge_with_base(
        base_name="score_df",
        dfs_dict=dfs_dict,
        keys=[key]
    )
    return merged



def merge_with_base(base_name: str, dfs_dict: dict[str, pd.DataFrame], keys: list[str]) -> dict[str, pd.DataFrame]:
    """
    Merge all dataframes in a dictionary into the base dataframe identified by base_name.
    Skips merging the base with itself. Avoids duplicate columns by dropping *_dup columns.
    Returns a dictionary with the fully merged dataframe under the key "fully merged".

    Args:
        base_name (str): Name of the base dataframe in dfs_dict.
        dfs_dict (dict[str, pd.DataFrame]): Dictionary of dataframes to merge.
        keys (list[str]): List of keys (column names) to merge on, matching order of dfs_dict excluding base.

    Returns:
        dict[str, pd.DataFrame]: Dictionary with a single key "fully merged" containing the merged dataframe.
    """
    if base_name not in dfs_dict:
        raise ValueError(f"[Transformer] Base dataframe '{base_name}' not found in the dictionary.")
    if len(keys) != len(dfs_dict)-1:
        if len(keys) == 1:
            keys = keys * len(other_names)
        else:
            raise ValueError("[Transformer] Length of keys does not match number of DataFrames to merge.")
    merged_df = dfs_dict[base_name].copy()
    other_names = [name for name in dfs_dict if name != base_name]

    for i, name in enumerate(other_names):
        df = dfs_dict[name]
        merged_df = pd.merge(
            merged_df,
            df,
            on=keys[i],
            how="left",
            suffixes=("", "_dup")
        )
        # Drop duplicate columns
        dup_cols = [c for c in merged_df.columns if c.endswith("_dup")]
        merged_df.drop(columns=dup_cols, inplace=True)

        # Move "Plausible Value 1 in Mathematics" to the end if it exists
        if "Plausible Value 1 in Mathematics" in merged_df.columns:
            cols = [c for c in merged_df.columns if c != "Plausible Value 1 in Mathematics"] + ["Plausible Value 1 in Mathematics"]
            merged_df = merged_df[cols]

    return merged_df



def get_other_dfs(dfs: list[pd.DataFrame], score_index: int):
    """Return a list of DataFrames excluding the score DataFrame."""
    return [df for i, df in enumerate(dfs) if i != score_index]

# -------------------------------------------------------------------------
# Main Transformer class
# -------------------------------------------------------------------------
class Transformer:
    def __init__(self):
        """Transformer object — reusable for different PISA datasets."""
        pass
    def drop_unwanted_columns(self, df: pd.DataFrame, user_drop_cols=None, keep_cols=None) -> pd.DataFrame:
        """Drop user-specified and automatically unwanted columns."""
        if user_drop_cols is None:
            user_drop_cols = []
        if keep_cols is None:
            keep_cols = [f"Plausible Value {a} in Mathematics" for a in range(1, 11)]

        # ------------------------------------------------------------------
        # Check user-specified columns exist in df
        # ------------------------------------------------------------------
        missing_cols = [col for col in user_drop_cols if col not in df.columns]
        if missing_cols:
            print(f"[Warning] The following user-specified columns do not exist in the DataFrame:\n{missing_cols}")

        # ------------------------------------------------------------------
        # Build list of columns to drop
        # ------------------------------------------------------------------
        to_drop = [col for col in user_drop_cols if col in df.columns]  # only existing ones

        for col in df.columns:
            low = col.lower()
            if low.startswith("final") or (low.startswith("plausible value") and col not in keep_cols):
                to_drop.append(col)

        df.drop(columns=to_drop, inplace=True, errors="ignore")

        return df

    def split_dataframe(self,df: pd.DataFrame, col_ranges: list[tuple[int, int]], ids_col=None) -> list[pd.DataFrame]:
        """
        Split dataframe into two parts:
        1) Columns within col_ranges combined
        2) Columns outside col_ranges
        Include ids_col in both splits if not already present.

        Parameters:
            df: DataFrame to split
            col_ranges: list of tuples (start, end) for column index ranges
            ids_col: column name or list of column names to keep in each split

        Returns:
            list of two DataFrames
        """
        all_range_cols = []
        for start, end in col_ranges:
            all_range_cols.extend(df.columns[start:end].to_list())

        # Ensure ids_col is a list
        if ids_col is not None:
            ids_cols_list = [ids_col] if isinstance(ids_col, str) else list(ids_col)
        else:
            ids_cols_list = []

        # 1 First split: columns in col_ranges + ids_col
        split1_cols = list(dict.fromkeys(ids_cols_list + all_range_cols))  # remove duplicates, keep order
        split1 = df[split1_cols]

        # 2 Second split: columns not in col_ranges + ids_col
        remaining_cols = [c for c in df.columns if c not in all_range_cols]
        split2_cols = list(dict.fromkeys(ids_cols_list + remaining_cols))
        split2 = df[split2_cols]

        return [split1, split2]

    # ---------------------------------------------------------------------
    # High-level orchestration
    # ---------------------------------------------------------------------
    def run(
        self,
        input_path: str = None,
        score_col: str = None,
        dfs: dict[str, pd.DataFrame] = None,
        ids_col: list[str] = None
    ):
        """
        Run flexible transformation pipeline.

        Returns:
            dict of {name: DataFrame} after processing
        """
        if score_col is None:
            raise ValueError("[Transformer] Please provide a score column name as a target")

        if dfs is None and input_path is None:
            raise ValueError("[Transformer] You must provide either dfs or input_path")

        dfs_dict_transformed = {}

        # -------------------------------
        # 1) Dictionary of DataFrames provided directly
        # -------------------------------
        if dfs is not None:
            if not isinstance(dfs, dict):
                raise ValueError("[Transformer] dfs must be a dictionary of {name: DataFrame}")
            dfs_dict_transformed = self.process_dfs(dfs, score_col, ids_col)

        # -------------------------------
        # 2) Folder input
        # -------------------------------
        elif input_path is not None and not is_file(input_path):
            dfs_dict_transformed = self.process_folder(input_path, score_col, ids_col)

        # -------------------------------
        # 3) Single CSV file
        # -------------------------------
        elif input_path is not None and is_file(input_path):
            df = read_csv(input_path)
            dfs_dict_transformed[os.path.basename(input_path)] = df

        # -------------------------------
        # 4) Drop unwanted columns
        # -------------------------------
        dfs_dict_transformed = {k: self.drop_unwanted_columns(v) for k, v in dfs_dict_transformed.items()}

        # -------------------------------
        # 5) Apply score leveling
        # -------------------------------
        for name, df in dfs_dict_transformed.items():
            if score_col in df.columns:
                df[f"{score_col}_level"] = df[score_col].apply(assign_math_level)
                dfs_dict_transformed[name] = df

        print(f"[Transformer] Finished processing")
        return dfs_dict_transformed


    # ---------------------------------------------------------------------
    # Folder mode
    # ---------------------------------------------------------------------
    def process_folder(self, input_folder, score_col, ids_col):
        dfs_dict = load_csvs_from_folder(input_folder) 
        merged_dict = self.process_dfs(dfs_dict, score_col, ids_col)
        return merged_dict


    # ---------------------------------------------------------------------
    # Process dictionary of DataFrames
    # ---------------------------------------------------------------------
    def process_dfs(self, dfs: dict[str, pd.DataFrame], score_col, ids_col=None) -> dict:
        """
        Process a dictionary of DataFrames:
        1) Merge each other DF individually with score_df.
        2) Merge all DFs together with score_df into a full merged DataFrame.

        Parameters:
            dfs: dict of {name: DataFrame}
            score_col: column name of the score
            ids_col: column(s) to use for merging

        Returns:
            dict of {name: merged DataFrame}, including "full_merged"
        """
        # 1) Find score DataFrame
        score_name, score_df = find_score_dataframe(dfs, score_col)
        if score_df is None:
            return {}

        merged_dict = {score_name: score_df}
        keys = []

        # 2) Merge each DF individually with score_df
        for name, df in dfs.items():
            if name == score_name:
                continue

            key_to_use = determine_merge_keys(score_df, df, ids_col)
            if not key_to_use:
                print(f"[Transformer] Info: No common key found for merge, skipping DF '{name}'.")
                continue

            keys.append(key_to_use)
            merged = merge_each_with_score(score_df, df, key_to_use ,score_col,ids_col)
            merged_dict[name] = merged

        # 3) Merge all other DFs together into full merged
        if keys:
            fully_merged_df = merge_with_base(score_name, merged_dict, keys)
            merged_dict["full_merged"] = fully_merged_df
        else:
            print("[Warning] No valid merge key found for full merge.")

        print("[Transformer] Folder processing completed.")
        return merged_dict
