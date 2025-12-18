import os
import shutil
import pandas as pd
from typing import List, Tuple, Optional, Dict, Union, Set
from pisa_pipeline.data_processing.sav_loader import SAVloader
from pisa_pipeline.data_processing.cleaner import CSVCleaner
from pisa_pipeline.data_processing.transformer import Transformer
from pisa_pipeline.utils.io import save_dataframe_to_csv
from pisa_pipeline.utils.algo_utils import detect_columns

class PipelineService:
    """
    Core service layer for PISA pipeline processing.
    Separates business logic from GUI.
    """

    def auto_detect_ids(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Auto-detect IDs: (score_col, school_col, student_col)"""
        return detect_columns(df)

    def get_best_ids(self, dfs_dict: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Determine best columns across multiple dataframes."""
        best_score_col = None
        best_school_col = None
        best_student_col = None
        max_school_unique = -1
        max_student_unique = -1

        for _, df in dfs_dict.items():
            try:
                s_col, sch_col, stu_col = self.auto_detect_ids(df)

                if best_score_col is None and s_col is not None:
                    best_score_col = s_col

                if sch_col in df.columns:
                    n_unique = df[sch_col].nunique(dropna=True)
                    if best_school_col is None or n_unique > max_school_unique:
                        best_school_col = sch_col
                        max_school_unique = n_unique

                if stu_col in df.columns:
                    n_unique = df[stu_col].nunique(dropna=True)
                    if best_student_col is None or n_unique > max_student_unique:
                        best_student_col = stu_col
                        max_student_unique = n_unique

            except Exception:
                continue

        return best_score_col, best_school_col, best_student_col

    def load_and_label(self, file_path: str, country_code: str, save_unlabeled: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Load and label a single file.
        Returns dictionary of created files map: {path: dataframe, ...}
        """
        if file_path.lower().endswith((".sps", ".spss")):
            from pisa_pipeline.data_processing.spss_loader import SPSSloader
            loader = SPSSloader()
            df_labeled, df_unlabeled = loader.run(file_path, country_code=country_code)
        elif file_path.lower().endswith(".txt"):
            # Check if it is a syntax file disguised as .txt
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    head = f.read(2048).upper()
                    if "DATA LIST" in head or "VARIABLE LABELS" in head:
                        print(f"[INFO] Detected SPSS Syntax in .txt file: {os.path.basename(file_path)}")
                        from pisa_pipeline.data_processing.spss_loader import SPSSloader
                        loader = SPSSloader()
                        df_labeled, df_unlabeled = loader.run(file_path, country_code=country_code)
                    else:
                        print(f"[WARN] Input file {os.path.basename(file_path)} is a text file but does not look like SPSS Syntax.")
                        return {}
            except Exception as e:
                print(f"[ERROR] Failed to inspect .txt file {file_path}: {e}")
                return {}
        else:
            loader = SAVloader()
            df_labeled, df_unlabeled = loader.run(file_path, country_code)

        if df_labeled is None:
            return {}

        results = {}
        
        # Determine output path
        parent_dir = os.path.dirname(file_path)
        out_dir = os.path.join(parent_dir, "labeled")
        os.makedirs(out_dir, exist_ok=True)
        
        base = os.path.splitext(os.path.basename(file_path))[0]
        out_path = os.path.join(out_dir, f"{base}.csv")
        save_dataframe_to_csv(df_labeled, out_path)
        
        results[out_path] = df_labeled

        if save_unlabeled and df_unlabeled is not None:
            unl_out = os.path.join(out_dir, f"{base}_unlabeled.csv")
            save_dataframe_to_csv(df_unlabeled, unl_out)
            # We don't necessarily return unlabeled df to the pipeline flow, but we save it.

        return results

    def clean_file(self, 
                   file_path: str, 
                   df: pd.DataFrame, 
                   score_col: str, 
                   school_col: str, 
                   student_col: str,
                   missing_thr: float, 
                   uniform_thr: float, 
                   correlation_thr: float) -> Optional[Tuple[str, pd.DataFrame]]:
        """
        Clean a single dataframe.
        Returns (output_path, cleaned_dataframe) or None.
        """
        cleaner = CSVCleaner()
        base = os.path.splitext(os.path.basename(file_path))[0]
        
        # Detect parent directory structure
        parent_dir = os.path.dirname(file_path)
        if os.path.basename(parent_dir) == "labeled":
            root_data_dir = os.path.dirname(parent_dir)
        else:
            root_data_dir = parent_dir
        
        out_dir = os.path.join(root_data_dir, "cleaned")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{base}.csv")

        try:
            df_clean = cleaner.run(
                df,
                base,
                ids=[student_col, school_col],
                missing_threshold=missing_thr,
                uniform_threshold=uniform_thr,
                correlation_threshold=correlation_thr,
                target=score_col
            )
            save_dataframe_to_csv(df_clean, out_path)
            return out_path, df_clean
        except Exception as e:
            print(f"[ERROR] Cleaning {file_path}: {e}")
            return None

    def transform_files(self, 
                        dfs_dict: Dict[str, pd.DataFrame], 
                        score_col: str, 
                        ids_cols: List[str], 
                        split_ranges: Optional[List[Tuple[int, int]]] = None,
                        root_output_dir: str = "") -> Dict[str, pd.DataFrame]:
        """
        Transform multiple dataframes. 
        Returns map {output_path: transformed_df}.
        """
        transformer = Transformer()
        
        # Apply splitting if requested
        processing_dict = dfs_dict
        if split_ranges:
            split_dict = {}
            for name, df in dfs_dict.items():
                parts = transformer.split_dataframe(df, split_ranges, ids_col=ids_cols)
                split_dict[f"{name}_part1"] = parts[0]
                split_dict[f"{name}_part2"] = parts[1]
            processing_dict = split_dict

        transformed_map = transformer.run(dfs=processing_dict, score_col=score_col, ids_col=ids_cols)
        
        results = {}
        
        out_dir = os.path.join(root_output_dir, "leveled")
        os.makedirs(out_dir, exist_ok=True)

        for base_name, df_t in transformed_map.items():
            out_path = os.path.join(out_dir, f"{base_name}.csv")
            save_dataframe_to_csv(df_t, out_path)
            results[out_path] = df_t
            
        return results

    def drop_columns_and_backup(self, 
                                file_path: str, 
                                df: pd.DataFrame, 
                                columns_to_drop: List[str]) -> Tuple[pd.DataFrame, str]:
        """
        Drop columns from file, creating a hidden backup first.
        Returns (new_dataframe, backup_path).
        """
        # Create backup
        backup_dir = os.path.join(os.path.dirname(file_path), ".backups")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, os.path.basename(file_path))
        shutil.copy2(file_path, backup_path)

        # Drop cols
        df_dropped = df.drop(columns=columns_to_drop)
        
        # Overwrite
        save_dataframe_to_csv(df_dropped, file_path)
        
        return df_dropped, backup_path

    def restore_backup(self, original_path: str, backup_path: str) -> None:
        """Restore file from backup and clean up backup."""
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup not found: {backup_path}")
            
        shutil.copy2(backup_path, original_path)
        
        # Cleanup
        try:
            os.remove(backup_path)
            backup_dir = os.path.dirname(backup_path)
            if not os.listdir(backup_dir):
                os.rmdir(backup_dir)
        except OSError:
            pass
