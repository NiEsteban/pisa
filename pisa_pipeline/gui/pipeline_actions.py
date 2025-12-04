import threading
import os
from tkinter import messagebox
from typing import TYPE_CHECKING, List, Tuple, Optional, Dict
import pandas as pd
from pisa_pipeline.data_processing.sav_loader import SAVloader
from pisa_pipeline.data_processing.cleaner import CSVCleaner
from pisa_pipeline.data_processing.transformer import Transformer
from pisa_pipeline.utils.io import save_dataframe_to_csv
from pisa_pipeline.utils.algo_utils import detect_columns

if TYPE_CHECKING:
    from pisa_pipeline.gui.main_window import StepwisePipelineGUI

class PipelineActions:
    """Handles all pipeline actions (Load, Clean, Transform)"""

    def __init__(self, gui: "StepwisePipelineGUI"):
        self.gui = gui

        # Connect button commands
        self.gui.btn_load_label.config(command=self.action_load_label)
        self.gui.btn_clean.config(command=self.action_clean)
        self.gui.btn_transform.config(command=self.action_transform)
        self.gui.btn_full_pipeline.config(command=self.run_full_pipeline)
        self.gui.btn_drop_columns.config(command=self.action_drop_columns)

    def _get_files_to_process(self) -> List[str]:
        """Get list of files to process based on selection"""
        sel = self.gui.file_listbox.curselection()
        if sel:
            return [self.gui.selected_files[i] for i in sel]
        # If nothing selected, return all files
        return [f for f in self.gui.selected_files if f.endswith(("_labeled.csv", "_cleaned.csv", ".csv", ".sav"))]

    def auto_detect_and_fill_ids(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Auto-detect IDs with improved score column detection."""
        auto_score, auto_school, auto_student = detect_columns(df)

        # Update entries if empty
        if auto_score:
            self.gui.entry_score.delete(0, "end")
            self.gui.entry_score.insert(0, auto_score)
        if auto_school:
            self.gui.entry_school.delete(0, "end")
            self.gui.entry_school.insert(0, auto_school)
        if auto_student:
            self.gui.entry_student.delete(0, "end")
            self.gui.entry_student.insert(0, auto_student)

        print(f"[PIPELINE] Auto-detected: score={auto_score}, school={auto_school}, student={auto_student}")
        return auto_score, auto_school, auto_student

    def get_best_ids(self, dfs_dict: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Determine the best columns for score, school, and student IDs.
        - School and student IDs: prefer columns with more unique values
        - Score ID: keep first non-None
        Returns: (score_col, school_col, student_col)
        """
        best_score_col = None
        best_school_col = None
        best_student_col = None
        max_school_unique = -1
        max_student_unique = -1

        for df_id, df in dfs_dict.items():
            try:
                s_col, sch_col, stu_col = self.auto_detect_and_fill_ids(df)

                # Score: only replace if None
                if best_score_col is None and s_col is not None:
                    best_score_col = s_col

                # School: replace if None or has fewer unique values
                if sch_col in df.columns:
                    n_unique = df[sch_col].nunique(dropna=True)
                    if best_school_col is None or n_unique > max_school_unique:
                        best_school_col = sch_col
                        max_school_unique = n_unique

                # Student: replace if None or has fewer unique values
                if stu_col in df.columns:
                    n_unique = df[stu_col].nunique(dropna=True)
                    if best_student_col is None or n_unique > max_student_unique:
                        best_student_col = stu_col
                        max_student_unique = n_unique
            except Exception as e:
                print(f"Skipping {df_id} due to error: {e}")
                continue

        return best_score_col, best_school_col, best_student_col

    def action_drop_columns(self) -> None:
        """Drop checked columns from the currently selected file(s)."""
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files selected.")
            return

        def worker():
            for file_path in files:
                try:
                    # Get the dataframe
                    fr = self.gui.file_results.get(file_path, {})
                    df = next((v for v in [fr.get("transformed"), fr.get("cleaned"), fr.get("labeled")] if v is not None), None)
                    
                    if df is None:
                        if os.path.exists(file_path):
                            try:
                                df = pd.read_csv(file_path, encoding="cp1252")
                            except Exception as e:
                                print(f"[ERROR] Failed to load {file_path}: {e}")
                                continue
                        else:
                            print(f"[PIPELINE] File not found: {file_path}")
                            continue

                    # Get columns to drop
                    cols_to_drop = self.gui.columns_to_drop_map.get(file_path, set())
                    if not cols_to_drop:
                        print(f"[PIPELINE] No columns marked for dropping in {os.path.basename(file_path)}")
                        continue

                    # Drop the columns
                    existing_cols = [c for c in cols_to_drop if c in df.columns]
                    if not existing_cols:
                        print(f"[PIPELINE] No marked columns exist in {os.path.basename(file_path)}")
                        continue

                    df_dropped = df.drop(columns=existing_cols)
                    print(f"[PIPELINE] Dropped {len(existing_cols)} columns from {os.path.basename(file_path)}")

                    # Save the file
                    base, ext = os.path.splitext(file_path)
                    out_path = f"{base}_dropped{ext}"
                    save_dataframe_to_csv(df_dropped, out_path)

                    # Update GUI state
                    self.gui.file_results[out_path] = {
                        "transformed": df_dropped if "transformed" in fr else None,
                        "cleaned": df_dropped if "cleaned" in fr else None,
                        "labeled": df_dropped if "labeled" in fr else None
                    }
                    
                    # Clear the drop map for the original file
                    if file_path in self.gui.columns_to_drop_map:
                        del self.gui.columns_to_drop_map[file_path]

                    # Update file list
                    self.gui.file_manager._replace_file_in_list(file_path, out_path)
                    self.gui.file_manager._select_single_file_in_list(out_path)
                    self.gui.root.after(0, lambda p=out_path: self.gui.column_display.display_columns_for_file(p))

                    print(f"[PIPELINE] Saved dropped columns file: {out_path}")

                except Exception as e:
                    print(f"[ERROR] Failed to drop columns from {file_path}: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def _parse_split_ranges(self, ranges_str: str) -> List[Tuple[int, int]]:
        """Parse split ranges string like '0:10, 20:30' into list of tuples."""
        try:
            ranges = []
            for part in ranges_str.split(','):
                part = part.strip()
                if ':' in part:
                    start, end = part.split(':')
                    ranges.append((int(start.strip()), int(end.strip())))
            return ranges
        except Exception as e:
            print(f"[ERROR] Failed to parse split ranges '{ranges_str}': {e}")
            return []

    def _process_label(self, files: List[str]) -> List[str]:
        """Process files through the label step."""
        labeled_files = []
        for f in files:
            try:
                print(f"[PIPELINE] Loading and labeling: {f}")
                loader = SAVloader()
                df_labeled, df_unlabeled = loader.run(f, self.gui.country_code.get())

                if df_labeled is None:
                    print(f"[PIPELINE] Skipping {f} (no labeled rows).")
                    continue

                base = os.path.splitext(os.path.basename(f))[0]
                out_path = os.path.join(os.path.dirname(f), f"{base}_labeled.csv")
                save_dataframe_to_csv(df_labeled, out_path)

                self.gui.file_results[out_path] = {"labeled": df_labeled}
                self.gui.file_manager._replace_file_in_list(f, out_path)
                self.gui.file_manager._select_single_file_in_list(out_path)

                self.gui.root.after(0, lambda p=out_path: self.gui.column_display.display_columns_for_file(p))

                # Auto-detect IDs
                self.gui.root.after(0, lambda d=df_labeled: self.auto_detect_and_fill_ids(d))

                if self.gui.save_unlabel_var.get() and df_unlabeled is not None:
                    unl_out = os.path.join(os.path.dirname(f), f"{base}_unlabeled.csv")
                    save_dataframe_to_csv(df_unlabeled, unl_out)
                    print(f"[PIPELINE] Saved unlabeled → {unl_out}")

                labeled_files.append(out_path)
                print(f"[PIPELINE] Labeled file saved: {out_path}")
            except Exception as e:
                print(f"[ERROR] {f}: {e}")

        return labeled_files

    def _process_clean(self, files: List[str]) -> List[str]:
        """Process files through the clean step, dropping duplicated columns by default."""
        cleaned_files = []
        for f in files:
            try:
                fr = self.gui.file_results.get(f, {})
                df = next((v for v in [fr.get("cleaned"), fr.get("labeled"), fr.get("transformed")] if v is not None), None)
                if df is None:
                    if os.path.exists(f):
                        try:
                            df = pd.read_csv(f, encoding="cp1252")
                        except Exception as e:
                            print(f"[ERROR] Failed to load {f}: {e}")
                            continue
                    else:
                        print(f"[PIPELINE] Skipping {f}, file not found.")
                        continue

                cleaner = CSVCleaner()
                score_col = self.gui.entry_score.get().strip()
                school_col = self.gui.entry_school.get().strip()
                student_col = self.gui.entry_student.get().strip()

                if not all([score_col, school_col, student_col]):
                    auto_s, auto_school, auto_stu = self.auto_detect_and_fill_ids(df)
                    score_col = score_col or auto_s
                    school_col = school_col or auto_school
                    student_col = student_col or auto_stu

                base = os.path.splitext(os.path.basename(f))[0]

                # Clean the data, dropping duplicated columns by default
                df_clean = cleaner.run(
                    df,
                    base,
                    [student_col, school_col],
                    self.gui.missing_thr.get(),
                    self.gui.uniform_thr.get(),
                    correlation_threshold=self.gui.correlation_thr.get(),
                    target=score_col
                )

                out = os.path.join(os.path.dirname(f), f"{base}_cleaned.csv")
                save_dataframe_to_csv(df_clean, out)
                self.gui.file_results[out] = {"cleaned": df_clean}
                self.gui.file_manager._replace_file_in_list(f, out)
                self.gui.file_manager._select_single_file_in_list(out)
                self.gui.root.after(0, lambda p=out: self.gui.column_display.display_columns_for_file(p))
                cleaned_files.append(out)
                print(f"[PIPELINE] Cleaned saved: {out}")

            except Exception as e:
                print(f"[ERROR] Cleaning {f}: {e}")

        return cleaned_files



    def _process_transform(self, files: List[str]) -> None:
        """Process files through the transform step."""
        dfs_dict = {}
        for f in files:
            try:
                fr = self.gui.file_results.get(f, {})
                df = fr.get("cleaned")
                if df is None and os.path.exists(f) and f.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(f, encoding="cp1252")
                        print(f"[PIPELINE] Loaded {os.path.basename(f)}")
                    except Exception as e:
                        print(f"[ERROR] Failed to load {f}: {e}")
                        continue
                if df is None:
                    print(f"[PIPELINE] Skipping {f} (no cleaned data).")
                    continue
                base = os.path.splitext(os.path.basename(f))[0]
                dfs_dict[base] = df
            except Exception as e:
                print(f"[ERROR] Preparing {f}: {e}")

        if not dfs_dict:
            print("[PIPELINE] No valid dataframes to transform.")
            return

        s_col = self.gui.entry_score.get().strip() or None
        sch_col = self.gui.entry_school.get().strip() or None
        stu_col = self.gui.entry_student.get().strip() or None

        if not s_col or not sch_col or not stu_col:
            (auto_s, auto_sch, auto_stu) = self.get_best_ids(dfs_dict)
            s_col = s_col or auto_s
            sch_col = sch_col or auto_sch
            stu_col = stu_col or auto_stu

        if not s_col:
            print("[ERROR] No valid score column detected. Cannot transform.")
            return

        # Handle dataset splitting if enabled
        if self.gui.split_dataset_var.get():
            ranges_str = self.gui.split_ranges_var.get().strip()
            if ranges_str:
                col_ranges = self._parse_split_ranges(ranges_str)
                if col_ranges:
                    print(f"[PIPELINE] Splitting datasets using ranges: {col_ranges}")
                    transformer = Transformer()
                    ids_list = [c for c in [stu_col, sch_col] if c]
                    
                    # Split each dataframe
                    split_dfs_dict = {}
                    for name, df in dfs_dict.items():
                        split_parts = transformer.split_dataframe(df, col_ranges, ids_col=ids_list)
                        split_dfs_dict[f"{name}_part1"] = split_parts[0]
                        split_dfs_dict[f"{name}_part2"] = split_parts[1]
                        print(f"[PIPELINE] Split {name} into 2 parts: {split_parts[0].shape}, {split_parts[1].shape}")
                    
                    # Replace dfs_dict with split versions
                    dfs_dict = split_dfs_dict

        transformer = Transformer()
        ids_list = [c for c in [stu_col, sch_col] if c]
        print(f"[PIPELINE] Transforming {len(dfs_dict)} file(s) with score_col={s_col}...")
        transformed = transformer.run(
            dfs=dfs_dict,
            score_col=s_col,
            ids_col=ids_list
        )

        first_file = files[0]
        outdir = os.path.join(os.path.dirname(first_file), "leveled")
        os.makedirs(outdir, exist_ok=True)

        for base_name, df_t in transformed.items():
            try:
                outpath = os.path.join(outdir, f"{base_name}_leveled.csv")
                save_dataframe_to_csv(df_t, outpath)
                self.gui.file_results[outpath] = {"transformed": df_t}
                self.gui.file_manager._add_file_to_list_if_missing(outpath)
                self.gui.file_manager._select_single_file_in_list(outpath)
                self.gui.root.after(0, lambda p=outpath: self.gui.column_display.display_columns_for_file(p))
                print(f"[PIPELINE] Transformed saved: {outpath}")
            except Exception as e:
                print(f"[ERROR] Saving transformed {base_name}: {e}")

    def action_load_label(self) -> None:
        """Load and label selected files"""
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files selected or available.")
            return

        def worker():
            self._process_label(files)

        threading.Thread(target=worker, daemon=True).start()

    def action_clean(self) -> None:
        """Clean selected files"""
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files available.")
            return

        def worker():
            self._process_clean(files)

        threading.Thread(target=worker, daemon=True).start()

    def action_transform(self) -> None:
        """Transform files using dictionary approach."""
        files = self._get_files_to_process()
        if not files:
            files = [p for p, v in self.gui.file_results.items() if "cleaned" in v]
            if not files:
                for f in self.gui.selected_files:
                    base, ext = os.path.splitext(f)
                    cleaned_path = f"{base}_cleaned.csv"
                    if os.path.exists(cleaned_path):
                        files.append(cleaned_path)

        if not files:
            print("[PIPELINE] No files to transform.")
            return

        def worker():
            self._process_transform(files)

        threading.Thread(target=worker, daemon=True).start()

    def run_full_pipeline(self) -> None:
        """Run complete pipeline: Load → Label → Clean → Transform for selected or all files."""
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files selected or available.")
            return

        def worker():
            try:
                print(f"[PIPELINE] Starting full pipeline for {len(files)} file(s)...")

                # Step 1: Load and Label
                print("[PIPELINE] Step 1/3: Loading and labeling...")
                labeled_files = self._process_label(files)
                if not labeled_files:
                    print("[PIPELINE] No files were successfully labeled. Aborting.")
                    return

                # Step 2: Clean
                print("[PIPELINE] Step 2/3: Cleaning...")
                cleaned_files = self._process_clean(labeled_files)
                if not cleaned_files:
                    print("[PIPELINE] No files were successfully cleaned. Aborting.")
                    return

                # Step 3: Transform
                print("[PIPELINE] Step 3/3: Transforming...")
                self._process_transform(cleaned_files)

                print("[PIPELINE] Full pipeline completed successfully!")
            except Exception as e:
                print(f"[ERROR] Full pipeline failed: {e}")

        threading.Thread(target=worker, daemon=True).start()