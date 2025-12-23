import threading
import os
from tkinter import messagebox
from typing import TYPE_CHECKING, List, Tuple, Optional, Dict
import pandas as pd

from pisa_pipeline.data_processing.pipeline_service import PipelineService
from pisa_pipeline.infrastructure.sav_loader import SAVLoader
from pisa_pipeline.controller.context import PipelineContext
from pisa_pipeline.utils.file_scanner import FileSystemScanner

if TYPE_CHECKING:
    from pisa_pipeline.gui.main_window import StepwisePipelineGUI


class PipelineController:
    """
    Handles all pipeline actions (Load, Clean, Transform) via the GUI.
    Delegates core logic to PipelineService.
    Responsible for Threading and UI Updates.
    Owns the Application State (Context).
    """

    def __init__(self, gui: "StepwisePipelineGUI"):
        self.gui = gui
        self.service = PipelineService()
        self.context = PipelineContext()

        # Connect button commands
        self.gui.btn_load_label.config(command=self.action_load_label)
        self.gui.btn_clean.config(command=self.action_clean)
        self.gui.btn_transform.config(command=self.action_transform)
        self.gui.btn_full_pipeline.config(command=self.run_full_pipeline)
        self.gui.btn_drop_columns.config(command=self.action_drop_columns)
        self.gui.btn_undo_drop.config(command=self.action_undo_last_drop)

    def _get_files_to_process(self) -> List[str]:
        """Get list of files to process based on selection"""
        return self.gui.file_manager.get_selected_files()

    def get_files_for_path(self, path: str) -> List[str]:
        """
        Get supported files for a given path (file or directory).
        Delegates to Infrastructure layer FileSystemScanner.
        Used by View components to maintain proper layer separation.
        """
        return FileSystemScanner.get_recursive_files(path)


    def auto_detect_and_fill_ids(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Auto-detect IDs and populate GUI entries (unless locked)."""
        auto_score, auto_school, auto_student = self.service.auto_detect_ids(df)

        # Check if locked
        if self.gui.ids_lock_var.get():
             print(f"[PIPELINE] Auto-detect found: score={auto_score}, school={auto_school}, student={auto_student} (Ignored: Locked)")
             return auto_score, auto_school, auto_student

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

    
    def on_selection_change(self, files: List[str]) -> None:
        """
        Called when file selection changes in the GUI.
        Auto-detects IDs based on selection (unless locked).
        """
        # If locked, do nothing
        if self.gui.ids_lock_var.get():
            return
        
        if not files:
            return

        # Helper to check if GUI fields are empty
        def fields_are_empty() -> bool:
            return (not self.gui.entry_score.get().strip() and 
                    not self.gui.entry_school.get().strip() and 
                    not self.gui.entry_student.get().strip())

        # CASE 1: Single file (Always auto-detect/overwrite unless locked)
        if len(files) == 1:
            file_path = files[0]
            if not os.path.isfile(file_path): return
            if not file_path.lower().endswith((".csv", ".sav", ".txt")): return

             # Run detection in thread to avoid blocking UI
            def detect_single():
                try:
                    df = None
                    if file_path in self.context.file_results:
                         fr = self.context.file_results[file_path]
                         df = next((v for v in [fr.get("cleaned"), fr.get("labeled"), fr.get("transformed")] if v is not None), None)
                    
                    if df is None:
                        # Quick load (header only if CSV?)
                        # For now, full load is safer for reliability, assuming files aren't massive.
                        # Optimization: read only a few rows if possible.
                         try:
                            if file_path.lower().endswith(".csv"):
                                df = pd.read_csv(file_path, encoding="cp1252", nrows=100)
                            elif file_path.lower().endswith(".sav"):
                                # SAV loader usually fast enough for metadata?
                                from pisa_pipeline.infrastructure.sav_loader import SAVLoader
                                loader = SAVLoader()
                                df, _ = loader.load(file_path, "UNK") # dummy code
                         except:
                             pass
                    
                    if df is not None:
                        # Schedule GUI update on main thread
                        self.gui.root.after(0, lambda: self.auto_detect_and_fill_ids(df))

                except Exception as e:
                    print(f"[WARN] Auto-detect failed for selection: {e}")

            threading.Thread(target=detect_single, daemon=True).start()

        # CASE 2: Multi file (Update ONLY if fields are empty)
        else:
            if fields_are_empty():
                 # We need to find "Best" IDs common to files?
                 # Strategy: Just pick the first file's IDs for now, or true intersection?
                 # User said: "check the best autoselection". 
                 # Let's try to detect from the first valid file to fill the empty fields.
                 
                def detect_multi():
                    try:
                        # Find first valid file
                         for file_path in files:
                            if not os.path.isfile(file_path): continue
                            
                            df = None
                            if file_path.lower().endswith(".csv"):
                                df = pd.read_csv(file_path, encoding="cp1252", nrows=50)
                            elif file_path.lower().endswith(".sav"):
                                continue # processing multiple SAVs just for preview might be slow
                            
                            if df is not None:
                                self.gui.root.after(0, lambda: self.auto_detect_and_fill_ids(df))
                                break # Stop after finding one valid set

                    except: pass
                
                threading.Thread(target=detect_multi, daemon=True).start()

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
                    fr = self.context.file_results.get(file_path, {})
                    df = next((v for v in [fr.get("transformed"), fr.get("cleaned"), fr.get("labeled")] if v is not None), None)
                    
                    if df is None:
                        if os.path.exists(file_path):
                            if file_path.lower().endswith(".sav"):
                                print(f"[ERROR] Cannot modify columns of raw .sav file: {file_path}")
                                print(f"[HINT] Please run 'Load & Label' first.")
                                continue
                            try:
                                df = pd.read_csv(file_path, encoding="cp1252")
                            except Exception as e:
                                print(f"[ERROR] Failed to load {file_path}: {e}")
                                continue
                        else:
                            print(f"[PIPELINE] File not found: {file_path}")
                            continue

                    # Get columns to drop
                    cols_to_drop = self.context.columns_to_drop_map.get(file_path, set())
                    if not cols_to_drop:
                        print(f"[PIPELINE] No columns marked for dropping in {os.path.basename(file_path)}")
                        continue

                    existing_cols = [c for c in cols_to_drop if c in df.columns]
                    if not existing_cols:
                        print(f"[PIPELINE] No marked columns exist in {os.path.basename(file_path)}")
                        continue

                    print(f"[PIPELINE] Dropping {len(existing_cols)} columns from {os.path.basename(file_path)}")

                    # Use Service
                    df_dropped, backup_path = self.service.drop_columns_and_backup(file_path, df, existing_cols)
                    self.last_backup = (file_path, backup_path)

                    # Update GUI state
                    fr = self.context.file_results.get(file_path, {})
                    self.context.file_results[file_path] = {
                        "transformed": df_dropped if "transformed" in fr else None,
                        "cleaned": df_dropped if "cleaned" in fr else None,
                        "labeled": df_dropped if "labeled" in fr else None
                    }
                    
                    # Clear drop map
                    if file_path in self.context.columns_to_drop_map:
                        del self.context.columns_to_drop_map[file_path]

                    # Refresh UI
                    self.gui.file_manager.refresh_folder(os.path.dirname(file_path))

                    def update_ui(path=file_path):
                        self.gui.file_manager.select_file(path)
                        self.gui.column_display.display_columns_for_file(path)
                    
                    self.gui.root.after(0, update_ui)
                    print(f"[PIPELINE] Saved dropped columns file (overwritten): {file_path}")

                except Exception as e:
                    import traceback
                    print(f"[ERROR] Dropping columns failed: {e}")
                    traceback.print_exc()

        threading.Thread(target=worker, daemon=True).start()

    def action_undo_last_drop(self) -> None:
        """Undo the last drop using backup."""
        if not hasattr(self, 'last_backup') or not self.last_backup:
            messagebox.showinfo("Undo", "No action to undo or backup missing.")
            return
            
        original_path, backup_path = self.last_backup
        
        try:
            self.service.restore_backup(original_path, backup_path)
            self.last_backup = None
            
            # Refresh GUI
            self.gui.file_manager.refresh_folder(os.path.dirname(original_path))
            
            def update_ui_undo(path=original_path):
                self.gui.file_manager.select_file(path)
                self.gui.column_display.display_columns_for_file(path)
            
            self.gui.root.after(0, update_ui_undo)
            
            print(f"[PIPELINE] Undo successful: Restored {os.path.basename(original_path)}")
            # messagebox.showinfo("Undo", f"Successfully restored {os.path.basename(original_path)}") # Optional spam
            
        except Exception as e:
             messagebox.showerror("Undo Error", f"Failed to restore file: {e}")

    def _parse_split_ranges(self, ranges_str: str) -> List[Tuple[int, int]]:
        """Parse split ranges string like '0:10, 20:30'."""
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

    def _process_label(self, input_files: List[str]) -> List[str]:
        """
        Step 1: Load raw files (SAV or SPSS Syntax) and apply labels.
        Decodes metadata (e.g., '1' -> 'Female') using the syntax or SAV dictionary.
        """
        labeled_files_output = []
        country_code_val = self.gui.country_code.get()
        save_unlabeled_flag = self.gui.save_unlabel_var.get()
        
        print(f"\n[STEP 1/3] Starting Load & Label for {len(input_files)} file(s)...")

        for file_path in input_files:
            file_extension = file_path.lower()
            
            # Allow .txt to pass; Service will check if it is syntax or data.
            # if file_extension.endswith(".txt"):
            #    # print(f"[HINT] Found .txt file: {os.path.basename(file_path)}. Please select the corresponding (.sps) syntax file to load it.")
            #    # continue

            if not file_extension.endswith((".sav", ".sps", ".spss")):
                continue

            try:
                print(f"[INFO] Processing: {os.path.basename(file_path)}")
                
                # Delegate to service layer
                results_map = self.service.load_and_label(file_path, country_code_val, save_unlabeled_flag)
                
                if not results_map:
                    print(f"[WARN] No data found for country '{country_code_val}' in {os.path.basename(file_path)}")
                    continue

                for output_path, labeled_dataframe in results_map.items():
                    labeled_files_output.append(output_path)
                    
                    # Update GUI state
                    self.context.file_results[output_path] = {"labeled": labeled_dataframe}
                    
                    # Refresh file tree
                    parent_directory = os.path.dirname(output_path)
                    self.gui.file_manager.refresh_folder(parent_directory)
                    
                    # Schedule UI updates on main thread
                    self.gui.root.after(0, lambda p=output_path: self.gui.file_manager.select_file(p))
                    self.gui.root.after(0, lambda p=output_path: self.gui.column_display.display_columns_for_file(p))
                    self.gui.root.after(0, lambda d=labeled_dataframe: self.auto_detect_and_fill_ids(d))
                    
                    print(f"[SUCCESS] Saved: {os.path.basename(output_path)}")
                    print(f"[HINT] Next step: 'Clean' or 'Transform'")

            except Exception as error:
                print(f"[ERROR] Failed to label {os.path.basename(file_path)}: {error}")

        print(f"[INFO] Load & Label completed. {len(labeled_files_output)} file(s) ready.")
        return labeled_files_output

    def _process_clean(self, input_files: List[str]) -> List[str]:
        """
        Step 2: Clean the data by removing 'garbage' columns.
        Garbage includes: too many missing values, uniform values, or highly correlated duplicates.
        """
        cleaned_files_output = []
        
        # User inputs
        user_score_col = self.gui.entry_score.get().strip()
        user_school_col = self.gui.entry_school.get().strip()
        user_student_col = self.gui.entry_student.get().strip()
        
        # Thresholds configuration
        missing_threshold = self.gui.missing_thr.get()
        uniform_threshold = self.gui.uniform_thr.get()
        correlation_threshold = self.gui.correlation_thr.get()

        print(f"\n[STEP 2/3] Starting Cleaning for {len(input_files)} file(s)...")
        print(f"[CONFIG] Thresholds: Missing>{missing_threshold}, Uniform>{uniform_threshold}, Corr>{correlation_threshold}")

        for file_path in input_files:
            # Skip output files to prevent re-processing outputs if folder is selected
            if "cleaned" in file_path or "leveled" in file_path: 
                continue

            try:
                # 1. Resolve dataframe (from memory or disk)
                file_record = self.context.file_results.get(file_path, {})
                dataframe = next(
                    (v for v in [file_record.get("cleaned"), file_record.get("labeled"), file_record.get("transformed")] if v is not None), 
                    None
                )
                
                if dataframe is None:
                    if os.path.exists(file_path):
                        if file_path.lower().endswith(".sav"):
                            print(f"[ERROR] Cannot clean raw .sav: {os.path.basename(file_path)}. Run 'Load & Label' first.")
                            continue
                        try:
                            dataframe = pd.read_csv(file_path, encoding="cp1252")
                        except Exception as error_load:
                            print(f"[ERROR] Invalid CSV {os.path.basename(file_path)}: {error_load}")
                            continue
                    else:
                        print(f"[WARN] File not found: {file_path}")
                        continue

                # 2. Determine ID columns (Score, School, Student)
                # Prioritize user input, fallback to auto-detection
                current_score = user_score_col
                current_school = user_school_col
                current_student = user_student_col

                # Check if user-provided columns actually exist in this file
                if current_score and current_score not in dataframe.columns:
                    print(f"[WARN] Column '{current_score}' missing in {os.path.basename(file_path)}. Auto-detecting...")
                    current_score = None
                if current_school and current_school not in dataframe.columns:
                    print(f"[WARN] Column '{current_school}' missing in {os.path.basename(file_path)}. Auto-detecting...")
                    current_school = None
                if current_student and current_student not in dataframe.columns:
                    print(f"[WARN] Column '{current_student}' missing in {os.path.basename(file_path)}. Auto-detecting...")
                    current_student = None
                
                # If any ID is missing, run auto-detection
                if not all([current_score, current_school, current_student]):
                    auto_score, auto_school, auto_student = self.service.auto_detect_ids(dataframe)
                    current_score = current_score or auto_score
                    current_school = current_school or auto_school
                    current_student = current_student or auto_student
                
                # Report active columns
                print(f"[INFO] Using IDs: Score='{current_score}', School='{current_school}', Student='{current_student}'")

                # 3. Clean the file
                result = self.service.clean_file(
                    file_path, dataframe, current_score, current_school, current_student, 
                    missing_threshold, uniform_threshold, correlation_threshold
                )
                
                if result:
                    output_path, cleaned_dataframe = result
                    self.context.file_results[output_path] = {"cleaned": cleaned_dataframe}
                    cleaned_files_output.append(output_path)
                    
                    self.gui.file_manager.refresh_folder(os.path.dirname(output_path))
                    
                    def update_ui_clean(path=output_path):
                        self.gui.file_manager.select_file(path)
                        self.gui.column_display.display_columns_for_file(path)
                    self.gui.root.after(0, update_ui_clean)
                    
                    print(f"[SUCCESS] Cleaned: {os.path.basename(output_path)}")

            except Exception as error:
                print(f"[ERROR] Cleaning failed for {os.path.basename(file_path)}: {error}")

        print(f"[INFO] Cleaning completed. {len(cleaned_files_output)} file(s) ready.")
        return cleaned_files_output

    def _process_transform(self, input_files: List[str]) -> None:
        """
        Step 3: Transform (Merge & Level) the data.
        Merges Student and School data, and converts scores to proficiency levels.
        """
        print(f"\n[STEP 3/3] Starting Transformation...")
        
        # 1. Prepare dataframes for merging
        dataframes_map: Dict[str, pd.DataFrame] = {}
        for file_path in input_files:
            if "leveled" in file_path: continue

            try:
                file_record = self.context.file_results.get(file_path, {})
                dataframe = file_record.get("cleaned")
                
                # If not in memory, try loading from disk
                if dataframe is None and os.path.exists(file_path):
                     if file_path.lower().endswith(".sav"):
                        print(f"[ERROR] Cannot transform .sav directly: {os.path.basename(file_path)}. Run 'Load & Label' first.")
                        continue
                     try:
                        dataframe = pd.read_csv(file_path, encoding="cp1252")
                     except Exception as error_load:
                        print(f"[ERROR] Invalid CSV {os.path.basename(file_path)}: {error_load}")
                        continue
                
                if dataframe is not None:
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    dataframes_map[base_name] = dataframe
            except Exception as error:
                print(f"[ERROR] Failed to prepare {os.path.basename(file_path)}: {error}")

        if not dataframes_map:
            print("[WARN] No valid files found to transform.")
            return

        print(f"[INFO] Aggregating {len(dataframes_map)} file(s) for merge...")

        # 2. Determine configuration (Columns & Splits)
        score_column = self.gui.entry_score.get().strip() or None
        school_column = self.gui.entry_school.get().strip() or None
        student_column = self.gui.entry_student.get().strip() or None

        # Auto-detect if missing
        if not score_column or not school_column or not student_column:
            (auto_score, auto_school, auto_student) = self.service.get_best_ids(dataframes_map)
            score_column = score_column or auto_score
            school_column = school_column or auto_school
            student_column = student_column or auto_student
        
        # Validation: Ensure score column exists in at least one dataframe
        found_score = any(score_column in df.columns for df in dataframes_map.values())
        if not found_score:
             print(f"[WARN] Score column '{score_column}' not found. Re-detecting...")
             (auto_score, _, _) = self.service.get_best_ids(dataframes_map)
             score_column = auto_score

        if not score_column:
            print("[ERROR] Score column could not be determined. Transformation aborted.")
            return

        print(f"[CONFIG] Target Score: '{score_column}'")
        print(f"[CONFIG] Merge Keys: {[c for c in [student_column, school_column] if c]}")

        dataset_split_ranges = None
        if self.gui.split_dataset_var.get():
             dataset_split_ranges = self._parse_split_ranges(self.gui.split_ranges_var.get().strip())
             print(f"[CONFIG] Splitting enabled: {dataset_split_ranges}")

        merge_keys = [c for c in [student_column, school_column] if c]
        
        # Determine root output directory
        first_input_file = input_files[0]
        parent_dir = os.path.dirname(first_input_file)
        if os.path.basename(parent_dir) in ["cleaned", "labeled"]:
            root_data_dir = os.path.dirname(parent_dir)
        else:
            root_data_dir = parent_dir

        # 3. Execute Transformation
        results_map = self.service.transform_files(
            dataframes_map, score_column, merge_keys, dataset_split_ranges, root_data_dir
        )
        
        for output_path, transformed_df in results_map.items():
            self.context.file_results[output_path] = {"transformed": transformed_df}
            self.gui.file_manager.refresh_folder(os.path.dirname(output_path))
            
            def update_ui_transform(path=output_path):
                self.gui.file_manager.select_file(path)
                self.gui.column_display.display_columns_for_file(path)
            self.gui.root.after(0, update_ui_transform)
            
            print(f"[SUCCESS] Transformed: {os.path.basename(output_path)}")
            
        print("[INFO] Transformation completed.")

    def action_load_label(self) -> None:
        """Button callback: Load & Label"""
        files = self._get_files_to_process()
        if not files: return
        threading.Thread(target=lambda: self._process_label(files), daemon=True).start()

    def action_clean(self) -> None:
        """Button callback: Clean"""
        files = self._get_files_to_process()
        if not files: return
        threading.Thread(target=lambda: self._process_clean(files), daemon=True).start()

    def action_transform(self) -> None:
        """Button callback: Transform"""
        files = self._get_files_to_process()
        if not files: # Fallback to looking for cleaned files
             files = [p for p, v in self.context.file_results.items() if "cleaned" in v]
        
        if not files:
            print("[WARN] No files selected or available for transform.")
            return

        threading.Thread(target=lambda: self._process_transform(files), daemon=True).start()

    def run_full_pipeline(self) -> None:
        """Button callback: Run All Steps Sequentially"""
        files = self._get_files_to_process()
        if not files: return

        def pipeline_worker():
            print(f"\n=== STARTING FULL PIPELINE ({len(files)} files) ===")
            
            # Step 1
            labeled_files = self._process_label(files)
            if not labeled_files: 
                print("[WARN] Pipeline stopped: No files labeled.")
                return
            
            # Step 2
            cleaned_files = self._process_clean(labeled_files)
            if not cleaned_files: 
                print("[WARN] Pipeline stopped: No files cleaned.")
                return
            
            # Step 3
            self._process_transform(cleaned_files)
            print("\n=== FULL PIPELINE COMPLETED ===")

        threading.Thread(target=pipeline_worker, daemon=True).start()