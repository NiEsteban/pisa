import os
import argparse
from typing import List, Tuple
import pandas as pd
from pisa_pipeline.data_processing.pipeline_service import PipelineService
from pisa_pipeline.utils.file_utils import resolve_folder_path, is_file

def main():
    parser = argparse.ArgumentParser(description="PISA Data Pipeline CLI")
    
    # Input/Output
    parser.add_argument("-f", "--folder", type=str, default="raw_data", help="Input file or folder")
    parser.add_argument("-s", "--save_unlabel", action="store_true", help="Save unlabeled CSVs")
    
    # Process Flags (Default: Run all if none specified)
    parser.add_argument("--only-label", action="store_true", help="Run only Label step")
    parser.add_argument("--only-clean", action="store_true", help="Run only Clean step")
    parser.add_argument("--only-transform", action="store_true", help="Run only Transform step")
    
    # Columns
    parser.add_argument("-scr", "--score_col", type=str, help="Score column name")
    parser.add_argument("-sch", "--school_id_col", type=str, help="School ID column name")
    parser.add_argument("-stu", "--student_id_col", type=str, help="Student ID column name")
    parser.add_argument("-c", "--country_code", type=str, default="MEX", help="Country code (e.g. MEX)")
    
    # Thresholds
    parser.add_argument("-mt", "--missing_threshold", type=float, default=1.0, help="Missing value threshold (0-1)")
    parser.add_argument("-ut", "--uniform_threshold", type=float, default=1.0, help="Uniformity threshold (0-1)")
    parser.add_argument("-ct", "--correlation_threshold", type=float, default=1.0, help="Correlation threshold (0-1)")
    
    # Splitting
    parser.add_argument("-sd", "--split_dataset", action="store_true", help="Enable dataset splitting")
    parser.add_argument("-sr", "--split_ranges", type=str, help="Split ranges (e.g. '0:10, 20:30')")

    args = parser.parse_args()
    
    # Determine steps
    run_label = True
    run_clean = True
    run_transform = True
    
    if args.only_label or args.only_clean or args.only_transform:
        run_label = args.only_label
        run_clean = args.only_clean
        run_transform = args.only_transform
        
        # Dependency logic: If generic run (e.g. just script.py), run all. 
        # But if specific flags, only run those.
        # NOTE: A full run usually requires sequential inputs.
        # If user asks --only-clean, we assume labeled files exist.
    
    path = resolve_folder_path(args.folder)
    if not os.path.exists(path):
        print(f"[ERROR] Path not found: {path}")
        return

    service = PipelineService()
    
    # -------------------------------------------------------------------------
    # 1. Collect Files
    # -------------------------------------------------------------------------
    files_to_process = []
    if os.path.isfile(path):
        files_to_process.append(path)
    else:
        for root, dirs, files in os.walk(path):
            # Skip output folders to prevent recursion loops
            if any(x in root for x in ["labeled", "cleaned", "leveled", ".backups"]):
                continue
            for f in files:
                if f.lower().endswith(('.sav', '.csv')):
                    files_to_process.append(os.path.join(root, f))

    if not files_to_process:
        print("[INFO] No files found to process.")
        return

    print(f"[INFO] Found {len(files_to_process)} file(s).")
    
    # -------------------------------------------------------------------------
    # 2. Label Step
    # -------------------------------------------------------------------------
    labeled_files = []
    if run_label:
        print("\n--- STEP 1: LOAD & LABEL ---")
        for f in files_to_process:
            if f.lower().endswith(".sav"):
                print(f"Processing: {os.path.basename(f)}")
                try:
                    res = service.load_and_label(f, args.country_code, args.save_unlabel)
                    labeled_files.extend(res.keys())
                except Exception as e:
                    print(f"[ERROR] Failed to label {f}: {e}")
            elif f.lower().endswith(".csv") and "labeled" in f:
                 # Already labeled
                 labeled_files.append(f)
            else:
                 # Raw CSV? Treat as source if not saving
                 pass 

    # If skipped label step, try to find existing labeled files
    if not run_label:
         # Rough heuristic: look for 'labeled' folder or assume inputs are labeled
         for root, dirs, files in os.walk(path):
             if "labeled" in root:
                 for f in files:
                     if f.endswith(".csv"):
                         labeled_files.append(os.path.join(root, f))
         if not labeled_files and os.path.isfile(path) and path.endswith(".csv"):
             labeled_files.append(path)

    # -------------------------------------------------------------------------
    # 3. Clean Step
    # -------------------------------------------------------------------------
    cleaned_files = []
    if run_clean and labeled_files:
        print("\n--- STEP 2: CLEAN ---")
        for f in labeled_files:
            print(f"Cleaning: {os.path.basename(f)}")
            try:
                # Load df
                df = pd.read_csv(f, encoding="cp1252")
                
                # Auto Detect IDs if needed
                cur_score = args.score_col
                cur_school = args.school_id_col
                cur_student = args.student_id_col
                
                # Smart validation
                if cur_score and cur_score not in df.columns: cur_score = None
                if cur_school and cur_school not in df.columns: cur_school = None
                if cur_student and cur_student not in df.columns: cur_student = None

                if not all([cur_score, cur_school, cur_student]):
                    auto_s, auto_sch, auto_stu = service.auto_detect_ids(df)
                    cur_score = cur_score or auto_s
                    cur_school = cur_school or auto_sch
                    cur_student = cur_student or auto_stu
                
                res = service.clean_file(
                    f, df, cur_score, cur_school, cur_student,
                    args.missing_threshold, args.uniform_threshold, args.correlation_threshold
                )
                if res:
                    cleaned_files.append(res[0])
            except Exception as e:
                print(f"[ERROR] Failed to clean {f}: {e}")

    # If skipped clean step
    if not run_clean:
         for root, dirs, files in os.walk(path):
             if "cleaned" in root:
                 for f in files:
                     if f.endswith(".csv"):
                         cleaned_files.append(os.path.join(root, f))
         if not cleaned_files and not run_label and os.path.isfile(path): # fallback
             cleaned_files.append(path)

    # -------------------------------------------------------------------------
    # 4. Transform Step
    # -------------------------------------------------------------------------
    if run_transform and cleaned_files:
        print("\n--- STEP 3: TRANSFORM ---")
        
        # Parse ranges
        ranges = []
        if args.split_dataset and args.split_ranges:
            try:
                for part in args.split_ranges.split(','):
                    if ':' in part:
                        s, e = part.split(':')
                        ranges.append((int(s), int(e)))
            except:
                print("[ERROR] Invalid split ranges format. Use '0:10, 20:30'")
        
        # Load all into dict
        dfs_dict = {}
        for f in cleaned_files:
            try:
                df = pd.read_csv(f, encoding="cp1252")
                base = os.path.splitext(os.path.basename(f))[0]
                dfs_dict[base] = df
            except: pass
            
        if dfs_dict:
            # Detect best IDs global
            (auto_s, auto_sch, auto_stu) = service.get_best_ids(dfs_dict)
            s_col = args.score_col or auto_s
            sch_col = args.school_id_col or auto_sch
            stu_col = args.student_id_col or auto_stu
            
            ids_list = [c for c in [stu_col, sch_col] if c]
            
            if s_col:
                # Output dir
                root_out = path if os.path.isdir(path) else os.path.dirname(path)
                
                res = service.transform_files(dfs_dict, s_col, ids_list, ranges, root_out)
                print(f"[SUCCESS] Transformed {len(res)} files.")
            else:
                print("[ERROR] Could not determine score column for transformation.")
    
    print("\n[INFO] Pipeline finished.")

if __name__ == "__main__":
    main()
