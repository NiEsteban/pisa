#!/usr/bin/env python3
"""
PISA Stepwise Pipeline GUI (fixed edition)
- Fixed encoding issues for .sav files
- Fixed full pipeline to process all files as dictionary for transformer
- Better score column detection
"""

import os
import threading
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
from typing import List, Dict, Set, Optional

# Import your pipeline components
from pisa_pipeline.data_processing.sav_loader import SAVloader
from pisa_pipeline.data_processing.cleaner import CSVCleaner
from pisa_pipeline.data_processing.tranformer import Transformer
from pisa_pipeline.utils.io import save_dataframe_to_csv, load_sav_metadata


# ---------------------------
# Helpers
# ---------------------------
def read_csv_header(filepath: str) -> List[str]:
    """Read CSV header with multiple encoding attempts."""
    for encoding in ["utf-8", "cp1252", "latin1", "iso-8859-1"]:
        try:
            with open(filepath, "r", encoding=encoding, errors="ignore") as f:
                first_line = f.readline()
            cols = [c.strip() for c in first_line.strip().split(",")]
            return cols
        except Exception:
            continue
    return []


def get_columns_from_file(filepath: str) -> List[str]:
    """Get column names from file with proper encoding handling."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        return read_csv_header(filepath)
    elif ext == ".sav":
        try:
            meta = load_sav_metadata(filepath)
            return list(meta.column_names)
        except Exception as e:
            print(f"[WARN] Could not load .sav metadata: {e}")
            return []
    else:
        try:
            df = pd.read_csv(filepath, nrows=0)
            return list(df.columns)
        except Exception:
            return []


# ---------------------------
# Logging redirect
# ---------------------------
class TextRedirector:
    def __init__(self, text_widget: tk.Text):
        self.text_widget = text_widget

    def write(self, message: str):
        if not message:
            return
        self.text_widget.config(state="normal")
        tag = "info"
        if "[ERROR]" in message or message.lower().startswith("error"):
            tag = "error"
        elif "[PIPELINE]" in message or message.lower().startswith("pipeline"):
            tag = "pipeline"
        if not message.endswith("\n"):
            message = message + "\n"
        try:
            self.text_widget.insert("end", message, tag)
        finally:
            self.text_widget.config(state="disabled")
            self.text_widget.see("end")

    def flush(self):
        pass


# ---------------------------
# Main GUI class
# ---------------------------
class StepwisePipelineGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("PISA Stepwise Pipeline")
        self.root.geometry("1200x720")

        # state
        self.selected_folder: Optional[str] = None
        self.selected_files: List[str] = []
        self.file_results: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.columns_to_drop_map: Dict[str, Set[str]] = {}

        # UI style
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Accent.TButton", foreground="white", background="#007acc")
        style.map("Accent.TButton", background=[("active", "#2b88d8")])

        # ---------------------------
        # Top Frame
        # ---------------------------
        top_frame = ttk.Frame(root)
        top_frame.pack(fill="x", padx=10, pady=8)

        ttk.Button(top_frame, text="Select Folder", command=self.browse_folder, style="Accent.TButton").pack(side="left", padx=6)
        ttk.Button(top_frame, text="Select Files", command=self.browse_files, style="Accent.TButton").pack(side="left", padx=6)
        ttk.Button(top_frame, text="Clear Selection", command=self.clear_selection).pack(side="left", padx=6)

        self.path_var = tk.StringVar()
        ttk.Entry(top_frame, textvariable=self.path_var).pack(side="left", fill="x", expand=True, padx=10)

        # ---------------------------
        # Main Pane
        # ---------------------------
        main_pane = ttk.Frame(root)
        main_pane.pack(fill="both", expand=True, padx=10, pady=6)

        # LEFT: Files List
        left_frame = ttk.Frame(main_pane)
        left_frame.pack(side="left", fill="y")

        ttk.Label(left_frame, text="Files").pack(anchor="w")
        self.file_listbox = tk.Listbox(left_frame, height=28, selectmode="extended", exportselection=False, width=36)
        self.file_listbox.pack(side="left", fill="y", padx=(0, 4))
        file_scroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.file_listbox.yview)
        file_scroll.pack(side="left", fill="y")
        self.file_listbox.config(yscrollcommand=file_scroll.set)
        self.file_listbox.bind("<<ListboxSelect>>", self.on_file_select)
        self.file_listbox.bind("<Button-1>", self.on_file_click)

        # CENTER: Columns Display
        center_frame = ttk.Frame(main_pane)
        center_frame.pack(side="left", expand=True, fill="both", padx=12)

        ttk.Label(center_frame, text="Columns (check to DROP during Transform)").pack(anchor="w")

        self.col_canvas = tk.Canvas(center_frame)
        self.col_canvas.pack(side="left", fill="both", expand=True)
        self.col_scroll = ttk.Scrollbar(center_frame, orient="vertical", command=self.col_canvas.yview)
        self.col_scroll.pack(side="right", fill="y")
        self.col_canvas.configure(yscrollcommand=self.col_scroll.set)

        self.col_inner_frame = ttk.Frame(self.col_canvas)
        self.col_canvas.create_window((0, 0), window=self.col_inner_frame, anchor="nw")
        self.col_inner_frame.bind("<Configure>", lambda e: self.col_canvas.configure(scrollregion=self.col_canvas.bbox("all")))

        # Scroll bindings
        self.col_canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.col_inner_frame.bind("<MouseWheel>", self._on_mousewheel)
        self.col_canvas.bind("<Button-4>", lambda ev: self.col_canvas.yview_scroll(-1, "units"))
        self.col_canvas.bind("<Button-5>", lambda ev: self.col_canvas.yview_scroll(1, "units"))
        self.col_inner_frame.bind("<Button-4>", lambda ev: self.col_canvas.yview_scroll(-1, "units"))
        self.col_inner_frame.bind("<Button-5>", lambda ev: self.col_canvas.yview_scroll(1, "units"))

        self.current_col_vars: Dict[str, tk.BooleanVar] = {}
        self.current_displayed_file: Optional[str] = None

        # RIGHT: Actions Panel
        right_frame = ttk.Frame(main_pane, width=280)
        right_frame.pack(side="right", fill="y", padx=(8, 0))

        self.actions_container = ttk.Frame(right_frame)
        self.actions_container.place(relx=0.5, rely=0.5, anchor="center")

        # ID Detection
        id_frame = ttk.LabelFrame(self.actions_container, text="Detected / Edit IDs")
        id_frame.pack(fill="x", pady=(0, 12), ipadx=4, ipady=4)
        
        ttk.Label(id_frame, text="Score column:").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        ttk.Label(id_frame, text="School ID:").grid(row=1, column=0, sticky="e", padx=4, pady=4)
        ttk.Label(id_frame, text="Student ID:").grid(row=2, column=0, sticky="e", padx=4, pady=4)

        self.entry_score = ttk.Entry(id_frame, width=24)
        self.entry_school = ttk.Entry(id_frame, width=24)
        self.entry_student = ttk.Entry(id_frame, width=24)
        self.entry_score.grid(row=0, column=1, padx=4, pady=2)
        self.entry_school.grid(row=1, column=1, padx=4, pady=2)
        self.entry_student.grid(row=2, column=1, padx=4, pady=2)

        # Load & Label Section
        label_frame = ttk.LabelFrame(self.actions_container, text="1. Load & Label")
        label_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)
        
        self.save_unlabel_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(label_frame, text="Save unlabeled", variable=self.save_unlabel_var).pack(anchor="w", padx=4, pady=2)
        
        ttk.Label(label_frame, text="Country code:").pack(anchor="w", padx=4)
        self.country_code = tk.StringVar(value="MEX")
        ttk.Entry(label_frame, textvariable=self.country_code, width=10).pack(anchor="w", padx=4, pady=(0, 4))
        
        ttk.Button(label_frame, text="Load & Label", command=self.action_load_label, style="Accent.TButton").pack(fill="x", padx=4, pady=4)

        # Clean Section
        clean_frame = ttk.LabelFrame(self.actions_container, text="2. Clean")
        clean_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)
        
        ttk.Label(clean_frame, text="Missing threshold:").pack(anchor="w", padx=4)
        self.missing_thr = tk.DoubleVar(value=1.0)
        ttk.Entry(clean_frame, textvariable=self.missing_thr, width=10).pack(anchor="w", padx=4, pady=(0, 4))
        
        ttk.Label(clean_frame, text="Uniform threshold:").pack(anchor="w", padx=4)
        self.uniform_thr = tk.DoubleVar(value=1.0)
        ttk.Entry(clean_frame, textvariable=self.uniform_thr, width=10).pack(anchor="w", padx=4, pady=(0, 4))
        
        ttk.Button(clean_frame, text="Clean", command=self.action_clean, style="Accent.TButton").pack(fill="x", padx=4, pady=4)

        # Transform Section
        transform_frame = ttk.LabelFrame(self.actions_container, text="3. Transform")
        transform_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)
        
        self.drop_during_transform_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(transform_frame, text="Apply column drops", variable=self.drop_during_transform_var).pack(anchor="w", padx=4, pady=2)
        
        ttk.Button(transform_frame, text="Transform", command=self.action_transform, style="Accent.TButton").pack(fill="x", padx=4, pady=4)

        # Full Pipeline
        ttk.Button(self.actions_container, text="Run Full Pipeline", command=self.run_full_pipeline, style="Accent.TButton").pack(fill="x", pady=8)

        # ---------------------------
        # Bottom: Log
        # ---------------------------
        bottom_frame = ttk.Frame(root)
        bottom_frame.pack(fill="both", padx=10, pady=(6, 10), expand=True)

        log_frame = ttk.LabelFrame(bottom_frame, text="Log")
        log_frame.pack(side="left", fill="both", expand=True)
        
        self.text_log = tk.Text(log_frame, height=10, state="disabled", wrap="none")
        self.text_log.tag_config("error", foreground="red")
        self.text_log.tag_config("pipeline", foreground="#007acc")
        self.text_log.tag_config("info", foreground="gray20")
        self.text_log.pack(fill="both", expand=True)
        
        sys.stdout = TextRedirector(self.text_log)
        sys.stderr = TextRedirector(self.text_log)

    # ---------------------------
    # File Management
    # ---------------------------
    def browse_folder(self):
        folder = filedialog.askdirectory()
        if not folder:
            return
        self.selected_folder = folder
        self.path_var.set(folder)
        self.populate_file_list(folder)

    def browse_files(self):
        files = filedialog.askopenfilenames(filetypes=[("Data files", "*.sav *.csv")])
        if files:
            self.selected_files = list(files)
            self.path_var.set("; ".join(self.selected_files))
            self.file_listbox.delete(0, "end")
            for f in self.selected_files:
                self.file_listbox.insert("end", os.path.basename(f))

    def clear_selection(self):
        self.file_listbox.selection_clear(0, "end")
        self.display_columns_for_file(None)

    def populate_file_list(self, folder: str):
        self.file_listbox.delete(0, "end")
        self.selected_files = []
        try:
            for fname in sorted(os.listdir(folder)):
                if fname.lower().endswith((".sav", ".csv")):
                    path = os.path.join(folder, fname)
                    self.selected_files.append(path)
                    self.file_listbox.insert("end", os.path.basename(path))
        except Exception as e:
            print(f"[ERROR] Could not list folder: {e}")

    def on_file_click(self, event):
        widget = event.widget
        index = widget.nearest(event.y)
        if index >= widget.size() or index < 0:
            self.file_listbox.selection_clear(0, "end")
            self.display_columns_for_file(None)

    def on_file_select(self, event):
        sel = self.file_listbox.curselection()
        if not sel:
            self.display_columns_for_file(None)
            return

        # Use the last selected index
        idx = sel[-1]
        file_path = self.selected_files[idx]
        self.display_columns_for_file(file_path)


    # ---------------------------
    # Columns Display
    # ---------------------------
    def display_columns_for_file(self, file_path):
        # Clear display
        self.col_canvas.delete("all")
        self.col_inner_frame.destroy()
        self.col_inner_frame = ttk.Frame(self.col_canvas)
        self.col_canvas.create_window((0, 0), window=self.col_inner_frame, anchor="nw")
        self.col_inner_frame.bind("<Configure>", lambda e: self.col_canvas.configure(scrollregion=self.col_canvas.bbox("all")))
        self.col_inner_frame.bind("<MouseWheel>", self._on_mousewheel)
        self.col_inner_frame.bind("<Button-4>", lambda ev: self.col_canvas.yview_scroll(-1, "units"))
        self.col_inner_frame.bind("<Button-5>", lambda ev: self.col_canvas.yview_scroll(1, "units"))
        self.current_col_vars.clear()
        self.current_displayed_file = None
        if file_path is None:
            return
        try:
            # Try to get dataframe from self.file_results first
            fr = self.file_results.get(file_path, {})
            df_src = next((df for df in [
                fr.get("transformed"),
                fr.get("cleaned"),
                fr.get("labeled")
            ] if df is not None), None)
            # If not in memory, load from disk
            if df_src is None and os.path.exists(file_path):
                ext = os.path.splitext(file_path)[1].lower()
                if ext == ".csv":
                    for encoding in ["cp1252", "utf-8", "latin1", "iso-8859-1"]:
                        try:
                            df_src = pd.read_csv(file_path, encoding=encoding, nrows=0)
                            break
                        except Exception:
                            continue
                elif ext == ".sav":
                    try:
                        cols = get_columns_from_file(file_path)
                        df_src = pd.DataFrame(columns=cols)
                    except Exception as e:
                        print(f"[ERROR] Failed to read .sav file columns: {e}")
                        return
            if df_src is None:
                print(f"[WARN] No data available for {file_path}")
                return
            cols = df_src.columns.tolist()
            self.current_displayed_file = file_path
            # Get existing drops
            existing_drops = self.columns_to_drop_map.get(file_path, set())
            # Create checkboxes
            for col in cols:
                var = tk.BooleanVar(value=(col in existing_drops))
                row_frame = ttk.Frame(self.col_inner_frame)
                row_frame.pack(anchor="w", pady=1, padx=3, fill="x")
                chk = ttk.Checkbutton(
                    row_frame,
                    variable=var,
                    command=lambda c=col, v=var: self._on_col_check_toggle(c, v)
                )
                chk.pack(side="left")
                lbl = ttk.Label(row_frame, text=col, cursor="hand2")
                lbl.pack(side="left", padx=(4, 0))
                lbl.bind("<Button-1>", lambda e, c=col: self._on_column_label_click(c))
                for widget in [chk, lbl, row_frame]:
                    widget.bind("<MouseWheel>", self._on_mousewheel)
                    widget.bind("<Button-4>", lambda ev: self.col_canvas.yview_scroll(-1, "units"))
                    widget.bind("<Button-5>", lambda ev: self.col_canvas.yview_scroll(1, "units"))
                self.current_col_vars[col] = var
            print(f"[INFO] Displaying {len(cols)} columns for {os.path.basename(file_path)}")
        except Exception as e:
            print(f"[ERROR] Failed to display columns for {file_path}: {e}")


        except Exception as e:
            print(f"[ERROR] Failed to display columns for {file_path}: {e}")

    def _on_column_label_click(self, col_name: str):
        print(f"[INFO] Clicked on column: {col_name}")

    def _on_col_check_toggle(self, col: str, var: tk.BooleanVar):
        if self.current_displayed_file is None:
            return
        
        drop_set = self.columns_to_drop_map.setdefault(self.current_displayed_file, set())
        if var.get():
            drop_set.add(col)
        else:
            drop_set.discard(col)

    def _on_mousewheel(self, event):
        self.col_canvas.yview_scroll(-int(event.delta / 120), "units")

    # ---------------------------
    # ID Detection with Better Score Detection
    # ---------------------------
    def auto_detect_and_fill_ids(self, df: pd.DataFrame):
        """Auto-detect IDs with improved score column detection."""
        
        cleaner = CSVCleaner()
        auto_score, auto_school, auto_student = cleaner.detect_columns(df)
        
        # Update entries if empty
        if auto_score:
            self.entry_score.delete(0, "end")
            self.entry_score.insert(0, auto_score)
        if auto_school:
            self.entry_school.delete(0, "end")
            self.entry_school.insert(0, auto_school)
        if auto_student:
            self.entry_student.delete(0, "end")
            self.entry_student.insert(0, auto_student)
        
        print(f"[PIPELINE] Auto-detected: score={auto_score}, school={auto_school}, student={auto_student}")
        return auto_score, auto_school, auto_student
    
    def get_best_ids(self, dfs_dict):
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



    # ---------------------------
    # Pipeline Actions
    # ---------------------------
    def _get_files_to_process(self) -> List[str]:
        sel = self.file_listbox.curselection()
        if sel:
            return [self.selected_files[i] for i in sel]
        # If nothing selected, return all files (use the latest paths)
        return [f for f in self.selected_files if f.endswith(("_labeled.csv", "_cleaned.csv", ".csv", ".sav"))]


    def action_load_label(self):
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files selected or available.")
            return

        def worker():
            for f in files:
                try:
                    print(f"[PIPELINE] Loading and labeling: {f}")
                    loader = SAVloader()
                    df_labeled, df_unlabeled = loader.run(f, self.country_code.get())
                    
                    if df_labeled is None:
                        print(f"[PIPELINE] Skipping {f} (no labeled rows).")
                        continue
                    
                    base = os.path.splitext(os.path.basename(f))[0]
                    out_path = os.path.join(os.path.dirname(f), f"{base}_labeled.csv")
                    save_dataframe_to_csv(df_labeled, out_path)
                    
                    self.file_results[out_path] = {"labeled": df_labeled}
                    self._replace_file_in_list(f, out_path)
                    self._select_single_file_in_list(out_path)
                    
                    self.root.after(0, lambda p=out_path: self.display_columns_for_file(p))
                    
                    # Auto-detect IDs
                    self.root.after(0, lambda d=df_labeled: self.auto_detect_and_fill_ids(d))
                    
                    if self.save_unlabel_var.get() and df_unlabeled is not None:
                        unl_out = os.path.join(os.path.dirname(f), f"{base}_unlabeled.csv")
                        save_dataframe_to_csv(df_unlabeled, unl_out)
                        print(f"[PIPELINE] Saved unlabeled → {unl_out}")
                    
                    print(f"[PIPELINE] Labeled file saved: {out_path}")
                except Exception as e:
                    print(f"[ERROR] {f}: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def action_clean(self):
        files = self._get_files_to_process()
        if not files:
            messagebox.showwarning("No files", "No files available.")
            return
        def worker():
            for f in files:
                try:
                    # Always use the latest file path
                    fr = self.file_results.get(f, {})
                    df = next((v for v in [
                        fr.get("cleaned"),
                        fr.get("labeled"),
                        fr.get("transformed")
                    ] if v is not None), None)
                    if df is None:
                        # If not in memory, try to load from disk
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
                    score_col = self.entry_score.get().strip()
                    school_col = self.entry_school.get().strip()
                    student_col = self.entry_student.get().strip()
                    if not all([score_col, school_col, student_col]):
                        auto_s, auto_school, auto_stu = self.auto_detect_and_fill_ids(df)
                        score_col = score_col or auto_s
                        school_col = school_col or auto_school
                        student_col = student_col or auto_stu
                    base = os.path.splitext(os.path.basename(f))[0]
                    df_clean = cleaner.run(
                        df,
                        base,
                        [student_col, school_col],
                        self.missing_thr.get(),
                        self.uniform_thr.get(),
                    )
                    out = os.path.join(os.path.dirname(f), f"{base}_cleaned.csv")
                    save_dataframe_to_csv(df_clean, out)
                    print(f"[PIPELINE] Cleaned saved: {out}")
                    self.file_results[out] = {"cleaned": df_clean}
                    self._replace_file_in_list(f, out)
                    self._select_single_file_in_list(out)
                    self.root.after(0, lambda p=out: self.display_columns_for_file(p))
                except Exception as e:
                    print(f"[ERROR] Cleaning {f}: {e}")
        threading.Thread(target=worker, daemon=True).start()


    def action_transform(self):
        """Transform files using dictionary approach."""
        files = self._get_files_to_process()
        if not files:
            # If no files selected, find all files with "cleaned" data
            files = [p for p, v in self.file_results.items() if "cleaned" in v]
            # Also look for files with "_cleaned.csv" in the directory
            if not files:
                for f in self.selected_files:
                    base, ext = os.path.splitext(f)
                    cleaned_path = f"{base}_cleaned.csv"
                    if os.path.exists(cleaned_path):
                        files.append(cleaned_path)
        if not files:
            print("[PIPELINE] No files to transform.")
            return

        def worker():
            dfs_dict = {}
            for f in files:
                try:
                    fr = self.file_results.get(f, {})
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

            # Get user-entered IDs
            s_col = self.entry_score.get().strip()  or None
            sch_col = self.entry_school.get().strip() or None
            stu_col = self.entry_student.get().strip() or None

            # Only auto-detect missing IDs
            if not s_col or not sch_col or not stu_col:
                (auto_s, auto_sch, auto_stu) = self.get_best_ids(dfs_dict)

                # Fill only the missing ones
                s_col = s_col or auto_s
                sch_col = sch_col or auto_sch
                stu_col = stu_col or auto_stu


            # Check if score column is valid (use best_df, not the first DataFrame) 
            if not s_col:
                print("[ERROR] No valid score column detected. Cannot transform.",s_col)
                return

            # Debug: Print detected columns

            # Apply drops if enabled
            if self.drop_during_transform_var.get():
                print("[PIPELINE] Applying column drops...")
                transformer = Transformer()
                for file_path in files:
                    base = os.path.splitext(os.path.basename(file_path))[0]
                    if base in dfs_dict:
                        drops = self.columns_to_drop_map.get(file_path, set())
                        if drops:
                            print(f"[PIPELINE] Dropping {len(drops)} columns from {base}")
                            dfs_dict[base] = transformer.drop_unwanted_columns(
                                dfs_dict[base],
                                user_drop_cols=list(drops)
                            )

            # Transform all files together
            transformer = Transformer()
            ids_list = [c for c in [stu_col, sch_col] if c]
            print(f"[PIPELINE] Transforming {len(dfs_dict)} file(s) with score_col={s_col}...")
            transformed = transformer.run(
                dfs=dfs_dict,
                score_col=s_col,
                ids_col=ids_list
            )

            # Save results
            first_file = files[0]
            outdir = os.path.join(os.path.dirname(first_file), "leveled")
            os.makedirs(outdir, exist_ok=True)
            for base_name, df_t in transformed.items():
                try:
                    outpath = os.path.join(outdir, f"{base_name}_leveled.csv")
                    save_dataframe_to_csv(df_t, outpath)
                    self.file_results[outpath] = {"transformed": df_t}
                    self._add_file_to_list_if_missing(outpath)
                    self._select_single_file_in_list(outpath)
                    self.root.after(0, lambda p=outpath: self.display_columns_for_file(p))
                    print(f"[PIPELINE] Transformed saved: {outpath}")
                except Exception as e:
                    print(f"[ERROR] Saving transformed {base_name}: {e}")

        threading.Thread(target=worker, daemon=True).start()



    def run_full_pipeline(self):
        return
    #     """Run complete pipeline: Load → Label → Clean → Transform ALL files as dictionary."""
    #     files = self._get_files_to_process()
    #     if not files:
    #         files = list(self.selected_files)
    #     if not files:
    #         messagebox.showwarning("No files", "No files to run pipeline on.")
    #         return

    #     def worker_sequence():
    #         # Step 1: Load & Label ALL files first
    #         print(f"[PIPELINE] ========== STEP 1: LOAD & LABEL ==========")
    #         labeled_files = []
    #         labeled_dfs = {}
            
    #         for f in files:
    #             try:
    #                 print(f"[PIPELINE] Loading: {os.path.basename(f)}")
    #                 loader = SAVloader()
    #                 df_labeled, df_unlabeled = loader.run(f, self.country_code.get())
                    
    #                 if df_labeled is None:
    #                     print(f"[PIPELINE] Skipping {f} (no labeled rows).")
    #                     continue
                    
    #                 base = os.path.splitext(os.path.basename(f))[0]
    #                 labeled_path = os.path.join(os.path.dirname(f), f"{base}_labeled.csv")
    #                 save_dataframe_to_csv(df_labeled, labeled_path)
                    
    #                 self.file_results[labeled_path] = {"labeled": df_labeled}
    #                 self._replace_file_in_list(f, labeled_path)
                    
    #                 labeled_files.append(labeled_path)
    #                 labeled_dfs[base] = df_labeled
                    
    #                 print(f"[PIPELINE] ✓ Labeled: {os.path.basename(labeled_path)}")
                    
    #             except Exception as e:
    #                 print(f"[ERROR] Loading {f}: {e}")
            
    #         if not labeled_files:
    #             print("[ERROR] No files were successfully labeled. Stopping.")
    #             return
            
    #         # Step 2: Auto-detect IDs from first labeled file
    #         print(f"\n[PIPELINE] ========== STEP 2: DETECT IDs ==========")
    #         first_df = next(iter(labeled_dfs.values()))
            
    #         try:
    #             # Use GUI method to detect and fill entries
    #             score_col, school_col, student_col = None, None, None
                
    #             # Try cleaner detection first
    #             cleaner = CSVCleaner()
    #             try:
    #                 score_col, school_col, student_col = cleaner.detect_columns(first_df)
    #             except Exception:
    #                 pass
                
    #             # Fallback: manual detection with better keywords
    #             if not score_col:
    #                 cols = first_df.columns.tolist()
    #                 for c in cols:
    #                     cl = c.lower()
    #                     if any(kw in cl for kw in ["plausible value", "pv1math", "pv", "math", "score"]):
    #                         score_col = c
    #                         break
                
    #             if not school_col:
    #                 cols = first_df.columns.tolist()
    #                 for c in cols:
    #                     cl = c.lower()
    #                     if any(kw in cl for kw in ["school", "schid", "sch_id"]):
    #                         school_col = c
    #                         break
                
    #             if not student_col:
    #                 cols = first_df.columns.tolist()
    #                 for c in cols:
    #                     cl = c.lower()
    #                     if any(kw in cl for kw in ["student", "stuid", "idstud"]):
    #                         student_col = c
    #                         break
                
    #             # Update GUI entries
    #             self.root.after(0, lambda: self.entry_score.delete(0, "end"))
    #             self.root.after(0, lambda s=score_col: self.entry_score.insert(0, s or ""))
    #             self.root.after(0, lambda: self.entry_school.delete(0, "end"))
    #             self.root.after(0, lambda s=school_col: self.entry_school.insert(0, s or ""))
    #             self.root.after(0, lambda: self.entry_student.delete(0, "end"))
    #             self.root.after(0, lambda s=student_col: self.entry_student.insert(0, s or ""))
                
    #             print(f"[PIPELINE] Detected IDs:")
    #             print(f"           Score   → {score_col}")
    #             print(f"           School  → {school_col}")
    #             print(f"           Student → {student_col}")
                
    #         except Exception as e:
    #             print(f"[ERROR] ID detection failed: {e}")
    #             score_col = school_col = student_col = None
            
    #         # Step 3: Clean ALL files
    #         print(f"\n[PIPELINE] ========== STEP 3: CLEAN ==========")
    #         cleaned_files = []
    #         cleaned_dfs = {}
            
    #         for labeled_path in labeled_files:
    #             try:
    #                 base = os.path.splitext(os.path.basename(labeled_path))[0].replace("_labeled", "")
    #                 df = labeled_dfs.get(base) or self.file_results.get(labeled_path, {}).get("labeled")
                    
    #                 if df is None:
    #                     print(f"[PIPELINE] Skipping {labeled_path} (no data).")
    #                     continue
                    
    #                 if not (school_col and student_col):
    #                     print(f"[PIPELINE] Missing IDs for {base}, skipping clean.")
    #                     continue
                    
    #                 print(f"[PIPELINE] Cleaning: {base}")
    #                 cleaner = CSVCleaner()
    #                 df_cleaned = cleaner.run(
    #                     df,
    #                     base,
    #                     [student_col, school_col],
    #                     self.missing_thr.get(),
    #                     self.uniform_thr.get(),
    #                 )
                    
    #                 cleaned_path = os.path.join(os.path.dirname(labeled_path), f"{base}_cleaned.csv")
    #                 save_dataframe_to_csv(df_cleaned, cleaned_path)
                    
    #                 self.file_results[cleaned_path] = {"cleaned": df_cleaned}
    #                 self._replace_file_in_list(labeled_path, cleaned_path)
                    
    #                 cleaned_files.append(cleaned_path)
    #                 cleaned_dfs[base] = df_cleaned
                    
    #                 print(f"[PIPELINE] ✓ Cleaned: {os.path.basename(cleaned_path)}")
                    
    #             except Exception as e:
    #                 print(f"[ERROR] Cleaning {labeled_path}: {e}")
            
    #         if not cleaned_files:
    #             print("[ERROR] No files were successfully cleaned. Stopping.")
    #             return
            
    #         # Step 4: Transform ALL files together as dictionary
    #         print(f"\n[PIPELINE] ========== STEP 4: TRANSFORM ==========")
            
    #         if not score_col:
    #             print("[ERROR] No score column detected. Cannot transform.")
    #             print("[INFO] Please set score column manually and run Transform step.")
    #             return
            
    #         try:
    #             # Apply column drops if enabled
    #             if self.drop_during_transform_var.get():
    #                 print("[PIPELINE] Applying column drops...")
    #                 transformer = Transformer()
                    
    #                 for cleaned_path in cleaned_files:
    #                     base = os.path.splitext(os.path.basename(cleaned_path))[0].replace("_cleaned", "")
    #                     if base in cleaned_dfs:
    #                         drops = self.columns_to_drop_map.get(cleaned_path, set())
    #                         if drops:
    #                             print(f"[PIPELINE] Dropping {len(drops)} columns from {base}")
    #                             cleaned_dfs[base] = transformer.drop_unwanted_columns(
    #                                 cleaned_dfs[base], 
    #                                 user_drop_cols=list(drops)
    #                             )
                
    #             # Transform all files together
    #             transformer = Transformer()
    #             ids_list = [c for c in [student_col, school_col] if c]
                
    #             print(f"[PIPELINE] Transforming {len(cleaned_dfs)} file(s) as dictionary...")
    #             transformed = transformer.run(
    #                 dfs=cleaned_dfs,
    #                 score_col=score_col,
    #                 ids_col=ids_list
    #             )
                
    #             # Save transformed files
    #             first_cleaned = cleaned_files[0]
    #             outdir = os.path.join(os.path.dirname(first_cleaned), "leveled")
    #             os.makedirs(outdir, exist_ok=True)
                
    #             for base_name, df_t in transformed.items():
    #                 try:
    #                     outpath = os.path.join(outdir, f"{base_name}_leveled.csv")
    #                     save_dataframe_to_csv(df_t, outpath)
                        
    #                     self.file_results[outpath] = {"transformed": df_t}
    #                     self._add_file_to_list_if_missing(outpath)
                        
    #                     print(f"[PIPELINE] ✓ Transformed: {os.path.basename(outpath)}")
                        
    #                 except Exception as e:
    #                     print(f"[ERROR] Saving {base_name}: {e}")
                
    #             print(f"\n[PIPELINE] ========== PIPELINE COMPLETE ==========")
    #             print(f"[PIPELINE] Processed {len(transformed)} file(s) successfully!")
                
    #         except Exception as e:
    #             print(f"[ERROR] Transform step failed: {e}")

    #     threading.Thread(target=worker_sequence, daemon=True).start()

    # ---------------------------
    # File List Management
    # ---------------------------
    def _replace_file_in_list(self, old_path: str, new_path: str):
        try:
            idx = self.selected_files.index(old_path)
            self.selected_files[idx] = new_path
            self.file_listbox.delete(idx)
            self.file_listbox.insert(idx, os.path.basename(new_path))
            self.file_listbox.selection_clear(0, "end")
            self.file_listbox.selection_set(idx)
            
            if old_path in self.columns_to_drop_map:
                self.columns_to_drop_map[new_path] = self.columns_to_drop_map.pop(old_path)
            if old_path in self.file_results:
                self.file_results[new_path] = self.file_results.pop(old_path)
        except ValueError:
            self._add_file_to_list_if_missing(new_path)

    def _add_file_to_list_if_missing(self, path: str):
        if path not in self.selected_files:
            self.selected_files.append(path)
            self.file_listbox.insert("end", os.path.basename(path))

    def _select_single_file_in_list(self, path: str):
        try:
            idx = self.selected_files.index(path)
            self.file_listbox.selection_clear(0, "end")
            self.file_listbox.selection_set(idx)
            self.file_listbox.see(idx)
        except ValueError:
            pass


# ---------------------------
# Entry Point
# ---------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = StepwisePipelineGUI(root)
    root.mainloop()