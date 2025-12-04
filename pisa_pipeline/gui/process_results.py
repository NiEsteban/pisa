import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from typing import List, Dict, Any
import pandas as pd
from pisa_pipeline.data_processing.process_results import ProcessResults
from pisa_pipeline.utils.io import save_top_x_to_excel
from pisa_pipeline.utils.algo_utils import detect_columns

class ProcessResultsGUI:
    """GUI for processing ranking and selection results"""

    def __init__(self, parent: ttk.Frame):
        self.parent = parent
        # State variables
        self.results_dir: str = ""
        self.csv_dir: str = ""
        self.results_name: List[str] = []
        self.essentials: Dict[str, List[str]] = {}
        self.ranking_filters: List[str] = ["CORR", "GAIN", "RELIEFF"]
        self.selection_filters: List[str] = ["subset", "wrapper"]
        self.scoring_weights: List[float] = [0.25, 0.30, 0.45]
        self.num_selected: int = 20
        self.results: Dict[str, Any] = {}
        # UI variables
        self.results_dir_var = tk.StringVar()
        self.csv_dir_var = tk.StringVar()
        self.results_name_var = tk.StringVar()
        self.essentials_var = tk.StringVar()
        self.ranking_filters_var = tk.StringVar(value=", ".join(self.ranking_filters))
        self.selection_filters_var = tk.StringVar(value=", ".join(self.selection_filters))
        self.scoring_weights_var = tk.StringVar(value=", ".join(map(str, self.scoring_weights)))
        self.num_selected_var = tk.IntVar(value=self.num_selected)
        # Layout
        self._setup_top_frame()
        self._setup_main_pane()
        self._setup_log_frame()

    def _setup_log_frame(self) -> None:
        """Create log panel"""
        log_frame = ttk.LabelFrame(self.parent, text="Log")
        log_frame.pack(side="bottom", fill="x", padx=10, pady=10)
        self.text_log = tk.Text(log_frame, height=10, state="disabled", wrap="none")
        self.text_log.tag_config("error", foreground="red")
        self.text_log.tag_config("pipeline", foreground="blue")
        self.text_log.tag_config("info", foreground="black")
        self.text_log.pack(fill="both", expand=True)

    def _setup_top_frame(self) -> None:
        """Create top toolbar with directory selection buttons"""
        top_frame = ttk.Frame(self.parent)
        top_frame.pack(fill="x", padx=10, pady=8)
        ttk.Button(
            top_frame,
            text="Select Results Directory",
            command=self._select_results_dir,
            style="Accent.TButton"
        ).pack(side="left", padx=6)
        ttk.Entry(
            top_frame,
            textvariable=self.results_dir_var,
            width=40
        ).pack(side="left", fill="x", expand=True, padx=5)
        ttk.Button(
            top_frame,
            text="Select CSV File",
            command=self._select_csv_file,
            style="Accent.TButton"
        ).pack(side="left", padx=6)
        ttk.Entry(
            top_frame,
            textvariable=self.csv_dir_var,
            width=40
        ).pack(side="left", fill="x", expand=True, padx=5)

    def _setup_main_pane(self) -> None:
        """Create main content area with parameters and actions"""
        main_pane = ttk.Frame(self.parent)
        main_pane.pack(fill="both", expand=True, padx=10, pady=6)
        # LEFT: Parameters
        self._setup_left_frame(main_pane)
        # RIGHT: Actions and Results
        self._setup_right_frame(main_pane)

    def _setup_left_frame(self, parent: ttk.Frame) -> None:
        """Create parameters panel"""
        left_frame = ttk.LabelFrame(parent, text="Parameters")
        left_frame.pack(side="left", fill="y", padx=(0, 12))
        ttk.Label(left_frame, text="Result Names (comma-separated):").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.results_name_var, width=40).pack(anchor="w", padx=4, pady=2)
        ttk.Label(left_frame, text="Essentials (comma-separated, per dataset):").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.essentials_var, width=40).pack(anchor="w", padx=4, pady=2)
        ttk.Label(left_frame, text="Ranking Filters (comma-separated):").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.ranking_filters_var, width=40).pack(anchor="w", padx=4, pady=2)
        ttk.Label(left_frame, text="Selection Filters (comma-separated):").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.selection_filters_var, width=40).pack(anchor="w", padx=4, pady=2)
        ttk.Label(left_frame, text="Scoring Weights (comma-separated):").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.scoring_weights_var, width=40).pack(anchor="w", padx=4, pady=2)
        ttk.Label(left_frame, text="Number of Selected Attributes:").pack(anchor="w", padx=4, pady=4)
        ttk.Entry(left_frame, textvariable=self.num_selected_var, width=10).pack(anchor="w", padx=4, pady=2)

    def _setup_right_frame(self, parent: ttk.Frame) -> None:
        """Create actions and results panel"""
        right_frame = ttk.Frame(parent)
        right_frame.pack(side="right", fill="both", expand=True)
        # Actions
        self._setup_actions_frame(right_frame)
        # Results
        self._setup_results_frame(right_frame)

    def _setup_actions_frame(self, parent: ttk.Frame) -> None:
        """Create actions panel"""
        actions_frame = ttk.LabelFrame(parent, text="Actions")
        actions_frame.pack(fill="x", pady=(0, 12), ipadx=4, ipady=4)
        ttk.Button(
            actions_frame,
            text="Run Process Results",
            command=self._run_process_results,
            style="Accent.TButton"
        ).pack(fill="x", padx=4, pady=4)
        # Add Save button
        ttk.Button(
            actions_frame,
            text="Save Top Selection Results",
            command=self._save_top_x_results,
            style="Accent.TButton"
        ).pack(fill="x", padx=4, pady=4)

    def _setup_results_frame(self, parent: ttk.Frame) -> None:
        """Create results panel"""
        results_frame = ttk.LabelFrame(parent, text="Results")
        results_frame.pack(fill="both", expand=True, ipadx=4, ipady=4)
        self.text_results = tk.Text(results_frame, height=20, state="disabled", wrap="none")
        self.text_results.pack(fill="both", expand=True)

    def _save_top_x_results(self) -> None:
        """Save top X ranking summary AND the dataset filtered to those top X variables.

        Matching logic is the same as in the old `save_selected_attributes`:
        - clean dataset column names
        - clean attribute names from the ranking
        - match only on cleaned names (exact, with optional very-close fuzzy fallback)
        """
        try:
            import os
            import re
            import pandas as pd
            from difflib import get_close_matches

            # -------------------------------
            # 0. Basic checks / UI settings
            # -------------------------------
            if not self.results:
                messagebox.showerror("Error", "No results to save. Run the process first.")
                return

            top_x = self.num_selected_var.get()
            output_dir = filedialog.askdirectory(title="Select Output Directory")
            if not output_dir:
                return

            dataset = self.results.get("dataset", None)
            if dataset is None:
                messagebox.showerror("Error", "No dataset found in results.")
                return

            all_results = self.results.get("all_results", {})
            if not all_results:
                messagebox.showerror("Error", "No analysis results found.")
                return

            output_file = os.path.join(output_dir, "top_selection_results.xlsx")

            # ---------------------------------
            # 1. Cleaning function (same as old)
            # ---------------------------------
            def clean_column_name(name: str) -> str:
                # Remove control chars (\x00–\x1F) and literal backslashes or \v
                name = re.sub(r"(\\v|[\x00-\x1F]|\\)+", "", str(name))
                return name.strip()

            # ---------------------------------
            # 2. Work on a copy with CLEANED headers
            #    (this mirrors your old script)
            # ---------------------------------
            dataset = dataset.copy()
            dataset.columns = [clean_column_name(c) for c in dataset.columns]

            # 2a. Detect ID and math columns on the CLEANED dataset
            score_col, school_col, student_col, leveled_score_col = detect_columns(
                dataset, detect_math_level=True
            )

            ids_col = [col for col in [school_col, student_col] if col and col in dataset.columns]
            if not ids_col:
                # Fallback: try to find columns with "id" in the name
                ids_col = [col for col in dataset.columns if "id" in col.lower()]

            print(f"Debug: (cleaned) ID Columns: {ids_col}, Math Column: {leveled_score_col}")

            # ---------------------------------
            # 3. Helper: map attribute name -> dataset column
            #    using CLEANED names only, like in save_selected_attributes
            # ---------------------------------
            dataset_clean_cols = list(dataset.columns)

            def map_attr_name_to_col(attr_name: str,
                                    use_fuzzy: bool = True,
                                    cutoff: float = 0.96):
                """
                Map an Attribute_Name coming from the ranking summary
                to a dataset column using cleaned names.

                1) exact match on cleaned name
                2) (optional) very strict fuzzy match on cleaned name
                NO index / Attribute_ID used.
                """
                if attr_name is None:
                    return None, "none"

                cleaned = clean_column_name(attr_name)
                if not cleaned:
                    return None, "none"

                # 1) exact cleaned match
                if cleaned in dataset_clean_cols:
                    return cleaned, "exact"

                # 2) optional fuzzy on cleaned names
                if use_fuzzy:
                    matches = get_close_matches(cleaned, dataset_clean_cols, n=1, cutoff=cutoff)
                    if matches:
                        return matches[0], "fuzzy"

                return None, "none"

            # ---------------------------------
            # 4. Write Excel with two sheets per dataset_key
            # ---------------------------------
            with pd.ExcelWriter(output_file, engine="openpyxl", mode="w") as writer:

                for dataset_key, result_data in all_results.items():
                    print(f"Processing results for: {dataset_key}")

                    full_summary = result_data.get("summary")
                    if full_summary is None or full_summary.empty:
                        continue

                    # ---- find the attribute name column (like before) ----
                    attr_name_col = None
                    for col in full_summary.columns:
                        low = col.lower()
                        if "attribute" in low and "name" in low:
                            attr_name_col = col
                            break

                    if attr_name_col is None:
                        for col in full_summary.columns:
                            low = col.lower()
                            if low in ("attribute", "variable", "feature"):
                                attr_name_col = col
                                break

                    if attr_name_col is None:
                        for col in full_summary.columns:
                            low = col.lower()
                            if "rank" not in low and "score" not in low:
                                attr_name_col = col
                                break

                    if attr_name_col is None:
                        attr_name_col = full_summary.columns[0]

                    print(f"Debug: Using attribute name column: {attr_name_col}")

                    # Get TOP X rows (assumed sorted)
                    top_summary = full_summary.head(top_x).copy()

                    # ------------------------------
                    # 4a. Map Attribute_Name -> dataset column (cleaned names)
                    # ------------------------------
                    mapped_dataset_columns = []
                    match_types = []

                    for _, row in top_summary.iterrows():
                        name_val = row[attr_name_col]
                        col_name, mtype = map_attr_name_to_col(
                            name_val,
                            use_fuzzy=True,   # set False if you want only exact
                            cutoff=0.96
                        )
                        mapped_dataset_columns.append(col_name)
                        match_types.append(mtype)

                    print(
                        "Debug: requested top attributes:",
                        list(top_summary[attr_name_col])
                    )
                    print(
                        "Debug: mapped dataset columns for", dataset_key, ":",
                        mapped_dataset_columns
                    )

                    # Add mapping info to summary sheet
                    top_summary["mapped_dataset_column"] = mapped_dataset_columns
                    top_summary["match_type"] = match_types

                    # unique, in order, non-empty
                    selected_cols = []
                    for c in mapped_dataset_columns:
                        if c and c not in selected_cols:
                            selected_cols.append(c)

                    if not selected_cols:
                        print("Warning: None of the top attributes could be matched to dataset columns.")
                        sheet_name_rank = f"Rank_{dataset_key}"[:31]
                        top_summary.to_excel(writer, sheet_name=sheet_name_rank, index=False)
                        continue

                    # ------------------------------
                    # 4b. Build the filtered dataset
                    #     (like save_selected_attributes: top-X + essentials)
                    # ------------------------------
                    # Essentials here are IDs + math level
                    essentials = ids_col.copy()
                    if leveled_score_col and leveled_score_col in dataset.columns:
                        essentials.append(leveled_score_col)

                    # All columns we want (top selected + essentials)
                    all_selected = selected_cols + essentials
                    all_selected_clean = [clean_column_name(a) for a in all_selected]

                    # Match against cleaned dataset columns (same as in old script)
                    cols_to_save = []
                    seen = set()
                    for c in all_selected_clean:
                        if c in dataset.columns and c not in seen:
                            seen.add(c)
                            cols_to_save.append(c)

                    if not cols_to_save:
                        print("Warning: No columns found in dataset after cleaning.")
                        sheet_name_rank = f"Rank_{dataset_key}"[:31]
                        top_summary.to_excel(writer, sheet_name=sheet_name_rank, index=False)
                        continue

                    subset_df = dataset[cols_to_save].copy()

                    print(f"Debug: final cols_to_save for {dataset_key}: {cols_to_save}")
                    print(f"Debug: subset_df shape for {dataset_key}: {subset_df.shape}")

                    # ------------------------------
                    # 4c. Save to Excel
                    # ------------------------------
                    sheet_name_rank = f"Rank_{dataset_key}"[:31]
                    sheet_name_data = f"Data_{dataset_key}"[:31]

                    top_summary.to_excel(writer, sheet_name=sheet_name_rank, index=False)
                    subset_df.to_excel(writer, sheet_name=sheet_name_data, index=False)

            messagebox.showinfo("Success", f"Saved Top {top_x} results to:\n{output_file}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            messagebox.showerror("Error", f"Failed to save results: {str(e)}")



    def _select_results_dir(self) -> None:
        """Select results directory"""
        dir_path = filedialog.askdirectory(
            title="Select Results Directory"
        )
        if dir_path:
            self.results_dir = dir_path
            self.results_dir_var.set(dir_path)
            # Debug: List files in the directory
            try:
                files = os.listdir(dir_path)
                print(f"Files in directory {dir_path}: {files}")
            except PermissionError:
                print(f"Permission denied for directory: {dir_path}")
            except Exception as e:
                print(f"Error listing files: {e}")

    def _select_csv_file(self) -> None:
        """Select a CSV file"""
        file_path = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file_path:
            self.csv_dir = file_path
            self.csv_dir_var.set(file_path)

    def _run_process_results(self) -> None:
        """Run the process results pipeline"""
        try:
            # Parse inputs
            self.results_name = [ds.strip() for ds in self.results_name_var.get().split(",") if ds.strip()]
            self.ranking_filters = [rf.strip() for rf in self.ranking_filters_var.get().split(",") if rf.strip()]
            self.selection_filters = [sf.strip() for sf in self.selection_filters_var.get().split(",") if sf.strip()]
            self.scoring_weights = [float(w.strip()) for w in self.scoring_weights_var.get().split(",") if w.strip()]
            self.num_selected = self.num_selected_var.get()
            #print(f"Debug: Number of selected attributes: {self.num_selected}")

            # Parse essentials
            self.essentials = {}
            for ess in self.essentials_var.get().split(";"):
                if ":" not in ess:
                    continue
                ds, attrs = ess.split(":", 1)
                ds = ds.strip()
                attrs = [a.strip() for a in attrs.split(",") if a.strip()]
                self.essentials[ds] = attrs
            #print(f"Debug: Essentials: {self.essentials}")

            # Run process
            processor = ProcessResults()
            #print(f"Debug: Results directory: {self.results_dir}")
            results = processor.run(
                results_dir=self.results_dir,
                dataset_path=self.csv_dir,
                essentials=self.essentials,
                ranking_filters=self.ranking_filters,
                selection_filters=self.selection_filters,
                scoring_weights=self.scoring_weights,
                num_selected=self.num_selected,
                results_name=self.results_name
            )

            # If "overall_summary" is missing, create it from "all_results" or "summaries"
            if "overall_summary" not in results or results["overall_summary"].empty:
                if "all_results" in results:
                    # Concatenate all dataset summaries into one DataFrame
                    all_summaries = []
                    for dataset_name, dataset_results in results["all_results"].items():
                        if "summary" in dataset_results:
                            all_summaries.append(dataset_results["summary"])
                    if all_summaries:
                        results["overall_summary"] = pd.concat(all_summaries, ignore_index=True)
                    else:
                        results["overall_summary"] = pd.DataFrame()  # Empty DataFrame
                elif "summaries" in results:
                    # Concatenate all summaries into one DataFrame
                    all_summaries = []
                    for dataset_name, summary_df in results["summaries"].items():
                        all_summaries.append(summary_df)
                    if all_summaries:
                        results["overall_summary"] = pd.concat(all_summaries, ignore_index=True)
                    else:
                        results["overall_summary"] = pd.DataFrame()  # Empty DataFrame

            print(f"Debug: Overall summary shape: {results['overall_summary'].shape if 'overall_summary' in results else 'No overall summary'}")

            # Store the results in the class attribute
            self.results = results

            # Add the dataset to the results dictionary
            self.results["dataset"] = pd.read_csv(self.csv_dir, encoding="cp1252")
            print(f"Debug: Dataset shape: {self.results['dataset'].shape}")

            # Display results
            self._display_results(results)
        except Exception as e:
            import traceback
            error_msg = f"Failed to run process: {e}\n{traceback.format_exc()}"
            print(error_msg)


    def _display_results(self, results: Dict[str, Any]) -> None:
        """Display results in the text widget"""
        self.text_results.config(state="normal")
        self.text_results.delete(1.0, tk.END)
        # Display overall summary if available
        if "overall_summary" in results and not results["overall_summary"].empty:
            self.text_results.insert(tk.END, "=== Overall Summary ===\n")
            self.text_results.insert(tk.END, results["overall_summary"].to_string(index=False) + "\n\n")
        # Display detailed results for each dataset
        if "all_results" in results:
            print(f'Debug: size of result -> {len(results["all_results"])}')
            for dataset_name, dataset_results in results["all_results"].items():
                self.text_results.insert(tk.END, f"\n{'='*60}\n")
                self.text_results.insert(tk.END, f"Dataset: {dataset_name}\n")
                self.text_results.insert(tk.END, f"{'='*60}\n\n")

                # Show top attributes summary
                if "summary" in dataset_results:
                    self.text_results.insert(tk.END, "Top Ranked Attributes:\n")
                    self.text_results.insert(tk.END, "-" * 60 + "\n")
                    self.text_results.insert(tk.END, dataset_results["summary"].to_string(index=False) + "\n\n")

        # Alternative: Display from summaries dict if all_results not present
        elif "summaries" in results:
            for dataset_name, summary_df in results["summaries"].items():
                self.text_results.insert(tk.END, f"\n{'='*60}\n")
                self.text_results.insert(tk.END, f"Dataset: {dataset_name}\n")
                self.text_results.insert(tk.END, f"{'='*60}\n\n")
                self.text_results.insert(tk.END, summary_df.to_string(index=False) + "\n\n")
        self.text_results.config(state="disabled")
        print("\n✅ Results displayed successfully in the Results panel")
