import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from typing import List, Dict, Any
import pandas as pd
from pisa_pipeline.data_processing.process_results import ProcessResults

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

        # NOTE: DO NOT redirect stdout here - it's handled by the main window

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

    def _setup_results_frame(self, parent: ttk.Frame) -> None:
        """Create results panel"""
        results_frame = ttk.LabelFrame(parent, text="Results")
        results_frame.pack(fill="both", expand=True, ipadx=4, ipady=4)

        self.text_results = tk.Text(results_frame, height=20, state="disabled", wrap="none")
        self.text_results.pack(fill="both", expand=True)

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

            # Parse essentials
            self.essentials = {}
            for ess in self.essentials_var.get().split(";"):
                if ":" not in ess:
                    continue
                ds, attrs = ess.split(":", 1)
                ds = ds.strip()
                attrs = [a.strip() for a in attrs.split(",") if a.strip()]
                self.essentials[ds] = attrs

            # Run process
            processor = ProcessResults()
            print(f"Results directory: {self.results_dir}")
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