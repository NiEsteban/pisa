#!/usr/bin/env python3
"""Main GUI window for PISA Stepwise Pipeline"""
import sys
import tkinter as tk
from tkinter import ttk
from typing import Optional, List, Dict, Set
import pandas as pd

# Global UI constants
MAIN_WINDOW_TITLE = "PISA Stepwise Pipeline"
MAIN_WINDOW_GEOMETRY = "1920x1080"
TAB_PROCESS_DATA_TITLE = "Process Data"
TAB_PROCESS_RESULTS_TITLE = "Process Results"
LOG_FRAME_HEIGHT = 10

# Import from package
from pisa_pipeline.utils.gui_utils import TextRedirector
from pisa_pipeline.gui.process_results import ProcessResultsGUI


class TabAwareRedirector:
    """Redirects output to the appropriate log based on active tab"""
    def __init__(self, notebook, text_log_data, text_log_results):
        self.notebook = notebook
        self.text_log_data = text_log_data
        self.text_log_results = text_log_results

    def write(self, message: str) -> None:
        if not message:
            return
        
        # Determine which tab is active
        try:
            current_tab = self.notebook.select()
            tab_index = self.notebook.index(current_tab)
        except:
            tab_index = 0  # Default to first tab if error
        
        # Select the appropriate text widget based on active tab
        if tab_index == 0:  # Process Data tab
            target_widget = self.text_log_data
        else:  # Process Results tab or any other
            target_widget = self.text_log_results

        # Write to the selected widget
        target_widget.config(state="normal")
        tag = "info"
        if "[ERROR]" in message or message.lower().startswith("error"):
            tag = "error"
        elif "[PIPELINE]" in message or message.lower().startswith("pipeline"):
            tag = "pipeline"
        
        if not message.endswith("\n"):
            message = message + "\n"
        
        try:
            target_widget.insert("end", message, tag)
        finally:
            target_widget.config(state="disabled")
            target_widget.see("end")

    def flush(self) -> None:
        pass


class StepwisePipelineGUI:
    """Main GUI application for PISA Stepwise Pipeline"""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(MAIN_WINDOW_TITLE)
        self.root.geometry(MAIN_WINDOW_GEOMETRY)

        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True)

        # Tab 1: Process Data
        self.tab_process_data = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_process_data, text=TAB_PROCESS_DATA_TITLE)

        # Tab 2: Process Results
        self.tab_process_results = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_process_results, text=TAB_PROCESS_RESULTS_TITLE)

        # Initialize Process Data tab
        self._init_process_data_tab()

        # Initialize Process Results tab
        self._init_process_results_tab()

        # Setup intelligent stdout/stderr redirection AFTER both tabs are created
        self._setup_output_redirection()

    def _init_process_data_tab(self) -> None:
        """Initialize the Process Data tab"""
        # State variables - initialize BEFORE creating sub-components
        self.selected_folder: Optional[str] = None
        self.selected_files: List[str] = []
        self.file_results: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.columns_to_drop_map: Dict[str, Set[str]] = {}

        # UI variables
        self.path_var = tk.StringVar()
        self.save_unlabel_var = tk.BooleanVar(value=False)
        self.country_code = tk.StringVar(value="MEX")
        self.missing_thr = tk.DoubleVar(value=1.0)
        self.uniform_thr = tk.DoubleVar(value=1.0)
        self.split_dataset_var = tk.BooleanVar(value=False)
        self.split_ranges_var = tk.StringVar(value="0:10, 20:30")

        # Widgets that will be created
        self.file_listbox = None
        self.col_canvas = None
        self.col_scroll = None
        self.col_inner_frame = None
        self.actions_container = None
        self.entry_score = None
        self.entry_school = None
        self.entry_student = None
        self.text_log = None

        # UI style
        self._setup_styles()

        # Layout for Process Data tab
        self._setup_top_frame(self.tab_process_data)
        self._setup_main_pane(self.tab_process_data)
        self._setup_bottom_frame(self.tab_process_data)

        # Import sub-components AFTER all widgets are created
        from pisa_pipeline.gui.file_manager import FileManager
        from pisa_pipeline.gui.column_display import ColumnDisplay
        from pisa_pipeline.gui.pipeline_actions import PipelineActions

        # Initialize sub-components
        self.file_manager = FileManager(self)
        self.column_display = ColumnDisplay(self)
        self.pipeline_actions = PipelineActions(self)

    def _init_process_results_tab(self) -> None:
        """Initialize the Process Results tab"""
        self.process_results_gui = ProcessResultsGUI(self.tab_process_results)

    def _setup_styles(self) -> None:
        """Configure UI styles"""
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Accent.TButton", foreground="white", background="#007acc")
        style.map("Accent.TButton", background=[("active", "#2b88d8")])

    def _setup_top_frame(self, parent: ttk.Frame) -> None:
        """Create top toolbar with file selection buttons"""
        top_frame = ttk.Frame(parent)
        top_frame.pack(fill="x", padx=10, pady=8)

        # Buttons - will be connected to methods later
        self.btn_select_folder = ttk.Button(
            top_frame,
            text="Select Folder",
            style="Accent.TButton"
        )
        self.btn_select_folder.pack(side="left", padx=6)

        self.btn_select_files = ttk.Button(
            top_frame,
            text="Select Files",
            style="Accent.TButton"
        )
        self.btn_select_files.pack(side="left", padx=6)

        self.btn_clear = ttk.Button(
            top_frame,
            text="Clear Selection"
        )
        self.btn_clear.pack(side="left", padx=6)

        ttk.Entry(top_frame, textvariable=self.path_var).pack(
            side="left", fill="x", expand=True, padx=10
        )

    def _setup_main_pane(self, parent: ttk.Frame) -> None:
        """Create main content area with file list, columns, actions, and log"""
        main_pane = ttk.Frame(parent)
        main_pane.pack(fill="both", expand=True, padx=10, pady=6)

        # LEFT SIDE: Container for Files, Columns, and Log
        left_container = ttk.Frame(main_pane)
        left_container.pack(side="left", fill="both", expand=True)

        # Create horizontal paned window for Files and Columns
        content_frame = ttk.Frame(left_container)
        content_frame.pack(fill="both", expand=True, pady=(0, 6))

        # LEFT: Files List
        self._setup_left_frame(content_frame)

        # CENTER: Columns Display
        self._setup_center_frame(content_frame)

        # Log at bottom of left side
        self._setup_left_bottom_log(left_container)

        # RIGHT: Actions Panel
        self._setup_right_frame(main_pane)

    def _setup_left_frame(self, parent: ttk.Frame) -> None:
        """Create file list panel"""
        left_frame = ttk.Frame(parent)
        left_frame.pack(side="left", fill="y")

        ttk.Label(left_frame, text="Files").pack(anchor="w")

        self.file_listbox = tk.Listbox(
            left_frame,
            height=28,
            selectmode="extended",
            exportselection=False,
            width=36
        )
        self.file_listbox.pack(side="left", fill="y", padx=(0, 4))

        file_scroll = ttk.Scrollbar(
            left_frame,
            orient="vertical",
            command=self.file_listbox.yview
        )
        file_scroll.pack(side="left", fill="y")
        self.file_listbox.config(yscrollcommand=file_scroll.set)

    def _setup_center_frame(self, parent: ttk.Frame) -> None:
        """Create columns display panel"""
        center_frame = ttk.Frame(parent)
        center_frame.pack(side="left", expand=True, fill="both", padx=12)

        # Header with label and drop button
        header_frame = ttk.Frame(center_frame)
        header_frame.pack(fill="x", anchor="w")

        ttk.Label(
            header_frame,
            text="Columns (check to mark for dropping)"
        ).pack(side="left")

        self.btn_drop_columns = ttk.Button(
            header_frame,
            text="Drop Checked Columns",
            style="Accent.TButton"
        )
        self.btn_drop_columns.pack(side="left", padx=10)

        # Scrollable canvas for columns
        canvas_frame = ttk.Frame(center_frame)
        canvas_frame.pack(fill="both", expand=True)

        self.col_canvas = tk.Canvas(canvas_frame, highlightthickness=0)
        self.col_scroll = ttk.Scrollbar(
            canvas_frame,
            orient="vertical",
            command=self.col_canvas.yview
        )
        
        self.col_canvas.pack(side="left", fill="both", expand=True)
        self.col_scroll.pack(side="right", fill="y")
        self.col_canvas.configure(yscrollcommand=self.col_scroll.set)

        self.col_inner_frame = ttk.Frame(self.col_canvas)
        canvas_window = self.col_canvas.create_window(
            (0, 0),
            window=self.col_inner_frame,
            anchor="nw"
        )
        
        # Update scroll region when content changes
        def on_frame_configure(event):
            self.col_canvas.configure(scrollregion=self.col_canvas.bbox("all"))
        
        self.col_inner_frame.bind("<Configure>", on_frame_configure)
        
        # Enable mousewheel scrolling
        def on_mousewheel(event):
            self.col_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        # Bind mousewheel to canvas and its children
        self.col_canvas.bind("<MouseWheel>", on_mousewheel)
        self.col_inner_frame.bind("<MouseWheel>", on_mousewheel)
        
        # Also bind to all child widgets (will be handled in column_display)
        def bind_to_mousewheel(widget):
            widget.bind("<MouseWheel>", on_mousewheel)
            for child in widget.winfo_children():
                bind_to_mousewheel(child)
        
        # Store binding function for use in column_display
        self.bind_column_mousewheel = bind_to_mousewheel

    def _setup_right_frame(self, parent: ttk.Frame) -> None:
        """Create actions panel on the right side"""
        right_frame = ttk.Frame(parent, width=280)
        right_frame.pack(side="right", fill="y", padx=(8, 0))

        self.actions_container = ttk.Frame(right_frame)
        self.actions_container.pack(fill="both", expand=True)

        # ID Detection
        self._setup_id_frame()

        # Load & Label Section
        self._setup_load_label_frame()

        # Clean Section
        self._setup_clean_frame()

        # Transform Section
        self._setup_transform_frame()

        # Full Pipeline Button
        self.btn_full_pipeline = ttk.Button(
            self.actions_container,
            text="Run Full Pipeline",
            style="Accent.TButton"
        )
        self.btn_full_pipeline.pack(fill="x", pady=8, padx=4)

    def _setup_id_frame(self) -> None:
        """Create ID detection frame"""
        id_frame = ttk.LabelFrame(
            self.actions_container,
            text="Detected / Edit IDs"
        )
        id_frame.pack(fill="x", pady=(0, 12), ipadx=4, ipady=4)

        ttk.Label(id_frame, text="Score column:").grid(
            row=0, column=0, sticky="e", padx=4, pady=4
        )
        ttk.Label(id_frame, text="School ID:").grid(
            row=1, column=0, sticky="e", padx=4, pady=4
        )
        ttk.Label(id_frame, text="Student ID:").grid(
            row=2, column=0, sticky="e", padx=4, pady=4
        )

        self.entry_score = ttk.Entry(id_frame, width=24)
        self.entry_school = ttk.Entry(id_frame, width=24)
        self.entry_student = ttk.Entry(id_frame, width=24)

        self.entry_score.grid(row=0, column=1, padx=4, pady=2)
        self.entry_school.grid(row=1, column=1, padx=4, pady=2)
        self.entry_student.grid(row=2, column=1, padx=4, pady=2)

    def _setup_load_label_frame(self) -> None:
        """Create Load & Label frame"""
        label_frame = ttk.LabelFrame(
            self.actions_container,
            text="1. Load & Label"
        )
        label_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)

        ttk.Checkbutton(
            label_frame,
            text="Save unlabeled",
            variable=self.save_unlabel_var
        ).pack(anchor="w", padx=4, pady=2)

        ttk.Label(label_frame, text="Country code:").pack(anchor="w", padx=4)
        ttk.Entry(
            label_frame,
            textvariable=self.country_code,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        self.btn_load_label = ttk.Button(
            label_frame,
            text="Load & Label",
            style="Accent.TButton"
        )
        self.btn_load_label.pack(fill="x", padx=4, pady=4)

    def _setup_clean_frame(self) -> None:
        """Create Clean frame"""
        clean_frame = ttk.LabelFrame(
            self.actions_container,
            text="2. Clean"
        )
        clean_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)

        ttk.Label(clean_frame, text="Missing threshold:").pack(
            anchor="w", padx=4
        )
        ttk.Entry(
            clean_frame,
            textvariable=self.missing_thr,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        ttk.Label(clean_frame, text="Uniform threshold:").pack(
            anchor="w", padx=4
        )
        ttk.Entry(
            clean_frame,
            textvariable=self.uniform_thr,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        self.btn_clean = ttk.Button(
            clean_frame,
            text="Clean",
            style="Accent.TButton"
        )
        self.btn_clean.pack(fill="x", padx=4, pady=4)

    def _setup_clean_frame(self) -> None:
        """Create Clean frame"""
        clean_frame = ttk.LabelFrame(
            self.actions_container,
            text="2. Clean"
        )
        clean_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)

        # Missing threshold
        ttk.Label(clean_frame, text="Missing threshold:").pack(anchor="w", padx=4)
        ttk.Entry(
            clean_frame,
            textvariable=self.missing_thr,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        # Uniform threshold
        ttk.Label(clean_frame, text="Uniform threshold:").pack(anchor="w", padx=4)
        ttk.Entry(
            clean_frame,
            textvariable=self.uniform_thr,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        # Correlation threshold (optional, if you want to allow users to change it)
        self.correlation_thr = tk.DoubleVar(value=1.0)
        ttk.Label(clean_frame, text="Correlation threshold:").pack(anchor="w", padx=4)
        ttk.Entry(
            clean_frame,
            textvariable=self.correlation_thr,
            width=10
        ).pack(anchor="w", padx=4, pady=(0, 4))

        # Clean button
        self.btn_clean = ttk.Button(
            clean_frame,
            text="Clean",
            style="Accent.TButton"
        )
        self.btn_clean.pack(fill="x", padx=4, pady=4)


    def _setup_transform_frame(self) -> None:
        """Create Transform frame"""
        transform_frame = ttk.LabelFrame(
            self.actions_container,
            text="3. Transform"
        )
        transform_frame.pack(fill="x", pady=(0, 8), ipadx=4, ipady=4)

        # Split dataset checkbox
        ttk.Checkbutton(
            transform_frame,
            text="Split dataset into 2 parts",
            variable=self.split_dataset_var
        ).pack(anchor="w", padx=4, pady=2)

        # Split ranges input
        ttk.Label(
            transform_frame, 
            text="Column ranges (e.g., 0:10, 20:30):"
        ).pack(anchor="w", padx=4)
        
        ttk.Entry(
            transform_frame,
            textvariable=self.split_ranges_var,
            width=24
        ).pack(anchor="w", padx=4, pady=(0, 4))

        self.btn_transform = ttk.Button(
            transform_frame,
            text="Transform",
            style="Accent.TButton"
        )
        self.btn_transform.pack(fill="x", padx=4, pady=4)

    def _setup_bottom_frame(self, parent: ttk.Frame) -> None:
        """Create bottom frame - now empty since log moved to left side"""
        # This method is kept for compatibility but does nothing
        # Log is now in _setup_left_bottom_log()
        pass

    def _setup_left_bottom_log(self, parent: ttk.Frame) -> None:
        """Create compact log panel on the left bottom (under Files and Columns)"""
        log_frame = ttk.LabelFrame(parent, text="Log")
        log_frame.pack(fill="both", expand=False)

        self.text_log = tk.Text(log_frame, height=8, state="disabled", wrap="word")
        self.text_log.tag_config("error", foreground="red")
        self.text_log.tag_config("pipeline", foreground="#007acc")
        self.text_log.tag_config("info", foreground="gray20")
        
        # Add scrollbar to log
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.text_log.yview)
        self.text_log.config(yscrollcommand=log_scroll.set)
        
        self.text_log.pack(side="left", fill="both", expand=True, padx=2, pady=2)
        log_scroll.pack(side="right", fill="y")

    def _setup_output_redirection(self) -> None:
        """Setup intelligent output redirection based on active tab"""
        # Create the tab-aware redirector
        redirector = TabAwareRedirector(
            self.notebook,
            self.text_log,
            self.process_results_gui.text_log
        )
        
        sys.stdout = redirector
        sys.stderr = redirector