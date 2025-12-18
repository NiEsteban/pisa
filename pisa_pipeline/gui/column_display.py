"""
Column display module with index numbers and statistics visualization.
"""
import tkinter as tk
from tkinter import ttk, messagebox
from typing import TYPE_CHECKING, Dict, Set, Optional
import pandas as pd
import os
import threading

if TYPE_CHECKING:
    from pisa_pipeline.gui.main_window import StepwisePipelineGUI

# Import statistics modules
from pisa_pipeline.utils.column_stats import ColumnStatsFactory
from pisa_pipeline.gui.stats_visualizer import StatsVisualizerFactory


class ColumnDisplay:
    """Handles column display with checkboxes, indices, and click-to-view stats"""

    def __init__(self, gui: "StepwisePipelineGUI"):
        self.gui = gui
        self.column_vars: Dict[str, tk.BooleanVar] = {}
        self.column_labels: Dict[str, ttk.Label] = {}
        
        # Binding is now handled in main_window.py via Treeview events


    def display_columns_for_file(self, file_path: str) -> None:
        """
        Display columns for a specific file with index numbers.
        
        Args:
            file_path: path to the file
        """
        # Clear existing column display
        for widget in self.gui.col_inner_frame.winfo_children():
            widget.destroy()
        
        self.column_vars.clear()
        self.column_labels.clear()

        # Get DataFrame
        df = self._get_dataframe(file_path)
        if df is None:
            if file_path:
                ttk.Label(
                    self.gui.col_inner_frame,
                    text=f"Unable to load: {os.path.basename(file_path)}"
                ).pack(anchor="w")
            else:
                # No file selected - clear display (done above)
                pass
            return

        # Initialize columns_to_drop set if not exists
        if file_path not in self.gui.columns_to_drop_map:
            self.gui.columns_to_drop_map[file_path] = set()

        # Display columns with index numbers
        for idx, col in enumerate(df.columns):
            self._create_column_row(file_path, col, idx, df)

    def _create_column_row(self, file_path: str, column_name: str, index: int, df: pd.DataFrame):
        """
        Create a row for a column with index, checkbox, name, and click handler.
        
        Args:
            file_path: path to the file
            column_name: name of the column
            index: column index (0-based)
            df: DataFrame containing the column
        """
        row_frame = ttk.Frame(self.gui.col_inner_frame)
        row_frame.pack(fill="x", pady=1)

        # Index label (fixed width)
        index_label = ttk.Label(
            row_frame,
            text=f"[{index}]",
            width=6,
            foreground="gray40",
            font=("Courier", 9)
        )
        index_label.pack(side="left", padx=(5, 5))

        # Checkbox for dropping
        var = tk.BooleanVar(value=column_name in self.gui.columns_to_drop_map[file_path])
        self.column_vars[column_name] = var

        chk = ttk.Checkbutton(
            row_frame,
            variable=var,
            command=lambda: self._toggle_drop_column(file_path, column_name)
        )
        chk.pack(side="left")

        # Column name label (clickable)
        col_label = ttk.Label(
            row_frame,
            text=column_name,
            cursor="hand2",
            foreground="blue"
        )
        col_label.pack(side="left", padx=(5, 0), fill="x", expand=True)
        
        # Store label reference
        self.column_labels[column_name] = col_label

        # Bind click event to show statistics
        col_label.bind("<Button-1>", lambda e: self._show_column_stats(file_path, column_name, df))
        
        # Add tooltip on hover
        self._add_tooltip(col_label, f"Click to view statistics for '{column_name}'")
        
        # Bind mousewheel scrolling to all widgets in this row
        if hasattr(self.gui, 'bind_column_mousewheel'):
            self.gui.bind_column_mousewheel(row_frame)

    def _toggle_drop_column(self, file_path: str, column_name: str):
        """Toggle column in the drop list"""
        if self.column_vars[column_name].get():
            self.gui.columns_to_drop_map[file_path].add(column_name)
        else:
            self.gui.columns_to_drop_map[file_path].discard(column_name)

    def _show_column_stats(self, file_path: str, column_name: str, df: pd.DataFrame):
        """
        Show statistics for the clicked column in a new window.
        
        Args:
            file_path: path to the file
            column_name: name of the column
            df: DataFrame containing the column
        """
        # Show loading message
        print(f"[INFO] Loading statistics for column: {column_name}")
        
        def compute_and_show():
            try:
                # Compute statistics (runs in thread)
                stats = ColumnStatsFactory.from_dataframe(df, column_name)
                
                if stats is None:
                    self.gui.root.after(0, lambda: messagebox.showerror(
                        "Error",
                        f"Could not compute statistics for '{column_name}'"
                    ))
                    return
                
                # Show visualizer in main thread
                self.gui.root.after(0, lambda: StatsVisualizerFactory.show_stats(self.gui.root, stats))
                # print(f"[INFO] Statistics displayed for: {column_name}") # Reduced verbosity as requested
                
            except Exception as e:
                print(f"[ERROR] Failed to compute statistics: {e}")
                self.gui.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Failed to compute statistics for '{column_name}':\n{str(e)}"
                ))
        
        # Run in background thread to avoid GUI freezing
        threading.Thread(target=compute_and_show, daemon=True).start()

    def _get_dataframe(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Get DataFrame for the given file path.
        
        Args:
            file_path: path to the file
            
        Returns:
            DataFrame or None if not available
        """
        # Check if already loaded in memory
        if not file_path:
            return None
            
        file_results = self.gui.file_results.get(file_path, {})
        
        # Try to get from memory (prioritize transformed > cleaned > labeled)
        for key in ["transformed", "cleaned", "labeled"]:
            if key in file_results and file_results[key] is not None:
                return file_results[key]
        
        # Load from disk if not in memory
        if os.path.exists(file_path) and file_path.lower().endswith(".csv"):
            try:
                df = pd.read_csv(file_path, encoding="cp1252")
                print(f"[INFO] Loaded {os.path.basename(file_path)} from disk")
                return df
            except Exception as e:
                print(f"[ERROR] Failed to load {file_path}: {e}")
                return None
        
        return None

    def _add_tooltip(self, widget, text: str):
        """Add a simple tooltip to a widget"""
        def on_enter(event):
            tooltip = tk.Toplevel()
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
            label = ttk.Label(
                tooltip,
                text=text,
                background="lightyellow",
                relief="solid",
                borderwidth=1,
                padding=5
            )
            label.pack()
            widget._tooltip = tooltip

        def on_leave(event):
            if hasattr(widget, '_tooltip'):
                widget._tooltip.destroy()
                del widget._tooltip

        widget.bind("<Enter>", on_enter)
        widget.bind("<Leave>", on_leave)

    def refresh_display(self, file_path: str):
        """Refresh the column display for a file"""
        self.display_columns_for_file(file_path)

    def clear_display(self):
        """Clear all column displays"""
        for widget in self.gui.col_inner_frame.winfo_children():
            widget.destroy()
        self.column_vars.clear()
        self.column_labels.clear()