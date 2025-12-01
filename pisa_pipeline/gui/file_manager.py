"""File management logic for the GUI"""
import os
from tkinter import filedialog
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pisa_pipeline.gui.main_window import StepwisePipelineGUI


class FileManager:
    """Handles file and folder selection and management"""
    
    def __init__(self, gui: "StepwisePipelineGUI"):
        self.gui = gui
        
        # Connect button commands
        self.gui.btn_select_folder.config(command=self.browse_folder)
        self.gui.btn_select_files.config(command=self.browse_files)
        self.gui.btn_clear.config(command=self.clear_selection)

    def browse_folder(self) -> None:
        """Open folder browser and populate file list"""
        folder = filedialog.askdirectory()
        if not folder:
            return
        self.gui.selected_folder = folder
        self.gui.path_var.set(folder)
        self.populate_file_list(folder)

    def browse_files(self) -> None:
        """Open file browser for selecting multiple files"""
        files = filedialog.askopenfilenames(
            filetypes=[("Data files", "*.sav *.csv")]
        )
        if files:
            self.gui.selected_files = list(files)
            self.gui.path_var.set("; ".join(self.gui.selected_files))
            self.gui.file_listbox.delete(0, "end")
            for f in self.gui.selected_files:
                self.gui.file_listbox.insert("end", os.path.basename(f))

    def clear_selection(self) -> None:
        """Clear file selection and column display"""
        self.gui.file_listbox.selection_clear(0, "end")
        self.gui.column_display.display_columns_for_file(None)

    def populate_file_list(self, folder: str) -> None:
        """Populate listbox with files from folder"""
        self.gui.file_listbox.delete(0, "end")
        self.gui.selected_files = []
        try:
            for fname in sorted(os.listdir(folder)):
                if fname.lower().endswith((".sav", ".csv")):
                    path = os.path.join(folder, fname)
                    self.gui.selected_files.append(path)
                    self.gui.file_listbox.insert("end", os.path.basename(path))
        except Exception as e:
            print(f"[ERROR] Could not list folder: {e}")

    def _replace_file_in_list(self, old_path: str, new_path: str) -> None:
        """Replace a file in the list with a new one"""
        try:
            idx = self.gui.selected_files.index(old_path)
            self.gui.selected_files[idx] = new_path
            self.gui.file_listbox.delete(idx)
            self.gui.file_listbox.insert(idx, os.path.basename(new_path))
            self.gui.file_listbox.selection_clear(0, "end")
            self.gui.file_listbox.selection_set(idx)
            
            # Transfer column drop settings
            if old_path in self.gui.columns_to_drop_map:
                self.gui.columns_to_drop_map[new_path] = \
                    self.gui.columns_to_drop_map.pop(old_path)
            if old_path in self.gui.file_results:
                self.gui.file_results[new_path] = \
                    self.gui.file_results.pop(old_path)
        except ValueError:
            self._add_file_to_list_if_missing(new_path)

    def _add_file_to_list_if_missing(self, path: str) -> None:
        """Add file to list if not already present"""
        if path not in self.gui.selected_files:
            self.gui.selected_files.append(path)
            self.gui.file_listbox.insert("end", os.path.basename(path))

    def _select_single_file_in_list(self, path: str) -> None:
        """Select a single file in the listbox"""
        try:
            idx = self.gui.selected_files.index(path)
            self.gui.file_listbox.selection_clear(0, "end")
            self.gui.file_listbox.selection_set(idx)
            self.gui.file_listbox.see(idx)
        except ValueError:
            pass