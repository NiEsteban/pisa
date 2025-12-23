"""Tree-based file management logic for the GUI"""
import os
import tkinter as tk
from tkinter import ttk, filedialog
from typing import TYPE_CHECKING, List, Set

if TYPE_CHECKING:
    from pisa_pipeline.gui.main_window import StepwisePipelineGUI

class TreeFileManager:
    """Handles file and folder selection and management using a Treeview"""
    
    def __init__(self, gui: "StepwisePipelineGUI"):
        self.gui = gui
        
        # Connect button commands
        self.gui.btn_select_folder.config(command=self.browse_folder)
        # self.gui.btn_select_files.config(command=self.browse_files) # Deprecated or re-purposed?
        # Maybe hide "browse files" if we are folder-centric, or keep it to add single files? 
        # Plan says "Add Folder vs Add File". Let's keep both but map them to tree.
        self.gui.btn_select_files.config(command=self.browse_files)
        self.gui.btn_clear.config(command=self.clear_selection)

        # Mapping from tree item ID to file path
        self.id_to_path = {}
        # Mapping from file path to tree item ID
        self.path_to_id = {}

    def setup_tree(self, tree: ttk.Treeview):
        """Configure the tree widget"""
        self.tree = tree
        self.tree.heading("#0", text="File System", anchor="w")
        
        # Add scrollbar if not exists
        # Bind expansion event for lazy loading
        self.tree.bind("<<TreeviewOpen>>", self._on_tree_open)

    def browse_folder(self) -> None:
        """Open folder browser and populate tree"""
        folder = filedialog.askdirectory()
        if not folder:
            return
        
        # Clear existing
        self.clear_selection()
        
        self.gui.controller.context.selected_folder = folder
        self.gui.path_var.set(folder)
        
        # Populate root
        self._populate_node("", folder, is_root=True)

    def browse_files(self) -> None:
        """Add specific files to the tree"""
        files = filedialog.askopenfilenames(
            filetypes=[("Data files", "*.sav *.csv")]
        )
        if not files:
            return
            
        parent_dir = os.path.dirname(files[0])
        self.gui.controller.context.selected_folder = parent_dir
        self.gui.path_var.set(parent_dir)
        self.clear_selection()
        
        # Populate as if folder was selected, but maybe highlight files?
        # For simplicity, just load the folder.
        self._populate_node("", parent_dir, is_root=True)


    def clear_selection(self) -> None:
        """Clear tree and selection"""
        self.tree.delete(*self.tree.get_children())
        self.id_to_path.clear()
        self.path_to_id.clear()
        self.gui.column_display.display_columns_for_file(None)

    def refresh_folder(self, folder_path: str) -> None:
        """Refresh a specific folder node in the tree."""
        if not folder_path: return

        # 1. Find the node ID for this path
        node_id = self.path_to_id.get(folder_path)
        
        # If not found, maybe we are inside the root but it wasn't expanded yet?
        if not node_id:
            # If the path is the root itself or inside it
            if self.gui.controller.context.selected_folder and folder_path == self.gui.controller.context.selected_folder:
                 self.browse_folder_manual(folder_path)
            elif self.gui.controller.context.selected_folder and folder_path.startswith(self.gui.controller.context.selected_folder):
                 # Fallback: reload entire root if we are inside the selection but lost track
                 self.browse_folder_manual(self.gui.controller.context.selected_folder)
            return

        # 2. If node found, refresh its children
        if node_id:
            # Save whether it was expanded
            was_open = self.tree.item(node_id, "open")
            
            # Delete current children
            children = self.tree.get_children(node_id)
            for child in children:
                self._delete_node_mappings(child)
                self.tree.delete(child)
            
            # Re-populate children from disk
            self._populate_node_children(node_id, folder_path)
            
            # Restore expansion state or force open to show new files
            if was_open:
                self.tree.item(node_id, open=True)
    



    def select_file(self, file_path: str) -> None:
        """
        Programmatically selects a specific file in the tree.
        Ensures the parent folder is expanded and the file is visible/focused.
        """
        # Ensure parent folder is expanded/loaded
        parent_dir = os.path.dirname(file_path)
        
        # Calling refresh_folder ensures the parent is loaded and knows about this file
        # (Useful if the file was just created)
        # However, we only need to lookup ID if it exists.
        
        node_id = self.path_to_id.get(file_path)
        if not node_id:
            # Maybe it wasn't loaded yet?
            self.refresh_folder(parent_dir)
            node_id = self.path_to_id.get(file_path)

        if node_id:
            self.tree.selection_set(node_id)
            self.tree.focus(node_id)
            self.tree.see(node_id)


    def _delete_node_mappings(self, node_id):
        """Recursively remove mappings for node and children to prevent memory leaks."""
        path = self.id_to_path.get(node_id)
        if path and path in self.path_to_id:
            del self.path_to_id[path]
        if node_id in self.id_to_path:
            del self.id_to_path[node_id]
            
        for child in self.tree.get_children(node_id):
            self._delete_node_mappings(child)

    def browse_folder_manual(self, path):
        """Manually trigger a folder browse action for a given path."""
        self.clear_selection()
        self._populate_node("", path, is_root=True)

    def get_selected_files(self) -> List[str]:
        """
        Returns a list of all selected files.
        If a folder is selected, it recursively finds all supported files inside it.
        """
        selected_ids = self.tree.selection()
        files = []
        for selected_id in selected_ids:
            path = self.id_to_path.get(selected_id)
            if not path: continue
            
            # Delegate to Controller for proper layer separation
            found_files = self.gui.controller.get_files_for_path(path)
            files.extend(found_files)
        
        return sorted(list(set(files))) # Return unique, sorted files

    def _on_tree_open(self, event):
        """Handler for tree node expansion lazy loading"""
        item_id = self.tree.focus()
        if not item_id:
             # Sometimes focus isn't set on open via icon click?
             # Try selection
             sel = self.tree.selection()
             if sel: item_id = sel[0]
        
        path = self.id_to_path.get(item_id)
        if path and os.path.isdir(path):
            # If it has a dummy child, refresh it
            children = self.tree.get_children(item_id)
            if len(children) == 1 and self.tree.item(children[0], "text") == "dummy":
                self.tree.delete(children[0])
                self._populate_node_children(item_id, path)

    def _populate_node(self, parent_id, path, is_root=False):
        """Populate a single node item."""
        name = os.path.basename(path) if not is_root else path
        
        # Decide icon/style based on type
        # For now just text
        
        if is_root:
            node_id = self.tree.insert(parent_id, "end", text=name, open=True)
            self._populate_node_children(node_id, path)
        else:
            # If directory, add dummy child for lazy loading
            if os.path.isdir(path):
                node_id = self.tree.insert(parent_id, "end", text=name, open=False)
                # Add dummy
                self.tree.insert(node_id, "end", text="dummy")
            else:
                 node_id = self.tree.insert(parent_id, "end", text=name)
        
        self.id_to_path[node_id] = path
        self.path_to_id[path] = node_id
        return node_id

    def _populate_node_children(self, parent_id, parent_path):
        """Populate children of a directory node."""
        try:
            # List dirs and files
             items = os.listdir(parent_path)
             # Sort: folders first, then files
             items.sort(key=lambda x: (not os.path.isdir(os.path.join(parent_path, x)), x.lower()))
             
             for item in items:
                 item_path = os.path.join(parent_path, item)
                 
                 # Filtering hidden files or irrelevant extensions
                 if item.startswith('.'): continue
                 if os.path.isfile(item_path):
                     if not item.lower().endswith(('.csv', '.sav', '.sps', '.txt')):
                         continue
                 
                 self._populate_node(parent_id, item_path, is_root=False)
                 
        except PermissionError:
             pass 

