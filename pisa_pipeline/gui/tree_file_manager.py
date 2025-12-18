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
        
        self.gui.selected_folder = folder
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
        self.gui.selected_folder = parent_dir
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

    def _populate_node(self, parent_id, path, is_root=False):
        """Populate tree node. If key 'lazy' is used, purely add dummy."""
        try:
            # Display name
            display_name = os.path.basename(path)
            if not display_name: display_name = path 
            
            # Skip unwanted folders
            if display_name.lower() in ["results", "resultados", "__pycache__", ".git"]:
                # Unless it's the root we explicitly asked for
                if not is_root:
                    return

            # Create node
            # Use path as ID to simplify lookups
            if self.tree.exists(path):
                # If it exists, maybe we are refreshing? 
                # Just ensure it's visible?
                node_id = path
            else:
                node_id = self.tree.insert(parent_id, "end", iid=path, text=display_name, values=["folder"])
            
            self.id_to_path[node_id] = path
            self.path_to_id[path] = node_id

            # Check if it has relevant content to decide on dummy
            if os.path.isdir(path):
                # Add dummy child ONLY if it has relevant content (peek)
                if not self.tree.get_children(node_id):
                    if self._has_relevant_content(path):
                        self.tree.insert(node_id, "end", text="dummy")
            
            # If root, we must expand it immediately
            if is_root:
                self.tree.item(node_id, open=True)
                self._populate_node_children(node_id, path)

        except PermissionError:
            pass
        except Exception as e:
            print(f"Error listing {path}: {e}")

    def _has_relevant_content(self, path: str) -> bool:
        """Check if folder contains relevant subfolders or files (shallow peek)"""
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.name.lower() in ["results", "resultados", "__pycache__", ".git", "$recycle.bin", "system volume information"]:
                        continue
                    if entry.is_dir():
                        return True
                    if entry.is_file() and entry.name.lower().endswith((".csv", ".sav", ".txt", ".sps", ".spss")):
                        return True
        except (PermissionError, OSError):
            return False
        return False

    def _on_tree_open(self, event):
        """Handle node expansion"""
        # robustly find the item
        try:
            item_id = self.tree.focus()
            if not item_id:
                # Try selection
                sel = self.tree.selection()
                if sel: item_id = sel[0]
            
            if not item_id: return

            # Verify it's the one expanding? 
            # Tkinter doesn't tell us easily. But typically user clicks the one gaining focus.
            
            # Check for dummy
            children = self.tree.get_children(item_id)
            if children and self.tree.item(children[0], "text") == "dummy":
                # Remove dummy
                self.tree.delete(children[0])
                
                # Populate real contents
                path = self.id_to_path.get(item_id)
                if path:
                    self._populate_node_children(item_id, path)
        except Exception as e:
            print(f"Error on tree open: {e}")

    def _populate_node_children(self, node_id, parent_path):
        """
        Populates the children of a specific tree node by listing the file system.
        Filters out system folders and unwanted file types.
        
        Args:
            node_id: The ID of the tree node to populate.
            parent_path: The file system path corresponding to this node.
        """
        try:
             # List contents
            try:
                entries = sorted(os.listdir(parent_path))
            except OSError:
                return

            directories = []
            files_list = []
            
            for entry_name in entries:
                # Filter out system/output folders and hidden files
                if entry_name.lower() in ["results", "resultados", "__pycache__", ".git", ".backups", "$recycle.bin", "system volume information"]:
                    continue
                    
                full_path = os.path.join(parent_path, entry_name)
                
                if os.path.isdir(full_path):
                    directories.append(full_path)
                elif entry_name.lower().endswith((".csv", ".sav", ".txt", ".sps", ".spss")):
                    files_list.append(full_path)
            
            # Add subdirectories to the tree
            for dir_path in directories:
                dir_name = os.path.basename(dir_path)
                if not self.tree.exists(dir_path):
                    dir_node = self.tree.insert(node_id, "end", iid=dir_path, text=dir_name, values=["folder"])
                    
                    # Add dummy node for lazy loading ONLY if relevant content exists inside
                    if self._has_relevant_content(dir_path):
                        self.tree.insert(dir_node, "end", text="dummy")
                        
                    self.id_to_path[dir_node] = dir_path
                    self.path_to_id[dir_path] = dir_node

            # Add files to the tree
            for file_path in files_list:
                file_name = os.path.basename(file_path)
                if not self.tree.exists(file_path):
                    self.tree.insert(node_id, "end", iid=file_path, text=file_name, values=["file"])
                    self.id_to_path[file_path] = file_path
                    self.path_to_id[file_path] = file_path
                    
        except Exception as error:
            print(f"Error expanding {parent_path}: {error}")

    def refresh_folder(self, folder_path: str) -> None:
        """
        Refreshes a specific folder node in the tree to show new files.
        Useful when a pipeline step creates new output files.
        """
        # 1. Find existing node for this folder
        if not os.path.exists(folder_path): return
        
        node_id = self.path_to_id.get(folder_path)
        
        # If not found, recursively check parent (maybe parent needs refresh first)
        if not node_id:
            parent_dir = os.path.dirname(folder_path)
            if self.path_to_id.get(parent_dir):
                return self.refresh_folder(parent_dir)
            elif self.gui.selected_folder and folder_path.startswith(self.gui.selected_folder):
                 # Fallback: reload entire root if we are inside the selection but lost track
                 self.browse_folder_manual(self.gui.selected_folder)
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
            
            if os.path.isfile(path):
                files.append(path)
            elif os.path.isdir(path):
                # Recursively get all files in the selected folder
                for root, _, filenames in os.walk(path):
                    for filename in filenames:
                        if filename.lower().endswith((".csv", ".sav", ".txt", ".sps", ".spss")):
                            files.append(os.path.join(root, filename))
        
        return sorted(list(set(files))) # Return unique, sorted files
