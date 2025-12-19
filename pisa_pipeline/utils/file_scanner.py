import os
from typing import List, Tuple, Set

class FileSystemScanner:
    """
    Utility class to handle file system scanning and filtering for the PISA pipeline.
    Decouples file system access from the GUI.
    """
    
    # Folders to ignore during scanning
    IGNORED_FOLDERS = {
        "results", "resultados", "__pycache__", ".git", 
        ".backups", "$recycle.bin", "system volume information"
    }
    
    # Supported file extensions
    SUPPORTED_EXTENSIONS = (".csv", ".sav", ".txt", ".sps", ".spss")

    @classmethod
    def scan_directory(cls, path: str) -> Tuple[List[str], List[str]]:
        """
        Scans a directory and returns sorted lists of relevant subdirectories and files.
        
        Args:
            path (str): The directory path to scan.
            
        Returns:
            Tuple[List[str], List[str]]: (list of directory paths, list of file paths)
        """
        try:
            entries = sorted(os.listdir(path))
        except OSError:
            return [], []

        directories = []
        files = []

        for entry_name in entries:
            # Filter ignored folders
            if entry_name.lower() in cls.IGNORED_FOLDERS:
                continue

            full_path = os.path.join(path, entry_name)

            if os.path.isdir(full_path):
                directories.append(full_path)
            elif entry_name.lower().endswith(cls.SUPPORTED_EXTENSIONS):
                files.append(full_path)

        return directories, files

    @classmethod
    def has_relevant_content(cls, path: str) -> bool:
        """
        Checks if a folder contains any relevant PISA files or subdirectories
        that are not ignored. Useful for lazy loading "dummy" nodes.
        
        Args:
            path (str): The directory path to check.
            
        Returns:
            bool: True if relevant content exists, False otherwise.
        """
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.name.lower() in cls.IGNORED_FOLDERS:
                        continue
                    if entry.is_dir():
                        return True
                    if entry.is_file() and entry.name.lower().endswith(cls.SUPPORTED_EXTENSIONS):
                        return True
        except (PermissionError, OSError):
            return False
        return False

    @classmethod
    def get_recursive_files(cls, path: str) -> List[str]:
        """
        Recursively finds all supported files in a directory.
        
        Args:
            path (str): The root directory or file path.
            
        Returns:
            List[str]: A sorted list of unique absolute file paths.
        """
        if os.path.isfile(path):
            return [path]
        
        files = []
        if os.path.isdir(path):
            for root, _, filenames in os.walk(path):
                # We should probably filter ignored folders during walk too, 
                # but os.walk is simpler. Let's filter matches.
                # Optimization: Modify dirs in-place to skip ignored
                # dirs[:] = [d for d in dirs if d not in cls.IGNORED_FOLDERS] 
                # (Can't do easily without custom walker, relying on post-filter for now or simple walk)
                
                parts = os.path.normpath(root).split(os.sep)
                if any(p.lower() in cls.IGNORED_FOLDERS for p in parts):
                    continue

                for filename in filenames:
                    if filename.lower().endswith(cls.SUPPORTED_EXTENSIONS):
                        files.append(os.path.join(root, filename))
                        
        return sorted(list(set(files)))
