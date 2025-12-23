from typing import List, Dict, Set, Optional
import pandas as pd

class PipelineContext:
    """
    Holds the application state/data model.
    Decoupled from Controller logic and View.
    """
    def __init__(self):
        self.selected_folder: Optional[str] = None
        self.selected_files: List[str] = []
        self.file_results: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.columns_to_drop_map: Dict[str, Set[str]] = {}
