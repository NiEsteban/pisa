from abc import ABC, abstractmethod
import pandas as pd
from typing import Tuple, Optional

class IDataLoader(ABC):
    """
    Interface for data loaders in the infrastructure layer.
    """
    
    @abstractmethod
    def load(self, path: str, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Load data from the specified path.
        
        Args:
            path: Path to the file.
            **kwargs: Additional arguments specific to the loader (e.g., country_code).
            
        Returns:
            Tuple containing (labeled_dataframe, raw_dataframe).
            Returns (None, None) if loading fails.
        """
        pass
