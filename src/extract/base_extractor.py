from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Optional

class Extractor(ABC):
    """Abstract base class for data extractors."""
    
    @abstractmethod
    def extract(self, file_path: str, config: Dict) -> Optional[DataFrame]:
        """Extract data into a Spark DataFrame."""
        pass