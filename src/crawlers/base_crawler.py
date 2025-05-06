from abc import ABC, abstractmethod

class Crawler(ABC):
    """Abstract base class for county-specific crawlers."""
    
    @abstractmethod
    def crawl(self) -> str:
        """Crawl data and return the file path of the downloaded data."""
        pass