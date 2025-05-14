from abc import ABC, abstractmethod
import logging

logger = logging.getLogger("data-generator")

class BaseAdapter(ABC):
    """Base class for all data source adapters"""
    
    def __init__(self, config):
        """Initialize the adapter with configuration"""
        self.config = config
        logger.info(f"Initializing {self.__class__.__name__}")
    
    @abstractmethod
    def connect(self):
        """Connect to the data source"""
        pass
    
    @abstractmethod
    def read_data(self, batch_size=100):
        """Read data from the source"""
        pass
    
    @abstractmethod
    def close(self):
        """Close the connection to the data source"""
        pass
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
