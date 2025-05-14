from abc import ABC, abstractmethod
import logging

logger = logging.getLogger("data-generator")

class BaseSink(ABC):
    """Base class for all data sinks"""
    
    def __init__(self, config):
        """Initialize the sink with configuration"""
        self.config = config
        logger.info(f"Initializing {self.__class__.__name__}")
    
    @abstractmethod
    def connect(self):
        """Connect to the sink"""
        pass
    
    @abstractmethod
    def send_data(self, data):
        """Send data to the sink"""
        pass
    
    @abstractmethod
    def close(self):
        """Close the connection to the sink"""
        pass
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
