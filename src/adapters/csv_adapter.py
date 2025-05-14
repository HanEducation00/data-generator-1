import os
import glob
import pandas as pd
import logging
from .base_adapter import BaseAdapter

logger = logging.getLogger("data-generator")

class CsvAdapter(BaseAdapter):
    """Adapter for reading data from CSV files"""
    
    def __init__(self, config):
        super().__init__(config)
        self.path = config.get("path", ".")
        self.pattern = config.get("pattern", "*.csv")
        self.files = []
        self.current_file = None
        self.current_data = None
        self.current_index = 0
    
    def connect(self):
        """Find all CSV files matching the pattern"""
        pattern = os.path.join(self.path, self.pattern)
        self.files = sorted(glob.glob(pattern))
        
        if not self.files:
            logger.warning(f"No CSV files found matching pattern: {pattern}")
        else:
            logger.info(f"Found {len(self.files)} CSV files")
            # Open the first file
            self._open_next_file()
    
    def _open_next_file(self):
        """Open the next file in the list"""
        if not self.files:
            return False
        
        self.current_file = self.files.pop(0)
        logger.info(f"Opening CSV file: {self.current_file}")
        
        try:
            self.current_data = pd.read_csv(self.current_file)
            self.current_index = 0
            return True
        except Exception as e:
            logger.error(f"Error reading CSV file {self.current_file}: {str(e)}")
            return self._open_next_file()  # Try the next file
    
    def read_data(self, batch_size=100):
        """Read a batch of data from the current CSV file"""
        if self.current_data is None:
            return []
        
        # Calculate end index for current batch
        end_index = min(self.current_index + batch_size, len(self.current_data))
        
        # If we've reached the end of the current file
        if self.current_index >= len(self.current_data):
            # Try to open the next file
            if not self._open_next_file():
                return []  # No more files
            
            # Recalculate end index for the new file
            end_index = min(batch_size, len(self.current_data))
        
        # Extract the batch
        batch = self.current_data.iloc[self.current_index:end_index]
        
        # Convert to list of dictionaries
        records = batch.to_dict(orient="records")
        
        # Update the current index
        self.current_index = end_index
        
        logger.debug(f"Read {len(records)} records from {self.current_file}")
        return records
    
    def close(self):
        """Close the adapter"""
        self.current_data = None
        logger.info("CSV adapter closed")
