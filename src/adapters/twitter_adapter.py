from .base_adapter import BaseAdapter
import os
import json
import glob

class TwitterAdapter(BaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        self.data_path = config.get('data_path')
        self.file_pattern = config.get('file_pattern', '*.json')
        self.files = []
        self.current_file_index = 0
        self.current_data = None
        self.current_position = 0
        
    def initialize(self):
        """Veri dosyalarını bul ve hazırla"""
        pattern = os.path.join(self.data_path, self.file_pattern)
        self.files = sorted(glob.glob(pattern))
        if not self.files:
            raise ValueError(f"No files found matching pattern: {pattern}")
        self._load_next_file()
        return True
        
    def _load_next_file(self):
        """Sıradaki dosyayı yükle"""
        if self.current_file_index < len(self.files):
            file_path = self.files[self.current_file_index]
            with open(file_path, 'r') as f:
                self.current_data = json.load(f)
            self.current_position = 0
            self.current_file_index += 1
            return True
        return False
        
    def get_data(self, batch_size):
        """Belirtilen batch boyutunda veri al"""
        if self.current_data is None:
            return []
            
        records = []
        remaining = batch_size
        
        while remaining > 0:
            # Mevcut dosyadan veri al
            if isinstance(self.current_data, list):
                available = len(self.current_data) - self.current_position
                if available > 0:
                    batch = min(available, remaining)
                    end_pos = self.current_position + batch
                    
                    batch_records = self.current_data[self.current_position:end_pos]
                    records.extend(batch_records)
                    
                    self.current_position += batch
                    remaining -= batch
            else:
                # Tek bir JSON nesnesi ise
                if self.current_position == 0:
                    records.append(self.current_data)
                    self.current_position = 1
                    remaining -= 1
            
            # Eğer mevcut dosya bittiyse ve daha veri gerekiyorsa, sonraki dosyaya geç
            if remaining > 0 and (
                (isinstance(self.current_data, list) and self.current_position >= len(self.current_data)) or
                (not isinstance(self.current_data, list) and self.current_position >= 1)
            ):
                if not self._load_next_file():
                    break  # Başka dosya kalmadı
        
        return records
        
    def has_more_data(self):
        """Daha fazla veri olup olmadığını kontrol et"""
        if self.current_data is not None:
            if isinstance(self.current_data, list):
                if self.current_position < len(self.current_data):
                    return True
            else:
                if self.current_position < 1:
                    return True
        return self.current_file_index < len(self.files)
