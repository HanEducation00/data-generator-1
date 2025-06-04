from .base_adapter import BaseAdapter
import os
import glob
import csv

class CsvAdapter(BaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        self.data_path = config.get('data_path')
        self.file_pattern = config.get('file_pattern', '*.csv')
        self.delimiter = config.get('delimiter', ',')
        self.files = []
        self.current_file_index = 0
        self.current_data = None
        self.current_position = 0
        self.headers = None
        
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
            with open(file_path, 'r', newline='') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                self.headers = next(reader)  # İlk satırı başlık olarak al
                self.current_data = list(reader)
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
            available = len(self.current_data) - self.current_position
            if available > 0:
                batch = min(available, remaining)
                end_pos = self.current_position + batch
                
                for i in range(self.current_position, end_pos):
                    row = self.current_data[i]
                    record = {self.headers[j]: value for j, value in enumerate(row) if j < len(self.headers)}
                    records.append(record)
                
                self.current_position += batch
                remaining -= batch
            
            # Eğer mevcut dosya bittiyse ve daha veri gerekiyorsa, sonraki dosyaya geç
            if remaining > 0 and self.current_position >= len(self.current_data):
                if not self._load_next_file():
                    break  # Başka dosya kalmadı
        
        return records
        
    def has_more_data(self):
        """Daha fazla veri olup olmadığını kontrol et"""
        if self.current_data is not None and self.current_position < len(self.current_data):
            return True
        return self.current_file_index < len(self.files)
