class BaseAdapter:
    def __init__(self, config):
        self.config = config
        
    def initialize(self):
        """Veri kaynağını başlat"""
        raise NotImplementedError("Subclasses must implement initialize()")
        
    def get_data(self, batch_size):
        """Belirtilen batch boyutunda veri al"""
        raise NotImplementedError("Subclasses must implement get_data()")
        
    def has_more_data(self):
        """Daha fazla veri olup olmadığını kontrol et"""
        raise NotImplementedError("Subclasses must implement has_more_data()")
