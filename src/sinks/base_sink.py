class BaseSink:
    def __init__(self, config):
        self.config = config
        
    def initialize(self):
        """Hedef sisteme bağlantıyı başlat"""
        raise NotImplementedError("Subclasses must implement initialize()")
        
    def send(self, records):
        """Verileri hedef sisteme gönder"""
        raise NotImplementedError("Subclasses must implement send()")
        
    def close(self):
        """Bağlantıyı kapat"""
        raise NotImplementedError("Subclasses must implement close()")
    
    # Eski metodlar için uyumluluk sağlayıcılar
    def connect(self):
        return self.initialize()
        
    def send_data(self, data):
        return self.send(data)
