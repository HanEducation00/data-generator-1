import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .base_sink import BaseSink

logger = logging.getLogger("data-generator")

class KafkaSink(BaseSink):
    """Sink for sending data to Kafka"""
    
    def __init__(self, config):
        """
        Initialize Kafka sink
        
        Args:
            config: Configuration dictionary
        """
        # BaseSink'in __init__ metodunu doğru parametreyle çağır
        super().__init__(config)
        
        # Yapılandırma parametrelerini al
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:19092")
        self.topic = config.get("topic", "data-stream")
        self.producer = None
        
        # Hemen bağlan
        self.initialize()
    
    def initialize(self):
        """Connect to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: str(v).encode('utf-8') if v else None
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise
    
    def send(self, data):
        """Send data to Kafka - Generator sınıfı tarafından çağrılır"""
        if not self.producer:
            logger.error("Not connected to Kafka, trying to reconnect...")
            self.initialize()
        
        try:
            # Batch verilerini Kafka'ya gönder
            future = self.producer.send(
                self.topic, 
                key=f"batch-{data.get('batch_id', 'unknown')}", 
                value=data
            )
            
            # Gönderimin tamamlanmasını bekle
            future.get(timeout=10)
            
            logger.info(f"Sent batch {data.get('batch_id')} with {data.get('record_count')} records to Kafka topic {self.topic}")
            return True
            
        except KafkaError as e:
            logger.error(f"Error sending batch to Kafka: {str(e)}")
            return False
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
