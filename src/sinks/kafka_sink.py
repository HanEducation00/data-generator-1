import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .base_sink import BaseSink

logger = logging.getLogger("data-generator")

class KafkaSink(BaseSink):
    """Sink for sending data to Kafka"""
    
    def __init__(self, config):
        super().__init__(config)
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        self.topic = config.get("topic", "data-stream")
        self.producer = None
    
    def connect(self):
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
    
    def send_data(self, data):
        """Send data to Kafka"""
        if not self.producer:
            raise RuntimeError("Not connected to Kafka")
        
        sent_count = 0
        for record in data:
            try:
                # Use the 'id' field as the key if available
                key = record.get('id', None)
                
                # Send the record to Kafka
                future = self.producer.send(self.topic, key=key, value=record)
                
                # Wait for the send to complete
                future.get(timeout=10)
                sent_count += 1
                
            except KafkaError as e:
                logger.error(f"Error sending message to Kafka: {str(e)}")
        
        logger.debug(f"Sent {sent_count} messages to Kafka topic {self.topic}")
        return sent_count
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
