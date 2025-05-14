import time
import logging
import importlib
from datetime import datetime

logger = logging.getLogger("data-generator")

class DataGenerator:
    """Main data generator class"""
    
    def __init__(self, config):
        """Initialize the generator with configuration"""
        self.config = config
        self.source = None
        self.sink = None
        self.start_time = None
        self.messages_sent = 0
        
        # Load settings
        self.settings = config.get("settings", {})
        self.messages_per_second = self.settings.get("messages_per_second", 1)
        self.batch_size = self.settings.get("batch_size", 100)
        self.run_time = self.settings.get("run_time", 0)  # 0 = run indefinitely
        
        logger.info(f"Generator initialized with {self.messages_per_second} msgs/sec")
    
    def _load_adapter(self):
        """Load the appropriate adapter based on configuration"""
        source_config = self.config.get("source", {})
        adapter_type = source_config.get("type", "").lower()
        
        if not adapter_type:
            raise ValueError("Source type not specified in configuration")
        
        try:
            # Dynamically import the adapter module
            module_name = f"src.adapters.{adapter_type}_adapter"
            module = importlib.import_module(module_name)
            
            # Get the adapter class (assuming it follows naming convention)
            class_name = f"{adapter_type.capitalize()}Adapter"
            adapter_class = getattr(module, class_name)
            
            # Create an instance of the adapter
            self.source = adapter_class(source_config)
            logger.info(f"Loaded adapter: {adapter_class.__name__}")
            
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load adapter for type '{adapter_type}': {str(e)}")
            raise ValueError(f"Unsupported source type: {adapter_type}")
    
    def _load_sink(self):
        """Load the appropriate sink based on configuration"""
        sink_config = self.config.get("sink", {})
        sink_type = sink_config.get("type", "").lower()
        
        if not sink_type:
            raise ValueError("Sink type not specified in configuration")
        
        try:
            # Dynamically import the sink module
            module_name = f"src.sinks.{sink_type}_sink"
            module = importlib.import_module(module_name)
            
            # Get the sink class (assuming it follows naming convention)
            class_name = f"{sink_type.capitalize()}Sink"
            sink_class = getattr(module, class_name)
            
            # Create an instance of the sink
            self.sink = sink_class(sink_config)
            logger.info(f"Loaded sink: {sink_class.__name__}")
            
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load sink for type '{sink_type}': {str(e)}")
            raise ValueError(f"Unsupported sink type: {sink_type}")
    
    def run(self):
        """Run the data generator"""
        self._load_adapter()
        self._load_sink()
        
        self.start_time = datetime.now()
        logger.info(f"Starting data generation at {self.start_time}")
        
        try:
            with self.source, self.sink:
                self._generate_data()
        except KeyboardInterrupt:
            logger.info("Generator interrupted by user")
        except Exception as e:
            logger.error(f"Error during data generation: {str(e)}")
            raise
        finally:
            self._print_summary()
    
    def _generate_data(self):
        """Generate and send data at the specified rate"""
        while True:
            # Check if we should stop based on run_time
            if self.run_time > 0:
                elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
                if elapsed_seconds >= self.run_time:
                    logger.info(f"Reached configured run time of {self.run_time} seconds")
                    break
            
            # Calculate timing
            batch_start_time = time.time()
            
            # Read data from source
            data_batch = self.source.read_data(batch_size=self.batch_size)
            
            if not data_batch:
                logger.info("No more data to read from source")
                break
            
            # Send data to sink
            self.sink.send_data(data_batch)
            
            # Update counters
            batch_size = len(data_batch)
            self.messages_sent += batch_size
            
            # Calculate sleep time to maintain desired rate
            elapsed = time.time() - batch_start_time
            desired_batch_time = batch_size / self.messages_per_second
            sleep_time = max(0, desired_batch_time - elapsed)
            
            if sleep_time > 0:
                time.sleep(sleep_time)
            
            # Log progress
            if self.messages_sent % (self.batch_size * 10) == 0:
                logger.info(f"Sent {self.messages_sent} messages so far")
    
    def _print_summary(self):
        """Print a summary of the data generation run"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info("=" * 50)
        logger.info("Data Generation Summary")
        logger.info("=" * 50)
        logger.info(f"Start time: {self.start_time}")
        logger.info(f"End time: {end_time}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Messages sent: {self.messages_sent}")
        
        if duration > 0:
            rate = self.messages_sent / duration
            logger.info(f"Average rate: {rate:.2f} messages/second")
        
        logger.info("=" * 50)
