import argparse
import sys
import os
from src.utils.logging_utils import setup_logging
from src.utils.config_loader import load_config
from src.generator import DataGenerator

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Data Generator")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    return parser.parse_args()

def main():
    """Main entry point for the application"""
    # Parse command line arguments
    args = parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Setup logging
        log_level = config.get("settings", {}).get("log_level", "INFO")
        logger = setup_logging(log_level)
        
        # Create and run the generator
        generator = DataGenerator(config)
        generator.run()
        
        return 0
    except FileNotFoundError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Configuration error: {str(e)}", file=sys.stderr)
        return 1
    except Exception as
