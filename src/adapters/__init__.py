from .csv_adapter import CsvAdapter
from .json_adapter import JsonAdapter
from .cyme_adapter import CymeAdapter
from .twitter_adapter import TwitterAdapter

def get_adapter(config):
    adapter_type = config.get('type', '').lower()
    
    if adapter_type == 'csv':
        return CsvAdapter(config)
    elif adapter_type == 'json':
        return JsonAdapter(config)
    elif adapter_type == 'cyme':
        return CymeAdapter(config)
    elif adapter_type == 'twitter':
        return TwitterAdapter(config)
    else:
        raise ValueError(f"Unknown adapter type: {adapter_type}")
