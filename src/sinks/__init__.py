from .kafka_sink import KafkaSink
from .s3_sink import S3Sink
from .sql_sink import SqlSink

def get_sink(config):
    sink_type = config.get('type', '').lower()
    
    if sink_type == 'kafka':
        return KafkaSink(config)
    elif sink_type == 's3':
        return S3Sink(config)
    elif sink_type == 'sql':
        return SqlSink(config)
    else:
        raise ValueError(f"Unknown sink type: {sink_type}")
