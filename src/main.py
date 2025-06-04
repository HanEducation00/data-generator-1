#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ana Uygulama

Bu script, veri jeneratörünü başlatır ve çalıştırır.
"""

import logging
import sys
import os
import argparse

# Doğru import yolları
from src.utils.config_loader import ConfigLoader
from src.adapters.cyme_adapter import CymeAdapter
from src.sinks.kafka_sink import KafkaSink
from src.generator import Generator

# Loglama ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('generator.log')
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Ana uygulama fonksiyonu"""
    try:
        # Komut satırı argümanlarını ayrıştır
        parser = argparse.ArgumentParser(description='Veri Jeneratörü')
        parser.add_argument('--config', type=str, default='config/generator_config.yaml',
                            help='Yapılandırma dosyasının yolu')
        args = parser.parse_args()
        
        # Yapılandırma dosyasını oku
        config_path = args.config
        logger.info(f"Yapılandırma dosyası okunuyor: {config_path}")
        
        config_loader = ConfigLoader(config_path)
        config = config_loader.load()
        
        # Kaynak ve hedef yapılandırmalarını al
        source_config = config.get('source', {})
        sink_config = config.get('sink', {})
        generator_config = config.get('generator', {})
        
        # Kaynak tipi kontrolü
        source_type = source_config.get('type')
        if source_type != 'cyme':
            logger.error(f"Desteklenmeyen kaynak tipi: {source_type}")
            return
        
        # Hedef tipi kontrolü
        sink_type = sink_config.get('type')
        if sink_type != 'kafka':
            logger.error(f"Desteklenmeyen hedef tipi: {sink_type}")
            return
        
        # Adaptör ve hedef nesnelerini oluştur
        adapter = CymeAdapter(
            data_path=source_config.get('data_path'),
            file_pattern=source_config.get('file_pattern')
        )
        
        sink = KafkaSink(sink_config)
        
        
        # Jeneratörü oluştur ve çalıştır
        generator = Generator(adapter, sink, generator_config)
        
        logger.info("Veri jeneratörü başlatılıyor...")
        generator.run()
        
        # Kaynakları temizle
        sink.close()
        
        logger.info("Veri jeneratörü tamamlandı.")
        
    except Exception as e:
        logger.error(f"Uygulama çalışırken hata oluştu: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
