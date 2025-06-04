#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yapılandırma Yükleyici

Bu modül, yapılandırma dosyasını okur ve yapılandırma parametrelerini sağlar.
"""

import yaml
import logging
import os

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Yapılandırma dosyasını yöneten sınıf"""
    
    def __init__(self, config_path):
        """
        Yapılandırma yükleyiciyi başlatır.
        
        Args:
            config_path (str): Yapılandırma dosyasının yolu
        """
        self.config_path = config_path
    
    def load(self):
        """
        Yapılandırma dosyasını yükler.
        
        Returns:
            dict: Yapılandırma parametreleri
        """
        try:
            if not os.path.exists(self.config_path):
                logger.error(f"Yapılandırma dosyası bulunamadı: {self.config_path}")
                raise FileNotFoundError(f"Yapılandırma dosyası bulunamadı: {self.config_path}")
            
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            
            logger.info(f"Yapılandırma dosyası başarıyla yüklendi: {self.config_path}")
            
            # Yapılandırma doğrulama
            self._validate_config(config)
            
            return config
        except Exception as e:
            logger.error(f"Yapılandırma dosyası yüklenirken hata oluştu: {str(e)}")
            raise
    
    def _validate_config(self, config):
        """
        Yapılandırma parametrelerini doğrular.
        
        Args:
            config (dict): Yapılandırma parametreleri
        
        Raises:
            ValueError: Geçersiz yapılandırma durumunda
        """
        # Gerekli bölümlerin varlığını kontrol et
        required_sections = ['source', 'sink', 'generator']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Yapılandırma dosyasında '{section}' bölümü bulunamadı")
        
        # Kaynak yapılandırmasını doğrula
        source = config['source']
        if 'type' not in source:
            raise ValueError("Kaynak yapılandırmasında 'type' alanı bulunamadı")
        if source['type'] == 'cyme':
            if 'data_path' not in source:
                raise ValueError("CYME kaynak yapılandırmasında 'data_path' alanı bulunamadı")
            if 'file_pattern' not in source:
                raise ValueError("CYME kaynak yapılandırmasında 'file_pattern' alanı bulunamadı")
        
        # Hedef yapılandırmasını doğrula
        sink = config['sink']
        if 'type' not in sink:
            raise ValueError("Hedef yapılandırmasında 'type' alanı bulunamadı")
        if sink['type'] == 'kafka':
            if 'bootstrap_servers' not in sink:
                raise ValueError("Kafka hedef yapılandırmasında 'bootstrap_servers' alanı bulunamadı")
            if 'topic' not in sink:
                raise ValueError("Kafka hedef yapılandırmasında 'topic' alanı bulunamadı")
        
        # Jeneratör yapılandırmasını doğrula
        generator = config['generator']
        if 'batch_size' not in generator:
            logger.warning("Jeneratör yapılandırmasında 'batch_size' alanı bulunamadı, varsayılan değer kullanılacak: 100")
        if 'interval_ms' not in generator:
            logger.warning("Jeneratör yapılandırmasında 'interval_ms' alanı bulunamadı, varsayılan değer kullanılacak: 1000")
