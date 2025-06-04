#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Jeneratörü

Bu modül, ham verileri okur, işler ve hedef sistemlere gönderir.
Verileri zaman dilimlerine göre gruplandırır ve sıralı işler.
"""

import logging
import time
import signal
import sys
from datetime import datetime
import json

logger = logging.getLogger(__name__)

# Sinyal işleyici
def signal_handler(sig, frame):
    logger.info("Program kullanıcı tarafından durduruldu (Ctrl+C)")
    sys.exit(0)

# SIGINT (Ctrl+C) sinyalini yakala
signal.signal(signal.SIGINT, signal_handler)

class Generator:
    """Veri jeneratörü sınıfı"""
    
    def __init__(self, adapter, sink, config):
        """
        Veri jeneratörünü başlatır.
        
        Args:
            adapter: Veri adaptörü (örn. CymeAdapter)
            sink: Veri hedefi (örn. KafkaSink)
            config (dict): Yapılandırma parametreleri
        """
        self.adapter = adapter
        self.sink = sink
        self.config = config
        
        # Yapılandırma parametrelerini al
        self.batch_size = config.get("batch_size", 100)
        self.interval_ms = config.get("interval_ms", 1000)
        self.max_records = config.get("max_records", 0)
        self.loop = config.get("loop", False)
    
    def run(self):
        """
        Veri jeneratörünü çalıştırır.
        Dosyaları sıralı okur, verileri zaman dilimlerine göre gruplandırır ve hedef sisteme gönderir.
        """
        try:
            # Dosyaları al ve sırala
            files = self.adapter.get_files()
            logger.info(f"Toplam {len(files)} dosya işlenecek.")
            
            # Tüm zaman dilimlerini ve verilerini saklamak için sözlük
            all_time_intervals = {}
            sent_count = 0
            batch_count = 0
            
            # Dosyaları sırayla işle
            for file_idx, file_path in enumerate(files, 1):
                file_name = file_path.split("/")[-1]
                logger.info(f"Dosya işleniyor ({file_idx}/{len(files)}): {file_name}")
                
                # Dosyadan verileri oku
                records = self.adapter.read_file(file_path)
                logger.info(f"{file_name}: {len(records)} kayıt okundu.")
                
                # Her kaydı işle
                for line_number, raw_data in enumerate(records, 1):
                    # Ham veriyi işle ve zaman dilimlerine göre gruplandır
                    interval_data = self.adapter.parse_record(raw_data, file_name, line_number)
                    
                    if interval_data:
                        # Her zaman dilimi için verileri sakla
                        for interval_id, data_list in interval_data.items():
                            if interval_id not in all_time_intervals:
                                all_time_intervals[interval_id] = []
                            
                            all_time_intervals[interval_id].extend(data_list)
                
                # Her dosya işlendikten sonra, o ana kadar toplanan verileri gönder
                if all_time_intervals:
                    logger.info(f"Dosya {file_name} işlendi, toplanan verileri gönderiliyor...")
                    
                    # Zaman dilimlerini sırala
                    sorted_intervals = sorted(all_time_intervals.keys())
                    
                    # Her zaman dilimi için verileri gönder
                    for interval_id in sorted_intervals:
                        interval_records = all_time_intervals[interval_id]
                        
                        # Zaman dilimi verilerini batch'lere böl
                        batch_size = self.batch_size if self.batch_size > 0 else 150  # Varsayılan batch boyutu
                        
                        # Zaman dilimi başlangıcını logla
                        logger.info(f"Zaman dilimi {interval_id} için {len(interval_records)} kayıt gönderiliyor...")
                        
                        # Batch'ler halinde gönder
                        for i in range(0, len(interval_records), batch_size):
                            batch = interval_records[i:i+batch_size]
                            
                            # Her kayda zaman dilimi bilgisini ekle
                            for record in batch:
                                record["batch_id"] = f"{batch_count}"
                                record["processing_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            
                            # Batch'i JSON formatına dönüştür
                            batch_json = {
                                "interval_id": interval_id,
                                "batch_id": batch_count,
                                "record_count": len(batch),
                                "processing_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                                "records": batch
                            }
                            
                            # Hedef sisteme gönder
                            self.sink.send(batch_json)
                            
                            sent_count += len(batch)
                            batch_count += 1
                            
                            # Batch gönderim aralığı kadar bekle (aynı zaman dilimi içindeki batch'ler arasında)
                            if i + batch_size < len(interval_records):
                                time.sleep(0.1)  # 100 ms bekle
                        
                        # Zaman dilimi sonunu logla
                        logger.info(f"Zaman dilimi {interval_id} için toplam {len(interval_records)} kayıt gönderildi.")
                        
                        # Zaman dilimleri arasında bekle
                        logger.info(f"Bir sonraki zaman dilimi için {self.interval_ms/1000} saniye bekleniyor...")
                        time.sleep(self.interval_ms / 1000)
                        
                        # Maksimum kayıt sayısı kontrolü
                        if self.max_records > 0 and sent_count >= self.max_records:
                            logger.info(f"Maksimum kayıt sayısına ulaşıldı: {self.max_records}")
                            return
                    
                    # Gönderilen verileri temizle
                    all_time_intervals.clear()
            
            logger.info("Tüm veriler gönderildi.")
            
        except Exception as e:
            logger.error(f"Veri jeneratörü çalışırken hata oluştu: {str(e)}", exc_info=True)

    
    def _send_data_by_intervals(self, all_time_intervals, sorted_intervals):
        """
        Zaman dilimlerine göre gruplandırılmış verileri hedef sisteme gönderir.
        
        Args:
            all_time_intervals (dict): Zaman dilimlerine göre gruplandırılmış veriler
            sorted_intervals (list): Sıralanmış zaman dilimi tanımlayıcıları
        """
        sent_count = 0
        batch_count = 0
        
        logger.info("Veriler hedef sisteme gönderiliyor...")
        
        try:
            while True:
                for interval_id in sorted_intervals:
                    interval_records = all_time_intervals[interval_id]
                    
                    # Batch'ler halinde gönder
                    for i in range(0, len(interval_records), self.batch_size):
                        batch = interval_records[i:i+self.batch_size]
                        
                        # Her kayda zaman dilimi bilgisini ekle
                        for record in batch:
                            record["batch_id"] = f"{batch_count}"
                            record["processing_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        
                        # Batch'i JSON formatına dönüştür
                        batch_json = {
                            "interval_id": interval_id,
                            "batch_id": batch_count,
                            "record_count": len(batch),
                            "processing_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                            "records": batch
                        }
                        
                        # Hedef sisteme gönder
                        self.sink.send(batch_json)
                        
                        sent_count += len(batch)
                        batch_count += 1
                        
                        logger.info(f"Zaman dilimi {interval_id} için {len(batch)} kayıt gönderildi. "
                                   f"Toplam: {sent_count} kayıt, {batch_count} batch.")
                        
                        # Gönderim aralığı kadar bekle
                        time.sleep(self.interval_ms / 1000)
                        
                        # Maksimum kayıt sayısı kontrolü
                        if self.max_records > 0 and sent_count >= self.max_records:
                            logger.info(f"Maksimum kayıt sayısına ulaşıldı: {self.max_records}")
                            return
                
                # Döngü kontrolü
                if not self.loop:
                    logger.info("Tüm veriler gönderildi.")
                    break
                
                logger.info("Döngü modu etkin, veriler tekrar gönderiliyor...")
                
        except KeyboardInterrupt:
            logger.info("Program kullanıcı tarafından durduruldu.")
        except Exception as e:
            logger.error(f"Veri gönderimi sırasında hata oluştu: {str(e)}", exc_info=True)
