#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CYME Adaptörü

Bu modül, CYME formatındaki ham veri dosyalarını okur ve ayrıştırır.
Dosyaları gün numarasına göre sıralı işler.
"""

import os
import glob
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

class CymeAdapter:
    """CYME formatındaki ham veri dosyalarını okuyan ve ayrıştıran adaptör"""
    
    def __init__(self, data_path, file_pattern):
        """
        CYME adaptörünü başlatır.
        
        Args:
            data_path (str): Veri dosyalarının bulunduğu dizin
            file_pattern (str): Dosya deseni (örn. "cyme_load_timeseries_day_*.txt")
        """
        self.data_path = data_path
        self.file_pattern = file_pattern
    
    def get_files(self):
        """
        Belirtilen desene uyan dosyaları bulur ve gün numarasına göre sıralar.
        
        Returns:
            list: Sıralanmış dosya yollarının listesi
        """
        try:
            # Dosya desenine uyan tüm dosyaları bul
            file_pattern_path = os.path.join(self.data_path, self.file_pattern)
            files = glob.glob(file_pattern_path)
            
            # Dosyaları gün numarasına göre sırala
            files = sorted(files, key=self._extract_day_number)
            
            logger.info(f"Toplam {len(files)} dosya bulundu ve gün numarasına göre sıralandı.")
            return files
        except Exception as e:
            logger.error(f"Dosyalar listelenirken hata oluştu: {str(e)}")
            raise
    
    def _extract_day_number(self, file_path):
        """
        Dosya adından gün numarasını çıkarır.
        
        Args:
            file_path (str): Dosya yolu
        
        Returns:
            int: Gün numarası
        """
        file_name = os.path.basename(file_path)
        match = re.search(r'day_(\d+)', file_name)
        if match:
            return int(match.group(1))
        return 0
    
    def read_file(self, file_path):
        """
        Belirtilen dosyayı okur ve satırları döndürür.
        
        Args:
            file_path (str): Dosya yolu
        
        Returns:
            list: Dosyadaki satırların listesi (başlık satırları hariç)
        """
        try:
            with open(file_path, 'r') as file:
                # Tüm satırları oku
                lines = file.readlines()
                
                # Başlık satırlarını atla
                data_lines = []
                for line in lines:
                    # Başlık satırlarını atla
                    if line.startswith('[PROFILE_VALUES]') or line.startswith('FORMAT='):
                        continue
                    # Boş olmayan satırları ekle
                    if line.strip():
                        data_lines.append(line.strip())
                
                return data_lines
        except Exception as e:
            logger.error(f"Dosya okunurken hata oluştu: {file_path}, {str(e)}")
            return []


    
    def parse_record(self, raw_data, file_name, line_number):
        """
        Ham veri satırını ayrıştırır.
        
        Args:
            raw_data (str): Ham veri satırı
            file_name (str): Dosya adı
            line_number (int): Satır numarası
        
        Returns:
            dict: Ayrıştırılmış veri veya None (geçersiz veri durumunda)
        """
        try:
            # Veriyi virgülle ayır
            parts = raw_data.split(',')
            
            # Veri doğrulama
            if len(parts) < 107:  # 11 meta bilgi + 96 değer
                logger.warning(f"Geçersiz veri satırı (yetersiz alan): {file_name}, satır {line_number}")
                return None
            
            # Meta bilgileri çıkar
            customer_id = parts[0]
            profile_type = parts[1]
            year = parts[6]
            month = parts[7]
            day = parts[8]
            
            # Kullanıcı ID'si kontrolü
            if not (customer_id.startswith('res_') or customer_id.startswith('com_')):
                logger.warning(f"Geçersiz kullanıcı ID'si: {customer_id}, {file_name}, satır {line_number}")
                return None
            
            # Tarih oluştur
            try:
                # Ay adını ay numarasına çevir
                month_num = self._month_name_to_number(month)
                date_str = f"{year}-{month_num:02d}-{int(day):02d}"
                record_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError as e:
                logger.warning(f"Geçersiz tarih: {year}-{month}-{day}, {file_name}, satır {line_number}: {str(e)}")
                return None
            
            # Yük değerlerini çıkar (11. indeksten başlayarak)
            load_values = parts[11:107]  # 96 değer
            
            # Her 15 dakikalık dilim için bir kayıt oluştur
            interval_data = {}
            
            for interval_idx, load_value in enumerate(load_values):
                # Geçerli yük değeri kontrolü
                try:
                    load_percentage = float(load_value)
                except ValueError:
                    logger.warning(f"Geçersiz yük değeri: {load_value}, {file_name}, satır {line_number}")
                    continue
                
                # Saat ve dakika hesapla
                minutes = interval_idx * 15
                hour = minutes // 60
                minute = minutes % 60
                
                # Zaman damgası oluştur (YYYY-MM-DD HH:MM formatında)
                interval_time = record_date.replace(hour=hour, minute=minute, second=0)
                interval_timestamp = interval_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # Zaman dilimi tanımlayıcısı (YYYY-MM-DD HH:MM formatında)
                interval_id = interval_time.strftime("%Y-%m-%d %H:%M")
                
                # Veri sözlüğü oluştur
                data = {
                    "raw_data": raw_data,
                    "file_name": file_name,
                    "line_number": line_number,
                    "customer_id": customer_id,
                    "profile_type": profile_type,
                    "year": int(year),
                    "month": month,
                    "month_num": month_num,
                    "day": int(day),
                    "date": date_str,
                    "interval_id": interval_id,
                    "interval_idx": interval_idx,
                    "hour": hour,
                    "minute": minute,
                    "timestamp": interval_timestamp,
                    "load_percentage": load_percentage
                }
                
                # Zaman dilimine göre gruplandır
                if interval_id not in interval_data:
                    interval_data[interval_id] = []
                
                interval_data[interval_id].append(data)
            
            return interval_data
        
        except Exception as e:
            logger.error(f"Veri ayrıştırılırken hata oluştu: {file_name}, satır {line_number}: {str(e)}")
            return None
    
    def _month_name_to_number(self, month_name):
        """
        Ay adını ay numarasına çevirir.
        
        Args:
            month_name (str): Ay adı (örn. "JANUARY", "FEBRUARY", ...)
        
        Returns:
            int: Ay numarası (1-12)
        """
        month_dict = {
            "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
            "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
            "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12
        }
        
        return month_dict.get(month_name.upper(), 1)  # Varsayılan olarak 1 (Ocak)
