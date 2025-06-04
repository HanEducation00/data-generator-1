- setup.py: Projenin bir Python paketi olarak kurulmasını sağlar.
- config: Verinin alınacağı-gönderileceği-veri tipi-veri gönderme sayısı bilgileri
- src: ANA İŞLEMLER
  Ana Modüller:
generator.py: Veri üretimini kontrol eden ana sınıf.
main.py: Uygulamanın giriş noktası, komut satırı argümanlarını işler.

  Adapters Dizini:
  Farklı veri kaynaklarından veri okumak için adaptörler:

base_adapter.py    : Tüm adaptörler için temel sınıf.
csv_adapter.py     : CSV dosyalarından veri okumak için.
json_adapter.py    : JSON dosyalarından veri okumak için.
twitter_adapter.py : Twitter verisi biçiminde veri işlemek için.
cyme_adapter.py    : CYME (elektrik dağıtım sistemi) verileri için özel adaptör.

  Sinks Dizini:
  Verilerin gönderileceği hedefleri tanımlayan modüller:

base_sink.py       : Tüm hedefler için temel sınıf.
kafka_sink.py      : Kafka'ya veri göndermek için.
s3_sink.py         : AWS S3'e veri yüklemek için.
sql_sink.py        : SQL veritabanlarına veri eklemek için.

  Utils Dizini
  Yardımcı işlevler ve sınıflar:

config_loader.py   : Yapılandırma dosyalarını okur ve doğrular.
logging_utils.py   : Loglama fonksiyonları.
schema_validator.py: Veri şemalarını doğrulamak için.


Veri Kaynağı (CSV/JSON/CYME) → Adapter → Generator → Sink (Kafka/S3/SQL)
Adapter  : Kaynak verilerini okur ve yapılandırır
Generator: Adaptör'den verileri alır, hız kontrolü ve işleme yapar
Sink     : İşlenmiş verileri hedef sisteme (Kafka, S3, SQL DB) gönder

Kullanım Örneği
Örneğin, CYME verilerini Kafka'ya göndermek için:
config/cyme_config.yaml dosyasını oluşturun
Bu yapılandırmada doğru adaptör (cyme_adapter) ve sink (kafka_sink) belirtin
python3 -m src.main --config config/cyme_kafka_config.yaml
 komutuyla çalıştırın

conda env list
conda activate data-gen-env