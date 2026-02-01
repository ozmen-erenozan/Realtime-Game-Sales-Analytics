# Realtime Game Sales Analytics

Bu proje, video oyun satış verilerini simüle eden, **Apache Spark** ve **Iceberg** kullanarak işleyen ve **Streamlit** ile canlı olarak görselleştiren bir büyük veri analitik platformudur.

# Özellikler

- **Canlı Veri Simülasyonu:** Gerçek oyun verilerini (dataset) zaman damgasıyla parçalara ayırarak anlık akış sağlar.
- **Büyük Veri İşleme (Spark Streaming):** Akan veriyi gerçek zamanlı okur, temizler ve ML modeli ile tahmin üretir.
- **Modern Depolama (Apache Iceberg):** Verileri ACID transaksiyon özellikli Iceberg formatında saklar.
- **Canlı Dashboard:** Streamlit ara yüzü ile yapay zeka tahminlerini ve gerçek satışları anlık karşılaştırır.

# Proje Yapısı

- `producer_simulator.py`: Veri akışını başlatan simülatör.
- `stream_to_iceberg_fixed.py`: Spark Streaming ile veriyi işleyen ve Iceberg'e kaydeden ana motor.
- `dashboard.py`: Verileri görselleştiren kullanıcı arayüzü.
- `train_model.py`: Satış tahmin modelini (Linear Regression) eğiten script.
- `vgsales.csv`: Kullanılan orijinal veri seti.

# Kurulum ve Gereksinimler

Projenin çalışması için aşağıdaki yazılımların kurulu olması gerekir:

1.  **Python 3.8+**
2.  **Java JDK 17** (Amazon Corretto 17 önerilir) - Spark için gereklidir.
3.  **Gerekli Kütüphaneler:**
    ```bash
    pip install pyspark pandas streamlit plotly scikit-learn
    ```

# Nasıl Çalıştırılır?

Projeyi çalıştırmak için **3 ayrı terminal** açmanız gerekir:

# 1. Adım: Simülatörü Başlat (Terminal 1)
Bu script, verileri `stream_input` klasörüne parça parça göndermeye başlar.
```bash
python producer_simulator.py
```

# 2. Adım: Spark Streaming Motorunu Başlat (Terminal 2)
Verileri okur, işler ve Iceberg tablosuna yazar. (İlk çalıştırmada model yok hatası alırsanız önce `python train_model.py` çalıştırın).
```bash
python stream_to_iceberg_fixed.py
```

# 3. Adım: Dashboard'u Başlat (Terminal 3)
Tarayıcıda analizleri görmek için:
```bash
streamlit run dashboard.py
```

# Notlar
- İlk kez çalıştırıyorsanız `train_model.py` dosyasını bir kez çalıştırarak AI modelini oluşturun.
- `stream_input` ve `checkpoints` klasörleri çalışma sırasında otomatik oluşturulur ve `gitignore` ile hariç tutulmuştur.
