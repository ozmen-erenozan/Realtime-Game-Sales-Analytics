import pandas as pd
import time
import os
import shutil

# --- AYARLAR ---
SOURCE_FILE = "vgsales.csv"       # Direkt orijinal dosyayı kullanıyoruz
STREAM_FOLDER = "stream_input"    
CHUNK_SIZE = 10                   
SLEEP_TIME = 5                    

print(" SİMÜLATÖR BAŞLATILIYOR...")

# Klasör temizliği
if os.path.exists(STREAM_FOLDER):
    shutil.rmtree(STREAM_FOLDER)
os.makedirs(STREAM_FOLDER)

# Kaynak Kontrolü
if not os.path.exists(SOURCE_FILE):
    print(f" HATA: '{SOURCE_FILE}' bulunamadı! Lütfen dosyayı proje klasörüne koy.")
    exit()

# Veriyi yükle ve anlık temizle
print(" Veri okunuyor ve temizleniyor...")
df_source = pd.read_csv(SOURCE_FILE)
# Boş verileri at
df_source = df_source.dropna(subset=['Year', 'Publisher', 'Genre', 'Platform', 'Global_Sales'])
# Yılı tam sayı yap
df_source['Year'] = df_source['Year'].astype(int)

print(f" {len(df_source)} adet oyun hafızaya alındı. Yayın başlıyor...")
print("-" * 50)

counter = 1
try:
    while True:
        df_chunk = df_source.sample(n=CHUNK_SIZE)
        
        # Zaman damgası ekle
        current_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        df_chunk['Transaction_Time'] = current_time
        
        file_name = f"sales_part_{counter}.csv"
        file_path = os.path.join(STREAM_FOLDER, file_name)
        df_chunk.to_csv(file_path, index=False)
        
        print(f" [{current_time}] Paket #{counter} gönderildi -> {file_name}")
        counter += 1
        time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
    print("\n Yayın kesildi.")