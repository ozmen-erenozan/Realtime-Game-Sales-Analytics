import os
import sys

# 1. AYARLAR
java_path = r"C:\Program Files\Amazon Corretto\jdk17.0.17_10" 
if os.path.exists(java_path):
    os.environ["JAVA_HOME"] = java_path
    os.environ["PATH"] = os.path.join(java_path, "bin") + ";" + os.environ["PATH"]

current_python = sys.executable
os.environ['PYSPARK_PYTHON'] = current_python
os.environ['PYSPARK_DRIVER_PYTHON'] = current_python

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

print(" SPARK STREAMING: FULL CAPTURE MODU...")

spark = SparkSession.builder \
    .appName("VGSales_Full_Capture") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. MODELİ YÜKLE
model_path = "saved_streaming_model"
if not os.path.exists(model_path):
    print(" HATA: Model yok! 'train_final.py' çalıştır.")
    sys.exit()
loaded_model = PipelineModel.load(model_path)

# 3. AKIŞI DİNLE (Kısıtlama Yok!)
user_schema = StructType([
    StructField("Rank", IntegerType()), StructField("Name", StringType()),
    StructField("Platform", StringType()), StructField("Year", IntegerType()),
    StructField("Genre", StringType()), StructField("Publisher", StringType()),
    StructField("NA_Sales", DoubleType()), StructField("EU_Sales", DoubleType()),
    StructField("JP_Sales", DoubleType()), StructField("Other_Sales", DoubleType()),
    StructField("Global_Sales", DoubleType()), StructField("Transaction_Time", StringType())
])

# BURASI ÖNEMLİ: maxFilesPerTrigger'ı kaldırdık!
# Spark artık klasörde ne kadar dosya birikmişse hepsini tek seferde alacak.
df_stream = spark.readStream \
    .schema(user_schema) \
    .option("header", "true") \
    .csv("stream_input")

# Temizlik (Null Safe)
df_clean = df_stream.na.drop(subset=["Genre", "Platform", "Publisher", "Year"])

# 4. TAHMİN
predictions = loaded_model.transform(df_clean)

output_df = predictions.select(
    col("Name"),
    col("Global_Sales").alias("Real"),
    col("prediction").alias("AI_Tahmin"),
    col("Transaction_Time")
)

print(" TÜM VERİLER YAKALANIYOR...")

# 5. EKRANA BAS (LİMİTSİZ)
# numRows=1000 yaptık. Bir pakette 1000 oyun olsa bile hepsini listeler.
# truncate=False yaptık. İsimleri kesmeden tam gösterir.
query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .trigger(processingTime='0 seconds') \
    .start()

query.awaitTermination()