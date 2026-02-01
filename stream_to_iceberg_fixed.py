import os
import sys
import shutil

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

# --- KRİTİK DÜZELTME 1: Mutlak Yol (Absolute Path) ---
# Bu satır, Dashboard ile Spark'ın aynı klasöre bakmasını garanti eder.
warehouse_path = os.path.abspath("iceberg_warehouse")
print(f" Iceberg Depo Yolu: {warehouse_path}")

# Checkpoint temizliği
checkpoint_dir = "checkpoints/vgsales_iceberg"
if os.path.exists(checkpoint_dir):
    try:
        shutil.rmtree(checkpoint_dir)
    except:
        pass

print(" SPARK STREAMING: FULL CAPTURE MODU...")

PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"

spark = SparkSession.builder \
    .appName("VGSales_Iceberg_Writer") \
    .config("spark.jars.packages", PKG) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. MODELİ YÜKLE
model_path = "saved_streaming_model"
if not os.path.exists(model_path):
    print(" HATA: Model yok! 'train_final.py' çalıştır.")
    sys.exit()
loaded_model = PipelineModel.load(model_path)

# Tabloyu Yarat
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.live_predictions (
        Name STRING, Platform STRING, Genre STRING, 
        Real_Sales DOUBLE, AI_Prediction DOUBLE, Transaction_Time STRING
    ) USING iceberg
""")

# 3. AKIŞI DİNLE
user_schema = StructType([
    StructField("Rank", IntegerType()), StructField("Name", StringType()),
    StructField("Platform", StringType()), StructField("Year", IntegerType()),
    StructField("Genre", StringType()), StructField("Publisher", StringType()),
    StructField("NA_Sales", DoubleType()), StructField("EU_Sales", DoubleType()),
    StructField("JP_Sales", DoubleType()), StructField("Other_Sales", DoubleType()),
    StructField("Global_Sales", DoubleType()), StructField("Transaction_Time", StringType())
])

df_stream = spark.readStream \
    .schema(user_schema) \
    .option("header", "true") \
    .csv("stream_input")

# Temizlik (Null Safe)
df_clean = df_stream.na.drop(subset=["Genre", "Platform", "Publisher", "Year"])

# 4. TAHMİN
predictions = loaded_model.transform(df_clean)

output_df = predictions.select(
    col("Name"), col("Platform"), col("Genre"),
    col("Global_Sales").alias("Real_Sales"),
    col("prediction").alias("AI_Prediction"),
    col("Transaction_Time")
)

print(" Veriler Iceberg'e yazılıyor... (Arka planda çalışır)")

# 5. YAZMA
query = output_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("path", "local.db.live_predictions") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()