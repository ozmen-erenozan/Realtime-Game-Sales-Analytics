import os
import shutil
import sys

# Java Ayarı
java_path = r"C:\Program Files\Amazon Corretto\jdk17.0.17_10" 
if os.path.exists(java_path):
    os.environ["JAVA_HOME"] = java_path
    os.environ["PATH"] = os.path.join(java_path, "bin") + ";" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

print(" MODEL EĞİTİMİ BAŞLIYOR...")

spark = SparkSession.builder.appName("VGSales_Trainer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Veriyi Yükle
if not os.path.exists("vgsales.csv"):
    print(" HATA: vgsales.csv yok!")
    sys.exit()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("vgsales.csv")
# Temizlik
df = df.na.drop(subset=["Year", "Publisher", "Genre", "Platform", "Global_Sales"])
df = df.withColumn("Year", df["Year"].cast("int"))
df = df.withColumn("Global_Sales", df["Global_Sales"].cast("double"))

# Pipeline
genre_indexer = StringIndexer(inputCol="Genre", outputCol="Genre_Idx", handleInvalid="keep")
platform_indexer = StringIndexer(inputCol="Platform", outputCol="Platform_Idx", handleInvalid="keep")
publisher_indexer = StringIndexer(inputCol="Publisher", outputCol="Publisher_Idx", handleInvalid="keep")

assembler = VectorAssembler(
    inputCols=["Genre_Idx", "Platform_Idx", "Publisher_Idx", "Year"],
    outputCol="features"
)

rf = RandomForestRegressor(featuresCol="features", labelCol="Global_Sales", numTrees=50, maxBins=2000)

pipeline = Pipeline(stages=[genre_indexer, platform_indexer, publisher_indexer, assembler, rf])

# Eğitim ve Kayıt
print(" Eğitiliyor...")
model = pipeline.fit(df)

model_path = "saved_streaming_model"
if os.path.exists(model_path):
    try:
        shutil.rmtree(model_path)
    except:
        pass

model.save(model_path)
print(f" MODEL HAZIR: {model_path}")
spark.stop()