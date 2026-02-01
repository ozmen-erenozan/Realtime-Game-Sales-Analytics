import streamlit as st
import pandas as pd
import time
import sys
import os
import plotly.express as px
import plotly.graph_objects as go

# ==============================================================================
# 1. AYARLAR VE BAĞLANTI (SPARK & ICEBERG)
# ==============================================================================
st.set_page_config(page_title="Big Data Game Analytics", layout="wide", page_icon="🎮")

@st.cache_resource
def get_spark_session():
    java_path = r"C:\Program Files\Amazon Corretto\jdk17.0.17_10"
    if os.path.exists(java_path):
        os.environ["JAVA_HOME"] = java_path
        os.environ["PATH"] = os.path.join(java_path, "bin") + ";" + os.environ["PATH"]

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    warehouse_path = os.path.abspath("iceberg_warehouse")
    PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"

    spark = (
        SparkSession.builder.appName("VGSales_Dashboard_Client")
        .config("spark.jars.packages", PKG)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )

    return spark


# Spark'ı başlat (veya önbellekten getir)
try:
    spark = get_spark_session()
except Exception as e:
    st.error(
        f"Spark başlatılamadı. Lütfen tüm terminalleri kapatıp tekrar deneyin. Hata: {e}"
    )
    st.stop()

# ==============================================================================
# 2. VERİ YÜKLEME FONKSİYONLARI
# ==============================================================================


@st.cache_data
def load_historical_data():
    """
    Geçmiş veri (statik CSV). İstersen burayı vgsales_cleaned.csv yapabilirsin.
    """
    if os.path.exists("vgsales.csv"):
        return pd.read_csv("vgsales.csv")
    return pd.DataFrame()


def load_live_data():
    """
    Iceberg tablosundan EN YENİ 25 kaydı alır,
    sonra zaman ekseninde SOLDAN SAĞA doğru görmek için Transaction_Time'a göre artan sıralar.
    """

    spark.catalog.refreshTable("local.db.live_predictions")
    
    try:
        df = spark.sql(
            """
            SELECT * FROM local.db.live_predictions
            ORDER BY Transaction_Time DESC
            LIMIT 25
        """
        ).toPandas()

        if df.empty:
            return df

        # Zaman tipine çevir
        if "Transaction_Time" in df.columns:
            df["Transaction_Time"] = pd.to_datetime(df["Transaction_Time"])
            # Grafikte soldan sağa zamanın akması için küçükten büyüğe sırala
            df = df.sort_values("Transaction_Time")

        return df
    except Exception:
        return pd.DataFrame()


# ==============================================================================
# 3. ARAYÜZ TASARIMI
# ==============================================================================
st.title(" Oyun Sektörü Büyük Veri Analiz Platformu")
st.markdown("---")

tab1, tab2 = st.tabs([" GEÇMİŞ ANALİZİ (Historical)", " CANLI YAYIN (Real-Time)"])

# ------------------------------------------------------------------------------
# TAB 1: GEÇMİŞ VERİLERİN ANALİZİ
# ------------------------------------------------------------------------------
with tab1:
    df_hist = load_historical_data()

    if not df_hist.empty:
        st.header(" 1980–2020 Pazar Analizi")

        # Filtre
        selected_genre = st.selectbox(
            "Oyun Türü Filtrele:", ["Tümü"] + list(df_hist["Genre"].unique())
        )
        if selected_genre != "Tümü":
            df_hist_filtered = df_hist[df_hist["Genre"] == selected_genre]
        else:
            df_hist_filtered = df_hist

        # KPI kartları
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Toplam Oyun", f"{len(df_hist_filtered):,}")
        col2.metric(
            "Toplam Hasılat", f"${df_hist_filtered['Global_Sales'].sum():,.0f}M"
        )
        col3.metric(
            "En Başarılı Yıl",
            int(df_hist_filtered.groupby("Year")["Global_Sales"].sum().idxmax()),
        )
        col4.metric("Lider Platform", df_hist_filtered["Platform"].mode()[0])

        st.markdown("---")

        # Grafikler
        c1, c2 = st.columns(2)

        with c1:
            st.subheader(" Yıllara Göre Satış Trendi")
            sales_by_year = (
                df_hist_filtered.groupby("Year")["Global_Sales"]
                .sum()
                .reset_index()
            )
            fig_year = px.area(
                sales_by_year,
                x="Year",
                y="Global_Sales",
                color_discrete_sequence=["#FF4B4B"],
            )
            st.plotly_chart(fig_year, use_container_width=True)

        with c2:
            st.subheader(" En Çok Satan 10 Yayıncı")
            top_publishers = (
                df_hist_filtered.groupby("Publisher")["Global_Sales"]
                .sum()
                .nlargest(10)
                .reset_index()
            )
            fig_pub = px.bar(
                top_publishers,
                x="Global_Sales",
                y="Publisher",
                orientation="h",
                color="Global_Sales",
            )
            st.plotly_chart(fig_pub, use_container_width=True)

        st.subheader(" Detaylı Veri İnceleme")
        st.dataframe(df_hist_filtered.head(100), use_container_width=True)

    else:
        st.error(
            "Hata: 'vgsales.csv' dosyası bulunamadı. Lütfen 'one_time_cleaner.py' kodunu çalıştırın."
        )

# ------------------------------------------------------------------------------
# TAB 2: CANLI AKIŞ VE TAHMİN
# ------------------------------------------------------------------------------
with tab2:
    st.header(" Canlı Veri Akışı ve Yapay Zeka Tahminleri")
    st.info("Bu panel, Spark Streaming ve Iceberg entegrasyonunu gösterir.")

    df_live = load_live_data()

    debug_count = spark.sql("SELECT count(*) AS cnt FROM local.db.live_predictions").toPandas()
    debug_max_ts = spark.sql("SELECT max(Transaction_Time) AS max_ts FROM local.db.live_predictions").toPandas()
    st.write("Toplam kayıt sayısı:", int(debug_count["cnt"][0]))
    st.write("Tablodaki en son Transaction_Time:", debug_max_ts["max_ts"][0])

    if df_live.empty:
        st.warning(
            " Veri bekleniyor... Lütfen Simülatörü (Terminal 1) ve Spark Motorunu (Terminal 2) çalıştırın."
        )
    else:
        # En güncel satır (zaman olarak en büyük)
        latest = df_live.iloc[-1]  # df_live zaman artan sırada, son satır en yeni

        diff = latest["AI_Prediction"] - latest["Real_Sales"]
        diff_color = "normal" if abs(diff) < 2 else "inverse"

        # KPI kartları
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Son İşlenen Oyun", latest["Name"])
        m2.metric("Gerçek Satış", f"${latest['Real_Sales']}M")
        m3.metric("AI Tahmini", f"${latest['AI_Prediction']:.2f}M")
        m4.metric("Model Sapması", f"{diff:.2f}M", delta_color=diff_color)

        st.markdown("---")

        # Canlı grafik
        st.subheader(" Canlı Model Performansı (Son 25 İşlem)")

        # X ekseni → zaman (Transaction_Time) varsa onu kullan
        if "Transaction_Time" in df_live.columns:
            x_axis = df_live["Transaction_Time"]
            x_title = "Zaman"
        else:
            x_axis = list(range(len(df_live)))
            x_title = "İşlem Sırası"

        fig_live = go.Figure()
        fig_live.add_trace(
            go.Scatter(
                x=x_axis,
                y=df_live["Real_Sales"],
                mode="lines+markers",
                name="Gerçek Satış",
            )
        )
        fig_live.add_trace(
            go.Scatter(
                x=x_axis,
                y=df_live["AI_Prediction"],
                mode="lines+markers",
                name="AI Tahmini",
                line=dict(dash="dot", color="orange"),
            )
        )

        fig_live.update_layout(
            xaxis_title=x_title,
            yaxis_title="Satış (Milyon)",
            height=400,
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        )

        st.plotly_chart(fig_live, use_container_width=True)

        # Canlı tablo
        st.subheader(" Iceberg Veritabanı (Anlık)")
        st.dataframe(df_live, use_container_width=True)

    #  2 saniye sonra tüm script'i baştan çalıştır → son 25 kayıtla grafik güncellenir
    time.sleep(2)
    st.rerun()
