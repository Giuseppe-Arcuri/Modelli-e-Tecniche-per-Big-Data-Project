from pyspark.sql.functions import col, avg, count, desc
import streamlit as st
import pandas as pd
import pydeck as pdk
import datetime

#=========================================================
# 4. Query (Trasformazioni Spark)
# =========================================================
def get_kpi_metrics(df):
    """Calcola i KPI globali per la dashboard."""
    #I KPI (acronimo di Key Performance Indicators, in italiano Indicatori Chiave di Prestazione) sono dei numeri specifici che ti
    # dicono, a colpo d'occhio, se un'attività sta andando bene o male.
    # Action 1: Conta totale righe
    total_flights = df.count()

    # Action 2: Filtra e conta i cancellati
    cancelled_flights = df.filter(col("Cancelled") == 1).count()

    # Action 3: Calcola la media del ritardo (ignora i null)
    # Usiamo 'ArrDelay' come da readme.html
    avg_delay = df.agg(avg("ArrDelay")).collect()[0][0]

    return total_flights, cancelled_flights, avg_delay


def get_top_delayed_routes(df):
    """Trova le tratte (Origine -> Destinazione) con più ritardo."""
    # GroupBy: Crea uno Shuffle (ridistribuzione dati nel cluster simulato)
    return df.groupBy("Origin", "Dest") \
        .agg(avg("ArrDelay").alias("Ritardo_Medio"), count("*").alias("Num_Voli")) \
        .filter(col("Num_Voli") > 100) \
        .orderBy(desc("Ritardo_Medio")) \
        .limit(10) \
        .toPandas()


def get_delay_sources(df):
    """Analizza le cause di ritardo (basato sulle colonne BTS)."""
    # Calcoliamo la media dei minuti per categoria
    res = df.select(
        avg("CarrierDelay").alias("Compagnia"),
        avg("WeatherDelay").alias("Meteo"),
        avg("NASDelay").alias("Sistema Naz."),
        avg("SecurityDelay").alias("Sicurezza"),
        avg("LateAircraftDelay").alias("Aereo in Ritardo")
    ).toPandas()
    return res.transpose().reset_index()



# --- SEZIONE MAPPA GEOSPAZIALE ---
def prepare_map_data(df_spark, coords_pd):
    # 1. Spark aggrega i dati: Conta voli per ogni aeroporto di origine
    # Questa operazione riduce milioni di righe a poche migliaia (Scaling down)
    traffic_data = df_spark.groupBy("Origin").count().toPandas()

    # 2. Pandas unisce i dati con le coordinate
    # JOIN locale (poiché il risultato aggregato è piccolo)
    merged_data = pd.merge(traffic_data, coords_pd, left_on="Origin", right_on="IATA_CODE")

    return merged_data