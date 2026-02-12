from src.config import get_spark_session
from pyspark.sql.functions import col, avg, count, desc, max, sum, when
from pyspark.sql.types import IntegerType, FloatType
import pandas as pd
import streamlit as st
import os

# =========================================================
# CONFIGURAZIONE AMBIENTE
# =========================================================
# I dati sono distribuiti in più file dentro la cartella.
# Spark li leggerà come un unico "Data Lake".

DATASET_FOLDER = "dataset_voli"
COORDS_FILE = "coordinates.csv"


# =========================================================
# INGESTION & DATA CLEANING
# =========================================================
@st.cache_resource
def load_data():
    spark = get_spark_session()

    if not os.path.exists(DATASET_FOLDER):
        st.error(f"ERRORE: La cartella '{DATASET_FOLDER}' non esiste. Crea la cartella e mettici dentro i CSV.")
        return None

    # --- LETTURA (Ingestion) ---
    # Leggiamo l'intera directory. Spark gestisce il partizionamento automaticamente.
    # inferSchema=True: Spark scansiona i file per capire se "10" è un numero o una stringa.
    df = spark.read.option("header", "true").csv(DATASET_FOLDER, inferSchema=True)

    # --- CASTING (Data Cleaning) ---
    # Forziamo i tipi corretti basandoci sul dataset.
    # Se questi campi sono stringhe, i calcoli matematici falliscono.
    cols_to_int = [
        "ArrDelay",  # Ritardo all'arrivo
        "DepDelay",  # Ritardo alla partenza
        "Distance",  # Distanza in miglia
        "Cancelled",  # 1 se cancellato
        "CarrierDelay",  # Ritardo imputabile alla compagnia
        "WeatherDelay",  # Ritardo meteo
        "NASDelay",  # Ritardo sistema aereo nazionale
        "SecurityDelay",  # Ritardo sicurezza
        "LateAircraftDelay"  # Ritardo accumulato dal volo precedente
    ]

    for c in cols_to_int:
        # Controlliamo se la colonna esiste per evitare errori se mancano dati
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    # --- OTTIMIZZAZIONE (Caching) ---
    # Salviamo il DataFrame nella RAM.
    # Senza questo, ogni grafico rileggerebbe i CSV dal disco (Lento).
    df = df.cache()
    return df

@st.cache_data
def load_coordinates():
    # File di piccole dimensioni (Lookup Table): usiamo Pandas.
    if os.path.exists(COORDS_FILE):
        return pd.read_csv(COORDS_FILE)
    return None
