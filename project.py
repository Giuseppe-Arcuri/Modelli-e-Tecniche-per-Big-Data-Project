import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, max, sum, when
from pyspark.sql.types import IntegerType, FloatType
import pandas as pd
import pydeck as pdk
import os

# =========================================================
# 1. CONFIGURAZIONE AMBIENTE
# =========================================================
# I dati sono distribuiti in più file dentro la cartella.
# Spark li leggerà come un unico "Data Lake".

DATASET_FOLDER = "dataset_voli"
COORDS_FILE = "coordinates.csv"
st.set_page_config(page_title="Big Data Analytics: Voli USA", layout="wide")


# =========================================================
# 2. INIZIALIZZAZIONE ENGINE SPARK
# =========================================================
# @st.cache_resource è FONDAMENTALE. Spark impiega 5-10 secondi ad avviarsi. Senza questo decoratore, Streamlit
# riavvierebbe Spark ogni volta che viene cliccato un bottone, rendendo l'app inutilizzabile.

@st.cache_resource
def get_spark_session():
    """
    Crea il Driver Program (il coordinatore del cluster Spark).
    """

    return SparkSession.builder \
            .appName("Progetto_BigData_Voli") \
            .config("spark.driver.memory", "4g") \
            .master("local[*]") \
            .getOrCreate()

    # Assegniamo 4GB di RAM al driver per gestire i risultati delle aggregazioni
    # "local[*]" significa: usa tutti i core della CPU del tuo PC
    # per simulare i "Worker Nodes" e parallelizzare i calcoli.

