import streamlit as st
from pyspark.sql import SparkSession

# =========================================================
# INIZIALIZZAZIONE ENGINE SPARK
# =========================================================
# @st.cache_resource è FONDAMENTALE. Spark impiega 5-10 secondi ad avviarsi. Senza questo decoratore, Streamlit
# riavvierebbe Spark ogni volta che viene cliccato un bottone, rendendo l'app inutilizzabile.

@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
            .appName("Progetto_BigData_Voli") \
            .config("spark.driver.memory", "4g") \
            .config("spark.driver.host", "localhost") \
            .master("local[*]") \
            .getOrCreate()

    # Assegniamo 4GB di RAM al driver per gestire i risultati delle aggregazioni
    # "local[*]" significa: usa tutti i core della CPU del tuo PC
    # per simulare i "Worker Nodes" e parallelizzare i calcoli.