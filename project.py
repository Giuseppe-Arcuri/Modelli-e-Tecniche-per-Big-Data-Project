import streamlit as st
import os

# ==========================================
# CONFIGURAZIONE HADOOP PER WINDOWS
# ==========================================
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['hadoop.home.dir'] = "C:\\hadoop"
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

# ==========================================
# FIX PER WINDOWS
# ==========================================
import socketserver
# PySpark su Windows cerca UnixStreamServer che non esiste.
# Lo sostituiamo con TCPServer per evitare l'errore.
if 'UnixStreamServer' not in dir(socketserver):
    socketserver.UnixStreamServer = socketserver.TCPServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, max, sum, when
from pyspark.sql.types import IntegerType, FloatType
import pandas as pd
import pydeck as pdk
import datetime

# =========================================================
# 1. CONFIGURAZIONE AMBIENTE
# =========================================================
# I dati sono distribuiti in più file dentro la cartella.
# Spark li leggerà come un unico "Data Lake".

DATASET_FOLDER = "dataset_voli"
COORDS_FILE = "coordinates.csv"
st.set_page_config(page_title="Big Data Analytics: Voli USA", layout="wide")

st.sidebar.image("logo_unical.png", width=200)
st.sidebar.info("Progetto di Big Data Analytics - Anno 2025/2026")
st.sidebar.image("img.png", width=200)


# =========================================================
# 2. INIZIALIZZAZIONE ENGINE SPARK
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


# =========================================================
# 3. INGESTION & DATA CLEANING
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


# =========================================================
# 4. LOGICA DI BUSINESS (Trasformazioni Spark)
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


# =========================================================
# 5. FRONTEND (Streamlit)
# =========================================================

st.title(" Analisi Big Data: Traffico Aereo USA")
st.markdown("""
Dashboard analitica basata su **Apache Spark**.
I dati vengono processati in parallelo simulando un cluster locale. Tutti i dati fanno riferimento ai soli voli USA partiti nell'anno 2013
""")

st.image("immagine_aereo.png", use_container_width=True)

# Caricamento Dati
with st.spinner("Avvio Spark Driver e Ingestione Dati..."):
    df_spark = load_data()
    coords_pd = load_coordinates()

if df_spark:
    # --- RIGHE KPI ---
    tot, canc, delay = get_kpi_metrics(df_spark)

    col1, col2, col3 = st.columns(3)
    col1.metric("Voli Totali (Dataset)", f"{tot:,}")
    col2.metric("Voli Cancellati", f"{canc:,}", delta_color="inverse")
    col3.metric("Ritardo Medio Arrivo", f"{delay:.2f} min", delta_color="inverse")

    st.divider()

    # --- SEZIONE RICERCA ---
    st.subheader("Ricerca Avanzata Voli")

    # Usiamo st.expander per nascondere la ricerca se non serve, tenendo pulita la dashboard
    with st.expander("Apri filtri di ricerca"):

        # Creiamo un Form: così Spark parte solo quando premi "Cerca", non mentre scrivi
        with st.form("search_form"):
            c1, c2, c3 = st.columns(3)

            with c1:
                # Input Data: Streamlit ti dà un calendario
                search_date = st.date_input("Data del Volo",
                value = datetime.date(2013, 1, 1),  # Parte dal 1 Gennaio 2013
                min_value = datetime.date(2013, 1, 1),  # Non posso andare prima
                max_value = datetime.date(2013, 12, 31))  # Non posso andare dopo
            with c2:
                # Input Origine (es. JFK)
                search_origin = st.text_input("Aeroporto Origine (es. JFK)").upper()
            with c3:
                # Input Destinazione (es. LAX)
                search_dest = st.text_input("Aeroporto Destinazione (es. LAX)").upper()

            c4, c5 = st.columns(2)
            with c4:
                # Compagnia Aerea (es. AA, DL)
                search_airline = st.text_input("Codice Compagnia (es. AA)").upper()
            with c5:
                # Numero Volo
                search_flight_num = st.text_input("Numero Volo (es. 1125)")

            # Bottone di invio del form
            submitted = st.form_submit_button("Cerca Voli")

    # --- LOGICA DI FILTRO SPARK ---
    if submitted:
        # 1. Partiamo dal DataFrame completo
        results = df_spark

        # 2. Applichiamo i filtri "a catena" SOLO se l'utente ha scritto qualcosa
        # Questa tecnica si chiama "Dynamic Query Building"

        if search_date:
            # Convertiamo la data in stringa 'YYYY-MM-DD' perché nel CSV è spesso stringa
            date_str = search_date.strftime("%Y-%m-%d")
            results = results.filter(col("FlightDate") == date_str)

        if search_origin:
            results = results.filter(col("Origin") == search_origin)

        if search_dest:
            results = results.filter(col("Dest") == search_dest)

        if search_airline:
            results = results.filter(col("Reporting_Airline") == search_airline)

        if search_flight_num:
            # Correzione: castiamo la colonna a stringa per evitare errori di tipo
            results = results.filter(col("Flight_Number_Reporting_Airline").cast("string") == search_flight_num)

        # 3. Eseguiamo la conta (Action)
        count_res = results.count()

        if count_res > 0:
            st.success(f"Trovati {count_res} voli corrispondenti ai criteri.")

            # Mostriamo i risultati convertendo in Pandas
            # Limitiamo a 100 righe per non intasare il browser se la ricerca è troppo generica
            df_view = results.select(
                "FlightDate", "Reporting_Airline", "Origin", "Dest",
                "DepDelay", "ArrDelay", "Cancelled"
            ).limit(100).toPandas()

            st.dataframe(df_view)
        else:
            st.error("Nessun volo trovato con questi criteri. Prova a rimuovere qualche filtro.")

    st.divider()

    # --- SEZIONE GRAFICI ---
    c_left, c_right = st.columns(2, gap= "large")

    with c_left:
        st.subheader("Tratte più Critiche")
        st.markdown("*Tratte con >100 voli ordinate per ritardo medio*")
        top_routes = get_top_delayed_routes(df_spark)
        st.dataframe(top_routes.style.format({"Ritardo_Medio": "{:.1f} min"}))


    with c_right:
        st.subheader("Cause dei Ritardi")
        st.markdown("*Incidenza media in minuti per ogni volo in ritardo*")
        causes = get_delay_sources(df_spark)
        causes.columns = ["Causa", "Minuti_Medi"]
        st.bar_chart(causes.set_index("Causa"))

    st.divider()

    # --- SEZIONE MAPPA GEOSPAZIALE ---
    st.subheader("Mappa di Calore: Voli in Partenza")

    if coords_pd is not None:
        # 1. Spark aggrega i dati: Conta voli per ogni aeroporto di origine
        # Questa operazione riduce milioni di righe a poche migliaia (Scaling down)
        traffic_data = df_spark.groupBy("Origin").count().toPandas()

        # 2. Pandas unisce i dati con le coordinate
        # JOIN locale (poiché il risultato aggregato è piccolo)
        try:
            map_data = pd.merge(traffic_data, coords_pd, left_on="Origin", right_on="IATA_CODE")

            # Normalizzazione per la visualizzazione (raggio del punto)
            map_data["radius_scaled"] = map_data["count"]

            st.pydeck_chart(pdk.Deck(
                map_style='https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
                initial_view_state=pdk.ViewState(latitude=38, longitude=-95, zoom=3),
                layers=[
                    pdk.Layer(
                        'ScatterplotLayer',
                        data=map_data,
                        get_position='[LONGITUDE, LATITUDE]',
                        get_color='[0, 120, 255, 180]',
                        get_radius='radius_scaled',
                        pickable=True,
                        opacity=0.6
                    ),
                ],
                tooltip={"html": "<b>{Origin}</b>: {count} voli partiti"}
            ))
        except Exception as e:
            st.warning(f"Errore generazione mappa: {e}. Verifica i nomi delle colonne nel file coordinates.csv")
    else:
        st.info("File coordinates.csv non trovato. Mappa disabilitata.")

    st.divider()

else:
    st.warning("Nessun dato caricato. Controlla la cartella 'dataset_voli'.")

#per avviare: python -m streamlit run project.py
