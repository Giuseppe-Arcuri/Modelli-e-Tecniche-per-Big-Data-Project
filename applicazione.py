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

import pydeck as pdk
import datetime
from pyspark.sql.functions import col

from src.etl import load_data, load_coordinates
from src.analytics import get_kpi_metrics, get_top_delayed_routes, get_delay_sources, prepare_map_data

# =========================================================
# FRONTEND (Streamlit)
# =========================================================
st.set_page_config(page_title="Big Data Analytics: Voli USA", layout="wide")

st.sidebar.image("logo_unical.png", width=200)
st.sidebar.info("Progetto di Big Data Analytics - Anno 2025/2026")
st.sidebar.image("img.png", width=200)

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
        try:
            map_data = prepare_map_data(df_spark, coords_pd)

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

#per avviare: python -m streamlit run applicazione.py