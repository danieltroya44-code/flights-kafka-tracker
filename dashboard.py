import streamlit as st
import pandas as pd
import pydeck as pdk
import json
import time
from kafka import KafkaConsumer
import os

st.set_page_config(page_title="Radar Kafka Live", layout="wide")
st.title("Radar de Tráfico Aéreo en Tiempo Real (Kafka Stream)")

# Guardamos el estado de los aviones en memoria de la sesión
if "aircraft_db" not in st.session_state:
    st.session_state.aircraft_db = {}

# Inicializar Consumer (con un timeout para no bloquear el render)
@st.cache_resource
def get_consumer():
  kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  return KafkaConsumer(
      "live-flights",
      bootstrap_servers=[kafka_host],
      auto_offset_reset="latest",
      enable_auto_commit=True,
      consumer_timeout_ms=1000,
      value_deserializer=lambda m: json.loads(m.decode("utf-8")),
  )
consumer = get_consumer()

# Consumir nuevos mensajes que hayan llegado al topic
for message in consumer:
    flight = message.value
    st.session_state.aircraft_db[flight["icao24"]] = flight

# Convertir datos a DataFrame
df = pd.DataFrame(st.session_state.aircraft_db.values())

if not df.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Aeronaves activas", len(df))
    col2.metric("Altitud Máxima (m)", int(df["altitude_m"].max() or 0))
    col3.metric("Velocidad Máxima (m/s)", int(df["velocity_ms"].max() or 0))

    # Configuración de la capa en el mapa (puntos 3D según altitud)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_color="[255, 140, 0, 200]",
        get_radius=15000,
        pickable=True,
        auto_highlight=True
    )

    # Vista centrada en el promedio de coordenadas
    view_state = pdk.ViewState(
        latitude=float(df["latitude"].mean()),
        longitude=float(df["longitude"].mean()),
        zoom=5,
        pitch=30,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "Vuelo: {callsign}\nPaís: {origin_country}\nAlt: {altitude_m} m\nVel: {velocity_ms} m/s"}
    ))

    st.subheader("Tabla de Vuelos")
    st.dataframe(df[["callsign", "origin_country", "latitude", "longitude", "altitude_m", "velocity_ms"]])
else:
    st.info("Esperando telemetría desde Kafka...")

# Auto-refrescar la pantalla cada 3 segundos
time.sleep(3)
st.rerun()