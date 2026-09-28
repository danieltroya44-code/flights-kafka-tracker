import time
import json
import random
import requests
from kafka import KafkaProducer
import os
KAFKA_HOST = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_HOST],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

# 1. Configuración de Zona (Bounding Box: España / Portugal)
BOUNDING_BOX = {
    "lamin": 35.0,
    "lomin": -10.0,
    "lamax": 44.0,
    "lomax": 4.5
}

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

URL = "https://opensky-network.org/api/states/all"

# 2. Flota fantasma para cuando la API se bloquee (mantienen su posición y avanzan)
SIMULATED_FLEET = [
    {"icao24": "SIM001", "callsign": "IBE1020", "country": "Spain", "lat": 40.4168, "lon": -3.7038, "alt": 10500, "vel": 240, "hdg": 45},
    {"icao24": "SIM002", "callsign": "AEA4501", "country": "Spain", "lat": 41.3879, "lon": 2.1699, "alt": 8200, "vel": 210, "hdg": 180},
    {"icao24": "SIM003", "callsign": "RYR2234", "country": "Ireland", "lat": 37.3891, "lon": -5.9845, "alt": 11000, "vel": 260, "hdg": 90},
    {"icao24": "SIM004", "callsign": "VLG8890", "country": "Spain", "lat": 39.4699, "lon": -0.3763, "alt": 6000, "vel": 190, "hdg": 270},
    {"icao24": "SIM005", "callsign": "TAP6012", "country": "Portugal", "lat": 38.7223, "lon": -9.1393, "alt": 9500, "vel": 230, "hdg": 315},
]

def emit_simulated_data():
    """Genera movimiento artificial si la API real falla o da 429."""
    print("[MODO SIMULACIÓN] Generando telemetría sintética de respaldo...")
    for plane in SIMULATED_FLEET:
        # Hacemos que se muevan ligeramente en cada ciclo
        plane["lat"] += random.uniform(-0.04, 0.04)
        plane["lon"] += random.uniform(-0.04, 0.04)
        plane["alt"] += random.randint(-100, 100)
        plane["vel"] += random.randint(-5, 5)

        payload = {
            "icao24": plane["icao24"],
            "callsign": plane["callsign"],
            "origin_country": plane["country"] + " (Simulado)",
            "longitude": plane["lon"],
            "latitude": plane["lat"],
            "altitude_m": plane["alt"],
            "velocity_ms": plane["vel"],
            "heading": plane["hdg"],
            "vertical_rate": random.randint(-2, 2),
            "timestamp": int(time.time())
        }

        producer.send(
            topic='live-flights',
            key=plane["callsign"],
            value=payload
        )
    producer.flush()

print("Iniciando Pipeline (OpenSky API con Auto-Fallback a Simulación)...")

# 3. Bucle principal con conmutación automática
while True:
    try:
        response = requests.get(URL, params=BOUNDING_BOX, timeout=8)
        
        # Caso A: La API responde OK
        if response.status_code == 200:
            data = response.json()
            states = data.get("states", [])
            
            if states:
                print(f"[DATOS REALES] {len(states)} aeronaves recibidas de OpenSky...")
                for s in states:
                    flight_id = (s[1] or s[0] or "UNKNOWN").strip()
                    lat = s[6]
                    lon = s[5]
                    
                    if lat is None or lon is None:
                        continue

                    flight_payload = {
                        "icao24": s[0],
                        "callsign": flight_id,
                        "origin_country": s[2],
                        "longitude": lon,
                        "latitude": lat,
                        "altitude_m": s[7] or 0,
                        "velocity_ms": s[9] or 0,
                        "heading": s[10] or 0,
                        "vertical_rate": s[11] or 0,
                        "timestamp": data.get("time", int(time.time()))
                    }

                    producer.send(
                        topic='live-flights',
                        key=flight_id,
                        value=flight_payload
                    )
                producer.flush()
            else:
                print("Sin aeronaves reales en la zona. Activando simulación...")
                emit_simulated_data()

        # Caso B: Límite de peticiones alcanzado (HTTP 429)
        elif response.status_code == 429:
            print("[HTTP 429] Límite de API alcanzado. Saltando a datos simulados...")
            emit_simulated_data()

        # Cualquier otro error HTTP
        else:
            print(f"Error HTTP {response.status_code}. Saltando a simulación temporal...")
            emit_simulated_data()

    # Caso C: Error de conexión / Internet caído
    except Exception as e:
        print(f"Error de conexión con la API ({e}). Usando modo simulado...")
        emit_simulated_data()

    # Esperamos 10 segundos entre cada ciclo
    time.sleep(10)