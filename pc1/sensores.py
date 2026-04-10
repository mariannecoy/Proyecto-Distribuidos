# =============================================================================
# pc1/sensores.py — Simulación de sensores de tráfico (PC1)
#
# Genera eventos de tres tipos y los envía al Broker (PUSH/PULL):
#   - Cámara       → EVENTO_LONGITUD_COLA      (Lq): volumen y velocidad
#   - Espira       → EVENTO_CONTEO_VEHICULAR   (Cv): vehículos que cruzan
#   - GPS          → EVENTO_DENSIDAD_TRAFICO   (Dt): densidad y velocidad GPS
#
# Uso: python sensores.py [intervalo_seg]
#      intervalo_seg: segundos entre generaciones (default=5)
#
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import json
import time
import random
import threading
from datetime import datetime, timezone
from config import (
    PC1_IP, PUERTO_SENSORES_A_BROKER,
    FILAS, COLUMNAS,
    INTERVALO_SENSOR_SEG, INTERVALO_ESPIRA_SEG
)


def ts_ahora():
    """Timestamp UTC en formato ISO."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lista_intersecciones():
    """Genera todas las intersecciones de la cuadrícula NxM."""
    return [f"INT_{f}{c}" for f in FILAS for c in COLUMNAS]


# ── Generadores de eventos por tipo de sensor ─────────────────────────────────

def evento_camara(interseccion: str) -> dict:
    """
    EVENTO_LONGITUD_COLA (Lq)
    Mide el número de vehículos en espera y la velocidad promedio.
    Las filas B, C, D simulan mayor densidad (horas pico).
    """
    fila   = interseccion[4]          # letra de la fila, ej. 'C'
    es_pico = fila in ["B", "C", "D"]
    volumen = random.randint(3, 18) if es_pico else random.randint(0, 8)
    # A mayor volumen, menor velocidad
    vel = random.uniform(5, 28) if volumen > 8 else random.uniform(28, 50)
    return {
        "sensor_id":          f"CAM-{interseccion[4:]}",
        "tipo_sensor":        "camara",
        "interseccion":       interseccion,
        "volumen":            volumen,
        "velocidad_promedio": round(vel, 2),
        "timestamp":          ts_ahora()
    }


def evento_espira(interseccion: str) -> dict:
    """
    EVENTO_CONTEO_VEHICULAR (Cv)
    Cuenta los vehículos que pasan sobre la espira inductiva en un intervalo.
    """
    ts_inicio = ts_ahora()
    return {
        "sensor_id":          f"ESP-{interseccion[4:]}",
        "tipo_sensor":        "espira_inductiva",
        "interseccion":       interseccion,
        "vehiculos_contados": random.randint(2, 20),
        "intervalo_segundos": INTERVALO_ESPIRA_SEG,
        "timestamp_inicio":   ts_inicio,
        "timestamp_fin":      ts_ahora(),
        "timestamp":          ts_inicio
    }


def evento_gps(interseccion: str) -> dict:
    """
    EVENTO_DENSIDAD_TRAFICO (Dt)
    Reporta densidad vehicular y velocidad promedio desde GPS.
    Nivel de congestión: ALTA (<10 km/h), NORMAL (10-39), BAJA (>39)
    """
    velocidad = random.uniform(5, 60)
    densidad  = random.uniform(5, 50)
    if velocidad < 10:
        nivel = "ALTA"
    elif velocidad <= 39:
        nivel = "NORMAL"
    else:
        nivel = "BAJA"
    return {
        "sensor_id":          f"GPS-{interseccion[4:]}",
        "tipo_sensor":        "gps",
        "interseccion":       interseccion,
        "nivel_congestion":   nivel,
        "velocidad_promedio": round(velocidad, 2),
        "densidad":           round(densidad, 2),
        "timestamp":          ts_ahora()
    }


# ── Hilo de sensor por intersección ───────────────────────────────────────────

class HiloSensor(threading.Thread):
    """
    Un hilo por intersección.
    Genera y envía los tres tipos de evento en cada ciclo.
    El tópico del mensaje = tipo_sensor, para que el broker pueda filtrar.
    """

    def __init__(self, interseccion: str, socket_push: zmq.Socket, intervalo: float):
        super().__init__(daemon=True, name=f"Sensor-{interseccion}")
        self.interseccion = interseccion
        self.socket       = socket_push
        self.intervalo    = intervalo

    def run(self):
        print(f"[SENSOR] Iniciado: {self.interseccion} | intervalo={self.intervalo}s")
        while True:
            try:
                for generador in [evento_camara, evento_espira, evento_gps]:
                    evento = generador(self.interseccion)
                    topico = evento["tipo_sensor"]
                    # Formato del mensaje: "<topico> <json>"
                    self.socket.send_string(f"{topico} {json.dumps(evento)}")
                    print(f"  [SENSOR][{self.interseccion}] {topico} → "
                          f"vol={evento.get('volumen','—')} "
                          f"vel={evento.get('velocidad_promedio','—')} km/h")
                time.sleep(self.intervalo)
            except Exception as e:
                print(f"[SENSOR][{self.interseccion}] ERROR: {e}")
                time.sleep(2)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # El intervalo se puede pasar como argumento de línea de comandos
    intervalo = float(sys.argv[1]) if len(sys.argv) > 1 else INTERVALO_SENSOR_SEG

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    addr   = f"tcp://{PC1_IP}:{PUERTO_SENSORES_A_BROKER}"
    socket.connect(addr)

    print(f"[PC1-SENSORES] Conectando al broker en {addr}")
    print(f"[PC1-SENSORES] Ciudad: {len(FILAS)}x{len(COLUMNAS)} = "
          f"{len(FILAS)*len(COLUMNAS)} intersecciones | intervalo={intervalo}s")
    time.sleep(1)  # dar tiempo al broker para estar listo

    # Arrancar un hilo por intersección con desfase aleatorio
    hilos = []
    for inter in lista_intersecciones():
        time.sleep(random.uniform(0.05, 0.15))  # desfase para no saturar
        h = HiloSensor(inter, socket, intervalo)
        h.start()
        hilos.append(h)

    print(f"[PC1-SENSORES] {len(hilos)} sensores activos. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[PC1-SENSORES] Deteniendo...")
    finally:
        socket.close()
        ctx.term()


if __name__ == "__main__":
    main()
