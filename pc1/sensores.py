# =============================================================================
# pc1/sensores.py — Simulación de sensores de tráfico (PC1)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Simula tres tipos de sensores por cada intersección de la cuadrícula NxM:
#   - Cámara       → EVENTO_LONGITUD_COLA (Lq): volumen de vehículos y velocidad.
#   - Espira       → EVENTO_CONTEO_VEHICULAR (Cv): vehículos que cruzan la espira.
#   - GPS          → EVENTO_DENSIDAD_TRAFICO (Dt): densidad y velocidad GPS.
#
# Cada intersección tiene su propio hilo de ejecución. Los eventos se envían
# al Broker en PC1 mediante el patrón PUSH/PULL (asíncrono y desacoplado).
#
# Uso:
#   python sensores.py [intervalo_seg]
#   intervalo_seg: segundos entre ciclos de generación (default=5)
#
# Ejemplos:
#   python sensores.py        # intervalo de 5 segundos (Escenario B)
#   python sensores.py 10     # intervalo de 10 segundos (Escenario A)
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


def ts_ahora() -> str:
    """Retorna el timestamp UTC actual en formato ISO-8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lista_intersecciones() -> list[str]:
    """
    Genera la lista de todas las intersecciones de la cuadrícula NxM.

    Returns:
        Lista de strings con formato "INT_<FILA><COLUMNA>" (ej. ["INT_A1", "INT_A2", ...]).
    """
    return [f"INT_{f}{c}" for f in FILAS for c in COLUMNAS]


# =============================================================================
# Generadores de eventos por tipo de sensor
# =============================================================================

def evento_camara(interseccion: str) -> dict:
    """
    Genera un EVENTO_LONGITUD_COLA (Lq) simulando una cámara de tráfico.

    Mide el número de vehículos en espera (volumen) y la velocidad promedio.
    Las filas centrales (B, C, D) simulan mayor densidad para representar
    zonas más congestionadas de la ciudad.

    Args:
        interseccion: Código de la intersección (ej. "INT_C5").

    Returns:
        Diccionario con los campos del evento según el formato del enunciado.
    """
    fila    = interseccion[4]              # Letra de fila, ej. 'C'
    es_pico = fila in ["B", "C", "D"]     # Zonas de mayor tráfico
    volumen = random.randint(3, 18) if es_pico else random.randint(0, 8)

    # Correlación inversa: a mayor volumen, menor velocidad promedio
    if volumen > 8:
        vel = random.uniform(5, 28)        # Congestión
    elif volumen > 4:
        vel = random.uniform(20, 40)       # Tráfico moderado
    else:
        vel = random.uniform(35, 50)       # Flujo libre

    return {
        "sensor_id":          f"CAM-{interseccion[4:]}",
        "tipo_sensor":        "camara",
        "interseccion":       interseccion,
        "volumen":            volumen,          # Vehículos en espera de cambio de semáforo
        "velocidad_promedio": round(vel, 2),    # Velocidad máxima: 50 km/h
        "timestamp":          ts_ahora()
    }


def evento_espira(interseccion: str) -> dict:
    """
    Genera un EVENTO_CONTEO_VEHICULAR (Cv) simulando una espira inductiva.

    Cuenta los vehículos que pasan sobre la espira en un intervalo fijo de 30s,
    coincidiendo con el ciclo de cambio de semáforo.

    Args:
        interseccion: Código de la intersección.

    Returns:
        Diccionario con los campos del evento de espira inductiva.
    """
    ts_inicio = ts_ahora()
    return {
        "sensor_id":          f"ESP-{interseccion[4:]}",
        "tipo_sensor":        "espira_inductiva",
        "interseccion":       interseccion,
        "vehiculos_contados": random.randint(2, 20),   # Vehículos que cruzaron la espira
        "intervalo_segundos": INTERVALO_ESPIRA_SEG,    # Ventana de 30 segundos
        "timestamp_inicio":   ts_inicio,
        "timestamp_fin":      ts_ahora(),
        "timestamp":          ts_inicio
    }


def evento_gps(interseccion: str) -> dict:
    """
    Genera un EVENTO_DENSIDAD_TRAFICO (Dt) simulando un sensor GPS vehicular.

    Reporta la densidad vehicular y la velocidad promedio según datos GPS.
    El nivel de congestión se clasifica según la velocidad:
        - ALTA:   velocidad < 10 km/h
        - NORMAL: velocidad entre 10 y 39 km/h
        - BAJA:   velocidad > 39 km/h (flujo libre)

    Args:
        interseccion: Código de la intersección.

    Returns:
        Diccionario con los campos del evento GPS.
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


# =============================================================================
# Hilo de sensor por intersección
# =============================================================================

class HiloSensor(threading.Thread):
    """
    Hilo daemon que simula todos los sensores de una intersección.

    En cada ciclo genera y envía los tres tipos de evento (cámara, espira, GPS)
    al Broker mediante el patrón PUSH/PULL. El tópico del mensaje coincide con
    el tipo de sensor, lo que permite al Broker y a la Analítica filtrar por tópico.

    Formato de mensaje: "<tipo_sensor> <json_payload>"
    Ejemplo: "camara {"sensor_id": "CAM-C5", ...}"

    Attributes:
        interseccion (str): Código de la intersección asignada a este hilo.
        socket (zmq.Socket): Socket PUSH compartido para enviar eventos.
        intervalo (float): Segundos entre ciclos de generación.
    """

    def __init__(self, interseccion: str, socket_push: zmq.Socket, intervalo: float):
        """
        Args:
            interseccion: Código de la intersección (ej. "INT_C5").
            socket_push:  Socket PUSH del proceso principal (compartido).
            intervalo:    Segundos entre generaciones de eventos.
        """
        super().__init__(daemon=True, name=f"Sensor-{interseccion}")
        self.interseccion = interseccion
        self.socket       = socket_push
        self.intervalo    = intervalo

    def run(self):
        """
        Bucle principal del sensor: genera los tres eventos y los envía al Broker.
        Aplica un desfase aleatorio entre tipos de sensor para distribuir la carga.
        """
        print(f"[SENSOR] Iniciado: {self.interseccion} | intervalo={self.intervalo}s")
        while True:
            try:
                for generador in [evento_camara, evento_espira, evento_gps]:
                    evento = generador(self.interseccion)
                    topico = evento["tipo_sensor"]
                    # El formato "topico json" permite al SUB filtrar por prefijo
                    self.socket.send_string(f"{topico} {json.dumps(evento)}")
                    print(f"  [SENSOR][{self.interseccion}] {topico:20s} "
                          f"vol={evento.get('volumen', '—'):>3} "
                          f"vel={evento.get('velocidad_promedio', '—'):>5} km/h")
                    time.sleep(random.uniform(0.05, 0.15))   # Pequeño desfase entre tipos

                time.sleep(self.intervalo)

            except Exception as exc:
                print(f"[SENSOR][{self.interseccion}] ERROR: {exc}")
                time.sleep(2)


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada del proceso de sensores (PC1).

    Crea un socket PUSH compartido, arranca un HiloSensor por cada intersección
    de la cuadrícula y bloquea hasta recibir Ctrl+C.

    Argumentos de línea de comandos:
        intervalo_seg (float, opcional): Segundos entre ciclos. Default = INTERVALO_SENSOR_SEG.
    """
    intervalo = float(sys.argv[1]) if len(sys.argv) > 1 else INTERVALO_SENSOR_SEG

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    addr   = f"tcp://{PC1_IP}:{PUERTO_SENSORES_A_BROKER}"
    socket.connect(addr)

    intersecciones = lista_intersecciones()
    print(f"[PC1-SENSORES] Conectando al broker en {addr}")
    print(f"[PC1-SENSORES] Ciudad: {len(FILAS)}x{len(COLUMNAS)} = "
          f"{len(intersecciones)} intersecciones | intervalo={intervalo}s")
    time.sleep(1)   # Dar tiempo al broker para estar listo antes del primer envío

    # Arrancar un hilo por intersección con pequeño desfase para evitar saturación inicial
    hilos = []
    for inter in intersecciones:
        time.sleep(random.uniform(0.05, 0.15))
        h = HiloSensor(inter, socket, intervalo)
        h.start()
        hilos.append(h)

    print(f"[PC1-SENSORES] {len(hilos)} sensores activos. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[PC1-SENSORES] Deteniendo sensores...")
    finally:
        socket.close()
        ctx.term()
        print("[PC1-SENSORES] Apagado completo.")


if __name__ == "__main__":
    main()
