# =============================================================================
# metricas.py — Recolección de métricas de desempeño (Tabla 1 del enunciado)
#
# Mide para cada escenario de prueba:
#   Métrica 1: Cantidad de eventos almacenados en BD en un intervalo de 2 minutos
#   Métrica 2: Tiempo desde que el usuario solicita una acción hasta que
#              el semáforo cambia de estado en la BD
#
# Escenario A: 1 sensor de cada tipo, datos cada 10s  (diseño simple)
# Escenario B: 2 sensores de cada tipo, datos cada 5s (diseño multihilo)
#
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'shared'))

import zmq
import json
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from config import PC3_IP, PUERTO_MONITOREO_REP, DB_REPLICA_PATH, DB_PRINCIPAL_PATH


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def contar_eventos_bd(ruta_db: str, segundos: int = 120) -> int:
    """Cuenta eventos guardados en los últimos N segundos."""
    limite = (datetime.now(timezone.utc) - timedelta(seconds=segundos)).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn   = sqlite3.connect(ruta_db)
    cur    = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM eventos_sensores WHERE recibido_en >= ?", (limite,))
    count  = cur.fetchone()[0]
    conn.close()
    return count


def medir_tiempo_cambio_semaforo(interseccion: str, ruta_db: str, timeout_seg=10) -> float:
    """
    Envía comando PRIORIDAD_EMERGENCIA a PC3 y mide cuánto tiempo tarda
    en reflejarse el cambio a VERDE en la BD.
    Retorna el tiempo en segundos, o -1 si no cambió dentro del timeout.
    """
    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.connect(f"tcp://{PC3_IP}:{PUERTO_MONITOREO_REP}")

    t_inicio = time.time()
    socket.send_string(json.dumps({
        "accion": "PRIORIDAD_EMERGENCIA",
        "interseccion": interseccion,
        "tipo_vehiculo": "ambulancia"
    }))
    try:
        socket.recv_string()  # esperar ACK de PC3
    except zmq.Again:
        pass
    socket.close()
    ctx.term()

    # Polling en BD hasta que el semáforo cambie a VERDE
    deadline = time.time() + timeout_seg
    while time.time() < deadline:
        conn = sqlite3.connect(ruta_db)
        cur  = conn.cursor()
        cur.execute("SELECT estado FROM semaforos WHERE interseccion=?", (interseccion,))
        fila = cur.fetchone()
        conn.close()
        if fila and fila[0] == "VERDE":
            return time.time() - t_inicio
        time.sleep(0.1)
    return -1


def ejecutar_escenario(nombre: str, descripcion: str, ruta_db: str):
    """Ejecuta las mediciones para un escenario y retorna los resultados."""
    print(f"\n{'='*62}")
    print(f"  ESCENARIO: {nombre}")
    print(f"  {descripcion}")
    print(f"  BD: {ruta_db}")
    print(f"{'='*62}")

    # ── Métrica 1: eventos en 2 minutos ───────────────────────────────────────
    print(f"\n[{ts()}] Esperando 120s para medir throughput de BD...")
    time.sleep(120)
    eventos = contar_eventos_bd(ruta_db, segundos=120)
    tasa    = round(eventos / 120, 2)
    print(f"[{ts()}] ✅ Métrica 1 — Eventos en 2 min: {eventos} | Tasa: {tasa} evt/s")

    # ── Métrica 2: tiempo de respuesta del semáforo ───────────────────────────
    interseccion = "INT_C3"
    print(f"\n[{ts()}] Midiendo tiempo de respuesta para {interseccion} (5 repeticiones)...")
    tiempos = []
    for i in range(5):
        t = medir_tiempo_cambio_semaforo(interseccion, ruta_db)
        tiempos.append(t)
        print(f"  Intento {i+1}: {f'{t:.3f}s' if t >= 0 else 'TIMEOUT'}")
        time.sleep(3)

    validos = [t for t in tiempos if t >= 0]
    promedio = round(sum(validos) / len(validos), 3) if validos else None
    minimo   = round(min(validos), 3) if validos else None
    maximo   = round(max(validos), 3) if validos else None

    if validos:
        print(f"[{ts()}] ✅ Métrica 2 — Tiempo respuesta: "
              f"avg={promedio}s | min={minimo}s | max={maximo}s")
    else:
        print(f"[{ts()}] ❌ Métrica 2 — Todos los intentos fallaron (timeout)")

    return {
        "escenario":          nombre,
        "descripcion":        descripcion,
        "eventos_2min":       eventos,
        "tasa_eventos_seg":   tasa,
        "tiempo_resp_avg_s":  promedio,
        "tiempo_resp_min_s":  minimo,
        "tiempo_resp_max_s":  maximo,
    }


def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║   MÉTRICAS DE RENDIMIENTO — Tabla 1 del Proyecto ║")
    print("╚══════════════════════════════════════════════════╝")
    print("\nAsegúrese de que todos los servicios estén corriendo.")
    print("\nEscenarios disponibles:")
    print("  A — 1 sensor/tipo, intervalo=10s  (broker simple)")
    print("  B — 2 sensores/tipo, intervalo=5s (broker multihilo)")

    esc  = input("\nEscenario a medir (A / B / ambos): ").strip().upper()
    bd_s = input("¿Qué BD usar? (principal/replica): ").strip().lower()
    ruta = DB_PRINCIPAL_PATH if "principal" in bd_s else DB_REPLICA_PATH

    resultados = []

    if esc in ("A", "AMBOS"):
        resultados.append(ejecutar_escenario(
            "A - Diseño Simple",
            "1 sensor de cada tipo, datos cada 10s, broker simple",
            ruta
        ))

    if esc in ("B", "AMBOS"):
        resultados.append(ejecutar_escenario(
            "B - Diseño Multihilo",
            "2 sensores de cada tipo, datos cada 5s, broker multihilo",
            ruta
        ))

    print("\n\n══════════════ RESUMEN FINAL ══════════════")
    for r in resultados:
        print(f"\n  {r['escenario']}: {r['descripcion']}")
        print(f"    Eventos en 2 min:  {r['eventos_2min']}")
        print(f"    Tasa:              {r['tasa_eventos_seg']} evt/s")
        print(f"    Tiempo respuesta:  {r['tiempo_resp_avg_s']}s (promedio)")

    # Guardar en JSON para graficar
    import json as _json
    with open("resultados_metricas.json", "w", encoding="utf-8") as f:
        _json.dump(resultados, f, indent=2, ensure_ascii=False)
    print("\n  Guardado en resultados_metricas.json")


if __name__ == "__main__":
    main()
