# =============================================================================
# metricas.py — Recolección de métricas de desempeño (Tabla 1 del enunciado)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
#
# Mide las dos variables dependientes de la Tabla 1 para los cuatro casos
# de la matriz: (carga A o B) x (broker simple o multihilo).
#
#   Métrica 1 (M1): Cantidad de eventos almacenados en BD en 2 minutos.
#                   Indica el throughput del sistema completo.
#
#   Métrica 2 (M2): Tiempo desde que el usuario solicita una acción hasta que
#                   el semáforo cambia de estado en la BD (latencia extremo a extremo).
#
# Cargas (Tabla 1):
#   A: 1 sensor de cada tipo,  datos cada 10s
#   B: 2 sensores de cada tipo, datos cada 5s
#
# Brokers:
#   simple    — un único hilo en PC1
#   multihilo — pool de workers en PC1
#
# Uso:
#   python metricas.py
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

# ── Parche de rutas de BD ────────────────────────────────────────────────────
# Las BD se crean en pc3/ y pc2/ porque ahí se ejecutan monitoreo.py y
# analitica.py respectivamente. Resolvemos las rutas absolutas para poder
# ejecutar metricas.py desde cualquier directorio.
_RAIZ = os.path.dirname(os.path.abspath(__file__))
DB_PRINCIPAL_PATH = os.path.join(_RAIZ, "pc3", DB_PRINCIPAL_PATH)
DB_REPLICA_PATH   = os.path.join(_RAIZ, "pc2", DB_REPLICA_PATH)


def _ts() -> str:
    """Retorna la hora UTC actual formateada (HH:MM:SS)."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Métrica 1: Throughput — eventos almacenados en 2 minutos
# =============================================================================

def contar_eventos_bd(ruta_db: str, segundos: int = 120) -> int:
    """Cuenta los eventos de sensor almacenados en los últimos N segundos."""
    limite = (datetime.now(timezone.utc) - timedelta(seconds=segundos)
              ).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        conn  = sqlite3.connect(ruta_db)
        cur   = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM eventos_sensores WHERE recibido_en >= ?",
            (limite,)
        )
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as exc:
        print(f"[METRICAS] Error contando eventos en {ruta_db}: {exc}")
        return 0


# =============================================================================
# Métrica 2: Latencia — tiempo de respuesta del semáforo
# =============================================================================

def medir_latencia_semaforo(interseccion: str, ruta_db: str,
                             timeout_seg: float = 15.0) -> float:
    """Mide latencia extremo a extremo: PRIORIDAD_EMERGENCIA → cambio a VERDE."""
    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(f"tcp://{PC3_IP}:{PUERTO_MONITOREO_REP}")

    t0 = time.time()
    try:
        socket.send_string(json.dumps({
            "accion":         "PRIORIDAD_EMERGENCIA",
            "interseccion":   interseccion,
            "tipo_vehiculo":  "ambulancia"
        }))
        socket.recv_string()
    except zmq.Again:
        print(f"[METRICAS] Timeout esperando ACK de PC3 para {interseccion}")
    except Exception as exc:
        print(f"[METRICAS] Error enviando solicitud: {exc}")
    finally:
        socket.close()
        ctx.term()

    deadline = time.time() + timeout_seg
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(ruta_db)
            cur  = conn.cursor()
            cur.execute(
                "SELECT estado FROM semaforos WHERE interseccion=?",
                (interseccion,)
            )
            fila = cur.fetchone()
            conn.close()
            if fila and fila[0] == "VERDE":
                return time.time() - t0
        except Exception as exc:
            print(f"[METRICAS] Error consultando BD: {exc}")
        time.sleep(0.05)

    return -1.0


# =============================================================================
# Ejecución de escenario completo
# =============================================================================

def ejecutar_escenario(nombre: str, descripcion: str, ruta_db: str,
                       n_repeticiones: int = 5) -> dict:
    """Ejecuta todas las mediciones para un escenario y retorna los resultados."""
    print(f"\n{'='*65}")
    print(f"  ESCENARIO: {nombre}")
    print(f"  {descripcion}")
    print(f"  BD:        {ruta_db}")
    print(f"{'='*65}")

    # ── M1: Throughput (eventos en 2 minutos) ──────────────────────────────
    print(f"\n[{_ts()}] Esperando 120s para medir throughput...")
    print(f"[{_ts()}] (Asegúrese de que los sensores estén generando datos)")
    time.sleep(120)

    m1_eventos = contar_eventos_bd(ruta_db, segundos=120)
    m1_tasa    = round(m1_eventos / 120, 3)
    print(f"[{_ts()}] ✅ M1 — Eventos en 2 min: {m1_eventos} | Tasa: {m1_tasa} evt/s")

    # ── M2: Latencia de semáforo (promedio de N repeticiones) ──────────────
    interseccion = "INT_C3"
    print(f"\n[{_ts()}] Midiendo latencia para {interseccion} ({n_repeticiones} repeticiones)...")
    tiempos = []
    for i in range(n_repeticiones):
        t = medir_latencia_semaforo(interseccion, ruta_db)
        estado = f"{t:.3f}s" if t >= 0 else "TIMEOUT ❌"
        tiempos.append(t)
        print(f"  Repetición {i+1}/{n_repeticiones}: {estado}")
        time.sleep(3)

    validos  = [t for t in tiempos if t >= 0]
    m2_avg   = round(sum(validos) / len(validos), 4) if validos else None
    m2_min   = round(min(validos), 4) if validos else None
    m2_max   = round(max(validos), 4) if validos else None
    m2_exito = len(validos)

    if validos:
        print(f"[{_ts()}] ✅ M2 — Latencia: "
              f"avg={m2_avg}s | min={m2_min}s | max={m2_max}s "
              f"({m2_exito}/{n_repeticiones} exitosas)")
    else:
        print(f"[{_ts()}] ❌ M2 — Todos los intentos fallaron (timeout)")

    return {
        "escenario":         nombre,
        "descripcion":       descripcion,
        "bd_usada":          ruta_db,
        "m1_eventos_2min":   m1_eventos,
        "m1_tasa_evt_s":     m1_tasa,
        "m2_latencia_avg_s": m2_avg,
        "m2_latencia_min_s": m2_min,
        "m2_latencia_max_s": m2_max,
        "m2_exitosas":       m2_exito,
        "m2_total":          n_repeticiones
    }


# =============================================================================
# Main — ahora pregunta CARGA y BROKER por separado
# =============================================================================

def main():
    """
    Punto de entrada del módulo de métricas.

    Flujo de ejecución:
        1. Pregunta al usuario la carga (A o B) y el tipo de broker
           (simple o multihilo) que está corriendo actualmente.
        2. Verifica que la base de datos exista y tenga las tablas
           necesarias (eventos_sensores y semaforos). Si no, aborta
           con un mensaje explicativo.
        3. Muestra un cuadro de confirmación con los parámetros
           seleccionados, recordando al usuario verificar el banner
           del broker en la terminal correspondiente.
        4. Ejecuta el escenario seleccionado (mide M1 y M2).
        5. Guarda los resultados en un JSON con nombre descriptivo
           (resultados_<broker>_<intervalo>.json).

    No recibe argumentos ni retorna valores; toda la interacción se
    realiza por consola (input/print) y el resultado persiste en disco.
    """
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  MÉTRICAS DE RENDIMIENTO — Tabla 1 del Proyecto ISD      ║")
    print("║  Pontificia Universidad Javeriana                        ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print("\nMatriz de pruebas (4 combinaciones):")
    print("  Carga A:  1 sensor/tipo, intervalo=10s")
    print("  Carga B:  2 sensores/tipo, intervalo=5s")
    print("  Broker:   simple | multihilo")

    # Carga (variable independiente del experimento)
    carga = ""
    while carga not in ("A", "B"):
        carga = input("\nCarga aplicada por los sensores (A / B): ").strip().upper()

    if carga == "A":
        carga_desc = "1 sensor de cada tipo, intervalo=10s"
        archivo_sugerido = "10s"
    else:
        carga_desc = "2 sensores de cada tipo, intervalo=5s"
        archivo_sugerido = "5s"

    # Broker (variable independiente del experimento)
    broker = ""
    while broker not in ("SIMPLE", "MULTIHILO"):
        broker = input("Broker que está corriendo (simple / multihilo): ").strip().upper()

    nombre_escenario = f"Carga {carga} + Broker {broker.lower()}"
    descripcion      = f"{carga_desc}, broker en modo {broker.lower()}"

    # BD y repeticiones
    bd_s = input("¿Qué BD usar? (principal / replica) [principal]: ").strip().lower()
    ruta = DB_REPLICA_PATH if "replica" in bd_s else DB_PRINCIPAL_PATH

    reps = input("Número de repeticiones para M2 [5]: ").strip()
    reps = int(reps) if reps.isdigit() else 5

    # ── Verificación previa de la BD ───────────────────────────────────────
    if not os.path.exists(ruta):
        print(f"\n⚠  ADVERTENCIA: la BD {ruta} no existe.")
        print("   Asegúrese de haber ejecutado monitoreo.py (principal)")
        print("   o analitica.py (réplica) antes de correr las métricas.")
        sys.exit(1)
    try:
        conn = sqlite3.connect(ruta)
        cur  = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablas = {row[0] for row in cur.fetchall()}
        conn.close()
        faltantes = {"eventos_sensores", "semaforos"} - tablas
        if faltantes:
            print(f"\n⚠  ADVERTENCIA: la BD {ruta} no tiene las tablas {faltantes}.")
            print("   Probablemente fue creada vacía por error. Bórrela y reinicie")
            print("   monitoreo.py / analitica.py para que se recree correctamente.")
            sys.exit(1)
    except Exception as exc:
        print(f"\n⚠  Error verificando BD: {exc}")
        sys.exit(1)

    # ── Confirmación visual antes de empezar ───────────────────────────────
    print(f"\n┌─────────────────────────────────────────────────────────────┐")
    print(f"│ CONFIRMAR ANTES DE CONTINUAR:                               │")
    print(f"│   Carga:      {carga} ({carga_desc[:42]:42}) │")
    print(f"│   Broker:     {broker.lower():46} │")
    print(f"│   BD:         {os.path.basename(ruta):46} │")
    print(f"│   Reps. M2:   {reps:<46} │")
    print(f"└─────────────────────────────────────────────────────────────┘")
    print(f"\n⚠ Verifique en la Terminal 4 que el banner del broker coincide:")
    print(f"  - simple    → [BROKER-SIMPLE]")
    print(f"  - multihilo → [BROKER-MULTIHILO] workers=N")

    confirma = input("\n¿Continuar? (s/n): ").strip().lower()
    if confirma not in ("s", "si", "sí", "y", "yes"):
        print("Cancelado por el usuario.")
        sys.exit(0)

    print(f"\n[{_ts()}] Iniciando mediciones. BD: {ruta}\n")

    r = ejecutar_escenario(nombre_escenario, descripcion, ruta, reps)

    # ── Resumen final ──────────────────────────────────────────────────────
    print(f"\n\n{'═'*65}")
    print("  RESUMEN FINAL DE MÉTRICAS")
    print(f"{'═'*65}")
    print(f"\n  {r['escenario']}")
    print(f"    {r['descripcion']}")
    print(f"    M1 — Eventos en 2 min:  {r['m1_eventos_2min']}")
    print(f"    M1 — Tasa (evt/s):       {r['m1_tasa_evt_s']}")
    print(f"    M2 — Latencia avg (s):   {r['m2_latencia_avg_s']}")
    print(f"    M2 — Latencia min (s):   {r['m2_latencia_min_s']}")
    print(f"    M2 — Latencia max (s):   {r['m2_latencia_max_s']}")
    print(f"    M2 — Exitosas:           {r['m2_exitosas']}/{r['m2_total']}")

    # Nombre de archivo descriptivo y único
    archivo = f"resultados_{broker.lower()}_{archivo_sugerido}.json"
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump([r], f, indent=2, ensure_ascii=False)
    print(f"\n  Resultados guardados en: {archivo}")


if __name__ == "__main__":
    main()
