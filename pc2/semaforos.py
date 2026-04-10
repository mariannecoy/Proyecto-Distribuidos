# =============================================================================
# pc2/semaforos.py — Servicio de Control de Semáforos (PC2)
#
# Recibe comandos de la analítica (PUSH/PULL) y ejecuta cambios de estado.
# Responsabilidades:
#   - Mantener el estado actual de todos los semáforos en memoria
#   - Actualizar la BD réplica con cada cambio
#   - Temporizador automático: verde → rojo tras la duración indicada
#   - Imprimir por pantalla todas las operaciones realizadas
#
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import json
import time
import threading
from datetime import datetime, timezone
from config import PUERTO_SEMAFOROS_PULL, DB_REPLICA_PATH
from database import inicializar_bd, actualizar_semaforo, consultar_todos_semaforos


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ── Estado en memoria de todos los semáforos ─────────────────────────────────
_semaforos = {}    # { "INT_A1": {"estado": "ROJO", "modo": "NORMAL", "duracion_seg": 15} }
_lock      = threading.Lock()


def _aplicar_cambio(interseccion: str, estado: str, modo: str, duracion: int):
    """
    Aplica un cambio de estado al semáforo:
      1. Actualiza el estado en memoria
      2. Persiste el cambio en la BD réplica
      3. Imprime la operación realizada
    """
    with _lock:
        anterior = _semaforos.get(interseccion, {}).get("estado", "DESCONOCIDO")
        _semaforos[interseccion] = {"estado": estado, "modo": modo, "duracion_seg": duracion}

    actualizar_semaforo(DB_REPLICA_PATH, interseccion, estado, modo, duracion)

    icono = "🟢" if estado == "VERDE" else "🔴"
    print(f"[SEMAFORO][{ts()}] {icono} {interseccion}: {anterior} → {estado} "
          f"| Modo={modo} | Duración={duracion}s")


def _temporizador_verde(interseccion: str, duracion: int):
    """
    Espera la duración del verde y vuelve el semáforo a ROJO automáticamente.
    Solo actúa si el semáforo sigue en VERDE (nadie lo cambió entretanto).
    """
    time.sleep(duracion)
    with _lock:
        estado_actual = _semaforos.get(interseccion, {}).get("estado")
    if estado_actual == "VERDE":
        _aplicar_cambio(interseccion, "ROJO", "NORMAL", 15)


def _hilo_resumen():
    """Imprime un resumen del estado de todos los semáforos cada 300 segundos."""
    while True:
        time.sleep(300)
        with _lock:
            total     = len(_semaforos)
            verdes    = sum(1 for v in _semaforos.values() if v["estado"] == "VERDE")
            prioridad = sum(1 for v in _semaforos.values() if v["modo"] == "PRIORIDAD")
            congestion= sum(1 for v in _semaforos.values() if v["modo"] == "CONGESTION")
        print(f"\n[SEMAFORO][{ts()}] ── RESUMEN ─────────────────────────────────")
        print(f"  Total: {total} | 🟢 Verdes: {verdes} | 🔴 Rojos: {total - verdes}")
        print(f"  Prioridad: {prioridad} | ⚠  Congestión: {congestion}")
        print(f"[SEMAFORO][{ts()}] ────────────────────────────────────────────\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    inicializar_bd(DB_REPLICA_PATH)

    ctx  = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SEMAFOROS_PULL}")

    print(f"[PC2-SEMAFOROS] Escuchando comandos en :{PUERTO_SEMAFOROS_PULL}")
    threading.Thread(target=_hilo_resumen, daemon=True).start()

    try:
        while True:
            msg = pull.recv_string()
            cmd = json.loads(msg)

            inter    = cmd.get("interseccion")
            estado   = cmd.get("estado", "VERDE")
            modo     = cmd.get("modo", "NORMAL")
            duracion = cmd.get("duracion_seg", 15)

            if not inter:
                print(f"[SEMAFORO] Comando sin intersección ignorado: {cmd}")
                continue

            _aplicar_cambio(inter, estado, modo, duracion)

            # Iniciar temporizador para retorno automático a ROJO
            if estado == "VERDE":
                t = threading.Thread(
                    target=_temporizador_verde,
                    args=(inter, duracion),
                    daemon=True
                )
                t.start()

    except KeyboardInterrupt:
        print("[PC2-SEMAFOROS] Detenido.")
    finally:
        pull.close()
        ctx.term()


if __name__ == "__main__":
    main()
