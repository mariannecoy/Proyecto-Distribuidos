# =============================================================================
# pc2/semaforos.py — Servicio de Control de Semáforos (PC2)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Recibe comandos de la analítica vía PUSH/PULL y ejecuta cambios de estado
# en los semáforos simulados. Responsabilidades:
#   - Mantener el estado actual de todos los semáforos en memoria.
#   - Actualizar la BD réplica (PC2) y la BD principal (PC3) en cada cambio.
#   - Temporizador automático: fase verde → rojo tras la duración indicada.
#   - Imprimir por pantalla todas las operaciones realizadas.
#
# Integración con failover:
#   - Si PC3 está caído, los cambios se persisten solo en la réplica local.
#   - Cuando PC3 se recupera, vuelve a actualizar la BD principal.
#
# Uso: python semaforos.py
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import json
import time
import threading
from datetime import datetime, timezone
from config import (
    PC3_IP, PUERTO_SEMAFOROS_PULL,
    PUERTO_BD_PRINCIPAL_PULL, DB_REPLICA_PATH
)
from database import inicializar_bd, actualizar_semaforo, consultar_todos_semaforos
from failover import estado_pc3


def _ts() -> str:
    """Retorna la hora UTC actual formateada para logs (HH:MM:SS)."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Estado en memoria de todos los semáforos
# =============================================================================

# Diccionario: { "INT_A1": {"estado": "ROJO", "modo": "NORMAL", "duracion_seg": 15} }
_semaforos: dict = {}
_lock = threading.Lock()

# Socket PUSH hacia BD principal en PC3 (inicializado en main)
_push_bd_principal: zmq.Socket = None
_ctx_global: zmq.Context       = None


# =============================================================================
# Lógica de cambio de estado
# =============================================================================

def _aplicar_cambio(interseccion: str, estado: str, modo: str, duracion: int) -> None:
    """
    Aplica un cambio de estado a un semáforo simulado.

    Pasos:
        1. Actualiza el estado en el diccionario en memoria (thread-safe).
        2. Persiste el cambio en la BD réplica local (siempre).
        3. Persiste en BD principal (PC3) si está disponible (failover).
        4. Imprime la operación realizada con icono de color.

    Args:
        interseccion: Código de la intersección (ej. "INT_C3").
        estado:       Nuevo estado: "VERDE" o "ROJO".
        modo:         Modo de operación: "NORMAL", "CONGESTION", "PRIORIDAD" o "MANUAL".
        duracion:     Duración en segundos de la fase verde.
    """
    with _lock:
        anterior = _semaforos.get(interseccion, {}).get("estado", "DESCONOCIDO")
        _semaforos[interseccion] = {
            "estado":       estado,
            "modo":         modo,
            "duracion_seg": duracion
        }

    # 1. Persistir en BD réplica local (siempre disponible)
    actualizar_semaforo(DB_REPLICA_PATH, interseccion, estado, modo, duracion)

    # 2. Persistir en BD principal en PC3 (si no está caído)
    if _push_bd_principal and estado_pc3.disponible:
        try:
            paquete = json.dumps({
                "tipo": "semaforo",
                "data": {
                    "interseccion": interseccion,
                    "estado":       estado,
                    "modo":         modo,
                    "duracion_seg": duracion
                }
            })
            _push_bd_principal.send_string(paquete, zmq.NOBLOCK)
        except zmq.Again:
            print(f"[SEMAFORO][{_ts()}] BD principal saturada, semáforo no enviado a PC3")
        except Exception as exc:
            print(f"[SEMAFORO][{_ts()}] Error enviando a PC3: {exc}")
    elif not estado_pc3.disponible:
        print(f"[SEMAFORO][{_ts()}] ⚠  PC3 CAÍDO — cambio solo en réplica local")

    icono = "🟢" if estado == "VERDE" else "🔴"
    print(f"[SEMAFORO][{_ts()}] {icono} {interseccion}: {anterior} → {estado} "
          f"| Modo={modo} | Duración={duracion}s")


def _temporizador_verde(interseccion: str, duracion: int) -> None:
    """
    Espera la duración de la fase verde y vuelve el semáforo a ROJO.

    Solo actúa si el semáforo sigue en VERDE al vencer el tiempo.
    Así se evita revertir un cambio manual posterior.

    Args:
        interseccion: Código de la intersección.
        duracion:     Duración en segundos de la fase verde.
    """
    time.sleep(duracion)
    with _lock:
        estado_actual = _semaforos.get(interseccion, {}).get("estado")
    if estado_actual == "VERDE":
        print(f"[SEMAFORO][{_ts()}] ⏱  {interseccion}: temporizador verde vencido → ROJO")
        _aplicar_cambio(interseccion, "ROJO", "NORMAL", 15)


def _hilo_resumen() -> None:
    """
    Hilo daemon que imprime un resumen del estado de todos los semáforos cada 60s.
    Útil para monitorear el sistema durante las pruebas de rendimiento.
    """
    while True:
        time.sleep(60)
        with _lock:
            total      = len(_semaforos)
            verdes     = sum(1 for v in _semaforos.values() if v["estado"] == "VERDE")
            prioridad  = sum(1 for v in _semaforos.values() if v["modo"] == "PRIORIDAD")
            congestion = sum(1 for v in _semaforos.values() if v["modo"] == "CONGESTION")

        print(f"\n[SEMAFORO][{_ts()}] ══ RESUMEN ══════════════════════════════════")
        print(f"  Total intersecciones: {total}")
        print(f"  🟢 En verde:          {verdes}")
        print(f"  🔴 En rojo:           {total - verdes}")
        print(f"  🚨 Prioridad activa:  {prioridad}")
        print(f"  ⚠  Congestión:        {congestion}")
        print(f"  BD activa:            {'PRINCIPAL' if estado_pc3.disponible else 'RÉPLICA'}")
        print(f"[SEMAFORO][{_ts()}] ═══════════════════════════════════════════════\n")


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada del servicio de control de semáforos (PC2).

    Inicializa la BD réplica, configura los sockets y entra en el bucle
    principal de recepción de comandos de la analítica.
    """
    global _push_bd_principal, _ctx_global

    inicializar_bd(DB_REPLICA_PATH)

    _ctx_global = zmq.Context()

    # Socket PULL: recibe comandos de la analítica
    pull = _ctx_global.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SEMAFOROS_PULL}")

    # Socket PUSH: envía actualizaciones a la BD principal en PC3
    _push_bd_principal = _ctx_global.socket(zmq.PUSH)
    _push_bd_principal.connect(f"tcp://{PC3_IP}:{PUERTO_BD_PRINCIPAL_PULL}")

    print(f"[PC2-SEMAFOROS][{_ts()}] Escuchando comandos en :{PUERTO_SEMAFOROS_PULL}")
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
                print(f"[SEMAFORO][{_ts()}] Comando sin intersección ignorado: {cmd}")
                continue

            _aplicar_cambio(inter, estado, modo, duracion)

            # Lanzar temporizador automático para volver a ROJO
            if estado == "VERDE":
                t = threading.Thread(
                    target=_temporizador_verde,
                    args=(inter, duracion),
                    daemon=True
                )
                t.start()

    except KeyboardInterrupt:
        print(f"[PC2-SEMAFOROS][{_ts()}] Detenido por el usuario.")
    finally:
        pull.close()
        if _push_bd_principal:
            _push_bd_principal.close()
        _ctx_global.term()
        print(f"[PC2-SEMAFOROS][{_ts()}] Apagado completo.")


if __name__ == "__main__":
    main()
