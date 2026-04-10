# =============================================================================
# pc2/failover.py — Monitor de Heartbeat y Failover hacia BD Réplica (PC2)
#
# Detecta automáticamente si PC3 falla usando el patrón heartbeat:
#   - Envía un ping a PC3 cada HEARTBEAT_INTERVALO_SEG segundos
#   - Si PC3 no responde en HEARTBEAT_TIMEOUT_SEG → marca como CAÍDO
#   - El sistema conmuta transparentemente a la BD réplica en PC2
#   - Cuando PC3 se recupera, vuelve a usar la BD principal
#
# Se importa desde analitica.py para integrar la lógica de failover.
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
from config import (
    PC3_IP, PUERTO_MONITOREO_REP,
    HEARTBEAT_INTERVALO_SEG, HEARTBEAT_TIMEOUT_SEG,
    DB_PRINCIPAL_PATH, DB_REPLICA_PATH
)


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ── Estado global de PC3 ─────────────────────────────────────────────────────

class EstadoPC3:
    """
    Mantiene el estado de disponibilidad de PC3.
    Expone db_activa() para que el resto del sistema sepa qué BD usar.
    """

    def __init__(self):
        self._lock       = threading.Lock()
        self._disponible = True
        self._callbacks  = []   # funciones llamadas cuando cambia el estado

    @property
    def disponible(self) -> bool:
        with self._lock:
            return self._disponible

    def registrar_callback(self, fn):
        """Registra una función a llamar cuando PC3 cambia de estado."""
        self._callbacks.append(fn)

    def actualizar(self, ok: bool):
        """Actualiza el estado y dispara callbacks si hubo cambio."""
        with self._lock:
            cambio = (self._disponible != ok)
            self._disponible = ok
        if cambio:
            estado_str = "DISPONIBLE ✅" if ok else "CAÍDO 🔴"
            print(f"[FAILOVER][{ts()}] PC3 ahora: {estado_str}")
            for cb in self._callbacks:
                try:
                    cb(ok)
                except Exception as e:
                    print(f"[FAILOVER] Error en callback: {e}")

    def db_activa(self) -> str:
        """Retorna la ruta de la BD que debe usarse actualmente."""
        return DB_PRINCIPAL_PATH if self.disponible else DB_REPLICA_PATH


# Instancia global compartida por todo el sistema en PC2
estado_pc3 = EstadoPC3()


# ── Hilo de heartbeat ─────────────────────────────────────────────────────────

class HiloHeartbeat(threading.Thread):
    """
    Envía pings periódicos a PC3 y actualiza estado_pc3 según las respuestas.
    Si PC3 falla, el resto del sistema automáticamente usará la BD réplica.
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="Heartbeat")
        self.ctx = ctx

    def _ping_pc3(self) -> bool:
        """Envía un heartbeat a PC3 y retorna True si responde OK."""
        req = self.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, int(HEARTBEAT_TIMEOUT_SEG * 1000))
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{PC3_IP}:{PUERTO_MONITOREO_REP}")
        try:
            req.send_string(json.dumps({"accion": "HEARTBEAT"}))
            resp = json.loads(req.recv_string())
            return resp.get("status") == "OK"
        except (zmq.Again, Exception):
            return False
        finally:
            req.close()

    def run(self):
        fallos = 0
        print(f"[HEARTBEAT][{ts()}] Iniciado | "
              f"intervalo={HEARTBEAT_INTERVALO_SEG}s | "
              f"timeout={HEARTBEAT_TIMEOUT_SEG}s")

        while True:
            ok = self._ping_pc3()
            if ok:
                fallos = 0
                if not estado_pc3.disponible:
                    print(f"[HEARTBEAT][{ts()}] PC3 recuperado → volviendo a BD principal")
                estado_pc3.actualizar(True)
                print(f"[HEARTBEAT][{ts()}] PC3 OK | BD activa: {estado_pc3.db_activa()}")
            else:
                fallos += 1
                print(f"[HEARTBEAT][{ts()}] ⚠  PC3 sin respuesta ({fallos} fallo(s))")
                if fallos >= 2:
                    if estado_pc3.disponible:
                        print(f"[HEARTBEAT][{ts()}] PC3 CAÍDO → activando réplica: {DB_REPLICA_PATH}")
                    estado_pc3.actualizar(False)

            time.sleep(HEARTBEAT_INTERVALO_SEG)


def get_db_activa() -> str:
    """Función de conveniencia para obtener la BD activa desde cualquier módulo."""
    return estado_pc3.db_activa()


# ── Main (para prueba standalone del heartbeat) ───────────────────────────────

def main():
    ctx     = zmq.Context()
    monitor = HiloHeartbeat(ctx)

    estado_pc3.registrar_callback(
        lambda ok: print(
            f"[FAILOVER] BD activa: {'PRINCIPAL' if ok else 'RÉPLICA'} ({estado_pc3.db_activa()})"
        )
    )
    monitor.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("[FAILOVER] Detenido.")
    finally:
        ctx.term()


if __name__ == "__main__":
    main()
