# =============================================================================
# pc2/failover.py — Monitor de Heartbeat y Failover hacia BD Réplica (PC2)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Detecta automáticamente la falla de PC3 usando el patrón Heartbeat:
#   - Envía un ping REQ/REP a PC3 cada HEARTBEAT_INTERVALO_SEG segundos.
#   - Si PC3 no responde tras HEARTBEAT_TIMEOUT_SEG, lo declara CAÍDO.
#   - Todo el sistema conmuta transparentemente a la BD réplica en PC2.
#   - Cuando PC3 se recupera, el sistema vuelve a usar la BD principal.
#
# Uso: importado por analitica.py y semaforos.py para obtener la BD activa.
# También puede ejecutarse de forma standalone para monitorear la conectividad.
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


def _ts() -> str:
    """Retorna el timestamp actual formateado para logs (HH:MM:SS)."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Estado global de disponibilidad de PC3
# =============================================================================

class EstadoPC3:
    """
    Mantiene el estado de disponibilidad de PC3 de forma thread-safe.

    Expone db_activa() para que cualquier módulo del sistema sepa
    en todo momento a qué base de datos debe escribir o leer.

    Atributos:
        disponible (bool): True si PC3 responde, False si está caído.
    """

    def __init__(self):
        """
        Inicializa el estado de PC3.

        Atributos privados:
            _lock:       Lock para acceso thread-safe a _disponible.
            _disponible: Estado actual; se asume True al arrancar para
                         evitar falsos negativos antes del primer heartbeat.
            _callbacks:  Lista de funciones a invocar cuando cambie el estado.
        """
        self._lock       = threading.Lock()
        self._disponible = True      # Se asume disponible al arrancar
        self._callbacks  = []        # Funciones a llamar cuando cambia el estado

    @property
    def disponible(self) -> bool:
        """Retorna True si PC3 está disponible (thread-safe)."""
        with self._lock:
            return self._disponible

    def registrar_callback(self, fn) -> None:
        """
        Registra una función a invocar cuando PC3 cambia de estado.

        Args:
            fn: Callable que recibe un booleano (True=disponible, False=caído).
        """
        self._callbacks.append(fn)

    def actualizar(self, ok: bool) -> None:
        """
        Actualiza el estado de PC3 y dispara los callbacks si hubo cambio.

        Args:
            ok: True si PC3 respondió al heartbeat, False si no respondió.
        """
        with self._lock:
            hubo_cambio      = (self._disponible != ok)
            self._disponible = ok

        if hubo_cambio:
            etiqueta = "DISPONIBLE ✅" if ok else "CAÍDO 🔴"
            print(f"[FAILOVER][{_ts()}] Estado de PC3 cambió → {etiqueta}")
            print(f"[FAILOVER][{_ts()}] BD activa: {self.db_activa()}")
            for cb in self._callbacks:
                try:
                    cb(ok)
                except Exception as exc:
                    print(f"[FAILOVER] Error en callback: {exc}")

    def db_activa(self) -> str:
        """
        Retorna la ruta de la BD que debe usarse según el estado de PC3.

        Returns:
            Ruta a DB_PRINCIPAL_PATH si PC3 está disponible,
            o DB_REPLICA_PATH si PC3 está caído.
        """
        return DB_PRINCIPAL_PATH if self.disponible else DB_REPLICA_PATH


# Instancia global compartida por todos los módulos de PC2
estado_pc3 = EstadoPC3()


# =============================================================================
# Hilo de heartbeat — monitorea PC3 en segundo plano
# =============================================================================

class HiloHeartbeat(threading.Thread):
    """
    Hilo daemon que envía pings periódicos a PC3 y actualiza estado_pc3.

    Patrón utilizado: Health Check (variante de Heartbeat).
    - Si PC3 falla 2 o más veces consecutivas → se activa la réplica.
    - En cuanto PC3 responde de nuevo → se vuelve a la BD principal.
    La conmutación es transparente para el resto del sistema gracias a
    estado_pc3.db_activa().
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido con el proceso principal.
        """
        super().__init__(daemon=True, name="Heartbeat-PC3")
        self.ctx = ctx

    def _ping_pc3(self) -> bool:
        """
        Envía una solicitud HEARTBEAT a PC3 y retorna True si responde OK.

        Crea un socket REQ temporal por cada intento para evitar estados
        sucios en caso de timeout (patrón Lazy Pirate simplificado).

        Returns:
            True si PC3 respondió con status OK, False en cualquier otro caso.
        """
        req = self.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, int(HEARTBEAT_TIMEOUT_SEG * 1000))
        req.setsockopt(zmq.LINGER, 0)   # No esperar al cerrar si hay mensajes pendientes
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
        """Bucle principal del heartbeat: ping → actualizar estado → esperar."""
        fallos_consecutivos = 0
        print(f"[HEARTBEAT][{_ts()}] Iniciado "
              f"| intervalo={HEARTBEAT_INTERVALO_SEG}s "
              f"| timeout={HEARTBEAT_TIMEOUT_SEG}s "
              f"| objetivo=PC3 {PC3_IP}:{PUERTO_MONITOREO_REP}")

        while True:
            ok = self._ping_pc3()

            if ok:
                fallos_consecutivos = 0
                if not estado_pc3.disponible:
                    print(f"[HEARTBEAT][{_ts()}] PC3 recuperado → "
                          f"volviendo a BD principal ({DB_PRINCIPAL_PATH})")
                estado_pc3.actualizar(True)
                print(f"[HEARTBEAT][{_ts()}] PC3 OK ✅")
            else:
                fallos_consecutivos += 1
                print(f"[HEARTBEAT][{_ts()}] ⚠  PC3 sin respuesta "
                      f"({fallos_consecutivos} fallo(s) consecutivo(s))")
                # Declarar caído tras 2 fallos consecutivos para evitar falsos positivos
                if fallos_consecutivos >= 2:
                    if estado_pc3.disponible:
                        print(f"[HEARTBEAT][{_ts()}] PC3 CAÍDO 🔴 → "
                              f"activando réplica ({DB_REPLICA_PATH})")
                    estado_pc3.actualizar(False)

            time.sleep(HEARTBEAT_INTERVALO_SEG)


def get_db_activa() -> str:
    """
    Función de conveniencia: retorna la ruta de la BD activa en este momento.

    Puede importarse desde cualquier módulo de PC2 sin necesidad de
    referenciar directamente la instancia estado_pc3.

    Returns:
        Ruta al archivo SQLite actualmente en uso (principal o réplica).
    """
    return estado_pc3.db_activa()


# =============================================================================
# Main — ejecución standalone para pruebas de conectividad
# =============================================================================

def main():
    """
    Modo standalone: muestra el estado de PC3 en tiempo real.
    Útil para verificar la conectividad antes de arrancar el sistema completo.
    """
    ctx     = zmq.Context()
    monitor = HiloHeartbeat(ctx)

    # Callback de ejemplo: imprime la BD activa en cada cambio de estado
    estado_pc3.registrar_callback(
        lambda ok: print(
            f"[FAILOVER-CALLBACK] BD activa → "
            f"{'PRINCIPAL (' + DB_PRINCIPAL_PATH + ')' if ok else 'RÉPLICA (' + DB_REPLICA_PATH + ')'}"
        )
    )

    monitor.start()
    print(f"[FAILOVER] Monitoreando PC3 en {PC3_IP}:{PUERTO_MONITOREO_REP}. "
          f"Ctrl+C para detener.")

    try:
        while True:
            time.sleep(5)
            print(f"[FAILOVER] Estado actual → PC3: "
                  f"{'OK' if estado_pc3.disponible else 'CAÍDO'} | "
                  f"BD: {estado_pc3.db_activa()}")
    except KeyboardInterrupt:
        print("[FAILOVER] Detenido.")
    finally:
        ctx.term()


if __name__ == "__main__":
    main()
