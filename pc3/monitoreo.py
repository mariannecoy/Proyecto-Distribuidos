# =============================================================================
# pc3/monitoreo.py — Servicio de Monitoreo, Consulta y BD Principal (PC3)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Responsabilidades:
#   1. Recibir datos de la analítica (PC2) y persistirlos en la BD principal (PULL).
#   2. Atender consultas e indicaciones directas del usuario/cliente (REQ/REP).
#   3. Reenviar indicaciones de control a la analítica en PC2 (REQ/REP).
#   4. Responder solicitudes de Heartbeat para el monitor de failover de PC2.
#   5. Imprimir por pantalla todas las operaciones realizadas.
#
# Consultas disponibles (desde cliente.py):
#   GET_SEMAFOROS       → Estado actual de todos los semáforos.
#   GET_SEMAFORO        → Estado de un semáforo específico.
#   GET_CONGESTION      → Historial de congestión (filtrable por fecha e intersección).
#   GET_HISTORIAL       → Historial de cambios de semáforo.
#   GET_PRIORIDADES     → Eventos de priorización registrados.
#
# Indicaciones directas:
#   PRIORIDAD_EMERGENCIA → Forzar ola verde para vehículo de emergencia.
#   CAMBIO_SEMAFORO      → Cambio manual de semáforo por el operador.
#
# Uso: python monitoreo.py
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
    PC2_IP,
    PUERTO_BD_PRINCIPAL_PULL,
    PUERTO_MONITOREO_REP,
    PUERTO_ANALITICA_REP,
    DB_PRINCIPAL_PATH
)
from database import (
    inicializar_bd,
    guardar_evento_sensor, guardar_congestion,
    actualizar_semaforo,   guardar_prioridad,
    consultar_todos_semaforos, consultar_semaforo,
    consultar_congestion_historica, consultar_historial_semaforos,
    consultar_prioridades
)


def _ts() -> str:
    """Retorna la hora UTC actual formateada para logs (HH:MM:SS)."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Hilo 1: Persistencia — recibe datos de la analítica y los guarda en BD principal
# =============================================================================

class HiloPersistencia(threading.Thread):
    """
    Escucha en el puerto PULL y persiste en la BD principal los paquetes
    enviados por la analítica (PC2).

    Tipos de paquetes soportados:
        - "evento_sensor": evento crudo de cámara, espira o GPS.
        - "congestion":    condición de tráfico detectada.
        - "semaforo":      cambio de estado de semáforo.
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido.
        """
        super().__init__(daemon=True, name="PC3-PULL-BD")
        self.ctx = ctx

    def run(self):
        """
        Bucle principal del hilo de persistencia en la BD principal.

        Recibe mensajes vía PUSH/PULL desde la analítica y el controlador
        de semáforos en PC2, y persiste los datos en la base de datos
        maestra ubicada en PC3.

        Tipos de mensaje atendidos:
            "evento_sensor": Lecturas crudas de los sensores con todos sus
                             campos numéricos (velocidad, densidad, etc.).
            "congestion":    Eventos detectados por la analítica, con la
                             clasificación NORMAL/CONGESTION/PRIORIDAD.
            "semaforo":      Cambios de estado de semáforos con su modo
                             (NORMAL/CONGESTION/PRIORIDAD/MANUAL) y duración.

        Por cada mensaje:
            1. Deserializa el JSON recibido.
            2. Delega al módulo database la inserción en la tabla
               correspondiente (eventos_sensores, congestion_eventos,
               semaforos / historial_cambios).
            3. Registra la operación por consola para trazabilidad.

        El bucle es infinito y daemon. Si ocurre una excepción la registra
        y continúa, para no detener el servicio por errores aislados.
        """
        pull = self.ctx.socket(zmq.PULL)
        pull.bind(f"tcp://*:{PUERTO_BD_PRINCIPAL_PULL}")
        print(f"[PC3-BD][{_ts()}] Recibiendo datos en :{PUERTO_BD_PRINCIPAL_PULL}")

        while True:
            try:
                msg  = pull.recv_string()
                data = json.loads(msg)
                tipo = data.get("tipo")
                body = data.get("data", {})

                if tipo == "evento_sensor":
                    guardar_evento_sensor(
                        DB_PRINCIPAL_PATH,
                        body.get("sensor_id"), body.get("tipo_sensor"),
                        body.get("interseccion"), body, body.get("timestamp", "")
                    )
                    print(f"[PC3-BD][{_ts()}] ✔ Sensor: "
                          f"{body.get('sensor_id')} @ {body.get('interseccion')}")

                elif tipo == "congestion":
                    guardar_congestion(
                        DB_PRINCIPAL_PATH,
                        body.get("interseccion"), body.get("nivel"),
                        body.get("cola"), body.get("velocidad"), body.get("densidad")
                    )
                    print(f"[PC3-BD][{_ts()}] ✔ Congestión: "
                          f"{body.get('interseccion')} → {body.get('nivel')}")

                elif tipo == "semaforo":
                    actualizar_semaforo(
                        DB_PRINCIPAL_PATH,
                        body.get("interseccion"), body.get("estado"),
                        body.get("modo"), body.get("duracion_seg", 15)
                    )
                    print(f"[PC3-BD][{_ts()}] ✔ Semáforo: "
                          f"{body.get('interseccion')} → {body.get('estado')}")

                else:
                    print(f"[PC3-BD][{_ts()}] ⚠ Tipo de paquete desconocido: {tipo}")

            except Exception as exc:
                print(f"[PC3-BD][{_ts()}] ERROR: {exc}")
                time.sleep(0.5)


# =============================================================================
# Hilo 2: REP — atiende consultas e indicaciones del usuario/cliente
# =============================================================================

class HiloMonitoreoREP(threading.Thread):
    """
    Patrón REQ/REP. Atiende solicitudes del cliente (pc3/cliente.py) y del
    monitor de heartbeat de PC2.

    Las indicaciones de control (PRIORIDAD_EMERGENCIA, CAMBIO_SEMAFORO)
    se reenvían a la analítica en PC2 para que sean ejecutadas.
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido.
        """
        super().__init__(daemon=True, name="PC3-REP")
        self.ctx = ctx

    def _enviar_a_analitica(self, cmd: dict) -> dict:
        """
        Reenvía un comando de control a la analítica en PC2 y retorna la respuesta.

        Crea un socket REQ temporal con timeout de 4 segundos para evitar
        bloqueos permanentes si PC2 no responde.

        Args:
            cmd: Diccionario con la indicación a enviar.

        Returns:
            Respuesta de la analítica como diccionario, o error si hay timeout.
        """
        req = self.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 4000)    # 4 segundos de timeout
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{PC2_IP}:{PUERTO_ANALITICA_REP}")
        try:
            req.send_string(json.dumps(cmd))
            return json.loads(req.recv_string())
        except zmq.Again:
            return {"status": "ERROR", "msg": "PC2 no responde (timeout 4s)"}
        except Exception as exc:
            return {"status": "ERROR", "msg": str(exc)}
        finally:
            req.close()

    def run(self):
        """
        Bucle principal del servidor REP de monitoreo en PC3.

        Es el punto único de entrada para todas las solicitudes del cliente
        interactivo (cliente.py) y de cualquier otro consumidor externo
        (incluido metricas.py). Atiende dos tipos de operaciones:

        Consultas a la BD principal (resuelve localmente):
            HEARTBEAT:        Verificación de vida; responde OK + timestamp.
            GET_SEMAFOROS:    Estado actual de todos los semáforos.
            GET_SEMAFORO:     Estado de una intersección específica.
            GET_CONGESTION:   Historial de eventos de congestión, con filtros
                              opcionales de fecha y/o intersección.
            GET_HISTORIAL:    Historial de cambios de semáforo.
            GET_PRIORIDADES:  Eventos de priorización registrados.

        Indicaciones de control (delega a la analítica en PC2):
            PRIORIDAD_EMERGENCIA: Reenvía a PC2 vía _enviar_a_analitica().
            CAMBIO_SEMAFORO:      Reenvía a PC2 para cambio manual.

        Toda respuesta incluye un campo "status" (OK/ERROR) y opcionalmente
        un "data" con la información. Las indicaciones que requieren a PC2
        usan un timeout de 4 s para evitar bloqueos si PC2 está caído.

        El bucle es infinito y daemon.
        """
        rep = self.ctx.socket(zmq.REP)
        rep.bind(f"tcp://*:{PUERTO_MONITOREO_REP}")
        print(f"[PC3-MONITOREO][{_ts()}] Escuchando en :{PUERTO_MONITOREO_REP}")

        while True:
            try:
                msg    = rep.recv_string()
                req    = json.loads(msg)
                accion = req.get("accion", "").upper()
                print(f"[PC3-MONITOREO][{_ts()}] Solicitud: {accion}")

                # ── Consultas a BD principal ───────────────────────────────

                if accion == "GET_SEMAFOROS":
                    # Retorna estado actual de todos los semáforos registrados
                    data = consultar_todos_semaforos(DB_PRINCIPAL_PATH)
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_SEMAFORO":
                    # Retorna estado de un semáforo específico
                    inter = req.get("interseccion")
                    data  = consultar_semaforo(DB_PRINCIPAL_PATH, inter)
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_CONGESTION":
                    # Historial de congestión con filtros opcionales de fecha e intersección
                    data = consultar_congestion_historica(
                        DB_PRINCIPAL_PATH,
                        desde=req.get("desde"),
                        hasta=req.get("hasta"),
                        interseccion=req.get("interseccion")
                    )
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_HISTORIAL":
                    # Historial de cambios de semáforo
                    data = consultar_historial_semaforos(
                        DB_PRINCIPAL_PATH,
                        interseccion=req.get("interseccion"),
                        limite=req.get("limite", 100)
                    )
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_PRIORIDADES":
                    # Eventos de priorización de emergencia
                    data = consultar_prioridades(
                        DB_PRINCIPAL_PATH,
                        interseccion=req.get("interseccion")
                    )
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                # ── Indicaciones directas → reenvío a analítica PC2 ───────

                elif accion == "PRIORIDAD_EMERGENCIA":
                    inter  = req.get("interseccion")
                    tipo_v = req.get("tipo_vehiculo", "ambulancia")
                    # Registrar en BD principal antes de reenviar
                    guardar_prioridad(DB_PRINCIPAL_PATH, inter, tipo_v, "usuario")
                    resp = self._enviar_a_analitica({
                        "accion":         "PRIORIDAD_EMERGENCIA",
                        "interseccion":   inter,
                        "tipo_vehiculo":  tipo_v
                    })
                    print(f"[PC3-MONITOREO][{_ts()}] 🚨 Prioridad {tipo_v} en {inter} "
                          f"→ respuesta analítica: {resp.get('status')}")
                    rep.send_string(json.dumps(resp))

                elif accion == "CAMBIO_SEMAFORO":
                    resp = self._enviar_a_analitica({
                        "accion":       "CAMBIO_SEMAFORO",
                        "interseccion": req.get("interseccion"),
                        "estado":       req.get("estado", "VERDE")
                    })
                    print(f"[PC3-MONITOREO][{_ts()}] 🔧 Cambio manual en "
                          f"{req.get('interseccion')} → {req.get('estado')}")
                    rep.send_string(json.dumps(resp))

                # ── Heartbeat — responde al monitor de failover de PC2 ────

                elif accion == "HEARTBEAT":
                    rep.send_string(json.dumps({"status": "OK", "ts": _ts()}))

                else:
                    rep.send_string(json.dumps({
                        "status": "ERROR",
                        "msg":    f"Acción desconocida: {accion}"
                    }))

            except Exception as exc:
                print(f"[PC3-MONITOREO][{_ts()}] ERROR: {exc}")
                try:
                    rep.send_string(json.dumps({"status": "ERROR", "msg": str(exc)}))
                except Exception:
                    pass


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada del servicio de monitoreo y BD principal (PC3).

    Inicializa la BD principal y lanza los dos hilos de servicio:
        - HiloPersistencia: recibe y guarda datos de la analítica.
        - HiloMonitoreoREP: atiende consultas e indicaciones del usuario.
    """
    inicializar_bd(DB_PRINCIPAL_PATH)

    ctx = zmq.Context()
    HiloPersistencia(ctx).start()
    HiloMonitoreoREP(ctx).start()

    print(f"[PC3][{_ts()}] Monitoreo y BD principal activos. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[PC3][{_ts()}] Deteniendo servicio de monitoreo...")
    finally:
        ctx.term()
        print(f"[PC3][{_ts()}] Apagado completo.")


if __name__ == "__main__":
    main()
