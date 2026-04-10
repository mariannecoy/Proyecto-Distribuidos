# =============================================================================
# pc3/monitoreo.py — Servicio de Monitoreo, Consulta y BD Principal (PC3)
#
# Responsabilidades:
#   1. Recibir datos de la analítica (PC2) y guardarlos en la BD principal → PULL
#   2. Atender consultas e indicaciones directas del usuario → REQ/REP
#   3. Reenviar indicaciones directas a la analítica (PC2) → REQ/REP
#   4. Imprimir por pantalla todas las operaciones que va realizando
#
# Consultas disponibles:
#   - Estado actual de semáforos (todos o uno específico)
#   - Historial de congestión (filtrable por fecha e intersección)
#   - Historial de cambios de semáforo
#   - Eventos de priorización (ambulancias, etc.)
#
# Indicaciones directas:
#   - PRIORIDAD_EMERGENCIA: forzar ola verde para vehículo de emergencia
#   - CAMBIO_SEMAFORO: cambio manual de semáforo
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
    PC2_IP,
    PUERTO_BD_PRINCIPAL_PULL,
    PUERTO_MONITOREO_REP,
    PUERTO_ANALITICA_REP,
    DB_PRINCIPAL_PATH
)
from database import (
    inicializar_bd,
    guardar_evento_sensor, guardar_congestion,
    actualizar_semaforo, guardar_prioridad,
    consultar_todos_semaforos, consultar_semaforo,
    consultar_congestion_historica, consultar_historial_semaforos,
    consultar_prioridades
)


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ── Hilo 1: recepción y persistencia en BD principal ─────────────────────────

class HiloPersistencia(threading.Thread):
    """
    Recibe paquetes de datos enviados por la analítica (PC2) vía PUSH/PULL
    y los persiste en la BD principal.
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="PC3-PULL-BD")
        self.ctx = ctx

    def run(self):
        pull = self.ctx.socket(zmq.PULL)
        pull.bind(f"tcp://*:{PUERTO_BD_PRINCIPAL_PULL}")
        print(f"[PC3-BD][{ts()}] Recibiendo datos en :{PUERTO_BD_PRINCIPAL_PULL}")

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
                    print(f"[PC3-BD][{ts()}] ✔ Evento guardado: "
                          f"{body.get('sensor_id')} @ {body.get('interseccion')}")

                elif tipo == "congestion":
                    guardar_congestion(
                        DB_PRINCIPAL_PATH,
                        body.get("interseccion"), body.get("nivel"),
                        body.get("cola"), body.get("velocidad"), body.get("densidad")
                    )
                    print(f"[PC3-BD][{ts()}] ✔ Congestión: "
                          f"{body.get('interseccion')} → {body.get('nivel')}")

                elif tipo == "semaforo":
                    actualizar_semaforo(
                        DB_PRINCIPAL_PATH,
                        body.get("interseccion"), body.get("estado"),
                        body.get("modo"), body.get("duracion_seg", 15)
                    )

            except Exception as e:
                print(f"[PC3-BD] ERROR: {e}")
                time.sleep(0.5)


# ── Hilo 2: atención de consultas e indicaciones del usuario ─────────────────

class HiloMonitoreoREP(threading.Thread):
    """
    Patrón REQ/REP.
    Atiende al cliente (usuario o PC2) con consultas e indicaciones directas.
    Las indicaciones de control se reenvían a la analítica en PC2.
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="PC3-REP")
        self.ctx = ctx

    def _enviar_a_analitica(self, cmd: dict) -> dict:
        """Reenvía un comando a la analítica de PC2 (REQ/REP) y retorna la respuesta."""
        req = self.ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 4000)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{PC2_IP}:{PUERTO_ANALITICA_REP}")
        try:
            req.send_string(json.dumps(cmd))
            return json.loads(req.recv_string())
        except zmq.Again:
            return {"status": "ERROR", "msg": "PC2 no responde (timeout)"}
        except Exception as e:
            return {"status": "ERROR", "msg": str(e)}
        finally:
            req.close()

    def run(self):
        rep = self.ctx.socket(zmq.REP)
        rep.bind(f"tcp://*:{PUERTO_MONITOREO_REP}")
        print(f"[PC3-MONITOREO][{ts()}] Escuchando en :{PUERTO_MONITOREO_REP}")

        while True:
            try:
                msg    = rep.recv_string()
                req    = json.loads(msg)
                accion = req.get("accion", "").upper()
                print(f"[PC3-MONITOREO][{ts()}] Solicitud: {accion}")

                # ── Consultas a BD principal ───────────────────────────────
                if accion == "GET_SEMAFOROS":
                    data = consultar_todos_semaforos(DB_PRINCIPAL_PATH)
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_SEMAFORO":
                    data = consultar_semaforo(DB_PRINCIPAL_PATH, req.get("interseccion"))
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_CONGESTION":
                    data = consultar_congestion_historica(
                        DB_PRINCIPAL_PATH,
                        desde=req.get("desde"),
                        hasta=req.get("hasta"),
                        interseccion=req.get("interseccion")
                    )
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_HISTORIAL":
                    data = consultar_historial_semaforos(
                        DB_PRINCIPAL_PATH,
                        interseccion=req.get("interseccion"),
                        limite=req.get("limite", 100)
                    )
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                elif accion == "GET_PRIORIDADES":
                    data = consultar_prioridades(DB_PRINCIPAL_PATH, req.get("interseccion"))
                    rep.send_string(json.dumps({"status": "OK", "data": data}))

                # ── Indicaciones directas → se reenvían a PC2 analítica ───
                elif accion == "PRIORIDAD_EMERGENCIA":
                    inter  = req.get("interseccion")
                    tipo_v = req.get("tipo_vehiculo", "ambulancia")
                    guardar_prioridad(DB_PRINCIPAL_PATH, inter, tipo_v, "usuario")
                    resp = self._enviar_a_analitica({
                        "accion": "PRIORIDAD_EMERGENCIA",
                        "interseccion": inter,
                        "tipo_vehiculo": tipo_v
                    })
                    print(f"[PC3-MONITOREO][{ts()}] Prioridad {tipo_v} en {inter}")
                    rep.send_string(json.dumps(resp))

                elif accion == "CAMBIO_SEMAFORO":
                    resp = self._enviar_a_analitica({
                        "accion": "CAMBIO_SEMAFORO",
                        "interseccion": req.get("interseccion"),
                        "estado": req.get("estado", "VERDE")
                    })
                    rep.send_string(json.dumps(resp))

                elif accion == "HEARTBEAT":
                    rep.send_string(json.dumps({"status": "OK", "ts": ts()}))

                else:
                    rep.send_string(json.dumps({
                        "status": "ERROR", "msg": f"Accion desconocida: {accion}"
                    }))

            except Exception as e:
                print(f"[PC3-MONITOREO] ERROR: {e}")
                try:
                    rep.send_string(json.dumps({"status": "ERROR", "msg": str(e)}))
                except Exception:
                    pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    inicializar_bd(DB_PRINCIPAL_PATH)

    ctx = zmq.Context()
    HiloPersistencia(ctx).start()
    HiloMonitoreoREP(ctx).start()

    print(f"[PC3] Monitoreo y BD principal activos. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[PC3] Detenido.")
    finally:
        ctx.term()


if __name__ == "__main__":
    main()
