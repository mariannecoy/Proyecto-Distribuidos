# =============================================================================
# pc2/analitica.py — Servicio de Analítica (PC2)
#
# Responsabilidades:
#   1. Suscribirse a eventos del broker PC1 (PUB/SUB)
#   2. Procesar eventos y detectar congestión con reglas simples
#   3. Enviar datos a BD principal (PC3) y réplica (PC2) → PUSH/PULL
#   4. Enviar comandos de cambio de semáforo al servicio de control → PUSH/PULL
#   5. Recibir indicaciones directas del monitoreo PC3 → REQ/REP
#   6. Imprimir por pantalla el estado del tráfico y acciones tomadas
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
    PC1_IP, PC2_IP, PC3_IP,
    PUERTO_BROKER_A_PC2,
    PUERTO_ANALITICA_REP,
    PUERTO_SEMAFOROS_PULL,
    PUERTO_BD_REPLICA_PULL,
    PUERTO_BD_PRINCIPAL_PULL,
    UMBRAL_COLA_NORMAL, UMBRAL_VEL_NORMAL, UMBRAL_DENSIDAD_NORMAL,
    TIEMPO_VERDE_NORMAL, TIEMPO_VERDE_CONGESTION, TIEMPO_VERDE_PRIORIDAD,
    DB_REPLICA_PATH
)
from database import (
    inicializar_bd, guardar_evento_sensor, guardar_congestion,
    guardar_prioridad, actualizar_semaforo, consultar_todos_semaforos
)


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ── Estado acumulado de sensores por intersección ────────────────────────────
# Guarda la última lectura de cada tipo de sensor por intersección
_estado_sensores = {}   # { "INT_A1": { "camara": {...}, "gps": {...}, ... } }
_lock_estado     = threading.Lock()


def _determinar_condicion(datos_inter: dict):
    """
    Aplica las reglas de tráfico y retorna (nivel, cola, velocidad, densidad).

    Reglas del grupo:
      NORMAL:    Q < 5  AND Vp > 35  AND D < 20
      CONGESTION:Q >= 5 OR  Vp <= 35 OR  D >= 20
    """
    cam   = datos_inter.get("camara", {})
    gps   = datos_inter.get("gps", {})

    cola      = cam.get("volumen", 0)
    velocidad = cam.get("velocidad_promedio") or gps.get("velocidad_promedio", 50)
    densidad  = gps.get("densidad", 0)

    if (cola < UMBRAL_COLA_NORMAL and
            velocidad > UMBRAL_VEL_NORMAL and
            densidad < UMBRAL_DENSIDAD_NORMAL):
        nivel = "NORMAL"
    else:
        nivel = "CONGESTION"

    return nivel, cola, velocidad, densidad


def _hacer_comando_semaforo(interseccion, estado, modo, duracion):
    """Construye el dict de comando para el controlador de semáforos."""
    return {
        "interseccion": interseccion,
        "estado":       estado,
        "modo":         modo,
        "duracion_seg": duracion,
        "timestamp":    datetime.now(timezone.utc).isoformat()
    }


# ── Hilo principal: suscriptor al broker ──────────────────────────────────────

class HiloSuscriptor(threading.Thread):
    """
    Se suscribe al broker de PC1 y procesa cada evento entrante.
    Por cada evento:
      - Guarda en BD réplica local
      - Reenvía a BD principal en PC3
      - Detecta congestión y emite comando de semáforo
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="Analitica-SUB")
        self.ctx = ctx

    def run(self):
        # SUB al broker
        sub = self.ctx.socket(zmq.SUB)
        sub.connect(f"tcp://{PC1_IP}:{PUERTO_BROKER_A_PC2}")
        sub.setsockopt_string(zmq.SUBSCRIBE, "camara")
        sub.setsockopt_string(zmq.SUBSCRIBE, "espira_inductiva")
        sub.setsockopt_string(zmq.SUBSCRIBE, "gps")

        # PUSH a BD principal en PC3
        push_bd_principal = self.ctx.socket(zmq.PUSH)
        push_bd_principal.connect(f"tcp://{PC3_IP}:{PUERTO_BD_PRINCIPAL_PULL}")

        # PUSH a BD réplica en PC2 (local)
        push_bd_replica = self.ctx.socket(zmq.PUSH)
        push_bd_replica.connect(f"tcp://127.0.0.1:{PUERTO_BD_REPLICA_PULL}")

        # PUSH al controlador de semáforos
        push_semaforo = self.ctx.socket(zmq.PUSH)
        push_semaforo.connect(f"tcp://127.0.0.1:{PUERTO_SEMAFOROS_PULL}")

        print(f"[ANALITICA][{ts()}] Suscrito al broker en {PC1_IP}:{PUERTO_BROKER_A_PC2}")

        while True:
            try:
                msg    = sub.recv_string()
                partes = msg.split(" ", 1)
                if len(partes) < 2:
                    continue
                topico, payload = partes
                evento = json.loads(payload)
                inter  = evento.get("interseccion", "DESCONOCIDA")

                # 1. Guardar en BD réplica local (asíncrono, no bloqueante)
                guardar_evento_sensor(
                    DB_REPLICA_PATH,
                    evento.get("sensor_id"), topico, inter,
                    evento, evento.get("timestamp", "")
                )

                # 2. Reenviar a BD principal en PC3
                paquete_bd = json.dumps({"tipo": "evento_sensor", "data": evento})
                try:
                    push_bd_principal.send_string(paquete_bd)
                except Exception as e:
                    print(f"[ANALITICA] No se pudo enviar a PC3: {e}")

                # 3. Actualizar estado acumulado del sensor
                with _lock_estado:
                    if inter not in _estado_sensores:
                        _estado_sensores[inter] = {}
                    clave = "espira" if topico == "espira_inductiva" else topico
                    _estado_sensores[inter][clave] = evento
                    datos = dict(_estado_sensores[inter])

                # 4. Detectar condición de tráfico
                nivel, cola, vel, dens = _determinar_condicion(datos)

                # 5. Enviar congestión a BD principal
                paquete_cong = json.dumps({
                    "tipo": "congestion",
                    "data": {"interseccion": inter, "nivel": nivel,
                             "cola": cola, "velocidad": vel, "densidad": dens}
                })
                try:
                    push_bd_principal.send_string(paquete_cong)
                except Exception as e:
                    print(f"[ANALITICA] No se pudo enviar congestión a PC3: {e}")

                # 6. Decidir acción y enviar comando al controlador de semáforos
                if nivel == "NORMAL":
                    cmd = _hacer_comando_semaforo(inter, "VERDE", "NORMAL", TIEMPO_VERDE_NORMAL)
                    print(f"[ANALITICA][{ts()}] {inter} | NORMAL "
                          f"(Q={cola:.0f}, Vp={vel:.1f}, D={dens:.1f}) "
                          f"→ Verde {TIEMPO_VERDE_NORMAL}s")
                else:
                    cmd = _hacer_comando_semaforo(inter, "VERDE", "CONGESTION", TIEMPO_VERDE_CONGESTION)
                    print(f"[ANALITICA][{ts()}] {inter} | ⚠ CONGESTION "
                          f"(Q={cola:.0f}, Vp={vel:.1f}, D={dens:.1f}) "
                          f"→ Verde extendido {TIEMPO_VERDE_CONGESTION}s")

                push_semaforo.send_string(json.dumps(cmd))

            except zmq.ZMQError:
                pass
            except Exception as e:
                print(f"[ANALITICA] ERROR procesando evento: {e}")
                time.sleep(0.5)


# ── Hilo REP: atiende indicaciones directas de PC3 ───────────────────────────

class HiloIndicacionesREP(threading.Thread):
    """
    Patrón REQ/REP.
    Recibe indicaciones directas del servicio de monitoreo (PC3):
      - PRIORIDAD_EMERGENCIA: ola verde para vehículo de emergencia
      - CAMBIO_SEMAFORO:      cambio manual de semáforo
      - CONSULTA_ESTADO:      devuelve estado actual de semáforos
      - HEARTBEAT:            verificación de disponibilidad
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="Analitica-REP")
        self.ctx = ctx

    def run(self):
        rep = self.ctx.socket(zmq.REP)
        rep.bind(f"tcp://*:{PUERTO_ANALITICA_REP}")

        push_semaforo = self.ctx.socket(zmq.PUSH)
        push_semaforo.connect(f"tcp://127.0.0.1:{PUERTO_SEMAFOROS_PULL}")

        print(f"[ANALITICA-REP][{ts()}] Escuchando indicaciones en :{PUERTO_ANALITICA_REP}")

        while True:
            try:
                msg    = rep.recv_string()
                req    = json.loads(msg)
                accion = req.get("accion", "").upper()
                print(f"[ANALITICA-REP][{ts()}] Indicación recibida: {accion}")

                if accion == "HEARTBEAT":
                    rep.send_string(json.dumps({"status": "OK", "ts": ts()}))

                elif accion == "PRIORIDAD_EMERGENCIA":
                    inter  = req.get("interseccion")
                    tipo_v = req.get("tipo_vehiculo", "ambulancia")
                    guardar_prioridad(DB_REPLICA_PATH, inter, tipo_v, "usuario")
                    cmd = _hacer_comando_semaforo(inter, "VERDE", "PRIORIDAD", TIEMPO_VERDE_PRIORIDAD)
                    push_semaforo.send_string(json.dumps(cmd))
                    print(f"[ANALITICA-REP][{ts()}] PRIORIDAD {tipo_v} en {inter} "
                          f"→ ola verde {TIEMPO_VERDE_PRIORIDAD}s")
                    rep.send_string(json.dumps({"status": "OK", "interseccion": inter}))

                elif accion == "CAMBIO_SEMAFORO":
                    inter  = req.get("interseccion")
                    estado = req.get("estado", "VERDE")
                    cmd    = _hacer_comando_semaforo(inter, estado, "MANUAL", TIEMPO_VERDE_NORMAL)
                    push_semaforo.send_string(json.dumps(cmd))
                    print(f"[ANALITICA-REP][{ts()}] MANUAL: {inter} → {estado}")
                    rep.send_string(json.dumps({"status": "OK", "interseccion": inter, "estado": estado}))

                elif accion == "CONSULTA_ESTADO":
                    semaforos = consultar_todos_semaforos(DB_REPLICA_PATH)
                    rep.send_string(json.dumps({"status": "OK", "semaforos": semaforos}))

                else:
                    rep.send_string(json.dumps({"status": "ERROR", "msg": f"Accion desconocida: {accion}"}))

            except Exception as e:
                print(f"[ANALITICA-REP] ERROR: {e}")
                try:
                    rep.send_string(json.dumps({"status": "ERROR", "msg": str(e)}))
                except Exception:
                    pass


# ── Hilo receptor BD réplica ──────────────────────────────────────────────────

class HiloReplicaBD(threading.Thread):
    """
    Recibe paquetes de datos y los persiste en la BD réplica local.
    Patrón PULL — escucha en PUERTO_BD_REPLICA_PULL.
    """

    def __init__(self, ctx: zmq.Context):
        super().__init__(daemon=True, name="Replica-PULL")
        self.ctx = ctx

    def run(self):
        pull = self.ctx.socket(zmq.PULL)
        pull.bind(f"tcp://*:{PUERTO_BD_REPLICA_PULL}")
        print(f"[BD-REPLICA][{ts()}] Escuchando en :{PUERTO_BD_REPLICA_PULL}")

        while True:
            try:
                msg  = pull.recv_string()
                data = json.loads(msg)
                tipo = data.get("tipo")
                body = data.get("data", {})

                if tipo == "evento_sensor":
                    guardar_evento_sensor(
                        DB_REPLICA_PATH,
                        body.get("sensor_id"), body.get("tipo_sensor"),
                        body.get("interseccion"), body, body.get("timestamp", "")
                    )
                elif tipo == "congestion":
                    guardar_congestion(
                        DB_REPLICA_PATH,
                        body.get("interseccion"), body.get("nivel"),
                        body.get("cola"), body.get("velocidad"), body.get("densidad")
                    )
                elif tipo == "semaforo":
                    actualizar_semaforo(
                        DB_REPLICA_PATH,
                        body.get("interseccion"), body.get("estado"),
                        body.get("modo"), body.get("duracion_seg", 15)
                    )
            except Exception as e:
                print(f"[BD-REPLICA] ERROR: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    inicializar_bd(DB_REPLICA_PATH)

    ctx = zmq.Context()

    HiloSuscriptor(ctx).start()
    HiloIndicacionesREP(ctx).start()
    HiloReplicaBD(ctx).start()

    print(f"[PC2-ANALITICA] Todos los servicios iniciados. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[PC2-ANALITICA] Detenido.")
    finally:
        ctx.term()


if __name__ == "__main__":
    main()
