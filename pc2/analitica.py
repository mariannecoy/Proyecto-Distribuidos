# =============================================================================
# pc2/analitica.py — Servicio de Analítica (PC2)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Núcleo de procesamiento del sistema. Responsabilidades:
#   1. Suscribirse a eventos del broker PC1 (PUB/SUB).
#   2. Procesar eventos y detectar congestión con reglas simples.
#   3. Persistir datos en BD principal (PC3) Y réplica (PC2) → PUSH/PULL.
#      Si PC3 falla, el failover activa la réplica automáticamente.
#   4. Enviar comandos de cambio de semáforo al controlador → PUSH/PULL.
#   5. Recibir indicaciones directas del monitoreo de PC3 → REQ/REP.
#   6. Imprimir por pantalla el estado del tráfico y las acciones tomadas.
#
# Reglas de tráfico implementadas:
#   NORMAL:     Q < 5  AND Vp > 35 AND D < 20  → verde 15s
#   CONGESTION: Q >= 5 OR  Vp <= 35 OR D >= 20  → verde extendido 30s
#   PRIORIDAD:  Forzada por usuario              → ola verde 60s
#
# Uso: python analitica.py
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

# Importar el módulo de failover para conmutación automática de BD
from failover import HiloHeartbeat, estado_pc3, get_db_activa


def _ts() -> str:
    """Retorna la hora UTC actual formateada para logs (HH:MM:SS)."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Estado acumulado de sensores por intersección
# =============================================================================

# Almacena la última lectura de cada tipo de sensor por intersección.
# Clave: código de intersección. Valor: dict con tipo → evento.
_estado_sensores: dict = {}
_lock_estado = threading.Lock()


# =============================================================================
# Lógica de reglas de tráfico
# =============================================================================

def _determinar_condicion(datos_inter: dict) -> tuple[str, float, float, float]:
    """
    Aplica las reglas de tráfico sobre los datos acumulados de una intersección.

    Reglas del grupo:
        NORMAL:     Q < 5  AND Vp > 35 AND D < 20
        CONGESTION: Q >= 5 OR  Vp <= 35 OR D >= 20

    Args:
        datos_inter: Dict con últimas lecturas por tipo de sensor
                     (keys: "camara", "gps", "espira").

    Returns:
        Tupla (nivel, cola, velocidad, densidad).
        nivel: "NORMAL" o "CONGESTION".
    """
    cam = datos_inter.get("camara", {})
    gps = datos_inter.get("gps", {})

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


def _construir_comando(interseccion: str, estado: str,
                       modo: str, duracion: int) -> dict:
    """
    Construye el diccionario de comando para el controlador de semáforos.

    Args:
        interseccion: Código de la intersección objetivo.
        estado:       "VERDE" o "ROJO".
        modo:         "NORMAL", "CONGESTION", "PRIORIDAD" o "MANUAL".
        duracion:     Duración en segundos de la fase verde.

    Returns:
        Diccionario con todos los campos del comando de semáforo.
    """
    return {
        "interseccion": interseccion,
        "estado":       estado,
        "modo":         modo,
        "duracion_seg": duracion,
        "timestamp":    datetime.now(timezone.utc).isoformat()
    }


# =============================================================================
# Funciones auxiliares de envío a BDs (con soporte de failover)
# =============================================================================

def _enviar_a_bd_principal(push_socket: zmq.Socket, paquete: dict) -> None:
    """
    Envía un paquete a la BD principal en PC3.
    Si PC3 está caído (detectado por failover), la operación se omite con log.

    Args:
        push_socket: Socket PUSH conectado a PC3.
        paquete:     Diccionario con "tipo" y "data" a persistir.
    """
    if not estado_pc3.disponible:
        print(f"[ANALITICA][{_ts()}] ⚠  PC3 CAÍDO — omitiendo envío a BD principal")
        return
    try:
        push_socket.send_string(json.dumps(paquete), zmq.NOBLOCK)
    except zmq.Again:
        print(f"[ANALITICA][{_ts()}] BD principal saturada, mensaje descartado")
    except Exception as exc:
        print(f"[ANALITICA][{_ts()}] Error enviando a PC3: {exc}")


def _guardar_en_replica(datos: dict, tipo: str) -> None:
    """
    Persiste un registro directamente en la BD réplica (PC2) de forma local.
    Este método no usa ZeroMQ sino acceso directo a SQLite para la réplica local.

    Args:
        datos: Diccionario con los datos del evento o condición.
        tipo:  "evento_sensor", "congestion", "semaforo" o "prioridad".
    """
    try:
        if tipo == "evento_sensor":
            guardar_evento_sensor(
                DB_REPLICA_PATH,
                datos.get("sensor_id"), datos.get("tipo_sensor"),
                datos.get("interseccion"), datos, datos.get("timestamp", "")
            )
        elif tipo == "congestion":
            guardar_congestion(
                DB_REPLICA_PATH,
                datos.get("interseccion"), datos.get("nivel"),
                datos.get("cola"), datos.get("velocidad"), datos.get("densidad")
            )
        elif tipo == "semaforo":
            actualizar_semaforo(
                DB_REPLICA_PATH,
                datos.get("interseccion"), datos.get("estado"),
                datos.get("modo"), datos.get("duracion_seg", 15)
            )
        elif tipo == "prioridad":
            guardar_prioridad(
                DB_REPLICA_PATH,
                datos.get("interseccion"), datos.get("tipo_vehiculo", "ambulancia"),
                datos.get("solicitado_por", "sistema")
            )
    except Exception as exc:
        print(f"[ANALITICA][{_ts()}] Error en BD réplica local ({tipo}): {exc}")


# =============================================================================
# Hilo 1: Suscriptor al broker — procesamiento central de eventos
# =============================================================================

class HiloSuscriptor(threading.Thread):
    """
    Se suscribe al broker de PC1 (PUB/SUB) y procesa cada evento entrante.

    Por cada evento recibido:
        1. Persiste en BD réplica local (PC2).
        2. Reenvía a BD principal en PC3 (si está disponible).
        3. Actualiza el estado acumulado del sensor.
        4. Aplica reglas y detecta condición de tráfico.
        5. Emite comando de semáforo al servicio de control.
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido con el proceso principal.
        """
        super().__init__(daemon=True, name="Analitica-SUB")
        self.ctx = ctx

    def run(self):
        """
        Bucle principal del hilo de analítica de sensores.

        Configura tres sockets ZeroMQ:
            - SUB hacia el broker en PC1 (recibe eventos de sensores).
            - PUSH hacia la BD principal en PC3 (persistencia maestra).
            - PUSH hacia el controlador de semáforos local en PC2.

        Por cada mensaje recibido:
            1. Identifica el tópico (camara, espira_inductiva o gps).
            2. Persiste el evento en la BD principal vía PUSH a PC3
               (con failover automático a la BD réplica si PC3 está caído).
            3. Actualiza el estado acumulado del sensor en memoria.
            4. Aplica las reglas de tráfico (umbrales de cola, velocidad,
               densidad) para clasificar la condición de la intersección.
            5. Si detecta congestión, emite un comando hacia el controlador
               de semáforos para extender la fase verde.

        El bucle es infinito y solo se rompe al cerrar el proceso.
        Es daemon, por lo que termina automáticamente con el main.
        """
        # ── Conectar sockets ──────────────────────────────────────────────────
        sub = self.ctx.socket(zmq.SUB)
        sub.connect(f"tcp://{PC1_IP}:{PUERTO_BROKER_A_PC2}")
        # Suscribirse a los tres tópicos de sensores
        for topico in ("camara", "espira_inductiva", "gps"):
            sub.setsockopt_string(zmq.SUBSCRIBE, topico)

        # PUSH a BD principal en PC3
        push_bd_principal = self.ctx.socket(zmq.PUSH)
        push_bd_principal.connect(f"tcp://{PC3_IP}:{PUERTO_BD_PRINCIPAL_PULL}")

        # PUSH al controlador de semáforos (local en PC2)
        push_semaforo = self.ctx.socket(zmq.PUSH)
        push_semaforo.connect(f"tcp://127.0.0.1:{PUERTO_SEMAFOROS_PULL}")

        print(f"[ANALITICA][{_ts()}] Suscrito al broker {PC1_IP}:{PUERTO_BROKER_A_PC2}")

        while True:
            try:
                msg    = sub.recv_string()
                partes = msg.split(" ", 1)
                if len(partes) < 2:
                    continue
                topico, payload = partes
                evento = json.loads(payload)
                inter  = evento.get("interseccion", "DESCONOCIDA")

                # 1. Guardar en BD réplica local
                _guardar_en_replica(evento, "evento_sensor")

                # 2. Reenviar a BD principal en PC3 (con failover automático)
                _enviar_a_bd_principal(push_bd_principal,
                                       {"tipo": "evento_sensor", "data": evento})

                # 3. Actualizar estado acumulado de sensores para esta intersección
                with _lock_estado:
                    if inter not in _estado_sensores:
                        _estado_sensores[inter] = {}
                    # Normalizar clave "espira_inductiva" → "espira" para el dict
                    clave = "espira" if topico == "espira_inductiva" else topico
                    _estado_sensores[inter][clave] = evento
                    datos = dict(_estado_sensores[inter])

                # 4. Detectar condición de tráfico aplicando reglas
                nivel, cola, vel, dens = _determinar_condicion(datos)

                # 5. Guardar condición en ambas BDs
                datos_cong = {
                    "interseccion": inter, "nivel": nivel,
                    "cola": cola,          "velocidad": vel, "densidad": dens
                }
                _guardar_en_replica(datos_cong, "congestion")
                _enviar_a_bd_principal(push_bd_principal,
                                       {"tipo": "congestion", "data": datos_cong})

                # 6. Decidir acción de semáforo y emitir comando
                if nivel == "NORMAL":
                    cmd = _construir_comando(inter, "VERDE", "NORMAL", TIEMPO_VERDE_NORMAL)
                    print(f"[ANALITICA][{_ts()}] {inter} NORMAL "
                          f"(Q={cola:.0f}, Vp={vel:.1f}, D={dens:.1f}) "
                          f"→ 🟢 Verde {TIEMPO_VERDE_NORMAL}s")
                else:
                    cmd = _construir_comando(inter, "VERDE", "CONGESTION", TIEMPO_VERDE_CONGESTION)
                    print(f"[ANALITICA][{_ts()}] {inter} ⚠ CONGESTION "
                          f"(Q={cola:.0f}, Vp={vel:.1f}, D={dens:.1f}) "
                          f"→ 🟢 Verde extendido {TIEMPO_VERDE_CONGESTION}s")

                push_semaforo.send_string(json.dumps(cmd))

            except zmq.ZMQError:
                pass
            except Exception as exc:
                print(f"[ANALITICA] ERROR procesando evento: {exc}")
                time.sleep(0.5)


# =============================================================================
# Hilo 2: REP — atiende indicaciones directas de PC3 (usuario u operador)
# =============================================================================

class HiloIndicacionesREP(threading.Thread):
    """
    Patrón REQ/REP. Escucha indicaciones directas del servicio de monitoreo.

    Acciones soportadas:
        HEARTBEAT:           Verificación de disponibilidad de la analítica.
        PRIORIDAD_EMERGENCIA:Ola verde para paso de vehículo de emergencia.
        CAMBIO_SEMAFORO:     Cambio manual de semáforo por operador.
        CONSULTA_ESTADO:     Devuelve el estado actual de todos los semáforos.
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido.
        """
        super().__init__(daemon=True, name="Analitica-REP")
        self.ctx = ctx

    def run(self):
        """
        Bucle principal del hilo REP de indicaciones directas.

        Atiende solicitudes síncronas (REQ/REP) provenientes del servicio
        de monitoreo en PC3, que a su vez recibe peticiones del usuario u
        operador a través del cliente interactivo.

        Acciones soportadas:
            HEARTBEAT:            Responde con OK + timestamp para confirmar
                                  que la analítica está viva.
            PRIORIDAD_EMERGENCIA: Crea una ola verde para vehículos de
                                  emergencia; emite cambio de semáforo con
                                  duración extendida (TIEMPO_VERDE_PRIORIDAD).
            CAMBIO_SEMAFORO:      Cambio manual ordenado por el operador.
            CONSULTA_ESTADO:      Retorna el estado actual de todos los
                                  semáforos en formato JSON.

        Cada solicitud genera una respuesta JSON con campo "status" (OK/ERROR)
        y, cuando aplica, un campo "data" con la información solicitada.
        El bucle es infinito y daemon.
        """
        rep = self.ctx.socket(zmq.REP)
        rep.bind(f"tcp://*:{PUERTO_ANALITICA_REP}")

        push_semaforo = self.ctx.socket(zmq.PUSH)
        push_semaforo.connect(f"tcp://127.0.0.1:{PUERTO_SEMAFOROS_PULL}")

        print(f"[ANALITICA-REP][{_ts()}] Escuchando indicaciones en :{PUERTO_ANALITICA_REP}")

        while True:
            try:
                msg    = rep.recv_string()
                req    = json.loads(msg)
                accion = req.get("accion", "").upper()
                print(f"[ANALITICA-REP][{_ts()}] Indicación recibida: {accion}")

                if accion == "HEARTBEAT":
                    rep.send_string(json.dumps({"status": "OK", "ts": _ts()}))

                elif accion == "PRIORIDAD_EMERGENCIA":
                    inter  = req.get("interseccion")
                    tipo_v = req.get("tipo_vehiculo", "ambulancia")
                    # Guardar en réplica y en BD activa según failover
                    _guardar_en_replica(
                        {"interseccion": inter, "tipo_vehiculo": tipo_v,
                         "solicitado_por": "usuario"}, "prioridad"
                    )
                    cmd = _construir_comando(inter, "VERDE", "PRIORIDAD", TIEMPO_VERDE_PRIORIDAD)
                    push_semaforo.send_string(json.dumps(cmd))
                    print(f"[ANALITICA-REP][{_ts()}] 🚨 PRIORIDAD {tipo_v} en {inter} "
                          f"→ ola verde {TIEMPO_VERDE_PRIORIDAD}s")
                    rep.send_string(json.dumps({"status": "OK", "interseccion": inter}))

                elif accion == "CAMBIO_SEMAFORO":
                    inter  = req.get("interseccion")
                    estado = req.get("estado", "VERDE").upper()
                    cmd    = _construir_comando(inter, estado, "MANUAL", TIEMPO_VERDE_NORMAL)
                    push_semaforo.send_string(json.dumps(cmd))
                    print(f"[ANALITICA-REP][{_ts()}] 🔧 MANUAL: {inter} → {estado}")
                    rep.send_string(json.dumps({"status": "OK",
                                                "interseccion": inter,
                                                "estado": estado}))

                elif accion == "CONSULTA_ESTADO":
                    semaforos = consultar_todos_semaforos(DB_REPLICA_PATH)
                    rep.send_string(json.dumps({"status": "OK", "semaforos": semaforos}))

                else:
                    rep.send_string(json.dumps({
                        "status": "ERROR",
                        "msg":    f"Acción desconocida: {accion}"
                    }))

            except Exception as exc:
                print(f"[ANALITICA-REP] ERROR: {exc}")
                try:
                    rep.send_string(json.dumps({"status": "ERROR", "msg": str(exc)}))
                except Exception:
                    pass


# =============================================================================
# Hilo 3: PULL — receptor de la BD réplica (backup de escrituras)
# =============================================================================

class HiloReplicaBD(threading.Thread):
    """
    Recibe paquetes de datos vía PUSH/PULL y los persiste en la BD réplica local.

    Este hilo garantiza que la réplica en PC2 esté siempre actualizada,
    incluso cuando PC3 está disponible. Sirve como respaldo para failover.
    """

    def __init__(self, ctx: zmq.Context):
        """
        Args:
            ctx: Contexto ZeroMQ compartido.
        """
        super().__init__(daemon=True, name="Replica-PULL")
        self.ctx = ctx

    def run(self):
        """
        Bucle principal del hilo de réplica local.

        Recibe mensajes vía PUSH/PULL desde otros componentes del sistema
        (analítica y semáforos) y los persiste en la base de datos réplica
        local de PC2. Este hilo es esencial para el mecanismo de failover:
        cuando PC3 está disponible, la réplica se actualiza en paralelo y
        sirve como respaldo; cuando PC3 cae, los demás componentes redirigen
        sus escrituras directamente aquí.

        Tipos de mensaje atendidos:
            "evento_sensor": Lecturas de los sensores (cámara, espira, GPS).
            "congestion":    Eventos de condición de tráfico detectada.
            "semaforo":      Cambios de estado de semáforos.

        Cada mensaje se deserializa de JSON y se delega a la función
        correspondiente del módulo database. Los errores de persistencia
        se registran por consola pero no detienen el hilo.
        """
        pull = self.ctx.socket(zmq.PULL)
        pull.bind(f"tcp://*:{PUERTO_BD_REPLICA_PULL}")
        print(f"[BD-REPLICA][{_ts()}] Escuchando en :{PUERTO_BD_REPLICA_PULL}")

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
            except Exception as exc:
                print(f"[BD-REPLICA] ERROR: {exc}")


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada del servicio de analítica (PC2).

    Inicializa la BD réplica, lanza el Heartbeat hacia PC3 y arranca
    los tres hilos de servicio: suscriptor, REP y receptor de réplica.
    """
    inicializar_bd(DB_REPLICA_PATH)

    ctx = zmq.Context()

    # Arrancar monitor de heartbeat para detectar fallos de PC3
    HiloHeartbeat(ctx).start()

    # Registrar callback que informa del cambio de BD activa
    estado_pc3.registrar_callback(
        lambda ok: print(
            f"[ANALITICA][{_ts()}] 🔄 Failover → BD activa: "
            f"{'PRINCIPAL' if ok else 'RÉPLICA'} ({get_db_activa()})"
        )
    )

    # Lanzar los tres hilos de servicio
    HiloSuscriptor(ctx).start()
    HiloIndicacionesREP(ctx).start()
    HiloReplicaBD(ctx).start()

    print(f"[PC2-ANALITICA][{_ts()}] Todos los servicios iniciados. Ctrl+C para detener.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[PC2-ANALITICA][{_ts()}] Deteniendo analítica...")
    finally:
        ctx.term()
        print(f"[PC2-ANALITICA][{_ts()}] Apagado completo.")


if __name__ == "__main__":
    main()
