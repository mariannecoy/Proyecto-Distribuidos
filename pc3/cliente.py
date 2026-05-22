# =============================================================================
# pc3/cliente.py — Consola interactiva de Monitoreo y Control
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Interfaz de usuario para interactuar con el sistema de tráfico en tiempo real.
# Se comunica con el servicio de monitoreo en PC3 mediante el patrón REQ/REP.
#
# Funcionalidades:
#   1. Consultar el estado actual de todos los semáforos.
#   2. Consultar el semáforo de una intersección específica.
#   3. Ver historial de congestión (con filtros de fecha e intersección).
#   4. Ver historial de cambios de semáforo.
#   5. Ver eventos de priorización registrados.
#   6. Forzar prioridad de emergencia (ambulancia, bomberos, etc.).
#   7. Cambio manual de semáforo por el operador.
#   8. Verificar disponibilidad de PC3 (heartbeat).
#
# Uso:
#   python cliente.py
#   python cliente.py <PC3_IP>     # Para especificar IP desde línea de comandos
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import json
from config import PC3_IP, PUERTO_MONITOREO_REP


# =============================================================================
# Funciones de comunicación y presentación
# =============================================================================

def enviar_req(socket: zmq.Socket, cmd: dict) -> dict:
    """
    Serializa y envía un comando al servicio de monitoreo, retorna la respuesta.

    Args:
        socket: Socket REQ ya conectado a PC3.
        cmd:    Diccionario con la acción y parámetros.

    Returns:
        Respuesta del servidor como diccionario.
    """
    socket.send_string(json.dumps(cmd))
    return json.loads(socket.recv_string())


def mostrar_respuesta(resp: dict) -> None:
    """
    Imprime la respuesta del servidor de forma legible en consola.

    Si la respuesta indica error, muestra el mensaje de error.
    Si es una lista, muestra cada elemento en una línea.
    Si es un diccionario individual, lo muestra formateado con indentación.

    Args:
        resp: Diccionario de respuesta del servidor.
    """
    if resp.get("status") != "OK":
        print(f"  ❌ Error: {resp.get('msg', 'Sin detalle')}")
        return

    data = resp.get("data", resp)
    if isinstance(data, list):
        if not data:
            print("  (sin resultados)")
        else:
            for item in data:
                print(f"  → {json.dumps(item, ensure_ascii=False)}")
    elif isinstance(data, dict):
        print(json.dumps(data, ensure_ascii=False, indent=4))
    else:
        print(f"  → {data}")


MENU = """
╔═══════════════════════════════════════════════════════════╗
║      GESTIÓN INTELIGENTE DE TRÁFICO URBANO — PC3          ║
║             Consola de Monitoreo y Control                ║
╠═══════════════════════════════════════════════════════════╣
║  1. Ver estado de TODOS los semáforos                     ║
║  2. Ver semáforo de una intersección específica           ║
║  3. Historial de congestión (filtros opcionales)          ║
║  4. Historial de cambios de semáforo                      ║
║  5. Ver eventos de priorización (emergencias)             ║
║  6. Forzar prioridad de emergencia                        ║
║  7. Cambio manual de semáforo                             ║
║  8. Verificar estado de PC3 (heartbeat)                   ║
║  0. Salir                                                 ║
╚═══════════════════════════════════════════════════════════╝"""


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada de la consola interactiva del usuario.

    Conecta al servicio de monitoreo en PC3 y presenta el menú principal
    en un bucle hasta que el usuario elige salir (opción 0).
    """
    # Permitir sobreescribir la IP de PC3 desde línea de comandos para pruebas
    ip_pc3 = sys.argv[1] if len(sys.argv) > 1 else PC3_IP

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)   # 5 segundos de timeout
    socket.setsockopt(zmq.LINGER, 0)
    addr   = f"tcp://{ip_pc3}:{PUERTO_MONITOREO_REP}"
    socket.connect(addr)
    print(f"[CLIENTE] Conectado al servicio de monitoreo en {addr}")

    while True:
        print(MENU)
        opcion = input("  Opción: ").strip()

        try:
            if opcion == "1":
                # Consulta todos los semáforos registrados en BD principal
                resp = enviar_req(socket, {"accion": "GET_SEMAFOROS"})
                total = len(resp.get("data", []))
                print(f"\n── Estado actual de semáforos ({total} registros) ────")
                mostrar_respuesta(resp)

            elif opcion == "2":
                # Consulta un semáforo específico
                inter = input("  Intersección (ej. INT_A1): ").strip().upper()
                resp  = enviar_req(socket, {"accion": "GET_SEMAFORO",
                                            "interseccion": inter})
                print(f"\n── Semáforo {inter} ──")
                mostrar_respuesta(resp)

            elif opcion == "3":
                # Historial de congestión con filtros opcionales
                print("  Filtros opcionales (Enter para omitir):")
                desde = input("    Desde (YYYY-MM-DDTHH:MM:SSZ): ").strip() or None
                hasta = input("    Hasta (YYYY-MM-DDTHH:MM:SSZ): ").strip() or None
                inter = input("    Intersección: ").strip().upper() or None
                resp  = enviar_req(socket, {
                    "accion":       "GET_CONGESTION",
                    "desde":        desde,
                    "hasta":        hasta,
                    "interseccion": inter
                })
                n = len(resp.get("data", []))
                print(f"\n── Historial de congestión ({n} registros) ──")
                mostrar_respuesta(resp)

            elif opcion == "4":
                # Historial de cambios de semáforo
                inter = input("  Intersección (Enter=todas): ").strip().upper() or None
                resp  = enviar_req(socket, {
                    "accion":       "GET_HISTORIAL",
                    "interseccion": inter,
                    "limite":       50
                })
                n = len(resp.get("data", []))
                print(f"\n── Historial de semáforos ({n} registros) ──")
                mostrar_respuesta(resp)

            elif opcion == "5":
                # Eventos de priorización registrados
                inter = input("  Intersección (Enter=todas): ").strip().upper() or None
                resp  = enviar_req(socket, {"accion": "GET_PRIORIDADES",
                                            "interseccion": inter})
                n = len(resp.get("data", []))
                print(f"\n── Eventos de priorización ({n} registros) ──")
                mostrar_respuesta(resp)

            elif opcion == "6":
                # Indicación directa: ola verde para vehículo de emergencia
                inter  = input("  Intersección objetivo (ej. INT_C3): ").strip().upper()
                tipo_v = input("  Tipo de vehículo [ambulancia]: ").strip() or "ambulancia"
                print(f"\n  Enviando prioridad de emergencia para {tipo_v} en {inter}...")
                resp = enviar_req(socket, {
                    "accion":         "PRIORIDAD_EMERGENCIA",
                    "interseccion":   inter,
                    "tipo_vehiculo":  tipo_v
                })
                print("── Respuesta ──")
                mostrar_respuesta(resp)

            elif opcion == "7":
                # Indicación directa: cambio manual de semáforo
                inter  = input("  Intersección (ej. INT_B2): ").strip().upper()
                estado = input("  Estado (VERDE/ROJO): ").strip().upper()
                if estado not in ("VERDE", "ROJO"):
                    print("  ⚠ Estado inválido. Use VERDE o ROJO.")
                    continue
                resp = enviar_req(socket, {
                    "accion":       "CAMBIO_SEMAFORO",
                    "interseccion": inter,
                    "estado":       estado
                })
                print("── Respuesta ──")
                mostrar_respuesta(resp)

            elif opcion == "8":
                # Heartbeat: verificar que PC3 esté activo y respondiendo
                resp   = enviar_req(socket, {"accion": "HEARTBEAT"})
                estado = "✅ ACTIVO" if resp.get("status") == "OK" else "❌ SIN RESPUESTA"
                print(f"\n  PC3: {estado}")
                if resp.get("ts"):
                    print(f"  Timestamp PC3: {resp['ts']}")

            elif opcion == "0":
                print("  Cerrando consola. ¡Hasta luego!")
                break

            else:
                print("  ⚠ Opción no válida. Ingrese un número del 0 al 8.")

        except zmq.Again:
            print("  ❌ Timeout: PC3 no responde. Verifique que monitoreo.py esté activo.")
        except Exception as exc:
            print(f"  ❌ Error inesperado: {exc}")

    socket.close()
    ctx.term()


if __name__ == "__main__":
    main()
