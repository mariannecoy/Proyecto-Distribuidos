# =============================================================================
# pc3/cliente.py — Consola interactiva de monitoreo y control
#
# Permite al usuario:
#   - Consultar el estado actual de semáforos
#   - Ver historial de congestión (por período o intersección)
#   - Ver historial de cambios de semáforo
#   - Ver eventos de priorización registrados
#   - Forzar prioridad para vehículo de emergencia (ambulancia, etc.)
#   - Cambiar manualmente el estado de un semáforo
#
# Comunica con PC3 usando el patrón REQ/REP.
#
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import json
from config import PC3_IP, PUERTO_MONITOREO_REP


def enviar_req(socket, cmd: dict) -> dict:
    """Envía un comando y retorna la respuesta."""
    socket.send_string(json.dumps(cmd))
    return json.loads(socket.recv_string())


def mostrar_respuesta(resp: dict):
    """Imprime la respuesta de forma legible."""
    if resp.get("status") != "OK":
        print(f" Error: {resp.get('msg')}")
        return
    data = resp.get("data", resp)
    if isinstance(data, list):
        if not data:
            print("  (sin resultados)")
        for item in data:
            print(f"  → {json.dumps(item, ensure_ascii=False)}")
    elif isinstance(data, dict):
        print(f"  → {json.dumps(data, ensure_ascii=False, indent=2)}")
    else:
        print(f"  → {data}")


MENU = """
╔═══════════════════════════════════════════════════════╗
║    GESTIÓN INTELIGENTE DE TRÁFICO URBANO — PC3        ║
║              Consola de Monitoreo y Control           ║
╠═══════════════════════════════════════════════════════╣
║  1. Ver estado de TODOS los semáforos                 ║
║  2. Ver semáforo de una intersección específica       ║
║  3. Historial de congestión (filtros opcionales)      ║
║  4. Historial de cambios de semáforo                  ║
║  5. Ver eventos de priorización (emergencias)         ║
║  6. Forzar prioridad de emergencia                    ║
║  7. Cambio manual de semáforo                         ║
║  8. Verificar estado de PC3 (heartbeat)               ║
║  0. Salir                                             ║
╚═══════════════════════════════════════════════════════╝"""


def main():
    ctx    = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    addr   = f"tcp://{PC3_IP}:{PUERTO_MONITOREO_REP}"
    socket.connect(addr)
    print(f"[CLIENTE] Conectado a PC3 en {addr}")

    while True:
        print(MENU)
        opcion = input("  Opción: ").strip()

        try:
            if opcion == "1":
                resp = enviar_req(socket, {"accion": "GET_SEMAFOROS"})
                print("\n── Estado actual de semáforos ──────────────────────────")
                mostrar_respuesta(resp)

            elif opcion == "2":
                inter = input("  Intersección (ej. INT_A1): ").strip().upper()
                resp  = enviar_req(socket, {"accion": "GET_SEMAFORO", "interseccion": inter})
                mostrar_respuesta(resp)

            elif opcion == "3":
                print("  Filtros (Enter para omitir):")
                desde = input("    Desde (YYYY-MM-DDTHH:MM:SSZ): ").strip() or None
                hasta = input("    Hasta (YYYY-MM-DDTHH:MM:SSZ): ").strip() or None
                inter = input("    Intersección: ").strip().upper() or None
                resp  = enviar_req(socket, {
                    "accion": "GET_CONGESTION",
                    "desde": desde, "hasta": hasta, "interseccion": inter
                })
                registros = resp.get("data", [])
                print(f"\n── Historial de congestión ({len(registros)} registros) ──")
                mostrar_respuesta(resp)

            elif opcion == "4":
                inter = input("  Intersección (Enter=todas): ").strip().upper() or None
                resp  = enviar_req(socket, {
                    "accion": "GET_HISTORIAL",
                    "interseccion": inter, "limite": 50
                })
                print(f"\n── Historial de semáforos ──")
                mostrar_respuesta(resp)

            elif opcion == "5":
                inter = input("  Intersección (Enter=todas): ").strip().upper() or None
                resp  = enviar_req(socket, {"accion": "GET_PRIORIDADES", "interseccion": inter})
                print(f"\n── Eventos de priorización ──")
                mostrar_respuesta(resp)

            elif opcion == "6":
                inter  = input("  Intersección (ej. INT_C3): ").strip().upper()
                tipo_v = input("  Tipo de vehículo [ambulancia]: ").strip() or "ambulancia"
                resp   = enviar_req(socket, {
                    "accion": "PRIORIDAD_EMERGENCIA",
                    "interseccion": inter, "tipo_vehiculo": tipo_v
                })
                print(f"\n── Respuesta ──")
                mostrar_respuesta(resp)

            elif opcion == "7":
                inter  = input("  Intersección (ej. INT_B2): ").strip().upper()
                estado = input("  Estado (VERDE/ROJO): ").strip().upper()
                if estado not in ("VERDE", "ROJO"):
                    print("  ⚠ Estado inválido. Use VERDE o ROJO.")
                    continue
                resp = enviar_req(socket, {
                    "accion": "CAMBIO_SEMAFORO",
                    "interseccion": inter, "estado": estado
                })
                mostrar_respuesta(resp)

            elif opcion == "8":
                resp = enviar_req(socket, {"accion": "HEARTBEAT"})
                estado = "ACTIVO" if resp.get("status") == "OK" else "SIN RESPUESTA"
                print(f"  PC3 {estado} | {resp}")

            elif opcion == "0":
                print("  Cerrando consola...")
                break

            else:
                print("  Opción no válida.")

        except zmq.Again:
            print(" Timeout: PC3 no responde. Verifique que el servicio esté activo.")
        except Exception as e:
            print(f"   Error: {e}")

    socket.close()
    ctx.term()


if __name__ == "__main__":
    main()
