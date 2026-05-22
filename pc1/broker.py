# =============================================================================
# pc1/broker.py — Broker ZeroMQ (PC1)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Actúa como intermediario entre los sensores de tráfico (PC1) y el servicio
# de analítica (PC2). Desacopla productores y consumidores de mensajes.
#
# Flujo de datos:
#   Sensores → [PUSH/PULL] → Broker → [PUB/SUB] → Analítica PC2
#
# Dos modos de operación:
#   - simple:     Un hilo, un socket PULL y un socket PUB. Diseño original.
#   - multihilo:  Pool de N workers internos que procesan en paralelo.
#                 Diseño modificado para pruebas de desempeño (Tabla 1).
#
# Uso:
#   python broker.py [simple|multihilo] [num_workers]
#
# Ejemplos:
#   python broker.py simple            # modo simple, diseño original
#   python broker.py multihilo         # modo multihilo, 4 workers (default)
#   python broker.py multihilo 8       # modo multihilo, 8 workers
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import threading
import time
from datetime import datetime, timezone
from config import PUERTO_SENSORES_A_BROKER, PUERTO_BROKER_A_PC2


def _ts() -> str:
    """Retorna la hora UTC actual formateada (HH:MM:SS) para logs."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# =============================================================================
# Contador de mensajes para métricas de rendimiento
# =============================================================================

_lock_contador  = threading.Lock()
_total_mensajes = 0
_t_inicio       = time.time()


def _registrar_mensaje() -> None:
    """Incrementa el contador global de mensajes procesados (thread-safe)."""
    global _total_mensajes
    with _lock_contador:
        _total_mensajes += 1


def _hilo_estadisticas() -> None:
    """
    Hilo daemon que imprime el throughput del broker cada 15 segundos.
    Sirve como monitor de rendimiento en tiempo real.
    """
    while True:
        time.sleep(15)
        with _lock_contador:
            elapsed = time.time() - _t_inicio
            tps = _total_mensajes / elapsed if elapsed > 0 else 0
            print(f"[BROKER][{_ts()}] 📊 Mensajes totales: {_total_mensajes} "
                  f"| Throughput: {tps:.2f} msg/s")


# =============================================================================
# Modo simple — diseño original (un solo hilo)
# =============================================================================

def modo_simple(ctx: zmq.Context) -> None:
    """
    Broker básico de un solo hilo.

    Patrón: PULL ← sensores → reenvía → PUB → PC2 analítica.
    Este es el diseño base descrito en el enunciado del proyecto.

    Ventajas:  Simple, sin condiciones de carrera.
    Desventaja: No escala bien con muchos sensores (cuello de botella).

    Args:
        ctx: Contexto ZeroMQ del proceso.
    """
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SENSORES_A_BROKER}")

    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{PUERTO_BROKER_A_PC2}")

    print(f"[BROKER-SIMPLE][{_ts()}] PULL en :{PUERTO_SENSORES_A_BROKER} "
          f"| PUB en :{PUERTO_BROKER_A_PC2}")
    threading.Thread(target=_hilo_estadisticas, daemon=True).start()

    while True:
        try:
            mensaje = pull.recv_string()
            pub.send_string(mensaje)
            _registrar_mensaje()
            topico = mensaje.split(" ", 1)[0]
            print(f"[BROKER-SIMPLE][{_ts()}] → {topico}")
        except zmq.ZMQError as exc:
            print(f"[BROKER-SIMPLE] Error ZMQ: {exc}")
            break


# =============================================================================
# Modo multihilo — diseño modificado para experimentos de rendimiento
# =============================================================================

class WorkerBroker(threading.Thread):
    """
    Worker individual dentro del pool multihilo del broker.

    Lee mensajes de la cola interna (inproc DEALER) y los publica
    al socket PUB compartido. El mutex lock_pub garantiza acceso exclusivo
    al socket PUB ya que ZeroMQ no es thread-safe a nivel de socket.

    Attributes:
        ctx (zmq.Context): Contexto ZeroMQ compartido.
        pub (zmq.Socket):  Socket PUB compartido (con lock).
        lock (threading.Lock): Mutex para acceso al socket PUB.
        wid (int): Identificador del worker para trazabilidad en logs.
    """

    def __init__(self, ctx: zmq.Context, pub: zmq.Socket,
                 lock_pub: threading.Lock, wid: int):
        """
        Args:
            ctx:      Contexto ZeroMQ.
            pub:      Socket PUB compartido.
            lock_pub: Lock para acceso exclusivo al socket PUB.
            wid:      Número de identificación del worker.
        """
        super().__init__(daemon=True, name=f"BrokerWorker-{wid}")
        self.ctx  = ctx
        self.pub  = pub
        self.lock = lock_pub
        self.wid  = wid

    def run(self):
        """
        Bucle del worker: recibe de la cola interna y publica al exterior.
        Usa socket DEALER para consumir del distribuidor interno.
        """
        dealer = self.ctx.socket(zmq.DEALER)
        dealer.connect("inproc://broker_workers")
        print(f"[BROKER-WORKER-{self.wid}][{_ts()}] Listo")

        while True:
            try:
                frames  = dealer.recv_multipart()
                mensaje = frames[-1].decode()

                # Publicar al socket PUB con exclusión mutua
                with self.lock:
                    self.pub.send_string(mensaje)

                _registrar_mensaje()
                topico = mensaje.split(" ", 1)[0]
                print(f"[BROKER-W{self.wid}][{_ts()}] → {topico}")

            except Exception as exc:
                print(f"[BROKER-WORKER-{self.wid}] ERROR: {exc}")


def modo_multihilo(ctx: zmq.Context, num_workers: int = 4) -> None:
    """
    Broker con pool de N workers para mayor throughput (diseño modificado).

    Arquitectura interna:
        PULL (externo) → Distribuidor DEALER → Pool de Workers DEALER → PUB (externo)

    Ventaja: paraleliza el procesamiento de mensajes, lo que mejora el
    throughput bajo alta carga de sensores (experimento Tabla 1, Escenario B).

    Args:
        ctx:         Contexto ZeroMQ del proceso.
        num_workers: Número de hilos workers a lanzar (default 4).
    """
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SENSORES_A_BROKER}")

    pub      = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{PUERTO_BROKER_A_PC2}")
    lock_pub = threading.Lock()

    # Socket ROUTER interno que distribuye trabajo a los workers
    router = ctx.socket(zmq.ROUTER)
    router.bind("inproc://broker_workers")

    print(f"[BROKER-MULTIHILO][{_ts()}] "
          f"PULL en :{PUERTO_SENSORES_A_BROKER} "
          f"| PUB en :{PUERTO_BROKER_A_PC2} "
          f"| workers={num_workers}")

    # Arrancar el pool de workers
    for i in range(num_workers):
        WorkerBroker(ctx, pub, lock_pub, i).start()

    threading.Thread(target=_hilo_estadisticas, daemon=True).start()

    # Distribuidor: recibe del PULL externo y pasa al pool interno via DEALER
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect("inproc://broker_workers")

    while True:
        try:
            mensaje = pull.recv_string()
            dealer.send_string(mensaje)
        except zmq.ZMQError as exc:
            print(f"[BROKER-MULTIHILO] Error ZMQ: {exc}")
            break


# =============================================================================
# Main
# =============================================================================

def main():
    """
    Punto de entrada del Broker (PC1).

    Lee el modo de operación y número de workers de los argumentos de línea
    de comandos, luego inicia el broker en el modo indicado.
    """
    modo        = sys.argv[1] if len(sys.argv) > 1 else "simple"
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    ctx = zmq.Context()
    print(f"[PC1-BROKER][{_ts()}] Iniciando en modo: {modo.upper()}")

    try:
        if modo == "multihilo":
            modo_multihilo(ctx, num_workers)
        else:
            modo_simple(ctx)
    except KeyboardInterrupt:
        print(f"[PC1-BROKER][{_ts()}] Detenido por el usuario.")
    finally:
        ctx.term()
        print(f"[PC1-BROKER][{_ts()}] Contexto ZMQ cerrado.")


if __name__ == "__main__":
    main()
