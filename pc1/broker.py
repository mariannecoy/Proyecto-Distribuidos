# =============================================================================
# pc1/broker.py — Broker ZeroMQ (PC1)
#
# Actúa como intermediario entre los sensores y PC2:
#   - Recibe eventos de los sensores mediante patrón PULL
#   - Los reenvía a PC2 (analítica) mediante patrón PUB
#
# Dos modos de operación 
#   - simple:     un solo hilo, diseño original 
#   - multihilo:  grupo de workers para mayor rendimiento
#
# Uso: python broker.py [simple|multihilo] [num_workers]
#
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import zmq
import threading
import time
from config import PUERTO_SENSORES_A_BROKER, PUERTO_BROKER_A_PC2

# ── Contador global de mensajes para métricas ─────────────────────────────────
_lock_contador  = threading.Lock()
_total_mensajes = 0
_t_inicio       = time.time()


def _registrar_mensaje():
    global _total_mensajes
    with _lock_contador:
        _total_mensajes += 1


def _hilo_estadisticas():
    """Imprime throughput cada 15 segundos."""
    while True:
        time.sleep(15)
        with _lock_contador:
            elapsed = time.time() - _t_inicio
            tps = _total_mensajes / elapsed if elapsed > 0 else 0
            print(f"[BROKER] Mensajes totales: {_total_mensajes} | "
                  f"Throughput: {tps:.2f} msg/seg")


# ── Modo simple  ───────────────────────────────

def modo_simple(ctx: zmq.Context):
    """
    Broker básico de un solo hilo.
    PULL ← sensores | PUB → PC2 analítica
    """
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SENSORES_A_BROKER}")

    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{PUERTO_BROKER_A_PC2}")

    print(f"[BROKER-SIMPLE] PULL en :{PUERTO_SENSORES_A_BROKER} | "
          f"PUB en :{PUERTO_BROKER_A_PC2}")
    threading.Thread(target=_hilo_estadisticas, daemon=True).start()

    while True:
        try:
            mensaje = pull.recv_string()
            pub.send_string(mensaje)
            _registrar_mensaje()
            topico = mensaje.split(" ", 1)[0]
            print(f"[BROKER-SIMPLE] Reenviado → tópico: {topico}")
        except zmq.ZMQError as e:
            print(f"[BROKER-SIMPLE] Error ZMQ: {e}")
            break


# ── Modo multihilo (diseño modificado para métricas) ─────────────────────────

class WorkerBroker(threading.Thread):
    """
    Worker individual del pool multihilo.
    Lee del socket inproc y publica al socket PUB compartido.
    """

    def __init__(self, ctx: zmq.Context, pub: zmq.Socket, lock_pub: threading.Lock, wid: int):
        super().__init__(daemon=True, name=f"BrokerWorker-{wid}")
        self.ctx     = ctx
        self.pub     = pub
        self.lock    = lock_pub
        self.wid     = wid

    def run(self):
        dealer = self.ctx.socket(zmq.DEALER)
        dealer.connect("inproc://broker_workers")
        print(f"[BROKER-WORKER-{self.wid}] Listo")
        while True:
            try:
                frames  = dealer.recv_multipart()
                mensaje = frames[-1].decode()
                with self.lock:
                    self.pub.send_string(mensaje)
                _registrar_mensaje()
                topico = mensaje.split(" ", 1)[0]
                print(f"[BROKER-W{self.wid}] Reenviado → {topico}")
            except Exception as e:
                print(f"[BROKER-WORKER-{self.wid}] ERROR: {e}")


def modo_multihilo(ctx: zmq.Context, num_workers: int = 4):
    """
    Broker con pool de N workers para mayor throughput.
    Usa ROUTER/DEALER internamente para distribuir trabajo.
    """
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{PUERTO_SENSORES_A_BROKER}")

    pub      = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{PUERTO_BROKER_A_PC2}")
    lock_pub = threading.Lock()

    router = ctx.socket(zmq.ROUTER)
    router.bind("inproc://broker_workers")

    print(f"[BROKER-MULTIHILO] PULL en :{PUERTO_SENSORES_A_BROKER} | "
          f"PUB en :{PUERTO_BROKER_A_PC2} | workers={num_workers}")

    # Arrancar workers
    for i in range(num_workers):
        WorkerBroker(ctx, pub, lock_pub, i).start()

    threading.Thread(target=_hilo_estadisticas, daemon=True).start()

    # Distribuidor: PULL → inproc DEALER
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect("inproc://broker_workers")

    while True:
        try:
            mensaje = pull.recv_string()
            dealer.send_string(mensaje)
        except zmq.ZMQError as e:
            print(f"[BROKER-MULTIHILO] Error ZMQ: {e}")
            break


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    modo        = sys.argv[1] if len(sys.argv) > 1 else "simple"
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    ctx = zmq.Context()
    print(f"[PC1-BROKER] Iniciando en modo: {modo.upper()}")
    try:
        if modo == "multihilo":
            modo_multihilo(ctx, num_workers)
        else:
            modo_simple(ctx)
    except KeyboardInterrupt:
        print("[PC1-BROKER] Detenido.")
    finally:
        ctx.term()


if __name__ == "__main__":
    main()
