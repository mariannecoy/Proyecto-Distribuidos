# Gestión Inteligente de Tráfico Urbano
**Pontificia Universidad Javeriana — Introducción a los Sistemas Distribuidos**
Autores: Marianne Coy, Daniel Díaz — 2026

---

## Estructura del proyecto

```
trafico/
├── shared/
│   ├── config.py       ← IPs, puertos, umbrales de tráfico, rutas de BD
│   └── database.py     ← Esquema SQLite + operaciones CRUD
├── pc1/
│   ├── broker.py       ← Broker ZeroMQ: PULL(sensores) → PUB(PC2)
│   └── sensores.py     ← Simula cámara, espira inductiva y GPS
├── pc2/
│   ├── analitica.py    ← Analítica: detecta congestión, aplica reglas
│   ├── semaforos.py    ← Controlador de semáforos con temporizador
│   └── failover.py     ← Heartbeat y conmutación automática a réplica
├── pc3/
│   ├── monitoreo.py    ← BD principal + REP para consultas del usuario
│   └── cliente.py      ← Consola interactiva de monitoreo y control
└── metricas.py         ← Medición de métricas de desempeño (Tabla 1)
```

---

## Dependencias

```bash
pip install pyzmq
# sqlite3 viene incluido en Python estándar
```

---

## Orden de ejecución (abrir una terminal por cada proceso)

```
Terminal 1 — PC3: BD principal y monitoreo
  cd pc3
  python monitoreo.py

Terminal 2 — PC2: Controlador de semáforos
  cd pc2
  python semaforos.py

Terminal 3 — PC2: Analítica
  cd pc2
  python analitica.py

Terminal 4 — PC1: Broker ZeroMQ
  # Modo simple (diseño original):
  cd pc1
  python broker.py simple

  # Modo multihilo (para pruebas de rendimiento):
  cd pc1
  python broker.py multihilo 4

Terminal 5 — PC1: Sensores
  # El argumento es el intervalo en segundos (default=5)
  cd pc1
  python sensores.py 5

Terminal 6 — Consola de usuario (opcional)
  cd pc3
  python cliente.py
```

---

## Ejecución en 3 máquinas físicas

Editar `shared/config.py`:
```python
PC1_IP = "192.168.1.10"
PC2_IP = "192.168.1.20"
PC3_IP = "192.168.1.30"
```
Luego ejecutar cada proceso en su máquina correspondiente.

---

## Prueba de failover (falla de PC3)

1. Arrancar todo el sistema normalmente.
2. Detener `pc3/monitoreo.py` con Ctrl+C.
3. Observar en `pc2/analitica.py` los mensajes de heartbeat fallido.
4. Tras ~9 segundos, el sistema conmuta automáticamente a la BD réplica (PC2).
5. Reiniciar `pc3/monitoreo.py` → recuperación automática.

---

## Reglas de tráfico implementadas

| Condición   | Cola (Q)  | Velocidad (Vp) | Densidad (D)  | Fase verde |
|-------------|-----------|----------------|---------------|------------|
| NORMAL      | < 5 veh   | > 35 km/h      | < 20 veh/km   | 15 s       |
| CONGESTION  | >= 5 veh  | <= 35 km/h     | >= 20 veh/km  | 30 s       |
| PRIORIDAD   | Emergencia forzada por usuario                  | 60 s       |

---

## Patrones ZeroMQ utilizados

- **PUB/SUB**: Sensores → Broker → Analítica (flujo asíncrono de eventos)
- **PUSH/PULL**: Analítica → Semáforos, Analítica → BD principal, Analítica → BD réplica
- **REQ/REP**: PC3 ↔ Analítica (indicaciones directas), Usuario ↔ PC3 (consultas)
