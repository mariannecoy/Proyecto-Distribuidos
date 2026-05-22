# Gestión Inteligente de Tráfico Urbano
**Pontificia Universidad Javeriana — Introducción a los Sistemas Distribuidos**
Autores: Marianne Coy, Daniel Díaz — 2026

---

## Estructura del proyecto

```
trafico/
├── shared/
│   ├── config.py         ← IPs, puertos, umbrales de tráfico, rutas de BD
│   └── database.py       ← Esquema SQLite + operaciones CRUD (principal y réplica)
├── pc1/
│   ├── broker.py         ← Broker ZeroMQ: PULL(sensores) → PUB(PC2) | modos simple/multihilo
│   └── sensores.py       ← Simula cámara, espira inductiva y GPS por intersección
├── pc2/
│   ├── analitica.py      ← Analítica: detecta congestión, aplica reglas, failover integrado
│   ├── semaforos.py      ← Controlador de semáforos: PULL(comandos), temporizador automático
│   └── failover.py       ← Heartbeat y conmutación automática a BD réplica
├── pc3/
│   ├── monitoreo.py      ← BD principal + REP para consultas e indicaciones del usuario
│   └── cliente.py        ← Consola interactiva de monitoreo y control
└── metricas.py           ← Medición de métricas de desempeño (Tabla 1)
```

---

## Dependencias

```bash
pip install pyzmq
# sqlite3 viene incluido en Python estándar (≥ 3.10)
```

---

## Configuración de IPs

Antes de ejecutar en máquinas físicas, editar `shared/config.py`:

```python
PC1_IP = "192.168.1.10"   # IP real de la máquina con broker y sensores
PC2_IP = "192.168.1.20"   # IP real de la máquina con analítica y semáforos
PC3_IP = "192.168.1.30"   # IP real de la máquina con monitoreo y BD principal
```

Para pruebas en una sola máquina, usar `"127.0.0.1"` en las tres.

---

## Orden de ejecución

**Abrir una terminal por proceso. Respetar el orden para evitar errores de conexión.**

```bash
# ── Terminal 1 — PC3: BD principal y servicio de monitoreo ───────────────
cd pc3
python monitoreo.py

# ── Terminal 2 — PC2: Controlador de semáforos ───────────────────────────
cd pc2
python semaforos.py

# ── Terminal 3 — PC2: Servicio de analítica (incluye failover) ───────────
cd pc2
python analitica.py

# ── Terminal 4 — PC1: Broker ZeroMQ ──────────────────────────────────────
# Modo simple (diseño original — Escenario A):
cd pc1
python broker.py simple

# Modo multihilo (diseño modificado — Escenario B):
cd pc1
python broker.py multihilo 4

# ── Terminal 5 — PC1: Sensores de tráfico ────────────────────────────────
# Escenario A: 1 sensor por intersección, datos cada 10s
cd pc1
python sensores.py 10

# Escenario B: datos cada 5s (lanzar dos veces en terminales separadas)
cd pc1
python sensores.py 5

# ── Terminal 6 — Consola del usuario (opcional, puede ejecutarse en PC3) ─
cd pc3
python cliente.py
```

---

## Cuadrícula de la ciudad

La ciudad tiene una cuadrícula de **3x3** (configurable en `config.py`):

```
     Col 1    Col 2    Col 3
Fila A: INT_A1  INT_A2  INT_A3
Fila B: INT_B1  INT_B2  INT_B3
Fila C: INT_C1  INT_C2  INT_C3
```

Cada intersección tiene tres sensores simulados: cámara (`CAM`), espira (`ESP`) y GPS.
Los sensores de filas B y C simulan mayor densidad vehicular (zonas céntricas).

---

## Reglas de tráfico implementadas

| Condición   | Cola Q     | Velocidad Vp | Densidad D  | Fase verde |
|-------------|-----------|--------------|-------------|------------|
| NORMAL      | < 5 veh   | > 35 km/h    | < 20 veh/km | 15 s       |
| CONGESTION  | ≥ 5 veh   | ≤ 35 km/h    | ≥ 20 veh/km | 30 s       |
| PRIORIDAD   | Forzada por usuario (emergencia)          | 60 s       |

Nota: la condición CONGESTION se activa si **cualquiera** de los tres criterios
se incumple (operador OR). La condición NORMAL requiere que **todos** se cumplan (AND).

---

## Patrones ZeroMQ utilizados

| Patrón   | Flujo                                          |
|----------|------------------------------------------------|
| PUSH/PULL | Sensores → Broker (ingesta asíncrona)          |
| PUB/SUB  | Broker → Analítica (distribución por tópico)   |
| PUSH/PULL | Analítica → Semáforos (comandos de control)    |
| PUSH/PULL | Analítica → BD principal PC3                   |
| PUSH/PULL | Analítica → BD réplica PC2                     |
| REQ/REP  | PC3 Monitoreo ↔ Analítica (indicaciones)        |
| REQ/REP  | Cliente ↔ PC3 Monitoreo (consultas usuario)     |
| REQ/REP  | Heartbeat PC2 → PC3 (detección de fallos)      |

---

## Prueba de failover (falla de PC3)

1. Arrancar el sistema completo normalmente.
2. Detener `pc3/monitoreo.py` con **Ctrl+C**.
3. Observar en la consola de `pc2/analitica.py`:
   - Mensajes de heartbeat fallido (⚠ PC3 sin respuesta).
   - Tras ~6-9 segundos: "PC3 CAÍDO 🔴 → activando réplica".
4. El sistema continúa operando usando la BD réplica en PC2.
5. Reiniciar `pc3/monitoreo.py` → recuperación automática en ~3 segundos.

**La conmutación es transparente para el cliente** — las consultas siguen funcionando
contra la réplica sin necesidad de reiniciar ningún proceso.

---

## Consultas disponibles desde el cliente

| Opción | Acción                  | Descripción                                   |
|--------|-------------------------|-----------------------------------------------|
| 1      | GET_SEMAFOROS           | Estado actual de todos los semáforos          |
| 2      | GET_SEMAFORO            | Estado de una intersección específica         |
| 3      | GET_CONGESTION          | Historial de congestión (filtros de fecha)    |
| 4      | GET_HISTORIAL           | Historial de cambios de semáforo              |
| 5      | GET_PRIORIDADES         | Eventos de priorización registrados           |
| 6      | PRIORIDAD_EMERGENCIA    | Forzar ola verde para vehículo de emergencia  |
| 7      | CAMBIO_SEMAFORO         | Cambio manual de semáforo por operador        |
| 8      | HEARTBEAT               | Verificar disponibilidad de PC3               |

---

## Medición de métricas de desempeño (Tabla 1)

```bash
# Asegurarse de que todo el sistema esté corriendo antes de ejecutar
python metricas.py
```

El script guía interactivamente la medición y guarda los resultados en
`resultados_metricas.json` para importar en Excel y construir los gráficos del informe.

### Escenario A (diseño simple):
```bash
# Broker:
python broker.py simple
# Sensores (1 grupo, 10s de intervalo):
python sensores.py 10
```

### Escenario B (diseño multihilo):
```bash
# Broker:
python broker.py multihilo 4
# Sensores (2 grupos independientes, 5s de intervalo):
# Terminal 5a:
python sensores.py 5
# Terminal 5b (segunda instancia):
python sensores.py 5
```

---

## Especificaciones de hardware para el informe de métricas

Al ejecutar las pruebas, registrar:
- CPU (modelo y número de núcleos)
- RAM disponible
- Sistema operativo y versión
- Python (versión)
- pyzmq (versión: `python -c "import zmq; print(zmq.__version__)"`)
- Tipo de red (Ethernet/WiFi, velocidad)
- SQLite (versión: `python -c "import sqlite3; print(sqlite3.sqlite_version)"`)
