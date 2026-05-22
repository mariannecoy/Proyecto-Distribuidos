# =============================================================================
# shared/config.py
# Configuración global del sistema de Gestión Inteligente de Tráfico Urbano
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Este módulo centraliza TODAS las constantes del sistema: IPs, puertos,
# parámetros de la cuadrícula, umbrales de tráfico y rutas de base de datos.
# Para desplegar en máquinas físicas, editar únicamente las IPs de PC1, PC2 y PC3.
# =============================================================================

# ── IPs de los tres computadores ─────────────────────────────────────────────
# Cambiar estas IPs cuando se ejecuta en máquinas físicas distintas.
# Para pruebas locales se puede usar "127.0.0.1" en todas.
PC1_IP = "10.43.99.55"
PC2_IP = "10.43.99.70"
PC3_IP = "10.43.99.213"

# ── Puertos ZeroMQ ────────────────────────────────────────────────────────────
# Convención: cada flujo de datos tiene su propio puerto para evitar colisiones.

# PC1 — Broker
PUERTO_SENSORES_A_BROKER = 5555   # Sensores  → Broker   (PUSH/PULL)
PUERTO_BROKER_A_PC2      = 5556   # Broker    → Analítica PC2 (PUB/SUB)

# PC2 — Analítica y Semáforos
PUERTO_ANALITICA_REP     = 5557   # PC3/cliente → Analítica: indicaciones directas (REQ/REP)
PUERTO_SEMAFOROS_PULL    = 5558   # Analítica → Semáforos: comandos de control (PUSH/PULL)
PUERTO_BD_REPLICA_PULL   = 5559   # Analítica/Semáforos → BD réplica PC2 (PUSH/PULL)

# PC3 — Monitoreo y BD principal
PUERTO_BD_PRINCIPAL_PULL = 5560   # Analítica/Semáforos → BD principal PC3 (PUSH/PULL)
PUERTO_MONITOREO_REP     = 5561   # Usuario/Heartbeat → Monitoreo PC3 (REQ/REP)

# ── Cuadrícula de la ciudad (N filas x M columnas) ───────────────────────────
# Notación de intersección: INT_<FILA><COLUMNA>, ej. INT_B2
FILAS    = ["A", "B", "C"]
COLUMNAS = [1, 2, 3]

# ── Parámetros de sensores ────────────────────────────────────────────────────
INTERVALO_SENSOR_SEG = 5    # Segundos entre ciclos de medición (Escenario A=10, B=5)
INTERVALO_ESPIRA_SEG = 30   # Ventana de conteo de la espira inductiva (coincide con ciclo semáforo)

# ── Reglas de tráfico (umbrales para clasificar condición de circulación) ─────
# NORMAL:    Q < 5  AND Vp > 35 AND D < 20
# CONGESTION:Q >= 5 OR  Vp <= 35 OR  D >= 20
# PRIORIDAD: forzada por usuario u operador del sistema
UMBRAL_COLA_NORMAL     = 5    # vehículos en espera (Q)
UMBRAL_VEL_NORMAL      = 35   # velocidad promedio en km/h (Vp)
UMBRAL_DENSIDAD_NORMAL = 20   # densidad vehicular en veh/km (D)

# ── Tiempos de fase verde por condición (segundos) ───────────────────────────
TIEMPO_VERDE_NORMAL     = 15   # Tráfico fluido
TIEMPO_VERDE_CONGESTION = 30   # Congestión detectada: se extiende la fase verde
TIEMPO_VERDE_PRIORIDAD  = 60   # Emergencia: ola verde para paso prioritario

# ── Rutas de bases de datos SQLite ────────────────────────────────────────────
DB_PRINCIPAL_PATH = "bd_principal_pc3.db"   # En PC3 (BD maestra)
DB_REPLICA_PATH   = "bd_replica_pc2.db"     # En PC2 (respaldo/failover)

# ── Heartbeat — detección automática de fallo de PC3 ─────────────────────────
HEARTBEAT_INTERVALO_SEG = 3    # Cada cuántos segundos se envía el ping
HEARTBEAT_TIMEOUT_SEG   = 9    # Si no responde en este tiempo, se declara caído
