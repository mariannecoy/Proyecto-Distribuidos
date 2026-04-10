# =============================================================================
# shared/config.py
# Configuración global del sistema de Gestión Inteligente de Tráfico Urbano
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

# ── IPs de los tres computadores ─────────────────────────────────────────────
# Cambiar estas IPs si se ejecuta en máquinas físicas distintas
PC1_IP = "127.0.0.1"
PC2_IP = "127.0.0.1"
PC3_IP = "127.0.0.1"

# ── Puertos ZeroMQ ────────────────────────────────────────────────────────────
# PC1: sensores hacen PUSH al broker; broker hace PUB hacia PC2
PUERTO_SENSORES_A_BROKER  = 5555   # Sensores  → Broker   (PUSH/PULL)
PUERTO_BROKER_A_PC2       = 5556   # Broker    → Analitica PC2 (PUB/SUB)

# PC2: analitica recibe indicaciones de PC3, envía comandos a semáforos y BDs
PUERTO_ANALITICA_REP      = 5557   # PC3       → Analitica: indicaciones directas (REQ/REP)
PUERTO_SEMAFOROS_PULL     = 5558   # Analitica → Semáforos: comandos (PUSH/PULL)
PUERTO_BD_REPLICA_PULL    = 5559   # Analitica → BD réplica PC2 (PUSH/PULL)

# PC3: recibe datos de analitica para BD principal; atiende consultas de usuario
PUERTO_BD_PRINCIPAL_PULL  = 5560   # Analitica → BD principal PC3 (PUSH/PULL)
PUERTO_MONITOREO_REP      = 5561   # Usuario   → Monitoreo PC3 (REQ/REP)

# ── Ciudad: cuadrícula N filas x M columnas ───────────────────────────────────
FILAS    = ["A", "B", "C"]
COLUMNAS = [1, 2, 3]

# ── Parámetros de sensores ────────────────────────────────────────────────────
INTERVALO_SENSOR_SEG  = 5    # Segundos entre mediciones (Escenario A=10, B=5)
INTERVALO_ESPIRA_SEG  = 30   # Ventana de conteo de la espira inductiva

# ── Reglas de tráfico ─────────────────────────────────────────────────────────
# NORMAL:    Q < 5  AND Vp > 35  AND D < 20
# CONGESTION:Q >= 5 OR  Vp <= 35 OR  D >= 20
# PRIORIDAD: Evento de emergencia
UMBRAL_COLA_NORMAL      = 5    # vehículos en espera (Q)
UMBRAL_VEL_NORMAL       = 35   # km/h (Vp)
UMBRAL_DENSIDAD_NORMAL  = 20   # veh/km (D)

# ── Tiempos de semáforo en VERDE (segundos) ───────────────────────────────────
TIEMPO_VERDE_NORMAL     = 15
TIEMPO_VERDE_CONGESTION = 30
TIEMPO_VERDE_PRIORIDAD  = 60

# ── Rutas de bases de datos SQLite ────────────────────────────────────────────
DB_PRINCIPAL_PATH = "bd_principal_pc3.db"
DB_REPLICA_PATH   = "bd_replica_pc2.db"

# ── Heartbeat para detección de fallo de PC3 ─────────────────────────────────
HEARTBEAT_INTERVALO_SEG = 3
HEARTBEAT_TIMEOUT_SEG   = 9
