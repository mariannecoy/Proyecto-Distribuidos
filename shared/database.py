# =============================================================================
# shared/database.py
# Módulo de base de datos SQLite compartido entre PC2 (réplica) y PC3 (principal)
# Define el esquema y todas las operaciones de lectura/escritura
# Autores: Marianne Coy, Daniel Díaz
# =============================================================================

import sqlite3
import json
from datetime import datetime, timezone


def _ahora():
    """Retorna el timestamp actual en formato ISO UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def conectar(ruta_db: str) -> sqlite3.Connection:
    """Abre (o crea) una conexión SQLite con row_factory para dicts."""
    conn = sqlite3.connect(ruta_db, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_bd(ruta_db: str):
    """
    Crea todas las tablas necesarias si no existen.
    Debe llamarse al arrancar PC2 y PC3.
    """
    conn = conectar(ruta_db)
    cur  = conn.cursor()

    # ── Tabla: eventos crudos de los tres tipos de sensores ──────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eventos_sensores (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id     TEXT NOT NULL,
            tipo_sensor   TEXT NOT NULL,        -- camara | espira_inductiva | gps
            interseccion  TEXT NOT NULL,
            datos_json    TEXT NOT NULL,         -- payload completo en JSON
            timestamp_sensor TEXT NOT NULL,      -- timestamp del sensor
            recibido_en   TEXT NOT NULL          -- timestamp de llegada a la BD
        )
    """)

    # ── Tabla: estado actual de cada semáforo ────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS semaforos (
            interseccion        TEXT PRIMARY KEY,
            estado              TEXT NOT NULL DEFAULT 'ROJO',     -- VERDE | ROJO
            modo                TEXT NOT NULL DEFAULT 'NORMAL',   -- NORMAL | CONGESTION | PRIORIDAD
            duracion_seg        INTEGER NOT NULL DEFAULT 15,
            ultima_actualizacion TEXT NOT NULL
        )
    """)

    # ── Tabla: historial de todos los cambios de semáforo ────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS historial_semaforos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion    TEXT NOT NULL,
            estado_anterior TEXT,
            estado_nuevo    TEXT NOT NULL,
            modo            TEXT NOT NULL,
            motivo          TEXT,
            timestamp       TEXT NOT NULL
        )
    """)

    # ── Tabla: condiciones de congestión detectadas por la analítica ─────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS congestion (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion TEXT NOT NULL,
            nivel        TEXT NOT NULL,   -- NORMAL | CONGESTION | PRIORIDAD
            cola         REAL,            -- Q: vehículos en espera
            velocidad    REAL,            -- Vp: km/h
            densidad     REAL,            -- D: veh/km
            timestamp    TEXT NOT NULL
        )
    """)

    # ── Tabla: eventos de priorización (ambulancias, bomberos, etc.) ─────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eventos_prioridad (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion  TEXT NOT NULL,
            tipo_vehiculo TEXT NOT NULL DEFAULT 'ambulancia',
            solicitado_por TEXT NOT NULL DEFAULT 'sistema',   -- sistema | usuario
            timestamp     TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print(f"[BD] Inicializada: {ruta_db}")


# ── Escritura ─────────────────────────────────────────────────────────────────

def guardar_evento_sensor(ruta_db, sensor_id, tipo_sensor, interseccion, datos: dict, ts_sensor):
    """Inserta un evento crudo de sensor."""
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO eventos_sensores (sensor_id, tipo_sensor, interseccion, datos_json, timestamp_sensor, recibido_en) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (sensor_id, tipo_sensor, interseccion, json.dumps(datos), ts_sensor, _ahora())
    )
    conn.commit()
    conn.close()


def actualizar_semaforo(ruta_db, interseccion, estado, modo, duracion):
    """
    Inserta o actualiza el estado de un semáforo (UPSERT).
    También registra el cambio en el historial.
    """
    conn = conectar(ruta_db)
    cur  = conn.cursor()
    ahora = _ahora()

    # Obtener estado anterior para el historial
    cur.execute("SELECT estado FROM semaforos WHERE interseccion=?", (interseccion,))
    fila = cur.fetchone()
    estado_anterior = fila["estado"] if fila else None

    # UPSERT del estado actual
    cur.execute("""
        INSERT INTO semaforos (interseccion, estado, modo, duracion_seg, ultima_actualizacion)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(interseccion) DO UPDATE SET
            estado=excluded.estado,
            modo=excluded.modo,
            duracion_seg=excluded.duracion_seg,
            ultima_actualizacion=excluded.ultima_actualizacion
    """, (interseccion, estado, modo, duracion, ahora))

    # Registrar en historial
    cur.execute("""
        INSERT INTO historial_semaforos (interseccion, estado_anterior, estado_nuevo, modo, motivo, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (interseccion, estado_anterior, estado, modo, "automatico", ahora))

    conn.commit()
    conn.close()


def guardar_congestion(ruta_db, interseccion, nivel, cola, velocidad, densidad):
    """Registra una condición de congestión detectada."""
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO congestion (interseccion, nivel, cola, velocidad, densidad, timestamp) VALUES (?,?,?,?,?,?)",
        (interseccion, nivel, cola, velocidad, densidad, _ahora())
    )
    conn.commit()
    conn.close()


def guardar_prioridad(ruta_db, interseccion, tipo_vehiculo, solicitado_por):
    """Registra un evento de priorización de emergencia."""
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO eventos_prioridad (interseccion, tipo_vehiculo, solicitado_por, timestamp) VALUES (?,?,?,?)",
        (interseccion, tipo_vehiculo, solicitado_por, _ahora())
    )
    conn.commit()
    conn.close()


# ── Consultas ─────────────────────────────────────────────────────────────────

def consultar_todos_semaforos(ruta_db):
    conn = conectar(ruta_db)
    rows = [dict(r) for r in conn.execute("SELECT * FROM semaforos ORDER BY interseccion")]
    conn.close()
    return rows


def consultar_semaforo(ruta_db, interseccion):
    conn = conectar(ruta_db)
    cur  = conn.cursor()
    cur.execute("SELECT * FROM semaforos WHERE interseccion=?", (interseccion,))
    fila = cur.fetchone()
    conn.close()
    return dict(fila) if fila else None


def consultar_congestion_historica(ruta_db, desde=None, hasta=None, interseccion=None):
    """Consulta historial de congestión con filtros opcionales de tiempo e intersección."""
    conn  = conectar(ruta_db)
    query = "SELECT * FROM congestion WHERE 1=1"
    params = []
    if desde:
        query += " AND timestamp >= ?"
        params.append(desde)
    if hasta:
        query += " AND timestamp <= ?"
        params.append(hasta)
    if interseccion:
        query += " AND interseccion = ?"
        params.append(interseccion)
    query += " ORDER BY timestamp DESC LIMIT 500"
    rows = [dict(r) for r in conn.execute(query, params)]
    conn.close()
    return rows


def consultar_historial_semaforos(ruta_db, interseccion=None, limite=100):
    conn = conectar(ruta_db)
    if interseccion:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM historial_semaforos WHERE interseccion=? ORDER BY timestamp DESC LIMIT ?",
            (interseccion, limite)
        )]
    else:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM historial_semaforos ORDER BY timestamp DESC LIMIT ?", (limite,)
        )]
    conn.close()
    return rows


def consultar_prioridades(ruta_db, interseccion=None):
    conn = conectar(ruta_db)
    if interseccion:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM eventos_prioridad WHERE interseccion=? ORDER BY timestamp DESC", (interseccion,)
        )]
    else:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM eventos_prioridad ORDER BY timestamp DESC LIMIT 200"
        )]
    conn.close()
    return rows


def contar_eventos_recientes(ruta_db, segundos=120):
    """Cuenta eventos guardados en los últimos N segundos (métrica de rendimiento)."""
    from datetime import timedelta
    limite = (datetime.now(timezone.utc) - timedelta(seconds=segundos)).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn   = conectar(ruta_db)
    cur    = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM eventos_sensores WHERE recibido_en >= ?", (limite,))
    count = cur.fetchone()[0]
    conn.close()
    return count
