# =============================================================================
# shared/database.py
# Módulo de persistencia SQLite compartido entre PC2 (réplica) y PC3 (principal)
#
# Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos
# Autores: Marianne Coy, Daniel Díaz
#
# Define el esquema completo de la BD y todas las operaciones de lectura/escritura.
# Tanto la BD principal (PC3) como la réplica (PC2) usan las mismas tablas y
# funciones; la diferencia está únicamente en la ruta del archivo SQLite.
# =============================================================================

import sqlite3
import json
from datetime import datetime, timezone, timedelta


def _ahora() -> str:
    """Retorna el timestamp actual en formato ISO-8601 UTC (ej. 2026-02-09T15:10:00Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def conectar(ruta_db: str) -> sqlite3.Connection:
    """
    Abre (o crea) una conexión SQLite con row_factory configurada para
    retornar filas como diccionarios (sqlite3.Row).

    Args:
        ruta_db: Ruta al archivo .db de SQLite.

    Returns:
        Objeto de conexión SQLite listo para usar.
    """
    conn = sqlite3.connect(ruta_db, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_bd(ruta_db: str) -> None:
    """
    Crea todas las tablas necesarias si no existen.
    Debe llamarse al iniciar PC2 (réplica) y PC3 (principal).

    Tablas creadas:
        - eventos_sensores:   eventos crudos de los tres tipos de sensor.
        - semaforos:          estado actual de cada semáforo (UPSERT).
        - historial_semaforos:registro de todos los cambios de semáforo.
        - congestion:         condiciones de tráfico detectadas por analítica.
        - eventos_prioridad:  priorización de emergencias (ambulancias, etc.).

    Args:
        ruta_db: Ruta al archivo SQLite a inicializar.
    """
    conn = conectar(ruta_db)
    cur  = conn.cursor()

    # Tabla: eventos crudos de sensores (cámara, espira inductiva, GPS)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eventos_sensores (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id      TEXT    NOT NULL,
            tipo_sensor    TEXT    NOT NULL,        -- camara | espira_inductiva | gps
            interseccion   TEXT    NOT NULL,
            datos_json     TEXT    NOT NULL,         -- payload completo serializado en JSON
            timestamp_sensor TEXT  NOT NULL,         -- timestamp generado por el sensor
            recibido_en    TEXT    NOT NULL           -- timestamp de llegada a esta BD
        )
    """)

    # Tabla: estado actual de cada semáforo (una fila por intersección)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS semaforos (
            interseccion         TEXT    PRIMARY KEY,
            estado               TEXT    NOT NULL DEFAULT 'ROJO',      -- VERDE | ROJO
            modo                 TEXT    NOT NULL DEFAULT 'NORMAL',    -- NORMAL | CONGESTION | PRIORIDAD | MANUAL
            duracion_seg         INTEGER NOT NULL DEFAULT 15,
            ultima_actualizacion TEXT    NOT NULL
        )
    """)

    # Tabla: historial de todos los cambios de semáforo
    cur.execute("""
        CREATE TABLE IF NOT EXISTS historial_semaforos (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion   TEXT    NOT NULL,
            estado_anterior TEXT,
            estado_nuevo   TEXT    NOT NULL,
            modo           TEXT    NOT NULL,
            motivo         TEXT,
            timestamp      TEXT    NOT NULL
        )
    """)

    # Tabla: condiciones de tráfico detectadas por el servicio de analítica
    cur.execute("""
        CREATE TABLE IF NOT EXISTS congestion (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion TEXT    NOT NULL,
            nivel        TEXT    NOT NULL,   -- NORMAL | CONGESTION | PRIORIDAD
            cola         REAL,               -- Q: número de vehículos en espera
            velocidad    REAL,               -- Vp: velocidad promedio en km/h
            densidad     REAL,               -- D: densidad vehicular en veh/km
            timestamp    TEXT    NOT NULL
        )
    """)

    # Tabla: eventos de priorización de emergencia (ambulancias, bomberos, etc.)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eventos_prioridad (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            interseccion   TEXT    NOT NULL,
            tipo_vehiculo  TEXT    NOT NULL DEFAULT 'ambulancia',
            solicitado_por TEXT    NOT NULL DEFAULT 'sistema',   -- sistema | usuario
            timestamp      TEXT    NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print(f"[BD] Inicializada correctamente: {ruta_db}")


# =============================================================================
# Operaciones de escritura
# =============================================================================

def guardar_evento_sensor(ruta_db: str, sensor_id: str, tipo_sensor: str,
                          interseccion: str, datos: dict, ts_sensor: str) -> None:
    """
    Inserta un evento crudo de sensor en la base de datos.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        sensor_id:    Identificador del sensor (ej. "CAM-C5").
        tipo_sensor:  Tipo de sensor: "camara", "espira_inductiva" o "gps".
        interseccion: Código de intersección (ej. "INT_C5").
        datos:        Diccionario con el payload completo del evento.
        ts_sensor:    Timestamp generado por el sensor (ISO-8601).
    """
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO eventos_sensores "
        "(sensor_id, tipo_sensor, interseccion, datos_json, timestamp_sensor, recibido_en) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (sensor_id, tipo_sensor, interseccion, json.dumps(datos), ts_sensor, _ahora())
    )
    conn.commit()
    conn.close()


def actualizar_semaforo(ruta_db: str, interseccion: str,
                        estado: str, modo: str, duracion: int,
                        motivo: str = "automatico") -> None:
    """
    Inserta o actualiza el estado de un semáforo (UPSERT) y registra el cambio
    en el historial.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        interseccion: Código de intersección (ej. "INT_B2").
        estado:       Nuevo estado del semáforo: "VERDE" o "ROJO".
        modo:         Modo de operación: "NORMAL", "CONGESTION", "PRIORIDAD" o "MANUAL".
        duracion:     Duración en segundos de la fase verde.
        motivo:       Causa del cambio (para trazabilidad en el historial).
    """
    conn  = conectar(ruta_db)
    cur   = conn.cursor()
    ahora = _ahora()

    # Obtener estado anterior para registrar en historial
    cur.execute("SELECT estado FROM semaforos WHERE interseccion=?", (interseccion,))
    fila = cur.fetchone()
    estado_anterior = fila["estado"] if fila else None

    # UPSERT: insertar o actualizar el estado actual
    cur.execute("""
        INSERT INTO semaforos (interseccion, estado, modo, duracion_seg, ultima_actualizacion)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(interseccion) DO UPDATE SET
            estado=excluded.estado,
            modo=excluded.modo,
            duracion_seg=excluded.duracion_seg,
            ultima_actualizacion=excluded.ultima_actualizacion
    """, (interseccion, estado, modo, duracion, ahora))

    # Registrar en historial solo si el estado cambió
    cur.execute("""
        INSERT INTO historial_semaforos
            (interseccion, estado_anterior, estado_nuevo, modo, motivo, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (interseccion, estado_anterior, estado, modo, motivo, ahora))

    conn.commit()
    conn.close()


def guardar_congestion(ruta_db: str, interseccion: str, nivel: str,
                       cola: float, velocidad: float, densidad: float) -> None:
    """
    Registra una condición de tráfico detectada por el servicio de analítica.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        interseccion: Código de intersección.
        nivel:        Nivel de congestión: "NORMAL", "CONGESTION" o "PRIORIDAD".
        cola:         Longitud de cola Q (número de vehículos en espera).
        velocidad:    Velocidad promedio Vp (km/h).
        densidad:     Densidad vehicular D (veh/km).
    """
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO congestion "
        "(interseccion, nivel, cola, velocidad, densidad, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (interseccion, nivel, cola, velocidad, densidad, _ahora())
    )
    conn.commit()
    conn.close()


def guardar_prioridad(ruta_db: str, interseccion: str,
                      tipo_vehiculo: str, solicitado_por: str) -> None:
    """
    Registra un evento de priorización de emergencia.

    Args:
        ruta_db:       Ruta al archivo SQLite.
        interseccion:  Código de intersección donde ocurre la priorización.
        tipo_vehiculo: Tipo de vehículo prioritario (ej. "ambulancia", "bomberos").
        solicitado_por:"sistema" (automático) o "usuario" (manual desde PC3).
    """
    conn = conectar(ruta_db)
    conn.execute(
        "INSERT INTO eventos_prioridad "
        "(interseccion, tipo_vehiculo, solicitado_por, timestamp) "
        "VALUES (?, ?, ?, ?)",
        (interseccion, tipo_vehiculo, solicitado_por, _ahora())
    )
    conn.commit()
    conn.close()


# =============================================================================
# Operaciones de consulta
# =============================================================================

def consultar_todos_semaforos(ruta_db: str) -> list[dict]:
    """
    Retorna el estado actual de todos los semáforos registrados.

    Args:
        ruta_db: Ruta al archivo SQLite.

    Returns:
        Lista de diccionarios con los campos de la tabla semaforos.
    """
    conn = conectar(ruta_db)
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM semaforos ORDER BY interseccion"
    )]
    conn.close()
    return rows


def consultar_semaforo(ruta_db: str, interseccion: str) -> dict | None:
    """
    Retorna el estado actual de un semáforo específico.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        interseccion: Código de intersección.

    Returns:
        Diccionario con el estado del semáforo, o None si no existe.
    """
    conn = conectar(ruta_db)
    cur  = conn.cursor()
    cur.execute("SELECT * FROM semaforos WHERE interseccion=?", (interseccion,))
    fila = cur.fetchone()
    conn.close()
    return dict(fila) if fila else None


def consultar_congestion_historica(ruta_db: str, desde: str = None,
                                   hasta: str = None,
                                   interseccion: str = None) -> list[dict]:
    """
    Consulta el historial de condiciones de tráfico con filtros opcionales.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        desde:        Timestamp de inicio del rango (ISO-8601, opcional).
        hasta:        Timestamp de fin del rango (ISO-8601, opcional).
        interseccion: Filtrar por intersección específica (opcional).

    Returns:
        Lista de registros de congestión ordenados por timestamp descendente.
    """
    conn   = conectar(ruta_db)
    query  = "SELECT * FROM congestion WHERE 1=1"
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


def consultar_historial_semaforos(ruta_db: str, interseccion: str = None,
                                  limite: int = 100) -> list[dict]:
    """
    Consulta el historial de cambios de semáforos.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        interseccion: Filtrar por intersección específica (None = todas).
        limite:       Número máximo de registros a retornar.

    Returns:
        Lista de cambios de semáforo ordenados por timestamp descendente.
    """
    conn = conectar(ruta_db)
    if interseccion:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM historial_semaforos "
            "WHERE interseccion=? ORDER BY timestamp DESC LIMIT ?",
            (interseccion, limite)
        )]
    else:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM historial_semaforos ORDER BY timestamp DESC LIMIT ?",
            (limite,)
        )]
    conn.close()
    return rows


def consultar_prioridades(ruta_db: str, interseccion: str = None) -> list[dict]:
    """
    Consulta los eventos de priorización de emergencia registrados.

    Args:
        ruta_db:      Ruta al archivo SQLite.
        interseccion: Filtrar por intersección específica (None = todas).

    Returns:
        Lista de eventos de prioridad ordenados por timestamp descendente.
    """
    conn = conectar(ruta_db)
    if interseccion:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM eventos_prioridad "
            "WHERE interseccion=? ORDER BY timestamp DESC",
            (interseccion,)
        )]
    else:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM eventos_prioridad ORDER BY timestamp DESC LIMIT 200"
        )]
    conn.close()
    return rows


def contar_eventos_recientes(ruta_db: str, segundos: int = 120) -> int:
    """
    Cuenta los eventos de sensor almacenados en los últimos N segundos.
    Utilizado como métrica de rendimiento (Tabla 1 del enunciado).

    Args:
        ruta_db:  Ruta al archivo SQLite.
        segundos: Ventana de tiempo hacia atrás (default 120 = 2 minutos).

    Returns:
        Número de eventos almacenados en el período indicado.
    """
    limite = (datetime.now(timezone.utc) - timedelta(seconds=segundos)
              ).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn   = conectar(ruta_db)
    cur    = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM eventos_sensores WHERE recibido_en >= ?",
        (limite,)
    )
    count = cur.fetchone()[0]
    conn.close()
    return count
