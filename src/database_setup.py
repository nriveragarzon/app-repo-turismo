# ------------------------------
# 1. Importar módulos necesarios
# ------------------------------
import snowflake_analitica as snowflake_analitica

# Warnings
import warnings

# Suprimir todas las advertencias de tipo UserWarning
warnings.filterwarnings("ignore", category=UserWarning)

# ------------------------------------------------
# 2. Definir archivo de configuración de Snowflake
# ------------------------------------------------
json_path = './.streamlit/snowflake_credentials.json'

# --------------------------
# 3. Crear sesión y conexión
# --------------------------
sesion_activa, conexion_activa = snowflake_analitica.create_session_from_json(json_file_path = json_path)

# ----------------------
# 4. Crear base de datos
# ----------------------
# Definir el query para crear la base de datos para el repositorio
query_sql_database = 'CREATE OR REPLACE DATABASE REPOSITORIO_TURISMO'
# Ejecutar query
sesion_activa.sql(query_sql_database).collect()

# -------------------------------------------------------
# 5. Cambiar ubicación a la base de datos del repositorio
# -------------------------------------------------------
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO')

# -----------------
# 6. Crear esquemas
# -----------------
query_sql_esquemas = """
    CREATE OR REPLACE SCHEMA MIGRACION;
    CREATE OR REPLACE SCHEMA OAG;
    CREATE OR REPLACE SCHEMA FORWARDKEYS;
    CREATE OR REPLACE SCHEMA CREDIBANCO;
    CREATE OR REPLACE SCHEMA GLOBALDATA;
    CREATE OR REPLACE SCHEMA IATAGAP;
    CREATE OR REPLACE SCHEMA SEGUIMIENTO;
    CREATE OR REPLACE SCHEMA CORRELATIVAS;
    CREATE OR REPLACE SCHEMA AUDITORIA;
    CREATE OR REPLACE SCHEMA VISTAS;
"""
snowflake_analitica.ejecutar_script_sql_snowpark(sesion_activa, query_sql_esquemas)

# -----------------------------
# 7. Crear tabla de seguimiento
# -----------------------------
# Usar el esquema de seguimiento
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='SEGUIMIENTO')

# Definir el query para crear la tabla de seguimiento
sql_tabla_seguimiento = """
CREATE TABLE SEGUIMIENTO_EVENTOS (
    TIPO_EVENTO STRING,
    DETALLE_EVENTO STRING,
    UNIDAD STRING,
    FECHA_HORA TIMESTAMP
);
"""

# Crear tabla
sesion_activa.sql(sql_tabla_seguimiento).collect()

# ---------------------------
# 8. Crear tabla de auditoria
# ---------------------------

# Usar el esquema de auditoria
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='AUDITORIA')

# Crear tabla para generar IDs
sesion_activa.sql("CREATE TABLE IF NOT EXISTS ID_GENERATOR (ID INTEGER PRIMARY KEY);").collect()
sesion_activa.sql("INSERT INTO ID_GENERATOR (ID) VALUES (0);").collect()

# Crear procedimiento almacenado para obtener el próximo ID
procedimiento = """
CREATE OR REPLACE PROCEDURE GET_NEXT_ID()
RETURNS INTEGER
LANGUAGE SQL
AS $$
DECLARE
    NEXT_ID INTEGER;
BEGIN
    -- Incrementar el ID
    UPDATE ID_GENERATOR
    SET ID = ID + 1;

    -- Obtener el próximo ID
    SELECT ID INTO NEXT_ID
    FROM ID_GENERATOR;

    RETURN NEXT_ID;
END;
$$
"""

# Ejecutar el procedimiento en Snowflake
sesion_activa.sql(procedimiento).collect()


# Definir el query para crear la tabla de auditoria de cargues
sql_tabla_auditoria_cargue = """
CREATE TABLE AUDITORIA_CARGUES (
    ID_AUDITORIA        INTEGER,                                -- Identificador único
    NOMBRE_ESQUEMA_DESTINO VARCHAR(255) NOT NULL,               -- Esquema de destino del cargue
    NOMBRE_TABLA        VARCHAR(255) NOT NULL,                  -- Nombre de la tabla de destino
    FECHA_CARGUE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Fecha y hora del cargue
    RUTA_ARCHIVO        VARCHAR(512) NOT NULL,                  -- Ruta completa o nombre del archivo CSV
    NUMERO_REGISTROS    INTEGER,                                -- Número de registros cargados
    MENSAJE             VARCHAR(512) NOT NULL                   -- Mensaje de resultado del cargue a Snowflake
);
"""
# Crear tabla
sesion_activa.sql(sql_tabla_auditoria_cargue).collect()

print("Proceso de creación de base de datos y esquemas exitoso.")

# ---------------------------
# 9. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()