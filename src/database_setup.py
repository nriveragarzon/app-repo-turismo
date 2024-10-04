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
json_path = 'C:/Users/nrivera/OneDrive - PROCOLOMBIA/Documentos/022-Repositorio-Turismo/app-repo-turismo/.streamlit/snowflake_credentials.json'

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
print("Proceso de creación de base de datos y esquemas exitoso.")

# ---------------------------
# 8. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()




