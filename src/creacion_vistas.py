# ------------------------------
# 1. Importar módulos necesarios
# ------------------------------
import snowflake_analitica as snowflake_analitica

# Warnings
import warnings

# OS
import os

# Importar pandas
import pandas as pd

# Prettyprint
import pprint

# Suprimir todas las advertencias de tipo UserWarning
warnings.filterwarnings("ignore", category=UserWarning)

# Aumentar número de columnas que se pueden ver
pd.options.display.max_columns = None
# En los dataframes, mostrar los float con dos decimales
pd.options.display.float_format = '{:,.10f}'.format
# Cada columna será tan grande como sea necesario para mostrar todo su contenido
pd.set_option('display.max_colwidth', 0)

# ------------------------------------------------
# 2. Definir archivo de configuración de Snowflake
# ------------------------------------------------
json_path = './.streamlit/snowflake_credentials.json'

# --------------------------
# 3. Crear sesión y conexión
# --------------------------
sesion_activa, conexion_activa = snowflake_analitica.create_session_from_json(json_file_path = json_path)

# -------------------------------------------------------
# 4. Cambiar ubicación a la base de datos del repositorio
# -------------------------------------------------------
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO')

# ------------------------------
# 5. Verificación de completitud
# ------------------------------

# Creación de la vista de Geografía

# Query prinicipal de Geografía
query_sql_geografia = """
CREATE OR REPLACE VIEW VISTAS.GEOGRAFIA AS
SELECT PAISES.M49_CODE,
    PAISES.ISO_ALPHA2_CODE,
    PAISES.ISO_ALPHA3_CODE,
    PAISES.COUNTRY_OR_AREA,
    CONTINENETES.REGION_NAME,
    MIGRACION.CODIGO_PAIS_MIGRACION,
    MIGRACION.NOMBRE_PAIS_MIGRACION,
    MIGRACION.REGION_NAME_TURISMO,
    MIGRACION.REGION_NAME_TURISMO_AGREGADA,
    MIGRACION.HUB_NAME_TURISMO,
    GLOBALDATA.NOMBRE_GLOBAL_DATA,
    OAG.NOMBRE_OAG,
    CREDIBANCO.NOMBRE_CREDIBANCO,
    IATAGAP.NOMBRE_IATA_GAP,
    REGIONES.SUB_REGION_NAME,
    FORWARDKEYS.COUNTRYCODE AS COUNTRYCODE_FORWARDKEYS
FROM CORRELATIVAS.PAISES AS PAISES
    LEFT JOIN CORRELATIVAS.CONTINENTES AS CONTINENETES ON PAISES.REGION_CODE = CONTINENETES.REGION_CODE
    LEFT JOIN CORRELATIVAS.PAISES_MIGRACION AS MIGRACION ON PAISES.M49_CODE = MIGRACION.M49_CODE
    LEFT JOIN CORRELATIVAS.PAISES_GLOBALDATA AS GLOBALDATA ON PAISES.M49_CODE = GLOBALDATA.M49_CODE
    LEFT JOIN CORRELATIVAS.PAISES_OAG AS OAG ON PAISES.M49_CODE = OAG.M49_CODE
    LEFT JOIN CORRELATIVAS.PAISES_CREDIBANCO AS CREDIBANCO ON PAISES.M49_CODE = CREDIBANCO.M49_CODE
    LEFT JOIN CORRELATIVAS.PAISES_IATAGAP AS IATAGAP ON PAISES.M49_CODE = IATAGAP.M49_CODE
    LEFT JOIN CORRELATIVAS.REGIONES AS REGIONES ON PAISES.SUB_REGION_CODE = REGIONES.SUB_REGION_CODE
    LEFT JOIN (SELECT DISTINCT COUNTRYCODE, M49_CODE FROM CORRELATIVAS.PAISES_FORWARDKEYS) AS FORWARDKEYS ON PAISES.M49_CODE = FORWARDKEYS.M49_CODE;
"""

# Creación de la vista
pd.DataFrame(sesion_activa.sql(query_sql_geografia).collect())

print("Actualizando la vista de datos geográficos")

# Definición de consultas

print("Verificando completitud de países y ciudades...")

query_sql_global_data = """
SELECT DISTINCT PAISES.COUNTRY AS PAIS_GLOBALDATA,
    GEOGRAFIA.COUNTRY_OR_AREA AS PAIS
FROM (
    SELECT DISTINCT VIAJEROS_MUNDO.COUNTRY AS COUNTRY
    FROM GLOBALDATA.FLUJO_VIAJEROS_MUNDO AS VIAJEROS_MUNDO

    UNION ALL

    SELECT DISTINCT NOCHES_PROMEDIO.COUNTRY AS COUNTRY
    FROM GLOBALDATA.NOCHES_PROMEDIO AS NOCHES_PROMEDIO

    UNION ALL

    SELECT DISTINCT CATEGORIAS_GASTO.COUNTRY AS COUNTRY
    FROM GLOBALDATA.CATEGORIAS_GASTO AS CATEGORIAS_GASTO

    UNION ALL

    SELECT DISTINCT RANGO_EDAD.COUNTRY AS COUNTRY
    FROM GLOBALDATA.RANGO_EDAD AS RANGO_EDAD

    UNION ALL

    SELECT DISTINCT MOTIVO_VIAJE.COUNTRY AS COUNTRY
    FROM GLOBALDATA.MOTIVO_VIAJE AS MOTIVO_VIAJE

    UNION ALL

    SELECT DISTINCT FORMA_VIAJE.COUNTRY AS COUNTRY
    FROM GLOBALDATA.FORMA_VIAJE AS FORMA_VIAJE

    UNION ALL

    SELECT DISTINCT VIAJEROS_REGION.COUNTRY AS COUNTRY
    FROM GLOBALDATA.FLUJO_VIAJEROS_REGION AS VIAJEROS_REGION

    UNION ALL

    SELECT DISTINCT VIAJEROS_REGION.COUNTRY_OF_ORIGIN_DESTINATION AS COUNTRY
    FROM GLOBALDATA.FLUJO_VIAJEROS_REGION AS VIAJEROS_REGION

    UNION ALL

    SELECT DISTINCT MICE.COUNTRY AS COUNTRY
    FROM GLOBALDATA.FLUJO_MICE AS MICE
) AS PAISES
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA ON PAISES.COUNTRY = GEOGRAFIA.NOMBRE_GLOBAL_DATA
WHERE GEOGRAFIA.COUNTRY_OR_AREA IS NULL;"""

query_sql_oag_paises = """
SELECT DISTINCT PAISES.CODIGO,
    PAISES.PAIS
FROM (
    SELECT DISTINCT OAG_CONECTIVIDAD_MUNDO.DEP_IATA_COUNTRY_CODE AS CODIGO,
        GEOGRAFIA_DEP.COUNTRY_OR_AREA AS PAIS
    FROM OAG.CONECTIVIDAD_DIRECTA AS OAG_CONECTIVIDAD_MUNDO
        LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_DEP ON OAG_CONECTIVIDAD_MUNDO.DEP_IATA_COUNTRY_CODE = GEOGRAFIA_DEP.COUNTRYCODE_FORWARDKEYS

    UNION ALL

    SELECT DISTINCT OAG_CONECTIVIDAD_MUNDO.ARR_IATA_COUNTRY_CODE AS CODIGO,
        GEOGRAFIA_ARR.COUNTRY_OR_AREA AS PAIS
    FROM OAG.CONECTIVIDAD_DIRECTA AS OAG_CONECTIVIDAD_MUNDO
        LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_ARR ON OAG_CONECTIVIDAD_MUNDO.ARR_IATA_COUNTRY_CODE = GEOGRAFIA_ARR.COUNTRYCODE_FORWARDKEYS
) AS PAISES
WHERE PAISES.PAIS IS NULL;
"""

query_sql_oag_ciudades = """
SELECT DISTINCT OAG_CONECTIVIDAD_MUNDO.ARR_CITY_CODE,
    OAG_CONECTIVIDAD_MUNDO.ARR_CITY_NAME,
    AEROPUERTOS.COD_DANE_MUNICIPIO,
	AEROPUERTOS.MUNICIPIO_DANE,
	AEROPUERTOS.COD_DANE_DEPARTAMENTO,
	AEROPUERTOS.DEPARTAMENTO_DANE 
FROM OAG.CONECTIVIDAD_DIRECTA AS OAG_CONECTIVIDAD_MUNDO
    LEFT JOIN CORRELATIVAS.DIVIPOLA_AEROPUERTOS AS AEROPUERTOS ON OAG_CONECTIVIDAD_MUNDO.ARR_CITY_CODE = AEROPUERTOS.ARR_CITY_CODE
WHERE OAG_CONECTIVIDAD_MUNDO.ARR_IATA_COUNTRY_CODE = 'CO'
    AND AEROPUERTOS.COD_DANE_MUNICIPIO IS NULL;
"""

query_forward_keys = """
SELECT *
FROM 
    (
SELECT DISTINCT FORWARDKEYS_RESERVAS_PAISES.TRIP_ORIGIN_COUNTRY AS COD,
    GEOGRAFIA_DEP.COUNTRY_OR_AREA AS PAIS
FROM FORWARDKEYS.RESERVAS AS FORWARDKEYS_RESERVAS_PAISES
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_DEP ON FORWARDKEYS_RESERVAS_PAISES.TRIP_ORIGIN_COUNTRY = GEOGRAFIA_DEP.COUNTRYCODE_FORWARDKEYS

UNION ALL

SELECT DISTINCT FORWARDKEYS_BUSQUEDAS_PAISES.SEARCH_ORIGIN_COUNTRY AS COD,
    GEOGRAFIA_DEP.COUNTRY_OR_AREA AS PAIS,
FROM FORWARDKEYS.BUSQUEDAS AS FORWARDKEYS_BUSQUEDAS_PAISES
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_DEP ON FORWARDKEYS_BUSQUEDAS_PAISES.SEARCH_ORIGIN_COUNTRY = GEOGRAFIA_DEP.COUNTRYCODE_FORWARDKEYS
    ) AS PAISES
WHERE PAISES.PAIS IS NULL;
"""

query_sql_credibanco = """
SELECT DISTINCT CREDIBANCO_GASTO.PAIS_ORIGEN,
    GEOGRAFIA.COUNTRY_OR_AREA AS PAIS
FROM CREDIBANCO.GASTO AS CREDIBANCO_GASTO 
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA ON CREDIBANCO_GASTO.PAIS_ORIGEN = GEOGRAFIA.NOMBRE_CREDIBANCO
WHERE GEOGRAFIA.COUNTRY_OR_AREA IS NULL; 
"""

query_sql_iatagap = """
SELECT DISTINCT *
FROM 
    (
SELECT IATAGAP_AGENCIAS.TRAVEL_AGENCY_COUNTRY AS COD,
    GEOGRAFIA_AGENCIA.COUNTRY_OR_AREA AS PAIS
FROM IATAGAP.AGENCIAS AS IATAGAP_AGENCIAS
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_AGENCIA ON IATAGAP_AGENCIAS.TRAVEL_AGENCY_COUNTRY = GEOGRAFIA_AGENCIA.NOMBRE_IATA_GAP

UNION ALL

SELECT IATAGAP_AGENCIAS.TRIP_ORIGIN_COUNTRY AS COD,
    GEOGRAFIA_DEP.COUNTRY_OR_AREA AS PAIS
FROM IATAGAP.AGENCIAS AS IATAGAP_AGENCIAS
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_DEP ON IATAGAP_AGENCIAS.TRIP_ORIGIN_COUNTRY = GEOGRAFIA_DEP.NOMBRE_IATA_GAP
    
UNION ALL

SELECT IATAGAP_AGENCIAS.TRIP_DESTINATION_COUNTRY AS COD,
    GEOGRAFIA_ARR.COUNTRY_OR_AREA AS PAIS
FROM IATAGAP.AGENCIAS AS IATAGAP_AGENCIAS
    LEFT JOIN VISTAS.GEOGRAFIA AS GEOGRAFIA_ARR ON IATAGAP_AGENCIAS.TRIP_DESTINATION_COUNTRY = GEOGRAFIA_ARR.NOMBRE_IATA_GAP
    ) AS PAISES
WHERE PAISES.PAIS IS NULL;
"""

# Inicializar la variable para errores críticos
errores_criticos = False
mensajes_errores = []  # Para acumular mensajes de error

# Lista de consultas y descripciones para identificar los errores
consultas = [
    {"query": query_sql_global_data, "descripcion": "Verificación de completitud de países en Global Data"},
    {"query": query_sql_oag_paises, "descripcion": "Verificación de completitud de países en OAG Países"},
    {"query": query_sql_oag_ciudades, "descripcion": "Verificación de completitud de ciudades en OAG Ciudades"},
    {"query": query_forward_keys, "descripcion": "Verificación de completitud de países en Forward Keys"},
    {"query": query_sql_credibanco, "descripcion": "Verificación de completitud de países en Credibanco"},
    {"query": query_sql_iatagap, "descripcion": "Verificación de completitud de países en IATAGAP"},
]

# Ejecutar todas las consultas
for index, consulta in enumerate(consultas, start=1):
    try:
        print(f"Ejecutando consulta {index}/{len(consultas)}: {consulta['descripcion']}...")
        
        # Ejecutar la consulta
        tabla_existe = pd.DataFrame(sesion_activa.sql(consulta["query"]).collect())
        
        # Verificar si la consulta devuelve resultados
        if not tabla_existe.empty:
            errores_criticos = True  # Cambiar el estado de error
            mensajes_errores.append(f"CRÍTICO: {consulta['descripcion']} devolvió resultados. Verificar completitud.")
        
        print(f"Consulta {index}/{len(consultas)} completada.")
    except Exception as e:
        errores_criticos = True
        mensajes_errores.append(f"Error ejecutando la consulta: {consulta['descripcion']}. Detalles: {str(e)}")
        print(f"Error durante la ejecución de la consulta {index}/{len(consultas)}: {str(e)}")

# Verificar el estado final y dar mensajes
if errores_criticos:
    # Imprimir todos los mensajes de error
    print("\nResumen de errores críticos:")
    for mensaje in mensajes_errores:
        print(mensaje)
    
    # Detener la ejecución del programa
    raise ValueError("Errores críticos detectados. Revisa los mensajes de error.")
else:
    print("\nTodas las consultas se ejecutaron correctamente. No se detectaron errores.")

# ---------------------
# 6. Creación de vistas
# ---------------------

print("Creando vistas para el aplicativo...")

# Leer el archivo SQL con la codificación correcta
file_path = '.\src\creacion_vistas.sql'
with open(file_path, 'r', encoding='utf-8') as file:
    sql_script = file.read()

# Ejecutar funciones de ETL
snowflake_analitica.ejecutar_script_sql_snowpark(sesion_activa, sql_script)

print('Vistas materializadas creadas correctamente.')

# ---------------------------
# 7. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()