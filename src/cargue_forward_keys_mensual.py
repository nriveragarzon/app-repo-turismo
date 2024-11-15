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

# -------------------------------------------------
# 2. Definir archivos de configuración de Snowflake
# -------------------------------------------------
json_path_procolombia = './.streamlit/snowflake_credentials.json'
json_path_forward_keys = './.streamlit/snowflake_credentials_forwardkeys.json'

# --------------------------------------------------------
# 3. Crear sesión y conexión en Procolombia y Forward Keys
# --------------------------------------------------------
sesion_activa_procolombia, conexion_activa_procolombia = snowflake_analitica.create_session_from_json(json_file_path = json_path_procolombia)
sesion_activa_forward_keys, conexion_activa_forward_keys = snowflake_analitica.create_session_from_json(json_file_path = json_path_forward_keys)

# -------------------------------------------------------
# 4. Cambiar ubicación a la base de datos del repositorio
# -------------------------------------------------------
snowflake_analitica.update_session_params(sesion_activa_procolombia, database='REPOSITORIO_TURISMO', schema='FORWARDKEYS')

# ------------------------------------------------------------------
# 5. Obtener base de datos del último mes disponible en Forward Keys
# ------------------------------------------------------------------

# Obtener parámetros

# Consulta para obtener el año máximo
query_year = """
SELECT MAX(YEAR(A.SEARCH_DATE))
FROM REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS AS A;
"""

# Año máximo cargado en el repositorio
max_year = pd.DataFrame(sesion_activa_procolombia.sql(query_year).collect())
max_year = max_year.iloc[0, 0]

# Año máximo cargado
print(f"El año maximo cargado en el repositorio es {max_year}")

# Usar el año máximo como parámetro
print(f"Usando {2024} como año para identificar el máximo mes cargado")

# Consulta para obtener el mes máximo
query_month = f"""
SELECT MAX(MONTH(A.SEARCH_DATE))
FROM REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS AS A
WHERE YEAR(A.SEARCH_DATE) = {max_year};
"""

# Mes máximo cargado en el repositorio
max_month = pd.DataFrame(sesion_activa_procolombia.sql(query_month).collect())
max_month = max_month.iloc[0, 0]

# Mes máximo cargado
print(f"El mes maximo cargado en el repositorio es {max_month}")

# Conclusión de identificación del mes y año máximo en el repositorio
print(f"El corte de información cargada en el repositorio es {max_year}-{max_month}")

# Determinar el siguiente mes y el año correspondiente
if max_month == 12:
    param_year = max_year + 1
    param_month = 1
else:
    param_year = max_year
    param_month = max_month + 1

# Nuevos parámetros para cargar información desde Forward Keys
print(f"Los nuevos parámetros para cargar la información son {param_year}-{param_month}")

# Obtener información con los nuevos parámetros

# Consulta del mes a actualizar
query_month = f"""
SELECT A.SEARCH_DATE,
    A.SEARCH_INTERNATIONAL,
    A.SEARCH_ORIGIN_CITY,
    A.SEARCH_ORIGIN_COUNTRY,
    A.SEARCH_DESTINATION_CITY,
    A.SEARCH_DESTINATION_COUNTRY,
    A.SEGMENT_TYPE,
    A.TRIP_TYPE,
    A.SEARCH_DEPARTURE_DATE,
    A.LOS_AT_DESTINATION_CAT,
    A.SEARCH_PAX
FROM CUSTOM_SPACE.PUBLIC.PROYECTO_TURISMO_BUSQUEDAS AS A
WHERE YEAR(A.SEARCH_DATE) = {param_year}
    AND MONTH(A.SEARCH_DATE) = {param_month};
"""

# Obtener dataframe
df_mes = pd.DataFrame(sesion_activa_forward_keys.sql(query_month).collect())

# Cerrar sesión en ForwardKeys
sesion_activa_forward_keys.close()
conexion_activa_forward_keys.close()

# Mensaje de cerrar sesión en ForwardKeys
print("Sesión en Forward Keys cerrada correctamente")

# Exportar a CSV el mes en curso

# Nombre del archivo
nombre_archivo = "forward_keys_" + str(param_year) + str(param_month)

# Exportar
df_mes.to_csv(f'./data/FORWARDKEYS/Meses/{nombre_archivo}.csv', sep='|', index=False)

# Mensaje de éxito en la exportación
print(f"{nombre_archivo} exportado correctamente a CSV")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Ruta donde están los archivos CSV
path_forward_keys = './data/FORWARDKEYS/Meses'

# Columna que debe ser numérica
columnas_float64 = ['SEARCH_PAX']

# Cargar el archivo CSV en un DataFrame
archivo_csv = os.path.join(path_forward_keys, nombre_archivo + '.csv')
df = pd.read_csv(archivo_csv, sep='|', dtype=str)

# Convertir las columnas definidas a float64 si existen en el DataFrame
for col in columnas_float64:
    col_clean = snowflake_analitica.clean_column_name(col)
    if col_clean in df.columns:
        df[col_clean] = pd.to_numeric(df[col_clean], errors='coerce')

# Convertir todas las columnas que no están en la lista 'columnas_float64' a string
columnas_otros = [col for col in df.columns if col not in [snowflake_analitica.clean_column_name(c) for c in columnas_float64]]
df[columnas_otros] = df[columnas_otros].astype(str)    

# Mostrar que se ha cargado y validado correctamente
print(f"Archivo {nombre_archivo} cargado y validado.")

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Llamar a la función upload_dataframe_to_snowflake
mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
    sesion_activa=sesion_activa_procolombia, 
    df=df, 
    nombre_tabla='BUSQUEDAS', 
    create_table=False, 
    overwrite=False, 
    ram_gb=32
)

# Almacenar el resultado en un diccionario
resultado = {
    'nombre_tabla': 'BUSQUEDAS',
    'df': 'df',
    'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
}

# Agregar el resultado a la lista
resultados_carga.append(resultado)

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)

# Imprimir los mensajes
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
pprint.pprint(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos Forward Keys terminado.")

# ---------------------------
# 8. Cerrar sesión y conexión
# ---------------------------
sesion_activa_procolombia.close()
sesion_activa_procolombia.close()