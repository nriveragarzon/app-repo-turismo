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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='OAG')

# ----------------------------------
# 5. Leer y transformar archivos OAG
# ----------------------------------

# Ruta donde está el archivo
path_oag = './data/OAG/'

# Archivo de conectividad
file_oag = 'Conectividad_directa_OAG.xlsx'

# Hoja de datos
sheet_oag = 'Export'

# Importar datos
df_oag = pd.read_excel(path_oag + file_oag, sheet_name=sheet_oag)

# Limpiar los nombres de las columnas
df_oag.columns = [snowflake_analitica.clean_column_name(col) for col in df_oag.columns]

# Lista de columnas que deben ser float64
columnas_float64 = ['FREQUENCY', 'SEATS_TOTAL']

# Convertir las columnas definidas a float64 si existen en el DataFrame
for col in columnas_float64:
    if col in df_oag.columns:
        df_oag[col] = pd.to_numeric(df_oag[col], errors='coerce') 

# Convertir todas las columnas que no están en la lista 'columnas_float64' a string
columnas_otros = [col for col in df_oag.columns if col not in columnas_float64]
df_oag[columnas_otros] = df_oag[columnas_otros].astype(str)

# Mostrar que se ha cargado y limpiado correctamente
print(f"Archivo {file_oag} cargado, nombres de columnas limpiados, columnas especificadas convertidas a float64")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Cambiar ubicación de la sesión para carga de datos OAG
snowflake_analitica.update_session_params(sesion_activa,  database='REPOSITORIO_TURISMO', schema='OAG')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Llamar a la función upload_dataframe_to_snowflake
mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
    sesion_activa=sesion_activa, 
    df=df_oag, 
    nombre_tabla='CONECTIVIDAD_DIRECTA', 
    create_table=True, 
    overwrite=True, 
    ram_gb=32
)

# Almacenar el resultado en un diccionario
resultado = {
    'nombre_tabla': 'CONECTIVIDAD_DIRECTA',
    'df': 'df_oag',
    'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
}

# Agregar el resultado a la lista
resultados_carga.append(resultado)

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)
df_resultados_carga

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos OAG terminado.")

# ---------------------------
# 7. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()