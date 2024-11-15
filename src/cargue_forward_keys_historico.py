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
sesion_activa_procolombia, conexion_activa_procolombia = snowflake_analitica.create_session_from_json(json_file_path = json_path)

# -------------------------------------------------------
# 4. Cambiar ubicación a la base de datos del repositorio
# -------------------------------------------------------
snowflake_analitica.update_session_params(sesion_activa_procolombia, database='REPOSITORIO_TURISMO', schema='FORWARDKEYS')

# ------------------------------------------
# 5. Leer y transformar archivos FORWARDKEYS
# ------------------------------------------

# Ruta donde están los archivos
path_forward_keys = './data/FORWARDKEYS'

# Lista de nombres de los archivos CSV
nombres_archivos = [
    'forward_keys_2022.csv',
    'forward_keys_2023.csv',
    'forward_keys_2024.csv'
]

# Columna que debe ser numérica
columnas_float64 = ['SEARCH_PAX']

# Listas de DataFrames
nombres_finales_dfs = []  # Lista para nombres finales de los DataFrames (df_nombrearchivo)

# Iterar sobre la lista de archivos y cargar cada archivo como un DataFrame
for archivo in nombres_archivos:
    
    # Crear el nombre del DataFrame basado en el nombre del archivo (sin la extensión .csv), en minúsculas
    nombre_df = f"df_{archivo.split('.')[0].lower()}"

    # Agregar el nombre del DataFrame a la lista nombres_finales_dfs
    nombres_finales_dfs.append(nombre_df)

    # Cargar el archivo CSV en un DataFrame
    archivo_csv = os.path.join(path_forward_keys, archivo)
    df = pd.read_csv(archivo_csv, sep='|', dtype=str)

    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        col_clean = snowflake_analitica.clean_column_name(col)
        if col_clean in df.columns:
            df[col_clean] = pd.to_numeric(df[col_clean], errors='coerce')

    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df.columns if col not in [snowflake_analitica.clean_column_name(c) for c in columnas_float64]]
    df[columnas_otros] = df[columnas_otros].astype(str)    

    # Asignar el DataFrame a una variable global con el nombre dinámico
    globals()[nombre_df] = df
    
    # Mostrar que se ha cargado y validado correctamente
    print(f"Archivo {archivo} cargado y validado.")

# Concatenar dfs en uno solo
df_forward_keys_2022_202409 = pd.concat([df_forward_keys_2022, df_forward_keys_2023, df_forward_keys_2024])

# Final del proceso
print("Archivos concatenados correctamente")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Llamar a la función upload_dataframe_to_snowflake
mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
    sesion_activa=sesion_activa_procolombia, 
    df=df_forward_keys_2022_202409, 
    nombre_tabla='BUSQUEDAS', 
    create_table=True, 
    overwrite=True, 
    ram_gb=32
)

# Almacenar el resultado en un diccionario
resultado = {
    'nombre_tabla': 'BUSQUEDAS',
    'df': 'df_forward_keys_2022_202409',
    'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
}

# Agregar el resultado a la lista
resultados_carga.append(resultado)

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)

# Imprimir los mensajes de carga
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
pprint.pprint(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos Forward Keys terminado.")

# ----------------------------
# 7. Actualizar tipos de datos
# ----------------------------

# Mensaje del proceso
print("Iniciando proceso de transformación de texto a fechas de las columnas correspondientes")

# Crear queries para actualizar el tipo de columna de fechas a date
query_update = """
-- Paso 1: Crear columnas temporales de tipo DATE
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS ADD COLUMN SEARCH_DATE_TEMP DATE;
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS ADD COLUMN SEARCH_DEPARTURE_DATE_TEMP DATE;

-- Paso 2: Copiar los datos convertidos en formato DATE a las nuevas columnas
UPDATE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS SET SEARCH_DATE_TEMP = TO_DATE(SEARCH_DATE, 'YYYY-MM-DD');
UPDATE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS SET SEARCH_DEPARTURE_DATE_TEMP = TO_DATE(SEARCH_DEPARTURE_DATE, 'YYYY-MM-DD');

-- Paso 3: Eliminar las columnas originales de tipo VARCHAR
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS DROP COLUMN SEARCH_DATE;
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS DROP COLUMN SEARCH_DEPARTURE_DATE;

-- Paso 4: Renombrar las columnas temporales para que tengan los nombres originales
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS RENAME COLUMN SEARCH_DATE_TEMP TO SEARCH_DATE;
ALTER TABLE REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS RENAME COLUMN SEARCH_DEPARTURE_DATE_TEMP TO SEARCH_DEPARTURE_DATE;
"""

snowflake_analitica.ejecutar_script_sql_snowpark(sesion_activa_procolombia, query_update)
  
# ---------------------------
# 8. Cerrar sesión y conexión
# ---------------------------
sesion_activa_procolombia.close()
sesion_activa_procolombia.close()