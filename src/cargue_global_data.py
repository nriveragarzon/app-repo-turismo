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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='GLOBALDATA')

# -----------------------------------------
# 5. Leer y transformar archivos GlobalData
# -----------------------------------------

# Ruta donde están los archivos CSV
path_global_data = './data/GLOBALDATA'

# Lista de nombres de los archivos CSV
nombres_archivos = [
    'Categorias_gasto.csv',
    'Flujo_MICE.csv',
    'Flujo_viajeros_mundo.csv',
    'Flujo_viajeros_region.csv',
    'Forma_viaje.csv',
    'Motivo_viaje.csv',
    'Noches_promedio.csv',
    'Rango_edad.csv'
]

# Lista de columnas que deben ser float64
columnas_float64 = ['VALUE_1', 'VALUE', 'AVERAGE_LENGTH_OF_TRIP_BY_TYPE_DAYS', 'LATITUD_GENERADO', 'LONGITUD_GENERADO']

# Listas de df y nombres
nombres_finales_dfs = []  # Lista para nombres finales de los DataFrames (df_nombrearchivo)
nombres_archivos_mayus = []  # Lista para nombres de archivos sin .csv y en mayúsculas

# Iterar sobre la lista de archivos y cargar cada archivo como un DataFrame
for archivo in nombres_archivos:
    # Crear el nombre del DataFrame basado en el nombre del archivo (sin la extensión .csv), en minúsculas
    nombre_df = f"df_{archivo.split('.')[0].lower()}"
    
    # Agregar el nombre del DataFrame a la lista nombres_finales_dfs
    nombres_finales_dfs.append(nombre_df)
    
    # Agregar el nombre del archivo (sin la extensión .csv) en mayúsculas a nombres_archivos_mayus
    nombres_archivos_mayus.append(archivo.split('.')[0].upper())
    
    # Cargar el archivo CSV en un DataFrame, especificando el separador como ',' y el punto decimal como '.'
    archivo_csv = os.path.join(path_global_data, archivo)
    df = pd.read_csv(archivo_csv, sep=',', decimal='.')
    
    # Limpiar los nombres de las columnas
    df.columns = [snowflake_analitica.clean_column_name(col) for col in df.columns]
    
    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') 
    
    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df.columns if col not in columnas_float64]
    df[columnas_otros] = df[columnas_otros].astype(str)
    
    # Asignar el DataFrame a una variable global con el nombre dinámico
    globals()[nombre_df] = df

    # Mostrar que se ha cargado y limpiado correctamente
    print(f"Archivo {archivo} cargado, nombres de columnas limpiados, columnas especificadas convertidas a float64 en {nombre_df}")


# Elejir solo columnas válidas del df_flujo_viajeros_region
df_flujo_viajeros_region = df_flujo_viajeros_region[['COUNTRY', 'COUNTRY_1', 'COUNTRY_OF_ORIGIN_DESTINATION', 'COUNTRY_OF_ORIGIN_DESTINATION_1', 'YEAR', 'INDEX', 'DATA_POINTS', 'VALUE']]

# --------------------
# 6. Subir a Snowflake
# --------------------

# Cambiar ubicación de la sesión para carga de datos Global Data
snowflake_analitica.update_session_params(sesion_activa,  database='REPOSITORIO_TURISMO', schema='GLOBALDATA')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Loop para subir los DataFrames a Snowflake
for df_name, nombre_tabla in zip(nombres_finales_dfs, nombres_archivos_mayus):
    # Obtener el DataFrame a partir del nombre almacenado en la variable global
    df = globals()[df_name]
    
    # Llamar a la función upload_dataframe_to_snowflake
    mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
        sesion_activa=sesion_activa, 
        df=df, 
        nombre_tabla=nombre_tabla, 
        create_table=True, 
        overwrite=True, 
        ram_gb=32
    )

    # Almacenar el resultado en un diccionario
    resultado = {
        'nombre_tabla': nombre_tabla,
        'df': df_name,
        'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
    }

    # Agregar el resultado a la lista
    resultados_carga.append(resultado)

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)

# Imprimir los mensajes
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
print(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos GlobalData terminado.")

# ---------------------------
# 7. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()

