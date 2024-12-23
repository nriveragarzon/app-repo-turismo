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
print("Sesiones en Procolombia y Forward Keys creadas correctamente")

# -----------------------------------------
# 4. Obtener archivos de cargue 2024 - 2026
# -----------------------------------------

# Lista de parámetros
param_year_list = [2024, 2025, 2026]
param_month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de extracción de datos desde el servidor de Forward Keys...')

# Loop a través de los parámetros
for param_year in param_year_list:
    for param_month in param_month_list:

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
            A.SEARCH_PAX,
            YEAR(A.SEARCH_DATE) AS YEAR,
            MONTH(A.SEARCH_DATE) AS MONTH
        FROM CUSTOM_SPACE.PUBLIC.PROYECTO_TURISMO_BUSQUEDAS AS A
        WHERE YEAR(A.SEARCH_DATE) = {param_year}
            AND MONTH(A.SEARCH_DATE) = {param_month};
        """

        # Obtener dataframe
        df_mes = pd.DataFrame(sesion_activa_forward_keys.sql(query_month).collect())

        # Verificar si el dataframe está vacío
        if df_mes.empty:
            print(f"No hay datos para el año {param_year} y el mes {param_month}.")
            continue

        # Exportar a CSV del mes consultado
        # Nombre del archivo
        nombre_archivo = f"forward_keys_{param_year}_{param_month:02d}"

        # Exportar
        try:
            df_mes.to_csv(f'./data/FORWARDKEYS_BUSQUEDAS/Meses/{nombre_archivo}.csv', sep='|', index=False)
            # Mensaje de éxito en la exportación
            print(f"{nombre_archivo} exportado correctamente a CSV")
        except Exception as e:
            print(f"Error al exportar {nombre_archivo} a CSV. Detalles: {e}")

# Cerrar sesión en ForwardKeys
sesion_activa_forward_keys.close()
conexion_activa_forward_keys.close()

# Mensaje de cerrar sesión en ForwardKeys
print("Sesión en Forward Keys cerrada correctamente")

# -------------------------------------------
# 5. Leer y transformar archivos Forward Keys
# -------------------------------------------

# Ruta donde están los archivos
path_forward_keys = './data/FORWARDKEYS_BUSQUEDAS/Meses/'

# Lista de archivos nuevos para subir
files_forward_keys = os.listdir(path_forward_keys)

# Archivos a subir
print(f"Los archivos a cargar son: {files_forward_keys}")

# Lista de rutas de archivos
rutas_archivos = [path_forward_keys + archivo for archivo in files_forward_keys]

# Columna que debe ser numérica
columnas_float64 = ['SEARCH_PAX']

# Diccionario con los nombres de datos y columnas esperados en la importación
expected_schema = {'columns': ['SEARCH_DATE',
                                'SEARCH_INTERNATIONAL',
                                'SEARCH_ORIGIN_CITY',
                                'SEARCH_ORIGIN_COUNTRY',
                                'SEARCH_DESTINATION_CITY',
                                'SEARCH_DESTINATION_COUNTRY',
                                'SEGMENT_TYPE',
                                'TRIP_TYPE',
                                'SEARCH_DEPARTURE_DATE',
                                'LOS_AT_DESTINATION_CAT',
                                'SEARCH_PAX',
                                'YEAR',
                                'MONTH']}

# Mensaje de inicio de proceso de importación
print('Iniciando proceso de importación...')

# Verificar si la lista de archivos está vacía
if not files_forward_keys:
    raise ValueError("No hay archivos válidos para cargar. Verifique la lista de archivos.")

# Iterar sobre los archivos a importar

# Crear una lista vacía para concatenar los datos
dfs_fk = []

# Loop de los archivos válidos (1 o más archivos)

for file_fk in files_forward_keys:
    # Importar datos
    df_fk_insumo = pd.read_csv(path_forward_keys + file_fk, sep='|', dtype=str)

    # Limpiar los nombres de las columnas
    df_fk_insumo.columns = [snowflake_analitica.clean_column_name(col) for col in df_fk_insumo.columns]

    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        if col in df_fk_insumo.columns:
            df_fk_insumo[col] = pd.to_numeric(df_fk_insumo[col], errors='coerce') 

    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df_fk_insumo.columns if col not in columnas_float64]
    df_fk_insumo[columnas_otros] = df_fk_insumo[columnas_otros].astype(str)

    # Mostrar que se ha cargado y limpiado correctamente
    print(f"Archivo {file_fk} cargado, nombres de columnas limpiados, columnas especificadas convertidas a float64")

    # Agregar el DataFrame procesado a la lista
    dfs_fk.append(df_fk_insumo)

# Concatenar todos los DataFrames en uno solo para validación
df_fk_validacion = pd.concat(dfs_fk, ignore_index=True)

# Validación de columnas
columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in expected_schema['columns']]
columnas_df = list(df_fk_validacion.columns)
columnas_faltantes = set(columnas_esperadas) - set(columnas_df)
columnas_extras = set(columnas_df) - set(columnas_esperadas)

# Inicializar variable para errores críticos
errores_criticos = False

# Construir el mensaje de validación
mensaje_validacion = "\nResultados de la validación de columnas:\n"
if columnas_faltantes:
    mensaje_validacion += f"  - Columnas faltantes: {sorted(columnas_faltantes)}\n"
    errores_criticos = True
if columnas_extras:
    mensaje_validacion += f"  - Columnas adicionales no esperadas: {sorted(columnas_extras)}\n"
    errores_criticos = True
if not columnas_faltantes and not columnas_extras:
    mensaje_validacion += "  Todas las columnas esperadas están presentes.\n"

# Imprimir resultados de validación
print(mensaje_validacion)

# Si hay errores críticos, detener el proceso
if errores_criticos:
    raise ValueError("Se encontraron errores en la validación de las columnas. Proceso detenido.")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Drop tabla de búsquedas para volverla a cargar desde cero con información actualizada
print("Eliminar la tabla anterior para cargar los datos desde cero con información actualizada")
sesion_activa_procolombia.sql("DROP TABLE IF EXISTS REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS").collect()

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Loop para subir los DataFrames a Snowflake
for df, nombre_archivo in zip(dfs_fk, rutas_archivos):

    # Cambiar ubicación de la sesión para carga de datos de ForwardKeys
    snowflake_analitica.update_session_params(sesion_activa_procolombia,  database='REPOSITORIO_TURISMO', schema='FORWARDKEYS')

    # Obtener números de registros
    obs = len(df)

    # Verificar que la tabla exista en Snowflake
    try:
        tabla_existe = pd.DataFrame(sesion_activa_procolombia.sql("""SELECT 1 FROM REPOSITORIO_TURISMO.FORWARDKEYS.BUSQUEDAS LIMIT 1;""").collect())
        print("La tabla existe. Se procede a cargar los datos.")
        create_table_param = False
        overwrite_param = False
    except Exception as e:
        print(f"Advertencia: La tabla REPOSITORIO_TURISMO.OAG.CONECTIVIDAD_DIRECTA no existe. Detalles: {e}")
        print("Se procede a crear la tabla para insertar los datos.")
        create_table_param = True
        overwrite_param = True

    # Llamar a la función upload_dataframe_to_snowflake
    mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
        sesion_activa=sesion_activa_procolombia, 
        df=df, 
        nombre_tabla='BUSQUEDAS', 
        create_table=create_table_param, 
        overwrite=overwrite_param, 
        ram_gb=32
    )

    # Almacenar el resultado en un diccionario
    resultado = {
        'nombre_tabla': 'BUSQUEDAS',
        'df': nombre_archivo,
        'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
    }

    # Agregar el resultado a la lista
    resultados_carga.append(resultado)

    # Cambiar ubicación de la sesión para carga de datos a la tabla de auditoria
    snowflake_analitica.update_session_params(sesion_activa_procolombia,  database='REPOSITORIO_TURISMO', schema='AUDITORIA')

    # Registrar evento de cargue
    resultado_str = '\n'.join(mensajes)
    snowflake_analitica.registrar_evento_auditoria(sesion_activa=sesion_activa_procolombia, 
                                                    nombre_esquema_destino='FORWARDKEYS', 
                                                    nombre_tabla='BUSQUEDAS', 
                                                    ruta_archivo=nombre_archivo, 
                                                    numero_registros=obs, 
                                                    mensaje=resultado_str)
    # Mensaje de resultado
    print(f"{nombre_archivo} cargado y auditado.")

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)

# Imprimir los mensajes de carga
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
pprint.pprint(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos OAG terminado.")

# ---------------------------
# 7. Cerrar sesión y conexión
# ---------------------------
sesion_activa_procolombia.close()
sesion_activa_procolombia.close()

