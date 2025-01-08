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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='OAG')

# ----------------------------------
# 5. Leer y transformar archivos OAG
# ----------------------------------

# Ruta donde están los archivos
path_oag = './data/OAG/Meses/'

# Lista de archivos nuevos para subir
files_oag = snowflake_analitica.validador_cargue(sesion_activa, path_oag, 'OAG')

# Lista de rutas de archivos
rutas_archivos = [path_oag + archivo for archivo in files_oag]

# Hoja de datos
sheet_oag = 'Export'

# Lista de columnas que deben ser float64
columnas_float64 = ['FREQUENCY', 'SEATS_TOTAL']

# Diccionario con los nombres de datos y columnas esperados en la importación
expected_schema = {'columns': ['CARRIER_NAME',
                                'DEP_AIRPORT_CODE',
                                'DEP_AIRPORT_NAME',
                                'DEP_CITY_CODE',
                                'DEP_CITY_NAME',
                                'DEP_IATA_COUNTRY_CODE',
                                'DEP_IATA_COUNTRY_NAME',
                                'ARR_AIRPORT_CODE',
                                'ARR_AIRPORT_NAME',
                                'ARR_CITY_CODE',
                                'ARR_CITY_NAME',
                                'ARR_IATA_COUNTRY_CODE',
                                'ARR_IATA_COUNTRY_NAME',
                                'FREQUENCY',
                                'SEATS_TOTAL',
                                'TIME_SERIES']}

# Mensaje de inicio de proceso de importación
print('Iniciando proceso de importación...')

# Verificar si la lista de archivos está vacía
if not files_oag:
    raise ValueError("No hay archivos válidos para cargar. Verifique la lista de archivos.")

# Iterar sobre los archivos a importar

# Crear una lista vacía para concatenar los datos
dfs_oag = []

# Loop de los archivos válidos (1 o más archivos)
for file_oag in files_oag:
    # Importar datos
    df_oag_insumo = pd.read_excel(path_oag + file_oag, sheet_name=sheet_oag)

    # Limpiar los nombres de las columnas
    df_oag_insumo.columns = [snowflake_analitica.clean_column_name(col) for col in df_oag_insumo.columns]

    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        if col in df_oag_insumo.columns:
            df_oag_insumo[col] = pd.to_numeric(df_oag_insumo[col], errors='coerce') 

    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df_oag_insumo.columns if col not in columnas_float64]
    df_oag_insumo[columnas_otros] = df_oag_insumo[columnas_otros].astype(str)

    # Verificar y actualizar los valores según las condiciones
    # Condición 1: DEP_IATA_COUNTRY_CODE = 'nan' y DEP_IATA_COUNTRY_NAME = 'Namibia'
    if 'DEP_IATA_COUNTRY_CODE' in df_oag_insumo.columns and 'DEP_IATA_COUNTRY_NAME' in df_oag_insumo.columns:
        df_oag_insumo.loc[
            (df_oag_insumo['DEP_IATA_COUNTRY_CODE'] == 'nan') & (df_oag_insumo['DEP_IATA_COUNTRY_NAME'] == 'Namibia'),
            'DEP_IATA_COUNTRY_CODE'
        ] = 'NA'

    # Condición 2: ARR_IATA_COUNTRY_CODE = 'nan' y ARR_IATA_COUNTRY_NAME = 'Namibia'
    if 'ARR_IATA_COUNTRY_CODE' in df_oag_insumo.columns and 'ARR_IATA_COUNTRY_NAME' in df_oag_insumo.columns:
        df_oag_insumo.loc[
            (df_oag_insumo['ARR_IATA_COUNTRY_CODE'] == 'nan') & (df_oag_insumo['ARR_IATA_COUNTRY_NAME'] == 'Namibia'),
            'ARR_IATA_COUNTRY_CODE'
        ] = 'NA'

    # Mostrar que se ha cargado y limpiado correctamente
    print(f"Archivo {file_oag} cargado, nombres de columnas limpiados, columnas especificadas convertidas a float64")

    # Agregar el DataFrame procesado a la lista
    dfs_oag.append(df_oag_insumo)


# Concatenar todos los DataFrames en uno solo para validación
df_oag_validacion = pd.concat(dfs_oag, ignore_index=True)

# Validación de columnas
columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in expected_schema['columns']]
columnas_df = list(df_oag_validacion.columns)
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

# Validar que se está cargando un mes que no esté incluido en la base de datos

# Meses cargados en la base de datos
try:
    df_meses_cargados = pd.DataFrame(sesion_activa.sql("""SELECT DISTINCT A.TIME_SERIES
                                                          FROM REPOSITORIO_TURISMO.OAG.CONECTIVIDAD_DIRECTA AS A
                                                          ORDER BY 1 ASC;""").collect())
    df_meses_cargados = df_meses_cargados['TIME_SERIES'].unique()
except Exception as e:
    print(f"Advertencia: No se pudo consultar la tabla REPOSITORIO_TURISMO.OAG.CONECTIVIDAD_DIRECTA. Detalles: {e}")
    df_meses_cargados = []

# Mes(es) que se va(van) a cargar
df_oag_meses_a_cargar = df_oag_validacion['TIME_SERIES'].unique()

# Encontrar los meses que ya están en la base de datos
meses_duplicados = [mes for mes in df_oag_meses_a_cargar if mes in df_meses_cargados]

# Meses que se van a cargar
print("Meses que se van a cargar:")
print(df_oag_meses_a_cargar)

# Meses que ya están en la base de datos
print("Meses que ya están en la base de datos:")
print(df_meses_cargados)

# Meses repetidos
print("Meses repetidos:")
print(meses_duplicados)

# Variable para registrar errores críticos
errores_criticos = False

# Comprobar si hay meses duplicados
if meses_duplicados:
    print(f"Alerta: Los siguientes meses ya existen en la base de datos y no se pueden cargar: {meses_duplicados}")
    raise ValueError("Se encontraron errores en la validación de los meses. Proceso detenido.")
else:
    print("Validación exitosa: Los meses a cargar no están en la base de datos.")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Loop para subir los DataFrames a Snowflake
for df, nombre_archivo in zip(dfs_oag, rutas_archivos):

    # Cambiar ubicación de la sesión para carga de datos de OAG
    snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='OAG')

    # Obtener números de registros
    obs = len(df)

    # Verificar que la tabla exista en Snowflake
    try:
        tabla_existe = pd.DataFrame(sesion_activa.sql("""SELECT 1 FROM REPOSITORIO_TURISMO.OAG.CONECTIVIDAD_DIRECTA LIMIT 1;""").collect())
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
        sesion_activa=sesion_activa, 
        df=df, 
        nombre_tabla='CONECTIVIDAD_DIRECTA', 
        create_table=create_table_param, 
        overwrite=overwrite_param, 
        ram_gb=32
    )

    # Almacenar el resultado en un diccionario
    resultado = {
        'nombre_tabla': 'CONECTIVIDAD_DIRECTA',
        'df': nombre_archivo,
        'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
    }

    # Agregar el resultado a la lista
    resultados_carga.append(resultado)

    # Cambiar ubicación de la sesión para carga de datos a la tabla de auditoria
    snowflake_analitica.update_session_params(sesion_activa,  database='REPOSITORIO_TURISMO', schema='AUDITORIA')

    # Registrar evento de cargue
    resultado_str = '\n'.join(mensajes)
    snowflake_analitica.registrar_evento_auditoria(sesion_activa=sesion_activa, 
                                                    nombre_esquema_destino='OAG', 
                                                    nombre_tabla='CONECTIVIDAD_DIRECTA', 
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

# -----------------------------------------
# 7. Validar tipos de columnas en Snowflake
# -----------------------------------------

# Mensaje
print("Validando tabla y columnas cargadas...")

# Crear el diccionario de validación de sql
expected_sql_schema = {'CONECTIVIDAD_DIRECTA': {'columns': ['ARR_AIRPORT_CODE',
   'ARR_AIRPORT_NAME',
   'ARR_CITY_CODE',
   'ARR_CITY_NAME',
   'ARR_IATA_COUNTRY_CODE',
   'ARR_IATA_COUNTRY_NAME',
   'CARRIER_NAME',
   'DEP_AIRPORT_CODE',
   'DEP_AIRPORT_NAME',
   'DEP_CITY_CODE',
   'DEP_CITY_NAME',
   'DEP_IATA_COUNTRY_CODE',
   'DEP_IATA_COUNTRY_NAME',
   'FREQUENCY',
   'SEATS_TOTAL',
   'TIME_SERIES'],
  'dtypes': {'ARR_AIRPORT_CODE': 'TEXT',
   'ARR_AIRPORT_NAME': 'TEXT',
   'ARR_CITY_CODE': 'TEXT',
   'ARR_CITY_NAME': 'TEXT',
   'ARR_IATA_COUNTRY_CODE': 'TEXT',
   'ARR_IATA_COUNTRY_NAME': 'TEXT',
   'CARRIER_NAME': 'TEXT',
   'DEP_AIRPORT_CODE': 'TEXT',
   'DEP_AIRPORT_NAME': 'TEXT',
   'DEP_CITY_CODE': 'TEXT',
   'DEP_CITY_NAME': 'TEXT',
   'DEP_IATA_COUNTRY_CODE': 'TEXT',
   'DEP_IATA_COUNTRY_NAME': 'TEXT',
   'FREQUENCY': 'FLOAT',
   'SEATS_TOTAL': 'FLOAT',
   'TIME_SERIES': 'TEXT'}}}

# Obtener una tabla con los tipos de datos de la tabla subida
df_real_schema = pd.DataFrame(sesion_activa.sql("""SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE
                                                        FROM REPOSITORIO_TURISMO.INFORMATION_SCHEMA.COLUMNS AS A
                                                        WHERE A.TABLE_SCHEMA = 'OAG' AND A.TABLE_NAME = 'CONECTIVIDAD_DIRECTA'
                                                        ORDER BY A.TABLE_NAME, A.COLUMN_NAME ASC""").collect())
# Convertir el esquema real en un diccionario para comparar 

# Crear el diccionario esperado
real_sql_schema_dict = {}

# Iterar sobre cada tabla en el DataFrame
for table_name in df_real_schema['TABLE_NAME'].unique():
    # Filtrar las filas correspondientes a la tabla actual
    table_df = df_real_schema[df_real_schema['TABLE_NAME'] == table_name]
    
    # Obtener la lista de columnas
    columns = table_df['COLUMN_NAME'].tolist()
    
    # Crear el diccionario de tipos de datos
    dtypes = dict(zip(table_df['COLUMN_NAME'], table_df['DATA_TYPE']))
    
    # Agregar al diccionario principal
    real_sql_schema_dict[table_name] = {
        'columns': columns,
        'dtypes': dtypes
    }

# Inicializar una lista para almacenar las diferencias
differences = []

# Obtener el conjunto de todas las tablas presentes en ambos esquemas
expected_tables = set(expected_sql_schema.keys())
real_tables = set(real_sql_schema_dict.keys())
all_tables = expected_tables.union(real_tables)

# Comparar los esquemas
for table in all_tables:
    table_differences = {}
    
    # Verificar si la tabla existe en ambos esquemas
    in_expected = table in expected_sql_schema
    in_real = table in real_sql_schema_dict
    
    if not in_real:
        # La tabla falta en el esquema real (diferencia crítica)
        table_differences['missing_in_real'] = True
        differences.append({'table': table, 'differences': table_differences})
        print(f"Tabla '{table}' está en el esquema esperado pero falta en el esquema real")
        continue  # Continuar con la siguiente tabla
    
    if not in_expected:
        # La tabla es adicional en el esquema real
        table_differences['extra_in_real'] = True
        differences.append({'table': table, 'differences': table_differences})
        print(f"Tabla '{table}' está en el esquema real pero no en el esquema esperado")
        continue  # Continuar con la siguiente tabla
    
    # Si la tabla existe en ambos esquemas, comparar columnas
    expected_columns = set(expected_sql_schema[table]['columns'])
    real_columns = set(real_sql_schema_dict[table]['columns'])
    
    missing_columns = expected_columns - real_columns
    extra_columns = real_columns - expected_columns
    
    if missing_columns:
        table_differences['missing_columns'] = list(missing_columns)
        print(f"Tabla '{table}': Columnas faltantes en el esquema real: {missing_columns}")
    
    if extra_columns:
        table_differences['extra_columns'] = list(extra_columns)
        print(f"Tabla '{table}': Columnas adicionales en el esquema real: {extra_columns}")
    
    # Comparar tipos de datos para las columnas comunes
    common_columns = expected_columns.intersection(real_columns)
    dtype_differences = {}
    
    for column in common_columns:
        expected_dtype = expected_sql_schema[table]['dtypes'].get(column)
        real_dtype = real_sql_schema_dict[table]['dtypes'].get(column)
        
        if expected_dtype != real_dtype:
            dtype_differences[column] = {'expected': expected_dtype, 'real': real_dtype}
            print(f"Tabla '{table}', Columna '{column}': Tipo de dato esperado '{expected_dtype}', tipo de dato real '{real_dtype}'")
    
    if dtype_differences:
        table_differences['dtype_differences'] = dtype_differences
    
    # Si hay diferencias en la tabla, agregarlas a la lista de diferencias
    if table_differences:
        differences.append({'table': table, 'differences': table_differences})

# Identificar diferencias críticas
critical_differences = []
for diff in differences:
    table = diff['table']
    diffs = diff['differences']
    if 'missing_in_real' in diffs or 'missing_columns' in diffs or 'dtype_differences' in diffs:
        # Considerar estas diferencias como críticas
        critical_differences.append(diff)

# Mostrar diferencias críticas si las hay
if critical_differences:
    print("\nSe encontraron diferencias críticas:")
    for diff in critical_differences:
        table = diff['table']
        diffs = diff['differences']
        print(f"\nTabla: {table}")
        if 'missing_in_real' in diffs:
            print("  - La tabla está ausente en el esquema real (CRÍTICO).")
        if 'missing_columns' in diffs:
            print(f"  - Columnas faltantes en el esquema real: {diffs['missing_columns']}")
        if 'dtype_differences' in diffs:
            print("  - Diferencias en tipos de datos:")
            for col, types in diffs['dtype_differences'].items():
                print(f"    * Columna '{col}': esperado '{types['expected']}', real '{types['real']}'")
else:
    print("\nNo se encontraron diferencias críticas entre los esquemas.")

# Filtrar filas donde la columna 'mensajes' contiene 'Error' o 'error'
df_errores = df_resultados_carga[df_resultados_carga['mensajes'].str.contains('Error|error', case=False, na=False)]

# Verificar si el DataFrame no está vacío
if not df_errores.empty:
    print("Errores encontrados:")
    print(df_errores)
else:
    print("No se encontraron errores.")

# ---------------------------
# 8. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()