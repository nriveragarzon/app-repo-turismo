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

# Diccionario con los nombres de datos y columnas esperadas en la importación
expected_schema = {'Categorias_gasto.csv': {'columns': ['EXPENDITURE_BY_TOURISM_TYPE',
                                      'REGION',
                                      'COUNTRY',
                                      '_SECTOR_',
                                      'YEAR',
                                      'VALUE_1',
                                      'AXIS',
                                      'UNITS_CUST']},
 'Flujo_MICE.csv': {'columns': ['REGION',
                                'COUNTRY',
                                'SUB_INDICATORS_1',
                                'DATA_POINTS',
                                'YEAR',
                                'VALUE']},
 'Flujo_viajeros_mundo.csv': {'columns': ['REGION',
                                          'COUNTRY',
                                          'DATA_POINTS',
                                          'SUB_INDICATORS_1',
                                          'YEAR',
                                          'VALUE']},
 'Flujo_viajeros_region.csv': {'columns': ['_',
                                           'COUNTRY',
                                           'COUNTRY_1',
                                           'COUNTRY_OF_ORIGIN_DESTINATION',
                                           'COUNTRY_OF_ORIGIN_DESTINATION_1',
                                           'YEAR',
                                           'INDEX',
                                           'DATA_POINTS',
                                           'VALUE']},
 'Forma_viaje.csv': {'columns': ['REGION',
                                 'COUNTRY',
                                 'DATA_POINTS',
                                 'SUB_INDICATORS_1',
                                 'YEAR',
                                 'VALUE']},
 'Motivo_viaje.csv': {'columns': ['REGION',
                                  'COUNTRY',
                                  'PURPOSE',
                                  'YEAR',
                                  'DATA_POINTS',
                                  'VALUE']},
 'Noches_promedio.csv': {'columns': ['COUNTRY',
                                     'SUB_INDICATORS_1_TD_TT_TDF_NOOFOVERNIGHTSTAYS',
                                     'AVERAGE_LENGTH_OF_TRIP_BY_TYPE_DAYS',
                                     'YEAR_COPY',
                                     'LATITUD_GENERADO',
                                     'LONGITUD_GENERADO']},
 'Rango_edad.csv': {'columns': ['REGION',
                                'COUNTRY',
                                'SUB_INDICATORS_1',
                                'YEAR',
                                'UNITS',
                                'VALUE']}}

# Listas de DataFrames y nombres
nombres_finales_dfs = []  # Lista para nombres finales de los DataFrames (df_nombrearchivo)
nombres_archivos_mayus = []  # Lista para nombres de archivos sin .csv y en mayúsculas

# Lista para almacenar resultados de validación
resultados_validacion = []

# Verificar que todos los archivos esperados existen
archivos_en_directorio = os.listdir(path_global_data)
archivos_faltantes = set(nombres_archivos) - set(archivos_en_directorio)
if archivos_faltantes:
    raise FileNotFoundError(f"Los siguientes archivos no se encontraron en la ruta {path_global_data}: {archivos_faltantes}")

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

    # Obtener esquema esperado para este archivo
    esquema_esperado = expected_schema.get(archivo)
    if not esquema_esperado:
        raise ValueError(f"No se encontró esquema esperado para el archivo {archivo} en expected_schema.")

    # Validación de columnas
    columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in esquema_esperado['columns']]
    columnas_df = list(df.columns)
    columnas_faltantes = set(columnas_esperadas) - set(columnas_df)
    columnas_extras = set(columnas_df) - set(columnas_esperadas)

    mensaje_validacion = f"Validando archivo {archivo}:\n"
    if columnas_faltantes:
        mensaje_validacion += f"  Columnas faltantes: {columnas_faltantes}\n"
    if columnas_extras:
        mensaje_validacion += f"  Columnas adicionales: {columnas_extras}\n"
    if not columnas_faltantes and not columnas_extras:
        mensaje_validacion += "  Todas las columnas esperadas están presentes.\n"

    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        col_clean = snowflake_analitica.clean_column_name(col)
        if col_clean in df.columns:
            df[col_clean] = pd.to_numeric(df[col_clean], errors='coerce')

    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df.columns if col not in [snowflake_analitica.clean_column_name(c) for c in columnas_float64]]
    df[columnas_otros] = df[columnas_otros].astype(str)    
    
    # Agregar mensaje de validación a los resultados
    resultados_validacion.append(mensaje_validacion)

    # Asignar el DataFrame a una variable global con el nombre dinámico
    globals()[nombre_df] = df

    # Mostrar que se ha cargado y validado correctamente
    print(f"Archivo {archivo} cargado y validado.")


# Imprimir resultados de validación
print("\nResultados de la validación de archivos:")
for mensaje in resultados_validacion:
    print(mensaje)

# Si hay errores críticos, el proceso se detiene
errores_criticos = False
for mensaje in resultados_validacion:
    if "Columnas faltantes" in mensaje or "Columnas adicionales" in mensaje:
        errores_criticos = True
        break

if errores_criticos:
    raise ValueError("Se encontraron errores en la validación de los archivos. Proceso detenido.")

# Elejir solo columnas válidas del df_flujo_viajeros_region
if 'df_flujo_viajeros_region' in globals():
    df_flujo_viajeros_region = globals()['df_flujo_viajeros_region']
    columnas_validas = ['COUNTRY', 'COUNTRY_1', 'COUNTRY_OF_ORIGIN_DESTINATION', 'COUNTRY_OF_ORIGIN_DESTINATION_1', 'YEAR', 'INDEX', 'DATA_POINTS', 'VALUE']
    df_flujo_viajeros_region = df_flujo_viajeros_region[columnas_validas]

# --------------------
# 6. Subir a Snowflake
# --------------------

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

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

# Imprimir los mensajes de carga
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
pprint.pprint(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cargue de datos GlobalData terminado.")

# -----------------------------------------
# 7. Validar tipos de columnas en Snowflake
# -----------------------------------------

 # Mensaje de inicio de proceso
print("Validando las tablas cargadas...")

# Crear el diccionario de validación de sql
expected_sql_schema = {'CATEGORIAS_GASTO': {'columns': ['AXIS',
   'COUNTRY',
   'EXPENDITURE_BY_TOURISM_TYPE',
   'REGION',
   'UNITS_CUST',
   'VALUE_1',
   'YEAR',
   '_SECTOR_'],
  'dtypes': {'AXIS': 'TEXT',
   'COUNTRY': 'TEXT',
   'EXPENDITURE_BY_TOURISM_TYPE': 'TEXT',
   'REGION': 'TEXT',
   'UNITS_CUST': 'TEXT',
   'VALUE_1': 'FLOAT',
   'YEAR': 'TEXT',
   '_SECTOR_': 'TEXT'}},
 'FLUJO_MICE': {'columns': ['COUNTRY',
   'DATA_POINTS',
   'REGION',
   'SUB_INDICATORS_1',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'DATA_POINTS': 'TEXT',
   'REGION': 'TEXT',
   'SUB_INDICATORS_1': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}},
 'FLUJO_VIAJEROS_MUNDO': {'columns': ['COUNTRY',
   'DATA_POINTS',
   'REGION',
   'SUB_INDICATORS_1',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'DATA_POINTS': 'TEXT',
   'REGION': 'TEXT',
   'SUB_INDICATORS_1': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}},
 'FLUJO_VIAJEROS_REGION': {'columns': ['COUNTRY',
   'COUNTRY_1',
   'COUNTRY_OF_ORIGIN_DESTINATION',
   'COUNTRY_OF_ORIGIN_DESTINATION_1',
   'DATA_POINTS',
   'INDEX',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'COUNTRY_1': 'TEXT',
   'COUNTRY_OF_ORIGIN_DESTINATION': 'TEXT',
   'COUNTRY_OF_ORIGIN_DESTINATION_1': 'TEXT',
   'DATA_POINTS': 'TEXT',
   'INDEX': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}},
 'FORMA_VIAJE': {'columns': ['COUNTRY',
   'DATA_POINTS',
   'REGION',
   'SUB_INDICATORS_1',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'DATA_POINTS': 'TEXT',
   'REGION': 'TEXT',
   'SUB_INDICATORS_1': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}},
 'MOTIVO_VIAJE': {'columns': ['COUNTRY',
   'DATA_POINTS',
   'PURPOSE',
   'REGION',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'DATA_POINTS': 'TEXT',
   'PURPOSE': 'TEXT',
   'REGION': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}},
 'NOCHES_PROMEDIO': {'columns': ['AVERAGE_LENGTH_OF_TRIP_BY_TYPE_DAYS',
   'COUNTRY',
   'LATITUD_GENERADO',
   'LONGITUD_GENERADO',
   'SUB_INDICATORS_1_TD_TT_TDF_NOOFOVERNIGHTSTAYS',
   'YEAR_COPY'],
  'dtypes': {'AVERAGE_LENGTH_OF_TRIP_BY_TYPE_DAYS': 'FLOAT',
   'COUNTRY': 'TEXT',
   'LATITUD_GENERADO': 'FLOAT',
   'LONGITUD_GENERADO': 'FLOAT',
   'SUB_INDICATORS_1_TD_TT_TDF_NOOFOVERNIGHTSTAYS': 'TEXT',
   'YEAR_COPY': 'TEXT'}},
 'RANGO_EDAD': {'columns': ['COUNTRY',
   'REGION',
   'SUB_INDICATORS_1',
   'UNITS',
   'VALUE',
   'YEAR'],
  'dtypes': {'COUNTRY': 'TEXT',
   'REGION': 'TEXT',
   'SUB_INDICATORS_1': 'TEXT',
   'UNITS': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}}}

# Obtener una tabla con los tipos de datos de las tablas subidas 
df_real_schema = pd.DataFrame(sesion_activa.sql("""SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE
                                                        FROM REPOSITORIO_TURISMO.INFORMATION_SCHEMA.COLUMNS AS A
                                                        WHERE A.TABLE_SCHEMA = 'GLOBALDATA'
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

# ---------------------------
# 8. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()

