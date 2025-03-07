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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='IATAGAP')

# --------------------------------------
# 5. Leer y transformar archivos IATAGAP
# --------------------------------------

# Ruta donde está el archivo
path_iata = './data/IATA-GAP/Meses'

# Obtener todas las subcarpetas dentro de path_iata
subcarpetas = os.listdir(path_iata)

# Crear paths con subcarpetas
sub_paths_iata = [path_iata + '/' + subcarpeta for subcarpeta in subcarpetas]

# Crear lista de rutas completas de los archivos en cada subcarpeta
rutas_archivos = []
for sub_path in sub_paths_iata:
    if os.path.isdir(sub_path):
        archivos = os.listdir(sub_path)
        rutas_archivos.extend([sub_path + '/' + archivo for archivo in archivos])

# Hoja de datos
sheet_iata = 'Data'

# Listas de archivos para subir
files_iata = snowflake_analitica.validador_cargue_path(sesion_activa, rutas_archivos, 'IATAGAP')

# Diccionario con los nombres de datos y columnas esperados en la importación
expected_schema = {'columns': ['TRAVEL_AGENCY_NAME',
                                'TRAVEL_AGENCY_CITY',
                                'TRAVEL_AGENCY_COUNTRY',
                                'TRIP_ORIGIN_CITY',
                                'TRIP_ORIGIN_COUNTRY',
                                'TRIP_DESTINATION_COUNTRY',
                                'TOTAL'],
                   'columns_melted':['TRAVEL_AGENCY_NAME', 
                                      'TRAVEL_AGENCY_CITY', 
                                      'TRAVEL_AGENCY_COUNTRY',
                                      'TRIP_ORIGIN_CITY', 
                                      'TRIP_ORIGIN_COUNTRY', 
                                      'TRIP_DESTINATION_COUNTRY',
                                      'YEAR', 
                                      'VALUE']}

# Mensaje de inicio de proceso de importación
print('Iniciando proceso de importación...')

# Verificar si la lista de archivos está vacía
if not files_iata:
    raise ValueError("No hay archivos válidos para cargar. Verifique la lista de archivos.")

# Iterar sobre los archivos a importar

# Crear una lista vacía para concatenar los datos
dfs_iata = []

# Crear lista vacía para concaen

# Loop de los archivos válidos (1 o más archivos)

for file_iata in files_iata:

    ################
    # Importar datos
    ################
    df_iata_insumo = pd.read_excel(file_iata, sheet_name=sheet_iata, skiprows=4)
    
    ########################
    # Validación de columnas
    ########################
    # Limpiar los nombres de las columnas
    df_iata_insumo.columns = [snowflake_analitica.clean_column_name(col) for col in df_iata_insumo.columns]
    
    # Validación de columnas
    columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in expected_schema['columns']]
    columnas_df = list(df_iata_insumo.columns)
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
        mensaje_validacion += f"  - Columnas adicionales o de trimestres: {sorted(columnas_extras)}\n"
        errores_criticos = False

    # Si hay errores críticos, detener el proceso
    if errores_criticos:
        raise ValueError("Se encontraron errores en la validación de las columnas. Proceso detenido.")

    ###########################
    # Proceso de transformación
    ###########################

    # Eliminar la columna de totales
    df_iata_insumo = df_iata_insumo.drop(['TOTAL'], axis=1)

    # Columnas a melt
    id_columns = ['TRAVEL_AGENCY_NAME', 'TRAVEL_AGENCY_CITY', 'TRAVEL_AGENCY_COUNTRY', 'TRIP_ORIGIN_CITY', 'TRIP_ORIGIN_COUNTRY', 'TRIP_DESTINATION_COUNTRY']
    cols_a_melt = list(set(list(df_iata_insumo.columns)) - set(id_columns))
    # Mensaje de columnas para el melt
    pprint.pprint(f"Columnas a realizar melt: {cols_a_melt}")

    # Realizar melt
    df_iata_insumo_melted = pd.melt(
        df_iata_insumo,
        id_vars=id_columns,
        value_vars=cols_a_melt,
        var_name='YEAR',
        value_name='VALUE'
    )

    # Eliminar el carácter "_" de la columna 'YEAR'
    df_iata_insumo_melted['YEAR'] = df_iata_insumo_melted['YEAR'].str.replace('_', '')

    # Transformar en númerica la columna valor
    df_iata_insumo_melted['VALUE'] = df_iata_insumo_melted['VALUE'].astype(float)

    # Mostrar que se ha cargado y limpiado correctamente
    print(f"Archivo {file_iata} cargado, nombres de columnas limpiados, derretido (melted) y columnas especificadas convertidas a float64")

    # Agregar el DataFrame procesado a la lista
    dfs_iata.append(df_iata_insumo_melted)

# Concatenar todos los DataFrames en uno solo para validación
dfs_iata_validacion = pd.concat(dfs_iata, ignore_index=True)

# Validación de columnas
columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in expected_schema['columns_melted']]
columnas_df = list(dfs_iata_validacion.columns)
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

# Validar que se está cargando una combinación de datos que no estén incluidos en la base de datos

# Combinaciones cargadas en la base de datos
try:
    df_combinaciones_cargadas = pd.DataFrame(sesion_activa.sql("""SELECT DISTINCT A.TRIP_ORIGIN_COUNTRY,
                                                            A.TRIP_DESTINATION_COUNTRY,
                                                            A.YEAR
                                                        FROM REPOSITORIO_TURISMO.IATAGAP.AGENCIAS AS A
                                                        ORDER BY 1,2,3 ASC;""").collect())
    # Convertir las combinaciones cargadas en un conjunto de tuplas para comparar
    combinaciones_cargadas = set(df_combinaciones_cargadas[['TRIP_ORIGIN_COUNTRY', 'TRIP_DESTINATION_COUNTRY', 'YEAR']].itertuples(index=False, name=None))
except Exception as e:
    print(f"Advertencia: No se pudo consultar la tabla REPOSITORIO_TURISMO.IATAGAP.AGENCIAS. Detalles: {e}")
    combinaciones_cargadas = set()

# Combinaciones que se van a cargar
nuevas_combinaciones = set(dfs_iata_validacion[['TRIP_ORIGIN_COUNTRY', 'TRIP_DESTINATION_COUNTRY', 'YEAR']].itertuples(index=False, name=None))

# Encontrar combinaciones duplicadas
combinaciones_duplicadas = nuevas_combinaciones.intersection(combinaciones_cargadas)

# Verificar si hay combinaciones duplicadas y detener el proceso si es necesario
errores_criticos = False
if combinaciones_duplicadas:
    errores_criticos = True
    print(f"Error: Las siguientes combinaciones de país de origen, país de destino y año ya existen en la base de datos y no se pueden cargar: {combinaciones_duplicadas}")
else:
    print("Validación exitosa: Las combinaciones a cargar no están en la base de datos.")

# Si hay errores críticos, detener el proceso
if errores_criticos:
    raise ValueError("Se encontraron errores en la validación de las combinaciones. Proceso detenido.")

# --------------------
# 6. Subir a Snowflake
# --------------------

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Loop para subir los DataFrames a Snowflake
for df, nombre_archivo in zip(dfs_iata, rutas_archivos):

    # Cambiar ubicación de la sesión para carga de datos de IATAGAP
    snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='IATAGAP')

    # Obtener números de registros
    obs = len(df)

    # Verificar que la tabla exista en Snowflake
    try:
        tabla_existe = pd.DataFrame(sesion_activa.sql("""SELECT 1 FROM REPOSITORIO_TURISMO.IATAGAP.AGENCIAS LIMIT 1;""").collect())
        print("La tabla existe. Se procede a cargar los datos.")
        create_table_param = False
        overwrite_param = False
    except Exception as e:
        print(f"Advertencia: La tabla REPOSITORIO_TURISMO.IATAGAP.AGENCIAS no existe. Detalles: {e}")
        print("Se procede a crear la tabla para insertar los datos.")
        create_table_param = True
        overwrite_param = True

    # Llamar a la función upload_dataframe_to_snowflake
    mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
        sesion_activa=sesion_activa, 
        df=df, 
        nombre_tabla='AGENCIAS', 
        create_table=create_table_param, 
        overwrite=overwrite_param, 
        ram_gb=32
    )

    # Almacenar el resultado en un diccionario
    resultado = {
        'nombre_tabla': 'AGENCIAS',
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
                                                    nombre_esquema_destino='IATAGAP', 
                                                    nombre_tabla='AGENCIAS', 
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
print("Proceso de cague de datos IATAGAP terminado.")

# -----------------------------------------
# 7. Validar tipos de columnas en Snowflake
# -----------------------------------------

# Mensaje
print("Validando nueva información cargada...")

# Crear el diccionario de validación de sql
expected_sql_schema = {'AGENCIAS': {'columns': ['TRAVEL_AGENCY_CITY',
   'TRAVEL_AGENCY_COUNTRY',
   'TRAVEL_AGENCY_NAME',
   'TRIP_DESTINATION_COUNTRY',
   'TRIP_ORIGIN_CITY',
   'TRIP_ORIGIN_COUNTRY',
   'VALUE',
   'YEAR'],
  'dtypes': {'TRAVEL_AGENCY_CITY': 'TEXT',
   'TRAVEL_AGENCY_COUNTRY': 'TEXT',
   'TRAVEL_AGENCY_NAME': 'TEXT',
   'TRIP_DESTINATION_COUNTRY': 'TEXT',
   'TRIP_ORIGIN_CITY': 'TEXT',
   'TRIP_ORIGIN_COUNTRY': 'TEXT',
   'VALUE': 'FLOAT',
   'YEAR': 'TEXT'}}}

# Obtener una tabla con los tipos de datos de la tabla subida
df_real_schema = pd.DataFrame(sesion_activa.sql("""SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE
                                                        FROM REPOSITORIO_TURISMO.INFORMATION_SCHEMA.COLUMNS AS A
                                                        WHERE A.TABLE_SCHEMA = 'IATAGAP' AND A.TABLE_NAME = 'AGENCIAS'
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