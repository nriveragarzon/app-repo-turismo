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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='CREDIBANCO')

# -----------------------------------------
# 5. Leer y transformar archivos Credibanco
# -----------------------------------------

# Ruta donde está el archivo
path_credibanco = './data/CREDIBANCO/Meses/'

# Lista de archivos nuevos para subir
files_credibanco = snowflake_analitica.validador_cargue(sesion_activa, path_credibanco, 'CREDIBANCO')

# Lista de rutas de archivos
rutas_archivos = [path_credibanco + archivo for archivo in files_credibanco]

# Lista de columnas que deben ser float64
columnas_float64 = ['FACTURACION_COP', 'FACTURACION_USD', 'TURISTAS', 'TRANSACCIONES', 'TICKET_PROMEDIO_TURISTA', 'TICKET_PROMEDIO_TRANSACCION']

# Diccionario con los nombres de datos y columnas esperados en la importación
expected_schema = {'columns': ['ANIO',
                                'MES',
                                'DEPARTAMENTO_DESTINO',
                                'CIUDAD_DESTINO',
                                'CD_DANE_CIUDAD_DESTINO',
                                'PAIS_ORIGEN',
                                'CATEGORIA',
                                'CLASIFICACION_CATEGORIA',
                                'FACTURACION_COP',
                                'FACTURACION_USD',
                                'TURISTAS',
                                'TRANSACCIONES',
                                'TICKET_PROMEDIO_TURISTA',
                                'TICKET_PROMEDIO_TRANSACCION']}

# Mensaje de inicio de proceso de importación
print('Iniciando proceso de importación...')

# Verificar si la lista de archivos está vacía
if not files_credibanco:
    raise ValueError("No hay archivos válidos para cargar. Verifique la lista de archivos.")

# Iterar sobre los archivos a importar

# Crear una lista vacía para concatenar los datos
dfs_credibanco = []

# Loop de los archivos válidos (1 o más archivos)

for file_credibanco in files_credibanco:
    # Importar datos
    df_credibanco_insumo = pd.read_csv(path_credibanco + file_credibanco, sep=';', decimal='.', dtype=str)

    # Limpiar los nombres de las columnas
    df_credibanco_insumo.columns = [snowflake_analitica.clean_column_name(col) for col in df_credibanco_insumo.columns]

    # Convertir las columnas definidas a float64 si existen en el DataFrame
    for col in columnas_float64:
        if col in df_credibanco_insumo.columns:
            df_credibanco_insumo[col] = pd.to_numeric(df_credibanco_insumo[col], errors='coerce') 

    # Convertir todas las columnas que no están en la lista 'columnas_float64' a string
    columnas_otros = [col for col in df_credibanco_insumo.columns if col not in columnas_float64]
    df_credibanco_insumo[columnas_otros] = df_credibanco_insumo[columnas_otros].astype(str)

    # Mostrar que se ha cargado y limpiado correctamente
    print(f"Archivo {file_credibanco} cargado, nombres de columnas limpiados, columnas especificadas convertidas a float64")

    # Agregar el DataFrame procesado a la lista
    dfs_credibanco.append(df_credibanco_insumo)

# Concatenar todos los DataFrames en uno solo para validación
df_credibanco_validacion = pd.concat(dfs_credibanco, ignore_index=True)

# Validación de columnas
columnas_esperadas = [snowflake_analitica.clean_column_name(col) for col in expected_schema['columns']]
columnas_df = list(df_credibanco_validacion.columns)
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

# Directorio para exportar errores
path_errores = './data/CREDIBANCO/Errores/'

# Iniciar validación por columna e identificación de errores críticos
print('Iniciando validación por columna e identificación de errores críticos...')

# Inicializar la variable de errores críticos
errores_criticos = False

# Lista de años válidos
anios_validos = ['2022', '2023', '2024', '2025']

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'ANIO'
print("Validando la columna 'ANIO'...")
try:
    for df in dfs_credibanco:
        # Identificar años únicos en el DataFrame
        years_list = list(df['ANIO'].unique())

        # Mostrar los años identificados
        print(f"Años identificados en el DataFrame {numero_archivo}: {years_list}")

        # Verificar si hay años no válidos
        anios_no_validos = [anio for anio in years_list if anio not in anios_validos]

        if anios_no_validos:
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con años no válidos
            mask_anios_invalidos = df['ANIO'].isin(anios_no_validos)
            df_errores = df[mask_anios_invalidos]

            # Mostrar los años no válidos
            print(f"Años no válidos encontrados en el DataFrame {numero_archivo}: {anios_no_validos}")
            print("Exportando los errores a un archivo...")

            # Exportar el DataFrame con errores a un archivo CSV
            archivo_errores = os.path.join(path_errores, f'errores_anio_df_{numero_archivo}.csv')
            df_errores.to_csv(archivo_errores, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron años no válidos en el DataFrame {numero_archivo}: {anios_no_validos}")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("Todos los años son válidos. La validación fue exitosa.")

except ValueError as e:
    print(f"Error crítico en la validación de años: {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'MES'

# Lista de meses válidos
meses_validos = [str(i) for i in range(1, 13)] 

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'MES'
print("Validando la columna 'MES'...")
try:
    for df in dfs_credibanco:
        # Identificar meses únicos en el DataFrame
        meses_list = list(df['MES'].unique())

        # Mostrar los meses identificados
        print(f"Meses identificados en el DataFrame {numero_archivo}: {meses_list}")

        # Verificar si hay meses no válidos
        meses_no_validos = [mes for mes in meses_list if mes not in meses_validos]

        if meses_no_validos:
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con meses no válidos
            mask_meses_invalidos = df['MES'].isin(meses_no_validos)
            df_errores = df[mask_meses_invalidos]

            # Mostrar los meses no válidos
            print(f"Meses no válidos encontrados en el DataFrame {numero_archivo}: {meses_no_validos}")
            print("Exportando los errores a un archivo...")

            # Exportar el DataFrame con errores a un archivo CSV
            archivo_errores = os.path.join(path_errores, f'errores_mes_df_{numero_archivo}.csv')
            df_errores.to_csv(archivo_errores, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron meses no válidos en el DataFrame {numero_archivo}: {meses_no_validos}")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("Todos los meses son válidos. La validación fue exitosa.")

except ValueError as e:
    print(f"Error crítico en la validación de meses: {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'DEPARTAMENTO_DESTINO'

# Lista de valores válidos para la columna DEPARTAMENTO_DESTINO
departamentos_validos = [
    'AMAZONAS', 'ANTIOQUIA', 'ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y', 
    'ATLANTICO', 'BOGOTA, D. C.', 'BOLIVAR', 'BOYACA', 'CALDAS', 'CAQUETA',
    'CASANARE', 'CAUCA', 'CESAR', 'CHOCO', 'CORDOBA', 'CUNDINAMARCA',
    'GUAVIARE', 'HUILA', 'LA GUAJIRA', 'MAGDALENA', 'META', 'NARINO',
    'NORTE DE SANTANDER', 'PUTUMAYO', 'QUINDIO', 'RISARALDA',
    'SANTANDER', 'SUCRE', 'TOLIMA', 'VALLE DEL CAUCA', 'GUAINIA',
    'VICHADA', 'ARAUCA', 'nan', 'VAUPES'
]

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'DEPARTAMENTO_DESTINO' y ajustes
print("Validando la columna 'DEPARTAMENTO_DESTINO' y realizando ajustes...")
try:
    for df in dfs_credibanco:
        # Identificar valores únicos en la columna DEPARTAMENTO_DESTINO
        departamentos_list = list(df['DEPARTAMENTO_DESTINO'].unique())
        print(f"Departamentos identificados en el DataFrame {numero_archivo}: {departamentos_list}")

        # Verificar si hay valores no válidos
        departamentos_no_validos = [dep for dep in departamentos_list if dep not in departamentos_validos]

        if departamentos_no_validos:
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con departamentos no válidos
            mask_departamentos_invalidos = df['DEPARTAMENTO_DESTINO'].isin(departamentos_no_validos)
            df_errores = df[mask_departamentos_invalidos]

            # Mostrar los departamentos no válidos
            print(f"Departamentos no válidos encontrados en el DataFrame {numero_archivo}: {departamentos_no_validos}")
            print("Exportando los errores a un archivo...")

            # Exportar el DataFrame con errores a un archivo CSV
            archivo_errores = os.path.join(path_errores, f'errores_departamento_df_{numero_archivo}.csv')
            df_errores.to_csv(archivo_errores, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron departamentos no válidos en el DataFrame {numero_archivo}: {departamentos_no_validos}")

        # Ajuste para la ciudad ITAGÜI
        mask_itagui = (df['CIUDAD_DESTINO'] == 'ITAGÜI') & (df['CD_DANE_CIUDAD_DESTINO'] == '5360') & (df['DEPARTAMENTO_DESTINO'] == 'nan')
        if mask_itagui.any():
            print(f"Se encontraron {mask_itagui.sum()} registros con CIUDAD_DESTINO='ITAGÜI', CD_DANE_CIUDAD_DESTINO='5360' y DEPARTAMENTO_DESTINO='nan'.")
            df.loc[mask_itagui, 'DEPARTAMENTO_DESTINO'] = 'ANTIOQUIA'
            print("Cambio realizado: DEPARTAMENTO_DESTINO actualizado a 'ANTIOQUIA' para estos registros.")

        # Reemplazar 'nan' con 'NO INFORMADO'
        mask_nan = df['DEPARTAMENTO_DESTINO'] == 'nan'
        if mask_nan.any():
            print(f"Se encontraron {mask_nan.sum()} registros con DEPARTAMENTO_DESTINO='nan'.")
            df.loc[mask_nan, 'DEPARTAMENTO_DESTINO'] = 'NO INFORMADO'
            print("Cambio realizado: DEPARTAMENTO_DESTINO actualizado a 'NO INFORMADO' para estos registros.")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("Todos los valores de la columna 'DEPARTAMENTO_DESTINO' son válidos y los ajustes se realizaron correctamente.")

except ValueError as e:
    print(f"Error crítico en la validación de 'DEPARTAMENTO_DESTINO': {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'CIUDAD_DESTINO'

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'CIUDAD_DESTINO'
print("Validando la columna 'CIUDAD_DESTINO'...")
try:
    for df in dfs_credibanco:
        # Identificar filas donde 'CIUDAD_DESTINO' está vacío
        mask_vacios = df['CIUDAD_DESTINO'].isna() | (df['CIUDAD_DESTINO'].str.strip() == '') | df['CIUDAD_DESTINO'].isnull()

        # Verificar si existen valores vacíos
        if mask_vacios.any():
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Obtener las filas con valores vacíos para exportar
            filas_con_errores = df[mask_vacios]
            print(f"Se encontraron {mask_vacios.sum()} registros con valores vacíos en la columna 'CIUDAD_DESTINO'.")
            print("Detalles de los registros con errores:")
            print(filas_con_errores)

            # Exportar el DataFrame con errores a un archivo CSV
            archivo_errores = os.path.join(path_errores, f'errores_ciudad_destino_df_{numero_archivo}.csv')
            filas_con_errores.to_csv(archivo_errores, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron valores vacíos en la columna 'CIUDAD_DESTINO'. Total: {mask_vacios.sum()} registros.")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("La columna 'CIUDAD_DESTINO' pasó la validación correctamente. No se encontraron valores vacíos.")

except ValueError as e:
    print(f"Error crítico en la validación de 'CIUDAD_DESTINO': {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'CD_DANE_CIUDAD_DESTINO'

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'CD_DANE_CIUDAD_DESTINO'
print("Validando la columna 'CD_DANE_CIUDAD_DESTINO'...")
try:
    for df in dfs_credibanco:
        # Verificar si hay valores no numéricos
        mask_no_numerico = ~df['CD_DANE_CIUDAD_DESTINO'].str.isdigit()
        if mask_no_numerico.any():
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con valores no numéricos
            df_errores_no_numericos = df[mask_no_numerico]
            print(f"Se encontraron {mask_no_numerico.sum()} registros con valores no numéricos en 'CD_DANE_CIUDAD_DESTINO'.")
            print("Exportando los errores a un archivo...")

            # Exportar errores
            archivo_errores_no_numericos = os.path.join(path_errores, f'errores_cd_dane_no_numericos_df_{numero_archivo}.csv')
            df_errores_no_numericos.to_csv(archivo_errores_no_numericos, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores_no_numericos}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron valores no numéricos en 'CD_DANE_CIUDAD_DESTINO'. Total: {mask_no_numerico.sum()} registros.")

        # Obtener la distribución por longitud
        longitud_distribucion = df['CD_DANE_CIUDAD_DESTINO'].str.len().value_counts()
        print(f"Distribución por longitud de 'CD_DANE_CIUDAD_DESTINO':\n{longitud_distribucion}")

        # Verificar si hay longitudes mayores a 6
        mask_longitud_invalida = df['CD_DANE_CIUDAD_DESTINO'].str.len() > 6
        if mask_longitud_invalida.any():
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con longitud mayor a 6
            df_errores_longitud = df[mask_longitud_invalida]
            print(f"Se encontraron {mask_longitud_invalida.sum()} registros con longitud mayor a 6 en 'CD_DANE_CIUDAD_DESTINO'.")
            print("Exportando los errores a un archivo...")

            # Exportar errores
            archivo_errores_longitud = os.path.join(path_errores, f'errores_cd_dane_longitud_invalida_df_{numero_archivo}.csv')
            df_errores_longitud.to_csv(archivo_errores_longitud, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores_longitud}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron registros con longitud mayor a 6 en 'CD_DANE_CIUDAD_DESTINO'. Total: {mask_longitud_invalida.sum()} registros.")

        # Rellenar con ceros a la derecha los valores con longitud menor a 5
        mask_rellenar_ceros = df['CD_DANE_CIUDAD_DESTINO'].str.len() < 5
        if mask_rellenar_ceros.any():
            print(f"Se encontraron {mask_rellenar_ceros.sum()} registros con longitud menor a 5 en 'CD_DANE_CIUDAD_DESTINO'.")
            df.loc[mask_rellenar_ceros, 'CD_DANE_CIUDAD_DESTINO'] = df.loc[mask_rellenar_ceros, 'CD_DANE_CIUDAD_DESTINO'].str.zfill(5)
            print("Relleno con ceros a la derecha realizado para estos registros.")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("La columna 'CD_DANE_CIUDAD_DESTINO' pasó la validación correctamente.")

except ValueError as e:
    print(f"Error crítico en la validación de 'CD_DANE_CIUDAD_DESTINO': {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'PAIS_ORIGEN'

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Validación de la columna 'PAIS_ORIGEN'
print("Validando la columna 'PAIS_ORIGEN'...")
try:
    for df in dfs_credibanco:
        # Identificar filas donde 'PAIS_ORIGEN' está vacío
        mask_vacios = df['PAIS_ORIGEN'].isna() | (df['PAIS_ORIGEN'].str.strip() == '') | df['PAIS_ORIGEN'].isnull()

        # Verificar si existen valores vacíos
        if mask_vacios.any():
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con valores vacíos
            df_errores_vacios = df[mask_vacios]
            print(f"Se encontraron {mask_vacios.sum()} registros con valores vacíos en 'PAIS_ORIGEN'.")
            print("Exportando los errores a un archivo...")

            # Exportar errores
            archivo_errores_vacios = os.path.join(path_errores, f'errores_pais_origen_vacios_df_{numero_archivo}.csv')
            df_errores_vacios.to_csv(archivo_errores_vacios, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores_vacios}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron valores vacíos en 'PAIS_ORIGEN'. Total: {mask_vacios.sum()} registros.")

        # Reemplazar 'nan' con 'NO INFORMADO'
        mask_nan = df['PAIS_ORIGEN'] == 'nan'
        if mask_nan.any():
            print(f"Se encontraron {mask_nan.sum()} registros con 'PAIS_ORIGEN' igual a 'nan'.")
            df.loc[mask_nan, 'PAIS_ORIGEN'] = 'NO INFORMADO'
            print(f"Se encontraron {mask_nan.sum()} registros con 'PAIS_ORIGEN' igual a 'nan'.")
            print("Cambio realizado: 'PAIS_ORIGEN' actualizado a 'NO INFORMADO' para estos registros.")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("La columna 'PAIS_ORIGEN' pasó la validación correctamente. No se encontraron errores.")

except ValueError as e:
    print(f"Error crítico en la validación de 'PAIS_ORIGEN': {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Validación de la columna 'CLASIFICACION_CATEGORIA'

# Inicializar el número de archivo para diferenciar exportaciones
numero_archivo = 1

# Lista de valores válidos para la columna CLASIFICACION_CATEGORIA
valores_validos = ['INDIRECTO', 'DIRECTO', 'OTROS']

# Validación de la columna 'CLASIFICACION_CATEGORIA'
print("Validando la columna 'CLASIFICACION_CATEGORIA'...")
try:
    for df in dfs_credibanco:
        # Identificar valores únicos en la columna CLASIFICACION_CATEGORIA
        valores_encontrados = list(df['CLASIFICACION_CATEGORIA'].unique())
        print(f"Valores encontrados en el DataFrame {numero_archivo}: {valores_encontrados}")

        # Verificar si hay valores no válidos
        valores_no_validos = [valor for valor in valores_encontrados if valor not in valores_validos]

        if valores_no_validos:
            # Actualizar la variable de errores críticos
            errores_criticos = True

            # Filtrar las filas con valores no válidos
            mask_valores_invalidos = df['CLASIFICACION_CATEGORIA'].isin(valores_no_validos)
            df_errores = df[mask_valores_invalidos]

            # Mostrar los valores no válidos
            print(f"Valores no válidos encontrados en el DataFrame {numero_archivo}: {valores_no_validos}")
            print("Exportando los errores a un archivo...")

            # Exportar errores
            archivo_errores = os.path.join(path_errores, f'errores_clasificacion_categoria_df_{numero_archivo}.csv')
            df_errores.to_csv(archivo_errores, index=False, encoding='utf-8')
            print(f"Errores exportados correctamente a: {archivo_errores}")

            # Lanzar un error crítico
            raise ValueError(f"Se encontraron valores no válidos en 'CLASIFICACION_CATEGORIA'. Total: {mask_valores_invalidos.sum()} registros.")

        # Incrementar el número de archivo para la siguiente iteración
        numero_archivo += 1

    # Mensaje de validación exitosa
    print("La columna 'CLASIFICACION_CATEGORIA' pasó la validación correctamente. No se encontraron valores no válidos.")

except ValueError as e:
    print(f"Error crítico en la validación de 'CLASIFICACION_CATEGORIA': {e}")
    raise

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso: {e}")
    raise

# Combinaciones cargadas en la base de datos
try:
    df_combinaciones_cargadas = pd.DataFrame(sesion_activa.sql("""SELECT DISTINCT A.ANIO, 
                                                                    A.MES, 
                                                                    A.DEPARTAMENTO_DESTINO, 
                                                                    A.CIUDAD_DESTINO,
                                                                    A.CD_DANE_CIUDAD_DESTINO, 
                                                                    A.PAIS_ORIGEN, 
                                                                    A.CATEGORIA,
                                                                    A.CLASIFICACION_CATEGORIA
                                                                FROM REPOSITORIO_TURISMO.CREDIBANCO.GASTO AS A
                                                                ORDER BY 1, 2, 3, 4, 5, 6, 7, 8 ASC;""").collect())
    # Convertir las combinaciones cargadas en un conjunto de tuplas para comparar
    combinaciones_cargadas = set(df_combinaciones_cargadas[['ANIO', 'MES', 'DEPARTAMENTO_DESTINO', 'CIUDAD_DESTINO', 'CD_DANE_CIUDAD_DESTINO', 'PAIS_ORIGEN', 'CATEGORIA', 'CLASIFICACION_CATEGORIA']].itertuples(index=False, name=None))
except Exception as e:
    print(f"Advertencia: No se pudo consultar la tabla REPOSITORIO_TURISMO.CREDIBANCO.GASTO. Detalles: {e}")
    combinaciones_cargadas = set()

# Combinaciones que se van a cargar
nuevas_combinaciones = set(df_credibanco_validacion[['ANIO', 'MES', 'DEPARTAMENTO_DESTINO', 'CIUDAD_DESTINO', 'CD_DANE_CIUDAD_DESTINO', 'PAIS_ORIGEN', 'CATEGORIA', 'CLASIFICACION_CATEGORIA']].itertuples(index=False, name=None))

# Encontrar combinaciones duplicadas
combinaciones_duplicadas = nuevas_combinaciones.intersection(combinaciones_cargadas)

# Verificar si hay combinaciones duplicadas y detener el proceso si es necesario
errores_criticos = False
if combinaciones_duplicadas:
    errores_criticos = True
    print(f"Error: Las siguientes combinaciones ya existen en la base de datos y no se pueden cargar: {combinaciones_duplicadas}")
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
for df, nombre_archivo in zip(dfs_credibanco, rutas_archivos):

    # Cambiar ubicación de la sesión para carga de datos de Credibanco
    snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='CREDIBANCO')

    # Obtener números de registros
    obs = len(df)

    # Verificar que la tabla exista en Snowflake
    try:
        tabla_existe = pd.DataFrame(sesion_activa.sql("""SELECT 1 FROM REPOSITORIO_TURISMO.CREDIBANCO.GASTO LIMIT 1;""").collect())
        print("La tabla existe. Se procede a cargar los datos.")
        create_table_param = False
        overwrite_param = False
    except Exception as e:
        print(f"Advertencia: La tabla REPOSITORIO_TURISMO.CREDIBANCO.GASTO no existe. Detalles: {e}")
        print("Se procede a crear la tabla para insertar los datos.")
        create_table_param = True
        overwrite_param = True

    # Llamar a la función upload_dataframe_to_snowflake
    mensajes = snowflake_analitica.upload_dataframe_to_snowflake(
        sesion_activa=sesion_activa, 
        df=df, 
        nombre_tabla='GASTO', 
        create_table=create_table_param, 
        overwrite=overwrite_param, 
        ram_gb=32
    )

    # Almacenar el resultado en un diccionario
    resultado = {
        'nombre_tabla': 'GASTO',
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
                                                    nombre_esquema_destino='CREDIBANCO', 
                                                    nombre_tabla='GASTO', 
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
expected_sql_schema = {'GASTO': {'columns': ['ANIO',
   'CATEGORIA',
   'CD_DANE_CIUDAD_DESTINO',
   'CIUDAD_DESTINO',
   'CLASIFICACION_CATEGORIA',
   'DEPARTAMENTO_DESTINO',
   'FACTURACION_COP',
   'FACTURACION_USD',
   'MES',
   'PAIS_ORIGEN',
   'TICKET_PROMEDIO_TRANSACCION',
   'TICKET_PROMEDIO_TURISTA',
   'TRANSACCIONES',
   'TURISTAS'],
  'dtypes': {'ANIO': 'TEXT',
   'CATEGORIA': 'TEXT',
   'CD_DANE_CIUDAD_DESTINO': 'TEXT',
   'CIUDAD_DESTINO': 'TEXT',
   'CLASIFICACION_CATEGORIA': 'TEXT',
   'DEPARTAMENTO_DESTINO': 'TEXT',
   'FACTURACION_COP': 'FLOAT',
   'FACTURACION_USD': 'FLOAT',
   'MES': 'TEXT',
   'PAIS_ORIGEN': 'TEXT',
   'TICKET_PROMEDIO_TRANSACCION': 'FLOAT',
   'TICKET_PROMEDIO_TURISTA': 'FLOAT',
   'TRANSACCIONES': 'FLOAT',
   'TURISTAS': 'FLOAT'}}}

# Obtener una tabla con los tipos de datos de la tabla subida
df_real_schema = pd.DataFrame(sesion_activa.sql("""SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE
                                                        FROM REPOSITORIO_TURISMO.INFORMATION_SCHEMA.COLUMNS AS A
                                                        WHERE A.TABLE_SCHEMA = 'CREDIBANCO' AND A.TABLE_NAME = 'GASTO'
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