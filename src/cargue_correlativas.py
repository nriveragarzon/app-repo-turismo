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
snowflake_analitica.update_session_params(sesion_activa, database='REPOSITORIO_TURISMO', schema='CORRELATIVAS')

# -------------------------------------------
# 5. Leer y transformar archivos Correlativas
# -------------------------------------------

# Datos del archivo divipola
path_insumos = "./data/GEOGRAFIA/"
divipola_file = 'DIVIPOLA.xlsx'

# Importar datos
df_departamentos_divipola = pd.read_excel(path_insumos + divipola_file, sheet_name="Departamento", dtype=str)
df_municipio_divipola = pd.read_excel(path_insumos + divipola_file, sheet_name="Municipio", dtype=str)
df_departamentos_municipio_divipola = pd.read_excel(path_insumos + divipola_file, sheet_name="Departamento - Municipio", dtype=str)
df_aeropuertos = pd.read_excel(path_insumos + divipola_file, sheet_name="Aeropuertos", dtype=str)

# Nombres de los archivos cargados
df_departamentos_divipola_name = path_insumos + divipola_file + '/' + 'Departamento'
df_municipio_divipola_name = path_insumos + divipola_file + '/' + 'Municipio'
df_departamentos_municipio_divipola_name = path_insumos + divipola_file + '/' + 'Departamento - Municipio'
df_aeropuertos_name = path_insumos + divipola_file + '/' + 'Aeropuertos'

# Datos del modelos relacional de países
correlativa_file = 'MODELO RELACIONAL PAISES.xlsx'

# Importar datos
df_continentes = pd.read_excel(path_insumos + correlativa_file, sheet_name="CONTINENTES", dtype=str)
df_region = pd.read_excel(path_insumos + correlativa_file, sheet_name="REGION", dtype=str)
df_subregion = pd.read_excel(path_insumos + correlativa_file, sheet_name="SUBREGION", dtype=str)
df_paises = pd.read_excel(path_insumos + correlativa_file, sheet_name="PAISES", dtype=str)
df_paises_migracion = pd.read_excel(path_insumos + correlativa_file, sheet_name="MIGRACION", dtype=str)
df_paises_global_data = pd.read_excel(path_insumos + correlativa_file, sheet_name="GLOBALDATA", dtype=str)
df_paises_oag = pd.read_excel(path_insumos + correlativa_file, sheet_name="OAG", dtype=str)
df_paises_forwardkeys = pd.read_excel(path_insumos + correlativa_file, sheet_name="FORWARDKEYS", dtype=str)
df_paises_credibanco = pd.read_excel(path_insumos + correlativa_file, sheet_name="CREDIBANCO", dtype=str)
df_paises_iatagap = pd.read_excel(path_insumos + correlativa_file, sheet_name="IATAGAP", dtype=str)

# Eliminar la columna de validación manual
df_paises_migracion = df_paises_migracion.drop(columns=["¿ESTA_EN_PAISES?"])
df_paises_global_data = df_paises_global_data.drop(columns=["¿ESTA_EN_PAISES?"])
df_paises_oag = df_paises_oag.drop(columns=["¿ESTA_EN_PAISES?"])
df_paises_forwardkeys = df_paises_forwardkeys.drop(columns=["¿ESTA_EN_PAISES?"])
df_paises_credibanco = df_paises_credibanco.drop(columns=["¿ESTA_EN_PAISES?"])
df_paises_iatagap = df_paises_iatagap.drop(columns=["¿ESTA_EN_PAISES?"])

# Nombres de los archivos cargados
df_continentes_name = path_insumos + correlativa_file + '/' + 'CONTINENTES'
df_region_name = path_insumos + correlativa_file + '/' + 'REGION'
df_subregion_name = path_insumos + correlativa_file + '/' + 'SUBREGION'
df_paises_name = path_insumos + correlativa_file + '/' + 'PAISES'
df_paises_migracion_name = path_insumos + correlativa_file + '/' + 'MIGRACION'
df_paises_global_data_name = path_insumos + correlativa_file + '/' + 'GLOBALDATA'
df_paises_oag_name = path_insumos + correlativa_file + '/' + 'OAG'
df_paises_forwardkeys_name = path_insumos + correlativa_file + '/' + 'FORWARDKEYS'
df_paises_credibanco_name = path_insumos + correlativa_file + '/' + 'CREDIBANCO'
df_paises_iatagap_name = path_insumos + correlativa_file + '/' + 'IATAGAP'

# Lista de dfs a subir
bases_de_datos = [
    # DANE
    'df_departamentos_divipola',
    'df_municipio_divipola',
    'df_departamentos_municipio_divipola',
    'df_aeropuertos',
    # PAISES
    'df_continentes',
    'df_region',
    'df_subregion',
    'df_paises',
    'df_paises_migracion',
    'df_paises_global_data',
    'df_paises_oag',
    'df_paises_forwardkeys',
    'df_paises_credibanco',
    'df_paises_iatagap'
]

nombres_tablas = [
    # DANE
    'DIVIPOLA_DEPARTAMENTOS',
    'DIVIPOLA_MUNICIPIOS',
    'DIVIPOLA_DEPARTAMENTOS_MUNICIPIOS',
    'DIVIPOLA_AEROPUERTOS',
    # PAISES
    'CONTINENTES',
    'REGIONES',
    'SUBREGIONES',
    'PAISES',
    'PAISES_MIGRACION',
    'PAISES_GLOBALDATA',
    'PAISES_OAG',
    'PAISES_FORWARDKEYS',
    'PAISES_CREDIBANCO',
    'PAISES_IATAGAP'    
]

nombres_archivos = [
    # DANE
    df_departamentos_divipola_name,
    df_municipio_divipola_name,
    df_departamentos_municipio_divipola_name,
    df_aeropuertos_name,
    # PAISES
    df_continentes_name,
    df_region_name,
    df_subregion_name,
    df_paises_name,
    df_paises_migracion_name,
    df_paises_global_data_name,
    df_paises_oag_name,
    df_paises_forwardkeys_name,
    df_paises_credibanco_name,
    df_paises_iatagap_name
]

# --------------------
# 6. Subir a Snowflake
# --------------------

# Mensaje de inicio de proceso de cargue
print('Iniciando proceso de cargue...')

# Lista para almacenar los resultados de cada carga
resultados_carga = []

# Loop para subir los DataFrames a Snowflake
for df_name, nombre_tabla, nombre_archivo in zip(bases_de_datos, nombres_tablas, nombres_archivos):

    # Cambiar ubicación de la sesión para carga de datos de correlativas
    snowflake_analitica.update_session_params(sesion_activa,  database='REPOSITORIO_TURISMO', schema='CORRELATIVAS')

    # Obtener el DataFrame a partir del nombre almacenado en la variable global
    df = globals()[df_name]

    # Incluir Namibia en base de forward keys
    if df_name == 'df_paises_forwardkeys':
        # Verificar si las columnas necesarias existen en el DataFrame
        if 'COUNTRYCODE' in df.columns and 'COUNTRYNAME' in df.columns:
            # Aplicar la condición y asignar 'NA' a COUNTRYCODE cuando COUNTRYCODE está vacío y COUNTRYNAME es 'Namibia'
            df.loc[
                (df['COUNTRYCODE'].isna() | (df['COUNTRYCODE'] == '')) & (df['COUNTRYNAME'] == 'Namibia'),
                'COUNTRYCODE'
            ] = 'NA'

    # Obtener números de registros
    obs = len(df)

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
        'df': nombre_archivo,
        'mensajes': '\n'.join(mensajes),  # Unir los mensajes en un solo string para la tabla
    }

    # Cambiar ubicación de la sesión para carga de datos a la tabla de auditoria
    snowflake_analitica.update_session_params(sesion_activa,  database='REPOSITORIO_TURISMO', schema='AUDITORIA')

    # Registrar evento de cargue
    resultado_str = '\n'.join(mensajes)
    snowflake_analitica.registrar_evento_auditoria(sesion_activa=sesion_activa, 
                                                   nombre_esquema_destino='CORRELATIVAS', 
                                                   nombre_tabla=nombre_tabla, 
                                                   ruta_archivo=nombre_archivo, 
                                                   numero_registros=obs, 
                                                   mensaje=resultado_str)


    # Agregar el resultado a la lista
    resultados_carga.append(resultado)

    # Mensaje de resultado
    print(f"{nombre_archivo} cargado y auditado.")

# Convertir los resultados en un DataFrame para mostrar de manera organizada
df_resultados_carga = pd.DataFrame(resultados_carga)

# Imprimir los mensajes
cadena_mensajes = '\n'.join(df_resultados_carga['mensajes'])
pprint.pprint(cadena_mensajes)

# Imprimir mensaje de final de proceso
print("Proceso de cague de datos correlativas exitoso.")

# Filtrar filas donde la columna 'mensajes' contiene 'Error' o 'error'
df_errores = df_resultados_carga[df_resultados_carga['mensajes'].str.contains('Error|error', case=False, na=False)]

# Verificar si el DataFrame no está vacío
if not df_errores.empty:
    print("Errores encontrados:")
    print(df_errores)
else:
    print("No se encontraron errores.")

# ---------------------------
# 7. Cerrar sesión y conexión
# ---------------------------
sesion_activa.close()
conexion_activa.close()