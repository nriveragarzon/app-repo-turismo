# Procesamiento de datos
# Este modulo contiene todas las funciones necesarios para obtener, procesar y limpiar los datos del repositorio de turismo

# Importar módulos necesarios

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

#######################
# Funciones Global Data
#######################

def obtener_datos_global_data(pais_seleccionado, session):
    """
    Ejecuta múltiples consultas relacionadas con Global Data para un país seleccionado y devuelve los resultados.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de las consultas y los valores son DataFrames con los resultados.
    """
    # Diccionario de consultas con el parámetro dinámico `pais_seleccionado`
    consultas = {
        "viajeros_hacia_el_mundo": f"""
            SELECT PAIS,
                MEDIO, 
                YEAR,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_VIAJEROS_MUNDO
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "noches_pernoctacion_promedio": f"""
            SELECT PAIS, 
                YEAR,
                NOCHES
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_NOCHES_PROMEDIO
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "gasto_categorias": f"""
            SELECT PAIS,
                YEAR,
                CATEGORIA_GASTO,
                GASTO
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_CATEGORIAS_GASTO
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "rango_edad": f"""
            SELECT PAIS,
                YEAR, 
                RANGO_EDAD,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_RANGO_EDAD
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "motivo_viaje": f"""
            SELECT PAIS,
                YEAR,
                MOTIVO_VIAJE,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_MOTIVO_VIAJE
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "forma_viaje": f"""
            SELECT PAIS,
                YEAR,
                FORMA_VIAJE,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_FORMA_VIAJE
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "destinos_internacionales": f"""
            SELECT PAIS_ORIGEN,
                PAIS_DESTINO,
                YEAR,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_FLUJOS_VIAJEROS_REGION
            WHERE PAIS_ORIGEN = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """,
        "flujos_negocios": f"""
            SELECT PAIS,
                YEAR,
                MOTIVO_VIAJE,
                VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_MICE
            WHERE PAIS = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026');
        """
    }

    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_consulta, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_consulta}...")
            df_resultado = snowflake_analitica.ejecutar_consulta_segura(query, session)
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_consulta} para el país: {pais_seleccionado}")
            else:
                print(f"Datos obtenidos de {nombre_consulta} para {pais_seleccionado}: {len(df_resultado)} filas.")
            resultados[nombre_consulta] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_consulta}: {str(e)}")
            resultados[nombre_consulta] = pd.DataFrame()  # DataFrame vacío en caso de error

    return resultados

def procesar_datos_global_data(dataframes):
    """
    Procesa los datos obtenidos de Global Data.

    Parámetros:
    - dataframes (dict): Diccionario de DataFrames obtenidos de consultas a Global Data.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    resultados_procesados = {}

    try:
        # Flujo de viajeros al mundo
        df_viajeros_hacia_el_mundo = dataframes.get('viajeros_hacia_el_mundo', pd.DataFrame())
        if not df_viajeros_hacia_el_mundo.empty:
            # Serie de tiempo de viajeros hacia el mundo
            df_viajeros_serie_tiempo = pd.DataFrame(df_viajeros_hacia_el_mundo.groupby('YEAR')['VIAJEROS'].sum()).reset_index()
            resultados_procesados['viajeros_serie_tiempo'] = df_viajeros_serie_tiempo

            # Procesar viajeros por medio
            df_viajeros_medio = df_viajeros_hacia_el_mundo[['MEDIO', 'YEAR', 'VIAJEROS']]
            df_viajeros_medio['TOTAL_ANUAL'] = df_viajeros_medio.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_viajeros_medio['PARTICIPACION'] = (df_viajeros_medio['VIAJEROS'] / df_viajeros_medio['TOTAL_ANUAL']) * 100
            df_viajeros_medio = df_viajeros_medio[['YEAR', 'MEDIO', 'VIAJEROS', 'PARTICIPACION']]
            df_viajeros_medio = df_viajeros_medio.sort_values(by=['YEAR', 'MEDIO'])
            resultados_procesados['viajeros_medio'] = df_viajeros_medio
        else:
            # Devolver DataFrame vacío para la llave si no hay datos
            resultados_procesados['viajeros_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['viajeros_medio'] = pd.DataFrame()

        # Noches de pernoctación
        df_noches_pernoctacion = dataframes.get('noches_pernoctacion_promedio', pd.DataFrame())
        if not df_noches_pernoctacion.empty:
            resultados_procesados['noches_pernoctacion'] = df_noches_pernoctacion[['YEAR', 'NOCHES']]
        else:
            resultados_procesados['noches_pernoctacion'] = pd.DataFrame()

        # Gasto por categorías
        df_categorias_gasto = dataframes.get('gasto_categorias', pd.DataFrame())
        if not df_categorias_gasto.empty:
            # Serie de tiempo de gasto (se está sumando Average Expenditure per Outbound Tourist USD, suma de promedios)
            df_gasto_serie_tiempo = pd.DataFrame(df_categorias_gasto.groupby('YEAR')['GASTO'].sum()).reset_index()
            resultados_procesados['gasto_serie_tiempo'] = df_gasto_serie_tiempo

            # Procesar gasto por categoría
            df_gasto_categoria = df_categorias_gasto[['CATEGORIA_GASTO', 'YEAR', 'GASTO']]
            df_gasto_categoria['TOTAL_ANUAL'] = df_gasto_categoria.groupby('YEAR')['GASTO'].transform('sum')
            df_gasto_categoria['PARTICIPACION'] = (df_gasto_categoria['GASTO'] / df_gasto_categoria['TOTAL_ANUAL']) * 100
            df_gasto_categoria = df_gasto_categoria[['YEAR', 'CATEGORIA_GASTO', 'GASTO', 'PARTICIPACION']]
            df_gasto_categoria = df_gasto_categoria.sort_values(by=['YEAR', 'CATEGORIA_GASTO'])
            resultados_procesados['gasto_categoria'] = df_gasto_categoria
        else:
            resultados_procesados['gasto_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['gasto_categoria'] = pd.DataFrame()

        # Rango de edad (está medido en Thousand)
        df_rango_edad = dataframes.get('rango_edad', pd.DataFrame())
        if not df_rango_edad.empty:
            # Procesar rango de edad
            df_rango_edad['TOTAL_ANUAL'] = df_rango_edad.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_rango_edad['PARTICIPACION'] = (df_rango_edad['VIAJEROS'] / df_rango_edad['TOTAL_ANUAL']) * 100
            df_rango_edad = df_rango_edad[['YEAR', 'RANGO_EDAD', 'VIAJEROS', 'PARTICIPACION']]
            df_rango_edad = df_rango_edad.sort_values(by=['YEAR', 'RANGO_EDAD'])
            resultados_procesados['rango_edad'] = df_rango_edad
        else:
            resultados_procesados['rango_edad'] = pd.DataFrame()

        # Motivo de viaje
        df_motivo_viaje = dataframes.get('motivo_viaje', pd.DataFrame())
        if not df_motivo_viaje.empty:
            # Procesar motivo de viaje
            df_motivo_viaje['TOTAL_ANUAL'] = df_motivo_viaje.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_motivo_viaje['PARTICIPACION'] = (df_motivo_viaje['VIAJEROS'] / df_motivo_viaje['TOTAL_ANUAL']) * 100
            df_motivo_viaje = df_motivo_viaje[['YEAR', 'MOTIVO_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_motivo_viaje = df_motivo_viaje.sort_values(by=['YEAR', 'MOTIVO_VIAJE'])
            resultados_procesados['motivo_viaje'] = df_motivo_viaje
        else:
            resultados_procesados['motivo_viaje'] = pd.DataFrame()

        # Forma de viaje
        df_forma_viaje = dataframes.get('forma_viaje', pd.DataFrame())
        if not df_forma_viaje.empty:
            # Procesar forma de viaje
            df_forma_viaje['TOTAL_ANUAL'] = df_forma_viaje.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_forma_viaje['PARTICIPACION'] = (df_forma_viaje['VIAJEROS'] / df_forma_viaje['TOTAL_ANUAL']) * 100
            df_forma_viaje = df_forma_viaje[['YEAR', 'FORMA_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_forma_viaje = df_forma_viaje.sort_values(by=['YEAR', 'FORMA_VIAJE'])
            resultados_procesados['forma_viaje'] = df_forma_viaje
        else:
            resultados_procesados['forma_viaje'] = pd.DataFrame()

        # Destinos internacionales
        df_destinos = dataframes.get('destinos_internacionales', pd.DataFrame())
        if not df_destinos.empty:
            # Procesar destinos internacionales
            df_destinos['TOTAL_ANUAL'] = df_destinos.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_destinos['PARTICIPACION'] = (df_destinos['VIAJEROS'] / df_destinos['TOTAL_ANUAL']) * 100
            df_destinos = df_destinos.sort_values(by=['YEAR', 'PAIS_DESTINO'])
            resultados_procesados['destinos_internacionales'] = df_destinos
        else:
            resultados_procesados['destinos_internacionales'] = pd.DataFrame()

        # Flujos de negocios
        df_mice = dataframes.get('flujos_negocios', pd.DataFrame())
        if not df_mice.empty:
            # Procesar flujos de negocios (Outbound by MICE Penetration)
            df_mice['TOTAL_ANUAL'] = df_mice.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_mice['PARTICIPACION'] = (df_mice['VIAJEROS'] / df_mice['TOTAL_ANUAL']) * 100
            df_mice = df_mice[['YEAR', 'MOTIVO_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_mice = df_mice.sort_values(by=['YEAR', 'MOTIVO_VIAJE'])
            resultados_procesados['flujos_negocios'] = df_mice
        else:
            resultados_procesados['flujos_negocios'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

###############
# Funciones OAG
###############

def obtener_datos_oag(pais_seleccionado, session):
    """
    Ejecuta múltiples consultas relacionadas con OAG para un país seleccionado y devuelve los resultados.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de las consultas y los valores son DataFrames con los resultados.
    """
    # Diccionario de consultas con el parámetro dinámico `pais_seleccionado`
    consultas = {
        "conectividad_mundo": f"""
            SELECT PAIS_DEPARTURE,
                PAIS_ARRIVAL,
                TIME_SERIES,
                SUBSTR(TIME_SERIES, 1, 4) AS YEAR,
                FRECUENCIAS,
                SILLAS
            FROM REPOSITORIO_TURISMO.VISTAS.OAG_CONECTIVIDAD_MUNDO
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}';
        """,
        "conectividad_hacia_colombia": f"""
            SELECT PAIS_DEPARTURE,
                INITCAP(MUNICIPIO_DANE) AS MUNICIPIO_DANE,
                INITCAP(DEPARTAMENTO_DANE) AS DEPARTAMENTO_DANE,
                TIME_SERIES,
                SUBSTR(TIME_SERIES, 1, 4) AS YEAR,
                FRECUENCIAS,
                SILLAS
            FROM REPOSITORIO_TURISMO.VISTAS.OAG_CONECTIVIDAD_COLOMBIA
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}';
        """
    }

    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_consulta, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_consulta}...")
            df_resultado = snowflake_analitica.ejecutar_consulta_segura(query, session)
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_consulta} para el país: {pais_seleccionado}")
            else:
                print(f"Datos obtenidos de {nombre_consulta} para {pais_seleccionado}: {len(df_resultado)} filas.")
            resultados[nombre_consulta] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_consulta}: {str(e)}")
            resultados[nombre_consulta] = pd.DataFrame()  # DataFrame vacío en caso de error

    return resultados

def procesar_datos_oag(dataframes):
    """
    Procesa los datos obtenidos de OAG.

    Parámetros:
    - dataframes (dict): Diccionario de DataFrames obtenidos de consultas a OAG.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    resultados_procesados = {}

    try:
        # Conectividad del país con el mundo
        df_conectividad_mundo = dataframes.get('conectividad_mundo', pd.DataFrame())
        if not df_conectividad_mundo.empty:
            # Serie de tiempo de conectividad hacia el mundo
            df_conectividad_mundo_serie_tiempo = pd.DataFrame(
                df_conectividad_mundo.groupby('YEAR')[['FRECUENCIAS', 'SILLAS']].sum()
            ).reset_index()
            resultados_procesados['conectividad_mundo_serie_tiempo'] = df_conectividad_mundo_serie_tiempo

            # Procesar conectividad por destino
            df_conectividad_mundo_destino = df_conectividad_mundo[['PAIS_ARRIVAL', 'YEAR', 'FRECUENCIAS', 'SILLAS']]
            # Calcular participaciones porcentuales
            df_conectividad_mundo_destino['TOTAL_ANUAL_FRECUENCIAS'] = df_conectividad_mundo_destino.groupby('YEAR')['FRECUENCIAS'].transform('sum')
            df_conectividad_mundo_destino['PARTICIPACION_FRECUENCIAS'] = (
                df_conectividad_mundo_destino['FRECUENCIAS'] / df_conectividad_mundo_destino['TOTAL_ANUAL_FRECUENCIAS']
            ) * 100
            df_conectividad_mundo_destino['TOTAL_ANUAL_SILLAS'] = df_conectividad_mundo_destino.groupby('YEAR')['SILLAS'].transform('sum')
            df_conectividad_mundo_destino['PARTICIPACION_SILLAS'] = (
                df_conectividad_mundo_destino['SILLAS'] / df_conectividad_mundo_destino['TOTAL_ANUAL_SILLAS']
            ) * 100
            # Ordenar por año y destino
            df_conectividad_mundo_destino = df_conectividad_mundo_destino.sort_values(by=['YEAR', 'PAIS_ARRIVAL'])
            resultados_procesados['conectividad_mundo_destino'] = df_conectividad_mundo_destino
        else:
            resultados_procesados['conectividad_mundo_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['conectividad_mundo_destino'] = pd.DataFrame()

        # Conectividad del país hacia Colombia
        df_conectividad_colombia = dataframes.get('conectividad_hacia_colombia', pd.DataFrame())
        if not df_conectividad_colombia.empty:
            # Serie de tiempo de conectividad hacia Colombia
            df_conectividad_colombia_serie_tiempo = pd.DataFrame(
                df_conectividad_colombia.groupby('YEAR')[['FRECUENCIAS', 'SILLAS']].sum()
            ).reset_index()
            resultados_procesados['conectividad_colombia_serie_tiempo'] = df_conectividad_colombia_serie_tiempo

            # Procesar conectividad por municipio
            df_conectividad_colombia_municipio = df_conectividad_colombia[['MUNICIPIO_DANE', 'YEAR', 'FRECUENCIAS', 'SILLAS']]
            # Calcular participaciones porcentuales
            df_conectividad_colombia_municipio['TOTAL_ANUAL_FRECUENCIAS'] = df_conectividad_colombia_municipio.groupby('YEAR')['FRECUENCIAS'].transform('sum')
            df_conectividad_colombia_municipio['PARTICIPACION_FRECUENCIAS'] = (
                df_conectividad_colombia_municipio['FRECUENCIAS'] / df_conectividad_colombia_municipio['TOTAL_ANUAL_FRECUENCIAS']
            ) * 100
            df_conectividad_colombia_municipio['TOTAL_ANUAL_SILLAS'] = df_conectividad_colombia_municipio.groupby('YEAR')['SILLAS'].transform('sum')
            df_conectividad_colombia_municipio['PARTICIPACION_SILLAS'] = (
                df_conectividad_colombia_municipio['SILLAS'] / df_conectividad_colombia_municipio['TOTAL_ANUAL_SILLAS']
            ) * 100
            # Ordenar por año y municipio
            df_conectividad_colombia_municipio = df_conectividad_colombia_municipio.sort_values(by=['YEAR', 'MUNICIPIO_DANE'])
            resultados_procesados['conectividad_colombia_municipio'] = df_conectividad_colombia_municipio
        else:
            resultados_procesados['conectividad_colombia_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['conectividad_colombia_municipio'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

########################
# Funciones Forward Keys
########################

def obtener_datos_forward_keys(pais_seleccionado, session):
    """
    Ejecuta múltiples consultas relacionadas con Forward Keys para un país seleccionado y devuelve los resultados.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de las consultas y los valores son DataFrames con los resultados.
    """

    # Diccionario de consultas con el parámetro `pais_seleccionado`

    consultas = {
        "reservas_aereas": f"""
            SELECT 
                PAIS_DEPARTURE, 
                PAIS_ARRIVAL,
                FLIGHT_LEG_ARRIVAL_DATE,
                CASE 
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '01' THEN 'ene'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '02' THEN 'feb'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '03' THEN 'mar'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '04' THEN 'abr'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '05' THEN 'may'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '06' THEN 'jun'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '07' THEN 'jul'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '08' THEN 'ago'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '09' THEN 'sep'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '10' THEN 'oct'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '11' THEN 'nov'
                    WHEN TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'MM') = '12' THEN 'dic'
                END || '-' || TO_CHAR(TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD'), 'YY') AS FLIGHT_LEG_ARRIVAL_MONTH_YEAR,
                LOS_AT_DESTINATION_NIGHTS,
                CLASE_CABINA,
                PERFIL_PASAJERO,
                RESERVAS
            FROM REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_RESERVAS_PAISES
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}';
        """,
        "busquedas_aereas": f"""
            SELECT 
                PAIS_DEPARTURE,
                PAIS_ARRIVAL,
                SEARCH_DATE,
                CASE 
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '01' THEN 'ene'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '02' THEN 'feb'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '03' THEN 'mar'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '04' THEN 'abr'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '05' THEN 'may'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '06' THEN 'jun'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '07' THEN 'jul'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '08' THEN 'ago'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '09' THEN 'sep'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '10' THEN 'oct'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '11' THEN 'nov'
                    WHEN TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'MM') = '12' THEN 'dic'
                END || '-' || TO_CHAR(TO_DATE(SEARCH_DATE, 'YYYY-MM-DD'), 'YY') AS SEARCH_DATE_MONTH_YEAR,
                BUSQUEDAS
            FROM REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_BUSQUEDAS_PAISES
            WHERE TO_DATE(SEARCH_DATE, 'YYYY-MM-DD') BETWEEN DATEADD(MONTH, -6, CURRENT_DATE()) AND CURRENT_DATE()
            AND PAIS_DEPARTURE = '{pais_seleccionado}';
        """
    }

    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_consulta, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_consulta}...")
            df_resultado = snowflake_analitica.ejecutar_consulta_segura(query, session)
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_consulta} para el país: {pais_seleccionado}")
            else:
                print(f"Datos obtenidos de {nombre_consulta} para {pais_seleccionado}: {len(df_resultado)} filas.")
            resultados[nombre_consulta] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_consulta}: {str(e)}")
            resultados[nombre_consulta] = pd.DataFrame()  # DataFrame vacío en caso de error

    return resultados

def procesar_datos_forward_keys(dataframes):
    """
    Procesa los datos obtenidos de Forward Keys.

    Parámetros:
    - dataframes (dict): Diccionario de DataFrames obtenidos de consultas a Forward Keys.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    resultados_procesados = {}

    try:
        # Procesar reservas aéreas
        df_reservas = dataframes.get('reservas_aereas', pd.DataFrame())
        if not df_reservas.empty:
            # Serie de tiempo de reservas por país
            df_reservas_serie_tiempo = pd.DataFrame(
                df_reservas.groupby(['FLIGHT_LEG_ARRIVAL_MONTH_YEAR', 'PAIS_ARRIVAL'])['RESERVAS'].sum()
            ).reset_index()

            # Filtrar reservas hacia Colombia
            df_reservas_serie_tiempo_colombia = df_reservas_serie_tiempo[
                df_reservas_serie_tiempo['PAIS_ARRIVAL'] == 'Colombia'
            ]

            resultados_procesados['reservas_serie_tiempo'] = df_reservas_serie_tiempo
            resultados_procesados['reservas_serie_tiempo_colombia'] = df_reservas_serie_tiempo_colombia
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['reservas_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['reservas_serie_tiempo_colombia'] = pd.DataFrame()

        # Procesar búsquedas aéreas
        df_busquedas = dataframes.get('busquedas_aereas', pd.DataFrame())
        if not df_busquedas.empty:
            # Serie de tiempo de búsquedas por país
            df_busquedas_serie_tiempo = pd.DataFrame(
                df_busquedas.groupby(['SEARCH_DATE_MONTH_YEAR', 'PAIS_ARRIVAL'])['BUSQUEDAS'].sum()
            ).reset_index()

           # Filtrar búsquedas hacia Colombia
            df_busquedas_serie_tiempo_colombia = df_busquedas_serie_tiempo[
                df_busquedas_serie_tiempo['PAIS_ARRIVAL'] == 'Colombia'
            ]

            resultados_procesados['busquedas_serie_tiempo'] = df_busquedas_serie_tiempo
            resultados_procesados['busquedas_serie_tiempo_colombia'] = df_busquedas_serie_tiempo_colombia
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['busquedas_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['busquedas_serie_tiempo_colombia'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos de Forward Keys: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

######################
# Funciones Credibanco
######################

def obtener_datos_credibanco(pais_seleccionado, session):
    """
    Ejecuta múltiples consultas relacionadas con Credibanco para un país seleccionado y devuelve los resultados.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de las consultas y los valores son DataFrames con los resultados.
    """

    # Diccionario de consultas con el parámetro `pais_seleccionado`

    consultas = {
        "gasto_tarjeta_credito": f"""
            SELECT ANIO AS YEAR,
                PAIS,
                CATEGORIA,
                CLASIFICACION_CATEGORIA_FORMATADA,
                FACTURACION_COP,
                FACTURACION_USD,
                TURISTAS,
                TRANSACCIONES
            FROM REPOSITORIO_TURISMO.VISTAS.CREDIBANCO_GASTO
            WHERE PAIS = '{pais_seleccionado}';
        """
    }

    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_consulta, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_consulta}...")
            df_resultado = snowflake_analitica.ejecutar_consulta_segura(query, session)
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_consulta} para el país: {pais_seleccionado}")
            else:
                print(f"Datos obtenidos de {nombre_consulta} para {pais_seleccionado}: {len(df_resultado)} filas.")
            resultados[nombre_consulta] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_consulta}: {str(e)}")
            resultados[nombre_consulta] = pd.DataFrame()  # DataFrame vacío en caso de error

    return resultados

def procesar_datos_credibanco(dataframes):
    """
    Procesa los datos obtenidos de Credibanco.

    Parámetros:
    - dataframes (dict): Diccionario de DataFrames obtenidos de consultas a Credibanco.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    resultados_procesados = {}

    try:
        # Gasto con tarjeta de crédito
        df_gasto = dataframes.get('gasto_tarjeta_credito', pd.DataFrame())
        if not df_gasto.empty:
            # Gasto total y cálculo de promedios
            df_gasto_promedio = pd.DataFrame(df_gasto.groupby(['YEAR'])[['FACTURACION_USD', 'TURISTAS', 'TRANSACCIONES']].sum()).reset_index()
            df_gasto_promedio['GASTO_PROMEDIO_TARJETA'] = df_gasto_promedio['FACTURACION_USD'] / df_gasto_promedio['TURISTAS']
            df_gasto_promedio['GASTO_PROMEDIO_TRANSACCION'] = df_gasto_promedio['FACTURACION_USD'] / df_gasto_promedio['TRANSACCIONES']
            resultados_procesados['gasto_promedio'] = df_gasto_promedio

            # Gasto por categoría
            df_gasto_categoria = pd.DataFrame(df_gasto.groupby(['YEAR', 'CLASIFICACION_CATEGORIA_FORMATADA'])[['FACTURACION_USD']].sum()).reset_index()
            df_gasto_categoria['TOTAL_ANUAL'] = df_gasto_categoria.groupby('YEAR')['FACTURACION_USD'].transform('sum')
            df_gasto_categoria['PARTICIPACION'] = (df_gasto_categoria['FACTURACION_USD'] / df_gasto_categoria['TOTAL_ANUAL']) * 100
            df_gasto_categoria = df_gasto_categoria.sort_values(by=['YEAR', 'CLASIFICACION_CATEGORIA_FORMATADA'])
            resultados_procesados['gasto_categoria'] = df_gasto_categoria

            # Gasto por producto
            df_gasto_producto = pd.DataFrame(df_gasto.groupby(['YEAR', 'CATEGORIA'])[['FACTURACION_USD']].sum()).reset_index()
            df_gasto_producto['TOTAL_ANUAL'] = df_gasto_producto.groupby('YEAR')['FACTURACION_USD'].transform('sum')
            df_gasto_producto['PARTICIPACION'] = (df_gasto_producto['FACTURACION_USD'] / df_gasto_producto['TOTAL_ANUAL']) * 100
            df_gasto_producto = df_gasto_producto.sort_values(by=['YEAR', 'CATEGORIA'])
            resultados_procesados['gasto_producto'] = df_gasto_producto
        else:
            # Devolver DataFrames vacíos si no hay datos
            resultados_procesados['gasto_promedio'] = pd.DataFrame()
            resultados_procesados['gasto_categoria'] = pd.DataFrame()
            resultados_procesados['gasto_producto'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

###################
# Funciones IATAGAP
###################

def obtener_datos_iata_gap(pais_seleccionado, session):
    """
    Ejecuta múltiples consultas relacionadas con IATA-GAP para un país seleccionado y devuelve los resultados.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de las consultas y los valores son DataFrames con los resultados.
    """

    # Diccionario de consultas con el parámetro `pais_seleccionado`

    consultas = {
        "indicadores_agencias": f"""
            SELECT PAIS_AGENCIA,
                PAIS_DEPARTURE,
                YY AS YEAR,
                AGENCIAS
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}';
        """,
        "viajeros_agencias": f"""
            SELECT PAIS_AGENCIA,
                PAIS_DEPARTURE,
                YY AS YEAR,
                VIAJEROS_AGENCIAS
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS_VIAJEROS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}';
        """
    }

    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_consulta, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_consulta}...")
            df_resultado = snowflake_analitica.ejecutar_consulta_segura(query, session)
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_consulta} para el país: {pais_seleccionado}")
            else:
                print(f"Datos obtenidos de {nombre_consulta} para {pais_seleccionado}: {len(df_resultado)} filas.")
            resultados[nombre_consulta] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_consulta}: {str(e)}")
            resultados[nombre_consulta] = pd.DataFrame()  # DataFrame vacío en caso de error

    return resultados



