# Procesamiento de datos

# Este modulo contiene todas las funciones necesarios para obtener, procesar y limpiar los datos del repositorio de turismo

# Importar módulos necesarios

import src.snowflake_analitica as snowflake_analitica
from src.streamlit_analitica import formato_miles

# Warnings
import warnings

# OS
import os

# Importar pandas
import pandas as pd

# Prettyprint
import pprint

# Locale
import locale

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
                SUM(VIAJEROS) AS VIAJEROS
            FROM REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_FLUJOS_VIAJEROS_REGION
            WHERE PAIS_ORIGEN = '{pais_seleccionado}'
            AND YEAR IN ('2022', '2023', '2024', '2025', '2026')
            GROUP BY PAIS_ORIGEN,
                PAIS_DESTINO,
                YEAR;
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

   # Iniciar procesamiento de los datos (se usa la subfunción que se creó en el paso anterior)
          
    resultados_procesados = {}

    try:
        # Flujo de viajeros al mundo
        df_viajeros_hacia_el_mundo = dataframes.get('viajeros_hacia_el_mundo', pd.DataFrame())
        if not df_viajeros_hacia_el_mundo.empty:
            # Serie de tiempo de viajeros hacia el mundo
            df_viajeros_serie_tiempo = pd.DataFrame(df_viajeros_hacia_el_mundo.groupby('YEAR')['VIAJEROS'].sum()).reset_index()
            # Cambiar nombres de columnas 
            df_viajeros_serie_tiempo = df_viajeros_serie_tiempo.rename(columns={'YEAR' : 'Año', 'VIAJEROS' : 'Viajeros'})
            resultados_procesados['viajeros_serie_tiempo'] = df_viajeros_serie_tiempo

            # Procesar viajeros por medio
            df_viajeros_medio = df_viajeros_hacia_el_mundo[['MEDIO', 'YEAR', 'VIAJEROS']]
            df_viajeros_medio['TOTAL_ANUAL'] = df_viajeros_medio.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_viajeros_medio['PARTICIPACION'] = (df_viajeros_medio['VIAJEROS'] / df_viajeros_medio['TOTAL_ANUAL']) * 100
            # Filtrar columnas relevantes
            df_viajeros_medio = df_viajeros_medio[['YEAR', 'MEDIO', 'VIAJEROS', 'PARTICIPACION']]
            df_viajeros_medio = df_viajeros_medio.sort_values(by=['YEAR', 'MEDIO'])
            # Cambiar nombres de columnas
            df_viajeros_medio = df_viajeros_medio.rename(columns={'YEAR' : 'Año', 'MEDIO' : 'Medio de transporte', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['viajeros_medio'] = df_viajeros_medio
        else:
            # Devolver DataFrame vacío para la llave si no hay datos
            resultados_procesados['viajeros_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['viajeros_medio'] = pd.DataFrame()

        # Noches de pernoctación
        df_noches_pernoctacion = dataframes.get('noches_pernoctacion_promedio', pd.DataFrame())
        if not df_noches_pernoctacion.empty:
            # Cambiar nombres de columnas
            df_noches_pernoctacion = df_noches_pernoctacion.rename(columns={'PAIS' : 'País', 'YEAR' : 'Año', 'NOCHES' : 'Noches de percnotación'})
            resultados_procesados['noches_pernoctacion'] = df_noches_pernoctacion
        else:
            resultados_procesados['noches_pernoctacion'] = pd.DataFrame()

        # Gasto por categorías
        df_categorias_gasto = dataframes.get('gasto_categorias', pd.DataFrame())
        if not df_categorias_gasto.empty:
            # Serie de tiempo de gasto (se está sumando Average Expenditure per Outbound Tourist USD, suma de promedios)
            df_gasto_serie_tiempo = pd.DataFrame(df_categorias_gasto.groupby('YEAR')['GASTO'].sum()).reset_index()
            # Cambiar nombres de columnas
            df_gasto_serie_tiempo = df_gasto_serie_tiempo.rename(columns={'YEAR' : 'Año', 'GASTO' : 'Gasto (USD)'})            
            resultados_procesados['gasto_serie_tiempo'] = df_gasto_serie_tiempo

            # Procesar gasto por categoría
            df_gasto_categoria = df_categorias_gasto[['CATEGORIA_GASTO', 'YEAR', 'GASTO']]
            df_gasto_categoria['TOTAL_ANUAL'] = df_gasto_categoria.groupby('YEAR')['GASTO'].transform('sum')
            df_gasto_categoria['PARTICIPACION'] = (df_gasto_categoria['GASTO'] / df_gasto_categoria['TOTAL_ANUAL']) * 100
            df_gasto_categoria = df_gasto_categoria[['YEAR', 'CATEGORIA_GASTO', 'GASTO', 'PARTICIPACION']]
            df_gasto_categoria = df_gasto_categoria.sort_values(by=['YEAR', 'CATEGORIA_GASTO'])
            # Cambiar nombres de columnas
            df_gasto_categoria = df_gasto_categoria.rename(columns={'YEAR' : 'Año', 'CATEGORIA_GASTO' : 'Categoria de Gasto', 'GASTO' : 'Gasto (USD)', 'PARTICIPACION' : 'Participación (%)'})
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
            # Cambiar nombres de columnas
            df_rango_edad = df_rango_edad.rename(columns = {'YEAR' : 'Año', 'RANGO_EDAD' : 'Rango de Edad', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['rango_edad'] = df_rango_edad
        else:
            resultados_procesados['rango_edad'] = pd.DataFrame()

        # Motivo de viaje
        df_motivo_viaje = dataframes.get('motivo_viaje', pd.DataFrame())
        if not df_motivo_viaje.empty:
            # Procesar motivo de viaje
            df_motivo_viaje['TOTAL_ANUAL'] = df_motivo_viaje.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_motivo_viaje['PARTICIPACION'] = (df_motivo_viaje['VIAJEROS'] / df_motivo_viaje['TOTAL_ANUAL']) * 100
            # Filtrar columnas relevantes
            df_motivo_viaje = df_motivo_viaje[['YEAR', 'MOTIVO_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_motivo_viaje = df_motivo_viaje.sort_values(by=['YEAR', 'MOTIVO_VIAJE'])
            # Cambiar nombres de columnas
            df_motivo_viaje = df_motivo_viaje.rename(columns = {'YEAR' : 'Año', 'MOTIVO_VIAJE' : 'Motivo de Viaje', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['motivo_viaje'] = df_motivo_viaje
        else:
            resultados_procesados['motivo_viaje'] = pd.DataFrame()

        # Forma de viaje
        df_forma_viaje = dataframes.get('forma_viaje', pd.DataFrame())
        if not df_forma_viaje.empty:
            # Procesar forma de viaje
            df_forma_viaje['TOTAL_ANUAL'] = df_forma_viaje.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_forma_viaje['PARTICIPACION'] = (df_forma_viaje['VIAJEROS'] / df_forma_viaje['TOTAL_ANUAL']) * 100
            # Filtrar columnas relevantes
            df_forma_viaje = df_forma_viaje[['YEAR', 'FORMA_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_forma_viaje = df_forma_viaje.sort_values(by=['YEAR', 'FORMA_VIAJE'])
            # Cambiar nombres de columnas
            df_forma_viaje = df_forma_viaje.rename(columns = {'YEAR' : 'Año', 'FORMA_VIAJE' : 'Forma de Viaje', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['forma_viaje'] = df_forma_viaje
        else:
            resultados_procesados['forma_viaje'] = pd.DataFrame()

        # Destinos internacionales
        df_destinos = dataframes.get('destinos_internacionales', pd.DataFrame())
        if not df_destinos.empty:
            # Procesar destinos internacionales
            df_destinos['TOTAL_ANUAL'] = df_destinos.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_destinos['PARTICIPACION'] = (df_destinos['VIAJEROS'] / df_destinos['TOTAL_ANUAL']) * 100
            # Filtrar columnas relevantes
            df_destinos = df_destinos[['YEAR', 'PAIS_DESTINO', 'VIAJEROS', 'PARTICIPACION']]
            df_destinos = df_destinos.sort_values(by=['YEAR', 'PAIS_DESTINO'])
            
            # Crear agrupación de Top 10 para gráficar (El código sigue diciendo top 5 por construcción inicial por facilidad, pero puede cambiar el nlargest a x)
            # Sumar VIAJEROS por país para el periodo total
            suma_viajeros_por_pais = df_destinos.groupby('PAIS_DESTINO')['VIAJEROS'].sum().reset_index()

            # Obtener el top 10 de países con más viajeros
            top5_paises = suma_viajeros_por_pais.nlargest(10, 'VIAJEROS')['PAIS_DESTINO']

            # Obtener número de países
            num_paises = len(suma_viajeros_por_pais['PAIS_DESTINO'].unique())

            # Crear el nuevo DataFrame con top 10 y agrupar los demás bajo "Otros"
            if num_paises <= 10:
                # Si hay 10 países o menos, devolver el DataFrame original sin cambios
                df_destinos_top5 = df_destinos.copy()
            else:
                # Filtrar por los países en el top 10 y los demás como "Otros"
                df_destinos['PAIS_DESTINO'] = df_destinos['PAIS_DESTINO'].apply(
                    lambda x: x if x in top5_paises.values else 'Otros'
                )

                # Agrupar por país y año, sumando VIAJEROS y PARTICIPACION
                df_destinos_top5 = df_destinos.groupby(['YEAR', 'PAIS_DESTINO'], as_index=False).agg({
                    'VIAJEROS': 'sum',
                    'PARTICIPACION': 'sum'
                })
            # Cambiar nombres de columnas
            df_destinos_top5 = df_destinos_top5.rename(columns = {'YEAR' : 'Año', 'PAIS_DESTINO' : 'País Destino', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            # Agregar top5 a los resultados
            resultados_procesados['destinos_internacionales_top5'] = df_destinos_top5
        else:
            resultados_procesados['destinos_internacionales_top5'] = pd.DataFrame()

        # Flujos de negocios
        df_mice = dataframes.get('flujos_negocios', pd.DataFrame())
        if not df_mice.empty:
            # Procesar flujos de negocios (Outbound by MICE Penetration)
            df_mice['TOTAL_ANUAL'] = df_mice.groupby('YEAR')['VIAJEROS'].transform('sum')
            df_mice['PARTICIPACION'] = (df_mice['VIAJEROS'] / df_mice['TOTAL_ANUAL']) * 100
            # Filtrar columnas relevantes
            df_mice = df_mice[['YEAR', 'MOTIVO_VIAJE', 'VIAJEROS', 'PARTICIPACION']]
            df_mice = df_mice.sort_values(by=['YEAR', 'MOTIVO_VIAJE'])
            # Cambiar nombres de columnas
            df_mice = df_mice.rename(columns={'YEAR' : 'Año', 'MOTIVO_VIAJE' : 'Motivo de viaje', 'VIAJEROS' : 'Viajeros', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['flujos_negocios'] = df_mice
        else:
            resultados_procesados['flujos_negocios'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados


def datos_global_data(pais_seleccionado, sesion_activa):
    """
    Obtiene y procesa los datos de Global Data para un país seleccionado.

    Esta función combina la obtención y el procesamiento de los datos de Global Data 
    para un país específico utilizando las funciones `obtener_datos_global_data` y 
    `procesar_datos_global_data`.

    Parámetros:
    - pais_seleccionado (str): Nombre del país para el cual se desean obtener los datos.
    - sesion_activa: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados por categoría.

    Manejo de errores:
    - Si ocurre un error durante la obtención o procesamiento de los datos, se imprimirá 
      un mensaje de error y se retornará un diccionario vacío.
    """

    try:
        # Obtener los datos de Global Data
        print(f"Iniciando la obtención de datos para {pais_seleccionado}...")
        datos = obtener_datos_global_data(pais_seleccionado, sesion_activa)

        # Validar si se obtuvieron datos
        if not datos:
            print(f"No se obtuvieron datos para el país seleccionado: {pais_seleccionado}")
            return {}

        # Procesar los datos obtenidos
        print("Procesando los datos obtenidos...")
        datos_procesados = procesar_datos_global_data(datos)

        # Validar si el procesamiento fue exitoso
        if not datos_procesados:
            print(f"El procesamiento de datos falló para el país: {pais_seleccionado}")
            return {}

        print("Obtención y procesamiento de datos completados exitosamente.")
        return datos_procesados

    except Exception as e:
        # Manejo de errores generales
        print(f"Error durante la ejecución de 'datos_global_data': {str(e)}")
        return {}

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
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}'
                AND PAIS_ARRIVAL <> '{pais_seleccionado}';
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
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}'
                AND PAIS_ARRIVAL <> '{pais_seleccionado}';
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
            # Cambiar nombres de columnas
            df_conectividad_mundo_serie_tiempo = df_conectividad_mundo_serie_tiempo.rename(columns={'YEAR': 'Año', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas'})
            resultados_procesados['conectividad_mundo_serie_tiempo'] = df_conectividad_mundo_serie_tiempo

            # Procesar conectividad por destino
            # Cerrado y acumulado por destino
            df_conectividad_mundo_serie_tiempo_mensual = df_conectividad_mundo[['YEAR', 'PAIS_ARRIVAL', 'TIME_SERIES', 'FRECUENCIAS','SILLAS']]

            ###############
            # PASO 1: TOP 5
            ###############

            # Convertir TIME_SERIES a datetime para facilitar el manejo temporal
            df_conectividad_mundo_serie_tiempo_mensual['TIME_SERIES'] = pd.to_datetime(df_conectividad_mundo_serie_tiempo_mensual['TIME_SERIES'], format='%Y-%m')

            # Obtener el último año disponible
            ultimo_anio = df_conectividad_mundo_serie_tiempo_mensual['TIME_SERIES'].dt.year.max()

            # Filtrar datos solo del último año
            df_cerrado = df_conectividad_mundo_serie_tiempo_mensual[df_conectividad_mundo_serie_tiempo_mensual['TIME_SERIES'].dt.year == ultimo_anio]

            # Sumar FRECUENCIAS por país para el último año
            frecuencias_por_pais = df_cerrado.groupby('PAIS_ARRIVAL')['FRECUENCIAS'].sum().reset_index()

            # Seleccionar el top 5 de países con mayor FRECUENCIAS
            top_5_paises = frecuencias_por_pais.nlargest(10, 'FRECUENCIAS')['PAIS_ARRIVAL']

            # Obtener el número de países
            num_paises = len(frecuencias_por_pais['PAIS_ARRIVAL'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_paises <= 10:
                # Si hay 5 países o menos, no se agrupan bajo "Otros"
                df_top_otros = df_conectividad_mundo_serie_tiempo_mensual.copy()
            else:
                # Si hay más de 5 países, agrupar los demás como "Otros"
                df_top_otros = df_conectividad_mundo_serie_tiempo_mensual.copy()
                df_top_otros['PAIS_ARRIVAL'] = df_top_otros['PAIS_ARRIVAL'].apply(
                    lambda pais: pais if pais in top_5_paises.values else 'Otros'
                )

                # Agrupar por mes (TIME_SERIES) y país, sumando FRECUENCIAS y SILLAS
                df_top_otros = df_top_otros.groupby(['TIME_SERIES', 'PAIS_ARRIVAL'], as_index=False).agg({
                    'FRECUENCIAS': 'sum',
                    'SILLAS': 'sum'
                })
            
            #######################################
            # PASO 2: PERIODO CERRADO (2022 - 2023)
            #######################################

            # Obtener columnas de fecha
            df_top_otros['FECHA'] = df_top_otros['TIME_SERIES'].dt.year

            # Filtrar años de interés
            df_distribucion_cerrado = df_top_otros[df_top_otros['FECHA'].isin([2022, 2023])].copy()

            # Crear agrupación de años cerrados
            totales_cerrado = df_distribucion_cerrado.groupby(['FECHA', 'PAIS_ARRIVAL'])[['FRECUENCIAS', 'SILLAS']].sum().reset_index()

            # Calcular el total de frecuencias y sillas por año
            totales_anuales = totales_cerrado.groupby('FECHA')[['FRECUENCIAS', 'SILLAS']].sum().reset_index()
            totales_anuales = totales_anuales.rename(columns={'FRECUENCIAS': 'TOTAL_FRECUENCIAS', 'SILLAS': 'TOTAL_SILLAS'})

            # Unir los totales anuales con el dataframe original
            totales_cerrado = totales_cerrado.merge(totales_anuales, on='FECHA')

            # Transformar en str
            totales_cerrado['FECHA'] = totales_cerrado['FECHA'].astype(str)

            # Calcular la participación porcentual por país por año
            totales_cerrado['PARTICIPACION_FRECUENCIAS'] = totales_cerrado['FRECUENCIAS'] / totales_cerrado['TOTAL_FRECUENCIAS'] * 100
            totales_cerrado['PARTICIPACION_SILLAS'] = totales_cerrado['SILLAS'] / totales_cerrado['TOTAL_SILLAS'] * 100

            # Elegir columnas de interés
            totales_cerrado = totales_cerrado[['FECHA', 'PAIS_ARRIVAL', 'FRECUENCIAS', 'SILLAS', 'PARTICIPACION_FRECUENCIAS', 'PARTICIPACION_SILLAS']]

            #######################################
            # PASO 3: PERIODO CORRIDO (2023 - 2024)
            #######################################

            # Obtener columnas de fecha
            df_top_otros['MES'] = df_top_otros['TIME_SERIES'].dt.month

            # Obtener nombre de mes
            df_top_otros['MES_NAME'] = df_top_otros['TIME_SERIES'].dt.month_name(locale='es_ES.UTF-8')

            # Filtrar años de interés
            df_distribucion_corrido = df_top_otros[df_top_otros['FECHA'].isin([2023, 2024])].copy()

            # Obtener mes máximo disponible en 2024
            mes_maximo = df_distribucion_corrido[df_distribucion_corrido['FECHA']==2024]['MES'].max()

            # Obtener nombre del mes máximo
            mes_maximo_nombre = df_distribucion_corrido[df_distribucion_corrido['MES'] == mes_maximo]['MES_NAME'].unique()[0]

            # Filtrar los meses
            df_distribucion_corrido = df_distribucion_corrido[df_distribucion_corrido['MES'] <= mes_maximo]

            # Crear columna de fecha corrida
            df_distribucion_corrido['FECHA_CORRIDA'] = 'Enero - ' + mes_maximo_nombre + ' ' + df_distribucion_corrido['FECHA'].astype(str)

            # Crear agrupación de años cerrados
            totales_corrido = df_distribucion_corrido.groupby(['FECHA_CORRIDA', 'PAIS_ARRIVAL'])[['FRECUENCIAS', 'SILLAS']].sum().reset_index()

            # Calcular el total de frecuencias y sillas por año corrido
            totales_anuales = totales_corrido.groupby('FECHA_CORRIDA')[['FRECUENCIAS', 'SILLAS']].sum().reset_index()
            totales_anuales = totales_anuales.rename(columns={'FRECUENCIAS': 'TOTAL_FRECUENCIAS', 'SILLAS': 'TOTAL_SILLAS'})

            # Unir los totales anuales con el dataframe original
            totales_corrido = totales_corrido.merge(totales_anuales, on='FECHA_CORRIDA')

            # Calcular la participación porcentual por país por año corrido
            totales_corrido['PARTICIPACION_FRECUENCIAS'] = totales_corrido['FRECUENCIAS'] / totales_corrido['TOTAL_FRECUENCIAS'] * 100
            totales_corrido['PARTICIPACION_SILLAS'] = totales_corrido['SILLAS'] / totales_corrido['TOTAL_SILLAS'] * 100

            # Elegir columnas de interés
            totales_corrido = totales_corrido[['FECHA_CORRIDA', 'PAIS_ARRIVAL', 'FRECUENCIAS', 'SILLAS', 'PARTICIPACION_FRECUENCIAS', 'PARTICIPACION_SILLAS']]

            # Ordenar 
            totales_cerrado = totales_cerrado.sort_values(by=['FECHA', 'PAIS_ARRIVAL'])
            totales_corrido = totales_corrido.sort_values(by=['FECHA_CORRIDA', 'PAIS_ARRIVAL'])

            # Cambiar tipo de columna de fecha a str
            totales_cerrado['FECHA'] = totales_cerrado['FECHA'].astype(str)

            # Cambiar tipo de columna de fecha a str
            totales_corrido['FECHA_CORRIDA'] = totales_corrido['FECHA_CORRIDA'].astype(str)

            # Cambiar nombres de columnas
            totales_cerrado = totales_cerrado.rename(columns = {'FECHA': 'Año', 'PAIS_ARRIVAL': 'País Destino', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas', 'PARTICIPACION_FRECUENCIAS' : 'Participación Frecuencias (%)', 'PARTICIPACION_SILLAS' : 'Participación Sillas (%)'})
            totales_corrido = totales_corrido.rename(columns = {'FECHA_CORRIDA': 'Periodo', 'PAIS_ARRIVAL': 'País Destino', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas', 'PARTICIPACION_FRECUENCIAS' : 'Participación Frecuencias (%)', 'PARTICIPACION_SILLAS' : 'Participación Sillas (%)'})
            
            # Agregar dfs al resultado
            resultados_procesados['conectividad_mundo_destino_cerrado'] = totales_cerrado
            resultados_procesados['conectividad_mundo_destino_corrido'] = totales_corrido
        else:
            resultados_procesados['conectividad_mundo_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['conectividad_mundo_destino_cerrado'] = pd.DataFrame()
            resultados_procesados['conectividad_mundo_destino_corrido'] = pd.DataFrame()

        # Conectividad del país hacia Colombia
        df_conectividad_colombia = dataframes.get('conectividad_hacia_colombia', pd.DataFrame())
        if not df_conectividad_colombia.empty:
            # Serie de tiempo de conectividad hacia Colombia
            df_conectividad_colombia_serie_tiempo = pd.DataFrame(
                df_conectividad_colombia.groupby('YEAR')[['FRECUENCIAS', 'SILLAS']].sum()
            ).reset_index()

            # Cambiar nombres de columnas
            df_conectividad_colombia_serie_tiempo = df_conectividad_colombia_serie_tiempo.rename(columns={'YEAR': 'Año', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas'})

            resultados_procesados['conectividad_colombia_serie_tiempo'] = df_conectividad_colombia_serie_tiempo
            
            # Procesar conectividad por municipio
            # Cerrado y acumulado por destino
            df_conectividad_municipio_serie_tiempo_mensual = df_conectividad_colombia[['YEAR', 'MUNICIPIO_DANE', 'TIME_SERIES', 'FRECUENCIAS','SILLAS']]            

            ###############
            # PASO 1: TOP 5
            ###############

            # Convertir TIME_SERIES a datetime para facilitar el manejo temporal
            df_conectividad_municipio_serie_tiempo_mensual['TIME_SERIES'] = pd.to_datetime(df_conectividad_municipio_serie_tiempo_mensual['TIME_SERIES'], format='%Y-%m')

            # Obtener el último año disponible
            ultimo_anio = df_conectividad_municipio_serie_tiempo_mensual['TIME_SERIES'].dt.year.max()

            # Filtrar datos solo del último año
            df_cerrado = df_conectividad_municipio_serie_tiempo_mensual[df_conectividad_municipio_serie_tiempo_mensual['TIME_SERIES'].dt.year == ultimo_anio]

            # Sumar FRECUENCIAS por municipio para el último año
            frecuencias_por_municipio = df_cerrado.groupby('MUNICIPIO_DANE')['FRECUENCIAS'].sum().reset_index()

            # Seleccionar el top 5 de municipios con mayor FRECUENCIAS
            top_5_municipios = frecuencias_por_municipio.nlargest(10, 'FRECUENCIAS')['MUNICIPIO_DANE']

            # Obtener el número de municipios
            num_municipios = len(frecuencias_por_municipio['MUNICIPIO_DANE'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_municipios <= 10:
                # Si hay 5 municipios o menos, no se agrupan bajo "Otros"
                df_top_otros = df_conectividad_municipio_serie_tiempo_mensual.copy()
            else:
                # Si hay más de 5 municipios, agrupar los demás como "Otros"
                df_top_otros = df_conectividad_municipio_serie_tiempo_mensual.copy()
                df_top_otros['MUNICIPIO_DANE'] = df_top_otros['MUNICIPIO_DANE'].apply(
                    lambda pais: pais if pais in top_5_municipios.values else 'Otros'
                )

                # Agrupar por mes (TIME_SERIES) y municipio, sumando FRECUENCIAS y SILLAS
                df_top_otros = df_top_otros.groupby(['TIME_SERIES', 'MUNICIPIO_DANE'], as_index=False).agg({
                    'FRECUENCIAS': 'sum',
                    'SILLAS': 'sum'
                })

            #######################################
            # PASO 2: PERIODO CERRADO (2022 - 2023)
            #######################################

            # Obtener columnas de fecha
            df_top_otros['FECHA'] = df_top_otros['TIME_SERIES'].dt.year

            # Filtrar años de interés
            df_distribucion_cerrado = df_top_otros[df_top_otros['FECHA'].isin([2022, 2023])].copy()

            # Crear agrupación de años cerrados
            totales_cerrado = df_distribucion_cerrado.groupby(['FECHA', 'MUNICIPIO_DANE'])[['FRECUENCIAS', 'SILLAS']].sum().reset_index()

            # Calcular el total de frecuencias y sillas por año
            totales_anuales = totales_cerrado.groupby('FECHA')[['FRECUENCIAS', 'SILLAS']].sum().reset_index()
            totales_anuales = totales_anuales.rename(columns={'FRECUENCIAS': 'TOTAL_FRECUENCIAS', 'SILLAS': 'TOTAL_SILLAS'})

            # Unir los totales anuales con el dataframe original
            totales_cerrado = totales_cerrado.merge(totales_anuales, on='FECHA')

            # Calcular la participación porcentual por municipio por año
            totales_cerrado['PARTICIPACION_FRECUENCIAS'] = totales_cerrado['FRECUENCIAS'] / totales_cerrado['TOTAL_FRECUENCIAS'] * 100
            totales_cerrado['PARTICIPACION_SILLAS'] = totales_cerrado['SILLAS'] / totales_cerrado['TOTAL_SILLAS'] * 100

            # Unir los totales anuales con el dataframe original
            totales_cerrado = totales_cerrado.merge(totales_anuales, on='FECHA')

            # Transformar en str
            totales_cerrado['FECHA'] = totales_cerrado['FECHA'].astype(str)

            # Elegir columnas de interés
            totales_cerrado = totales_cerrado[['FECHA', 'MUNICIPIO_DANE', 'FRECUENCIAS', 'SILLAS', 'PARTICIPACION_FRECUENCIAS', 'PARTICIPACION_SILLAS']]

            # Cambiar tipo de columna de fecha a str
            totales_cerrado['FECHA'] = totales_cerrado['FECHA'].astype(str)

            #######################################
            # PASO 3: PERIODO CORRIDO (2023 - 2024)
            #######################################

            # Obtener columnas de fecha
            df_top_otros['MES'] = df_top_otros['TIME_SERIES'].dt.month

            # Obtener nombre de mes
            df_top_otros['MES_NAME'] = df_top_otros['TIME_SERIES'].dt.month_name(locale='es_ES.UTF-8')

            # Filtrar años de interés
            df_distribucion_corrido = df_top_otros[df_top_otros['FECHA'].isin([2023, 2024])].copy()

            # Obtener mes máximo disponible en 2024
            mes_maximo = df_distribucion_corrido[df_distribucion_corrido['FECHA']==2024]['MES'].max()

            # Obtener nombre del mes máximo
            mes_maximo_nombre = df_distribucion_corrido[df_distribucion_corrido['MES'] == mes_maximo]['MES_NAME'].unique()[0]

            # Filtrar los meses
            df_distribucion_corrido = df_distribucion_corrido[df_distribucion_corrido['MES'] <= mes_maximo]

            # Crear columna de fecha corrida
            df_distribucion_corrido['FECHA_CORRIDA'] = 'Enero - ' + mes_maximo_nombre + ' ' + df_distribucion_corrido['FECHA'].astype(str)

            # Crear agrupación de años cerrados
            totales_corrido = df_distribucion_corrido.groupby(['FECHA_CORRIDA', 'MUNICIPIO_DANE'])[['FRECUENCIAS', 'SILLAS']].sum().reset_index()

            # Calcular el total de frecuencias y sillas por año corrido
            totales_anuales = totales_corrido.groupby('FECHA_CORRIDA')[['FRECUENCIAS', 'SILLAS']].sum().reset_index()
            totales_anuales = totales_anuales.rename(columns={'FRECUENCIAS': 'TOTAL_FRECUENCIAS', 'SILLAS': 'TOTAL_SILLAS'})

            # Unir los totales anuales con el dataframe original
            totales_corrido = totales_corrido.merge(totales_anuales, on='FECHA_CORRIDA')

            # Calcular la participación porcentual por municipio por año corrido
            totales_corrido['PARTICIPACION_FRECUENCIAS'] = totales_corrido['FRECUENCIAS'] / totales_corrido['TOTAL_FRECUENCIAS'] * 100
            totales_corrido['PARTICIPACION_SILLAS'] = totales_corrido['SILLAS'] / totales_corrido['TOTAL_SILLAS'] * 100

            # Elegir columnas de interés
            totales_corrido = totales_corrido[['FECHA_CORRIDA', 'MUNICIPIO_DANE', 'FRECUENCIAS', 'SILLAS', 'PARTICIPACION_FRECUENCIAS', 'PARTICIPACION_SILLAS']]

            # Ordenar 
            totales_cerrado = totales_cerrado.sort_values(by=['FECHA', 'MUNICIPIO_DANE'])
            totales_corrido = totales_corrido.sort_values(by=['FECHA_CORRIDA', 'MUNICIPIO_DANE'])

            # Cambiar tipo de columna de fecha a str
            totales_cerrado['FECHA'] = totales_cerrado['FECHA'].astype(str)

            # Cambiar tipo de columna de fecha a str
            totales_corrido['FECHA_CORRIDA'] = totales_corrido['FECHA_CORRIDA'].astype(str)

            # Cambiar nombres de columnas
            totales_cerrado = totales_cerrado.rename(columns = {'FECHA': 'Año', 'MUNICIPIO_DANE': 'Municipio Destino', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas', 'PARTICIPACION_FRECUENCIAS' : 'Participación Frecuencias (%)', 'PARTICIPACION_SILLAS' : 'Participación Sillas (%)'})
            totales_corrido = totales_corrido.rename(columns = {'FECHA_CORRIDA': 'Periodo', 'MUNICIPIO_DANE': 'Municipio Destino', 'FRECUENCIAS' : 'Frecuencias', 'SILLAS' : 'Sillas', 'PARTICIPACION_FRECUENCIAS' : 'Participación Frecuencias (%)', 'PARTICIPACION_SILLAS' : 'Participación Sillas (%)'})

            # Agregar dfs al resultado
            resultados_procesados['conectividad_colombia_municipio_cerrado'] = totales_cerrado
            resultados_procesados['conectividad_colombia_municipio_corrido'] = totales_corrido
        else:
            resultados_procesados['conectividad_colombia_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['conectividad_colombia_municipio'] = pd.DataFrame()
            resultados_procesados['conectividad_colombia_municipio_cerrado'] = pd.DataFrame()
            resultados_procesados['conectividad_colombia_municipio_corrido'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

def datos_oag(pais_seleccionado, sesion_activa):
    """
    Obtiene y procesa datos relacionados con OAG para un país seleccionado.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - sesion_activa: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario donde las claves son los nombres descriptivos de los DataFrames procesados y los valores son los DataFrames resultantes.

    Manejo de errores:
    - Si ocurre un error en cualquiera de las funciones llamadas, se registra y se retorna un diccionario vacío.
    """
    try:
        # Obtener datos sin procesar
        print(f"Obteniendo datos de OAG para el país: {pais_seleccionado}...")
        datos_sin_procesar = obtener_datos_oag(pais_seleccionado, sesion_activa)

        # Validar que se hayan obtenido datos
        if not datos_sin_procesar:
            print(f"No se obtuvieron datos de OAG para el país: {pais_seleccionado}.")
            return {}

        # Procesar los datos
        print(f"Procesando datos de OAG para el país: {pais_seleccionado}...")
        datos_procesados = procesar_datos_oag(datos_sin_procesar)

        # Validar que el procesamiento haya sido exitoso
        if not datos_procesados:
            print(f"El procesamiento de datos falló para el país: {pais_seleccionado}.")
            return {}

        print(f"Datos procesados exitosamente para el país: {pais_seleccionado}.")
        return datos_procesados

    except Exception as e:
        # Manejo de errores durante la ejecución
        print(f"Error al obtener y procesar datos de OAG para el país: {pais_seleccionado}. Detalles: {str(e)}")
        return {}


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
                DATE_TRUNC('MONTH', TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD')) AS FECHA_USABLE,
                LOS_AT_DESTINATION_NIGHTS,
                CLASE_CABINA,
                PERFIL_PASAJERO,
                RESERVAS
            FROM REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_RESERVAS_PAISES
            WHERE PAIS_DEPARTURE = '{pais_seleccionado}'
            ORDER BY TO_DATE(FLIGHT_LEG_ARRIVAL_DATE, 'YYYY-MM-DD') ASC
            ;
        """,
        "busquedas_aereas": f"""
            SELECT 
                PAIS_DEPARTURE,
                PAIS_ARRIVAL,
                SEARCH_DATE,
                DATE_TRUNC('MONTH', TO_DATE(SEARCH_DATE, 'YYYY-MM-DD')) AS FECHA_USABLE,
                BUSQUEDAS
            FROM REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_BUSQUEDAS_PAISES
            WHERE TO_DATE(SEARCH_DATE, 'YYYY-MM-DD') BETWEEN DATEADD(MONTH, -14, CURRENT_DATE()) AND CURRENT_DATE()
            AND PAIS_DEPARTURE = '{pais_seleccionado}'
            ORDER BY TO_DATE(SEARCH_DATE, 'YYYY-MM-DD') ASC;
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

            # Crear una fecha con formato date
            df_reservas["FECHA_USABLE"] = pd.to_datetime(df_reservas["FECHA_USABLE"])

            # Cambiar nombre de la columna
            df_reservas = df_reservas.rename(columns={'FECHA_USABLE' : 'MES_ANIO'})

            # Agrupar por PAIS_ARRIVAL + columnas de orden y despliegue, y sumar RESERVAS
            df_agrupado = (
                df_reservas
                .groupby(["PAIS_ARRIVAL", "MES_ANIO"], as_index=False)
                ["RESERVAS"]
                .sum()
            )

            # Cambiar nombre
            df_agrupado = df_agrupado.rename(columns={'MES_ANIO' : 'FLIGHT_LEG_ARRIVAL_MONTH_YEAR'})

            # Filtrar reservas hacia Colombia
            df_reservas_serie_tiempo_colombia = df_agrupado[
                df_agrupado['PAIS_ARRIVAL'] == 'Colombia'
            ]

            # Filtrar reservas para los países diferentes a Colombia
            df_reservas_serie_tiempo = df_agrupado[
                df_agrupado['PAIS_ARRIVAL'] != 'Colombia'
            ]

            # Cambiar nombres de columnas
            df_reservas_serie_tiempo = df_reservas_serie_tiempo.rename(columns = {'PAIS_ARRIVAL': 'País', 'FLIGHT_LEG_ARRIVAL_MONTH_YEAR' : 'Fecha', 'RESERVAS': 'Reservas'})
            df_reservas_serie_tiempo_colombia = df_reservas_serie_tiempo_colombia.rename(columns = {'PAIS_ARRIVAL': 'País', 'FLIGHT_LEG_ARRIVAL_MONTH_YEAR' : 'Fecha', 'RESERVAS': 'Reservas'})

            resultados_procesados['reservas_serie_tiempo'] = df_reservas_serie_tiempo
            resultados_procesados['reservas_serie_tiempo_colombia'] = df_reservas_serie_tiempo_colombia
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['reservas_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['reservas_serie_tiempo_colombia'] = pd.DataFrame()

        # Procesar búsquedas aéreas
        df_busquedas = dataframes.get('busquedas_aereas', pd.DataFrame())
        if not df_busquedas.empty:

            # Crear una fecha con formato date
            df_busquedas["FECHA_USABLE"] = pd.to_datetime(df_busquedas["FECHA_USABLE"])

            # Cambiar nombre de la columna
            df_busquedas = df_busquedas.rename(columns={'FECHA_USABLE' : 'MES_ANIO'})
            
            # Agrupar por PAIS_ARRIVAL + columnas de orden y despliegue, y sumar busquedas
            df_agrupado = (
                df_busquedas
                .groupby(["PAIS_ARRIVAL", "MES_ANIO"], as_index=False)
                ["BUSQUEDAS"]
                .sum()
            )

            # Cambiar nombre
            df_agrupado = df_agrupado.rename(columns={'MES_ANIO' : 'SEARCH_DATE_MONTH_YEAR'})

           # Filtrar búsquedas hacia Colombia
            df_busquedas_serie_tiempo_colombia = df_agrupado[
                df_agrupado['PAIS_ARRIVAL'] == 'Colombia'
            ]

            # Filtrar búsquedas para los países diferentes a Colombia
            df_busquedas_serie_tiempo = df_agrupado[
                df_agrupado['PAIS_ARRIVAL'] != 'Colombia'
            ]

            # Cambiar nombres de columnas
            df_busquedas_serie_tiempo = df_busquedas_serie_tiempo.rename(columns = {'PAIS_ARRIVAL': 'País', 'SEARCH_DATE_MONTH_YEAR' : 'Fecha', 'BUSQUEDAS': 'Búsquedas'})
            df_busquedas_serie_tiempo_colombia = df_busquedas_serie_tiempo_colombia.rename(columns = {'PAIS_ARRIVAL': 'País', 'SEARCH_DATE_MONTH_YEAR' : 'Fecha', 'BUSQUEDAS': 'Búsquedas'})

            resultados_procesados['busquedas_serie_tiempo'] = df_busquedas_serie_tiempo
            resultados_procesados['busquedas_serie_tiempo_colombia'] = df_busquedas_serie_tiempo_colombia
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['busquedas_serie_tiempo'] = pd.DataFrame()
            resultados_procesados['busquedas_serie_tiempo_colombia'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

def datos_forward_keys(pais_seleccionado, sesion_activa):
    """
    Obtiene y procesa datos relacionados con Forward Keys para un país seleccionado.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - sesion_activa: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    try:
        # Obtener datos de Forward Keys
        print(f"Obteniendo datos de Forward Keys para el país: {pais_seleccionado}...")
        datos_obtenidos = obtener_datos_forward_keys(pais_seleccionado, sesion_activa)

        # Verificar si se obtuvieron datos
        if not datos_obtenidos:
            print(f"No se encontraron datos para el país: {pais_seleccionado}.")
            return {}

        # Procesar los datos obtenidos
        print(f"Procesando datos de Forward Keys para el país: {pais_seleccionado}...")
        datos_procesados = procesar_datos_forward_keys(datos_obtenidos)

        # Verificar si el procesamiento fue exitoso
        if not datos_procesados:
            print(f"No se pudieron procesar los datos para el país: {pais_seleccionado}.")
            return {}

        print(f"Datos de Forward Keys procesados correctamente para el país: {pais_seleccionado}.")
        return datos_procesados

    except Exception as e:
        # Manejo de errores
        print(f"Error al obtener o procesar datos de Forward Keys para el país {pais_seleccionado}: {str(e)}")
        return {}


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
                TURISTAS AS VIAJEROS,
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
            df_gasto_promedio = pd.DataFrame(df_gasto.groupby(['YEAR'])[['FACTURACION_USD', 'VIAJEROS', 'TRANSACCIONES']].sum()).reset_index()
            df_gasto_promedio['GASTO_PROMEDIO_TARJETA'] = df_gasto_promedio['FACTURACION_USD'] / df_gasto_promedio['VIAJEROS']
            df_gasto_promedio['GASTO_PROMEDIO_TRANSACCION'] = df_gasto_promedio['FACTURACION_USD'] / df_gasto_promedio['TRANSACCIONES']
            # Cambiar nombres de columnas
            df_gasto_promedio = df_gasto_promedio.rename(columns = {'YEAR' : 'Año', 'FACTURACION_USD' : 'Facturación (USD)', 'VIAJEROS' : 'Viajeros', 'TRANSACCIONES' : 'Transacciones', 'GASTO_PROMEDIO_TARJETA' : 'Gasto promedio tarjeta (USD)', 'GASTO_PROMEDIO_TRANSACCION' : 'Gasto promedio transacción (USD)'})
            resultados_procesados['gasto_promedio'] = df_gasto_promedio

            # Gasto por categoría
            df_gasto_categoria = pd.DataFrame(df_gasto.groupby(['YEAR', 'CLASIFICACION_CATEGORIA_FORMATADA'])[['FACTURACION_USD']].sum()).reset_index()
            df_gasto_categoria['TOTAL_ANUAL'] = df_gasto_categoria.groupby('YEAR')['FACTURACION_USD'].transform('sum')
            df_gasto_categoria['PARTICIPACION'] = (df_gasto_categoria['FACTURACION_USD'] / df_gasto_categoria['TOTAL_ANUAL']) * 100
            df_gasto_categoria = df_gasto_categoria.sort_values(by=['YEAR', 'CLASIFICACION_CATEGORIA_FORMATADA'])
            # Cambiar nombres de columnas
            df_gasto_categoria = df_gasto_categoria.rename(columns = {'YEAR' : 'Año', 'CLASIFICACION_CATEGORIA_FORMATADA' : 'Clasificación', 'FACTURACION_USD' : 'Facturación (USD)', 'TOTAL_ANUAL' : 'Total Anual (USD)', 'PARTICIPACION' : 'Participación (%)'})
            resultados_procesados['gasto_categoria'] = df_gasto_categoria

            # Gasto por producto
            
            #########
            # Directo
            #########

            # Obtener insumo para gastos directos
            df_categoria_insumo = df_gasto[df_gasto['CLASIFICACION_CATEGORIA_FORMATADA']=='Directo']
            df_gasto_producto = pd.DataFrame(df_categoria_insumo.groupby(['YEAR', 'CATEGORIA'])[['FACTURACION_USD']].sum()).reset_index()

            # Distribución agregada de productos
            df_gasto_total = pd.DataFrame(df_gasto_producto.groupby('CATEGORIA')[['FACTURACION_USD']].sum()).reset_index()

            # Seleccionar el top 5 de productos con mayor valor USD
            top_5_productos = df_gasto_total.nlargest(5, 'FACTURACION_USD')['CATEGORIA']

            # Obtener el número de productos
            num_productos = len(df_gasto_total['CATEGORIA'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_productos <= 5:
                # Si hay 5 productos o menos, no se agrupan bajo "Otros"
                df_top_otros = df_gasto_producto.copy()
            else:
                # Si hay más de 5 productos, agrupar los demás como "Otros"
                df_top_otros = df_gasto_producto.copy()
                df_top_otros['CATEGORIA'] = df_top_otros['CATEGORIA'].apply(
                    lambda producto: producto if producto in top_5_productos.values else 'Otros'
                )

                # Agrupar por año y productos, sumando USD
                df_top_otros = df_top_otros.groupby(['YEAR', 'CATEGORIA'], as_index=False).agg({
                    'FACTURACION_USD': 'sum'
                })

            # Participación
            df_top_otros['TOTAL_ANUAL'] = df_top_otros.groupby('YEAR')['FACTURACION_USD'].transform('sum')
            df_top_otros['PARTICIPACION'] = (df_top_otros['FACTURACION_USD'] / df_top_otros['TOTAL_ANUAL']) * 100
            
            # Cambiar nombres de columnas
            df_top_otros = df_top_otros.rename(columns={'YEAR' : 'Año', 'CATEGORIA' : 'Categoria', 'FACTURACION_USD' : 'Facturación (USD)', 'TOTAL_ANUAL' : 'Total Anual (USD)', 'PARTICIPACION' : 'Participación (%)'})
            
            # Agregar el df        
            resultados_procesados['gasto_producto_directo'] = df_top_otros

            ###########
            # Indirecto
            ###########

            # Obtener insumo para gastos indirectos
            df_categoria_insumo = df_gasto[df_gasto['CLASIFICACION_CATEGORIA_FORMATADA']=='Indirecto']
            df_gasto_producto = pd.DataFrame(df_categoria_insumo.groupby(['YEAR', 'CATEGORIA'])[['FACTURACION_USD']].sum()).reset_index()

            # Distribución agregada de productos
            df_gasto_total = pd.DataFrame(df_gasto_producto.groupby('CATEGORIA')[['FACTURACION_USD']].sum()).reset_index()

            # Seleccionar el top 5 de productos con mayor valor USD
            top_5_productos = df_gasto_total.nlargest(5, 'FACTURACION_USD')['CATEGORIA']

            # Obtener el número de productos
            num_productos = len(df_gasto_total['CATEGORIA'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_productos <= 5:
                # Si hay 5 productos o menos, no se agrupan bajo "Otros"
                df_top_otros = df_gasto_producto.copy()
            else:
                # Si hay más de 5 productos, agrupar los demás como "Otros"
                df_top_otros = df_gasto_producto.copy()
                df_top_otros['CATEGORIA'] = df_top_otros['CATEGORIA'].apply(
                    lambda producto: producto if producto in top_5_productos.values else 'Otros'
                )

                # Agrupar por año y productos, sumando USD
                df_top_otros = df_top_otros.groupby(['YEAR', 'CATEGORIA'], as_index=False).agg({
                    'FACTURACION_USD': 'sum'
                })

            # Participación
            df_top_otros['TOTAL_ANUAL'] = df_top_otros.groupby('YEAR')['FACTURACION_USD'].transform('sum')
            df_top_otros['PARTICIPACION'] = (df_top_otros['FACTURACION_USD'] / df_top_otros['TOTAL_ANUAL']) * 100

            # Cambiar nombres de columnas
            df_top_otros = df_top_otros.rename(columns={'YEAR' : 'Año', 'CATEGORIA' : 'Categoria', 'FACTURACION_USD' : 'Facturación (USD)', 'TOTAL_ANUAL' : 'Total Anual (USD)', 'PARTICIPACION' : 'Participación (%)'})
            
            # Agregar el df        
            resultados_procesados['gasto_producto_indirecto'] = df_top_otros

        else:
            # Devolver DataFrames vacíos si no hay datos
            resultados_procesados['gasto_promedio'] = pd.DataFrame()
            resultados_procesados['gasto_categoria'] = pd.DataFrame()
            resultados_procesados['gasto_producto_directo'] = pd.DataFrame()
            resultados_procesados['gasto_producto_indirecto'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

def datos_credibanco(pais_seleccionado, sesion_activa):
    """
    Obtiene y procesa datos relacionados con Credibanco para un país seleccionado.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - sesion_activa: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    try:
        # Obtener los datos
        print(f"Obteniendo datos de Credibanco para el país: {pais_seleccionado}...")
        datos_obtenidos = obtener_datos_credibanco(pais_seleccionado, sesion_activa)

        # Verificar si se obtuvieron datos
        if not datos_obtenidos:
            print(f"No se encontraron datos en Credibanco para el país: {pais_seleccionado}.")
            return {}

        # Procesar los datos
        print(f"Procesando datos de Credibanco para el país: {pais_seleccionado}...")
        datos_procesados = procesar_datos_credibanco(datos_obtenidos)

        # Verificar si el procesamiento fue exitoso
        if not datos_procesados:
            print(f"No se pudieron procesar los datos de Credibanco para el país: {pais_seleccionado}.")
            return {}

        print(f"Datos de Credibanco procesados correctamente para el país: {pais_seleccionado}.")
        return datos_procesados

    except Exception as e:
        # Manejo de errores
        print(f"Error al obtener o procesar datos de Credibanco para el país {pais_seleccionado}: {str(e)}")
        return {}


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
                YY AS YEAR,
                COUNT(DISTINCT AGENCIAS) AS AGENCIAS 
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}'
            GROUP BY PAIS_AGENCIA, YY;
        """,
        "ciudades_agencias": f"""
            SELECT INITCAP(TRAVEL_AGENCY_CITY) AS TRAVEL_AGENCY_CITY,
                YY AS YEAR,
                COUNT(DISTINCT AGENCIAS) AS AGENCIAS
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}'
            GROUP BY TRAVEL_AGENCY_CITY, YY;
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


def procesar_datos_iata_gap(dataframes):
    """
    Procesa los datos obtenidos de IATA GAP.

    Parámetros:
    - dataframes (dict): Diccionario de DataFrames obtenidos de consultas a IATA GAP.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    resultados_procesados = {}

    try:
        # Procesar indicadores de agencia
        df_agencias = dataframes.get('indicadores_agencias', pd.DataFrame())
        if not df_agencias.empty:
            # Serie de tiempo de agencias por año
            df_agencias_serie_tiempo = pd.DataFrame(df_agencias[['YEAR', 'AGENCIAS']])
            df_agencias_serie_tiempo = df_agencias_serie_tiempo.sort_values(by=['YEAR'])
            # Cambiar nombres de columnas
            df_agencias_serie_tiempo = df_agencias_serie_tiempo.rename(columns = {'YEAR' : 'Año', 'AGENCIAS' : 'Número de Agencias'})
            resultados_procesados['agencias_serie_tiempo'] = df_agencias_serie_tiempo
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['agencias_serie_tiempo'] = pd.DataFrame()
    

        # Procesar ciudades
        df_ciudades = dataframes.get('ciudades_agencias', pd.DataFrame())
        if not df_ciudades.empty:

            # Obtener el top 10 de ciudades con más agencias
            
            # Distribución agregada de agencias por ciudad
            df_agencias_total = pd.DataFrame(df_ciudades.groupby('TRAVEL_AGENCY_CITY')[['AGENCIAS']].sum()).reset_index()

            # Seleccionar el top 5 de agencias por ciudad
            top_5_agencias = df_agencias_total.nlargest(15, 'AGENCIAS')['TRAVEL_AGENCY_CITY']

            # Obtener el número de ciudades
            num_ciudades = len(df_agencias_total['TRAVEL_AGENCY_CITY'].unique())

            # Crear el nuevo DataFrame con top 10 y agrupar los demás bajo "Otros"
            if num_ciudades <= 10:
                # Si hay 10 ciudades o menos, no se agrupan bajo "Otros"
                df_top_otros = df_ciudades.copy()
            else:
                # Si hay más de 10 ciudades, agrupar los demás como "Otros"
                df_top_otros = df_ciudades.copy()
                df_top_otros['TRAVEL_AGENCY_CITY'] = df_top_otros['TRAVEL_AGENCY_CITY'].apply(
                    lambda ciudad: ciudad if ciudad in top_5_agencias.values else 'Otros'
                )

                # Agrupar por año y ciudad, sumando USD
                df_top_otros = df_top_otros.groupby(['YEAR', 'TRAVEL_AGENCY_CITY'], as_index=False).agg({
                    'AGENCIAS': 'sum'
                })
            
            # Participación
            df_top_otros['TOTAL_ANUAL'] = df_top_otros.groupby('YEAR')['AGENCIAS'].transform('sum')
            df_top_otros['PARTICIPACION'] = (df_top_otros['AGENCIAS'] / df_top_otros['TOTAL_ANUAL']) * 100
            df_top_otros = df_top_otros.sort_values(by=['YEAR'])

            # Cambiar nombres de columnas
            df_top_otros = df_top_otros.rename(columns = {'YEAR' : 'Año', 'TRAVEL_AGENCY_CITY' : 'Ciudad de la Agencia', 'AGENCIAS' : 'Número de Agencias', 'TOTAL_ANUAL' : 'Total Anual', 'PARTICIPACION' : 'Participación (%)'})

            resultados_procesados['agencias_ciudades'] = df_top_otros
        else:
            # Devolver DataFrame vacío para las claves si no hay datos
            resultados_procesados['agencias_ciudades'] = pd.DataFrame()

    except Exception as e:
        # Manejo de errores durante el procesamiento
        print(f"Error durante el procesamiento de los datos de IATA GAP: {str(e)}")

    # Devolver los resultados procesados
    return resultados_procesados

def datos_iata_gap(pais_seleccionado, sesion_activa):
    """
    Obtiene y procesa datos relacionados con IATA GAP para un país seleccionado.

    Parámetros:
    - pais_seleccionado (str): Nombre del país seleccionado.
    - sesion_activa: Objeto de conexión activo a Snowflake.

    Retorna:
    - dict: Diccionario con los DataFrames procesados y transformados.
    """
    try:
        # Paso 1: Obtener los datos
        print(f"Obteniendo datos de IATA GAP para el país: {pais_seleccionado}...")
        datos_obtenidos = obtener_datos_iata_gap(pais_seleccionado, sesion_activa)

        # Verificar si se obtuvieron datos
        if not datos_obtenidos:
            print(f"No se encontraron datos en IATA GAP para el país: {pais_seleccionado}.")
            return {}

        # Paso 2: Procesar los datos
        print(f"Procesando datos de IATA GAP para el país: {pais_seleccionado}...")
        datos_procesados = procesar_datos_iata_gap(datos_obtenidos)

        # Verificar si el procesamiento fue exitoso
        if not datos_procesados:
            print(f"No se pudieron procesar los datos de IATA GAP para el país: {pais_seleccionado}.")
            return {}

        print(f"Datos de IATA GAP procesados correctamente para el país: {pais_seleccionado}.")
        return datos_procesados

    except Exception as e:
        # Manejo de errores
        print(f"Error al obtener o procesar datos de IATA GAP para el país {pais_seleccionado}: {str(e)}")
        return {}

def calcular_tasa_variacion(valor_actual, valor_anterior):
    """
    Calcula la tasa de variación entre dos valores y la devuelve con dos decimales en formato numérico con coma decimal.
    
    Parámetros
    ----------
    valor_actual : float or int
        Valor más reciente o actual.
    valor_anterior : float or int
        Valor anterior con el cual se compara.
    
    Retorna
    -------
    str
        Tasa de variación formateada con dos decimales y coma decimal (Ejemplo: "2,23%").
        Si los valores son inválidos o generan una división por cero, devuelve None.
    """
    try:
        # Validar si los valores son nulos o no numéricos
        if valor_actual is None or valor_anterior is None:
            return None

        # Convertir a float en caso de que los valores sean enteros o strings numéricos
        valor_actual = float(valor_actual)
        valor_anterior = float(valor_anterior)

        # Manejar el caso donde valor_anterior es 0 para evitar división por infinito
        if valor_anterior == 0:
            return None

        # Calcular la tasa de variación
        tasa_variacion = ((valor_actual - valor_anterior) / valor_anterior) * 100

        # Retornar el resultado formateado con dos decimales y coma decimal
        return f"{tasa_variacion:,.2f}".replace(".", ",") + " %"
    
    except (ValueError, TypeError):
        return None
    
def filtrar_df_top_n(df: pd.DataFrame, year: int, categoria: str, top_n: int) -> pd.DataFrame:
    """
    Filtra un DataFrame por un año específico, selecciona las filas con mayor participación,
    formatea la columna "Participación (%)".

    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame de entrada con una columna "Año", una columna "Participación (%)"
        y otra que represente la categoría (por ejemplo "Producto", "País", etc.).
    year : int
        Año por el cual se quiere filtrar la información.
    categoria : str
        Nombre de la columna que representa la categoría en el DataFrame.
    top_n : int
        Cantidad máxima de filas a conservar basadas en los valores mayores de la columna
        "Participación (%)".

    Retorna
    -------
    pd.DataFrame
        DataFrame filtrado y transformado, con la columna "Participación (%)" formateada. 
        Devuelve un DataFrame vacío si no hay registros para el año especificado.

    Ejemplo de uso
    --------------
    >>> df_resultado = filtrar_df_top_n(df_original, 2022, "País", 5)
    >>> st.dataframe(df_resultado)
    """
    
    # 1) Crear una copia del DataFrame original para no alterar el entorno
    df_copy = df.copy()

    # 2) Filtrar el DataFrame por el año especificado
    df_copy = df_copy[df_copy["Año"] == year]

    # 3) Verificar si el DataFrame filtrado tiene datos
    if df_copy.empty:
        # Si no hay datos, se retorna un DataFrame vacío
        return df_copy

    # 4) Seleccionar las filas con mayor participación según el top_n
    #    Si top_n es mayor que el número de filas, se devuelven todas las filas
    df_copy = df_copy.nlargest(top_n, "Participación (%)")

    # 5) Formatear la columna "Participación (%)" usando la función formato_miles
    df_copy["Participación (%)"] = df_copy["Participación (%)"].apply(
        lambda x: formato_miles(valor=x, decimales=2)
    )

    # 6) Transformar la columna de categoria
    df_copy[categoria] = df_copy[categoria].str.lower()
        
    # 7) Retornar el DataFrame resultante
    return df_copy

def global_data_bullets_viajeros_mundo(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los flujos de viajeros de un país hacia el mundo,
    comparando dos periodos de tiempo y calculando la variación porcentual.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la serie de tiempo de viajeros.
    year_global_data_t_1 : int
        Año base para la comparación (t-1).
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con el número de viajeros y la variación porcentual entre ambos años.
        Si no hay datos disponibles, retorna None.
    """

    # Flujos de viajeros hacia el mundo
    df_flujos_viajeros_mundo = df_global_data.get('viajeros_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_flujos_viajeros_mundo.empty:

        # Volver diccionario
        dict_flujos_viajeros_mundo = df_flujos_viajeros_mundo.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_flujos_viajeros_mundo.get(year_global_data_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Viajeros', 0) * 1000

        # Agregar formato
        val_flujos_viajeros_mundo_t_1 =  formato_miles(valor=val_t_1, decimales=0)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_flujos_viajeros_mundo.get(year_global_data_t, {})

        # Extraer val t
        val_t = sub_dict.get('Viajeros', 0) * 1000

        # Agregar formato
        val_flujos_viajeros_mundo_t =  formato_miles(valor=val_t, decimales=0)

        # Calcular variación
        val_flujos_viajeros_mundo_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_flujos_viajeros_mundo_variacion:
            bullet_flujos_viajeros_mundo = f"En {year_global_data_t}, el número de viajeros de {pais_elegido} al mundo alcanza los {val_flujos_viajeros_mundo_t}, lo que representa una variación del {val_flujos_viajeros_mundo_variacion} en comparación con los {val_flujos_viajeros_mundo_t_1} de {year_global_data_t_1}."
        else:
            bullet_flujos_viajeros_mundo = f"En {year_global_data_t}, el número de viajeros de {pais_elegido} al mundo alcanza los {val_flujos_viajeros_mundo_t} en comparación con los {val_flujos_viajeros_mundo_t_1} de {year_global_data_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_flujos_viajeros_mundo = None

    # Resultado
    return bullet_flujos_viajeros_mundo

def global_data_bullets_medio_transporte(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los principales medios de transporte utilizados 
    por los viajeros de un país para salir al exterior en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre el medio de transporte 
        utilizado por los viajeros.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con los principales medios de transporte utilizados por los viajeros en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Medio de transporte
    df_medio_transporte = df_global_data.get('viajeros_medio', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_medio_transporte.empty:

        # Obtener el dataframe con el topn categorias
        df_medio_transporte_topn = filtrar_df_top_n(df=df_medio_transporte, year=year_global_data_t, categoria="Medio de transporte", top_n=3)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Medio de transporte']} ({row['Participación (%)']}%)" for _, row in df_medio_transporte_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_gasto_categoria = None
        if len(elementos) == 1:
            bullet_medio_transporte = f"El principal medio de transporte utilizado en {year_global_data_t} para las salidas internacionales de los viajeros de {pais_elegido} es {elementos[0]}."
        elif len(elementos) == 2:
            bullet_medio_transporte = f"El principal medio de transporte utilizado en {year_global_data_t} para las salidas internacionales de los viajeros de {pais_elegido} es {elementos[0]}, seguido por {elementos[1]}."
        else:
            bullet_medio_transporte = f"El principal medio de transporte utilizado en {year_global_data_t} para las salidas internacionales de los viajeros de {pais_elegido} es {elementos[0]}, seguido por {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_medio_transporte = None

    # Resultado
    return bullet_medio_transporte

def global_data_bullets_noches_percnotacion(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre el número promedio de noches de pernoctación 
    de los viajeros de un país en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre las noches de pernoctación.
    year_global_data_t_1 : int
        Año base para la comparación (t-1). No se usa en la función, pero se mantiene por consistencia.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con el promedio de noches de pernoctación en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Noches de percnotacion promedio
    df_noches_percnotacion = df_global_data.get('noches_pernoctacion', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_noches_percnotacion.empty:

        # Volver diccionario
        dict_noches_percnotacion = df_noches_percnotacion.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_noches_percnotacion.get(year_global_data_t, {})

        # Extraer val
        val = sub_dict.get('Noches de percnotación', 0)

        # Agregar formato
        val_noches_percnotacion_t =  formato_miles(valor=val, decimales=0)

        # Crear bullet
        bullet_noches_percnotacion = f"En {year_global_data_t}, los viajeros de {pais_elegido} registran un promedio de {val_noches_percnotacion_t} noches de pernoctación."   

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_noches_percnotacion = None

    # Resultado
    return bullet_noches_percnotacion

def global_data_bullets_rango_edad(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los rangos de edad predominantes de los viajeros 
    internacionales de un país en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre el rango de edad de los viajeros.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con los rangos de edad más representativos de los viajeros en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Rango de edad
    df_rango_edad = df_global_data.get('rango_edad', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_rango_edad.empty:

        # Obtener el dataframe con el topn categorias
        df_rango_edad_topn = filtrar_df_top_n(df=df_rango_edad, year=year_global_data_t, categoria="Rango de Edad", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Rango de Edad']} ({row['Participación (%)']}%)" for _, row in df_rango_edad_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_rango_edad = None
        if len(elementos) == 1:
            bullet_rango_edad = f"Predominantemente, los viajeros internacionales de {pais_elegido} se encuentran en el rango de edad de {elementos[0]} en {year_global_data_t}"
        elif len(elementos) == 2:
            bullet_rango_edad = f"Predominantemente, los viajeros internacionales de {pais_elegido} se encuentran en el rango de edad de {elementos[0]} en {year_global_data_t}, seguido por {elementos[1]}."
        else:
            bullet_rango_edad = f"Predominantemente, los viajeros internacionales de {pais_elegido} se encuentran en el rango de edad de {elementos[0]} en {year_global_data_t}, seguido por {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_rango_edad = None

    # Resultado
    return bullet_rango_edad

def global_data_bullets_motivo_viaje(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los principales motivos de viaje de los 
    viajeros internacionales de un país en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre los motivos de viaje.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con los motivos de viaje más representativos de los viajeros en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Motivo de viaje
    df_motivo_viaje = df_global_data.get('motivo_viaje', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_motivo_viaje.empty:

        # Obtener el dataframe con el topn categorias
        df_motivo_viaje_topn = filtrar_df_top_n(df=df_motivo_viaje, year=year_global_data_t, categoria="Motivo de Viaje", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Motivo de Viaje']} ({row['Participación (%)']}%)" for _, row in df_motivo_viaje_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_motivo_viaje = None
        if len(elementos) == 1:
            bullet_motivo_viaje = f"El principal motivo de viaje en {year_global_data_t} de los viajeros internacionales de {pais_elegido} es {elementos[0]}."
        elif len(elementos) == 2:
            bullet_motivo_viaje = f"El principal motivo de viaje en {year_global_data_t} de los viajeros internacionales de {pais_elegido} es {elementos[0]}, seguido por {elementos[1]}."
        else:
            bullet_motivo_viaje = f"El principal motivo de viaje en {year_global_data_t} de los viajeros internacionales de {pais_elegido} es {elementos[0]}, seguido por {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_motivo_viaje = None

    # Resultado
    return bullet_motivo_viaje

def global_data_bullets_forma_viaje(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las principales formas de viaje utilizadas 
    por los viajeros internacionales de un país en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre la forma de viaje.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con las principales formas de viaje elegidas por los viajeros en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Forma de viaje
    df_forma_viaje = df_global_data.get('forma_viaje', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_forma_viaje.empty:

        # Obtener el dataframe con el topn categorias
        df_forma_viaje_topn = filtrar_df_top_n(df=df_forma_viaje, year=year_global_data_t, categoria="Forma de Viaje", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Forma de Viaje']} ({row['Participación (%)']}%)" for _, row in df_forma_viaje_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_forma_viaje = None
        if len(elementos) == 1:
            bullet_forma_viaje = f"El ranking de las formas de viaje elegidas por los viajeros internacionales de {pais_elegido} en {year_global_data_t} es liderado por {elementos[0]}."
        elif len(elementos) == 2:
            bullet_forma_viaje = f"El ranking de las formas de viaje elegidas por los viajeros internacionales de {pais_elegido} en {year_global_data_t} es liderado por {elementos[0]}, seguida por {elementos[1]}."
        else:
            bullet_forma_viaje = f"El ranking de las formas de viaje elegidas por los viajeros internacionales de {pais_elegido} en {year_global_data_t} es liderado por {elementos[0]}, seguida por {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_forma_viaje = None

    # Resultado
    return bullet_forma_viaje

def global_data_bullets_destinos_internacionales(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido):

    """
    Genera textos en formato bullet con información sobre los principales destinos internacionales 
    visitados por los viajeros de un país en dos años consecutivos, permitiendo comparar tendencias.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre los destinos 
        internacionales visitados por los viajeros.
    year_global_data_t_1 : int
        Año base para la comparación (t-1).
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    tuple (str o None, str o None)
        - Un texto con los principales destinos visitados en el año t.
        - Un texto con los principales destinos visitados en el año t-1.
        Si no hay datos disponibles, retorna None en lugar del texto correspondiente.
    """

    # Destinos internacionales
    df_destinos_internacionales = df_global_data.get('destinos_internacionales_top5', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_destinos_internacionales.empty:

        # Filtras otros para que no salga en los bullets
        df_destinos_internacionales = df_destinos_internacionales[df_destinos_internacionales['País Destino'] != 'Otros']

        # Obtener el dataframe con el topn categorias para el año t
        df_destinos_internacionales_topn_t = filtrar_df_top_n(df=df_destinos_internacionales, year=year_global_data_t, categoria="País Destino", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos_t = [f"{row['País Destino']} ({row['Participación (%)']}%)" for _, row in df_destinos_internacionales_topn_t.iterrows()]

        # Obtener el dataframe con el topn categorias para el año t_1
        df_destinos_internacionales_topn_t_1 = filtrar_df_top_n(df=df_destinos_internacionales, year=year_global_data_t_1, categoria="País Destino", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos_t_1 = [f"{row['País Destino']} ({row['Participación (%)']}%)" for _, row in df_destinos_internacionales_topn_t_1.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos para year_t
        if len(elementos_t) == 0:
            bullet_destinos_internacionales_t = None
        if len(elementos_t) == 1:
            bullet_destinos_internacionales_t = f"En {year_global_data_t}, el principal destino visitado por el viajero de {pais_elegido} es {elementos_t[0].capitalize()}."
        elif len(elementos_t) == 2:
            bullet_destinos_internacionales_t = f"En {year_global_data_t}, los principales destinos visitados por el viajero de {pais_elegido} son {elementos_t[0].capitalize()} y {elementos_t[1].capitalize()}."
        else:
            bullet_destinos_internacionales_t = f"En {year_global_data_t}, los principales destinos visitados por el viajero de {pais_elegido} son {elementos_t[0].capitalize()}, {', '.join([e.capitalize() for e in elementos_t[1:-1]])} y {elementos_t[-1].capitalize()}."

        # Construcción del bullet en función de la cantidad de elementos para year_t_1
        if len(elementos_t_1) == 0:
            bullet_destinos_internacionales_t_1 = None
        if len(elementos_t_1) == 1:
            bullet_destinos_internacionales_t_1 = f"Mientras que {year_global_data_t_1}, el principal destino fue {elementos_t_1[0].capitalize()}."
        elif len(elementos_t_1) == 2:
            bullet_destinos_internacionales_t_1 = f"Mientras que {year_global_data_t_1}, los principales destinos fueron {elementos_t_1[0].capitalize()} y {elementos_t[1].capitalize()}."
        else:
            bullet_destinos_internacionales_t_1 = f"Mientras que {year_global_data_t_1}, los principales destinos fueron {elementos_t_1[0].capitalize()}, {', '.join([e.capitalize() for e in elementos_t[1:-1]])} y {elementos_t[-1].capitalize()}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_destinos_internacionales_t_1 = None
        bullet_destinos_internacionales_t = None

    # Resultado
    return bullet_destinos_internacionales_t_1, bullet_destinos_internacionales_t

def global_data_bullets_gasto_promedio(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre el gasto promedio en dólares (USD) de los viajeros 
    de un país en dos años consecutivos, permitiendo comparar su evolución.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre el gasto promedio.
    year_global_data_t_1 : int
        Año base para la comparación (t-1).
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con el gasto promedio por viajero en el año analizado, junto con la variación porcentual respecto 
        al año anterior. Si no hay datos disponibles, retorna None.
    """

    # Gasto promedio
    df_gasto_promedio = df_global_data.get('gasto_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_promedio.empty:

        # Volver diccionario
        dict_gasto_promedio = df_gasto_promedio.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_gasto_promedio.get(year_global_data_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Gasto (USD)', 0)

        # Agregar formato
        val_gasto_promedio_t_1 =  formato_miles(valor=val_t_1, decimales=0)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_gasto_promedio.get(year_global_data_t, {})

        # Extraer val t
        val_t = sub_dict.get('Gasto (USD)', 0)

        # Agregar formato
        val_gasto_promedio_t =  formato_miles(valor=val_t, decimales=0)

        # Calcular variación
        val_gasto_promedio_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_gasto_promedio_variacion:
            bullet_gasto_promedio = f"En {year_global_data_t}, el gasto promedio del viajero de {pais_elegido} es USD {val_gasto_promedio_t}, lo que representa una variación del {val_gasto_promedio_variacion} en comparación con los {val_gasto_promedio_t_1} USD de {year_global_data_t_1}."
        else:
            bullet_gasto_promedio = f"En {year_global_data_t}, el gasto promedio del viajero de {pais_elegido} es USD {val_gasto_promedio_t} en comparación con los {val_gasto_promedio_t_1} USD de {year_global_data_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_promedio = None

    # Resultado
    return bullet_gasto_promedio

def global_data_bullets_gasto_categoria(df_global_data, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las principales categorías de gasto 
    de los viajeros internacionales de un país en un año determinado.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre las categorías de gasto.
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con las principales categorías en las que los viajeros han gastado en el año analizado.
        Si no hay datos disponibles, retorna None.
    """

    # Gasto por categoria
    df_gasto_categoria = df_global_data.get('gasto_categoria', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_categoria.empty:

        # Obtener el dataframe con el topn categorias
        df_gasto_categoria_topn = filtrar_df_top_n(df=df_gasto_categoria, year=year_global_data_t, categoria="Categoria de Gasto", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Categoria de Gasto']} ({row['Participación (%)']}%)" for _, row in df_gasto_categoria_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_gasto_categoria = None
        if len(elementos) == 1:
            bullet_gasto_categoria = f"La actividad en donde más gastan los viajeros internacionales de {pais_elegido} en {year_global_data_t} es {elementos[0]}."
        elif len(elementos) == 2:
            bullet_gasto_categoria = f"La actividad en donde más gastan los viajeros internacionales de {pais_elegido} en {year_global_data_t} es {elementos[0]}, seguida por {elementos[1]}."
        else:
            bullet_gasto_categoria = f"La actividad en donde más gastan los viajeros internacionales de {pais_elegido} en {year_global_data_t} es {elementos[0]}, seguida por {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_categoria = None

    # Resultado
    return bullet_gasto_categoria

def global_data_bullets_mice(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los flujos de viajeros internacionales 
    de un país que han viajado por motivos de negocios, reuniones, incentivos, congresos y exposiciones (MICE), 
    comparando dos años consecutivos.

    Parámetros:
    -----------
    df_global_data : dict
        Diccionario que contiene los datos globales, incluyendo la información sobre los flujos de viajeros 
        por motivo de viaje.
    year_global_data_t_1 : int
        Año base para la comparación (t-1).
    year_global_data_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con el número de viajeros que han viajado por motivos MICE en el año analizado y su variación 
        porcentual respecto al año anterior. Si no hay datos disponibles, retorna None.
    """

    # MICE
    df_mice = df_global_data.get('flujos_negocios', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_mice.empty:

        # Filtra solo mice
        df_mice = df_mice[df_mice['Motivo de viaje']=='Reuniones, incentivos, congresos y exposiciones (MICE)']

        # Volver diccionario
        dict_mice = df_mice.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_mice.get(year_global_data_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Viajeros', 0) * 1000

        # Agregar formato
        val_mice_t_1 =  formato_miles(valor=val_t_1, decimales=0)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_mice.get(year_global_data_t, {})

        # Extraer val t
        val_t = sub_dict.get('Viajeros', 0) * 1000

        # Agregar formato
        val_mice_t =  formato_miles(valor=val_t, decimales=0)

        # Calcular variación
        val_mice_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_mice_variacion:
            bullet_mice = f"En {year_global_data_t}, se registran {val_mice_t} viajeros provenientes de {pais_elegido} al exterior por motivo de negocios, lo que representa una variación del {val_mice_variacion} en comparación con los {val_mice_t_1} viajeros de {year_global_data_t_1}."
        else:
            bullet_mice = f"En {year_global_data_t}, se registran {val_mice_t} viajeros provenientes de {pais_elegido} al exterior por motivo de negocios, en comparación con los {val_mice_t_1} viajeros de {year_global_data_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_mice = None

    # Resultado
    return bullet_mice

def oag_bullets_frecuencias_mundo(df_oag, year_oag_t_1, year_oag_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las frecuencias aéreas internacionales 
    directas de un país con el mundo, comparando dos años consecutivos.

    Parámetros:
    -----------
    df_oag : dict
        Diccionario que contiene los datos de conectividad aérea, incluyendo la serie de tiempo 
        de frecuencias internacionales.
    year_oag_t_1 : int
        Año base para la comparación (t-1).
    year_oag_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país para el cual se genera el informe.

    Retorna:
    --------
    str o None
        Un texto con el número de frecuencias aéreas internacionales directas en el año analizado 
        y su variación porcentual respecto al año anterior. Si no hay datos disponibles, retorna None.
    """

    #  Frecuencias con el mundo 
    df_frecuencias_mundo = df_oag.get('conectividad_mundo_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_frecuencias_mundo.empty:

        # Volver diccionario
        dict_frecuencias_mundo = df_frecuencias_mundo.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_frecuencias_mundo.get(year_oag_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_frecuencias_mundo_t_1 =  formato_miles(valor=val_t_1, decimales=0)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_frecuencias_mundo.get(year_oag_t, {})

        # Extraer val t
        val_t = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_frecuencias_mundo_t =  formato_miles(valor=val_t, decimales=0)

        # Calcular variación
        val_frecuencias_mundo_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_frecuencias_mundo_variacion:
            bullet_frecuencias_mundo = f"En {year_oag_t}, se registraron {val_frecuencias_mundo_t} frecuencias aéreas internacionales directas de {pais_elegido} con el mundo, lo que representa una variación del {val_frecuencias_mundo_variacion} en comparación con las {val_frecuencias_mundo_t_1} frecuencias de {year_oag_t_1}."
        else:
            bullet_frecuencias_mundo = f"En {year_oag_t}, se registraron {val_frecuencias_mundo_t} frecuencias aéreas internacionales directas de {pais_elegido} con el mundo en comparación con las {val_frecuencias_mundo_t_1} frecuencias de {year_oag_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_frecuencias_mundo = None

    # Resultado
    return bullet_frecuencias_mundo

def oag_bullets_paises_con_frecuencias(year_oag_t, pais_elegido, sesion_activa):

    """
    Genera un texto en formato bullet con información sobre la cantidad de países con los que un país 
    tiene conexión aérea directa en un año determinado.

    Parámetros:
    -----------
    year_oag_t : int
        Año de análisis.
    pais_elegido : str
        Nombre del país para el cual se genera el informe.
    sesion_activa : objeto de sesión
        Conexión activa a la base de datos en Snowflake para ejecutar la consulta SQL.

    Retorna:
    --------
    str o None
        Un texto con el número de países con los que el país seleccionado tiene frecuencias aéreas directas 
        en el año analizado. Si no hay datos disponibles, retorna None.
    """

    # Número de países con frecuencias

    # Constuir consulta
    query_paises_con_frecuencias = f"""
    SELECT COUNT(DISTINCT PAIS_ARRIVAL) AS PAISES
    FROM REPOSITORIO_TURISMO.VISTAS.OAG_CONECTIVIDAD_MUNDO
    WHERE PAIS_DEPARTURE = '{pais_elegido}'
        AND PAIS_ARRIVAL <> '{pais_elegido}'
        AND SUBSTR(TIME_SERIES, 1, 4) = {year_oag_t}
    """

    # Ejecutar
    try:
        df_paises_con_frecuencias = pd.DataFrame(sesion_activa.sql(query_paises_con_frecuencias).collect())
    except:
        df_paises_con_frecuencias = pd.DataFrame()

    # Procesar si no llegan vacíos
    if not df_paises_con_frecuencias.empty:

        try:
            # Extraer el valor
            val_t = int(df_paises_con_frecuencias.iloc[0, 0])
        except:
            val_t = 0  # Si hay un error en la extracción, asignar 0

        # Crear bullet
        bullet_paises_con_frecuencias = f"En {year_oag_t}, {pais_elegido} conectó con {val_t} países"

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_paises_con_frecuencias = None


    # Resultado
    return bullet_paises_con_frecuencias

def oag_bullets_frecuencias_destino_cerrado(df_oag, year_oag_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los principales países con conectividad aérea 
    en términos de frecuencias desde un país en un año determinado.

    Parámetros:
    -----------
    df_oag : dict
        Diccionario que contiene los datos de conectividad aérea, incluyendo información sobre las frecuencias 
        por destino en un año cerrado.
    year_oag_t : int
        Año de análisis.
    pais_elegido : str
        Nombre del país desde el cual se evalúa la conectividad aérea.

    Retorna:
    --------
    str o None
        Un texto con los principales países con mayor conectividad aérea en términos de frecuencias desde 
        el país analizado en el año especificado. Si no hay datos disponibles, retorna None.
    """

    # Frecuencias por destino año cerrado
    df_frecuencias_destino_cerrado = df_oag.get('conectividad_mundo_destino_cerrado', pd.DataFrame())

    # Procesar si no llegan vacíos
    if (not df_frecuencias_destino_cerrado.empty) and (not df_frecuencias_destino_cerrado[df_frecuencias_destino_cerrado['Año']==year_oag_t].empty):

        # Crear una copia solo con las variables necesarias
        df_copy = df_frecuencias_destino_cerrado[['Año', 'País Destino', 'Participación Frecuencias (%)']]

        # Cambiar nombre de columna
        df_copy = df_copy.rename(columns = {'Participación Frecuencias (%)' : 'Participación (%)'})

        # Filtrar otros
        df_copy = df_copy[df_copy['País Destino'] != 'Otros']

        # Obtener el dataframe con el topn categorias (Se usa el try porque es probable que haya un rezago en el cargue de datos y que no haya info para crear el bullet)
        try:
            df_frecuencias_destino_cerrado_topn = filtrar_df_top_n(df=df_copy, year=year_oag_t, categoria="País Destino", top_n=5)
        except:
            df_frecuencias_destino_cerrado_topn = pd.DataFrame()

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        # (Se usa el try porque es probable que haya un rezago en el cargue de datos y que no haya info para crear el bullet)
        try: 
            elementos = [f"{row['País Destino']} ({row['Participación (%)']}%)" for _, row in df_frecuencias_destino_cerrado_topn.iterrows()]
        except:
            elementos = []

        # Construcción del bullet en función de la cantidad de elementos para año cerrado
        if len(elementos) == 0:
            bullet_frecuencias_destino_cerrado_t = None
        if len(elementos) == 1:
            bullet_frecuencias_destino_cerrado_t = f"En {year_oag_t}, los países con mayor conectividad áerea de frecuencias desde {pais_elegido} fueron {elementos[0].capitalize()}."
        elif len(elementos) == 2:
            bullet_frecuencias_destino_cerrado_t = f"En {year_oag_t}, los países con mayor conectividad áerea de frecuencias desde {pais_elegido} fueron {elementos[0].capitalize()} y {elementos[1].capitalize()}."
        else:
            bullet_frecuencias_destino_cerrado_t = f"En {year_oag_t}, los países con mayor conectividad áerea de frecuencias desde {pais_elegido} fueron {elementos[0].capitalize()}, {', '.join([e.capitalize() for e in elementos[1:-1]])} y {elementos[-1].capitalize()}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_frecuencias_destino_cerrado_t = None

    # Resultado
    return bullet_frecuencias_destino_cerrado_t

def fk_mundo_bullets_reservas_aereas_mex_cost_chi_per(df_fk, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las reservas aéreas activas desde un país 
    hacia México, Costa Rica, Chile y Perú en un periodo determinado.

    Parámetros:
    -----------
    df_fk : dict
        Diccionario que contiene los datos de reservas aéreas en serie de tiempo.
    pais_elegido : str
        Nombre del país desde el cual se originan las reservas.

    Retorna:
    --------
    str o None
        Un texto con el número total de reservas aéreas en el periodo analizado, junto con la distribución 
        porcentual entre los países destino. Si no hay datos disponibles, retorna None.
    """

    # Reservas aéreas hacia México, Costa Rica, Chile y Perú
    df_reservas_aereas_mex_cost_chi_per = df_fk.get('reservas_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_reservas_aereas_mex_cost_chi_per.empty:

        # En español:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

        # Extraer meses
        mes_min = df_reservas_aereas_mex_cost_chi_per["Fecha"].min().strftime("%B")
        mes_max = df_reservas_aereas_mex_cost_chi_per["Fecha"].max().strftime("%B")
        year = df_reservas_aereas_mex_cost_chi_per["Fecha"].max().strftime("%Y")

        # Calcular reservas
        try:
            df_reservas_agrupadas = df_reservas_aereas_mex_cost_chi_per.groupby('País', as_index=False).agg({'Reservas': 'sum'})

            # Calcular la participación porcentual
            total_reservas = df_reservas_agrupadas['Reservas'].sum()
            df_reservas_agrupadas['Participación (%)'] = (df_reservas_agrupadas['Reservas'] / total_reservas) * 100

            # Formatear la columna "Participación (%)" usando la función formato_miles
            df_reservas_agrupadas["Participación (%)"] = df_reservas_agrupadas["Participación (%)"].apply(lambda x: formato_miles(valor=x, decimales=2))

            # Ordenar de mayor a menor por la columna 'Reservas'
            df_reservas_agrupadas = df_reservas_agrupadas.sort_values(by='Reservas', ascending=False)

            # Crear suma de reservas del periodo
            reservas_totales = formato_miles(valor=df_reservas_agrupadas['Reservas'].sum(), decimales=0)

            # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
            elementos = [f"{row['País']} ({row['Participación (%)']}%)" for _, row in df_reservas_agrupadas.iterrows()]

            # Construcción del bullet en función de la cantidad de elementos
            if len(elementos) == 0:
                bullet_reservas_aereas_mex_cost_chi_per = None
            if len(elementos) == 1:
                bullet_reservas_aereas_mex_cost_chi_per = f"Entre {mes_min} y {mes_max} de {year} se registran {reservas_totales} reservas aéreas activas provenientes de {pais_elegido}. La distribución de estas reservas es la siguiente: {elementos[0]}."
            elif len(elementos) == 2:
                bullet_reservas_aereas_mex_cost_chi_per = f"Entre {mes_min} y {mes_max} de {year} se registran {reservas_totales} reservas aéreas activas provenientes de {pais_elegido}. La distribución de estas reservas es la siguiente: {elementos[0]} y {elementos[1]}."
            else:
                bullet_reservas_aereas_mex_cost_chi_per = f"Entre {mes_min} y {mes_max} de {year} se registran {reservas_totales} reservas aéreas activas provenientes de {pais_elegido}. La distribución de estas reservas es la siguiente: {elementos[0]}, {', '.join(elementos[1:-1])} y {elementos[-1]}."
            
        except Exception as e:
            bullet_reservas_aereas_mex_cost_chi_per = None

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_reservas_aereas_mex_cost_chi_per = None

    # Resultado
    return bullet_reservas_aereas_mex_cost_chi_per

def fk_mundo_bullets_busquedas_aereas_mex_cost_chi_per(df_fk, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las búsquedas aéreas activas desde un país 
    hacia México, Costa Rica, Chile y Perú en un periodo determinado.

    Parámetros:
    -----------
    df_fk : dict
        Diccionario que contiene los datos de búsquedas aéreas en serie de tiempo.
    pais_elegido : str
        Nombre del país desde el cual se originan las búsquedas.

    Retorna:
    --------
    str o None
        Un texto con el número total de búsquedas aéreas en el periodo analizado, junto con la distribución 
        porcentual entre los países destino. Si no hay datos disponibles, retorna None.
    """

    # Busquedas aéreas hacia México, Costa Rica, Chile y Perú
    df_busquedas_aereas_mex_cost_chi_per = df_fk.get('busquedas_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_busquedas_aereas_mex_cost_chi_per.empty:

        # En español:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

        # Extraer meses
        year_min = df_busquedas_aereas_mex_cost_chi_per["Fecha"].min().strftime("%Y")
        mes_min = df_busquedas_aereas_mex_cost_chi_per["Fecha"].min().strftime("%B")

        mes_max = df_busquedas_aereas_mex_cost_chi_per["Fecha"].max().strftime("%B")
        year_max = df_busquedas_aereas_mex_cost_chi_per["Fecha"].max().strftime("%Y")

        # Calcular busquedas
        try:
            df_busquedas_agrupadas = df_busquedas_aereas_mex_cost_chi_per.groupby('País', as_index=False).agg({'Búsquedas': 'sum'})

            # Calcular la participación porcentual
            total_busquedas = df_busquedas_agrupadas['Búsquedas'].sum()
            df_busquedas_agrupadas['Participación (%)'] = (df_busquedas_agrupadas['Búsquedas'] / total_busquedas) * 100

            # Formatear la columna "Participación (%)" usando la función formato_miles
            df_busquedas_agrupadas["Participación (%)"] = df_busquedas_agrupadas["Participación (%)"].apply(lambda x: formato_miles(valor=x, decimales=2))

            # Ordenar de mayor a menor por la columna 'Reservas'
            df_busquedas_agrupadas = df_busquedas_agrupadas.sort_values(by='Búsquedas', ascending=False)

            # Crear suma de busquedas del periodo
            busquedas_totales = formato_miles(valor=df_busquedas_agrupadas['Búsquedas'].sum(), decimales=0)

            # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
            elementos = [f"{row['País']} ({row['Participación (%)']}%)" for _, row in df_busquedas_agrupadas.iterrows()]

            # Construcción del bullet en función de la cantidad de elementos
            if len(elementos) == 0:
                bullet_busquedas_aereas_mex_cost_chi_per = None
            if len(elementos) == 1:
                bullet_busquedas_aereas_mex_cost_chi_per = f"Entre {mes_min} de {year_min} y {mes_max} de {year_max} se registran {busquedas_totales} búsquedas aéreas activas provenientes de {pais_elegido}. La distribución de estas busquedas es la siguiente: {elementos[0]}."
            elif len(elementos) == 2:
                bullet_busquedas_aereas_mex_cost_chi_per = f"Entre {mes_min} de {year_min} y {mes_max} de {year_max} se registran {busquedas_totales} búsquedas aéreas activas provenientes de {pais_elegido}. La distribución de estas busquedas es la siguiente: {elementos[0]} y {elementos[1]}."
            else:
                bullet_busquedas_aereas_mex_cost_chi_per = f"Entre {mes_min} de {year_min} y {mes_max} de {year_max} se registran {busquedas_totales} búsquedas aéreas activas provenientes de {pais_elegido}. La distribución de estas busquedas es la siguiente: {elementos[0]}, {', '.join(elementos[1:-1])} y {elementos[-1]}."

        except Exception as e:
            bullet_busquedas_aereas_mex_cost_chi_per = None

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_busquedas_aereas_mex_cost_chi_per = None

    # Resultado
    return bullet_busquedas_aereas_mex_cost_chi_per

def oag_bullets_frecuencias_colombia(df_oag, year_oag_t_1, year_oag_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las frecuencias aéreas internacionales 
    directas entre un país y Colombia, comparando dos años consecutivos.

    Parámetros:
    -----------
    df_oag : dict
        Diccionario que contiene los datos de conectividad aérea, incluyendo la serie de tiempo 
        de frecuencias internacionales con Colombia.
    year_oag_t_1 : int
        Año base para la comparación (t-1).
    year_oag_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país desde el cual se analizan las frecuencias con Colombia.

    Retorna:
    --------
    str o None
        Un texto con el número de frecuencias aéreas internacionales directas entre el país seleccionado 
        y Colombia en el año analizado, junto con la variación porcentual respecto al año anterior. 
        Si no hay datos disponibles, retorna None.
    """

    #  Frecuencias con el colombia 
    df_frecuencias_colombia = df_oag.get('conectividad_colombia_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_frecuencias_colombia.empty:

        # Volver diccionario
        dict_frecuencias_colombia = df_frecuencias_colombia.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_frecuencias_colombia.get(year_oag_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_frecuencias_colombia_t_1 = formato_miles(valor=val_t_1, decimales=0)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_frecuencias_colombia.get(year_oag_t, {})

        # Extraer val t
        val_t = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_frecuencias_colombia_t =  formato_miles(valor=val_t, decimales=0)

        # Calcular variación
        val_frecuencias_colombia_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_frecuencias_colombia_variacion:
            bullet_frecuencias_colombia = f"En {year_oag_t}, se registraron {val_frecuencias_colombia_t} frecuencias aéreas internacionales directas entre {pais_elegido} con Colombia, lo que representa una variación del {val_frecuencias_colombia_variacion} en comparación con las {val_frecuencias_colombia_t_1} frecuencias de {year_oag_t_1}."
        else:
            bullet_frecuencias_colombia = f"En {year_oag_t}, se registraron {val_frecuencias_colombia_t} frecuencias aéreas internacionales directas de {pais_elegido} con Colombia en comparación con las {val_frecuencias_colombia_t_1} frecuencias de {year_oag_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_frecuencias_colombia = None

    # Resultado
    return bullet_frecuencias_colombia

def oag_bullets_frecuencias_municipio_cerrado(df_oag, year_oag_t_1, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre los municipios de Colombia con mayor 
    conectividad aérea en términos de frecuencias directas desde un país en un año determinado.

    Parámetros:
    -----------
    df_oag : dict
        Diccionario que contiene los datos de conectividad aérea, incluyendo información sobre las 
        frecuencias por municipio en un año cerrado.
    year_oag_t_1 : int
        Año base para la comparación (t-1).
    pais_elegido : str
        Nombre del país desde el cual se evalúa la conectividad aérea con los municipios de Colombia.

    Retorna:
    --------
    str
        Un texto con los municipios colombianos con mayor conectividad aérea en términos de frecuencias 
        directas desde el país analizado en el año especificado. Si no hay conectividad, retorna un mensaje 
        indicando la ausencia de vuelos directos.
    """

    # Frecuencias por municipio año cerrado
    df_frecuencias_municipio_cerrado = df_oag.get('conectividad_colombia_municipio_cerrado', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_frecuencias_municipio_cerrado.empty:

        # Crear una copia solo con las variables necesarias
        df_copy = df_frecuencias_municipio_cerrado[['Año', 'Municipio Destino', 'Participación Frecuencias (%)']]

        # Cambiar nombre de columna
        df_copy = df_copy.rename(columns = {'Participación Frecuencias (%)' : 'Participación (%)'})

        # Filtrar otros
        df_copy = df_copy[df_copy['Municipio Destino'] != 'Otros']

        # Obtener el dataframe con el topn categorias
        df_frecuencias_municipio_cerrado_topn = filtrar_df_top_n(df=df_copy, year=year_oag_t_1, categoria="Municipio Destino", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Municipio Destino']} ({row['Participación (%)']}%)" for _, row in df_frecuencias_municipio_cerrado_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos para año cerrado
        if len(elementos) == 0:
            bullet_frecuencias_municipio_cerrado_t = f"Actualmente Colombia no tiene conectividad directa con {pais_elegido}"
        if len(elementos) == 1:
            bullet_frecuencias_municipio_cerrado_t = f"En {year_oag_t_1}, los municipios con mayor conectividad áerea de frecuencias directas desde {pais_elegido} son {elementos[0].capitalize()}."
        elif len(elementos) == 2:
            bullet_frecuencias_municipio_cerrado_t = f"En {year_oag_t_1}, los municipios con mayor conectividad áerea de frecuencias directas desde {pais_elegido} son {elementos[0].capitalize()} y {elementos[1].capitalize()}."
        else:
            bullet_frecuencias_municipio_cerrado_t = f"En {year_oag_t_1}, los municipios con mayor conectividad áerea de frecuencias directas desde {pais_elegido} son {elementos[0].capitalize()}, {', '.join([e.capitalize() for e in elementos[1:-1]])} y {elementos[-1].capitalize()}."

    # Sin conectividad aérea
    else:
        bullet_frecuencias_municipio_cerrado_t = f"Actualmente Colombia no tiene conectividad directa con {pais_elegido}"

    # Resultado
    return bullet_frecuencias_municipio_cerrado_t

def credibanco_bullets_gasto_cerrado_promedio(df_credibanco, year_credibanco_t_1, year_credibanco_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre el gasto promedio con tarjeta de crédito 
    de los viajeros de un país en Colombia, comparando dos años consecutivos.

    Parámetros:
    -----------
    df_credibanco : dict
        Diccionario que contiene los datos de gasto con tarjeta de crédito en serie de tiempo.
    year_credibanco_t_1 : int
        Año base para la comparación (t-1).
    year_credibanco_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país desde el cual provienen los viajeros cuyo gasto se analiza.

    Retorna:
    --------
    str o None
        Un texto con el gasto promedio con tarjeta de crédito en Colombia en el año analizado, junto con 
        la variación porcentual respecto al año anterior. Si no hay datos disponibles, retorna None.
    """

    # Gasto cerrado promedio 
    df_gasto_credibanco_cerrado_promedio = df_credibanco.get('gasto_promedio', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_credibanco_cerrado_promedio.empty:

        # Volver diccionario
        dict_gasto_credibanco_cerrado_promedio = df_gasto_credibanco_cerrado_promedio.set_index('Año').T.to_dict()

        # Obtener valores para t_1

        # Extraer subdiccionario
        sub_dict = dict_gasto_credibanco_cerrado_promedio.get(year_credibanco_t_1, {})

        # Extraer val t_1
        val_t_1 = sub_dict.get('Gasto promedio tarjeta (USD)', 0)

        # Agregar formato
        val_gasto_credibanco_cerrado_promedio_t_1 = formato_miles(valor=val_t_1, decimales=1)

        # Obtener valores para t

        # Extraer subdiccionario
        sub_dict = dict_gasto_credibanco_cerrado_promedio.get(year_credibanco_t, {})

        # Extraer val t
        val_t = sub_dict.get('Gasto promedio tarjeta (USD)', 0)

        # Agregar formato
        val_gasto_credibanco_cerrado_promedio_t =  formato_miles(valor=val_t, decimales=1)

        # Calcular variación
        val_gasto_credibanco_cerrado_promedio_variacion = calcular_tasa_variacion(val_t, val_t_1)

        # Crear bullet
        if val_gasto_credibanco_cerrado_promedio_variacion:
            bullet_gasto_credibanco_cerrado_promedio = f"En {year_credibanco_t}, el gasto promedio del viajero de {pais_elegido} en Colombia con tarjeta de crédito fue USD {val_gasto_credibanco_cerrado_promedio_t}, lo que representa una variación del {val_gasto_credibanco_cerrado_promedio_variacion} en comparación con los {val_gasto_credibanco_cerrado_promedio_t_1} USD de {year_credibanco_t_1}."
        else:
            bullet_gasto_credibanco_cerrado_promedio = f"En {year_credibanco_t}, el gasto promedio del viajero de {pais_elegido} en Colombia con tarjeta de crédito fue USD {val_gasto_credibanco_cerrado_promedio_t} en comparación con los {val_gasto_credibanco_cerrado_promedio_t_1} USD de {year_credibanco_t_1}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_credibanco_cerrado_promedio = None

    # Resultado
    return bullet_gasto_credibanco_cerrado_promedio

def credibanco_bullets_gasto_directo_indirecto_cerrado(df_credibanco, year_credibanco_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre la participación y facturación del gasto 
    directo e indirecto de los viajeros de un país en Colombia en un año determinado.

    Parámetros:
    -----------
    df_credibanco : dict
        Diccionario que contiene los datos de gasto con tarjeta de crédito categorizados en directo e indirecto.
    year_credibanco_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país desde el cual provienen los viajeros cuyo gasto se analiza.

    Retorna:
    --------
    str o None
        Un texto con el porcentaje de participación y la facturación en USD del gasto directo e indirecto 
        de los viajeros en Colombia durante el año analizado. Si no hay datos disponibles, retorna None.
    """

    # Gasto directo e indirecto cerrado
    df_gasto_directo_indirecto_credibanco_cerrado = df_credibanco.get('gasto_categoria', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_directo_indirecto_credibanco_cerrado.empty:

        # Filtrar año
        df_gasto_directo_indirecto_credibanco_cerrado = df_gasto_directo_indirecto_credibanco_cerrado[df_gasto_directo_indirecto_credibanco_cerrado['Año']==year_credibanco_t]

        # Volver diccionario
        dict_gasto_directo_indirecto_credibanco_cerrado = df_gasto_directo_indirecto_credibanco_cerrado.set_index('Clasificación').T.to_dict()

        # Extraer subdiccionario
        sub_dict_directo = dict_gasto_directo_indirecto_credibanco_cerrado.get('Directo', {})
        sub_dict_indirecto = dict_gasto_directo_indirecto_credibanco_cerrado.get('Indirecto', {})

        # Extrae elementos

        # Directo
        part_directo = formato_miles(valor=sub_dict_directo.get('Participación (%)', 0), decimales=1)
        factu_directo = formato_miles(valor=sub_dict_directo.get('Facturación (USD)', 0), decimales=0)

        # Indirecto
        part_indirecto = formato_miles(valor=sub_dict_indirecto.get('Participación (%)', 0), decimales=1)
        factu_indirecto = formato_miles(valor=sub_dict_indirecto.get('Facturación (USD)', 0), decimales=0)

        # Crear bullet
        bullet_gasto_directo_indirecto_credibanco_cerrado = f"En {year_credibanco_t}, el gasto directo en Colombia del viajero de {pais_elegido} participa con el {part_directo}% y una facturación total de USD {factu_directo}, mientras que los gastos indirectos tienen una participación de {part_indirecto}% y una facturación total de USD {factu_indirecto}"

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_directo_indirecto_credibanco_cerrado = None

    # Resultado
    return bullet_gasto_directo_indirecto_credibanco_cerrado


def credibanco_bullets_gasto_directo_cerrado(df_credibanco, year_credibanco_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre la distribución del gasto directo en turismo 
    de los viajeros de un país en Colombia, basado en las principales categorías de gasto en un año determinado.

    Parámetros:
    -----------
    df_credibanco : dict
        Diccionario que contiene los datos de gasto con tarjeta de crédito categorizados por productos de gasto directo.
    year_credibanco_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país desde el cual provienen los viajeros cuyo gasto se analiza.

    Retorna:
    --------
    str o None
        Un texto con la distribución del gasto directo en turismo, especificando las principales categorías de gasto 
        y su porcentaje de participación en el año analizado. Si no hay datos disponibles, retorna None.
    """

    # Producto de gasto directo
    df_gasto_directo_cerrado = df_credibanco.get('gasto_producto_directo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_directo_cerrado.empty:

        # Filtrar otros
        df_gasto_directo_cerrado = df_gasto_directo_cerrado[df_gasto_directo_cerrado['Categoria'] != 'Otros']

        # Obtener el dataframe con el topn categorias
        df_gasto_directo_cerrado_topn = filtrar_df_top_n(df=df_gasto_directo_cerrado, year=year_credibanco_t, categoria="Categoria", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Categoria']} ({row['Participación (%)']}%)" for _, row in df_gasto_directo_cerrado_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_gasto_directo_cerrado = None
        if len(elementos) == 1:
            bullet_gasto_directo_cerrado = f"En {year_credibanco_t}, la distribución del gasto directo en turismo de los viajeros de {pais_elegido} es: {elementos[0]}."
        elif len(elementos) == 2:
            bullet_gasto_directo_cerrado = f"En {year_credibanco_t}, la distribución del gasto directo en turismo de los viajeros de {pais_elegido} es: {elementos[0]} y {elementos[1]}."
        else:
            bullet_gasto_directo_cerrado = f"En {year_credibanco_t}, la distribución del gasto directo en turismo de los viajeros de {pais_elegido} es: {elementos[0]}, {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_directo_cerrado = None

    # Resultado
    return bullet_gasto_directo_cerrado

def credibanco_bullets_gasto_indirecto_cerrado(df_credibanco, year_credibanco_t, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre la distribución del gasto indirecto en turismo 
    de los viajeros de un país en Colombia, basado en las principales categorías de gasto en un año determinado.

    Parámetros:
    -----------
    df_credibanco : dict
        Diccionario que contiene los datos de gasto con tarjeta de crédito categorizados por productos de gasto indirecto.
    year_credibanco_t : int
        Año actual de análisis (t).
    pais_elegido : str
        Nombre del país desde el cual provienen los viajeros cuyo gasto se analiza.

    Retorna:
    --------
    str o None
        Un texto con la distribución del gasto indirecto en turismo, especificando las principales categorías de gasto 
        y su porcentaje de participación en el año analizado. Si no hay datos disponibles, retorna None.
    """

    # Producto de gasto indirecto
    df_gasto_indirecto_cerrado = df_credibanco.get('gasto_producto_indirecto', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_indirecto_cerrado.empty:

        # Filtrar otros
        df_gasto_indirecto_cerrado = df_gasto_indirecto_cerrado[df_gasto_indirecto_cerrado['Categoria'] != 'Otros']

        # Obtener el dataframe con el topn categorias
        df_gasto_indirecto_cerrado_topn = filtrar_df_top_n(df=df_gasto_indirecto_cerrado, year=year_credibanco_t, categoria="Categoria", top_n=5)

        # Convertir las filas en una lista de strings con el formato "[categoría] ([porcentaje]%)"
        elementos = [f"{row['Categoria']} ({row['Participación (%)']}%)" for _, row in df_gasto_indirecto_cerrado_topn.iterrows()]

        # Construcción del bullet en función de la cantidad de elementos
        if len(elementos) == 0:
            bullet_gasto_indirecto_cerrado = None
        if len(elementos) == 1:
            bullet_gasto_indirecto_cerrado = f"En {year_credibanco_t}, la distribución del gasto indirecto en turismo de los viajeros de {pais_elegido} es: {elementos[0]}."
        elif len(elementos) == 2:
            bullet_gasto_indirecto_cerrado = f"En {year_credibanco_t}, la distribución del gasto indirecto en turismo de los viajeros de {pais_elegido} es: {elementos[0]} y {elementos[1]}."
        else:
            bullet_gasto_indirecto_cerrado = f"En {year_credibanco_t}, la distribución del gasto indirecto en turismo de los viajeros de {pais_elegido} es: {elementos[0]}, {', '.join(elementos[1:-1])} y {elementos[-1]}."

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_gasto_indirecto_cerrado = None

    # Resultado
    return bullet_gasto_indirecto_cerrado

def fk_colombia_bullets_reservas_aereas_colombia(df_fk, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las reservas aéreas activas 
    desde un país hacia Colombia en un periodo determinado.

    Parámetros:
    -----------
    df_fk : dict
        Diccionario que contiene los datos de reservas aéreas hacia Colombia en serie de tiempo.
    pais_elegido : str
        Nombre del país desde el cual se originan las reservas hacia Colombia.

    Retorna:
    --------
    str o None
        Un texto con el número total de reservas aéreas en el periodo analizado. 
        Si no hay datos disponibles, retorna None.
    """

    # Reservas aéreas hacia Colombia
    df_reservas_aereas_colombia = df_fk.get('reservas_serie_tiempo_colombia', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_reservas_aereas_colombia.empty:

        # En español:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

        # Extraer meses
        mes_min = df_reservas_aereas_colombia["Fecha"].min().strftime("%B")
        mes_max = df_reservas_aereas_colombia["Fecha"].max().strftime("%B")
        year = df_reservas_aereas_colombia["Fecha"].max().strftime("%Y")

        # Calcular reservas
        try:
            
            # Crear suma de reservas del periodo
            reservas_totales = formato_miles(valor=df_reservas_aereas_colombia['Reservas'].sum(), decimales=0)

            # Construcción del bullet 
            bullet_reservas_aereas_colombia = f"Entre {mes_min} y {mes_max} de {year} se registran {reservas_totales} reservas aéreas activas provenientes de {pais_elegido} hacia Colombia."

        except Exception as e:
            bullet_reservas_aereas_colombia = None

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_reservas_aereas_colombia = None

    # Resultado
    return bullet_reservas_aereas_colombia

def fk_colombia_bullets_busquedas_aereas_colombia(df_fk, pais_elegido):

    """
    Genera un texto en formato bullet con información sobre las búsquedas aéreas activas 
    desde un país hacia Colombia en un periodo determinado.

    Parámetros:
    -----------
    df_fk : dict
        Diccionario que contiene los datos de búsquedas aéreas hacia Colombia en serie de tiempo.
    pais_elegido : str
        Nombre del país desde el cual se originan las búsquedas hacia Colombia.

    Retorna:
    --------
    str o None
        Un texto con el número total de búsquedas aéreas en el periodo analizado. 
        Si no hay datos disponibles, retorna None.
    """

    # Busquedas aéreas hacia Colombia
    df_busquedas_aereas_colombia = df_fk.get('busquedas_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_busquedas_aereas_colombia.empty:

        # En español:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

        # Extraer meses
        year_min = df_busquedas_aereas_colombia["Fecha"].min().strftime("%Y")
        mes_min = df_busquedas_aereas_colombia["Fecha"].min().strftime("%B")

        mes_max = df_busquedas_aereas_colombia["Fecha"].max().strftime("%B")
        year_max = df_busquedas_aereas_colombia["Fecha"].max().strftime("%Y")

        # Calcular busquedas
        try:
            busquedas_totales = formato_miles(valor=df_busquedas_aereas_colombia['Búsquedas'].sum(), decimales=0)

            # Construcción del bullet
            bullet_busquedas_aereas_colombia = f"Entre {mes_min} de {year_min} y {mes_max} de {year_max} se registran {busquedas_totales} búsquedas aéreas activas provenientes de {pais_elegido} hacia Colombia"
            
        except Exception as e:
            bullet_busquedas_aereas_colombia = None

    # En caso de que no hayan datos devolver un bullet vacío
    else:
        bullet_busquedas_aereas_colombia = None

    # Resultado
    return bullet_busquedas_aereas_colombia


def obtener_bullets(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido, df_oag, year_oag_t_1, year_oag_t, sesion_activa, df_fk, df_credibanco, year_credibanco_t_1, year_credibanco_t):

    """
    Genera y retorna un conjunto de strings (bullets) que describen diversas métricas
    y estadísticas relacionadas con el comportamiento de viajeros, conectividad aérea
    y gastos en Colombia u otros destinos. Los datos se obtienen de distintas fuentes
    (df_global_data, df_oag, df_fk, df_credibanco) y se comparan entre dos periodos
    indicados.

    Parámetros
    ----------
    df_global_data : dict
        Diccionario con información global de viajeros (GlobalData).
    year_global_data_t_1 : int
        Año base para la comparación en la información de GlobalData (t-1).
    year_global_data_t : int
        Año actual de análisis en la información de GlobalData (t).
    pais_elegido : str
        País para el cual se generan las métricas y estadísticas.
    df_oag : dict
        Diccionario con información de conectividad aérea (OAG).
    year_oag_t_1 : int
        Año base para la comparación en la información de OAG (t-1).
    year_oag_t : int
        Año actual de análisis en la información de OAG (t).
    sesion_activa : objeto de sesión
        Conexión activa a la base de datos (Snowflake) para ejecutar consultas SQL.
    df_fk : dict
        Diccionario con información de reservas y búsquedas aéreas (ForwardKeys).
    df_credibanco : dict
        Diccionario con información de gastos con tarjeta de crédito (Credibanco).
    year_credibanco_t_1 : int
        Año base para la comparación en la información de Credibanco (t-1).
    year_credibanco_t : int
        Año actual de análisis en la información de Credibanco (t).

    Retorna
    -------
    dict
        Un diccionario con todos los bullets generados. Cada clave describe el tipo 
        de información contenida en el bullet, y su valor es el texto generado o None 
        si no hubo datos disponibles.
    """

    ############
    # GlobalData
    ############

    bullet_flujos_viajeros_mundo = global_data_bullets_viajeros_mundo(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido)
    bullet_medio_transporte = global_data_bullets_medio_transporte(df_global_data, year_global_data_t, pais_elegido)
    bullet_noches_percnotacion = global_data_bullets_noches_percnotacion(df_global_data, year_global_data_t, pais_elegido)
    bullet_rango_edad = global_data_bullets_rango_edad(df_global_data, year_global_data_t, pais_elegido)
    bullet_motivo_viaje = global_data_bullets_motivo_viaje(df_global_data, year_global_data_t, pais_elegido)
    bullet_forma_viaje = global_data_bullets_forma_viaje(df_global_data, year_global_data_t, pais_elegido)
    (bullet_destinos_internacionales_t_1, bullet_destinos_internacionales_t) = global_data_bullets_destinos_internacionales(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido)
    bullet_gasto_promedio = global_data_bullets_gasto_promedio(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido)
    bullet_gasto_categoria = global_data_bullets_gasto_categoria(df_global_data, year_global_data_t, pais_elegido)
    bullet_mice = global_data_bullets_mice(df_global_data, year_global_data_t_1, year_global_data_t, pais_elegido)

    ###########
    # OAG Mundo
    ###########

    bullet_frecuencias_mundo = oag_bullets_frecuencias_mundo(df_oag, year_oag_t_1, year_oag_t, pais_elegido)
    bullet_paises_con_frecuencias = oag_bullets_paises_con_frecuencias(year_oag_t, pais_elegido, sesion_activa)
    bullet_frecuencias_destino_cerrado_t = oag_bullets_frecuencias_destino_cerrado(df_oag, year_oag_t, pais_elegido)

    ##########
    # FK Mundo
    ##########

    bullet_reservas_aereas_mex_cost_chi_per = fk_mundo_bullets_reservas_aereas_mex_cost_chi_per(df_fk, pais_elegido)
    bullet_busquedas_aereas_mex_cost_chi_per = fk_mundo_bullets_busquedas_aereas_mex_cost_chi_per(df_fk, pais_elegido)

    ##############
    # OAG Colombia
    ##############

    bullet_frecuencias_colombia = oag_bullets_frecuencias_colombia(df_oag, year_oag_t_1, year_oag_t, pais_elegido)
    bullet_frecuencias_municipio_cerrado_t = oag_bullets_frecuencias_municipio_cerrado(df_oag, year_oag_t_1, pais_elegido)

    ############
    # Credibanco
    ############

    bullet_gasto_credibanco_cerrado_promedio = credibanco_bullets_gasto_cerrado_promedio(df_credibanco, year_credibanco_t_1, year_credibanco_t, pais_elegido)
    bullet_gasto_directo_indirecto_credibanco_cerrado = credibanco_bullets_gasto_directo_indirecto_cerrado(df_credibanco, year_credibanco_t, pais_elegido)
    bullet_gasto_directo_cerrado = credibanco_bullets_gasto_directo_cerrado(df_credibanco, year_credibanco_t, pais_elegido)
    bullet_gasto_indirecto_cerrado = credibanco_bullets_gasto_indirecto_cerrado(df_credibanco, year_credibanco_t, pais_elegido)

    #############
    # FK Colombia
    #############
    
    bullet_reservas_aereas_colombia = fk_colombia_bullets_reservas_aereas_colombia(df_fk, pais_elegido)
    bullet_busquedas_aereas_colombia = fk_colombia_bullets_busquedas_aereas_colombia(df_fk, pais_elegido)

    # Retornar todos los bullets en un diccionario
    return {
        "bullet_flujos_viajeros_mundo": bullet_flujos_viajeros_mundo,
        "bullet_medio_transporte": bullet_medio_transporte,
        "bullet_noches_percnotacion": bullet_noches_percnotacion,
        "bullet_rango_edad": bullet_rango_edad,
        "bullet_motivo_viaje": bullet_motivo_viaje,
        "bullet_forma_viaje": bullet_forma_viaje,
        "bullet_destinos_internacionales_t_1": bullet_destinos_internacionales_t_1,
        "bullet_destinos_internacionales_t": bullet_destinos_internacionales_t,
        "bullet_gasto_promedio": bullet_gasto_promedio,
        "bullet_gasto_categoria": bullet_gasto_categoria,
        "bullet_mice": bullet_mice,
        "bullet_frecuencias_mundo": bullet_frecuencias_mundo,
        "bullet_paises_con_frecuencias": bullet_paises_con_frecuencias,
        "bullet_frecuencias_destino_cerrado_t": bullet_frecuencias_destino_cerrado_t,
        "bullet_reservas_aereas_mex_cost_chi_per": bullet_reservas_aereas_mex_cost_chi_per,
        "bullet_busquedas_aereas_mex_cost_chi_per": bullet_busquedas_aereas_mex_cost_chi_per,
        "bullet_frecuencias_colombia": bullet_frecuencias_colombia,
        "bullet_frecuencias_municipio_cerrado_t": bullet_frecuencias_municipio_cerrado_t,
        "bullet_gasto_credibanco_cerrado_promedio": bullet_gasto_credibanco_cerrado_promedio,
        "bullet_gasto_directo_indirecto_credibanco_cerrado": bullet_gasto_directo_indirecto_credibanco_cerrado,
        "bullet_gasto_directo_cerrado": bullet_gasto_directo_cerrado,
        "bullet_gasto_indirecto_cerrado": bullet_gasto_indirecto_cerrado,
        "bullet_reservas_aereas_colombia": bullet_reservas_aereas_colombia,
        "bullet_busquedas_aereas_colombia": bullet_busquedas_aereas_colombia
    }