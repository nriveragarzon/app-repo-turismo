# Procesamiento de datos

# Este modulo contiene todas las funciones necesarios para obtener, procesar y limpiar los datos del repositorio de turismo

# Importar módulos necesarios

import src.snowflake_analitica as snowflake_analitica

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
            
            # Crear agrupación de Top 5 para gráficar
            # Sumar VIAJEROS por país para el periodo total
            suma_viajeros_por_pais = df_destinos.groupby('PAIS_DESTINO')['VIAJEROS'].sum().reset_index()

            # Obtener el top 5 de países con más viajeros
            top5_paises = suma_viajeros_por_pais.nlargest(5, 'VIAJEROS')['PAIS_DESTINO']

            # Obtener número de países
            num_paises = len(suma_viajeros_por_pais['PAIS_DESTINO'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_paises <= 5:
                # Si hay 5 países o menos, devolver el DataFrame original sin cambios
                df_destinos_top5 = df_destinos.copy()
            else:
                # Filtrar por los países en el top 5 y los demás como "Otros"
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
            top_5_paises = frecuencias_por_pais.nlargest(5, 'FRECUENCIAS')['PAIS_ARRIVAL']

            # Obtener el número de países
            num_paises = len(frecuencias_por_pais['PAIS_ARRIVAL'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_paises <= 5:
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
            top_5_municipios = frecuencias_por_municipio.nlargest(5, 'FRECUENCIAS')['MUNICIPIO_DANE']

            # Obtener el número de municipios
            num_municipios = len(frecuencias_por_municipio['MUNICIPIO_DANE'].unique())

            # Crear el nuevo DataFrame con top 5 y agrupar los demás bajo "Otros"
            if num_municipios <= 5:
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
            WHERE TO_DATE(SEARCH_DATE, 'YYYY-MM-DD') BETWEEN DATEADD(MONTH, -6, CURRENT_DATE()) AND CURRENT_DATE()
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
            # Cambiar nombres de columnas
            df_gasto_promedio = df_gasto_promedio.rename(columns = {'YEAR' : 'Año', 'FACTURACION_USD' : 'Facturación (USD)', 'TURISTAS' : 'Turistas', 'TRANSACCIONES' : 'Transacciones', 'GASTO_PROMEDIO_TARJETA' : 'Gasto promedio tarjeta (USD)', 'GASTO_PROMEDIO_TRANSACCION' : 'Gasto promedio transacción (USD)'})
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
                YEAR AS YEAR,
                COUNT(DISTINCT AGENCIAS) AS AGENCIAS 
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}'
            GROUP BY PAIS_AGENCIA, YEAR;
        """,
        "ciudades_agencias": f"""
            SELECT INITCAP(TRAVEL_AGENCY_CITY) AS TRAVEL_AGENCY_CITY,
                YEAR AS YEAR,
                COUNT(DISTINCT AGENCIAS) AS AGENCIAS
            FROM REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS
            WHERE PAIS_AGENCIA = '{pais_seleccionado}'
            GROUP BY TRAVEL_AGENCY_CITY, YEAR;
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
