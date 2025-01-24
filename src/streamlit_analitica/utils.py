# Librerias
import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import requests
import plotly.graph_objects as go
from io import BytesIO
import src.snowflake_analitica as snowflake_analitica
import src.procesamiento_datos as procesamiento_datos
import src.plotly_analitica as plotly_analitica

# Geolocalización
def mostrar_mapa(pais):
    """
    Función que, a partir del código ISO de un país (ej. 'CO', 'US', 'MX'),
    obtiene sus coordenadas y muestra un mapa con una capa de datos
    generados aleatoriamente alrededor de la ubicación del país.

    Parámetros:
    -----------
    pais : str
        Código ISO del país (2 o 3 letras). Ejemplo: 'CO' para Colombia.

    Acciones:
    ---------
    1. Hace una solicitud HTTP a la API de restcountries.com para obtener
       la información del país.
    2. Extrae la latitud y longitud del país.
    3. Genera datos de ejemplo de latitudes y longitudes aleatorias
       distribuidas alrededor de la ubicación del país.
    4. Renderiza un mapa interactivo con pydeck usando un HexagonLayer.

    Manejador de errores:
    ---------------------
    - Si ocurre algún problema de red o con la respuesta, se muestra un
      mensaje de error a través de st.error().
    - Si el país no existe o la respuesta no incluye coordenadas válidas,
      se muestra un mensaje de error.
    - Si ocurre cualquier otra excepción, se notifica al usuario indicando
      el tipo de error.

    Uso:
    ----
    Dentro de tu aplicación Streamlit, simplemente llama a:

        mostrar_mapa("CO")

    para mostrar el mapa del país correspondiente.
    """
    try:
        # Construcción de la URL de la API
        api_url = f"https://restcountries.com/v3.1/alpha/{pais.upper()}"
        
        # Solicitud GET
        response = requests.get(api_url)
        response.raise_for_status()  # Lanza un error si la solicitud falla

        # Transformar la respuesta en JSON
        country_data = response.json()
        if not country_data or not isinstance(country_data, list):
            st.error(f"No se encontraron datos para el país '{pais}'. Verifica el código ISO.")
            return

        # Se asume que la primera posición del arreglo tiene la info
        latlng = country_data[0].get('latlng', [None, None])
        if latlng[0] is None or latlng[1] is None:
            st.error(f"No se obtuvieron coordenadas válidas para el país '{pais}'.")
            return

        lat, lon = latlng[0], latlng[1]

        # Crear datos de ejemplo alrededor de la posición (lat, lon)
        chart_data = pd.DataFrame(
            np.random.randn(1000, 2) / [50, 50] + [lat, lon],
            columns=['lat', 'lon']
        )

        # Mostrar el mapa con pydeck
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/outdoors-v11",
            initial_view_state=pdk.ViewState(
                latitude=lat,
                longitude=lon,
                zoom=4,
                pitch=0
            ),
            layers=[
                pdk.Layer(
                    'HexagonLayer',
                    data=chart_data,
                    get_position='[lon, lat]',
                    radius=2,
                    elevation_scale=4,
                    elevation_range=[0, 0],
                    pickable=True,
                    extruded=True
                )
            ],
        ))

    except requests.exceptions.RequestException as req_err:
        st.error(f"Error en la solicitud a la API: {req_err}")
    except (IndexError, KeyError, TypeError) as parse_err:
        st.error(f"No se pudieron extraer correctamente los datos del país '{pais}': {parse_err}")
    except Exception as e:
        st.error(f"Ha ocurrido un error inesperado al generar el mapa de '{pais}': {e}")

def excel_download_buttons(df: pd.DataFrame, file_name: str = "export.xlsx") -> BytesIO:
    """
    Genera un archivo Excel en memoria a partir de un DataFrame y retorna un buffer en formato BytesIO.

    Descripción:
    ------------
    - Crea un objeto BytesIO y lo asocia con un ExcelWriter de pandas utilizando 'xlsxwriter' como motor.
    - Inicialmente, se escribe un DataFrame vacío en la hoja "Sheet1" para asegurar la creación de la hoja.
    - Se obtiene el objeto 'worksheet' de la hoja activa "Sheet1" y se escribe el texto "Centro de Inteligencia de Turismo (CIT)"" en la celda A1.
    - A continuación, se vuelca el DataFrame 'df' a partir de la fila 3 (startrow=3), incluyendo el encabezado (header=True).
    - El objeto 'writer' se cierra automáticamente al salir del bloque 'with'.
    - Finalmente, la función retorna el buffer con el contenido del Excel en memoria.

    Parámetros:
    -----------
    df : pd.DataFrame
        DataFrame que se desea exportar al archivo Excel.
    file_name : str, opcional (por defecto "export.xlsx")
        Nombre sugerido para el archivo Excel. Aunque no se usa directamente en la función,
        se recomienda mantenerlo para mantener consistencia si se utiliza con st.download_button().

    Retorna:
    --------
    BytesIO
        Objeto en memoria que contiene los datos del Excel. Útil para descargar con st.download_button
        o para enviarlo a otros servicios sin necesidad de escribir un archivo temporal.

    Uso:
    ----
    1. Invocar la función pasando un DataFrame como parámetro:
        excel_buffer = excel_download_buttons(mi_dataframe)
    2. Emplear 'excel_buffer' con 'st.download_button' para permitir que el usuario descargue el archivo:
        st.download_button(
            label="Descargar Excel",
            data=excel_buffer.getvalue(),   
            file_name="datos_exportados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    Consideraciones:
    ---------------
    - 'procolombia' se escribe en la primera celda (A1) a modo de encabezado o sello identificativo.
    """

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        # Se crea la hoja "Sheet1" escribiendo un DataFrame vacío
        pd.DataFrame().to_excel(writer, sheet_name="Sheet1", index=False, header=False)
        
        # Se obtiene la hoja activa
        worksheet = writer.sheets["Sheet1"]
        
        # Se escribe "Centro de Inteligencia de Turismo (CIT)"" en A1
        worksheet.write("A1", "Centro de Inteligencia de Turismo (CIT)")

        # Se vuelca el DataFrame a partir de la fila 3
        df.to_excel(writer, index=False, header=True, startrow=3)

        # Al cerrar el bloque 'with', se finaliza la escritura en 'buffer'
        return buffer

def mostrar_resultado_en_streamlit(resultado, fuente, detalle_evento, unidad, df=None):
    """
    Función que muestra en Streamlit un resultado según su tipo:
    
    - Si el resultado es un gráfico de Plotly (go.Figure), se utiliza st.plotly_chart().
      - Se muestra un texto de fuente (st.caption(fuente)).
      - Si se proporciona un DataFrame válido (df) y no está vacío,
        se genera un botón de descarga para exportar los datos en Excel.
    - Si el resultado es una cadena (str), se muestra con st.write().
      - También se muestra un texto de fuente.
    - En caso contrario, se muestra un mensaje indicando que el tipo no está soportado.

    La función también registra el evento de descarga en caso de que el usuario descargue el archivo Excel.

    Parámetros:
    -----------
    resultado : object
        Cualquier objeto que pueda ser devuelto por una función. 
        Esta función intentará identificar si es un go.Figure (gráfico de Plotly) o un str (cadena).
    fuente : str
        Texto que describe la fuente o referencia del resultado mostrado (por ejemplo, 
        "Fuente: Ministerio de Turismo").
    detalle_evento : str
        Texto que describe el detalle del evento que se registra en Snowflake.
    unidad : str
        Se refiere al país de la descarga.
    df : pd.DataFrame, opcional
        DataFrame con datos que se pueden descargar en Excel. 
        Si es None o está vacío, no se mostrará el botón de descarga.

    Retorna:
    --------
    None
        Se limita a imprimir el contenido en la interfaz de Streamlit.

    Uso:
    ----
        mostrar_resultado_en_streamlit(figura_de_plotly, "Fuente: Datos oficiales", df_data)
    """

    # Caso 1: Gráfico de Plotly
    if isinstance(resultado, go.Figure):
        st.plotly_chart(resultado)
        st.caption(f'Fuente: {fuente}')

        # Verificar si el DataFrame no es None y no está vacío
        if isinstance(df, pd.DataFrame) and not df.empty:
            st.download_button(
                label="Descargar Excel",
                data=excel_download_buttons(df),
                file_name="resultado_exportado.xlsx",
                mime="application/vnd.ms-excel",
                use_container_width=True,
                on_click=snowflake_analitica.registrar_evento,
                args=(st.session_state.session, 'Descarga archivo Excel', detalle_evento, unidad),
                key=detalle_evento
            )

    # Caso 2: Cadena de texto
    elif isinstance(resultado, str):
        st.write(resultado)
        st.caption(fuente)

    # Caso 3: Tipo no soportado
    else:
        st.warning(f"Tipo de resultado no reconocido o no soportado: {type(resultado)}")

# Función para obtener los datos
#@st.cache_data(show_spinner=True)
def obtener_datos(_pais_elegido):
    """
    1. Verifica si ya están los datos en 'st.session_state' para el país elegido.
    2. Si no existen o son de un país distinto, ejecuta la operación costosa.
    3. Guarda los resultados en st.session_state.
    4. Devuelve los DataFrames directamente desde session_state.
    """
    
    # Si aún no se ha cargado nada o se cambió de país
    if 'datos_cargados' not in st.session_state or st.session_state['datos_cargados']['pais'] != _pais_elegido:

        with st.spinner("Cargando datos..."):
            # Barra de progreso y realiza la lógica pesada
            progress_bar = st.progress(0)

            # Llamadas a procesamiento_datos

            # Global Data
            df_global_data = procesamiento_datos.datos_global_data(_pais_elegido, st.session_state.session)
            progress_bar.progress(20)

            # OAG
            df_oag = procesamiento_datos.datos_oag(_pais_elegido, st.session_state.session)
            progress_bar.progress(40)

            # Forward Keys
            df_fk = procesamiento_datos.datos_forward_keys(_pais_elegido, st.session_state.session)
            progress_bar.progress(60)

            # Credibanco
            df_credibanco = procesamiento_datos.datos_credibanco(_pais_elegido, st.session_state.session)
            progress_bar.progress(80)

            # IATA GAP
            df_iata = procesamiento_datos.datos_iata_gap(_pais_elegido, st.session_state.session)
            progress_bar.progress(100)

            # Se guardan los datos y el país de referencia en session_state
            st.session_state['datos_cargados'] = {
                'pais': _pais_elegido,
                'df_global_data': df_global_data,
                'df_oag': df_oag,
                'df_fk': df_fk,
                'df_credibanco': df_credibanco,
                'df_iata': df_iata
            }

            # Retorna los mismos DataFrames desde la sesión
            return (
                st.session_state['datos_cargados']['df_global_data'],
                st.session_state['datos_cargados']['df_oag'],
                st.session_state['datos_cargados']['df_fk'],
                st.session_state['datos_cargados']['df_credibanco'],
                st.session_state['datos_cargados']['df_iata']
            )
    
    else:
         # Si ya están cargados, se devuelven directamente
        return (
            st.session_state['datos_cargados']['df_global_data'],
            st.session_state['datos_cargados']['df_oag'],
            st.session_state['datos_cargados']['df_fk'],
            st.session_state['datos_cargados']['df_credibanco'],
            st.session_state['datos_cargados']['df_iata']
        )

# Función para obtener los gráficos de Global Data

def obtener_graficos_global_data(df_global_data, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de GlobalData 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_global_data : dict de DataFrames
        Estructura que contiene los DataFrames de GlobalData (p. ej. 'viajeros_medio', 'gasto_categoria', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_global_data' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:

            # Serie de tiempo de viajeros
            fig_time_series_viajeros = plotly_analitica.plot_single_time_series(df=df_global_data['viajeros_serie_tiempo'], date_col='YEAR', value_col='VIAJEROS', title="Flujo de viajeros hacia el mundo", x_label="Año", y_label="Viajeros", y_units=None, show_labels=True, decimal_places=0)

            # Viajeros por medio de transporte
            # Stacked H
            fig_stacked_h_medio_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['viajeros_medio'], date_col='YEAR', group_col='MEDIO', share_col='PARTICIPACION', decimal_places=1, title='Flujo de viajeros hacia el mundo por medio de transporte', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_medio_viajeros = plotly_analitica.plot_treemap(df = df_global_data['viajeros_medio'], date_col="YEAR", value_col="VIAJEROS", group_col="MEDIO", share_col="PARTICIPACION",decimal_places=1, title="Flujo de viajeros hacia el mundo por medio de transporte", group_label="Medio", value_label="Viajeros", share_label="Participación (%)")

            # Serie de tiempo noches de pernoctación
            fig_time_series_noches_percnotacion = plotly_analitica.plot_single_time_series(df=df_global_data['noches_pernoctacion'], date_col='YEAR', value_col='NOCHES', title="Noches de percnotación promedio", x_label="Año", y_label="Noches promedio", y_units=None, show_labels=True, decimal_places=0)

            # Serie de tiempo gasto
            fig_time_series_gasto = plotly_analitica.plot_single_time_series(df=df_global_data['gasto_serie_tiempo'], date_col='YEAR', value_col='GASTO', title="Gasto promedio del viajero al mundo", x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0)

            # Gasto por categoria
            # Stacked H
            fig_stacked_h_categoria_gasto = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['gasto_categoria'], date_col='YEAR', group_col='CATEGORIA_GASTO', share_col='PARTICIPACION', decimal_places=1, title='Gasto promedio del viajero al mundo por categoria', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_categoria_gasto = plotly_analitica.plot_treemap(df = df_global_data['gasto_categoria'], date_col="YEAR", value_col="GASTO", group_col="CATEGORIA_GASTO", share_col="PARTICIPACION", decimal_places=1, title="Gasto promedio del viajero al mundo por categoria", group_label="Categoria", value_label="Gasto", share_label="Participación (%)")

            # Viajeros por edad
            # Stacked H
            fig_stacked_h_edad_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['rango_edad'], date_col='YEAR', group_col='RANGO_EDAD', share_col='PARTICIPACION', decimal_places=1, title='Rango de edad del viajero promedio', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_edad_viajeros = plotly_analitica.plot_treemap(df = df_global_data['rango_edad'], date_col="YEAR", value_col="VIAJEROS", group_col="RANGO_EDAD", share_col="PARTICIPACION", decimal_places=1, title="Rango de edad del viajero promedio", group_label="Rango de edad", value_label="Viajeros", share_label="Participación (%)")

            # Motivo viaje
            # Stacked H
            fig_stacked_h_motivo_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['motivo_viaje'], date_col='YEAR', group_col='MOTIVO_VIAJE', share_col='PARTICIPACION', decimal_places=1, title='Motivo de viaje del viajero', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_motivo_viajeros = plotly_analitica.plot_treemap(df = df_global_data['motivo_viaje'], date_col="YEAR", value_col="VIAJEROS", group_col="MOTIVO_VIAJE", share_col="PARTICIPACION", decimal_places=1, title="Motivo de viaje del viajero", group_label="Motivo", value_label="Viajeros", share_label="Participación (%)")

            # Forma de viaje
            # Stacked H
            fig_stacked_h_forma_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['forma_viaje'], date_col='YEAR', group_col='FORMA_VIAJE', share_col='PARTICIPACION', decimal_places=1, title='Forma de viaje del viajero', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_forma_viajeros = plotly_analitica.plot_treemap(df = df_global_data['forma_viaje'], date_col="YEAR", value_col="VIAJEROS", group_col="FORMA_VIAJE", share_col="PARTICIPACION", decimal_places=1, title="Forma de viaje del viajero", group_label="Forma", value_label="Viajeros", share_label="Participación (%)") 

            # Destinos
            # Stacked H
            fig_stacked_h_destinos_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['destinos_internacionales_top5'], date_col='YEAR', group_col='PAIS_DESTINO', share_col='PARTICIPACION', decimal_places=1, title='Principales destinos internacionales', y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_destinos_viajeros = plotly_analitica.plot_treemap(df = df_global_data['destinos_internacionales_top5'], date_col="YEAR", value_col="VIAJEROS", group_col="PAIS_DESTINO", share_col="PARTICIPACION", decimal_places=1, title="Principales destinos internacionales", group_label="País", value_label="Viajeros", share_label="Participación (%)")

            # Serie de tiempo de Reuniones, incentivos, congresos y exposiciones (MICE)
            if 'MOTIVO_VIAJE' in df_global_data['flujos_negocios'].columns:
                df_mice = df_global_data['flujos_negocios'][df_global_data['flujos_negocios']['MOTIVO_VIAJE'] == 'Reuniones, incentivos, congresos y exposiciones (MICE)']
            else:
                df_mice = pd.DataFrame()
            fig_time_series_mice = plotly_analitica.plot_single_time_series(df=df_mice, date_col='YEAR', value_col='VIAJEROS', title="Flujos internacionales por motivo de negocios", x_label="Año", y_label="Viajeros", y_units=None, show_labels=True, decimal_places=0)

            # Guardar todo en session_state
            st.session_state['graficos_global_data'] = {
                'fig_time_series_viajeros': fig_time_series_viajeros,
                'fig_stacked_h_medio_viajeros': fig_stacked_h_medio_viajeros,
                'fig_treemap_medio_viajeros': fig_treemap_medio_viajeros,
                'fig_time_series_noches_percnotacion': fig_time_series_noches_percnotacion,
                'fig_time_series_gasto': fig_time_series_gasto,
                'fig_stacked_h_categoria_gasto': fig_stacked_h_categoria_gasto,
                'fig_treemap_categoria_gasto': fig_treemap_categoria_gasto,
                'fig_stacked_h_edad_viajeros': fig_stacked_h_edad_viajeros,
                'fig_treemap_edad_viajeros': fig_treemap_edad_viajeros,
                'fig_stacked_h_motivo_viajeros': fig_stacked_h_motivo_viajeros,
                'fig_treemap_motivo_viajeros': fig_treemap_motivo_viajeros,
                'fig_stacked_h_forma_viajeros': fig_stacked_h_forma_viajeros,
                'fig_treemap_forma_viajeros': fig_treemap_forma_viajeros,
                'fig_stacked_h_destinos_viajeros': fig_stacked_h_destinos_viajeros,
                'fig_treemap_destinos_viajeros': fig_treemap_destinos_viajeros,
                'fig_time_series_mice': fig_time_series_mice
            }

            # Return de los gráficos 
            return (   
                    fig_time_series_viajeros,
                    fig_stacked_h_medio_viajeros,
                    fig_treemap_medio_viajeros,
                    fig_time_series_noches_percnotacion,
                    fig_time_series_gasto,
                    fig_stacked_h_categoria_gasto,
                    fig_treemap_categoria_gasto,
                    fig_stacked_h_edad_viajeros,
                    fig_treemap_edad_viajeros,
                    fig_stacked_h_motivo_viajeros,
                    fig_treemap_motivo_viajeros,
                    fig_stacked_h_forma_viajeros,
                    fig_treemap_forma_viajeros,
                    fig_stacked_h_destinos_viajeros,
                    fig_treemap_destinos_viajeros,
                    fig_time_series_mice                                           
                )
    else:
         # Si ya están cargados, se devuelven directamente
        return(
            st.session_state['graficos_global_data']['fig_time_series_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_medio_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_medio_viajeros'],
            st.session_state['graficos_global_data']['fig_time_series_noches_percnotacion'],
            st.session_state['graficos_global_data']['fig_time_series_gasto'],
            st.session_state['graficos_global_data']['fig_stacked_h_categoria_gasto'],
            st.session_state['graficos_global_data']['fig_treemap_categoria_gasto'],
            st.session_state['graficos_global_data']['fig_stacked_h_edad_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_edad_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_motivo_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_motivo_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_forma_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_forma_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_destinos_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_destinos_viajeros'],
            st.session_state['graficos_global_data']['fig_time_series_mice']

        )

# Función para obtener los gráficos de OAG de conectividad con el mundo
def obtener_graficos_oag_mundo(df_oag, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de OAG Mundo 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_oag : dict de DataFrames
        Estructura que contiene los DataFrames de OAG (p. ej. 'frecuencias', 'sillas gasto_categoria', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_oag_mundo' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
        
            # Single Bar Chart: Conectividad del país con el mundo: Sillas
            fig_single_barchart_conectividad_mundo_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='YEAR', value_col='SILLAS', title='Conectividad del país con el mundo: Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

            # Single Bar Chart Conectividad del país con el mundo: Frencuencias 
            fig_single_barchart_conectividad_mundo_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='YEAR', value_col='FRECUENCIAS', title='Conectividad del país con el mundo: Frencuencias', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)
            
            # StackedH: Conectividad mundo cerrado destinos: Frecuencias
            fig_stacked_h_conectividad_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_cerrado'], date_col='FECHA', group_col='PAIS_ARRIVAL', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo cerrado destinos: Frecuencias', y_label=' Año', legend_title=" ")

            # StackedH: Conectividad mundo corrido destinos: Frecuencias
            fig_stacked_h_conectividad_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_corrido'], date_col='FECHA_CORRIDA', group_col='PAIS_ARRIVAL', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo corrido destinos: Frecuencias', y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_oag_mundo'] = {
                'fig_single_barchart_conectividad_mundo_sillas': fig_single_barchart_conectividad_mundo_sillas,
                'fig_single_barchart_conectividad_mundo_frecuencias': fig_single_barchart_conectividad_mundo_frecuencias,
                'fig_stacked_h_conectividad_frecuencias_destinos_cerrado': fig_stacked_h_conectividad_frecuencias_destinos_cerrado,
                'fig_stacked_h_conectividad_frecuencias_destinos_corrido': fig_stacked_h_conectividad_frecuencias_destinos_corrido
            }

            # Return de los gráficos 
            return (
                    fig_single_barchart_conectividad_mundo_sillas,
                    fig_single_barchart_conectividad_mundo_frecuencias,
                    fig_stacked_h_conectividad_frecuencias_destinos_cerrado,
                    fig_stacked_h_conectividad_frecuencias_destinos_corrido
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_sillas'],
            st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_frecuencias'],
            st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_cerrado'],
            st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_corrido']
         )


# Función para obtener los gráficos de Forward Keys de búsquedas y reservas con el mundo

def obtener_graficos_fk_mundo(df_fk, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Forward Keys Mundo 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_fk : dict de DataFrames
        Estructura que contiene los DataFrames de FK (p. ej. 'reservas', 'búsquedas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_fk_mundo' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
        
            # Reservas activas del país hacia México, Costa Rica, Perú y Chile
            fig_multiple_time_series_reservas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['reservas_serie_tiempo'], date_col='FLIGHT_LEG_ARRIVAL_MONTH_YEAR', value_col='RESERVAS', group_col='PAIS_ARRIVAL', title='Reservas aéreas activas del país hacia México, Costa Rica, Perú y Chile', x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

            # Búsquedas activas del país hacia México, Costa Rica, Perú y Chile
            fig_multiple_time_series_busquedas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['busquedas_serie_tiempo'], date_col='SEARCH_DATE_MONTH_YEAR', value_col='BUSQUEDAS', group_col='PAIS_ARRIVAL', title='Búsquedas aéreas activas del país hacia México, Costa Rica, Perú y Chile', x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

            # Guardar todo en session_state
            st.session_state['graficos_fk_mundo'] = {
                'fig_multiple_time_series_reservas_mundo': fig_multiple_time_series_reservas_mundo,
                'fig_multiple_time_series_busquedas_mundo': fig_multiple_time_series_busquedas_mundo
            }

            # Return de los gráficos
            return (
                    fig_multiple_time_series_reservas_mundo,
                    fig_multiple_time_series_busquedas_mundo
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_fk_mundo']['fig_multiple_time_series_reservas_mundo'],
            st.session_state['graficos_fk_mundo']['fig_multiple_time_series_busquedas_mundo']
         )


# Función para obtener los gráficos de OAG de conetividad con Colombia

def obtener_graficos_oag_colombia(df_oag, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de OAG Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_oag : dict de DataFrames
        Estructura que contiene los DataFrames de OAG (p. ej. 'frecuencias', 'sillas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_oag_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
    
            # Single Bar Chart: Conectividad del país con Colombia: Sillas
            fig_single_barchart_conectividad_colombia_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='YEAR', value_col='SILLAS', title='Conectividad del país con Colombia: Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

            # Single Bar Chart Conectividad del país con Colombia: Frencuencias 
            fig_single_barchart_conectividad_colombia_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='YEAR', value_col='FRECUENCIAS', title='Conectividad del país con Colombia: Frencuencias', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)
            
            # StackedH: Conectividad Colombia cerrado destinos: Frecuencias
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_cerrado'], date_col='FECHA', group_col='MUNICIPIO_DANE', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo cerrado municipios: Frecuencias', y_label=' Año', legend_title=" ")

            # StackedH: Conectividad Colombia corrido destinos: Frecuencias
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_corrido'], date_col='FECHA_CORRIDA', group_col='MUNICIPIO_DANE', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo corrido municipios: Frecuencias', y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_oag_colombia'] = {
                'fig_single_barchart_conectividad_colombia_sillas': fig_single_barchart_conectividad_colombia_sillas,
                'fig_single_barchart_conectividad_colombia_frecuencias': fig_single_barchart_conectividad_colombia_frecuencias,
                'fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado': fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado,
                'fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido': fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido
            }
    
            # Return de los gráficos en un diccionario
            return (
                    fig_single_barchart_conectividad_colombia_sillas,
                    fig_single_barchart_conectividad_colombia_frecuencias,
                    fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado,
                    fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_sillas'],
            st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_frecuencias'],
            st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado'],
            st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido']
         )


# Función para obtener los gráficos de Credibanco

def obtener_graficos_credibanco(df_credibanco, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Credibanco 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_credibanco : dict de DataFrames
        Estructura que contiene los DataFrames de Credibanco (p. ej. 'gasto', 'tarjetas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_credibanco' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
    
            # Gasto promedio
            fig_side_by_side_bar_gasto_promedio =  plotly_analitica.plot_side_by_side_bars(df=df_credibanco['gasto_promedio'], date_col='YEAR', var1_col='GASTO_PROMEDIO_TARJETA', var2_col='GASTO_PROMEDIO_TRANSACCION', title='Gasto promedio', x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0, legend_title=' ', legend_labels={'GASTO_PROMEDIO_TARJETA' : 'Gasto promedio por tarjeta', 'GASTO_PROMEDIO_TRANSACCION' : 'Gasto promedio por transacción'})

            # Gasto por categoria
            fig_stacked_h_gasto_categoria_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_categoria'], date_col='YEAR', group_col='CLASIFICACION_CATEGORIA_FORMATADA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por categoria', y_label='Año', legend_title=" ")

            # Gasto por categoria
            fig_treemap_gasto_categoria_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_categoria'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CLASIFICACION_CATEGORIA_FORMATADA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por categoria", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Gasto por productos directo
            fig_stacked_h_gasto_categoria_directo_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_directo'], date_col='YEAR', group_col='CATEGORIA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por producto directo', y_label='Año', legend_title=" ")

            # Gasto por producto directo
            fig_treemap_gasto_categoria_directo_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_directo'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CATEGORIA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por producto directo", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Gasto por productos indirecto
            fig_stacked_h_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_indirecto'], date_col='YEAR', group_col='CATEGORIA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por producto indirecto', y_label='Año', legend_title=" ")

            # Gasto por producto indirecto
            fig_treemap_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_indirecto'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CATEGORIA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por producto indirecto", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Guardar todo en session_state
            st.session_state['graficos_credibanco'] = {
                'fig_side_by_side_bar_gasto_promedio': fig_side_by_side_bar_gasto_promedio,
                'fig_stacked_h_gasto_categoria_credibanco': fig_stacked_h_gasto_categoria_credibanco,
                'fig_treemap_gasto_categoria_credibanco': fig_treemap_gasto_categoria_credibanco,
                'fig_stacked_h_gasto_categoria_directo_credibanco': fig_stacked_h_gasto_categoria_directo_credibanco,
                'fig_treemap_gasto_categoria_directo_credibanco': fig_treemap_gasto_categoria_directo_credibanco,
                'fig_stacked_h_gasto_categoria_indirecto_credibanco': fig_stacked_h_gasto_categoria_indirecto_credibanco,
                'fig_treemap_gasto_categoria_indirecto_credibanco': fig_treemap_gasto_categoria_indirecto_credibanco
            }

            # Return de los gráficos en un diccionario
            return (
                    fig_side_by_side_bar_gasto_promedio,
                    fig_stacked_h_gasto_categoria_credibanco,
                    fig_treemap_gasto_categoria_credibanco,
                    fig_stacked_h_gasto_categoria_directo_credibanco,
                    fig_treemap_gasto_categoria_directo_credibanco,
                    fig_stacked_h_gasto_categoria_indirecto_credibanco,
                    fig_treemap_gasto_categoria_indirecto_credibanco
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_credibanco']['fig_side_by_side_bar_gasto_promedio'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_credibanco'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_directo_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_directo_credibanco'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_indirecto_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_indirecto_credibanco']
         )


# Función para obtener los gráficos de Forward Keys de búsquedas y reservas con Colombia

def obtener_graficos_fk_colombia(df_fk, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Forward Keys Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_fk : dict de DataFrames
        Estructura que contiene los DataFrames de FK (p. ej. 'reservas', 'búsquedas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_fk_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
         
            # Reservas aéreas activas del país hacia Colombia
            fig_single_time_series_reservas_colombia = plotly_analitica.plot_single_time_series(df=df_fk['reservas_serie_tiempo_colombia'], date_col='FLIGHT_LEG_ARRIVAL_MONTH_YEAR', value_col='RESERVAS', title="Reservas aéreas activas del país hacia Colombia", x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, mensual=True)

            # Búsquedas activas del país hacia Colombia 
            fig_single_time_series_busquedas_colombia = plotly_analitica.plot_single_time_series(df=df_fk['busquedas_serie_tiempo_colombia'], date_col='SEARCH_DATE_MONTH_YEAR', value_col='BUSQUEDAS', title="Búsquedas aéreas activas del país hacia hacia Colombia", x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, mensual=True)

             # Guardar todo en session_state
            st.session_state['graficos_fk_colombia'] = {
                'fig_single_time_series_reservas_colombia': fig_single_time_series_reservas_colombia,
                'fig_single_time_series_busquedas_colombia': fig_single_time_series_busquedas_colombia
            }

            # Return de los gráficos en un diccionario
            return (
                    fig_single_time_series_reservas_colombia,
                    fig_single_time_series_busquedas_colombia
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_fk_colombia']['fig_single_time_series_reservas_colombia'],
            st.session_state['graficos_fk_colombia']['fig_single_time_series_busquedas_colombia']
         )


# Funciones para obtener los gráficos de IATA GAP de agencias que promocionan Colombia

def obtener_graficos_iata_colombia(df_iata, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de IATA GAP Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_iata : dict de DataFrames
        Estructura que contiene los DataFrames de IATA (p. ej. 'agencias', 'ciudades', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_iata_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:

            # Indicadores de agencias de ese mercado que venden Colombia como destino mostrar por q
            fig_single_time_series_agencias_colombia = plotly_analitica.plot_single_time_series(df=df_iata['agencias_serie_tiempo'], date_col='YEAR', value_col='AGENCIAS', title="Agencias que venden Colombia como destino", x_label="Año", y_label="Agencias", y_units='Número', show_labels=True, decimal_places=0)

            # Agencias que venden Colombia como destino por ciudad de la agencia 15
            fig_stacked_h_agencias_ciudades = plotly_analitica.plot_stacked_bar_chart_h(df=df_iata['agencias_ciudades'], date_col='YEAR', group_col='TRAVEL_AGENCY_CITY', share_col='PARTICIPACION', decimal_places=1, title='Agencias que venden Colombia como destino por ciudad de la agencia', y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_iata_colombia'] = {
                'fig_single_time_series_agencias_colombia': fig_single_time_series_agencias_colombia,
                'fig_stacked_h_agencias_ciudades': fig_stacked_h_agencias_ciudades
            }

            # Return de los gráficos en un diccionario
            return (
                    fig_single_time_series_agencias_colombia, 
                    fig_stacked_h_agencias_ciudades
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_iata_colombia']['fig_single_time_series_agencias_colombia'],
            st.session_state['graficos_iata_colombia']['fig_stacked_h_agencias_ciudades']
         )


# Función para Limpiar caches de dfs y gráficos al momento de elegir otro país
def on_selectbox_change():
    
    # Limpia la caché de datos
    st.cache_data.clear()

    # Limpia la clave 'datos_cargados' (si existe) en session_state
    if 'datos_cargados' in st.session_state:
        del st.session_state['datos_cargados']
    # Limpia la clave 'graficos_global_data' (si existe) en session_state
    if 'graficos_global_data' in st.session_state:
        del st.session_state['graficos_global_data']
    # Limpia la clave 'graficos_oag_mundo' (si existe) en session_state
    if 'graficos_oag_mundo' in st.session_state:
        del st.session_state['graficos_oag_mundo']
    # Limpia la clave 'graficos_fk_mundo' (si existe) en session_state
    if 'graficos_fk_mundo' in st.session_state:
        del st.session_state['graficos_fk_mundo']
    # Limpia la clave 'graficos_oag_colombia' (si existe) en session_state
    if 'graficos_oag_colombia' in st.session_state:
        del st.session_state['graficos_oag_colombia']
    # Limpia la clave 'graficos_credibanco' (si existe) en session_state
    if 'graficos_credibanco' in st.session_state:
        del st.session_state['graficos_credibanco']
    # Limpia la clave 'graficos_fk_colombia' (si existe) en session_state
    if 'graficos_fk_colombia' in st.session_state:
        del st.session_state['graficos_fk_colombia']
    # Limpia la clave 'graficos_iata_colombia' (si existe) en session_state
    if 'graficos_iata_colombia' in st.session_state:
        del st.session_state['graficos_iata_colombia']
