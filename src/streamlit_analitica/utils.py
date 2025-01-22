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

def set_expander_state(expander_name):
    """
    Función que gestiona el estado de varios expanders en una aplicación de Streamlit,
    garantizando que solo uno de ellos se mantenga abierto a la vez.

    Descripción:
    ------------
    - La función recorre todas las claves (keys) de st.session_state que comiencen
      con "expander_" y las pone en estado False, equivalentes a "cerradas".
    - Luego, activa (True) el expander cuyo nombre coincida con expander_name,
      manteniéndolo abierto.

    Parámetros:
    -----------
    expander_name : str
        Nombre del expander que se desea dejar abierto. Por ejemplo, si
        se desea que el expander con la clave "expander_informacion" se
        muestre abierto, se invoca la función con "informacion" como argumento.

    Retorna:
    --------
    None
        Esta función no retorna ningún valor; su único fin es manipular
        el estado global (st.session_state) de Streamlit.

    Uso:
    ----
    - Llamar a set_expander_state("nombre_del_expander") antes de crear
      o mostrar los expanders en la interfaz de Streamlit.
    - Se recomienda utilizar esta función dentro de callbacks o en eventos
      de botones para asegurar la coherencia de la lógica de despliegue.

    Ejemplo:
    --------
        if st.button("Abrir Expander 1"):
            set_expander_state("expander_1")

        if st.button("Abrir Expander 2"):
            set_expander_state("expander_2")

    Esto asegurará que solo uno de los expanders ("expander_1" o "expander_2")
    se muestre abierto y que el otro se cierre automáticamente.
    """
    for key in st.session_state:
        if key.startswith("expander_"):
            st.session_state[key] = False
    st.session_state[f"expander_{expander_name}"] = True


@st.cache_data(show_spinner=False)
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
    - La anotación @st.cache_data optimiza el rendimiento al cachear la generación del Excel, 
      evitando la recreación del mismo si los datos no han cambiado.
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

# Función para obtener los datos con cache
@st.cache_data(show_spinner=False)
def obtener_datos_con_cache(_pais_elegido):
    """
    Función que obtiene y retorna varios DataFrames basados en 'pais_elegido',
    mostrando un spinner y una barra de progreso mientras se cargan los datos.

    Parámetros:
    -----------
    pais_elegido : str
        Identificador del país para filtrar o procesar los datos en cada función.

    Retorna:
    --------
    df_global_data, df_oag, df_fk, df_credibanco, df_iata : pd.DataFrame
        DataFrames obtenidos de las funciones de procesamiento_datos, 
        retornados de forma individual (como valores separados).

    Notas:
    ------
    - Se usa 'show_spinner=False' en @st.cache_data para poder controlar 
      manualmente el spinner con 'st.spinner("...")'.
    - Cada DataFrame se calcula en pasos separados, incrementando la barra 
      de progreso para informar al usuario sobre la fase de carga.
    """

    progress_bar = st.progress(0)
    
    with st.spinner("Cargando datos..."):
        # Paso 1: df_global_data
        df_global_data = procesamiento_datos.datos_global_data(_pais_elegido, st.session_state.session)
        progress_bar.progress(20)

        # Paso 2: df_oag
        df_oag = procesamiento_datos.datos_oag(_pais_elegido, st.session_state.session)
        progress_bar.progress(40)

        # Paso 3: df_fk
        df_fk = procesamiento_datos.datos_forward_keys(_pais_elegido, st.session_state.session)
        progress_bar.progress(60)

        # Paso 4: df_credibanco
        df_credibanco = procesamiento_datos.datos_credibanco(_pais_elegido, st.session_state.session)
        progress_bar.progress(80)

        # Paso 5: df_iata
        df_iata = procesamiento_datos.datos_iata_gap(_pais_elegido, st.session_state.session)
        progress_bar.progress(100)

    return df_global_data, df_oag, df_fk, df_credibanco, df_iata

