# Librerias
import warnings
import streamlit as st
import time
from datetime import datetime, timedelta

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica
import src.snowflake_analitica as snowflake_analitica

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="Fuentes", 
                   page_icon = ':book:', 
                   layout="wide",  
                   initial_sidebar_state="expanded")

# Inclusión de la hoja de estilos de Bootstrap para mejorar la apariencia.
st.markdown("""
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
""", unsafe_allow_html=True)

# Incializar el estado en la página inicial 
if "page" not in st.query_params:
    st.query_params.page = '3'

# Incluir la barra de navegación
streamlit_analitica.navbar()

# Redirección condicional según el valor del parámetro 'page' en la URL.
if st.query_params.page == '1':
    st.switch_page("app.py") 
if st.query_params.page == '2':
    st.switch_page("pages/centro_inteligencia.py")

###########
# CONTENIDO
###########

# Estructura
leftsidebar, body, rightsidebar = st.columns([0.01,0.98, 0.01], gap='small',vertical_alignment='top')

# Aprovechar el máximo espacio horizontal de la pantalla
with body:

    st.title("Fuentes")

    # Contenido de fuentes
    st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Maven+Pro:wght@400&display=swap'); 
    
    /* Justificación del texto */
    .justify-text {
        text-align: justify;
    }
    
    .justify-text p {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    
    .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }    
    </style>

    <div class="justify-text">
        <div><h4>1. Flujo de viajeros hacia el mundo</h4></div>
        <ul class="indent">              
            <li><strong>GlobalData:</strong> es una plataforma líder en inteligencia de mercado que proporciona datos y análisis detallados sobre la industria turística. La base de datos de GlobalData está diseñada para ofrecer información precisa y en tiempo real enfocada el flujo internacional y el gasto a nivel de país.</li>
        </ul>
    </div>

    <div class="justify-text">
        <div><h4>2. Conectividad aérea</h4></div>
        <ul class="indent">              
            <li><strong>OAG (Official Aviation Guide) Schedule:</strong> es una fuente global de información sobre horarios de vuelos y detalles de las aerolíneas. Está diseñada para proporcionar a las aerolíneas, agencias de viajes, y otros actores de la industria de la aviación información detallada y precisa sobre los vuelos programados a nivel mundial. Permite obtener detalles sobre rutas tanto dentro de un país como internacionales, proporcionando una visión completa de la conectividad aérea.</li>
        </ul>
    </div>

    <div class="justify-text">
        <div><h4>3. Reservas y Búsquedas aéreas</h4></div>
        <ul class="indent">              
            <li><strong>FordwardKeys:</strong> es una plataforma global de análisis de datos de viajes y turismo que proporciona información detallada sobre las reservas aéreas activas y las búsquedas de vuelos internacionales, permitiendo entender el comportamiento y las tendencias de los viajeros, así como sobre la demanda de vuelos y destinos turísticos en todo el mundo. ForwardKeys se utiliza principalmente para realizar análisis predictivos, mejorar la toma de decisiones y optimizar las estrategias de marketing.</li>
        </ul>
    </div>

    <div class="justify-text">
        <div><h4>4. Flujo de viajeros hacia Colombia</h4></div>
        <ul class="indent">              
            <li><strong>Migración Colombia:</strong> es la entidad del gobierno de Colombia encargada de regular, controlar y facilitar los flujos migratorios hacía, desde y dentro del país. Su misión principal es velar por el cumplimiento de las políticas migratorias nacionales e internacionales, además de garantizar la seguridad de los procesos migratorios y promover la integración de los migrantes. La información que proporciona esta entidad permite entender los viajeros extranjeros que llegan a Colombia por país de origen y la ciudad a que viajarán.</li>
        </ul>
    </div>

    <div class="justify-text">
        <div><h4>5. Gasto de los viajeros con tarjeta de crédito en Colombia</h4></div>
        <ul class="indent">              
            <li><strong>CredibanCo:</strong> es una empresa colombiana especializada en la prestación de soluciones de pagos electrónicos, especialmente en el ámbito de la intermediación de pagos y procesamiento de transacciones para comercios y entidades financieras. La plataforma permite obtener información sobre el gasto realizado con tarjeta de crédito registrada en el exterior, identificando lo países que más gasto en actividades turísticas en Colombia.</li>
        </ul>
    </div>

    <div class="justify-text">
        <div><h4>6. Agencias que venden Colombia como destino</h4></div>
        <ul class="indent">              
            <li><strong>IATA-GAP (International Air Transport Association - Global Analytics Platform):</strong> es una plataforma global de análisis de datos proporcionada por la IATA (International Air Transport Association), la principal asociación comercial internacional de aerolíneas. El sistema IATA-GAP proporciona análisis avanzados e informes estratégicos basados en grandes volúmenes de datos recopilados de las operaciones de la aviación mundial. Permite identificar las agencias de viajes que venden Colombia como destino turístico.</li>
        </ul>
    </div>
    """, 
    unsafe_allow_html=True
    )

# Agregar footer
streamlit_analitica.footer()