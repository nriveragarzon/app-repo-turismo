# Librerias
import streamlit as st

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica

# Locale
import locale
locale.setlocale(locale.LC_ALL, "es_ES.UTF-8")

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="Inicio", 
                   page_icon = ':airplane_arriving:', 
                   layout="wide",  
                   initial_sidebar_state="expanded")

# Inclusión de la hoja de estilos de Bootstrap para mejorar la apariencia.
st.markdown("""
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
""", unsafe_allow_html=True)

# Incializar el estado en la página inicial 
if "page" not in st.query_params:
    st.query_params.page = '1'

# Marcador para volver al inicio
st.markdown("<a id='top'></a>", unsafe_allow_html=True)

# Incluir la barra de navegación
streamlit_analitica.navbar()

# Redirección condicional según el valor del parámetro 'page' en la URL.
if st.query_params.page == '2':
    st.switch_page("pages/centro_inteligencia.py")
if st.query_params.page == '3':
    st.switch_page("pages/fuentes.py")

# Estructura
leftsidebar, body, rightsidebar = st.columns([0.01,0.98, 0.01], gap='small',vertical_alignment='top')

# Aprovechar el máximo espacio horizontal de la pantalla
with body:
    
    # Mostrar contenido de la página inicial
    streamlit_analitica.home_page()

# Agregar footer
streamlit_analitica.footer()