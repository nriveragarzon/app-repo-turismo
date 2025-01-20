# Librerias
import warnings
import streamlit as st
import time
from datetime import datetime, timedelta

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica
import src.snowflake_analitica as snowflake_analitica

# Ignorar warnings
warnings.filterwarnings("ignore", message="Bad owner or permissions on")

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="Fuentes", page_icon = ':ledger:', layout="wide",  initial_sidebar_state="collapsed")

# Leer archivo de estilos
css = streamlit_analitica.load_css("static/style.css")
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

# Inclusión de la hoja de estilos de Bootstrap para mejorar la apariencia.
st.markdown("""
    <style>
        [data-testid="stColumn"] {
            padding: 20px 20px 20px 20px;
        }     
    </style> 
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

st.title("Fuentes")
st.write("Detalle de las fuentes empleadas en el CIT.")