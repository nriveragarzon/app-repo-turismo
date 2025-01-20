# Librerias
import warnings
import streamlit as st
import time
from datetime import datetime, timedelta

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica
import src.snowflake_analitica as snowflake_analitica
import src.plotly_analitica as plotly_analitica
import src.procesamiento_datos as procesamiento_datos

# Ignorar warnings
warnings.filterwarnings("ignore", message="Bad owner or permissions on")

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="CIT", page_icon = ':beach_with_umbrella:', layout="wide",  initial_sidebar_state="collapsed")

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
    st.query_params.page = '2'

# Incluir la barra de navegación
streamlit_analitica.navbar()

# Redirección condicional según el valor del parámetro 'page' en la URL.
if st.query_params.page == '1':
    st.switch_page("app.py") 
if st.query_params.page == '3':
    st.switch_page("pages/fuentes.py")

# Función para cargar contraseñas
def cargar_contraseñas(nombre_archivo):
    return st.secrets

# Cargar constraseñas
cargar_contraseñas(".streamlit/secrets.toml")

# Inicializar variables de sesión si no existen
if 'session' not in st.session_state:
    st.session_state.session = None  # Sesión inicializada como None
if 'last_activity_time' not in st.session_state:
    st.session_state.last_activity_time = datetime.now()  # Última actividad es el momento actual

# Definir tiempo de espera de sesión (15 minutos)
SESSION_TIMEOUT = timedelta(minutes=15)

###########
# CONTENIDO
###########

st.title("CIT")

# Actualizar flujo de Snowflake
snowflake_analitica.flujo_snowflake()

# Actualizar tiempo de última actividad
snowflake_analitica.update_last_activity()

# Selector de regiones
region_elegida = st.selectbox(label='Seleccione un continente:',
             options=snowflake_analitica.obtener_regiones_disponibles(st.session_state.session),
             placeholder='Elija una opción',
             index=None,
             help = 'Seleccione un único continente para refinar su búsqueda de países disponibles.', 
             key = 'widget_continentes'
             )

# Selector de países
if region_elegida:
    # El selector de países se habilita después de la elección de un continente        
    pais_elegido = st.selectbox(label='Seleccione un país:',
                options=snowflake_analitica.obtener_paises_por_region(region_elegida, st.session_state.session),
                placeholder='Elija una opción',
                index=None,
                help = 'Seleccione un único país para obtener información detallada de métricas de turismo.', 
                key = 'widget_paises'
                )
    
    # Habilitar contenido si se selecciona un país
    if pais_elegido:

        # Obtener geo datos
        iso_code = snowflake_analitica.obtener_iso_code(pais_elegido, st.session_state.session)[0]
              
        # Nombre del país elegido e imagen centrados en el mismo renglón
        col1, col2, col3, col4 = st.columns([0.3, 0.4, 0.3, 0.3], gap='small', vertical_alignment='center')
        with col1:
            st.write("")
        with col2:
            st.title(f'{pais_elegido}')
        with col3:
            st.image(f"https://flagcdn.com/h120/{iso_code.lower()}.png", width=100)
        with col4:
            st.write("")

        # Obtener datos del país elegido por el usuario

        # Global Data
        df_global_data =  procesamiento_datos.datos_global_data(pais_elegido, st.session_state.session)

        # OAG
        df_oag =  procesamiento_datos.datos_oag(pais_elegido, st.session_state.session)

        # Forward Keys
        df_fk =  procesamiento_datos.datos_forward_keys(pais_elegido, st.session_state.session)

        # Credibanco
        df_credibanco =  procesamiento_datos.datos_credibanco(pais_elegido, st.session_state.session)

        # IATA
        df_iata =  procesamiento_datos.datos_iata_gap(pais_elegido, st.session_state.session)


        ####################
        # TABLA DE CONTENIDO
        ####################

        #############################
        # Contenido de bases de datos
        #############################

        # Resultados generales del país
        with st.expander(f"**Resultados generales del país**", expanded=False):

            fig1 = plotly_analitica.plot_single_time_series(df=df_global_data['viajeros_serie_tiempo'], date_col='YEAR', value_col='VIAJEROS',
                        title="Flujo de viajeros hacia el mundo", 
                        x_label="Año", y_label="Viajeros", y_units=None, 
                        show_labels=True, decimal_places=0)
            
            streamlit_analitica.mostrar_resultado_en_streamlit(resultado = fig1, fuente= 'De los deseos', df = df_global_data['viajeros_serie_tiempo'])
        
        # Indicadores de turismo del mercado hacia el mundo
        with st.expander(f"**Indicadores de turismo del mercado hacia el mundo**", expanded=False):
            st.write('Indicadores de turismo del mercado hacia el mundo')

        # Indicadores de turismo del mercado hacia Colombia
        with st.expander(f"**Indicadores de turismo del mercado hacia Colombia**", expanded=False):
            st.write('Indicadores de turismo del mercado hacia Colombia')
        
        # Salida de colombianos hacia el mercado
        with st.expander(f"**Salida de colombianos hacia el mercado**", expanded=False):
            st.write('en construcción...')
        



