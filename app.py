#######################
# 0. Importar librerias
#######################
import warnings
import streamlit as st
import time
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", message="Bad owner or permissions on")

# Configuración página web
st.set_page_config(page_title="Centro de Inteligencia de Turismo", page_icon = ':world_map', layout="wide",  initial_sidebar_state="expanded")

# Imágenes
# Footer para todas las páginas
footer_img = 'static/images/banner_footer.png'

# Logos Procolombia
logo_img = 'static/images/logos - Comercio - ProColombia_COLOR.png'

# Secrets
def cargar_contraseñas(nombre_archivo):
    """
    Función para cargar contraseñas
    """
    return st.secrets

# Cargar contraseñas
cargar_contraseñas(".streamlit/secrets.toml")

# Inicializar variables de sesión si no existen
if 'session' not in st.session_state:
    st.session_state.session = None  # Sesión inicializada como None
if 'last_activity_time' not in st.session_state:
    st.session_state.last_activity_time = datetime.now()  # Última actividad es el momento actual

# Definir tiempo de espera de sesión (15 minutos)
SESSION_TIMEOUT = timedelta(minutes=15)

# Función para crear una nueva sesión con Snowflake
def create_session(retries=3, wait=5):
    """
    Crea una nueva sesión de Snowflake, con lógica de reintentos en caso de fallo.
    
    Args:
        retries (int): Número máximo de intentos de conexión. Por defecto, 3.
        wait (int): Tiempo en segundos entre intentos. Por defecto, 5.

    Returns:
        session: Objeto de sesión activa de Snowflake o None si todos los intentos fallan.
    """
    success = False
    for attempt in range(retries):
        try:
            # Conectar a Snowflake
            connection = st.connection("snowflake") # Establecer conexión con Snowflake
            sesion_activa = connection.session()  # Obtener sesión acti
            success = True
            break
        except Exception as e:
            print(f"Intento {attempt + 1} de {retries} fallido: \n{str(e)}")
            if attempt < retries - 1:
                time.sleep(wait) # Esperar antes del siguiente intento
    if success:
        return sesion_activa # Return el objeto de sesión de Snowflake
    else:
        print("Todos los intentos de conexión fallaron.")
        return None

# Función para verificar si la sesión ha expirado
def check_session():
    """
    Verifica si la sesión actual ha expirado. Si es así, cierra la sesión.
    """
    if (datetime.now() - st.session_state.last_activity_time) > SESSION_TIMEOUT:
        if st.session_state.session:
            st.session_state.session.close()  # Cerrar la sesión expirada
        st.session_state.session = None

# Función para obtener la sesión activa
def get_session():
    """
    Obtiene la sesión activa de Snowflake. Si no existe o ha expirado, crea una nueva.
    """
    check_session()  # Verificar si la sesión ha expirado
    if st.session_state.session is None:
        st.session_state.session = create_session()  # Crear nueva sesión si no existe

# Función para actualizar el tiempo de última actividad
def update_last_activity():
    """
    Actualiza el tiempo de última actividad de la sesión al momento actual.
    """
    st.session_state.last_activity_time = datetime.now()  # Registrar última actividad

def flujo_snowflake():
    """
    Gestiona el flujo para interactuar con Snowflake, asegurando que:
    1. Se registre la última actividad del usuario.
    2. Se obtenga una sesión activa, ya sea verificando la existente o creando una nueva.

    Este flujo utiliza funciones auxiliares para manejar la sesión y garantizar
    que el tiempo de espera (timeout) y la lógica de reconexión se respeten.
    """
    # Paso 1: Actualizar el tiempo de última actividad
    # Esto asegura que el registro de actividad esté actualizado para prevenir 
    # el cierre prematuro de la sesión por inactividad.
    update_last_activity()

    # Paso 2: Obtener la sesión activa
    # Verifica si hay una sesión activa. Si ha expirado o no existe, 
    # intenta crear una nueva sesión de Snowflake.
    get_session()

# Limpiar cache
def limpiar_cache():
    """
    Limpia el cache de datos de Streamlit.
    """
    st.cache_data.clear()  # Limpia el cache de datos de Streamlit

######################################
# 1. Definir el flujo de la aplicación
######################################

def main():
    
    # Flujo de Snowflake:
    flujo_snowflake()

    ## Menú de navegación
    ### Logo ProColombia
    with st.sidebar:
        st.image(logo_img, caption=None, use_column_width="always")
        st.markdown('#')     
    ## Páginas
    ### Opciones con iconos
    options = {
        "Portada": "Portada",
        "Centro de Conocimiento": "Centro de Conocimiento",
        "Fuentes": "Fuentes"
    }
    #### Configuración del sidebar
    page = st.sidebar.radio("Elija una página", list(options.keys()))
    selected_option = options.get(str(page))  # Usar get para manejar None de manera segura
    if selected_option:
        if selected_option == "Portada":
            page_portada()
        elif selected_option == "Centro de Conocimiento":
            centro_de_conocimiento()
        elif selected_option == "Fuentes":
            page_fuentes()
    ### Logo MINCIT
    with st.sidebar:
        ### Elaborado por la Coordinación de Analítica
        st.markdown('#')
        st.subheader("Elaborado por:") 
        st.write("Coordinación de Turismo, Gerencia de Inteligencia Comercial, ProColombia.") 
        st.write("Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.") 
        st.markdown('#')
        
        
###########################
# Personalización de estilo
###########################

# Función para cargar el CSS desde un archivo
def load_css(file_name):
    with open(file_name) as f:
        return f.read()

# Cargar y aplicar el CSS personalizado
css = load_css("static/style.css")
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

##############
# Contenido
##############

#########
# Portada
#########

def page_portada():

    # Actualizar tiempo de última actividad
    update_last_activity()

    # Contenido de la portada
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p, .justify-text div {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }
    </style>
    </div>
    <h2>Soporte</h2>
    <div class="justify-text">
        <p>Si tiene alguna pregunta o necesita asistencia, no dude en ponerse en contacto con el equipo de la Coordinación de Turismo o la Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.</p>
    </div>               
    """, unsafe_allow_html=True)
    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")

########################
# Centro de Conocimiento
########################

def centro_de_conocimiento():

    # Flujo de Snowflake:
    flujo_snowflake()

    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")


#########
# Fuentes
#########

def page_fuentes():
        
    # Actualizar tiempo de última actividad
    update_last_activity()

    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")



########################################
# Mostrar contenido de todas las páginas
########################################
if __name__ == "__main__":
    main()