# Librerías
import streamlit as st
import time
from datetime import datetime, timedelta
import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Inicializar variables de sesión si no existen
if 'session' not in st.session_state:
    st.session_state.session = None  # Sesión inicializada como None
if 'last_activity_time' not in st.session_state:
    st.session_state.last_activity_time = datetime.now()  # Última actividad es el momento actual

# Definir tiempo de espera de sesión (15 minutos)
SESSION_TIMEOUT = timedelta(minutes=15)

# Función para crear una nueva sesión con Snowflake
def create_session(retries=5, wait=10):
    """
    Crea y configura una sesión de Snowflake con reintentos en caso de fallos.

    Parámetros
        - **retries**: Número máximo de intentos para establecer la conexión (por defecto 5).
        - **wait**: Tiempo de espera en segundos entre reintentos (por defecto 10).

    Retorna:
        - **Session**: Objeto de sesión de Snowflake si la conexión es exitosa.
        - **None**: Si todos los intentos fallan.

    Excepciones:
        - **ValueError**: Si alguna variable de entorno requerida está vacía o no definida.

    Acciones:
        - Intenta conectarse a Snowflake utilizando las credenciales especificadas en las variables de entorno.
        - Cambia a una segunda clave privada si se detecta un error de token JWT.

    Ejemplo de uso:
        session = create_session(retries=3, wait=5)
    """
    load_dotenv()

    private_key_path = os.getenv('SF_PRIVATE_KEY_PATH_1')
    private_key_passphrase = os.getenv('SF_PRIVATE_KEY_PASSPHRASE_1')

    if not private_key_path:
        raise ValueError("La ruta de la llave privada del usuario de servicio no está definida o está vacía.")

    with open(private_key_path, "rb") as key_file:
        private_key = key_file.read()

    session_config = {
        "account": os.getenv('SF_ACCOUNT'),
        "user": os.getenv('SF_USER'),
        "private_key": private_key,
        "private_key_passphrase": private_key_passphrase,
        "database": os.getenv('SF_DATABASE'),
        "schema": os.getenv('SF_SCHEMA'),
        "warehouse": os.getenv('SF_WAREHOUSE'),
        "role": os.getenv('SF_ROLE'),
        "query_tag": "SEGMENTATION_APP"
    }

    if any(value is None or value == '' for value in session_config.values()):
        raise ValueError("Una o más variables de entorno están indefinidas o vacías.")

    success = False
    for attempt in range(retries):
        try:
            session = Session.builder.configs(session_config).create()
            success = True
            break
        
        except Exception as e:
            print(f"Intento {attempt + 1} de {retries} fallido: \n{str(e)}")
            
            if 'JWT token is invalid' in str(e):
                print("Error de token JWT, intentando con SF_PRIVATE_KEY_PATH_2 y SF_PRIVATE_KEY_PASSPHRASE_2")
                private_key_path_2 = os.getenv('SF_PRIVATE_KEY_PATH_2')
                private_key_passphrase_2 = os.getenv('SF_PRIVATE_KEY_PASSPHRASE_2')

                if not private_key_path_2 or not private_key_passphrase_2:
                    print("Las variables de entorno para la segunda clave privada no están definidas correctamente.")
                    return None

                with open(private_key_path_2, "rb") as key_file:
                    private_key_2 = key_file.read()

                session_config["private_key"] = private_key_2
                session_config["private_key_passphrase"] = private_key_passphrase_2
                
            if attempt < retries - 1:
                time.sleep(wait)

    if success:
        return session
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

# Función para insertar datos en la tabla de seguimiento
def registrar_evento(sesion_activa, tipo_evento, detalle_evento, unidad):
    """
    Registra un evento en la base de datos Snowflake.

    Args:
    - sesion_activa: Sesión activa de conexión a la base de datos.
    - tipo_evento (str): Tipo de evento ('selección' o 'descarga').
    - detalle_evento (str): Detalle de evento ('selección continente', 'selección país', etc)
    - unidad (str): Unidad específica del evento (e.g., 'América', 'Colombia').
    """
    try:
        # Crear consulta para el insert
        query_insert = f"""
        INSERT INTO REPOSITORIO_TURISMO.SEGUIMIENTO.SEGUIMIENTO_EVENTOS (TIPO_EVENTO, DETALLE_EVENTO, UNIDAD, FECHA_HORA) 
        VALUES ('{tipo_evento}', '{detalle_evento}', '{unidad}', CONVERT_TIMEZONE('America/Los_Angeles', 'America/Bogota', CURRENT_TIMESTAMP));
        """
        # Ejecutar la consulta SQL con los valores
        sesion_activa.sql(query_insert).collect()      
    # Error
    except Exception as e:
        st.write(f"Error al registrar evento: {e}")