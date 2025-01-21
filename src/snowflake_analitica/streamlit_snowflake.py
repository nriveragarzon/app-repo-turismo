# Librerías

import streamlit as st
import time
from datetime import datetime, timedelta

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
    # Crear objeto de conexión
    conn = sesion_activa.connection
    try:
        # Crear consulta para el insert
        query_insert = f"""
        INSERT INTO REPOSITORIO_TURISMO.SEGUIMIENTO.SEGUIMIENTO_EVENTOS (TIPO_EVENTO, DETALLE_EVENTO, UNIDAD, FECHA_HORA) 
        VALUES ('{tipo_evento}', '{detalle_evento}', '{unidad}', CONVERT_TIMEZONE('America/Los_Angeles', 'America/Bogota', CURRENT_TIMESTAMP));
        """
        # Crear un cursor para ejecutar la consulta
        cur = conn.cursor()
        try:
            # Ejecutar la consulta SQL con los valores
            cur.execute(query_insert)
        finally:
            # Cerrar el cursor
            cur.close()
    # Error
    except Exception as e:
        st.write(f"Error al registrar evento: {e}")