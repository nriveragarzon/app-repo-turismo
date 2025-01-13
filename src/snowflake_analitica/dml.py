# DML significa Data Manipulation Language o Lenguaje de Manipulación de Datos, en español. 
# Este lenguaje permite realizar diferentes acciones a los datos que se encuentran en una base de datos.
# Permite recuperar, almacenar, modificar, eliminar, insertar y actualizar datos de una base de datos.
# Elementos del DML (Data Manipulation Language)
    # SELECT: Utilizado para consultar registros de la base de datos que satisfagan un criterio determinado.
    # INSERT: Utilizado para cargar de datos en la base de datos en una única operación.
    # UPDATE: Utilizado para modificar los valores de los campos y registros especificados
    # DELETE: Utilizado para eliminar registros de una tabla de una base de datos.

# Librerías
import os
from snowflake.snowpark import Session
import pandas as pd

# Función para insertar datos en la tabla de auditoria
def registrar_evento_auditoria(sesion_activa, nombre_esquema_destino, nombre_tabla, ruta_archivo, numero_registros, mensaje):
    """
    Registra un evento en la base de datos Snowflake en la tabla de auditoría.

    Args:
    - sesion_activa: Objeto de sesión activa de conexión a Snowflake.
    - nombre_esquema_destino (str): Nombre del esquema de destino.
    - nombre_tabla (str): Nombre de la tabla de destino.
    - ruta_archivo (str): Ruta completa o nombre del archivo CSV cargado.
    - numero_registros (int): Número de registros cargados en la tabla.
    - mensaje (str): Mensaje de carga de Snowflake. 

    Raises:
    - Exception: Si ocurre algún error al ejecutar la consulta SQL.
    """
    try:
        # Eliminar comillas simples en el mensaje para evitar errores SQL
        mensaje = mensaje.replace("'", "")

        # Obtener el próximo ID llamando al procedimiento almacenado
        resultado_id = sesion_activa.sql("CALL AUDITORIA.GET_NEXT_ID();").collect()

        # Extraer el valor del ID del resultado
        id_auditoria = resultado_id[0][0]  # Asumiendo que el ID es el primer valor en el resultado

        # Crear consulta SQL con valores directos en el INSERT
        query_insert = f"""
        INSERT INTO REPOSITORIO_TURISMO.AUDITORIA.AUDITORIA_CARGUES (
            ID_AUDITORIA,
            NOMBRE_ESQUEMA_DESTINO, 
            NOMBRE_TABLA, 
            FECHA_CARGUE, 
            RUTA_ARCHIVO, 
            NUMERO_REGISTROS,
            MENSAJE
        ) 
        VALUES (
            {id_auditoria},
            '{nombre_esquema_destino}', 
            '{nombre_tabla}', 
            CONVERT_TIMEZONE('America/Los_Angeles', 'America/Bogota', CURRENT_TIMESTAMP), 
            '{ruta_archivo}', 
            {numero_registros},
            '{mensaje}'
        );
        """
        
        # Ejecutar la consulta SQL
        sesion_activa.sql(query_insert).collect()
        
        print("Evento de auditoría registrado con éxito.")
    
    except Exception as e:
        print(f"Error al registrar el evento de auditoría: {e}")
        raise  # Re-lanzar la excepción para manejo adicional si es necesario

# Función para comparar archivos cargados con datos a cargar sin tener en cuenta su path completo
def validador_cargue(session_activa, dir, esquema):
    
    """
    Valida los archivos nuevos y repetidos comparando los existentes en un directorio local
    contra los registros almacenados en Snowflake. Extrae el nombre de los archivos cargados 
    sin su directorio completo. Funciona para cargues en que los archivos se almacenan en una
    carpeta y solo varia su nombre. No funciona para cargues donde los archivos se llaman igual
    pero están en diferentes carpetas. 

    Args:
        - session_activa : Objeto de sesión activa de conexión a Snowflake.
        - dir (str): Ruta del directorio local que contiene los archivos a validar.
        - esquema (str): Nombre del esquema de Snowflake donde se consultarán los archivos cargados.

    Returns:
        - Lista de nombres de archivos que están presentes en el directorio local pero no existen en los registros de Snowflake (archivos nuevos).
    """

    # Consulta para obtener los archivos cargados en Snowflake
    try:
        command = f"""
        SELECT SPLIT_PART(A.RUTA_ARCHIVO, '/', -1) AS NOMBRE_ARCHIVO
        FROM REPOSITORIO_TURISMO.AUDITORIA.AUDITORIA_CARGUES AS A
        WHERE A.NOMBRE_ESQUEMA_DESTINO = '{esquema}';
        """
        # Obtener la lista de archivos en Snowflake
        archivos_sql = session_activa.sql(command).collect()
        archivos_sql = [row['NOMBRE_ARCHIVO'] for row in archivos_sql]
    except Exception as e:
        raise RuntimeError(f"Error al ejecutar la consulta en Snowflake: {e}")

    # Obtener la lista de archivos en el directorio
    archivos_dir = os.listdir(dir)

    # Comparar los archivos
    archivos_repetidos = list(set(archivos_sql).intersection(set(archivos_dir)))
    archivos_nuevos = list(set(archivos_dir) - set(archivos_sql))

    # Mostrar los resultados repetidos
    print("Archivos repetidos:")
    for archivo in archivos_repetidos:
        print(archivo)

    # Mostrar los resultados no repetidos que están en el dir pero no en el resultado de SQL
    print("\nArchivos nuevos en el directorio:")
    for archivo in archivos_nuevos:
        print(archivo)

    # Retornar la lista de archivos nuevos
    return list(archivos_nuevos)

# Función para comparar archivos cargados con archivos a cargar teniendo en cuenta su path completo
def validador_cargue_path(session_activa, files_list, esquema):
    """
    Valida los archivos nuevos y repetidos comparando los existentes en una lista de archivos para 
    cargar contra los registros almacenados en Snowflake. Extrae el nombre completo de los archivos cargados
    en Snowflake junto con su directorio completo. Funciona para cargues donde los archivos se llaman igual pero están en 
    diferentes carpetas.

    Args:
        - session_activa : Objeto de sesión activa de conexión a Snowflake.
        - files_list (list): Lista que contiene las rutas de los archivos a cargar.
        - esquema (str): Nombre del esquema de Snowflake donde se consultarán los archivos cargados.

    Returns:
        - List[str]: Lista de nombres de archivos con sus rutas que están presentes en el directorio local pero no existen en los registros de Snowflake (archivos nuevos).
    """

    # Verificar que files_list no esté vacío
    if not files_list:
        raise ValueError("La lista de archivos a cargar está vacía. Proporcione una lista válida.")

    # Consulta para obtener los archivos cargados en Snowflake
    command = f"""
    SELECT A.RUTA_ARCHIVO AS NOMBRE_ARCHIVO
    FROM REPOSITORIO_TURISMO.AUDITORIA.AUDITORIA_CARGUES AS A
    WHERE A.NOMBRE_ESQUEMA_DESTINO = '{esquema}';
    """

    # Obtener la lista de archivos en Snowflake
    try:
        archivos_sql = session_activa.sql(command).collect()
        archivos_sql = [row['NOMBRE_ARCHIVO'] for row in archivos_sql]
    except Exception as e:
        raise RuntimeError(f"Error al ejecutar la consulta en Snowflake: {e}")

    # Convertir las listas a conjuntos
    archivos_repetidos = list(set(archivos_sql).intersection(files_list))
    archivos_nuevos = list(set(files_list) - set(archivos_sql))

    # Mostrar los resultados repetidos
    print("Archivos repetidos:")
    if archivos_repetidos:
        for archivo in archivos_repetidos:
            print(archivo)
    else:
        print("No se encontraron archivos repetidos.")

    # Mostrar los resultados nuevos
    print("\nArchivos nuevos en el directorio:")
    if archivos_nuevos:
        for archivo in archivos_nuevos:
            print(archivo)
    else:
        print("No se encontraron archivos nuevos en el directorio.")

    # Retornar la lista de archivos nuevos
    return list(archivos_nuevos)

def obtener_selector(query, columna, session):
    """
    Ejecuta una consulta SQL para obtener valores únicos de una columna específica
    y devuelve una lista ordenada alfabéticamente.

    Parámetros:
    - query (str): Consulta SQL a ejecutar.
    - columna (str): Nombre de la columna cuyos valores únicos se quieren extraer.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - opciones (list): Lista de valores únicos de la columna, ordenados alfabéticamente.

    Excepciones:
    - ValueError: Si la columna especificada no existe en los resultados.
    - Exception: Si ocurre un error al ejecutar la consulta.
    """
    try:
        # Ejecutar la consulta SQL y recoger resultados
        resultados = session.sql(query).collect()

        # Verificar que los resultados contengan la columna solicitada
        if not resultados or columna not in resultados[0].asDict():
            raise ValueError(f"La columna '{columna}' no se encontró en los resultados de la consulta.")

        # Extraer valores únicos de la columna y ordenarlos
        opciones = sorted({row[columna] for row in resultados})

        return opciones
    except Exception as e:
        # Manejo de errores genéricos con mensaje detallado
        raise Exception(f"Error al ejecutar la consulta o procesar resultados: {str(e)}")
    
def obtener_regiones_disponibles(session):
    """
    Ejecuta una consulta SQL para obtener las regiones disponibles excluyendo ciertos valores fijos
    y devuelve una lista de regiones ordenada alfabéticamente.

    Parámetros:
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - regiones (list): Lista de regiones disponibles ordenadas alfabéticamente.

    Excepciones:
    - Exception: Si ocurre un error al ejecutar la consulta.
    """
    try:
        # Definir la consulta SQL
        query = """
        SELECT DISTINCT REGION_NAME
        FROM REPOSITORIO_TURISMO.CORRELATIVAS.CONTINENTES
        WHERE REGION_NAME NOT IN ('Antártida', 'No Declarados', 'No definido', 'Sin Especificar')
        """

        # Ejecutar la consulta SQL y recoger resultados
        resultados = session.sql(query).collect()

        # Extraer los nombres de las regiones y ordenarlos
        regiones = sorted({row['REGION_NAME'] for row in resultados})

        return regiones
    except Exception as e:
        # Manejo de errores con mensaje detallado
        raise Exception(f"Error al ejecutar la consulta o procesar resultados: {str(e)}")


def obtener_paises_por_region(region_seleccionada, session):
    """
    Ejecuta una consulta SQL para obtener los países de una región específica seleccionada por el usuario
    y devuelve una lista de países ordenada alfabéticamente.

    Parámetros:
    - region_seleccionada (str): Nombre de la región seleccionada por el usuario.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - paises (list): Lista de países pertenecientes a la región seleccionada, ordenados alfabéticamente.

    Excepciones:
    - ValueError: Si la región seleccionada es nula o vacía.
    - Exception: Si ocurre un error al ejecutar la consulta.
    """
    try:
        # Verificar que el parámetro no esté vacío o nulo
        if not region_seleccionada:
            raise ValueError("Debe proporcionar una región válida para realizar el filtro.")

        # Definir la consulta con el filtro dinámico
        query = f"""
        SELECT DISTINCT COUNTRY_OR_AREA
        FROM REPOSITORIO_TURISMO.VISTAS.GEOGRAFIA
        WHERE REGION_NAME = '{region_seleccionada}'
        """

        # Ejecutar la consulta SQL y recoger resultados
        resultados = session.sql(query).collect()

        # Extraer los nombres de los países y ordenarlos
        paises = sorted({row['COUNTRY_OR_AREA'] for row in resultados})

        return paises
    except Exception as e:
        # Manejo de errores con mensaje detallado
        raise Exception(f"Error al ejecutar la consulta o procesar resultados: {str(e)}")
    
def ejecutar_consulta_segura(query, session):
    """
    Ejecuta una consulta SQL sobre una tabla específica y devuelve los resultados como un DataFrame.
    Si la consulta no devuelve datos, retorna un DataFrame vacío.

    Parámetros:
    - query (str): Consulta SQL a ejecutar.
    - session: Objeto de conexión activo a Snowflake.

    Retorna:
    - DataFrame con los resultados de la consulta si tiene datos.
    - DataFrame vacío si no hay resultados.
    
    Excepciones:
    - Exception: Si ocurre un error durante la ejecución de la consulta.
    """
    try:
        # Ejecutar la consulta y recoger resultados
        resultados = session.sql(query).collect()

        # Verificar si hay datos en los resultados
        if resultados:
            # Convertir a DataFrame
            df = pd.DataFrame(resultados)
        else:
            # Si no hay resultados, devolver un DataFrame vacío
            df = pd.DataFrame()
        
        return df
    except Exception as e:
        # Manejo de errores en la ejecución de la consulta
        raise Exception(f"Error al ejecutar la consulta: {str(e)}")

def ejecutar_multiples_consultas(consultas, session, pais_seleccionado=None):
    """
    Ejecuta múltiples consultas SQL y almacena los resultados en un diccionario.
    Si una consulta no devuelve datos, guarda un DataFrame vacío en su lugar.

    Parámetros:
    - consultas (dict): Diccionario donde las claves son nombres descriptivos de las consultas 
                        y los valores son las consultas SQL a ejecutar.
    - session: Objeto de conexión activo a Snowflake.
    - pais_seleccionado (str, opcional): Nombre del país seleccionado, para usar como contexto en mensajes.

    Retorna:
    - dict: Diccionario donde las claves son los nombres de las consultas y los valores son DataFrames con los resultados.
    """
    # Inicializar el objeto para almacenar los resultados
    resultados = {}

    # Ejecutar cada consulta y almacenar los resultados
    for nombre_tabla, query in consultas.items():
        try:
            print(f"Ejecutando consulta para {nombre_tabla}...")
            
            # Usar la función robusta para ejecutar la consulta
            df_resultado = ejecutar_consulta_segura(query, session)
            
            if df_resultado.empty:
                print(f"No se encontraron datos en {nombre_tabla}" + 
                      (f" para el país: {pais_seleccionado}" if pais_seleccionado else "."))
            else:
                print(f"Datos obtenidos de {nombre_tabla}" + 
                      (f" para {pais_seleccionado}: {len(df_resultado)} filas." if pais_seleccionado else f": {len(df_resultado)} filas."))
            
            # Guardar los resultados en el diccionario
            resultados[nombre_tabla] = df_resultado
        except Exception as e:
            print(f"Error al ejecutar la consulta para {nombre_tabla}: {str(e)}")
            # Guardar un DataFrame vacío en caso de error
            resultados[nombre_tabla] = pd.DataFrame()

    return resultados
