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
