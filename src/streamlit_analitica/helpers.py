# Librerías
import base64
import streamlit as st

@st.cache_data(show_spinner=False)
def get_image(image_path):
    """
    Función que lee un archivo de imagen binario y lo convierte en una cadena base64.
    
    Parámetros:
    -----------
    image_path : str
        Ruta absoluta o relativa de la imagen a convertir.

    Retorna:
    --------
    str
        Cadena de texto en formato base64, útil para incrustar la imagen en componentes de Streamlit.
    
    Uso:
    ----
    Ideal para cargar imágenes estáticas que se mostrarán en Streamlit, evitando lecturas
    repetitivas y disminuyendo la latencia. Tras la lectura inicial, los resultados se almacenan
    en caché gracias a la anotación @st.cache_data.
    """

    # Se abre el archivo de imagen en modo binario (rb)
    with open(image_path, "rb") as image_file:
        # Se convierte el contenido a base64
        img_data = base64.b64encode(image_file.read()).decode()
    
    return img_data

@st.cache_data(show_spinner=False)
def get_icon(image_path):
    """
    Función que lee un archivo SVG (o cualquier archivo de texto) y lo convierte en una cadena base64.
    
    Parámetros:
    -----------
    image_path : str
        Ruta absoluta o relativa del archivo .svg a convertir.

    Retorna:
    --------
    str
        Cadena de texto en formato base64, conveniente para incrustar un ícono SVG en la interfaz.
    
    Uso:
    ----
    Recomendado para íconos SVG que se requieran en la aplicación. El uso de @st.cache_data
    permite que la conversión se realice una sola vez, optimizando el tiempo de ejecución.
    """

    # Se abre el archivo en modo lectura de texto (r)
    with open(image_path, "r") as file:
        svg_data = file.read()
        
    # Se codifica la cadena SVG en formato base64
    return base64.b64encode(svg_data.encode("utf-8")).decode("utf-8")


def load_css(file_name):
    """
    Función que carga el contenido de un archivo CSS y retorna su contenido como una cadena.

    Parámetros:
    -----------
    file_name : str
        Ruta absoluta o relativa del archivo CSS que se desea leer.

    Retorna:
    --------
    str
        Cadena de texto con el contenido completo del archivo CSS.

    Uso:
    ----
    - Permite inyectar estilos CSS personalizados en una aplicación Streamlit.
      Por ejemplo, usando:
        
        st.markdown(
            f"<style>{load_css('ruta/estilos.css')}</style>",
            unsafe_allow_html=True
        )
      
    - También se puede emplear en otros contextos donde se necesite acceder
      al contenido puro de un archivo CSS.

    Nota de robustez:
    ----------------
    - Se asume que el archivo CSS existe y es accesible. En caso contrario,
      Python lanzará un error (FileNotFoundError). Si se desea manejar
      excepciones, se puede envolver la lectura en un bloque try-except.
    """
    with open(file_name) as f:
        return f.read()


def limpiar_cache():
    """
    Función que limpia el caché de datos de Streamlit.

    Descripción:
    ------------
    - Utiliza el método st.cache_data.clear() para eliminar todo el contenido
      almacenado en la memoria caché de la aplicación.
    - Al invocar esta función, cualquier función con @st.cache_data deberá
      volver a procesar sus datos en la siguiente llamada.

    Parámetros:
    -----------
    No recibe parámetros.

    Retorna:
    --------
    None
        Esta función no retorna ningún valor.

    Uso:
    ----
    Ideal para situaciones en las que los datos o resultados cacheados
    requieren actualizarse, forzando a la aplicación a recalcular. Se puede
    invocar, por ejemplo, desde un botón en la interfaz de Streamlit:

        if st.button("Limpiar Caché"):
            limpiar_cache()

    Precaución:
    -----------
    - Al limpiar el caché, se incrementará el tiempo de procesamiento en la
      próxima ejecución de funciones cacheadas, ya que sus resultados
      deberán volver a ser generados.
    """
    st.cache_data.clear()  # Limpia el caché de datos de Streamlit

