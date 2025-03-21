# Librerias
import streamlit as st
from .helpers import get_icon, get_image

def navbar():
    """
    Función que construye la barra de navegación (navbar) de la aplicación,
    utilizando la estructura y componentes de Bootstrap.

    Descripción:
    ------------
    - Carga la imagen del logotipo desde 'static/images/icons.png' con la
      función get_image(), devolviendo una cadena en formato base64.
    - Carga los íconos en formato SVG (house, location, sources) para mostrarlos
      en el menú de navegación con la función get_icon().
    - Inyecta la hoja de estilos de Bootstrap (v5.3.3) y luego
      genera y renderiza la estructura HTML de la barra de navegación.
    - Usa st.markdown() con unsafe_allow_html=True para que Streamlit acepte
      código HTML y CSS incrustado.

    Parámetros:
    -----------
    Ninguno.

    Retorna:
    --------
    None
        Su propósito es inyectar la navbar en la aplicación Streamlit.

    Dependencias:
    -------------
    - get_image(), get_icon(): Funciones que retornan las imágenes
      en formato base64.
    - st.markdown: Permite inyectar HTML y CSS a la aplicación.

    Uso:
    ----
    - Se recomienda llamar a navbar() al inicio de la aplicación para que la
      barra de navegación permanezca fija en la parte superior.
    - Ejemplo:
        navbar()

    Nota:
    -----
    - El uso de 'unsafe_allow_html=True' es necesario para aceptar este tipo
      de contenido.
    """

    # Se asume que ya tienes definidas las funciones get_image() y get_icon().
    logo = get_image("static/images/icons.png")
    house = get_icon("static/images/house-solid.svg")
    location = get_icon("static/images/location-dot-solid.svg")
    sources = get_icon("static/images/check-to-slot-solid.svg")
    top = get_icon("static/images/top-dot-solid.svg")

    st.markdown(f"""
        <!-- Hoja de estilos de Bootstrap (CDN) -->
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" 
            rel="stylesheet"
            integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" 
            crossorigin="anonymous">
        
        <nav class="navbar fixed-top navbar-expand-lg" style="margin-top: 0px; background-color: #646464;">
            <div class="container-fluid">
                <a class="navbar-brand" href="#">
                    <img src="data:image/png;base64,{logo}" width="280" height="25">
                </a>
                <button class="navbar-toggler" type="button" data-bs-toggle="collapse"
                        data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent"
                        aria-expanded="false" aria-label="Toggle navigation">
                    <span class="navbar-toggler-icon"></span>
                </button>
                <div class="collapse navbar-collapse" id="navbarSupportedContent">
                    <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                        <li class="nav-item" id="nav-item-inicio">
                            <a class="nav-link text-white" href="?page=1" target="_self">
                                <img src="data:image/svg+xml;base64,{house}" width="20" height="20">
                                <span style="padding-left: 2px; font-size: 14px;">Inicio</span>
                            </a>
                        </li>
                        <li class="nav-item" id="nav-item-centro-de-inteligencia-turismo-internacional">
                            <a class="nav-link text-white" href="?page=2" target="_self">
                                <img src="data:image/svg+xml;base64,{location}" width="20" height="20">
                                <span style="padding-left: 1px; font-size: 14px;">CITI</span>
                            </a>
                        </li>
                        <li class="nav-item" id="nav-item-Fuentes">
                            <a class="nav-link text-white" href="?page=3" target="_self">
                                <img src="data:image/svg+xml;base64,{sources}" width="20" height="20">
                                <span style="padding-left: 2px; font-size: 14px;">Fuentes</span>
                            </a>
                        </li>
                    </ul>
                     <!-- Botón "Volver arriba" a la derecha -->
                    <ul class="navbar-nav ms-auto mb-2 mb-lg-0">
                        <li class="nav-item" id="nav-item-Top">
                            <a class="nav-link text-white" href="#top" target="_self">
                                <img src="data:image/svg+xml;base64,{top}" width="20" height="20">
                                <span style="padding-left: 2px; font-size: 14px;">Volver arriba</span>
                            </a>
                        </li>
                    </ul>
                </div>
            </div>
        </nav>
        <!-- Bootstrap JS -->
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
        """, unsafe_allow_html=True)

def home_page():  

    """
    Función que construye la página de inicio del Centro de Inteligencia de Turismo Internacional (CITI).

    Descripción:
    ------------
    - Inyecta estilos CSS personalizados para adecuar la presentación de los 
      elementos en pantalla: títulos, espacios y botones.
    - Muestra un título principal centrado ("Centro de Inteligencia de Turismo (CIT)").
    - Inicializa y utiliza una variable 'selected_option' dentro de st.session_state
      para determinar cuál de las secciones (Descripción, Beneficios, Alcance y límites)
      se visualiza en la tarjeta (card) principal.
    - Define un diccionario 'card_content' con la estructura:
        {texto_del_boton: (título_card, contenido_card)},
      donde cada botón invoca la función interna 'update_card(option)' para cambiar
      dinámicamente el contenido mostrado.
    - Emplea dos columnas: la primera (col1) para renderizar los botones 
      y la segunda (col2) para renderizar la tarjeta con el contenido seleccionado.
    - El contenido de cada sección, incluido el texto HTML, se maneja con 
      st.markdown(..., unsafe_allow_html=True), lo que permite inyectar código
      HTML sin restricciones.

    Parámetros:
    -----------
    Ninguno.

    Retorna:
    --------
    None
        La función no devuelve ningún valor; su propósito es mostrar la 
        interfaz inicial dentro de la aplicación.

    Uso:
    ----
    Se puede invocar esta función para desplegar la pantalla principal, por ejemplo:
        if st.query_params.page == '1':
            home_page()

    Dependencias:
    -------------
    - streamlit (st): Librería base para la creación de aplicaciones web interactivas.
    - st.session_state: Mecanismo para almacenar datos persistentes entre 
      recargas de la aplicación.
    - unsafe_allow_html=True: Permite la inserción de HTML y CSS en la 
      aplicación, abriendo la posibilidad de estilos y formateos avanzados.

    Notas sobre el diseño:
    ----------------------
    - Los botones están configurados con un background color #485A68; 
      y cambian a #FF4C4C al pasar el mouse (hover).
    - Se utiliza una altura fija (340px) en la tarjeta para el contenido
      desplazable (overflow-y: auto;), adaptado para exhibir textos de 
      longitud variable.
    """

    st.markdown("""
            <style>
                [data-testid="stVerticalBlock"] {
                    gap: 0.5rem;
                }
            </style>    
            <br>
            <h2 style="font-weight: bold; text-align: center; margin-top: 50px; margin-bottom:10px; margin-left: 60px;">Centro de Inteligencia de Turismo Internacional (CITI)</h2> 
            <br>
        """, unsafe_allow_html=True)
    
    # Inicialización de la opción seleccionada en el estado si no existe.
    if "selected_option" not in st.session_state:
        st.session_state.selected_option = "Descripción&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&gt;"

    # Función interna para actualizar la opción seleccionada.
    def update_card(option):
        st.session_state.selected_option = option
    
    # Diccionario con las diferentes secciones (título y contenido)
    card_content = {
        "Descripción&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&gt;": 
        ("Descripción general", "El Centro de Inteligencia de Turismo Internacional (CITI) es una plataforma interactiva que permite a los usuarios analizar y explorar datos relacionados con el turismo, como la cantidad de turistas por país de origen, el gasto promedio, la estacionalidad y otros indicadores clave. A través de gráficos dinámicos y tablas personalizables, facilita la comprensión de tendencias turísticas y el análisis de flujos de turistas a lo largo del tiempo, facilitando la toma de decisiones en el sector público y privado."),

        "Beneficios&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&gt;":
        ("Beneficios", """
        <p>El beneficio principal es facilitar la toma de decisiones estratégicas al proporcionar un acceso rápido, claro y detallado a datos clave sobre el comportamiento turístico. Algunos beneficios específicos incluyen:</p> 
        <ul style="margin-top: 0px;">
            <li style="font-size: 15px;">Mejora en la toma de decisiones: Permite a gobiernos, empresas y organismos turísticos identificar patrones de demanda, ajustar estrategias de marketing, y planificar infraestructuras y servicios de manera más eficiente.</li>
            <li style="font-size: 15px;">Optimización de recursos: Ayuda a asignar recursos de manera más efectiva, al conocer las preferencias y los flujos turísticos, lo que facilita la segmentación de mercados y la focalización de esfuerzos en destinos o segmentos específicos.</li>
            <li style="font-size: 15px;">Análisis de tendencias y estacionalidad: Facilita la identificación de períodos de alta y baja demanda, permitiendo planificar promociones o eventos que aumenten la afluencia de turistas en temporadas bajas.</li>
            <li style="font-size: 15px;">Monitoreo en tiempo real: Proporciona datos actualizados que permiten una respuesta ágil ante cambios repentinos en los flujos turísticos, como crisis económicas, desastres naturales o eventos internacionales.</li>
            <li style="font-size: 15px;">Mejor comprensión del mercado: Ofrece una visión detallada de los turistas (por país de origen, preferencias de gasto, etc.), lo que ayuda a personalizar la oferta turística y mejorar la experiencia del visitante.</li>
        </ul>
        """),

        "Alcance y límites&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&gt;":
        ("Alcance y límites", """
        <p> Alcances:</p>
        <ul style="margin-top: 0px;">
            <li style="font-size: 15px;">Análisis exhaustivo de flujos turísticos globales y locales.</li>
            <li style="font-size: 15px;">Herramientas de visualización interactivas.</li>
            <li style="font-size: 15px;">Predicción y análisis de estacionalidad.</li>
        </ul>
        <p> Límites:</p>
        <ul style="margin-top: 0px;">
            <li style="font-size: 15px;">Dependencia de la calidad y disponibilidad de los datos.</li>
            <li style="font-size: 15px;">Posible desactualización de datos en tiempo real.</li>
            <li style="font-size: 15px;">Cobertura geográfica limitada.</li>
            <li style="font-size: 15px;">Falta de datos cualitativos sobre la experiencia del turista.</li>
            <li style="font-size: 15px;">Necesidad de habilidades técnicas para un análisis profundo.</li>
        </ul>
        """),
    }

    # Inyección de estilos para los botones.
    st.markdown('''
        <style>
            button[data-testid="stBaseButton-primary"], .stDownloadButton>button {
                background-color: #485A68; 
                color: white;
                border: none; 
                padding: 12px 20px; 
                font-size: 12px; 
                border-radius: 8px; 
                cursor: pointer; 
                transition: background-color 0.3s ease; 
                width: 50%;
                margin-left: 250px;
                margin-top: 15px;
            }

            button[data-testid="stBaseButton-primary"]:hover, .stDownloadButton>button:hover {
                background-color: #FF4C4C; 
            }

            button[data-testid="stBaseButton-primary"]:focus, , .stDownloadButton>button:focus {
                outline: none; 
            }
                
        </style>
    ''', unsafe_allow_html=True)

     # Distribución en columnas    
    col1, col2 = st.columns([0.40, 0.60])

    # Columna izquierda: Botones para cambiar la sección
    with col1:                  
        for option in card_content.keys():
            if st.button(option, type="primary"):
                update_card(option)

    # Columna derecha: Tarjeta que muestra el contenido seleccionado.
    with col2:
        title, text = card_content[st.session_state.selected_option]    
        st.markdown(f'''
            <div class="card" style="width: 80%; margin-left: 5px; margin-bottom: 40px; margin-top: 15px; height: 340px; overflow-y: auto; border: 1px solid #ddd; border-radius: 8px; padding: 20px 20px;">
                <div class="card-body"> 
                    <h5 class="card-title" style="text-align:left; font-size: 16px; font-weight: bold; margin-top: 10px;">{title}</h5>
                    <p class="card-text" style="font-size: 15px; margin-top: 10px;">{text}</p>
                </div>
            </div>
        ''', unsafe_allow_html=True)    

def footer():
    """
    Agrega un pie de página personalizado en una aplicación de Streamlit.

    Esta función define y renderiza un pie de página con información de contacto, 
    enlaces de interés y secciones de los ejes estratégicos de ProColombia. Se 
    aplica CSS personalizado para ocultar el pie de página predeterminado de Streamlit 
    y agregar un nuevo diseño con una estructura más definida.

    Características:
    ----------------
    - Modifica el padding del contenedor principal de Streamlit.
    - Define estilos para enlaces con diferentes estados (normal, visitado, hover, activo).
    - Oculta el pie de página predeterminado (`footer{visibility:hidden;}`).
    - Crea un nuevo pie de página con:
        - Información de contacto de ProColombia.
        - Enlaces a los principales ejes estratégicos (Exportaciones, Inversión, Turismo, Marca País).
        - Enlaces de interés (Servicios al ciudadano, Sostenibilidad, PQRFS, Contacto).
    - Aplica estilos CSS para mejorar la presentación del contenido.

    Retorna:
    --------
    - Renderiza el pie de página en la aplicación de Streamlit usando `st.write()` con `unsafe_allow_html=True`.

    Ejemplo de uso:
    ---------------
    Llamar a la función `footer()` al final de la aplicación para que el pie de página se renderice correctamente.

    ```python
    import streamlit as st
    
    # Contenido de la app
    st.title("Mi Aplicación en Streamlit")

    # Renderizar el pie de página
    footer()
    ```

    """
        
    ft = """
    <style>
    [data-testid="stMainBlockContainer"] {
        padding: 100px 0px 0px 0px;
    }
    
    a:link , a:visited{
    color: #BFBFBF;  
    background-color: transparent;
    text-decoration: none;
    }

    a:hover,  a:active {
    color: #0283C3; 
    background-color: transparent;
    text-decoration: underline;
    }

    #page-container {
    position: relative;
    min-height: 10vh;
    }

    footer{
        visibility:hidden;
    }

    .footer {
    position: relative;
    left: 0;
    top:100px;
    bottom: 0;
    width: 100%;
    background-color: #646464;
    padding: 12px 380px;
    color: #808080; 
    text-align: left; 
    }
    
    .footer h5 {
        color: white; 
        font-size: 12px; 
        font-weight: bold;
    }
    
    .footer ul {
        list-style: none; 
        padding: 0; 
        margin: 0; 
    }
    
    .footer li {
        font-size: 12px;
        color: white; 
        line-height: 1.3; 
        padding-left: 0; 
        margin-left: 0;
    }
    
    .footer a   {
        color:white;
    }
    
    </style>

    <br><br><br>  <!-- Agregar espacio antes del pie de página -->
    <br><br><br>  <!-- Agregar espacio antes del pie de página -->


    <div id="page-container">

    <div class="footer">
        <div class="row" style="margin-left: 60px;">
                <div class="col-md-4">
                    <h5 style="margin-bottom: 3px;" >LÍNEAS DE ATENCIÓN</h5>
                    <ul>
                        <li>Calle 28 No 13A - 15 Piso 35-36</li>
                        <li>Bogotá - Colombia</li>
                        <li>+57 601 5600100</li>
                        <li>Fax: +57 601 5600104</li>
                        <li>Lun - Vi 8:30 A.M. - 5:30 P.M</li>
                    </ul>
                </div>
                <div class="col-md-3">
                    <h5 style="margin-bottom: 3px;">NUESTROS EJES</h5>
                    <ul>
                        <li><a href="https://procolombia.co/" target="_blank">Procolombia</a></li>
                        <li><a href="https://investincolombia.com.co/es" target="_blank">Inversión</a></li>
                        <li><a href="https://procolombia.co/colombiatrade" target="_blank">Exportaciones</a></li>
                        <li><a href="https://colombia.travel/es" target="_blank">Turismo</a></li>
                        <li><a href="https://colombia.co/" target="_blank">Marca País</a></li>
                    </ul>
                </div>
                <div class="col-md-4">
                    <h5 style="margin-bottom: 3px;">ENLACES DE INTERÉS</h5>
                    <ul>
                        <li><a href="https://procolombia.co/transparencia/glosario" target="_blank">Servicios al ciudadano</a></li>
                        <li><a href="https://procolombia.co/sostenibilidad" target="_blank">Informe de sostenibilidad</a></li>
                        <li><a href="https://procolombia.co/transparencia/preguntas-frecuentes" target="_blank">Preguntas frecuentes</a></li>
                        <li><a href="https://procolombia.co/transparencia/pqrfs" target="_blank">PQRFS</a></li>
                        <li><a href="https://procolombia.co/contacto" target="_blank">Contacto</a></li>
                    </ul>
                </div>
            </div>
        </div>
    </div>
    """
    return st.write(ft, unsafe_allow_html=True)