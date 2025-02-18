# Librerias
import warnings
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica
import src.snowflake_analitica as snowflake_analitica
import src.plotly_analitica as plotly_analitica
import src.datos_citi as procesamiento_datos

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="Dashboard", 
                   page_icon = ':beach_with_umbrella:', 
                   layout="wide",  
                   initial_sidebar_state="collapsed")

# Inclusión de la hoja de estilos de Bootstrap para mejorar la apariencia.
st.markdown("""
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

st.title("Centro de Inteligencia de Turismo (CIT)")

# Marcador para volver al inicio
st.markdown("<a id='top'></a>", unsafe_allow_html=True)

st.divider()

# Actualizar flujo de Snowflake
snowflake_analitica.flujo_snowflake()

# Actualizar tiempo de última actividad
snowflake_analitica.update_last_activity()

######################
# Selector de regiones
######################
region_elegida = st.selectbox(label='Seleccione un continente:',
             options=snowflake_analitica.obtener_regiones_disponibles(st.session_state.session),
             placeholder='Elija una opción',
             index=None,
             help = 'Seleccione un único continente para refinar su búsqueda de países disponibles.', 
             key = 'widget_continentes',
             on_change=streamlit_analitica.on_selectbox_change()
             )
st.divider()

####################
# Selector de países
####################
if region_elegida:
    
    # Registrar evento
    snowflake_analitica.registrar_evento(sesion_activa= st.session_state.session, tipo_evento = 'Selección de continente', detalle_evento = 'Selección de continente', unidad = region_elegida)


    # El selector de países se habilita después de la elección de un continente        
    pais_elegido = st.selectbox(label='Seleccione un país:',
                options=snowflake_analitica.obtener_paises_por_region(region_elegida, st.session_state.session),
                placeholder='Elija una opción',
                index=None,
                help = 'Seleccione un único país para obtener información detallada de métricas de turismo.', 
                key = 'widget_paises',
                on_change=streamlit_analitica.on_selectbox_change()
                )
    st.divider()
   
    # Habilitar contenido si se selecciona un país
    if pais_elegido:

        # Registrar evento
        snowflake_analitica.registrar_evento(sesion_activa= st.session_state.session, tipo_evento = 'Selección de país', detalle_evento = 'Visualización de país', unidad = pais_elegido)

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

        ###############################################
        # Obtener datos del país elegido por el usuario
        ###############################################
        df_global_data, df_oag, df_fk, df_credibanco, df_iata = streamlit_analitica.obtener_datos(_pais_elegido=pais_elegido)
        st.divider()

        ####################
        # TABLA DE CONTENIDO
        ####################

        st.markdown(f"""
        ## **Tabla de Contenido**

        ### **Indicadores de turismo de {pais_elegido} hacia el mundo**
        - [Flujos de viajeros hacia el mundo](#flujos-de-viajeros-hacia-el-mundo)
        - [Conectividad con el mundo](#conectividad-con-el-mundo)
        - [Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile](#reservas-y-busquedas-hacia-mexico-costa-rica-peru-y-chile)

        ### **Indicadores de turismo de {pais_elegido} hacia Colombia**
        - [Flujos de viajeros hacia Colombia](#flujos-de-viajeros-hacia-colombia)
        - [Conectividad con Colombia](#conectividad-con-colombia)
        - [Gasto con tarjeta de crédito en Colombia](#gasto-con-tarjeta-de-credito-en-colombia)
        - [Reservas y Búsquedas hacia Colombia](#reservas-y-busquedas-hacia-colombia)
        - [Agencias que venden Colombia como destino](#agencias-que-venden-colombia-como-destino)

        ### **Salida de colombianos hacia {pais_elegido}**
        - [Salida de colombianos hacia el mercado](#salida-de-colombianos-hacia-el-mercado)
        """)
        
        ###################################################
        # Indicadores de turismo del mercado hacia el mundo
        ###################################################
        st.divider()
        st.markdown(f"### **Indicadores de turismo de {pais_elegido} hacia el mundo**")

        # Nombres para las pestañas
        tab1_title = 'Ver distribución anual'
        tab2_title = 'Explorar datos por año'
        
        ###################################
        # Flujos de viajeros hacia el mundo
        ###################################

        st.markdown("<a id='flujos-de-viajeros-hacia-el-mundo'></a>", unsafe_allow_html=True)
        st.divider()
        st.subheader("Flujos de viajeros hacia el mundo")

        # Fuente
        global_data_fuente = 'GlobalData'

        # Gráficos Global Data
        (   
            fig_time_series_viajeros,
            fig_stacked_h_medio_viajeros,
            fig_treemap_medio_viajeros,
            fig_time_series_noches_percnotacion,
            fig_time_series_gasto,
            fig_stacked_h_categoria_gasto,
            fig_treemap_categoria_gasto,
            fig_stacked_h_edad_viajeros,
            fig_treemap_edad_viajeros,
            fig_stacked_h_motivo_viajeros,
            fig_treemap_motivo_viajeros,
            fig_stacked_h_forma_viajeros,
            fig_treemap_forma_viajeros,
            fig_stacked_h_destinos_viajeros,
            fig_treemap_destinos_viajeros,
            fig_time_series_mice
                                                    
        ) = streamlit_analitica.obtener_graficos_global_data(df_global_data, pais_elegido)

        # Crear dataframe para MICE para botón de descarga
        if 'Motivo de viaje' in df_global_data['flujos_negocios'].columns:
            df_mice = df_global_data['flujos_negocios'][df_global_data['flujos_negocios']['Motivo de viaje'] == 'Reuniones, incentivos, congresos y exposiciones (MICE)']
        else:
            df_mice = pd.DataFrame()
            
        # Expander
        with st.expander("Explora todos los indicadores"):

             # GRÁFICO ÚNICO A LA IZQUIERDA Y BOTONES DE CAMBIO A LA DERECHA
            
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Serie de tiempo de viajeros
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Serie de tiempo de viajeros', unidad=pais_elegido, df=df_global_data['viajeros_serie_tiempo'][['Año', 'Viajeros']])
                # Columna 2: Viajeros por medio de transporte
                with col2:
                    # Crear botones para cambiar entre gráficos
                    tab1, tab2 = st.tabs([tab1_title, tab2_title])
                    # Botón 1
                    with tab1:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_medio_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por medio de transporte - StackedH', unidad=pais_elegido, df=df_global_data['viajeros_medio'][['Año', 'Medio de transporte', 'Viajeros', 'Participación (%)']])
                    # Botón 2
                    with tab2:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_medio_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por medio de transporte - Treemap', unidad=pais_elegido, df=df_global_data['viajeros_medio'][['Año', 'Medio de transporte', 'Viajeros', 'Participación (%)']])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Serie de tiempo noches de pernoctación
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_noches_percnotacion, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Noches Percnotación', unidad=pais_elegido, df=df_global_data['noches_pernoctacion'][['Año', 'Noches de percnotación']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y BOTONES DE CAMBIO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Serie de tiempo gasto
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Serie de tiempo gasto', unidad=pais_elegido, df=df_global_data['gasto_serie_tiempo'][['Año', 'Gasto (USD)']])               
                # Columna 2: Gasto por categoria
                with col2:
                    # Crear botones para cambiar entre gráficos
                    tab1, tab2 = st.tabs([tab1_title, tab2_title])
                    # Botón 1
                    with tab1:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_categoria_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Gasto por categoria - StackedH', unidad=pais_elegido, df=df_global_data['gasto_categoria'][['Año', 'Gasto (USD)']])
                    # Botón 2
                    with tab2:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_categoria_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Gasto por categoria - Treemap', unidad=pais_elegido, df=df_global_data['gasto_categoria'][['Año', 'Gasto (USD)']])
            
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Viajeros por edad
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_edad_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por edad - StackedH', unidad=pais_elegido, df=df_global_data['rango_edad'][['Año', 'Rango de Edad', 'Viajeros', 'Participación (%)']])
                # Columna 2: Viajeros por edad
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_edad_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por edad - Treemap', unidad=pais_elegido, df=df_global_data['rango_edad'][['Año', 'Rango de Edad', 'Viajeros', 'Participación (%)']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Motivo viaje
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_motivo_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Motivo viaje - StackedH', unidad=pais_elegido, df=df_global_data['motivo_viaje'][['Año', 'Motivo de Viaje', 'Viajeros', 'Participación (%)']])
                # Columna 2: Motivo viaje
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_motivo_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Motivo viaje - Treemap', unidad=pais_elegido, df=df_global_data['motivo_viaje'][['Año', 'Motivo de Viaje', 'Viajeros', 'Participación (%)']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Forma de viaje
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_forma_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Forma de viaje - StackedH', unidad=pais_elegido, df=df_global_data['forma_viaje'][['Año', 'Forma de Viaje', 'Viajeros', 'Participación (%)']])
                # Columna 2: Forma de viaje
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_forma_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Forma de viaje - Treemap', unidad=pais_elegido, df=df_global_data['forma_viaje'][['Año', 'Forma de Viaje', 'Viajeros', 'Participación (%)']])
                    
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Destinos
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_destinos_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Destinos - StackedH', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'][['Año', 'País Destino', 'Viajeros', 'Participación (%)']])
                # Columna 2: Destinos
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_destinos_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Destinos - Treemap', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'][['Año', 'País Destino', 'Viajeros', 'Participación (%)']])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # MICE
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_mice, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - MICE', unidad=pais_elegido, df=df_mice)

        ###########################
        # Conectividad con el mundo
        ###########################
        st.divider()
        st.markdown("<a id='conectividad-con-el-mundo'></a>", unsafe_allow_html=True)
        st.subheader("Conectividad con el mundo")

        # Fuente
        oag_fuente = 'OAG'

        # Obtener gráficos OAG Mundo
        (
            fig_single_barchart_conectividad_mundo_sillas,
            fig_single_barchart_conectividad_mundo_frecuencias,
            fig_stacked_h_conectividad_frecuencias_destinos_cerrado,
            fig_stacked_h_conectividad_frecuencias_destinos_corrido
        ) = streamlit_analitica.obtener_graficos_oag_mundo(df_oag, pais_elegido)
        
        # Expander
        with st.expander("Explora todos los indicadores"):

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con el mundo: Sillas
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_sillas, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con el mundo: Sillas', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'][['Año', 'Frecuencias', 'Sillas']])
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con el mundo: Frencuencias 
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_frecuencias, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con el mundo: Frencuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'][['Año', 'Frecuencias', 'Sillas']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Conectividad mundo cerrado destinos: Frecuencias
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_cerrado, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad mundo cerrado destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_cerrado'][['Año', 'País Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])
                # Columna 2: StackedH: Conectividad mundo corrido destinos: Frecuencias
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_corrido, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - StackedH: Conectividad mundo corrido destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_corrido'][['Periodo', 'País Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])

        #############################################################
        # Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile
        #############################################################
        st.divider()
        st.markdown("<a id='reservas-y-busquedas-hacia-mexico-costa-rica-peru-y-chile'></a>", unsafe_allow_html=True)
        st.subheader("Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile")

        # Fuente
        fk_fuente = 'ForwardKeys'

        # Obtener gráficos FK Mundo
        (
            fig_multiple_time_series_reservas_mundo,
            fig_multiple_time_series_busquedas_mundo
        ) = streamlit_analitica.obtener_graficos_fk_mundo(df_fk, pais_elegido)

        # Expander
        with st.expander("Explora todos los indicadores"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Reservas activas del país hacia México, Costa Riva, Perú y Chile
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_reservas_mundo, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Reservas activas del país hacia México, Costa Rica, Perú y Chile', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo'][['País', 'Fecha', 'Reservas']])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Búsquedas activas del país hacia México, Costa Rica, Perú y Chile
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_busquedas_mundo, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Búsquedas activas del país hacia México, Costa Rica, Perú y Chile', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo'][['País', 'Fecha', 'Búsquedas']])

        ###################################################
        # Indicadores de turismo del mercado hacia Colombia
        ###################################################
        st.divider()
        st.markdown(f'### **Indicadores de turismo de {pais_elegido} hacia Colombia**')

        ###################################
        # Flujos de viajeros hacia Colombia
        ###################################
        st.divider()
        st.markdown("<a id='flujos-de-viajeros-hacia-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Flujos de viajeros hacia Colombia")

        # Expander
        with st.expander("Explora todos los indicadores"):
            st.write("Contenido")

        # Conectividad con Colombia
        st.markdown("<a id='conectividad-con-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Conectividad con Colombia")

        # Obtener gráficos OAG Colombia
        (
            fig_single_barchart_conectividad_colombia_sillas,
            fig_single_barchart_conectividad_colombia_frecuencias,
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado,
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido
        ) = streamlit_analitica.obtener_graficos_oag_colombia(df_oag, pais_elegido)

        # Expander
        with st.expander("Explora todos los indicadores"):

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con Colombia: Sillas
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_sillas, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con Colombia: Sillas', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'][['Año', 'Frecuencias', 'Sillas']])
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con Colombia: Frencuencias
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_frecuencias, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con Colombia: Frencuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'][['Año', 'Frecuencias', 'Sillas']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Conectividad Colombia cerrado destinos: Frecuencias
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad Colombia cerrado destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_cerrado'][['Año', 'Municipio Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])
                # Columna 2: StackedH: Conectividad Colombia corrido destinos: Frecuencias
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad Colombia corrido destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_corrido'][['Periodo', 'Municipio Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])

        ##########################################
        # Gasto con tarjeta de crédito en Colombia
        ##########################################
        st.divider()
        st.markdown("<a id='gasto-con-tarjeta-de-credito-en-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Gasto con tarjeta de crédito en Colombia")
        
        # Fuente
        credibanco_fuente = 'Credibanco'

        # Obtener gráficos Credibanco
        (
            fig_side_by_side_bar_gasto_promedio,
            fig_stacked_h_gasto_categoria_credibanco,
            fig_treemap_gasto_categoria_credibanco,
            fig_stacked_h_gasto_categoria_directo_credibanco,
            fig_treemap_gasto_categoria_directo_credibanco,
            fig_stacked_h_gasto_categoria_indirecto_credibanco,
            fig_treemap_gasto_categoria_indirecto_credibanco
        ) = streamlit_analitica.obtener_graficos_credibanco(df_credibanco, pais_elegido)

        # Expander
        with st.expander("Explora todos los indicadores"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Gasto promedio
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_side_by_side_bar_gasto_promedio, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Gasto promedio', unidad=pais_elegido, df=df_credibanco['gasto_promedio'][['Año', 'Facturación (USD)', 'Turistas', 'Transacciones', 'Gasto promedio tarjeta (USD)', 'Gasto promedio transacción (USD)']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por categoria
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por categoria', unidad=pais_elegido, df=df_credibanco['gasto_categoria'][['Año', 'Clasificación', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])
                
                # Columna 2 Gasto por categoria
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por categoria', unidad=pais_elegido, df=df_credibanco['gasto_categoria'][['Año', 'Clasificación', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por productos directo
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por productos directo', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])
                
                # Columna 2 Gasto por productos directo
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por productos directo', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])

                    
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por productos indirecto
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por productos indirecto', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])
                 
                # Columna 2 Gasto por productos indirecto
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por productos indirecto', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])          

        #####################################
        # Reservas y Búsquedas hacia Colombia
        #####################################
        st.divider()
        st.markdown("<a id='reservas-y-busquedas-hacia-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Reservas y Búsquedas hacia Colombia")

        # Obtener gráficos FK Colombia
        (
            fig_single_time_series_reservas_colombia,
            fig_single_time_series_busquedas_colombia
        ) = streamlit_analitica.obtener_graficos_fk_colombia(df_fk, pais_elegido)

        # Expander
        with st.expander("Explora todos los indicadores"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Reservas aéreas activas del país hacia Colombia
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_reservas_colombia, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Reservas aéreas activas del país hacia Colombia', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo_colombia'][['País', 'Fecha', 'Reservas']])
                

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Búsquedas activas del país hacia Colombia 
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_busquedas_colombia, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Búsquedas activas del país hacia Colombia', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo_colombia'][['País', 'Fecha', 'Búsquedas']])

        ###########################################
        # Agencias que venden Colombia como destino
        ###########################################
        st.divider()
        st.markdown("<a id='agencias-que-venden-colombia-como-destino'></a>", unsafe_allow_html=True)
        st.subheader("Agencias que venden Colombia como destino")

        # Fuente
        iata_fuente = 'IATA-GAP'

        # Obtener gráficos IATA-GAP
        (
            fig_single_time_series_agencias_colombia, 
            fig_stacked_h_agencias_ciudades
        ) = streamlit_analitica.obtener_graficos_iata_colombia(df_iata, pais_elegido)

        # Expander
        with st.expander("Explora todos los indicadores"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Indicadores de agencias de ese mercado que venden Colombia como destino mostrar por q
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_agencias_colombia, fuente=iata_fuente, detalle_evento='Descarga Excel IATAGAP - Indicadores de agencias de ese mercado que venden Colombia como destino', unidad=pais_elegido, df=df_iata['agencias_ciudades'][['Año', 'Número de Agencias']])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Agencias que venden Colombia como destino por ciudad de la agencia
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_agencias_ciudades, fuente=iata_fuente, detalle_evento='Descarga Excel IATAGAP - Agencias que venden Colombia como destino por ciudad de la agencia', unidad=pais_elegido, df=df_iata['agencias_ciudades'][['Año', 'Ciudad de la Agencia', 'Número de Agencias', 'Total Anual', 'Participación (%)']])

        ########################################
        # Salida de colombianos hacia el mercado
        ########################################
        st.divider()
        st.markdown(f'### **Salida de colombianos hacia {pais_elegido}**')
        st.markdown("<a id='salida-de-colombianos-hacia-el-mercado'></a>", unsafe_allow_html=True)
        st.subheader("Salida de colombianos hacia el mercado")

        # Expander
        with st.expander("Explora todos los indicadores"):
            st.write("Contenido")



