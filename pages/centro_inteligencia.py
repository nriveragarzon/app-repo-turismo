# Librerias
import requests
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Impotar modulos
import src.streamlit_analitica as streamlit_analitica
import src.snowflake_analitica as snowflake_analitica

# Configuración página web - tipo wide sin sidebar activa
st.set_page_config(page_title="Centro de Inteligencia de Turismo Internacional", 
                   page_icon = ':world_map:', 
                   layout="wide",  
                   initial_sidebar_state="expanded")

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

# Estructura
leftsidebar, body, rightsidebar = st.columns([0.01,0.98, 0.01], gap='small',vertical_alignment='top')

# Aprovechar el máximo espacio horizontal de la pantalla
with body:
    
    # Marcador para volver al inicio
    st.markdown("<a id='top'></a>", unsafe_allow_html=True)

    st.title("Centro de Inteligencia de Turismo Internacional (CITI)")

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
                url = f"https://flagcdn.com/h120/{iso_code.lower()}.png"
                response = requests.head(url)
                if response.status_code == 200:
                    st.image(f"https://flagcdn.com/h120/{iso_code.lower()}.png", width=100)
                else:
                    st.write("Bandera no disponible")
            with col4:
                st.write("")

            ###############################################
            # Obtener datos del país elegido por el usuario
            ###############################################
            df_global_data, df_oag, df_fk, df_credibanco, df_iata = streamlit_analitica.obtener_datos(_pais_elegido=pais_elegido)
            st.divider()

            # Obtener tabla de resumen
            df_resumen = streamlit_analitica.generar_tabla_resumen(pais_elegido=pais_elegido, df_global_data=df_global_data, df_oag=df_oag, df_credibanco=df_credibanco, df_iata=df_iata, year_global_data='2025', year_oag_mundo='2024', year_oag_colombia='2024', year_credibanco='2024', year_iata='2024')

            # Mostrar tabla de resumen
            if not df_resumen.empty:
                
                # Título
                st.markdown("## Resumen")
                st.dataframe(data=df_resumen, use_container_width=True, hide_index=True, key='tabla_resumen', on_select="ignore", selection_mode="multi-row")
                

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
            
            ##################################
            # Flujo de viajeros hacia el mundo
            ##################################

            st.markdown("<a id='flujos-de-viajeros-hacia-el-mundo'></a>", unsafe_allow_html=True)
            st.subheader(f"Flujo de viajeros de {pais_elegido} hacia el mundo")

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

                # Contenedor con la estructura: Gráfico único a la izquierda y botones de cambio a la derecha
                with st.container(height = 625):

                    # Gráfico único a la izquierda y botones de cambio a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Flujo de viajeros de {pais_elegido} hacia el mundo</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_viajeros, fuente=global_data_fuente, llave='graph_1')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['viajeros_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Viajeros (miles)', llave='boton_1', unidad=pais_elegido, df=df_global_data['viajeros_serie_tiempo'][['Año', 'Viajeros']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Flujo de viajeros de {pais_elegido} hacia el mundo por medio de transporte</h6>', unsafe_allow_html=True)
                        # Contenedor con los gráficos
                        with st.container(height = 500, border=True):
                            # Crear botones para cambiar entre gráficos
                            tab1, tab2 = st.tabs([tab1_title, tab2_title])
                            with tab1:
                                # Gráfico
                                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_medio_viajeros, fuente=global_data_fuente, llave='graph_2')
                            with tab2:
                                # Gráfico
                                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_medio_viajeros, fuente=global_data_fuente, llave='graph_3')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['viajeros_medio'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Viajeros (miles) por medio de transporte', llave='boton_2', unidad=pais_elegido, df=df_global_data['viajeros_medio'][['Año', 'Medio de transporte', 'Viajeros', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Noches de percnotación promedio de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_noches_percnotacion, fuente=global_data_fuente, llave='graph_4')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_global_data['noches_pernoctacion'].empty:
                        streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Noches de percnotación', llave='boton_3', unidad=pais_elegido, df=df_global_data['noches_pernoctacion'][['Año', 'Noches de percnotación']])

                # Contenedor con la estructura: Gráfico único a la izquierda y botones de cambio a la derecha
                with st.container(height = 625):

                    # Gráfico único a la izquierda y botones de cambio a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto promedio del viajero de {pais_elegido} al mundo</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_gasto, fuente=global_data_fuente, llave='graph_5')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['gasto_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Gasto (USD)', llave='boton_4', unidad=pais_elegido, df=df_global_data['gasto_serie_tiempo'][['Año', 'Gasto (USD)']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto promedio del viajero de {pais_elegido} al mundo por categoria</h6>', unsafe_allow_html=True)
                        # Contenedor con los gráficos
                        with st.container(height = 500, border=True):
                            # Crear botones para cambiar entre gráficos
                            tab1, tab2 = st.tabs([tab1_title, tab2_title])
                            with tab1:
                                # Gráfico
                                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_categoria_gasto, fuente=global_data_fuente, llave='graph_6')
                            with tab2:
                                # Gráfico
                                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_categoria_gasto, fuente=global_data_fuente, llave='graph_7')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['gasto_categoria'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Categoria de Gasto (USD)', llave='boton_5', unidad=pais_elegido, df=df_global_data['gasto_categoria'][['Año', 'Categoria de Gasto', 'Gasto (USD)', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Rango de edad del viajero promedio de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_edad_viajeros, fuente=global_data_fuente, llave='graph_8')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['rango_edad'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Rango de edad del viajero (miles) promedio', llave='boton_6', unidad=pais_elegido, df=df_global_data['rango_edad'][['Año', 'Rango de Edad', 'Viajeros', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_edad_viajeros, fuente=global_data_fuente, llave='graph_9')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['rango_edad'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Rango de edad del viajero (miles) promedio', llave='boton_7', unidad=pais_elegido, df=df_global_data['rango_edad'][['Año', 'Rango de Edad', 'Viajeros', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Motivos de viaje de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_motivo_viajeros, fuente=global_data_fuente, llave='graph_10')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['motivo_viaje'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Motivo de Viaje - Viajeros (miles)', llave='boton_8', unidad=pais_elegido, df=df_global_data['motivo_viaje'][['Año', 'Motivo de Viaje', 'Viajeros', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_motivo_viajeros, fuente=global_data_fuente, llave='graph_11')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['motivo_viaje'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Motivo de Viaje - Viajeros (miles)', llave='boton_9', unidad=pais_elegido, df=df_global_data['motivo_viaje'][['Año', 'Motivo de Viaje', 'Viajeros', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Forma de viaje de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_forma_viajeros, fuente=global_data_fuente, llave='graph_12')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['forma_viaje'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Forma de Viaje - Viajeros (miles)', llave='boton_10', unidad=pais_elegido, df=df_global_data['forma_viaje'][['Año', 'Forma de Viaje', 'Viajeros', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_forma_viajeros, fuente=global_data_fuente, llave='graph_13')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['forma_viaje'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Forma de Viaje - Viajeros (miles)', llave='boton_11', unidad=pais_elegido, df=df_global_data['forma_viaje'][['Año', 'Forma de Viaje', 'Viajeros', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Principales destinos internacionales de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_destinos_viajeros, fuente=global_data_fuente, llave='graph_14')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['destinos_internacionales_top5'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Principales destinos internacionales', llave='boton_12', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'][['Año', 'País Destino', 'Viajeros', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_destinos_viajeros, fuente=global_data_fuente, llave='graph_15')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_global_data['destinos_internacionales_top5'].empty:
                            streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Principales destinos internacionales - Viajeros (miles)', llave='boton_13', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'][['Año', 'País Destino', 'Viajeros', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Flujo de viajeros internacionales de {pais_elegido} por motivo de Reuniones, incentivos, congresos y exposiciones (MICE)</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_mice, fuente=global_data_fuente, llave='graph_16')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_mice.empty:
                        streamlit_analitica.boton_descarga(fuente=global_data_fuente, variable='Flujo de viajeros (miles) internacionales por MICE', llave='boton_14', unidad=pais_elegido, df=df_mice[['Año', 'Motivo de viaje', 'Viajeros']])
                
            ###########################
            # Conectividad con el mundo
            ###########################
            st.markdown("<a id='conectividad-con-el-mundo'></a>", unsafe_allow_html=True)
            st.subheader(f"Conectividad de {pais_elegido} con el mundo")

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

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):
            
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con el mundo: Sillas</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_sillas, fuente=oag_fuente, llave='graph_17')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_mundo_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con el mundo - Sillas', llave='boton_15', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'][['Año', 'Sillas']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con el mundo: Frecuencias</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_frecuencias, fuente=oag_fuente, llave='graph_18')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_mundo_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con el mundo - Frecuencias', llave='boton_16', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'][['Año', 'Frecuencias']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con algunos destinos internacionales: Frecuencias - Año cerrado</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_cerrado, fuente=oag_fuente, llave='graph_19')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_mundo_destino_cerrado'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con algunos destinos internacionales - Frecuencias - Año cerrado', llave='boton_17', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_cerrado'][['Año', 'País Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con algunos destinos internacionales: Frecuencias - Año corrido</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_corrido, fuente=oag_fuente, llave='graph_20')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_mundo_destino_corrido'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con algunos destinos internacionales - Frecuencias - Año corrido', llave='boton_18', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_corrido'][['Periodo', 'País Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])

            #############################################################
            # Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile
            #############################################################
            st.markdown("<a id='reservas-y-busquedas-hacia-mexico-costa-rica-peru-y-chile'></a>", unsafe_allow_html=True)
            st.subheader(f"Reservas y Búsquedas de {pais_elegido} hacia México, Costa Rica, Perú y Chile")

            # Fuente
            fk_fuente = 'ForwardKeys'

            # Obtener gráficos FK Mundo
            (
                fig_multiple_time_series_reservas_mundo,
                fig_multiple_time_series_busquedas_mundo
            ) = streamlit_analitica.obtener_graficos_fk_mundo(df_fk, pais_elegido)

            # Expander
            with st.expander("Explora todos los indicadores"):

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Reservas activas de {pais_elegido} hacia México, Costa Rica, Perú y Chile</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_reservas_mundo, fuente=fk_fuente, llave='graph_21')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_fk['reservas_serie_tiempo'].empty:
                        streamlit_analitica.boton_descarga(fuente=fk_fuente, variable='Reservas activas del país hacia México, Costa Rica, Perú y Chile', llave='boton_19', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo'][['País', 'Fecha', 'Reservas']])
                
                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Búsquedas activas de {pais_elegido} hacia México, Costa Rica, Perú y Chile</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_busquedas_mundo, fuente=fk_fuente, llave='graph_22')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_fk['busquedas_serie_tiempo'].empty:
                        streamlit_analitica.boton_descarga(fuente=fk_fuente, variable='Búsquedas activas del país hacia México, Costa Rica, Perú y Chile', llave='boton_20', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo'][['País', 'Fecha', 'Búsquedas']])

            ###################################################
            # Indicadores de turismo del mercado hacia Colombia
            ###################################################
            st.divider()
            st.markdown(f'### **Indicadores de turismo de {pais_elegido} hacia Colombia**')
            
            ##################################
            # Flujo de viajeros hacia Colombia
            ##################################

            st.markdown("<a id='flujos-de-viajeros-hacia-colombia'></a>", unsafe_allow_html=True)
            st.subheader(f"Flujo de viajeros de {pais_elegido} hacia Colombia")

            # Expander
            with st.expander("Explora todos los indicadores"):
                st.write("Contenido")

            ###########################
            # Conectividad con Colombia
            ###########################
            st.markdown("<a id='conectividad-con-colombia'></a>", unsafe_allow_html=True)
            st.subheader(f"Conectividad de {pais_elegido} con Colombia")

            # Obtener gráficos OAG Colombia
            (
                fig_single_barchart_conectividad_colombia_sillas,
                fig_single_barchart_conectividad_colombia_frecuencias,
                fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado,
                fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido
            ) = streamlit_analitica.obtener_graficos_oag_colombia(df_oag, pais_elegido)

            # Expander
            with st.expander("Explora todos los indicadores"):

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con Colombia: Sillas</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_sillas, fuente=oag_fuente, llave='graph_23')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_colombia_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con Colombia - Sillas', llave='boton_21', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'][['Año', 'Sillas']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad de {pais_elegido} con Colombia: Frecuencias</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_frecuencias, fuente=oag_fuente, llave='graph_24')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_colombia_serie_tiempo'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con Colombia - Frecuencias', llave='boton_22', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'][['Año', 'Frecuencias']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):

                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad del {pais_elegido} con Colombia: Frecuencias - Año cerrado</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado, fuente=oag_fuente, llave='graph_25')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_colombia_municipio_cerrado'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con algunos destinos internacionales - Frecuencias - Año cerrado', llave='boton_23', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_cerrado'][['Año', 'Municipio Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])
                    with col2: 
                        # Título
                        st.markdown(f'<h6 class="custom-header" style="text-align:center;">Conectividad del {pais_elegido} con Colombia: Frecuencias - Año corrido</h6>', unsafe_allow_html=True)
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido, fuente=oag_fuente, llave='graph_26')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_oag['conectividad_colombia_municipio_corrido'].empty:
                            streamlit_analitica.boton_descarga(fuente=oag_fuente, variable='Conectividad del país con algunos destinos internacionales - Frecuencias - Año corrido', llave='boton_24', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_corrido'][['Periodo', 'Municipio Destino', 'Frecuencias', 'Sillas', 'Participación Frecuencias (%)', 'Participación Sillas (%)']])


            ##########################################
            # Gasto con tarjeta de crédito en Colombia
            ##########################################
            st.markdown("<a id='gasto-con-tarjeta-de-credito-en-colombia'></a>", unsafe_allow_html=True)
            st.subheader(f"Gasto de los viajeros de {pais_elegido} con tarjeta de crédito en Colombia")
            
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

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto promedio de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_side_by_side_bar_gasto_promedio, fuente=credibanco_fuente, llave='graph_27')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_credibanco['gasto_promedio'].empty:
                        streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto promedio', llave='boton_25', unidad=pais_elegido, df=df_credibanco['gasto_promedio'][['Año', 'Facturación (USD)', 'Viajeros', 'Transacciones', 'Gasto promedio tarjeta (USD)', 'Gasto promedio transacción (USD)']])
                
                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):
                    
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto por categoria de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_credibanco, fuente=credibanco_fuente, llave='graph_28')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_categoria'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por categoria', llave='boton_26', unidad=pais_elegido, df=df_credibanco['gasto_categoria'])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_credibanco, fuente=credibanco_fuente, llave='graph_29')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_categoria'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por categoria', llave='boton_27', unidad=pais_elegido, df=df_credibanco['gasto_categoria'])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):
                    
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto por producto directo de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, llave='graph_30')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_producto_directo'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por productos - Directo', llave='boton_28', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, llave='graph_31')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_producto_directo'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por productos - Directo', llave='boton_29', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])

                # Contenedor con la estructura: Gráfico único a la izquierda y a la derecha
                with st.container(height = 625):
                    
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Gasto por producto indirecto de los viajeros de {pais_elegido}</h6>', unsafe_allow_html=True)
                    # Gráfico único a la izquierda y a la derecha
                    col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="top") 

                    # Columna 1
                    with col1:
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, llave='graph_32')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_producto_indirecto'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por producto - Indirecto', llave='boton_30', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])
                    with col2: 
                        # Contenedor con el gráfico
                        with st.container(height = 500, border=True):
                            # Gráfico
                            streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, llave='graph_33')
                        # Botón de descarga fuera del contenedor y dentro de la columna
                        if not df_credibanco['gasto_producto_indirecto'].empty:
                            streamlit_analitica.boton_descarga(fuente=credibanco_fuente, variable='Gasto por producto - Indirecto', llave='boton_31', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'][['Año', 'Categoria', 'Facturación (USD)', 'Total Anual (USD)', 'Participación (%)']])

                        
            #####################################
            # Reservas y Búsquedas hacia Colombia
            #####################################
            st.markdown("<a id='reservas-y-busquedas-hacia-colombia'></a>", unsafe_allow_html=True)
            st.subheader(f"Reservas y Búsquedas de {pais_elegido} hacia Colombia")

            # Obtener gráficos FK Colombia
            (
                fig_single_time_series_reservas_colombia,
                fig_single_time_series_busquedas_colombia
            ) = streamlit_analitica.obtener_graficos_fk_colombia(df_fk, pais_elegido)

            # Expander
            with st.expander("Explora todos los indicadores"):

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Reservas aéreas activas de {pais_elegido} hacia Colombia</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_reservas_colombia, fuente=fk_fuente, llave='graph_34')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_fk['reservas_serie_tiempo_colombia'].empty:
                        streamlit_analitica.boton_descarga(fuente=fk_fuente, variable='Reservas aéreas activas del país hacia Colombia', llave='boton_32', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo_colombia'][['País', 'Fecha', 'Reservas']])

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Búsquedas aéreas activas de {pais_elegido} hacia hacia Colombia</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_busquedas_colombia, fuente=fk_fuente, llave='graph_35')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_fk['busquedas_serie_tiempo_colombia'].empty:
                        streamlit_analitica.boton_descarga(fuente=fk_fuente, variable='Búsquedas activas del país hacia Colombia', llave='boton_33', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo_colombia'][['País', 'Fecha', 'Búsquedas']])    
                
            ###########################################
            # Agencias que venden Colombia como destino
            ###########################################
            st.markdown("<a id='agencias-que-venden-colombia-como-destino'></a>", unsafe_allow_html=True)
            st.subheader(f"Agencias de {pais_elegido} que venden Colombia como destino")

            # Fuente
            iata_fuente = 'IATA-GAP'
            iata_nota = 'Datos de 2024 actualizados al tercer trimestre'

            # Obtener gráficos IATA-GAP
            (
                fig_single_time_series_agencias_colombia, 
                fig_stacked_h_agencias_ciudades
            ) = streamlit_analitica.obtener_graficos_iata_colombia(df_iata, pais_elegido)

            # Expander
            with st.expander("Explora todos los indicadores"):

                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Agencias de {pais_elegido} que venden Colombia como destino</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_agencias_colombia, fuente=iata_fuente, llave='graph_36')
                        st.caption(f'Nota: {iata_nota}')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_iata['agencias_serie_tiempo'].empty:
                        streamlit_analitica.boton_descarga(fuente=iata_fuente, variable='Indicadores de agencias de ese mercado que venden Colombia como destino', llave='boton_34', unidad=pais_elegido, df=df_iata['agencias_serie_tiempo'][['Año', 'Número de Agencias']])
                
                # Contenedor con la estructura: Gráfico único en el centro
                with st.container(height = 625, border=True):
                    # Título
                    st.markdown(f'<h6 class="custom-header" style="text-align:center;">Agencias de {pais_elegido} que venden Colombia como destino por ciudad de la agencia</h6>', unsafe_allow_html=True)
                    # Contenedor con el gráfico
                    with st.container(height = 500, border=True):
                        # Gráfico
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_agencias_ciudades, fuente=iata_fuente, llave='graph_37')
                        st.caption(f'Nota: {iata_nota}')
                    # Botón de descarga fuera del contenedor del gráfico
                    if not df_iata['agencias_ciudades'].empty:
                        streamlit_analitica.boton_descarga(fuente=iata_fuente, variable='Agencias que venden Colombia como destino por ciudad de la agencia', llave='boton_35', unidad=pais_elegido, df=df_iata['agencias_ciudades'][['Año', 'Ciudad de la Agencia', 'Número de Agencias', 'Total Anual', 'Participación (%)']])


            ########################################
            # Salida de colombianos hacia el mercado
            ########################################
            st.divider()
            st.markdown(f'### **Salida de colombianos hacia {pais_elegido}**')
            st.markdown("<a id='salida-de-colombianos-hacia-el-mercado'></a>", unsafe_allow_html=True)
            st.subheader(f"Salida de colombianos hacia {pais_elegido}")

            # Expander
            with st.expander("Explora todos los indicadores"):
                st.write("Contenido")

# Agregar footer
streamlit_analitica.footer()