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
    
    # Registrar evento
    snowflake_analitica.registrar_evento(sesion_activa= st.session_state.session, tipo_evento = 'Selección de continente', detalle_evento = 'Selección de continente', unidad = region_elegida)


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

        # Obtener datos del país elegido por el usuario
        df_global_data, df_oag, df_fk, df_credibanco, df_iata = streamlit_analitica.obtener_datos_con_cache(_pais_elegido=pais_elegido)

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


        #############################
        # Contenido de bases de datos
        #############################

        ###################################################
        # Indicadores de turismo del mercado hacia el mundo
        ###################################################
        st.markdown(f"### **Indicadores de turismo de {pais_elegido} hacia el mundo**")
        
        # Flujos de viajeros hacia el mundo
        st.markdown("<a id='flujos-de-viajeros-hacia-el-mundo'></a>", unsafe_allow_html=True)
        st.subheader("Flujos de viajeros hacia el mundo")

        # Fuente
        global_data_fuente = 'GlobalData'

        # Serie de tiempo de viajeros
        fig_time_series_viajeros = plotly_analitica.plot_single_time_series(df=df_global_data['viajeros_serie_tiempo'], date_col='YEAR', value_col='VIAJEROS', title="Flujo de viajeros hacia el mundo", x_label="Año", y_label="Viajeros", y_units=None, show_labels=True, decimal_places=0)

        # Viajeros por medio de transporte
        # Stacked H
        fig_stacked_h_medio_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['viajeros_medio'], date_col='YEAR', group_col='MEDIO', share_col='PARTICIPACION', decimal_places=1, title='Flujo de viajeros hacia el mundo por medio de transporte', y_label=' Año', legend_title=" ")
        fig_stacked_h_medio_viajeros = fig_stacked_h_medio_viajeros.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_medio_viajeros = plotly_analitica.plot_treemap(df = df_global_data['viajeros_medio'], date_col="YEAR", value_col="VIAJEROS", group_col="MEDIO", share_col="PARTICIPACION",decimal_places=1, title="Flujo de viajeros hacia el mundo por medio de transporte", group_label="Medio", value_label="Viajeros", share_label="Participación (%)")

        # Serie de tiempo noches de pernoctación
        fig_time_series_noches_percnotacion = plotly_analitica.plot_single_time_series(df=df_global_data['noches_pernoctacion'], date_col='YEAR', value_col='NOCHES', title="Noches de percnotación promedio", x_label="Año", y_label="Noches promedio", y_units=None, show_labels=True, decimal_places=0)

        # Serie de tiempo gasto
        fig_time_series_gasto = plotly_analitica.plot_single_time_series(df=df_global_data['gasto_serie_tiempo'], date_col='YEAR', value_col='GASTO', title="Gasto promedio del viajero al mundo", x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0)

        # Gasto por categoria
        # Stacked H
        fig_stacked_h_categoria_gasto = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['gasto_categoria'], date_col='YEAR', group_col='CATEGORIA_GASTO', share_col='PARTICIPACION', decimal_places=1, title='Gasto promedio del viajero al mundo por categoria', y_label=' Año', legend_title=" ")
        fig_stacked_h_categoria_gasto = fig_stacked_h_categoria_gasto.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_categoria_gasto = plotly_analitica.plot_treemap(df = df_global_data['gasto_categoria'], date_col="YEAR", value_col="GASTO", group_col="CATEGORIA_GASTO", share_col="PARTICIPACION", decimal_places=1, title="Gasto promedio del viajero al mundo por categoria", group_label="Categoria", value_label="Gasto", share_label="Participación (%)")

        # Viajeros por edad
        # Stacked H
        fig_stacked_h_edad_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['rango_edad'], date_col='YEAR', group_col='RANGO_EDAD', share_col='PARTICIPACION', decimal_places=1, title='Rango de edad del viajero promedio', y_label=' Año', legend_title=" ")
        fig_stacked_h_edad_viajeros = fig_stacked_h_edad_viajeros.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_edad_viajeros = plotly_analitica.plot_treemap(df = df_global_data['rango_edad'], date_col="YEAR", value_col="VIAJEROS", group_col="RANGO_EDAD", share_col="PARTICIPACION", decimal_places=1, title="Rango de edad del viajero promedio", group_label="Rango de edad", value_label="Viajeros", share_label="Participación (%)")

        # Motivo viaje
        # Stacked H
        fig_stacked_h_motivo_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['motivo_viaje'], date_col='YEAR', group_col='MOTIVO_VIAJE', share_col='PARTICIPACION', decimal_places=1, title='Motivo de viaje del viajero', y_label=' Año', legend_title=" ")
        fig_stacked_h_motivo_viajeros = fig_stacked_h_motivo_viajeros.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_motivo_viajeros = plotly_analitica.plot_treemap(df = df_global_data['motivo_viaje'], date_col="YEAR", value_col="VIAJEROS", group_col="MOTIVO_VIAJE", share_col="PARTICIPACION", decimal_places=1, title="Motivo de viaje del viajero", group_label="Motivo", value_label="Viajeros", share_label="Participación (%)")

        # Forma de viaje
        # Stacked H
        fig_stacked_h_forma_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['forma_viaje'], date_col='YEAR', group_col='FORMA_VIAJE', share_col='PARTICIPACION', decimal_places=1, title='Forma de viaje del viajero', y_label=' Año', legend_title=" ")
        fig_stacked_h_forma_viajeros = fig_stacked_h_forma_viajeros.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_forma_viajeros = plotly_analitica.plot_treemap(df = df_global_data['forma_viaje'], date_col="YEAR", value_col="VIAJEROS", group_col="FORMA_VIAJE", share_col="PARTICIPACION", decimal_places=1, title="Forma de viaje del viajero", group_label="Forma", value_label="Viajeros", share_label="Participación (%)") 

        # Destinos
        # Stacked H
        fig_stacked_h_destinos_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['destinos_internacionales_top5'], date_col='YEAR', group_col='PAIS_DESTINO', share_col='PARTICIPACION', decimal_places=1, title='Principales destinos internacionales', y_label=' Año', legend_title=" ")
        fig_stacked_h_destinos_viajeros = fig_stacked_h_destinos_viajeros.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        # Treemap
        fig_treemap_destinos_viajeros = plotly_analitica.plot_treemap(df = df_global_data['destinos_internacionales_top5'], date_col="YEAR", value_col="VIAJEROS", group_col="PAIS_DESTINO", share_col="PARTICIPACION", decimal_places=1, title="Principales destinos internacionales", group_label="País", value_label="Viajeros", share_label="Participación (%)")

        # Serie de tiempo de Reuniones, incentivos, congresos y exposiciones (MICE)
        df_mice = df_global_data['flujos_negocios'][df_global_data['flujos_negocios']['MOTIVO_VIAJE']=='Reuniones, incentivos, congresos y exposiciones (MICE)']
        fig_time_series_mice = plotly_analitica.plot_single_time_series(df=df_mice, date_col='YEAR', value_col='VIAJEROS', title="Flujos internacionales por motivo de negocios", x_label="Año", y_label="Viajeros", y_units=None, show_labels=True, decimal_places=0)

        # Expander
        with st.expander("Datos"):

            # GRÁFICO ÚNICO A LA IZQUIERDA Y BOTONES DE CAMBIO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Serie de tiempo de viajeros
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Serie de tiempo de viajeros', unidad=pais_elegido, df=df_global_data['viajeros_serie_tiempo'])
                # Columna 2: Viajeros por medio de transporte
                with col2:
                    # Crear botones para cambiar entre gráficos
                    tab1, tab2 = st.tabs(["Stacked Barchart", f"Treemap Barchart año"])
                    # Botón 1
                    with tab1:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_medio_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por medio de transporte - StackedH', unidad=pais_elegido, df=df_global_data['viajeros_medio'])
                    # Botón 2
                    with tab2:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_medio_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por medio de transporte - Treemap', unidad=pais_elegido, df=df_global_data['viajeros_medio'])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Serie de tiempo noches de pernoctación
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_noches_percnotacion, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Noches Percnotación', unidad=pais_elegido, df=df_global_data['noches_pernoctacion'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y BOTONES DE CAMBIO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Serie de tiempo gasto
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Serie de tiempo gasto', unidad=pais_elegido, df=df_global_data['gasto_serie_tiempo'])               
                # Columna 2: Gasto por categoria
                with col2:
                    # Crear botones para cambiar entre gráficos
                    tab1, tab2 = st.tabs(["Stacked Barchart", f"Treemap Barchart año"])
                    # Botón 1
                    with tab1:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_categoria_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Gasto por categoria - StackedH', unidad=pais_elegido, df=df_global_data['gasto_categoria'])
                    # Botón 2
                    with tab2:
                        streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_categoria_gasto, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Gasto por categoria - Treemap', unidad=pais_elegido, df=df_global_data['gasto_categoria'])
            
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Viajeros por edad
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_edad_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por edad - StackedH', unidad=pais_elegido, df=df_global_data['rango_edad'])
                # Columna 2: Viajeros por edad
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_edad_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Viajeros por edad - Treemap', unidad=pais_elegido, df=df_global_data['rango_edad'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Motivo viaje
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_motivo_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Motivo viaje - StackedH', unidad=pais_elegido, df=df_global_data['motivo_viaje'])
                # Columna 2: Motivo viaje
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_motivo_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Motivo viaje - Treemap', unidad=pais_elegido, df=df_global_data['motivo_viaje'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Forma de viaje
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_forma_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Forma de viaje - StackedH', unidad=pais_elegido, df=df_global_data['forma_viaje'])
                # Columna 2: Forma de viaje
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_forma_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Forma de viaje - Treemap', unidad=pais_elegido, df=df_global_data['forma_viaje'])
                    
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Destinos
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_destinos_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Destinos - StackedH', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'])
                # Columna 2: Destinos
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_destinos_viajeros, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - Destinos - Treemap', unidad=pais_elegido, df=df_global_data['destinos_internacionales_top5'])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # MICE
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_time_series_mice, fuente=global_data_fuente, detalle_evento='Descarga Excel Global Data - MICE', unidad=pais_elegido, df=df_mice)

        # Conectividad con el mundo
        st.markdown("<a id='conectividad-con-el-mundo'></a>", unsafe_allow_html=True)
        st.subheader("Conectividad con el mundo")

        # Fuente
        oag_fuente = 'OAG'

        # Single Bar Chart: Conectividad del país con el mundo: Sillas
        fig_single_barchart_conectividad_mundo_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='YEAR', value_col='SILLAS', title='Conectividad del país con el mundo: Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

        # Single Bar Chart Conectividad del país con el mundo: Frencuencias 
        fig_single_barchart_conectividad_mundo_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='YEAR', value_col='FRECUENCIAS', title='Conectividad del país con el mundo: Frencuencias', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)
        
        # StackedH: Conectividad mundo cerrado destinos: Frecuencias
        fig_stacked_h_conectividad_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_cerrado'], date_col='FECHA', group_col='PAIS_ARRIVAL', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo cerrado destinos: Frecuencias', y_label=' Año', legend_title=" ")
        fig_stacked_h_conectividad_frecuencias_destinos_cerrado = fig_stacked_h_conectividad_frecuencias_destinos_cerrado.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # StackedH: Conectividad mundo corrido destinos: Frecuencias
        fig_stacked_h_conectividad_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_corrido'], date_col='FECHA_CORRIDA', group_col='PAIS_ARRIVAL', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo corrido destinos: Frecuencias', y_label=' Año', legend_title=" ")
        fig_stacked_h_conectividad_frecuencias_destinos_corrido = fig_stacked_h_conectividad_frecuencias_destinos_corrido.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})
        
        # Expander
        with st.expander("Datos"):

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con el mundo: Sillas
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_sillas, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con el mundo: Sillas', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'])
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con el mundo: Frencuencias 
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_mundo_frecuencias, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con el mundo: Frencuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_serie_tiempo'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Conectividad mundo cerrado destinos: Frecuencias
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_cerrado, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad mundo cerrado destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_cerrado'])
                # Columna 2: StackedH: Conectividad mundo corrido destinos: Frecuencias
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_frecuencias_destinos_corrido, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - StackedH: Conectividad mundo corrido destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_mundo_destino_corrido'])

        # Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile
        st.markdown("<a id='reservas-y-busquedas-hacia-mexico-costa-rica-peru-y-chile'></a>", unsafe_allow_html=True)
        st.subheader("Reservas y Búsquedas hacia México, Costa Rica, Perú y Chile")

        # Fuente
        fk_fuente = 'ForwardKeys'

        # Reservas activas del país hacia México, Costa Rica, Perú y Chile
        fig_multiple_time_series_reservas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['reservas_serie_tiempo'], date_col='FLIGHT_LEG_ARRIVAL_MONTH_YEAR', value_col='RESERVAS', group_col='PAIS_ARRIVAL', title='Reservas aéreas activas del país hacia México, Costa Rica, Perú y Chile', x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

        # Búsquedas activas del país hacia México, Costa Rica, Perú y Chile
        fig_multiple_time_series_busquedas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['busquedas_serie_tiempo'], date_col='SEARCH_DATE_MONTH_YEAR', value_col='BUSQUEDAS', group_col='PAIS_ARRIVAL', title='Búsquedas aéreas activas del país hacia México, Costa Rica, Perú y Chile', x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)
    
        # Expander
        with st.expander("Datos"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Reservas activas del país hacia México, Costa Riva, Perú y Chile
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_reservas_mundo, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Reservas activas del país hacia México, Costa Rica, Perú y Chile', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo'])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Búsquedas activas del país hacia México, Costa Rica, Perú y Chile
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_multiple_time_series_busquedas_mundo, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Búsquedas activas del país hacia México, Costa Rica, Perú y Chile', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo'])

        ###################################################
        # Indicadores de turismo del mercado hacia Colombia
        ###################################################
        st.markdown(f'### **Indicadores de turismo de {pais_elegido} hacia Colombia**')

        # Flujos de viajeros hacia Colombia
        st.markdown("<a id='flujos-de-viajeros-hacia-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Flujos de viajeros hacia Colombia")

        # Expander
        with st.expander("Datos"):
            st.write("Contenido")

        # Conectividad con Colombia
        st.markdown("<a id='conectividad-con-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Conectividad con Colombia")


        # Single Bar Chart: Conectividad del país con Colombia: Sillas
        fig_single_barchart_conectividad_colombia_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='YEAR', value_col='SILLAS', title='Conectividad del país con Colombia: Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

        # Single Bar Chart Conectividad del país con Colombia: Frencuencias 
        fig_single_barchart_conectividad_colombia_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='YEAR', value_col='FRECUENCIAS', title='Conectividad del país con Colombia: Frencuencias', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)
        
        # StackedH: Conectividad Colombia cerrado destinos: Frecuencias
        fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_cerrado'], date_col='FECHA', group_col='MUNICIPIO_DANE', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo cerrado municipios: Frecuencias', y_label=' Año', legend_title=" ")
        fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado = fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # StackedH: Conectividad Colombia corrido destinos: Frecuencias
        fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_corrido'], date_col='FECHA_CORRIDA', group_col='MUNICIPIO_DANE', share_col='PARTICIPACION_FRECUENCIAS', decimal_places=1, title='Conectividad mundo corrido municipios: Frecuencias', y_label=' Año', legend_title=" ")
        fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido = fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # Expander
        with st.expander("Datos"):

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con Colombia: Sillas
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_sillas, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con Colombia: Sillas', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'])
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Conectividad del país con Colombia: Frencuencias
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_barchart_conectividad_colombia_frecuencias, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad del país con Colombia: Frencuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_serie_tiempo'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1: Conectividad Colombia cerrado destinos: Frecuencias
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad Colombia cerrado destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_cerrado'])
                # Columna 2: StackedH: Conectividad Colombia corrido destinos: Frecuencias
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido, fuente=oag_fuente, detalle_evento='Descarga Excel OAG - Conectividad Colombia corrido destinos: Frecuencias', unidad=pais_elegido, df=df_oag['conectividad_colombia_municipio_corrido'])


        # Gasto con tarjeta de crédito en Colombia
        st.markdown("<a id='gasto-con-tarjeta-de-credito-en-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Gasto con tarjeta de crédito en Colombia")
        
        # Fuente
        credibanco_fuente = 'Credibanco'

        # Gasto promedio
        fig_side_by_side_bar_gasto_promedio =  plotly_analitica.plot_side_by_side_bars(df=df_credibanco['gasto_promedio'], date_col='YEAR', var1_col='GASTO_PROMEDIO_TARJETA', var2_col='GASTO_PROMEDIO_TRANSACCION', title='Gasto promedio', x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0, legend_title=' ', legend_labels={'GASTO_PROMEDIO_TARJETA' : 'Gasto promedio por tarjeta', 'GASTO_PROMEDIO_TRANSACCION' : 'Gasto promedio por transacción'})

        # Gasto por categoria
        fig_stacked_h_gasto_categoria_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_categoria'], date_col='YEAR', group_col='CLASIFICACION_CATEGORIA_FORMATADA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por categoria', y_label='Año', legend_title=" ")
        fig_stacked_h_gasto_categoria_credibanco = fig_stacked_h_gasto_categoria_credibanco.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # Gasto por categoria
        fig_treemap_gasto_categoria_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_categoria'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CLASIFICACION_CATEGORIA_FORMATADA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por categoria", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

        # Gasto por productos directo
        fig_stacked_h_gasto_categoria_directo_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_directo'], date_col='YEAR', group_col='CATEGORIA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por producto directo', y_label='Año', legend_title=" ")
        fig_stacked_h_gasto_categoria_directo_credibanco = fig_stacked_h_gasto_categoria_directo_credibanco.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # Gasto por producto directo
        fig_treemap_gasto_categoria_directo_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_directo'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CATEGORIA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por producto directo", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

        # Gasto por productos indirecto
        fig_stacked_h_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_indirecto'], date_col='YEAR', group_col='CATEGORIA', share_col='PARTICIPACION', decimal_places=1, title='Gasto por producto indirecto', y_label='Año', legend_title=" ")
        fig_stacked_h_gasto_categoria_indirecto_credibanco = fig_stacked_h_gasto_categoria_indirecto_credibanco.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # Gasto por producto indirecto
        fig_treemap_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_indirecto'], date_col="YEAR", value_col="FACTURACION_USD", group_col="CATEGORIA", share_col="PARTICIPACION", decimal_places=1, title="Gasto por producto indirecto", group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

        # Expander
        with st.expander("Datos"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Gasto promedio
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_side_by_side_bar_gasto_promedio, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Gasto promedio', unidad=pais_elegido, df=df_credibanco['gasto_promedio'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por categoria
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por categoria', unidad=pais_elegido, df=df_credibanco['gasto_categoria'])
                
                # Columna 2 Gasto por categoria
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por categoria', unidad=pais_elegido, df=df_credibanco['gasto_categoria'])

            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por productos directo
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por productos directo', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'])
                
                # Columna 2 Gasto por productos directo
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_directo_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por productos directo', unidad=pais_elegido, df=df_credibanco['gasto_producto_directo'])

                    
            # GRÁFICO ÚNICO A LA IZQUIERDA Y ÚNICO A LA DERECHA
            with st.container():
                # Creación de tablas
                col1, col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                # Columna 1 Gasto por productos indirecto
                with col1:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - StackedH - Gasto por productos indirecto', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'])
                 
                # Columna 2 Gasto por productos indirecto
                with col2:
                    streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_treemap_gasto_categoria_indirecto_credibanco, fuente=credibanco_fuente, detalle_evento='Descarga Excel Credibanco - Treemap - Gasto por productos indirecto', unidad=pais_elegido, df=df_credibanco['gasto_producto_indirecto'])          


        # Reservas y Búsquedas hacia Colombia
        st.markdown("<a id='reservas-y-busquedas-hacia-colombia'></a>", unsafe_allow_html=True)
        st.subheader("Reservas y Búsquedas hacia Colombia")

        # Reservas aéreas activas del país hacia Colombia
        fig_single_time_series_reservas_colombia = plotly_analitica.plot_single_time_series(df=df_fk['reservas_serie_tiempo_colombia'], date_col='FLIGHT_LEG_ARRIVAL_MONTH_YEAR', value_col='RESERVAS', title="Reservas aéreas activas del país hacia Colombia", x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, mensual=True)

        # Búsquedas activas del país hacia Colombia 
        fig_single_time_series_busquedas_colombia = plotly_analitica.plot_single_time_series(df=df_fk['busquedas_serie_tiempo_colombia'], date_col='SEARCH_DATE_MONTH_YEAR', value_col='BUSQUEDAS', title="Búsquedas aéreas activas del país hacia hacia Colombia", x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, mensual=True)

        # Expander
        with st.expander("Datos"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Reservas aéreas activas del país hacia Colombia
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_reservas_colombia, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Reservas aéreas activas del país hacia Colombia', unidad=pais_elegido, df=df_fk['reservas_serie_tiempo_colombia'])
                

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Búsquedas activas del país hacia Colombia 
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_busquedas_colombia, fuente=fk_fuente, detalle_evento='Descarga Excel FK - Búsquedas activas del país hacia Colombia', unidad=pais_elegido, df=df_fk['busquedas_serie_tiempo_colombia'])

        # Agencias que venden Colombia como destino
        st.markdown("<a id='agencias-que-venden-colombia-como-destino'></a>", unsafe_allow_html=True)
        st.subheader("Agencias que venden Colombia como destino")

        # Fuente
        iata_fuente = 'IATA-GAP'

        # Indicadores de agencias de ese mercado que venden Colombia como destino mostrar por q
        fig_single_time_series_agencias_colombia = plotly_analitica.plot_single_time_series(df=df_iata['agencias_serie_tiempo'], date_col='YEAR', value_col='AGENCIAS', title="Agencias que venden Colombia como destino", x_label="Año", y_label="Agencias", y_units='Número', show_labels=True, decimal_places=0)

        # Agencias que venden Colombia como destino por ciudad de la agencia 15
        fig_stacked_h_agencias_ciudades = plotly_analitica.plot_stacked_bar_chart_h(df=df_iata['agencias_ciudades'], date_col='YEAR', group_col='TRAVEL_AGENCY_CITY', share_col='PARTICIPACION', decimal_places=1, title='Agencias que venden Colombia como destino por ciudad de la agencia', y_label=' Año', legend_title=" ")
        fig_stacked_h_agencias_ciudades = fig_stacked_h_agencias_ciudades.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        # Expander
        with st.expander("Datos"):
            
            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Indicadores de agencias de ese mercado que venden Colombia como destino mostrar por q
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_single_time_series_agencias_colombia, fuente=iata_fuente, detalle_evento='Descarga Excel IATAGAP - Indicadores de agencias de ese mercado que venden Colombia como destino', unidad=pais_elegido, df=df_iata['agencias_ciudades'])

            # GRÁFICO ÚNICO EN EL CENTRO
            with st.container():
                # Agencias que venden Colombia como destino por ciudad de la agencia
                streamlit_analitica.mostrar_resultado_en_streamlit(resultado=fig_stacked_h_agencias_ciudades, fuente=iata_fuente, detalle_evento='Descarga Excel IATAGAP - Agencias que venden Colombia como destino por ciudad de la agencia', unidad=pais_elegido, df=df_iata['agencias_ciudades'])

        ########################################
        # Salida de colombianos hacia el mercado
        ########################################
        st.markdown(f'### **Salida de colombianos hacia {pais_elegido}**')
        st.markdown("<a id='salida-de-colombianos-hacia-el-mercado'></a>", unsafe_allow_html=True)
        st.subheader("Salida de colombianos hacia el mercado")

        # Expander
        with st.expander("Datos"):
            st.write("Contenido")




