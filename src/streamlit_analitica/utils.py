# Librerias
import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import requests
import plotly.graph_objects as go
from io import BytesIO
import src.snowflake_analitica as snowflake_analitica
import src.datos_citi as procesamiento_datos
import src.plotly_analitica as plotly_analitica
from openpyxl.utils import get_column_letter
import src.streamlit_analitica.helpers as helpers

# Función para obtener los datos
def obtener_datos(_pais_elegido):
    """
    1. Verifica si ya están los datos en 'st.session_state' para el país elegido.
    2. Si no existen o son de un país distinto, ejecuta la operación costosa.
    3. Guarda los resultados en st.session_state.
    4. Devuelve los DataFrames directamente desde session_state.
    """
    
    # Si aún no se ha cargado nada o se cambió de país
    if 'datos_cargados' not in st.session_state or st.session_state['datos_cargados']['pais'] != _pais_elegido:

        with st.spinner("Cargando datos..."):
            # Barra de progreso y realiza la lógica pesada
            progress_bar = st.progress(0)

            # Llamadas a procesamiento_datos

            # Global Data
            df_global_data = procesamiento_datos.datos_global_data(_pais_elegido, st.session_state.session)
            progress_bar.progress(20)

            # OAG
            df_oag = procesamiento_datos.datos_oag(_pais_elegido, st.session_state.session)
            progress_bar.progress(40)

            # Forward Keys
            df_fk = procesamiento_datos.datos_forward_keys(_pais_elegido, st.session_state.session)
            progress_bar.progress(60)

            # Credibanco
            df_credibanco = procesamiento_datos.datos_credibanco(_pais_elegido, st.session_state.session)
            progress_bar.progress(80)

            # IATA GAP
            df_iata = procesamiento_datos.datos_iata_gap(_pais_elegido, st.session_state.session)
            progress_bar.progress(100)

            # Se guardan los datos y el país de referencia en session_state
            st.session_state['datos_cargados'] = {
                'pais': _pais_elegido,
                'df_global_data': df_global_data,
                'df_oag': df_oag,
                'df_fk': df_fk,
                'df_credibanco': df_credibanco,
                'df_iata': df_iata
            }

            # Retorna los mismos DataFrames desde la sesión
            return (
                st.session_state['datos_cargados']['df_global_data'],
                st.session_state['datos_cargados']['df_oag'],
                st.session_state['datos_cargados']['df_fk'],
                st.session_state['datos_cargados']['df_credibanco'],
                st.session_state['datos_cargados']['df_iata']
            )
    
    else:
         # Si ya están cargados, se devuelven directamente
        return (
            st.session_state['datos_cargados']['df_global_data'],
            st.session_state['datos_cargados']['df_oag'],
            st.session_state['datos_cargados']['df_fk'],
            st.session_state['datos_cargados']['df_credibanco'],
            st.session_state['datos_cargados']['df_iata']
        )

# Función para obtener los gráficos de Global Data

def obtener_graficos_global_data(df_global_data, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de GlobalData 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_global_data : dict de DataFrames
        Estructura que contiene los DataFrames de GlobalData (p. ej. 'viajeros_medio', 'gasto_categoria', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_global_data' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:

            # Serie de tiempo de viajeros
            fig_time_series_viajeros = plotly_analitica.plot_single_time_series(df=df_global_data['viajeros_serie_tiempo'], date_col='Año', value_col='Viajeros', x_label="Año", y_label="Viajeros (miles)", y_units=None, show_labels=True, decimal_places=0)

            # Viajeros por medio de transporte
            # Stacked H
            fig_stacked_h_medio_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['viajeros_medio'], date_col='Año', group_col='Medio de transporte', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_medio_viajeros = plotly_analitica.plot_treemap(df = df_global_data['viajeros_medio'], date_col="Año", value_col="Viajeros", group_col="Medio de transporte", share_col="Participación (%)",decimal_places=1, group_label="Medio", value_label="Viajeros (miles)", share_label="Participación (%)")

            # Serie de tiempo noches de pernoctación
            fig_time_series_noches_percnotacion = plotly_analitica.plot_single_time_series(df=df_global_data['noches_pernoctacion'], date_col='Año', value_col='Noches de percnotación', x_label="Año", y_label="Noches promedio", y_units=None, show_labels=True, decimal_places=0)

            # Serie de tiempo gasto
            fig_time_series_gasto = plotly_analitica.plot_single_time_series(df=df_global_data['gasto_serie_tiempo'], date_col='Año', value_col='Gasto (USD)', x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0)

            # Gasto por categoria
            # Stacked H
            fig_stacked_h_categoria_gasto = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['gasto_categoria'], date_col='Año', group_col='Categoria de Gasto', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_categoria_gasto = plotly_analitica.plot_treemap(df = df_global_data['gasto_categoria'], date_col="Año", value_col="Gasto (USD)", group_col="Categoria de Gasto", share_col="Participación (%)", decimal_places=1, group_label="Categoria", value_label="Gasto (USD)", share_label="Participación (%)")

            # Viajeros por edad
            # Stacked H
            fig_stacked_h_edad_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['rango_edad'], date_col='Año', group_col='Rango de Edad', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_edad_viajeros = plotly_analitica.plot_treemap(df = df_global_data['rango_edad'], date_col="Año", value_col="Viajeros", group_col="Rango de Edad", share_col="Participación (%)", decimal_places=1, group_label="Rango de edad", value_label="Viajeros (miles)", share_label="Participación (%)")

            # Motivo viaje
            # Stacked H
            fig_stacked_h_motivo_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['motivo_viaje'], date_col='Año', group_col='Motivo de Viaje', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_motivo_viajeros = plotly_analitica.plot_treemap(df = df_global_data['motivo_viaje'], date_col="Año", value_col="Viajeros", group_col="Motivo de Viaje", share_col="Participación (%)", decimal_places=1, group_label="Motivo", value_label="Viajeros (miles)", share_label="Participación (%)")

            # Forma de viaje
            # Stacked H
            fig_stacked_h_forma_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['forma_viaje'], date_col='Año', group_col='Forma de Viaje', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_forma_viajeros = plotly_analitica.plot_treemap(df = df_global_data['forma_viaje'], date_col="Año", value_col="Viajeros", group_col="Forma de Viaje", share_col="Participación (%)", decimal_places=1, group_label="Forma", value_label="Viajeros (miles)", share_label="Participación (%)") 

            # Destinos
            # Stacked H
            fig_stacked_h_destinos_viajeros = plotly_analitica.plot_stacked_bar_chart_h(df=df_global_data['destinos_internacionales_top5'], date_col='Año', group_col='País Destino', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")
            # Treemap
            fig_treemap_destinos_viajeros = plotly_analitica.plot_treemap(df = df_global_data['destinos_internacionales_top5'], date_col="Año", value_col="Viajeros", group_col="País Destino", share_col="Participación (%)", decimal_places=1, group_label="País", value_label="Viajeros (miles)", share_label="Participación (%)")

            # Serie de tiempo de Reuniones, incentivos, congresos y exposiciones (MICE)
            if 'Motivo de viaje' in df_global_data['flujos_negocios'].columns:
                df_mice = df_global_data['flujos_negocios'][df_global_data['flujos_negocios']['Motivo de viaje'] == 'Reuniones, incentivos, congresos y exposiciones (MICE)']
            else:
                df_mice = pd.DataFrame()
            fig_time_series_mice = plotly_analitica.plot_single_time_series(df=df_mice, date_col='Año', value_col='Viajeros', x_label="Año", y_label="Viajeros (miles)", y_units=None, show_labels=True, decimal_places=0)

            # Guardar todo en session_state
            st.session_state['graficos_global_data'] = {
                'fig_time_series_viajeros': fig_time_series_viajeros,
                'fig_stacked_h_medio_viajeros': fig_stacked_h_medio_viajeros,
                'fig_treemap_medio_viajeros': fig_treemap_medio_viajeros,
                'fig_time_series_noches_percnotacion': fig_time_series_noches_percnotacion,
                'fig_time_series_gasto': fig_time_series_gasto,
                'fig_stacked_h_categoria_gasto': fig_stacked_h_categoria_gasto,
                'fig_treemap_categoria_gasto': fig_treemap_categoria_gasto,
                'fig_stacked_h_edad_viajeros': fig_stacked_h_edad_viajeros,
                'fig_treemap_edad_viajeros': fig_treemap_edad_viajeros,
                'fig_stacked_h_motivo_viajeros': fig_stacked_h_motivo_viajeros,
                'fig_treemap_motivo_viajeros': fig_treemap_motivo_viajeros,
                'fig_stacked_h_forma_viajeros': fig_stacked_h_forma_viajeros,
                'fig_treemap_forma_viajeros': fig_treemap_forma_viajeros,
                'fig_stacked_h_destinos_viajeros': fig_stacked_h_destinos_viajeros,
                'fig_treemap_destinos_viajeros': fig_treemap_destinos_viajeros,
                'fig_time_series_mice': fig_time_series_mice
            }

            # Return de los gráficos 
            return (   
                    st.session_state['graficos_global_data']['fig_time_series_viajeros'],
                    st.session_state['graficos_global_data']['fig_stacked_h_medio_viajeros'],
                    st.session_state['graficos_global_data']['fig_treemap_medio_viajeros'],
                    st.session_state['graficos_global_data']['fig_time_series_noches_percnotacion'],
                    st.session_state['graficos_global_data']['fig_time_series_gasto'],
                    st.session_state['graficos_global_data']['fig_stacked_h_categoria_gasto'],
                    st.session_state['graficos_global_data']['fig_treemap_categoria_gasto'],
                    st.session_state['graficos_global_data']['fig_stacked_h_edad_viajeros'],
                    st.session_state['graficos_global_data']['fig_treemap_edad_viajeros'],
                    st.session_state['graficos_global_data']['fig_stacked_h_motivo_viajeros'],
                    st.session_state['graficos_global_data']['fig_treemap_motivo_viajeros'],
                    st.session_state['graficos_global_data']['fig_stacked_h_forma_viajeros'],
                    st.session_state['graficos_global_data']['fig_treemap_forma_viajeros'],
                    st.session_state['graficos_global_data']['fig_stacked_h_destinos_viajeros'],
                    st.session_state['graficos_global_data']['fig_treemap_destinos_viajeros'],
                    st.session_state['graficos_global_data']['fig_time_series_mice']                                           
                )
    else:
         # Si ya están cargados, se devuelven directamente
        return(
            st.session_state['graficos_global_data']['fig_time_series_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_medio_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_medio_viajeros'],
            st.session_state['graficos_global_data']['fig_time_series_noches_percnotacion'],
            st.session_state['graficos_global_data']['fig_time_series_gasto'],
            st.session_state['graficos_global_data']['fig_stacked_h_categoria_gasto'],
            st.session_state['graficos_global_data']['fig_treemap_categoria_gasto'],
            st.session_state['graficos_global_data']['fig_stacked_h_edad_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_edad_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_motivo_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_motivo_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_forma_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_forma_viajeros'],
            st.session_state['graficos_global_data']['fig_stacked_h_destinos_viajeros'],
            st.session_state['graficos_global_data']['fig_treemap_destinos_viajeros'],
            st.session_state['graficos_global_data']['fig_time_series_mice']

        )

# Función para obtener los gráficos de OAG de conectividad con el mundo
def obtener_graficos_oag_mundo(df_oag, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de OAG Mundo 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_oag : dict de DataFrames
        Estructura que contiene los DataFrames de OAG (p. ej. 'frecuencias', 'sillas gasto_categoria', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_oag_mundo' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
        
            # Single Bar Chart: Conectividad del país con el mundo: Sillas
            fig_single_barchart_conectividad_mundo_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='Año', value_col='Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

            # Single Bar Chart Conectividad del país con el mundo: Frencuencias 
            fig_single_barchart_conectividad_mundo_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_mundo_serie_tiempo'], date_col='Año', value_col='Frecuencias', x_label="Año", y_label="Frecuencias", y_units=None, show_labels=True, decimal_places=0)
            
            # StackedH: Conectividad mundo cerrado destinos: Frecuencias
            fig_stacked_h_conectividad_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_cerrado'], date_col='Año', group_col='País Destino', share_col='Participación Frecuencias (%)', decimal_places=1, y_label=' Año', legend_title=" ")

            # StackedH: Conectividad mundo corrido destinos: Frecuencias
            fig_stacked_h_conectividad_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_mundo_destino_corrido'], date_col='Periodo', group_col='País Destino', share_col='Participación Frecuencias (%)', decimal_places=1, y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_oag_mundo'] = {
                'fig_single_barchart_conectividad_mundo_sillas': fig_single_barchart_conectividad_mundo_sillas,
                'fig_single_barchart_conectividad_mundo_frecuencias': fig_single_barchart_conectividad_mundo_frecuencias,
                'fig_stacked_h_conectividad_frecuencias_destinos_cerrado': fig_stacked_h_conectividad_frecuencias_destinos_cerrado,
                'fig_stacked_h_conectividad_frecuencias_destinos_corrido': fig_stacked_h_conectividad_frecuencias_destinos_corrido
            }

            # Return de los gráficos 
            return (
                    st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_sillas'],
                    st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_frecuencias'],
                    st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_cerrado'],
                    st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_corrido']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_sillas'],
            st.session_state['graficos_oag_mundo']['fig_single_barchart_conectividad_mundo_frecuencias'],
            st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_cerrado'],
            st.session_state['graficos_oag_mundo']['fig_stacked_h_conectividad_frecuencias_destinos_corrido']
         )

# Función para obtener los gráficos de Forward Keys de búsquedas y reservas con el mundo

def obtener_graficos_fk_mundo(df_fk, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Forward Keys Mundo 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_fk : dict de DataFrames
        Estructura que contiene los DataFrames de FK (p. ej. 'reservas', 'búsquedas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_fk_mundo' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
        
            # Reservas activas del país hacia México, Costa Rica, Perú y Chile
            fig_multiple_time_series_reservas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['reservas_serie_tiempo'], date_col='Fecha', value_col='Reservas', group_col='País', x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

            # Búsquedas activas del país hacia México, Costa Rica, Perú y Chile
            fig_multiple_time_series_busquedas_mundo = plotly_analitica.plot_multiple_time_series(df=df_fk['busquedas_serie_tiempo'], date_col='Fecha', value_col='Búsquedas', group_col='País', x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

            # Guardar todo en session_state
            st.session_state['graficos_fk_mundo'] = {
                'fig_multiple_time_series_reservas_mundo': fig_multiple_time_series_reservas_mundo,
                'fig_multiple_time_series_busquedas_mundo': fig_multiple_time_series_busquedas_mundo
            }

            # Return de los gráficos
            return (
                    st.session_state['graficos_fk_mundo']['fig_multiple_time_series_reservas_mundo'],
                    st.session_state['graficos_fk_mundo']['fig_multiple_time_series_busquedas_mundo']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_fk_mundo']['fig_multiple_time_series_reservas_mundo'],
            st.session_state['graficos_fk_mundo']['fig_multiple_time_series_busquedas_mundo']
         )

# Función para obtener los gráficos de OAG de conetividad con Colombia

def obtener_graficos_oag_colombia(df_oag, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de OAG Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_oag : dict de DataFrames
        Estructura que contiene los DataFrames de OAG (p. ej. 'frecuencias', 'sillas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_oag_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
    
            # Single Bar Chart: Conectividad del país con Colombia: Sillas
            fig_single_barchart_conectividad_colombia_sillas = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='Año', value_col='Sillas', x_label="Año", y_label="Sillas", y_units=None, show_labels=True, decimal_places=0)

            # Single Bar Chart Conectividad del país con Colombia: Frencuencias 
            fig_single_barchart_conectividad_colombia_frecuencias = plotly_analitica.plot_single_bar_chart(df=df_oag['conectividad_colombia_serie_tiempo'], date_col='Año', value_col='Frecuencias', x_label="Año", y_label="Frecuencias", y_units=None, show_labels=True, decimal_places=0)
            
            # StackedH: Conectividad Colombia cerrado destinos: Frecuencias
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_cerrado'], date_col='Año', group_col='Municipio Destino', share_col='Participación Frecuencias (%)', decimal_places=1, y_label=' Año', legend_title=" ")

            # StackedH: Conectividad Colombia corrido destinos: Frecuencias
            fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido = plotly_analitica.plot_stacked_bar_chart_h(df=df_oag['conectividad_colombia_municipio_corrido'], date_col='Periodo', group_col='Municipio Destino', share_col='Participación Frecuencias (%)', decimal_places=1, y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_oag_colombia'] = {
                'fig_single_barchart_conectividad_colombia_sillas': fig_single_barchart_conectividad_colombia_sillas,
                'fig_single_barchart_conectividad_colombia_frecuencias': fig_single_barchart_conectividad_colombia_frecuencias,
                'fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado': fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado,
                'fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido': fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido
            }
    
            # Return de los gráficos en un diccionario
            return (
                    st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_sillas'],
                    st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_frecuencias'],
                    st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado'],
                    st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_sillas'],
            st.session_state['graficos_oag_colombia']['fig_single_barchart_conectividad_colombia_frecuencias'],
            st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_cerrado'],
            st.session_state['graficos_oag_colombia']['fig_stacked_h_conectividad_colombia_frecuencias_destinos_corrido']
         )


# Función para obtener los gráficos de Credibanco

def obtener_graficos_credibanco(df_credibanco, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Credibanco 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_credibanco : dict de DataFrames
        Estructura que contiene los DataFrames de Credibanco (p. ej. 'gasto', 'tarjetas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_credibanco' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
    
            # Gasto promedio
            fig_side_by_side_bar_gasto_promedio =  plotly_analitica.plot_side_by_side_bars(df=df_credibanco['gasto_promedio'], date_col='Año', var1_col='Gasto promedio tarjeta (USD)', var2_col='Gasto promedio transacción (USD)', x_label="Año", y_label="Gasto", y_units='USD', show_labels=True, decimal_places=0, legend_title=' ', legend_labels={'Gasto promedio tarjeta (USD)' : 'Gasto promedio por tarjeta', 'Gasto promedio transacción (USD)' : 'Gasto promedio por transacción'})

            # Gasto por categoria
            fig_stacked_h_gasto_categoria_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_categoria'], date_col='Año', group_col='Clasificación', share_col='Participación (%)', decimal_places=1, y_label='Año', legend_title=" ")

            # Gasto por categoria
            fig_treemap_gasto_categoria_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_categoria'], date_col="Año", value_col="Facturación (USD)", group_col="Clasificación", share_col="Participación (%)", decimal_places=1, group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Gasto por productos directo
            fig_stacked_h_gasto_categoria_directo_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_directo'], date_col='Año', group_col='Categoria', share_col='Participación (%)', decimal_places=1, y_label='Año', legend_title=" ")

            # Gasto por producto directo
            fig_treemap_gasto_categoria_directo_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_directo'], date_col="Año", value_col="Facturación (USD)", group_col="Categoria", share_col="Participación (%)", decimal_places=1, group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Gasto por productos indirecto
            fig_stacked_h_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_stacked_bar_chart_h(df=df_credibanco['gasto_producto_indirecto'], date_col='Año', group_col='Categoria', share_col='Participación (%)', decimal_places=1, y_label='Año', legend_title=" ")

            # Gasto por producto indirecto
            fig_treemap_gasto_categoria_indirecto_credibanco = plotly_analitica.plot_treemap(df = df_credibanco['gasto_producto_indirecto'], date_col="Año", value_col="Facturación (USD)", group_col="Categoria", share_col="Participación (%)", decimal_places=1, group_label="Categoría", value_label="Facturación USD", share_label="Participación (%)")

            # Guardar todo en session_state
            st.session_state['graficos_credibanco'] = {
                'fig_side_by_side_bar_gasto_promedio': fig_side_by_side_bar_gasto_promedio,
                'fig_stacked_h_gasto_categoria_credibanco': fig_stacked_h_gasto_categoria_credibanco,
                'fig_treemap_gasto_categoria_credibanco': fig_treemap_gasto_categoria_credibanco,
                'fig_stacked_h_gasto_categoria_directo_credibanco': fig_stacked_h_gasto_categoria_directo_credibanco,
                'fig_treemap_gasto_categoria_directo_credibanco': fig_treemap_gasto_categoria_directo_credibanco,
                'fig_stacked_h_gasto_categoria_indirecto_credibanco': fig_stacked_h_gasto_categoria_indirecto_credibanco,
                'fig_treemap_gasto_categoria_indirecto_credibanco': fig_treemap_gasto_categoria_indirecto_credibanco
            }

            # Return de los gráficos en un diccionario
            return (
                    st.session_state['graficos_credibanco']['fig_side_by_side_bar_gasto_promedio'],
                    st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_credibanco'],
                    st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_credibanco'],
                    st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_directo_credibanco'],
                    st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_directo_credibanco'],
                    st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_indirecto_credibanco'],
                    st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_indirecto_credibanco']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_credibanco']['fig_side_by_side_bar_gasto_promedio'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_credibanco'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_directo_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_directo_credibanco'],
            st.session_state['graficos_credibanco']['fig_stacked_h_gasto_categoria_indirecto_credibanco'],
            st.session_state['graficos_credibanco']['fig_treemap_gasto_categoria_indirecto_credibanco']
         )

# Función para obtener los gráficos de Forward Keys de búsquedas y reservas con Colombia

def obtener_graficos_fk_colombia(df_fk, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de Forward Keys Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_fk : dict de DataFrames
        Estructura que contiene los DataFrames de FK (p. ej. 'reservas', 'búsquedas', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_fk_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:
         
            # Reservas aéreas activas del país hacia Colombia
            fig_single_time_series_reservas_colombia = plotly_analitica.plot_multiple_time_series(df=df_fk['reservas_serie_tiempo_colombia'], date_col='Fecha', value_col='Reservas', group_col='País', x_label="Año", y_label="Reservas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

            # Búsquedas activas del país hacia Colombia 
            fig_single_time_series_busquedas_colombia = plotly_analitica.plot_multiple_time_series(df=df_fk['busquedas_serie_tiempo_colombia'], date_col='Fecha', value_col='Búsquedas', group_col='País', x_label="Año", y_label="Búsquedas", y_units=None, show_labels=True, decimal_places=0, legend_title=None)

             # Guardar todo en session_state
            st.session_state['graficos_fk_colombia'] = {
                'fig_single_time_series_reservas_colombia': fig_single_time_series_reservas_colombia,
                'fig_single_time_series_busquedas_colombia': fig_single_time_series_busquedas_colombia
            }

            # Return de los gráficos en un diccionario
            return (
                    st.session_state['graficos_fk_colombia']['fig_single_time_series_reservas_colombia'],
                    st.session_state['graficos_fk_colombia']['fig_single_time_series_busquedas_colombia']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_fk_colombia']['fig_single_time_series_reservas_colombia'],
            st.session_state['graficos_fk_colombia']['fig_single_time_series_busquedas_colombia']
         )

# Funciones para obtener los gráficos de IATA GAP de agencias que promocionan Colombia

def obtener_graficos_iata_colombia(df_iata, _pais_elegido):
      
    """
    Comprueba en 'st.session_state' si ya están almacenados los gráficos de IATA GAP Colombia 
    para el país actual. Si no existen o el país cambió, se generan de nuevo y se guardan 
    en session_state. Devuelve un diccionario con todos los objetos de gráfico.

    Parámetros:
    -----------
    df_iata : dict de DataFrames
        Estructura que contiene los DataFrames de IATA (p. ej. 'agencias', 'ciudades', etc.).
    _pais_elegido : str
        Nombre o ISO del país elegido (para saber cuándo cambiar los gráficos).
    """

    # Verificar si ya existen gráficos en session_state y son del mismo país
    if 'graficos_iata_colombia' not in st.session_state \
       or st.session_state['datos_cargados'].get('pais') != _pais_elegido:

            # Indicadores de agencias de ese mercado que venden Colombia como destino mostrar por q
            fig_single_time_series_agencias_colombia = plotly_analitica.plot_single_time_series(df=df_iata['agencias_serie_tiempo'], date_col='Año', value_col='Número de Agencias', x_label="Año", y_label="Agencias", y_units='Número', show_labels=True, decimal_places=0)

            # Agencias que venden Colombia como destino por ciudad de la agencia 15
            fig_stacked_h_agencias_ciudades = plotly_analitica.plot_stacked_bar_chart_h(df=df_iata['agencias_ciudades'], date_col='Año', group_col='Ciudad de la Agencia', share_col='Participación (%)', decimal_places=1, y_label=' Año', legend_title=" ")

            # Guardar todo en session_state
            st.session_state['graficos_iata_colombia'] = {
                'fig_single_time_series_agencias_colombia': fig_single_time_series_agencias_colombia,
                'fig_stacked_h_agencias_ciudades': fig_stacked_h_agencias_ciudades
            }

            # Return de los gráficos en un diccionario
            return (
                    st.session_state['graficos_iata_colombia']['fig_single_time_series_agencias_colombia'],
                    st.session_state['graficos_iata_colombia']['fig_stacked_h_agencias_ciudades']
                )
    else:
         # Si ya están cargados, se devuelven directamente
         return(
            st.session_state['graficos_iata_colombia']['fig_single_time_series_agencias_colombia'],
            st.session_state['graficos_iata_colombia']['fig_stacked_h_agencias_ciudades']
         )
    
# Función para generar la tabla de resumen
def generar_tabla_resumen(pais_elegido, df_global_data, df_oag, df_credibanco, df_iata, year_global_data, year_oag_mundo, year_oag_colombia, year_credibanco, year_iata):

    """
    Esta función construye una tabla resumen con diversos indicadores de turismo para un país dado. 
    Cada indicador se extrae de diferentes DataFrames que están contenidos en diccionarios, 
    y luego se formatea de manera adecuada para mostrar el valor de forma legible.
    
    Parámetros
    ----------
    pais_elegido : str
        Nombre del país que se va a filtrar en los indicadores.

    df_global_data : dict
        Diccionario que contiene DataFrames relacionados con indicadores globales, 
        por ejemplo: flujos de viajeros, noches de pernoctación y gasto promedio.

    df_oag : dict
        Diccionario que contiene DataFrames relacionados con la conectividad aérea (OAG).

    df_credibanco : dict
        Diccionario que contiene DataFrames con la información de gasto con tarjeta de crédito.

    df_iata : dict
        Diccionario que contiene DataFrames con la información de agencias de viajes (IATA).

    year_global_data : int
        Año que se quiere consultar para los DataFrames de df_global_data.

    year_oag_mundo : int
        Año que se quiere consultar para la conectividad del país con el mundo.

    year_oag_colombia : int
        Año que se quiere consultar para la conectividad del país hacia Colombia.

    year_credibanco : int
        Año que se quiere consultar para la información de gasto con tarjeta de crédito en Colombia.

    year_iata : int
        Año que se quiere consultar para la información de agencias de viajes (IATA).

    Retorna
    -------
    pd.DataFrame
        Un DataFrame con dos columnas:
        - "Indicador": Descripción de cada indicador consultado.
        - "Valor": Valor formateado correspondiente a cada indicador (por ejemplo, miles de viajeros, USD, etc.).
    
    Ejemplo de uso
    --------------
    >>> df_resumen = generar_tabla_resumen(
    ...     "Brasil", 
    ...     df_global_data=datos_globales, 
    ...     df_oag=datos_oag, 
    ...     df_credibanco=datos_credibanco, 
    ...     df_iata=datos_iata, 
    ...     year_global_data=2022, 
    ...     year_oag_mundo=2022, 
    ...     year_oag_colombia=2022, 
    ...     year_credibanco=2022, 
    ...     year_iata=2022
    ... )
    ... print(df_resumen)
    """

    # Crear df vacío
    df_tabla_resumen = pd.DataFrame()

    # Flujos de viajeros hacia el mundo
    df_flujos_viajeros_mundo = df_global_data.get('viajeros_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_flujos_viajeros_mundo.empty:

        # Volver diccionario
        dict_flujos_viajeros_mundo = df_flujos_viajeros_mundo.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_flujos_viajeros_mundo.get(year_global_data, {})

        # Extraer val
        val = sub_dict.get('Viajeros', 0)

        # Agregar formato
        val_flujos_viajeros_mundo =  helpers.formato_miles(valor=val, decimales=0) + ' miles de viajeros'

        # Crear fila
        row_resumen_flujos_viajeros_mundo = pd.DataFrame({
            'Indicador': [f'Flujos de viajeros de {pais_elegido} hacia el mundo en {year_global_data}'],
            'Valor': [val_flujos_viajeros_mundo],
            'Fuente' : 'GlobalData'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_resumen_flujos_viajeros_mundo])

    # Noches de percnotacion promedio
    df_noches_percnotacion = df_global_data.get('noches_pernoctacion', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_noches_percnotacion.empty:

        # Volver diccionario
        dict_noches_percnotacion = df_noches_percnotacion.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_noches_percnotacion.get(year_global_data, {})

        # Extraer val
        val = sub_dict.get('Noches de percnotación', 0)

        # Agregar formato
        val_noches_percnotacion =  helpers.formato_miles(valor=val, decimales=0) + ' noches'

        # Crear fila
        row_noches_percnotacion = pd.DataFrame({
            'Indicador': [f'Noches de percnotación promedio de los viajeros de {pais_elegido} en {year_global_data}'],
            'Valor': [val_noches_percnotacion],
            'Fuente' : 'GlobalData'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_noches_percnotacion])

    # Gasto promedio del viajero al mundo
    df_gasto_promedio_viajero_mundo = df_global_data.get('gasto_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_promedio_viajero_mundo.empty:

        # Volver diccionario
        dict_gasto_promedio_viajero_mundo = df_gasto_promedio_viajero_mundo.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_gasto_promedio_viajero_mundo.get(year_global_data, {})

        # Extraer val
        val = sub_dict.get('Gasto (USD)', 0)

        # Agregar formato
        val_gasto_promedio_viajero_mundo =  helpers.formato_miles(valor=val, decimales=0) + ' USD'

        # Crear fila
        row_gasto_promedio_viajero_mundo = pd.DataFrame({
            'Indicador': [f'Gasto promedio del viajero de {pais_elegido} al mundo en {year_global_data}'],
            'Valor': [val_gasto_promedio_viajero_mundo],
            'Fuente' : 'GlobalData'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_gasto_promedio_viajero_mundo])

    # Conectividad del país con el mundo
    df_conectividad_mundo = df_oag.get('conectividad_mundo_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_conectividad_mundo.empty:

        # Volver diccionario
        dict_conectividad_mundo = df_conectividad_mundo.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_conectividad_mundo.get(year_oag_mundo, {})

        # Extraer val
        val = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_conectividad_mundo =  helpers.formato_miles(valor=val, decimales=0) + ' frecuencias'

        # Crear fila
        row_conectividad_mundo = pd.DataFrame({
            'Indicador': [f'Conectividad de {pais_elegido} con el mundo en {year_oag_mundo}'],
            'Valor': [val_conectividad_mundo],
            'Fuente' : 'OAG'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_conectividad_mundo])

    # Conectividad aérea hacia Colombia
    df_conectividad_colombia = df_oag.get('conectividad_colombia_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_conectividad_colombia.empty:

        # Volver diccionario
        dict_conectividad_colombia = df_conectividad_colombia.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_conectividad_colombia.get(year_oag_colombia, {})

        # Extraer val
        val = sub_dict.get('Frecuencias', 0)

        # Agregar formato
        val_conectividad_colombia =  helpers.formato_miles(valor=val, decimales=0) + ' frecuencias'

        # Crear fila
        row_conectividad_colombia = pd.DataFrame({
            'Indicador': [f'Conectividad aérea de {pais_elegido} hacia Colombia en {year_oag_colombia}'],
            'Valor': [val_conectividad_colombia],
            'Fuente' : 'OAG'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_conectividad_colombia])

    # Gasto con tarjeta de crédito proveniente de ese país en Colombia
    df_gasto_tarjeta_crédito_colombia = df_credibanco.get('gasto_promedio', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_gasto_tarjeta_crédito_colombia.empty:

        # Volver diccionario
        dict_gasto_tarjeta_crédito_colombia = df_gasto_tarjeta_crédito_colombia.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_gasto_tarjeta_crédito_colombia.get(year_credibanco, {})

        # Extraer val
        val = sub_dict.get('Gasto promedio tarjeta (USD)', 0)

        # Agregar formato
        val_gasto_tarjeta_crédito_colombia =  helpers.formato_miles(valor=val, decimales=1) + ' USD'

        # Crear fila
        row_gasto_tarjeta_crédito_colombia = pd.DataFrame({
            'Indicador': [f'Gasto promedio con tarjeta de crédito de los viajeros de {pais_elegido} en Colombia en {year_credibanco}'],
            'Valor': [val_gasto_tarjeta_crédito_colombia],
            'Fuente' : 'Credibanco'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_gasto_tarjeta_crédito_colombia])

    # Indicadores de agencias de ese mercado que venden Colombia como destino
    df_indicadores_agencias_colombia_destino = df_iata.get('agencias_serie_tiempo', pd.DataFrame())

    # Procesar si no llegan vacíos
    if not df_indicadores_agencias_colombia_destino.empty:

        # Volver diccionario
        dict_indicadores_agencias_colombia_destino = df_indicadores_agencias_colombia_destino.set_index('Año').T.to_dict()

        # Extraer subdiccionario
        sub_dict = dict_indicadores_agencias_colombia_destino.get(year_iata, {})

        # Extraer val
        val = sub_dict.get('Número de Agencias', 0)

        # Agregar formato
        val_indicadores_agencias_colombia_destino =  helpers.formato_miles(valor=val, decimales=0) + ' agencias'

        # Crear fila
        row_indicadores_agencias_colombia_destino = pd.DataFrame({
            'Indicador': [f'Agencias de {pais_elegido} que venden Colombia como destino en {year_iata}'],
            'Valor': [val_indicadores_agencias_colombia_destino],
            'Fuente' : 'IATA-GAP'
        })

        # Agregar fila a la tabla resumen
        df_tabla_resumen = pd.concat([df_tabla_resumen, row_indicadores_agencias_colombia_destino])

    # Resultado
    return df_tabla_resumen

# Función para Limpiar caches de dfs y gráficos al momento de elegir otro país
def on_selectbox_change():
    
    # Limpia la caché de datos
    st.cache_data.clear()

    # Limpia la clave 'datos_cargados' (si existe) en session_state
    if 'datos_cargados' in st.session_state:
        del st.session_state['datos_cargados']
    # Limpia la clave 'graficos_global_data' (si existe) en session_state
    if 'graficos_global_data' in st.session_state:
        del st.session_state['graficos_global_data']
    # Limpia la clave 'graficos_oag_mundo' (si existe) en session_state
    if 'graficos_oag_mundo' in st.session_state:
        del st.session_state['graficos_oag_mundo']
    # Limpia la clave 'graficos_fk_mundo' (si existe) en session_state
    if 'graficos_fk_mundo' in st.session_state:
        del st.session_state['graficos_fk_mundo']
    # Limpia la clave 'graficos_oag_colombia' (si existe) en session_state
    if 'graficos_oag_colombia' in st.session_state:
        del st.session_state['graficos_oag_colombia']
    # Limpia la clave 'graficos_credibanco' (si existe) en session_state
    if 'graficos_credibanco' in st.session_state:
        del st.session_state['graficos_credibanco']
    # Limpia la clave 'graficos_fk_colombia' (si existe) en session_state
    if 'graficos_fk_colombia' in st.session_state:
        del st.session_state['graficos_fk_colombia']
    # Limpia la clave 'graficos_iata_colombia' (si existe) en session_state
    if 'graficos_iata_colombia' in st.session_state:
        del st.session_state['graficos_iata_colombia']


def mostrar_resultado_en_streamlit(resultado, fuente, llave):

    # Caso 1: Gráfico de Plotly
    if isinstance(resultado, go.Figure):
        st.plotly_chart(resultado, use_container_width=True, key=llave)
        st.caption(f'Fuente: {fuente}')

    # Caso 2: Cadena de texto
    elif isinstance(resultado, str):
        st.write(resultado)
        st.caption(f'Fuente: {fuente}')

    # Caso 3: Tipo no soportado
    else:
        st.warning(f"Tipo de resultado no reconocido o no soportado: {type(resultado)}")


def excel_download_buttons(df: pd.DataFrame, fuente: str = 'CITI', variable: str = 'Turismo') -> BytesIO:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        # Crear la hoja
        workbook = writer.book
        worksheet = workbook.add_worksheet("Datos")
        writer.sheets["Datos"] = worksheet

        # Formatos
        bold_format = workbook.add_format({'bold': True})
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',  # Gris claro
            'border': 1
        })
        data_format = workbook.add_format({'border': 1})

        # Metadatos en filas superiores
        worksheet.write("A1", "Centro de Inteligencia de Turismo Internacional (CITI)", bold_format)
        worksheet.write("A2", f"Fuente: {fuente}", bold_format)
        worksheet.write("A3", f"Variable: {variable}", bold_format)

        # Cabeceras y datos
        n_rows, n_cols = df.shape

        # Escribir las cabeceras en la fila 4 (fila 5 “visual”)
        for j, col in enumerate(df.columns):
            max_len = max(len(str(col)), df[col].astype(str).map(len).max() if not df[col].empty else 0)
            worksheet.set_column(j, j, max_len + 2)  # Ajustar ancho
            worksheet.write(4, j, col, header_format)

        # Escribir los datos a partir de la fila 5 (fila 6 “visual”)
        for i in range(n_rows):
            for j in range(n_cols):
                worksheet.write(i + 5, j, df.iloc[i, j], data_format)

        writer.close()
    buffer.seek(0)
    return buffer


@st.fragment
def boton_descarga(fuente, variable, llave, unidad, df=None):
    
    # Verificar si el DataFrame no es None y no está vacío
    if isinstance(df, pd.DataFrame) and not df.empty:
        st.download_button(
            label="Descargar Excel",
            data=excel_download_buttons(df = df,
                                        fuente=fuente,
                                        variable = variable),
            file_name=f"Datos CITI - {fuente} - {unidad} - {variable}.xlsx",
            mime="application/vnd.ms-excel",
            use_container_width=True,
            on_click=snowflake_analitica.registrar_evento,
            args=(st.session_state.session, 'Descarga archivo Excel', f"Datos CITI - {fuente} - {unidad} - {variable}.xlsx", unidad),
            key=llave
        )

    # Caso 3: Tipo no soportado
    else:
        st.write("No hay datos diposnibles para descargar.")



# Geolocalización
def mostrar_mapa(pais):
    """
    Función que, a partir del código ISO de un país (ej. 'CO', 'US', 'MX'),
    obtiene sus coordenadas y muestra un mapa con una capa de datos
    generados aleatoriamente alrededor de la ubicación del país.

    Parámetros:
    -----------
    pais : str
        Código ISO del país (2 o 3 letras). Ejemplo: 'CO' para Colombia.

    Acciones:
    ---------
    1. Hace una solicitud HTTP a la API de restcountries.com para obtener
       la información del país.
    2. Extrae la latitud y longitud del país.
    3. Genera datos de ejemplo de latitudes y longitudes aleatorias
       distribuidas alrededor de la ubicación del país.
    4. Renderiza un mapa interactivo con pydeck usando un HexagonLayer.

    Manejador de errores:
    ---------------------
    - Si ocurre algún problema de red o con la respuesta, se muestra un
      mensaje de error a través de st.error().
    - Si el país no existe o la respuesta no incluye coordenadas válidas,
      se muestra un mensaje de error.
    - Si ocurre cualquier otra excepción, se notifica al usuario indicando
      el tipo de error.

    Uso:
    ----
    Dentro de tu aplicación Streamlit, simplemente llama a:

        mostrar_mapa("CO")

    para mostrar el mapa del país correspondiente.
    """
    try:
        # Construcción de la URL de la API
        api_url = f"https://restcountries.com/v3.1/alpha/{pais.upper()}"
        
        # Solicitud GET
        response = requests.get(api_url)
        response.raise_for_status()  # Lanza un error si la solicitud falla

        # Transformar la respuesta en JSON
        country_data = response.json()
        if not country_data or not isinstance(country_data, list):
            st.error(f"No se encontraron datos para el país '{pais}'. Verifica el código ISO.")
            return

        # Se asume que la primera posición del arreglo tiene la info
        latlng = country_data[0].get('latlng', [None, None])
        if latlng[0] is None or latlng[1] is None:
            st.error(f"No se obtuvieron coordenadas válidas para el país '{pais}'.")
            return

        lat, lon = latlng[0], latlng[1]

        # Crear datos de ejemplo alrededor de la posición (lat, lon)
        chart_data = pd.DataFrame(
            np.random.randn(1000, 2) / [50, 50] + [lat, lon],
            columns=['lat', 'lon']
        )

        # Mostrar el mapa con pydeck
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/outdoors-v11",
            initial_view_state=pdk.ViewState(
                latitude=lat,
                longitude=lon,
                zoom=4,
                pitch=0
            ),
            layers=[
                pdk.Layer(
                    'HexagonLayer',
                    data=chart_data,
                    get_position='[lon, lat]',
                    radius=2,
                    elevation_scale=4,
                    elevation_range=[0, 0],
                    pickable=True,
                    extruded=True
                )
            ],
        ))

    except requests.exceptions.RequestException as req_err:
        st.error(f"Error en la solicitud a la API: {req_err}")
    except (IndexError, KeyError, TypeError) as parse_err:
        st.error(f"No se pudieron extraer correctamente los datos del país '{pais}': {parse_err}")
    except Exception as e:
        st.error(f"Ha ocurrido un error inesperado al generar el mapa de '{pais}': {e}")


