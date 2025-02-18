# Librerías
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import locale

# Single time series
def plot_single_time_series(df, date_col, value_col, title=None, 
                            x_label="", y_label="", y_units=None, 
                            show_labels=False, decimal_places=0,
                            mensual = False):
    """
    Genera un gráfico de línea de una sola serie de tiempo usando Plotly Express.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str): Nombre de la columna que contiene las fechas.
        value_col (str): Nombre de la columna que contiene los valores de la serie de tiempo.
        title (str): Título del gráfico. Por defecto es None.
        x_label (str): Etiqueta del eje X. Por defecto es "".
        y_label (str): Etiqueta del eje Y. Por defecto es "".
        y_units (str): Unidades para agregar a la etiqueta del eje Y. Si es None, no se muestra.
        show_labels (bool): Si es True, muestra las etiquetas de los valores sobre la línea.
        decimal_places (int): Número de decimales para formatear los valores.
        mensual(bool): Si es true, la columna de date se vuelve formato datetime y en español.

    Retorna:
        plotly.graph_objs.Figure: Gráfico de línea generado.
        str: Mensaje de error si no hay datos o se produce una excepción.

    Ejemplo de uso:
        fig = plot_single_time_series(df, "date", "sales", title="Ventas Mensuales", 
                                      x_label="Mes", y_label="Ventas", y_units="USD", 
                                      show_labels=True, decimal_places=2)
    """
    try:
        # Verificar si el DataFrame está vacío
        if df.empty:
            return "No hay datos disponibles."
        
        # Cambiar el gráfico para manejar meses en español:
        if mensual == True:   
            # Establecer la configuración regional para español (España)
            locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Puedes cambiar 'es_ES.UTF-8' según tu entorno
            # Asegurarse de que la columna de fecha esté en formato datetime
            df[date_col] = pd.to_datetime(df[date_col])
         
        # Construir etiquetas dinámicas para los ejes
        y_label_text = (f"{y_label} ({y_units})" if y_units else y_label) if y_label else ""
        x_label_text = x_label if x_label else ""

        # Crear el gráfico de línea con etiquetas opcionales
        fig = px.line(
            df, 
            x=date_col, 
            y=value_col, 
            labels={
                date_col: x_label_text if x_label_text else None,
                value_col: y_label_text if y_label_text else None
            },
            text=value_col if show_labels else None  # Mostrar etiquetas si show_labels es True
        )

        # Configurar el diseño del gráfico
        fig.update_layout(
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            xaxis_title=x_label_text if x_label_text else None,
            yaxis_title=y_label_text if y_label_text else None,
            template="plotly_white"  # Estilo limpio
        )

        # Configurar el formato del eje Y con separadores de miles y decimales
        tick_format = ".0f"  # Crear el formato con decimales especificados
        fig.update_yaxes(tickformat=tick_format)  # Aplicar formato al eje Y

        # Configurar las etiquetas si están habilitadas
        if show_labels:
            # Formatear las etiquetas con separadores de miles y decimales en estilo español
            fig.update_traces(
                text=df[value_col].apply(lambda x: f"{x:,.{decimal_places}f}"
                                         .replace(",", "X")
                                         .replace(".", ",")
                                         .replace("X", ".")),
                textposition="top center"  # Posicionar las etiquetas sobre la línea
            )

        return fig

    except Exception as e:
        # Manejo de excepciones y retorno de un mensaje de error
        return f"Error generando el gráfico: {e}"

# Multiple time series
def plot_multiple_time_series(df, date_col, value_col, group_col,
                               title=None, x_label="", y_label="", y_units=None,
                               show_labels=False, decimal_places=0, legend_title=None):
    """
    Genera un gráfico de líneas para varias series de tiempo, ofreciendo un menú (dropdown) para filtrar qué series se ven en el gráfico.
   
    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str): Nombre de la columna que contiene las fechas.
        value_col (str): Nombre de la columna que contiene los valores de las series de tiempo.
        group_col (str): Nombre de la columna que define las agrupaciones (series múltiples).
        title (str): Título del gráfico.
        x_label (str): Etiqueta del eje X.
        y_label (str): Etiqueta del eje Y.
        y_units (str): Unidades para agregar a la etiqueta del eje Y.
        show_labels (bool): Si es True, muestra etiquetas de valores sobre las líneas.
        decimal_places (int): Número de decimales para formatear los valores.
        legend_title (str): Encabezado de la leyenda.
 
    Retorna:
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.
    """
    try:
        if df.empty:
            return "No hay datos disponibles."
 
        # Establecer la configuración regional para español (España)
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Puedes cambiar 'es_ES.UTF-8' según tu entorno
 
        # Asegurarse de que la columna de fecha esté en formato datetime
        df[date_col] = pd.to_datetime(df[date_col])
 
        # Pivotear a formato wide
        df_pivoted = df.pivot(index=date_col, columns=group_col, values=value_col)
 
        # Ejes
        y_label_text = f"{y_label} ({y_units})" if (y_label and y_units) else y_label
        x_label_text = x_label if x_label else ""
 
        # Crear la figura base
        fig = go.Figure()
 
        # Para cada serie (columna), creamos un trace separado
        all_series = df_pivoted.columns.tolist()
        for col in all_series:
            y_values = df_pivoted[col]
           
            # Si show_labels=True, se formatean los valores de acuerdo con el estilo español.
            if show_labels:
                text_values = []
                for val in y_values:
                    if pd.notnull(val):
                        val_str = f"{val:,.{decimal_places}f}"  
                        val_str = (val_str
                                   .replace(",", "X")   # "1X234.56"
                                   .replace(".", ",")   # "1X234,56"
                                   .replace("X", "."))  # "1.234,56"
                        text_values.append(val_str)
                    else:
                        text_values.append(None)
            else:
                text_values = None
 
            fig.add_trace(
                go.Scatter(
                    x=df_pivoted.index,
                    y=y_values,
                    mode='lines+markers+text' if show_labels else 'lines+markers',
                    text=text_values,
                    textposition='top center',
                    name=col  # El nombre del trace se corresponde con la serie
                )
            )
 
        # Formatear el eje X directamente en el gráfico con el formato de mes en español
        fig.update_layout(
            template="plotly_white",
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            xaxis_title=x_label_text,
            yaxis_title=y_label_text,
            xaxis=dict(
                tickmode='array',
                tickvals=df_pivoted.index,  # Usar el índice (mes-año) para los ticks
                ticktext=[date.strftime('%B-%Y') for date in df_pivoted.index]  # Formato Mes-Año en español
            ),
            # Leyenda en la parte inferior
            legend=dict(
                orientation="h",    # Leyenda horizontal
                y=-0.2,             # Ubicación debajo del gráfico
                x=0.5,
                xanchor="center",
                yanchor="top",
                title_text=legend_title if legend_title else " "
            )
        )
 
        # Formato del eje Y
        tick_format = f".{decimal_places}f"
        fig.update_yaxes(tickformat=tick_format)
 
        # Construir un dropdown que muestre/oculte cada serie
        updatemenus = [
            dict(
                type="dropdown",
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.1,
                yanchor="top",
                buttons=[
                    {
                        "label": "Mostrar todas",
                        "method": "update",
                        "args": [{"visible": [True]*len(all_series)}]
                    }
                ] + [
                    {
                        "label": serie,
                        "method": "update",
                        "args": [
                            {
                                "visible": [i == idx for i in range(len(all_series))]
                            }
                        ]
                    }
                    for idx, serie in enumerate(all_series)
                ]
            )
        ]
 
        fig.update_layout(updatemenus=updatemenus)
 
        return fig
 
    except Exception as e:
        return f"Error generando el gráfico: {e}"
    
# Stacked bar chart Horizontal
def plot_stacked_bar_chart_h(df, date_col, group_col, share_col, 
                            decimal_places=0, title=None, y_label=None, legend_title=None):
    """
    Genera un gráfico de barras apiladas horizontales, donde cada barra 
    representa un valor de la columna `date_col` (por ejemplo, años) y 
    está subdividida según los porcentajes de cada categoría definida en 
    `group_col`.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str):  Nombre de la columna que contiene los valores de fecha o año que se mostrarán en el eje Y.
        group_col (str): Nombre de la columna que define las categorías que se apilan en cada barra (por ejemplo, “Tipo de medio”).
        share_col (str):  Nombre de la columna que contiene el valor o porcentaje para cada categoría (por ejemplo, “Participación (%)”).
        y_label (str): Etiqueta del eje Y.
        decimal_places (int): Número de decimales que se mostrarán en las etiquetas de texto.
        title (str): Título general del gráfico. 
        legend_title (str): Encabezado de la leyenda.

    Retorna_
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.

    """
    try:
        # Verificar si el DataFrame está vacío
        if df.empty:
            return "No hay datos disponibles."

        # Ejes
        y_label_text = f"{y_label}" if y_label else ' '

        # Orden para categorias por mayor participación
        cat_order = (
            df.groupby(group_col)[share_col]
              .sum()
              .sort_values(ascending=False)
              .index
              .tolist()
        )       

        # Crear la columna de texto con formato español (punto para miles, coma para decimales)
        df["Valor"] = df[share_col].apply(
            lambda val: (
                f"{val:,.{decimal_places}f}".replace(",", "X").replace(".", ",").replace("X", ".") + "%"
            )
            if pd.notnull(val) else None
        )

        # Crear el gráfico de barras apiladas horizontales
        fig = px.bar(
            df,
            x=share_col,
            y=date_col,
            color=group_col,
            orientation='h',
            text="Valor",  # Usamos la columna con formato español
            labels={
                share_col: "Participación (%)",
                date_col: y_label_text,
                group_col: legend_title if legend_title else "Categoría"
            },
            category_orders={
                group_col: cat_order        # Ordenar las categorías de mayor a menor
            }            
        )

        # Ajustes de diseño
        fig.update_traces(textposition="inside", textfont_size=12)
        fig.update_layout(
            barmode='stack',
            xaxis=dict(title="Participación (%)"),
            yaxis=dict(title=y_label_text, categoryorder='category ascending'),
            legend_title=legend_title if legend_title else "Categoría""Categorías",
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center")
        )

        # Hacer cambio de orden en el eje Y
        fig.update_layout(barmode = 'stack', yaxis = {'autorange' : 'reversed'})

        return fig

    except Exception as e:
        return f"Error generando el gráfico: {e}"
    
# Stacked bar chart Vertical
def plot_stacked_bar_chart_v(df, date_col, group_col, share_col,
                            decimal_places=0, title=None, y_label=None, legend_title=None):
    """
    Genera un gráfico de barras apiladas verticales, donde cada barra
    representa un valor de la columna `date_col` (por ejemplo, años) y
    está subdividida según los porcentajes de cada categoría definida en
    `group_col`.
 
    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str):  Nombre de la columna que contiene los valores de fecha o año que se mostrarán en el eje X.
        group_col (str): Nombre de la columna que define las categorías que se apilan en cada barra (por ejemplo, “Tipo de medio”).
        share_col (str):  Nombre de la columna que contiene el valor o porcentaje para cada categoría (por ejemplo, “Participación (%)”).
        y_label (str): Etiqueta del eje Y.
        decimal_places (int): Número de decimales que se mostrarán en las etiquetas de texto.
        title (str): Título general del gráfico.
        legend_title (str): Encabezado de la leyenda.
 
    Retorna:
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.
    """
    try:
        # Verificar si el DataFrame está vacío
        if df.empty:
            return "No hay datos disponibles."
 
        # Ejes
        y_label_text = f"{y_label}" if y_label else ' '
 
        # Orden para categorias por mayor participación
        cat_order = (
            df.groupby(group_col)[share_col]
              .sum()
              .sort_values(ascending=False)
              .index
              .tolist()
        )      
 
        # Crear la columna de texto con formato español (punto para miles, coma para decimales)
        df["Valor"] = df[share_col].apply(
            lambda val: (
                f"{val:,.{decimal_places}f}".replace(",", "X").replace(".", ",").replace("X", ".") + "%"
            )
            if pd.notnull(val) else None
        )
 
        # Ordenar el DataFrame por la columna de fechas
        df = df.sort_values(by=date_col)
       
        # Asegurarse de que la columna de fechas sea categórica y ordenada
        df[date_col] = pd.Categorical(df[date_col], categories=sorted(df[date_col].unique()), ordered=True)
 
        # Crear el gráfico de barras apiladas verticales
        fig = px.bar(
            df,
            x=date_col,
            y=share_col,
            color=group_col,
            text="Valor",  # Usamos la columna con formato español
            labels={
                share_col: "Participación (%)",
                date_col: y_label_text,
                group_col: legend_title if legend_title else "Categoría"
            },
            category_orders={
                group_col: cat_order        # Ordenar las categorías de mayor a menor
            }            
        )
 
        # Ajustes de diseño
        fig.update_traces(textposition="inside", textfont_size=12)
        fig.update_layout(
            barmode='stack',
            xaxis=dict(title="Año", categoryorder='category ascending'),
            yaxis=dict(title="Participación (%)"),
            legend_title=legend_title if legend_title else "Categoría",
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center")
        )

        return fig
 
    except Exception as e:
        return f"Error generando el gráfico: {e}"
    

# Single bar chart
def plot_single_bar_chart(df, date_col, value_col, 
                          title=None, x_label="", y_label="", y_units=None,
                          show_labels=False, decimal_places=0):
    """
    Genera un gráfico de barras (bar chart) de una sola serie usando Plotly Express,
    aplicando convenciones de formato en español para las etiquetas (punto para miles y coma
    para decimales) si se solicitan.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str): Nombre de la columna que contiene las fechas o categorías para el eje X.
        value_col (str): Nombre de la columna que contiene los valores de la serie que se graficará.
        title (str): Título del gráfico. Si es None, el gráfico no tendrá título específico.
        x_label (str): Etiqueta del eje X.
        y_label (str): Etiqueta del eje Y.
        y_units (str): Unidades que se mostrarán junto al y_label. 
        show_labels (bool): Si es True, muestra las etiquetas de los valores encima de cada barra.
        decimal_places (int): Número de decimales que se mostrarán en las etiquetas.

    Retorna:
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.
    """
    try:
        # Verificar si el DataFrame está vacío
        if df.empty:
            return "No hay datos disponibles."
        
        # Construir la etiqueta del eje Y (con unidades si corresponde)
        y_label_text = f"{y_label} ({y_units})" if (y_label and y_units) else y_label
        x_label_text = x_label

        # Crear el gráfico de barras
        fig = px.bar(
            df,
            x=date_col,
            y=value_col,
            labels={
                date_col: x_label_text,
                value_col: y_label_text
            },
            text=value_col if show_labels else None  # Muestra las etiquetas si se solicita
        )

        # Ajustar el diseño del gráfico
        fig.update_layout(
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            xaxis_title=x_label_text,
            yaxis_title=y_label_text,
            template="plotly_white"
        )

        # Configurar el formato del eje Y (separadores de miles y decimales)
        tick_format = f".{decimal_places}f"  # Controla la cantidad de decimales
        fig.update_yaxes(tickformat=tick_format)

        # Si show_labels, formatear en estilo español y posicionar etiquetas
        if show_labels:
            fig.update_traces(
                text=df[value_col].apply(
                    lambda x: (
                        f"{x:,.{decimal_places}f}"
                        .replace(",", "X")
                        .replace(".", ",")
                        .replace("X", ".")
                    )
                    if pd.notnull(x) else None
                ),
                textposition="outside"  # Ubicar el texto encima de las barras
            )

        return fig

    except Exception as e:
        return f"Error generando el gráfico: {e}"

# Side by side bars

def plot_side_by_side_bars(df,date_col, var1_col, var2_col, title=None, x_label="",
    y_label="", y_units=None, show_labels=False, decimal_places=0,
    legend_title=None, legend_labels=None):
    """
    Genera un gráfico de barras agrupadas (lado a lado) para dos variables que
    comparten la misma escala en el eje Y. El eje X corresponde a la columna 
    `date_col`, y cada fecha/categoría tendrá dos barras: una para `var1_col` 
    y otra para `var2_col`.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str): Nombre de la columna que contiene la fecha o categoría para el eje X.
        var1_col (str): Nombre de la primera variable (columna) que se graficará como barras.
        var2_col (str): Nombre de la segunda variable (columna) que se graficará como barras.
        title (str): Título del gráfico. Si es None, no se muestra un título específico.
        x_label (str): Etiqueta del eje X.
        y_label (str): Etiqueta del eje Y.
        y_units (str): Unidades que se mostrarán junto al `y_label`. 
        show_labels (bool): Si es True, muestra las etiquetas de los valores sobre las barras.
        decimal_places (int): Número de decimales que se mostrarán en las etiquetas.
        legend_title (str): Título que se mostrará en la parte superior de la leyenda.
        legend_labels (dict): Diccionario que mapea los nombres originales de las columnas a los nombres que se mostrarán en la leyenda. 

    Retorna:
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.
    """
    try:
        # Verificar si el DataFrame está vacío
        if df.empty:
            return "No hay datos disponibles."

        # Construir la etiqueta del eje Y (con unidades si corresponde)
        y_label_text = f"{y_label} ({y_units})" if (y_label and y_units) else y_label

        # Crear un DataFrame "pivotado" (wide form) con las dos variables
        # Si ya está en wide, podemos simplemente asegurar que las columnas estén presentes.
        df_pivoted = df[[date_col, var1_col, var2_col]]

        # Crear el bar chart con barmode "group" para barras lado a lado
        fig = px.bar(
            df_pivoted,
            x=date_col,
            y=[var1_col, var2_col],  # Dos columnas => barras agrupadas
            labels={
                date_col: x_label, 
                "value": y_label_text  # Plotly usa "value" como etiqueta para wide form
            },
            barmode="group"  # Barras una junto a la otra
        )

        # Ajustar layout básico
        fig.update_layout(
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5},
            xaxis_title=x_label,
            yaxis_title=y_label_text,
            template="plotly_white",
            legend=dict(
                orientation="h", 
                y=-0.2, 
                x=0.5, 
                xanchor="center", 
                yanchor="top"
            )
        )

        # Si el usuario especifica un título para la leyenda
        if legend_title:
            fig.update_layout(legend_title_text=legend_title)

        # Formato del eje Y (número de decimales y separador de miles)
        tick_format = f".{decimal_places}f"
        fig.update_yaxes(tickformat=tick_format)

        # Si show_labels es True, formatear cada traza con etiquetas al estilo español
        if show_labels:
            # Cada traza en fig.data corresponde a una de las columnas [var1_col, var2_col]
            for trace in fig.data:
                # 'trace.name' coincide con el nombre de la columna
                col_name = trace.name  
                # Asignar el texto con formateo “español”
                text_values = df_pivoted[col_name].apply(
                    lambda x: (
                        f"{x:,.{decimal_places}f}"
                         .replace(",", "X")
                         .replace(".", ",")
                         .replace("X", ".")
                    )
                    if pd.notnull(x) else None
                )
                trace.text = text_values
                trace.textposition = "outside"

        # Renombrar las columnas en la leyenda según 'legend_labels'
        if legend_labels and isinstance(legend_labels, dict):
            for trace in fig.data:
                # Solo actualizamos si el nombre original está en el dict
                if trace.name in legend_labels:
                    trace.name = legend_labels[trace.name]
        return fig

    except Exception as e:
        return f"Error generando el gráfico: {e}"
    
# Treemap

def plot_treemap(df, date_col, value_col, group_col, share_col,
        decimal_places=0, title=None, group_label="Grupo",
        value_label="Valor", share_label="Participación"):
    """
    Genera un gráfico de tipo Treemap con un menú desplegable para 
    seleccionar distintos valores de la columna `date_col` (por ejemplo, distintos años).
    Por defecto, se muestra solo el primer valor de `date_col` y cada opción en el menú 
    solo visualiza un valor de `date_col` a la vez.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        date_col (str): Nombre de la columna que contiene la fecha o año (valores únicos que se usarán para crear diferentes treemaps).
        value_col (str): Nombre de la columna que contiene la magnitud principal (por ejemplo, número de viajeros).
        group_col (str): Nombre de la columna que define los grupos (los “hijos” en el treemap).
        share_col (str): Nombre de la columna que contiene el porcentaje o participación de cada grupo.
        decimal_places (int): Número de decimales para formatear `share_col`.
        title (str): Título del gráfico. Si es None, no se asigna un título específico.
        group_label (str): Etiqueta que se muestra en el tooltip para la variable definida por `group_col`.
        value_label (str): Etiqueta que se muestra en el tooltip para la variable definida por `value_col`.
        share_label (str): Etiqueta que se muestra en el tooltip para la variable definida por `share_col`.

    Retorna: 
        plotly.graph_objs.Figure: Gráfico interactivo de Plotly con menú de filtrado.
        str: Mensaje de error si no hay datos o se produce una excepción.
    """
    try:
        # Si el DataFrame está vacío, retornamos mensaje de error
        if df.empty:
            return "No hay datos disponibles."

        # Obtener la lista de valores únicos de la columna fecha (ej. años)
        all_dates = sorted(df[date_col].unique())

        # Crear figura vacía donde iremos agregando cada Treemap como traza
        fig = go.Figure()

        # Bucle para cada valor en 'all_dates'
        for idx, date_val in enumerate(all_dates):
            # Filtrar el DataFrame para el valor actual de la columna date_col
            subdf = df[df[date_col] == date_val].copy()

            # Formatear el porcentaje (share_col) con coma para decimales y punto para miles + '%'
            subdf["formatted_share"] = subdf[share_col].apply(
                lambda val: (
                    f"{val:,.{decimal_places}f}"
                    .replace(",", "X")
                    .replace(".", ",")
                    .replace("X", ".")
                    + "%"
                )
                if pd.notnull(val) else None
            )

            # Formatear la columna de valores (value_col) también al estilo “español”
            subdf["formatted_value"] = subdf[value_col].apply(
                lambda val: (
                    f"{val:,.0f}"
                    .replace(",", "X")
                    .replace(".", ",")
                    .replace("X", ".")
                )
                if pd.notnull(val) else None
            )

            # Crear etiquetas personalizadas con los parámetros de texto
            subdf["custom_label"] = subdf.apply(
                lambda row: (
                    f"{group_label}: {row[group_col]}"
                    + f"<br>{value_label}: {row['formatted_value']}"
                    + f"<br>{share_label}: {row['formatted_share']}"
                ),
                axis=1
            )

            # Crear la figura de Treemap para este subset
            subfig = px.treemap(
                subdf,
                path=[px.Constant("Total"), group_col],
                values=share_col,
                custom_data=["custom_label"]  # Para usar la etiqueta
            )

            # Ajustar la traza para usar nuestro custom_label
            subfig.update_traces(
                root_color="lightgrey",
                texttemplate="%{customdata[0]}"
            )

            # Extraer la traza y añadirla a la figura principal
            treemap_trace = subfig.data[0]
            # El nombre del trace es el valor de fecha (ej. 2022, 2023)
            treemap_trace.name = str(date_val)
            # Mostrar solo la primera traza, ocultar las demás
            treemap_trace.visible = True if idx == 0 else False

            fig.add_trace(treemap_trace)

        # Construir el menú (dropdown)
        buttons = []
        for i, date_val in enumerate(all_dates):
            # Cada botón hace visible solo la traza correspondiente
            visible_array = [False] * len(all_dates)
            visible_array[i] = True

            buttons.append(
                dict(
                    label=str(date_val),
                    method="update",
                    args=[{"visible": visible_array}]
                )
            )

        updatemenus = [
            dict(
                type="dropdown",
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.1,
                yanchor="top",
                buttons=buttons
            )
        ]

        # Ajustar diseño general de la figura
        fig.update_layout(
            updatemenus=updatemenus,
            margin=dict(t=50, l=25, r=25, b=25),
            title={'text': title, 'xanchor': 'center', 'yanchor': 'top', 'x':0.5}
        )

        return fig

    except Exception as e:
        return f"Error generando el gráfico: {e}"
