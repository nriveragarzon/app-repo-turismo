# ------------------------------
# 1. Importar módulos necesarios
# ------------------------------
import tkinter as tk
from tkinter import messagebox, ttk
import subprocess
import sys
import threading
import pandas as pd

# ------------------------------------------
# 2. Definir funciones para ejecutar scripts
# ------------------------------------------


def run_script(script_name, description, double_confirm=False):
    """
    Ejecuta un script de Python en un subproceso y muestra la salida en la interfaz de usuario.

    Args:
        script_name (str): Nombre del archivo o ruta del script de Python que se va a ejecutar.
        description (str): Descripción del script, utilizada para mostrar en mensajes al usuario.
        double_confirm (bool): Indica si se requiere una doble confirmación antes de ejecutar el script.
    """
    def execute():
        # Confirmar con el usuario
        if messagebox.askyesno("Confirmar", f"¿Desea ejecutar el script para {description}?"):
            # Si se requiere doble confirmación
            if double_confirm:
                if not messagebox.askyesno(
                    "ADVERTENCIA",
                    f"¡Este proceso puede realizar modificaciones irreversibles a la base de datos existente!\n\n"
                    f"¿Está seguro que leyó y entendió la documentación del proceso completamente?\n\n"
                    f"¿Está seguro de que desea continuar con {description}?"
                ):
                    return  # Si el usuario cancela, no se ejecuta el script

            # Limpiar el área de texto antes de ejecutar el nuevo script
            output_text.delete(1.0, tk.END)

            # Ejecutar el script y mostrar la salida
            process = subprocess.Popen(
                [sys.executable, '-u', script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # Line buffering
            )
            # Leer la salida en tiempo real
            for line in process.stdout:
                output_text.insert(tk.END, line)
                output_text.see(tk.END)
            process.wait()
            messagebox.showinfo("Completado", f"Ejecución de {description} completada.")

    # Ejecutar en un hilo separado para no congelar la interfaz
    threading.Thread(target=execute).start()

def run_forward_keys_script():
    """
    Pregunta al usuario qué script de Forward Keys desea ejecutar (histórico o mensual)
    y luego llama a run_script con los parámetros adecuados.
    """
    # Preguntar al usuario qué tipo de cargue desea realizar
    opcion = ask_historico_or_mensual()

    if opcion == 'historico':
        # Ejecutar el script de cargue Forward Keys Histórico
        script_name = "src/cargue_forward_keys_historico.py"
        description = "Cargue de cargue Forward Keys Histórico (cargar datos de 2022 a 2024/09 elimnando la tabla actual y agregando estos datos)"
    elif opcion == 'mensual':
        # Ejecutar el script de cargue Forward Keys Mensual
        script_name = "src/cargue_forward_keys_mensual.py"
        description = "Cargue de Forward Keys Mensual (cargar datos del último(s) mes(es) disponible(s) agregando nuevas filas a la tabla actual)"
    else:
        # El usuario cerró la ventana o no hizo una elección válida
        return

    # Llamar a run_script con el script seleccionado y doble confirmación
    run_script(script_name, description, double_confirm=True)

# ---------------------------------
# 3. Inicializar la ventana principal
# ---------------------------------

# Crear la ventana principal
root = tk.Tk()
root.title("Repositorio Cifras Turismo: Cargue de información")

# ----------------------------------
# 4. Crear área de texto para la salida
# ----------------------------------

# Área de texto para mostrar la salida
output_text = tk.Text(root, wrap='word', height=20)
output_text.pack(fill='both', expand=True)

# ---------------------------------
# 5. Crear botones para cada script
# ---------------------------------

# Botón para el setup de la base de datos con doble confirmación
btn_setup_db = tk.Button(
    root,
    text="0. Ejecutar Setup de Base de Datos",
    command=lambda: run_script(
        "src/database_setup.py",
        "Setup de Base de Datos (eliminar base de datos actual, crearla, crear esquemas y tablas de seguimiento y auditoria)",
        double_confirm=True  # Activar doble confirmación
    )
)
btn_setup_db.pack(fill='x')

# Botón para cargar tablas correlativas
btn_cargue_geo = tk.Button(
    root,
    text="1. Cargar tablas correlativas",
    command=lambda: run_script("src/cargue_correlativas.py", "Cargue de Tablas Correlativas (eliminar y cargar de nuevo la información de las tablas correlativas)")
)
btn_cargue_geo.pack(fill='x')

# Botón para cargar datos de GlobalData
btn_cargue_global = tk.Button(
    root,
    text="2. Cargar Datos de GlobalData",
    command=lambda: run_script("src/cargue_global_data.py", "Cargue de GlobalData (eliminar y cargar de nuevo la información de las tablas de GlobalData)")
)
btn_cargue_global.pack(fill='x')

# Botón para cargar datos de OAG con doble confirmación y selección de script
btn_cargue_oag = tk.Button(
    root,
    text="3. Cargar Datos de OAG",
    command=lambda: run_script("src/cargue_oag.py", "Cargue de OAG (cargar los últimos datos disponibles de la carpeta)")
)
btn_cargue_oag.pack(fill='x')

# Botón para cargar datos de IATA-GAP con doble confirmación y selección de script
btn_cargue_iata = tk.Button(
    root,
    text="4. Cargar Datos de IATA-GAP",
    command=lambda: run_script("src/cargue_iata.py", "Cargue de IATA-GAP (cargar los últimos datos disponibles de la carpeta)")
)
btn_cargue_iata.pack(fill='x')

# Botón para cargar datos de Forward Keys con doble confirmación y selección de script
btn_cargue_forward_keys = tk.Button(
    root,
    text="5. Cargar Datos de Forward Keys - Búsquedas",
    command=lambda: run_script("src/cargue_forward_keys_busquedas.py", "Cargue de Forward Keys (eliminar y cargar de nuevo la información desde cero para asegurar la mayor actualización posible con los archivos disponibles de la carpeta)")
)
btn_cargue_forward_keys.pack(fill='x')

# -------------------------------
# 6. Ejecutar el bucle principal
# -------------------------------

# Ejecutar el bucle principal de la interfaz
root.mainloop()