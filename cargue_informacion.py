# ------------------------------
# 1. Importar módulos necesarios
# ------------------------------
import tkinter as tk
from tkinter import messagebox, ttk
import subprocess
import sys  # Importar el módulo sys
import threading
import pandas as pd

# ------------------------------------------
# 2. Definir funciones para ejecutar scripts
# ------------------------------------------


def run_script(script_name, description):
    """
    Ejecuta un script de Python en un subproceso y muestra la salida en la interfaz de usuario.

    Esta función lanza un script en un subproceso separado para evitar que la interfaz de usuario
    se congele durante su ejecución. Antes de ejecutar el script, solicita la confirmación del 
    usuario a través de un cuadro de diálogo. Durante la ejecución, la salida del script se 
    muestra en tiempo real en un widget de texto de la interfaz. Una vez completada la ejecución, 
    se muestra un mensaje de finalización.

    Args:
        script_name (str): Nombre del archivo o ruta del script de Python que se va a ejecutar.
        description (str): Descripción del script, utilizada para mostrar en mensajes al usuario.

    Funcionalidad:
        - Solicita confirmación al usuario antes de ejecutar el script.
        - Ejecuta el script en un hilo separado para evitar bloquear la interfaz de usuario.
        - Muestra la salida del script en un widget de texto en tiempo real.
        - Informa al usuario cuando la ejecución ha finalizado.

    Notas:
        - El script se ejecuta en el mismo entorno de Python que la aplicación principal.
        - Utiliza `threading.Thread` para ejecutar el proceso de forma asíncrona y evitar el congelamiento
          de la interfaz gráfica.
    """
    def execute():
            # Confirmar con el usuario
            if messagebox.askyesno("Confirmar", f"¿Desea ejecutar el script para {description}?"):
                # Ejecutar el script y mostrar la salida
                process = subprocess.Popen(
                    [sys.executable, script_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                for line in process.stdout:
                    output_text.insert(tk.END, line)
                    output_text.see(tk.END)
                process.wait()
                messagebox.showinfo("Completado", f"Ejecución de {description} completada.")
    
    # Ejecutar en un hilo separado para no congelar la interfaz
    threading.Thread(target=execute).start()

# ---------------------------------
# 3. Inicializar la ventana principal
# ---------------------------------

"""
Se crea la ventana principal de la interfaz gráfica utilizando Tkinter.
Aquí también se define el área de texto donde se mostrará la salida de los scripts.
"""

# Crear la ventana principal
root = tk.Tk()
root.title("Ejecutor de Scripts")

# ----------------------------------
# 4. Crear área de texto para la salida
# ----------------------------------

"""
El área de texto `output_text` se utiliza para mostrar en tiempo real
la salida de cada script que se ejecuta.
"""

# Área de texto para mostrar la salida
output_text = tk.Text(root, wrap='word', height=20)
output_text.pack(fill='both', expand=True)

# ---------------------------------
# 5. Crear botones para cada script
# ---------------------------------

"""
Se crean botones para cada uno de los scripts que se pueden ejecutar.
Al presionar un botón, se llama a la función `run_script` con los parámetros
correspondientes.
"""

# Botón para el setup de la base de datos
btn_setup_db = tk.Button(
    root,
    text="0. Ejecutar Setup de Base de Datos",
    command=lambda: run_script("src/database_setup.py", "Setup de Base de Datos")
)
btn_setup_db.pack(fill='x')

# Botón para cargar datos de geografía
btn_cargue_geo = tk.Button(
    root,
    text="1. Cargar tablas correlativas",
    command=lambda: run_script("src/cargue_correlativas.py", "Cargue de Datos de Geografía")
)
btn_cargue_geo.pack(fill='x')

# Botón para cargar datos de GlobalData
btn_cargue_global = tk.Button(
    root,
    text="2. Cargar Datos de GlobalData",
    command=lambda: run_script("src/cargue_global_data.py", "Cargue de GlobalData")
)
btn_cargue_global.pack(fill='x')

# Botón para cargar datos de OAG
btn_cargue_oag = tk.Button(
    root,
    text="3. Cargar Datos de OAG",
    command=lambda: run_script("src/cargue_oag.py", "Cargue de OAG")
)
btn_cargue_oag.pack(fill='x')

# -------------------------------
# 6. Ejecutar el bucle principal
# -------------------------------

"""
Se inicia el bucle principal de la interfaz gráfica para que la ventana
se mantenga abierta y pueda interactuar con el usuario.
"""

# Ejecutar el bucle principal de la interfaz
root.mainloop()