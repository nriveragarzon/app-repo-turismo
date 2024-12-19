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

def ask_historico_or_mensual():
    """
    Muestra un cuadro de diálogo para que el usuario seleccione entre 'Histórico' y 'Mensual'.
    Retorna 'historico' o 'mensual' según la elección del usuario.
    """
    # Crear una ventana de diálogo
    dialog = tk.Toplevel(root)
    dialog.title("Periodicidad de cargue de datos")
    dialog.geometry("400x150")
    dialog.resizable(False, False)
    dialog.grab_set()  # Bloquear interacción con la ventana principal

    # Centrar la ventana en la pantalla
    dialog.update_idletasks()
    x = (dialog.winfo_screenwidth() - dialog.winfo_reqwidth()) // 2
    y = (dialog.winfo_screenheight() - dialog.winfo_reqheight()) // 2
    dialog.geometry(f"+{x}+{y}")

    # Etiqueta con la pregunta
    label = tk.Label(dialog, text="¿Desea ejecutar el cargue histórico o mensual?", font=("Arial", 12))
    label.pack(pady=20)

    # Variable para almacenar la elección del usuario
    choice = tk.StringVar()

    # Funciones para los botones
    def select_historico():
        choice.set('historico')
        dialog.destroy()

    def select_mensual():
        choice.set('mensual')
        dialog.destroy()

    # Frame para centrar los botones
    button_frame = tk.Frame(dialog)
    button_frame.pack(pady=10)

    # Botones para 'Histórico' y 'Mensual'
    btn_historico = tk.Button(button_frame, text="Histórico", width=15, command=select_historico)
    btn_historico.pack(side='left', padx=10)

    btn_mensual = tk.Button(button_frame, text="Mensual", width=15, command=select_mensual)
    btn_mensual.pack(side='right', padx=10)

    # Esperar a que el usuario haga una elección
    dialog.wait_window()

    return choice.get()

def run_oag_script():
    """
    Pregunta al usuario qué script de OAG desea ejecutar (histórico o mensual)
    y luego llama a run_script con los parámetros adecuados.
    """
    # Preguntar al usuario qué tipo de cargue desea realizar
    opcion = ask_historico_or_mensual()

    if opcion == 'historico':
        # Ejecutar el script de cargue OAG Histórico
        script_name = "src/cargue_oag_historico.py"
        description = "Cargue de OAG Histórico (cargar datos de 2022/01 a 2024/09 elimnando la tabla actual y agregando estos datos)"
    elif opcion == 'mensual':
        # Ejecutar el script de cargue OAG Mensual
        script_name = "src/cargue_oag_mensual.py"
        description = "Cargue de OAG Mensual (cargar datos del último mes agregando nuevas filas a la tabla actual)"
    else:
        # El usuario cerró la ventana o no hizo una elección válida
        return

    # Llamar a run_script con el script seleccionado y doble confirmación
    run_script(script_name, description, double_confirm=True)

def ask_historico_or_trimestral():
    """
    Muestra un cuadro de diálogo para que el usuario seleccione entre 'Histórico' y 'Trimestral'.
    Retorna 'historico' o 'trimestral' según la elección del usuario.
    """
    # Crear una ventana de diálogo
    dialog = tk.Toplevel(root)
    dialog.title("Periodicidad de cargue de datos")
    dialog.geometry("400x150")
    dialog.resizable(False, False)
    dialog.grab_set()  # Bloquear interacción con la ventana principal

    # Centrar la ventana en la pantalla
    dialog.update_idletasks()
    x = (dialog.winfo_screenwidth() - dialog.winfo_reqwidth()) // 2
    y = (dialog.winfo_screenheight() - dialog.winfo_reqheight()) // 2
    dialog.geometry(f"+{x}+{y}")

    # Etiqueta con la pregunta
    label = tk.Label(dialog, text="¿Desea ejecutar el cargue histórico o trimestral?", font=("Arial", 12))
    label.pack(pady=20)

    # Variable para almacenar la elección del usuario
    choice = tk.StringVar()

    # Funciones para los botones
    def select_historico():
        choice.set('historico')
        dialog.destroy()

    def select_mensual():
        choice.set('trimestral')
        dialog.destroy()

    # Frame para centrar los botones
    button_frame = tk.Frame(dialog)
    button_frame.pack(pady=10)

    # Botones para 'Histórico' y 'Trimestral'
    btn_historico = tk.Button(button_frame, text="Histórico", width=15, command=select_historico)
    btn_historico.pack(side='left', padx=10)

    btn_mensual = tk.Button(button_frame, text="Trimestral", width=15, command=select_mensual)
    btn_mensual.pack(side='right', padx=10)

    # Esperar a que el usuario haga una elección
    dialog.wait_window()

    return choice.get()

def run_iata_script():
    """
    Pregunta al usuario qué script de IATA desea ejecutar (histórico o trimestral)
    y luego llama a run_script con los parámetros adecuados.
    """
    # Preguntar al usuario qué tipo de cargue desea realizar
    opcion = ask_historico_or_trimestral()

    if opcion == 'historico':
        # Ejecutar el script de cargue IATA Histórico
        script_name = "src/cargue_iata_historico.py"
        description = "Cargue de IATA-GAP Histórico (cargar datos de 2022Q1 a 2024Q3 elimnando la tabla actual y agregando estos datos)"
    elif opcion == 'trimestral':
        # Ejecutar el script de cargue IATA Mensual
        script_name = "src/cargue_iata_trimestral.py"
        description = "Cargue de IATA-GAP Trimestral (cargar datos del último(s) Q disponible agregando nuevas filas a la tabla actual)"
    else:
        # El usuario cerró la ventana o no hizo una elección válida
        return

    # Llamar a run_script con el script seleccionado y doble confirmación
    run_script(script_name, description, double_confirm=True)

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
root.title("Ejecutor de Scripts")

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
    command=run_oag_script  # Llamar a la función que maneja la elección del usuario
)
btn_cargue_oag.pack(fill='x')

# Botón para cargar datos de IATA-GAP con doble confirmación y selección de script
btn_cargue_iata = tk.Button(
    root,
    text="4. Cargar Datos de IATA-GAP",
    command=run_iata_script  # Llamar a la función que maneja la elección del usuario
)
btn_cargue_iata.pack(fill='x')

# Botón para cargar datos de Forward Keys con doble confirmación y selección de script
btn_cargue_forward_keys = tk.Button(
    root,
    text="5. Cargar Datos de Forward Keys",
    command=run_forward_keys_script  # Llamar a la función que maneja la elección del usuario
)
btn_cargue_forward_keys.pack(fill='x')

# -------------------------------
# 6. Ejecutar el bucle principal
# -------------------------------

# Ejecutar el bucle principal de la interfaz
root.mainloop()
