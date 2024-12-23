# ------------------------------
# 1. Importar módulos necesarios
# ------------------------------
import tkinter as tk
from tkinter import messagebox, ttk
from tkinter.scrolledtext import ScrolledText
import subprocess
import sys
import threading
import pandas as pd

# ------------------------------------------
# 2. Definir funciones para ejecutar scripts
# ------------------------------------------

# Evento global para controlar la cancelación
cancel_event = threading.Event()

def toggle_buttons(state):
    """
    Habilita o deshabilita los botones durante la ejecución de un proceso.

    Args:
        state (str): Estado de los botones, puede ser 'normal' o 'disabled'.
    """
    for widget in buttons_frame.winfo_children():
        widget.configure(state=state)

def cancel_execution():
    """
    Detiene la ejecución del script en curso.
    """
    cancel_event.set()

def run_script(script_name, description, double_confirm=False):
    """
    Ejecuta un script de Python en un subproceso y muestra la salida en la interfaz de usuario.

    Args:
        script_name (str): Nombre del archivo o ruta del script de Python que se va a ejecutar.
        description (str): Descripción del script, utilizada para mostrar en mensajes al usuario.
        double_confirm (bool): Indica si se requiere una doble confirmación antes de ejecutar el script.
    """
    def execute():
        # Deshabilitar botones al iniciar la ejecución
        toggle_buttons('disabled')
        cancel_button.configure(state='normal')  # Habilitar botón de cancelación

        if messagebox.askyesno("Confirmar", f"¿Desea ejecutar el script para {description}?"):
            if double_confirm:
                if not messagebox.askyesno(
                    "ADVERTENCIA",
                    f"¡Este proceso puede realizar modificaciones irreversibles a la base de datos existente!\n\n"
                    f"¿Está seguro que leyó y entendió la documentación del proceso completamente?\n\n"
                    f"¿Está seguro de que desea continuar con {description}?"
                ):
                    # Habilitar botones si el usuario cancela
                    toggle_buttons('normal')
                    cancel_button.configure(state='disabled')
                    return

            # Limpiar el área de texto antes de ejecutar el nuevo script
            output_scroll.delete(1.0, tk.END)
            cancel_event.clear()  # Resetear el evento de cancelación

            try:
                # Ejecutar el script y capturar la salida
                process = subprocess.Popen(
                    [sys.executable, '-u', script_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1  # Line buffering
                )

                for line in iter(process.stdout.readline, ''):
                    if cancel_event.is_set():
                        process.terminate()  # Terminar el proceso
                        output_scroll.insert(tk.END, "\nProceso cancelado por el usuario.\n")
                        break
                    output_scroll.insert(tk.END, line)
                    output_scroll.see(tk.END)
                process.wait()

                if not cancel_event.is_set():
                    messagebox.showinfo("Completado", f"Ejecución de {description} completada.")
            except Exception as e:
                output_scroll.insert(tk.END, f"\nError: {e}\n")
            finally:
                # Habilitar botones al finalizar
                toggle_buttons('normal')
                cancel_button.configure(state='disabled')

    # Ejecutar en un hilo separado
    threading.Thread(target=execute).start()

# ---------------------------------
# 3. Inicializar la ventana principal
# ---------------------------------

# Crear la ventana principal
root = tk.Tk()
root.title("Repositorio Cifras Turismo: Cargue de información")

# Marco para la información fija (versión y botón de salir)
info_frame = ttk.Frame(root)
info_frame.pack(fill='x', padx=10, pady=5)

# Etiqueta para mostrar la versión
version_label = ttk.Label(info_frame, text="Repositorio Cifras Turismo v.2024.1.0", font=("Arial", 8), anchor="w")
version_label.pack(side="left", padx=5)

# Botón de salir
exit_button = ttk.Button(info_frame, text="Salir", command=root.quit)
exit_button.pack(side="right", padx=5)

# Botón de cancelación
cancel_button = ttk.Button(info_frame, text="Cancelar Ejecución", command=cancel_execution, state='disabled')
cancel_button.pack(side="right", padx=5)
# ----------------------------
# 4. Crear área para la salida
# ----------------------------

# Marco para el área de texto
output_frame = ttk.Frame(root)
output_frame.pack(fill='both', expand=True, padx=10, pady=10)

# Área de texto con scroll para mostrar la salida
output_scroll = ScrolledText(output_frame, wrap='word', height=20)
output_scroll.pack(fill='both', expand=True)

# ---------------------------------
# 5. Crear botones para cada script
# ---------------------------------

# Crear un marco para agrupar todos los botones
buttons_frame = ttk.Frame(root)
buttons_frame.pack(fill='x', padx=10, pady=10)

# Botón para el setup de la base de datos con doble confirmación
btn_setup_db = tk.Button(
    buttons_frame,
    text="0. Ejecutar Setup de Base de Datos",
    command=lambda: run_script(
        "src/database_setup.py",
        "Setup de Base de Datos (eliminar base de datos actual, crearla, crear esquemas y tablas de seguimiento y auditoria)",
        double_confirm=True  # Activar doble confirmación
    )
)
btn_setup_db.pack(fill='x', pady=5)

# Botón para cargar tablas correlativas
btn_cargue_geo = tk.Button(
    buttons_frame,
    text="1. Cargar tablas correlativas",
    command=lambda: run_script("src/cargue_correlativas.py", "Cargue de Tablas Correlativas (eliminar y cargar de nuevo la información de las tablas correlativas)")
)
btn_cargue_geo.pack(fill='x', pady=5)

# Botón para cargar datos de GlobalData
btn_cargue_global = tk.Button(
    buttons_frame,
    text="2. Cargar Datos de GlobalData",
    command=lambda: run_script("src/cargue_global_data.py", "Cargue de GlobalData (eliminar y cargar de nuevo la información de las tablas de GlobalData)")
)
btn_cargue_global.pack(fill='x', pady=5)

# Botón para cargar datos de OAG
btn_cargue_oag = tk.Button(
    buttons_frame,
    text="3. Cargar Datos de OAG",
    command=lambda: run_script("src/cargue_oag.py", "Cargue de OAG (cargar los últimos datos disponibles de la carpeta)")
)
btn_cargue_oag.pack(fill='x', pady=5)

# Botón para cargar datos de IATA-GAP
btn_cargue_iata = tk.Button(
    buttons_frame,
    text="4. Cargar Datos de IATA-GAP",
    command=lambda: run_script("src/cargue_iata.py", "Cargue de IATA-GAP (cargar los últimos datos disponibles de la carpeta)")
)
btn_cargue_iata.pack(fill='x', pady=5)

# Botón para cargar datos de Forward Keys - Búsquedas
btn_cargue_forward_keys_busquedas = tk.Button(
    buttons_frame,
    text="5. Cargar Datos de Forward Keys - Búsquedas",
    command=lambda: run_script("src/cargue_forward_keys_busquedas.py", "Cargue de Forward Keys - Búsquedas  (eliminar y cargar de nuevo la información desde cero los datos de búsquedas para asegurar la mayor actualización posible con los archivos disponibles de la carpeta)")
)
btn_cargue_forward_keys_busquedas.pack(fill='x', pady=5)

# Botón para cargar datos de Forward Keys - Reservas
btn_cargue_forward_keys_reservas = tk.Button(
    buttons_frame,
    text="6. Cargar Datos de Forward Keys - Reservas",
    command=lambda: run_script("src/cargue_forward_keys_reservas.py", "Cargue de Forward Keys - Reservas (eliminar y cargar de nuevo la información desde cero los datos de reservas para asegurar la mayor actualización posible con los archivos disponibles de la carpeta)")
)
btn_cargue_forward_keys_reservas.pack(fill='x', pady=5)

# -------------------------------
# 6. Ejecutar el bucle principal
# -------------------------------

# Ejecutar el bucle principal de la interfaz
root.mainloop()