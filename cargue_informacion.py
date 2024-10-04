# ------------------------------
# 1. Importar scripts necesarios
# ------------------------------
import src.snowflake_analitica as snowflake_analitica
import subprocess
import sys  # Importar el módulo sys

# ----------------------------------------
# 2. Ejecutar el setup de la base de datos
# ----------------------------------------

"""
El siguiente script se encarga de crear la base de datos,
esquemas y la tabla de seguimiento para el proyecto. No se debe correr 
todos los meses, solo una vez al inicio de la vida del proyecto o de la
cuenta de snowflake.
"""

# Ejecutar
subprocess.run([sys.executable, "src/database_setup.py"])



