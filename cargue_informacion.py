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

# ---------------------------------------
# 3 Ejecutar cargue de datos de Geografía
# ---------------------------------------

"""
El siguiente script se encarga de cargar a Snwflake las tablas 
correlativas del repositorio que incluyen correlativas de DIAN y
PAISES.No se debe correr todos los meses, solo cuando hayan cambios
o mejoras a las tablas correlativas.
"""

# Ejecutar
subprocess.run([sys.executable, "src/cargue_correlativas.py"])

# ----------------------------------------
# 4 Ejecutar cargue de datos de GlobalData
# ----------------------------------------

"""
El siguiente script se encarga de cargar a Snwflake las tablas 
de GlobalData.
"""

# Ejecutar
subprocess.run([sys.executable, "src/cargue_global_data.py"])

# ---------------------------------
# 5 Ejecutar cargue de datos de OAG
# ---------------------------------

"""
El siguiente script se encarga de cargar a Snwflake las tablas 
de OAG.
"""

# Ejecutar
subprocess.run([sys.executable, "src/cargue_oag.py"])







