-- ===========================================
-- Configuración y asignación de roles y permisos para el CITI
-- ===========================================

-- 1. Establecer el rol de seguridad para poder administrar roles
USE ROLE SECURITYADMIN;  
-- Se utiliza el rol SECURITYADMIN, que tiene privilegios para gestionar la seguridad y roles en Snowflake.

-- 2. Seleccionar el Warehouse adecuado para la ejecución de las consultas
USE WAREHOUSE WH_PROCOLOMBIA_ANALITICA;  
-- Se selecciona el warehouse WH_PROCOLOMBIA_ANALITICA para procesar las operaciones.

-- ======================================================
-- Creación del rol APP_CITI para el Centro de Inteligencia de Turismo Internacional (CITI)
-- ======================================================
-- Se crea (o reemplaza, si ya existe) el rol APP_CITI, asignándole un comentario descriptivo.
CREATE OR REPLACE ROLE "APP_CITI" 
    COMMENT = "Este es un rol creado para el CITI, tiene permisos de lectura a todas las vistas de REPOSITORIO_TURISMO y puede insertar datos a la tabla de seguimiento";

-- ======================================================
-- Definición de la jerarquía de roles
-- ======================================================
-- Se asigna el rol APP_CITI al rol SYSADMIN para integrarlo correctamente en el árbol de permisos.
GRANT ROLE APP_CITI TO ROLE "SYSADMIN";
-- Se asigna el rol APPS_MANAGER al rol APP_CITI para incluirlo en la jerarquía de roles de aplicaciones.
GRANT ROLE APPS_MANAGER TO ROLE "APP_CITI";

-- Se muestra la lista de roles para verificar la creación y asignación del rol APP_CITI.
SHOW ROLES;

-- ======================================================
-- Cambio de rol para asignar permisos a nivel de base de datos
-- ======================================================
-- Se cambia el rol actual a ACCOUNTADMIN, rol con máximos privilegios administrativos en Snowflake.
USE ROLE ACCOUNTADMIN;

-- Seleccionar la base de datos donde se encuentran los objetos a los que se asignarán permisos.
USE DATABASE REPOSITORIO_TURISMO;

-- ======================================================
-- Otorgamiento de permisos de lectura (SELECT) para cada objeto y vista específica
-- ======================================================
-- Permisos sobre la base de datos:
GRANT USAGE ON DATABASE REPOSITORIO_TURISMO TO ROLE APP_CITI;
-- Se otorga el permiso de USAGE sobre la base de datos REPOSITORIO_TURISMO para que el rol pueda acceder a ella.

-- Permisos sobre todos los esquemas:
GRANT USAGE ON ALL SCHEMAS IN DATABASE REPOSITORIO_TURISMO TO ROLE APP_CITI;
-- Permite al rol APP_CITI utilizar todos los esquemas dentro de la base de datos REPOSITORIO_TURISMO.

-- Auditoria:
-- Se conceden permisos de SELECT en todas y futuras tablas del esquema AUDITORIA.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.AUDITORIA TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.AUDITORIA TO ROLE APP_CITI;

-- Correlativas:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema CORRELATIVAS.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.CORRELATIVAS TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.CORRELATIVAS TO ROLE APP_CITI;

-- Credibanco:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema CREDIBANCO.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.CREDIBANCO TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.CREDIBANCO TO ROLE APP_CITI;

-- ForwardKeys:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema FORWARDKEYS.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.FORWARDKEYS TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.FORWARDKEYS TO ROLE APP_CITI;

-- GlobalData:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema GLOBALDATA.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.GLOBALDATA TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.GLOBALDATA TO ROLE APP_CITI;

-- IataGap:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema IATAGAP.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.IATAGAP TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.IATAGAP TO ROLE APP_CITI;

-- Migración:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema MIGRACION.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.MIGRACION TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.MIGRACION TO ROLE APP_CITI;

-- OAG:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema OAG.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.OAG TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.OAG TO ROLE APP_CITI;

-- Seguimiento:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema SEGUIMIENTO.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.SEGUIMIENTO TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.SEGUIMIENTO TO ROLE APP_CITI;

-- Vistas:
-- Se otorgan permisos de SELECT en todas y futuras tablas del esquema VISTAS.
GRANT SELECT ON FUTURE TABLES IN SCHEMA REPOSITORIO_TURISMO.VISTAS TO ROLE APP_CITI;
GRANT SELECT ON ALL TABLES IN SCHEMA REPOSITORIO_TURISMO.VISTAS TO ROLE APP_CITI;
-- Además, se otorgan permisos de SELECT específicos en ciertas vistas ya creadas.
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.CREDIBANCO_GASTO TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_BUSQUEDAS_PAISES TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GEOGRAFIA TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_CATEGORIAS_GASTO TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_FLUJOS_VIAJEROS_REGION TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_FORMA_VIAJE TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_MICE TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_NOCHES_PROMEDIO TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_RANGO_EDAD TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.FORWARDKEYS_RESERVAS_PAISES TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_MOTIVO_VIAJE TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.IATAGAP_AGENCIAS_VIAJEROS TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.OAG_CONECTIVIDAD_COLOMBIA TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.OAG_CONECTIVIDAD_MUNDO TO ROLE APP_CITI;
GRANT SELECT ON VIEW REPOSITORIO_TURISMO.VISTAS.GLOBALDATA_VIAJEROS_MUNDO TO ROLE APP_CITI;

-- Permitir el acceso a futuras vistas creadas en el esquema VISTAS.
GRANT SELECT ON FUTURE VIEWS IN SCHEMA REPOSITORIO_TURISMO.VISTAS TO ROLE APP_CITI;

-- ======================================================
-- Otorgamiento de permisos de escritura en la tabla de seguimiento
-- ======================================================
-- Se concede al rol APP_CITI los permisos de SELECT, INSERT, UPDATE y DELETE sobre la tabla de seguimiento.
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE REPOSITORIO_TURISMO.SEGUIMIENTO.SEGUIMIENTO_EVENTOS TO ROLE APP_CITI;

-- ======================================================
-- Verificación de permisos asignados al rol APP_CITI
-- ======================================================
-- Se muestran todos los permisos (grants) asignados al rol APP_CITI para confirmar que la configuración es correcta.
SHOW GRANTS TO ROLE APP_CITI;

-- ======================================================
-- Asignación del rol APP_CITI a un usuario de servicio
-- ======================================================
-- Se otorga el rol APP_CITI al usuario USER_SERVICE_ANALITICA, permitiéndole acceder a los permisos definidos.
GRANT ROLE APP_CITI TO USER USER_SERVICE_ANALITICA;