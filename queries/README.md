# Consultas de referencia

Q1/Q2 son ejecutables por los drivers que se implementarán, pero no hay runner ni conexiones configurados en este kit. Son traducciones del planteamiento acordado. Los pipelines son JSON puro; allowDiskUse se pasa como opción de ejecución, no dentro del pipeline.

Usar Q1 con configuración fija documentada, sin variar incidentalmente allowDiskUse entre perfiles. Para Q2: Mongo false/true; PostgreSQL work_mem 4MB/64MB por cada conexión de cliente, comprobado con SHOW. No bastará con ejecutar SET en otra sesión. Leer context/adaptacion_consultas.md sobre timestamps, índices y empates.

PostgreSQL devuelve columnas planas y MongoDB documentos anidados; normalizar fuera de la ventana para comprobar equivalencia. En la ventana, consumir todas las filas/documentos sin añadir procesamiento analítico asimétrico.

Fase 04 ejecutó las referencias sin modificarlas. El fixture conocido de 1,004 eventos verifica tres grupos Q1 y 1,000 filas Q2 con 1,000 eventos empatados en el corte; se admiten UUID distintos entre motores dentro del empate si filtros, campos y puntajes coinciden. Los planes reales están en `results/plans/phase04-profile-a/`; son diagnósticos externos a las mediciones oficiales y EXPLAIN ejecuta las consultas.
