#%% ==========================================================================================
# Importamos librerias
import pandas as pd
from inline_sql import sql, sql_val
import numpy as np
import matplotlib.pyplot as plt 
from   matplotlib import ticker  
from matplotlib import rcParams
import seaborn as sns 

#%% ==========================================================================================
# Importamos los CSV 

carpeta = "~/Downloads/sql/"
           

lista_secciones = pd.read_csv(carpeta+"lista-secciones.csv")
lista_secciones 

lista_sedes = pd.read_csv(carpeta+"lista-sedes.csv")
lista_sedes 

lista_sedes_datos = pd.read_csv(carpeta+"lista-sedes-datos.csv", on_bad_lines='skip')
lista_sedes_datos 

migraciones = pd.read_csv(carpeta+"migraciones.csv")
migraciones 

#%% ==========================================================================================
# Limpieza de datos

# Hay un Null en sede_id
lista_secciones.info()

# Lo saco porque es la columna mas importante de esta tabla
consultaSQL = """
               SELECT sede_id, sede_desc_castellano AS sede, tipo_seccion  
               FROM lista_secciones
               WHERE sede_id IS NOT NULL;
              """

lista_secciones_corregida = sql^ consultaSQL
print(lista_secciones_corregida) 

# Copruebo que ya no hay NULLS
lista_secciones_corregida.info()

# Compruebo que no todas las filas de tipo_seccion son 'seccion' porque de lo contrario seria informacion irrelevante 
sin_seccion = lista_secciones_corregida['tipo_seccion'] != 'Seccion'
lista_secciones_corregida[sin_seccion]

#%% ==========================================================================================

# No hay NULLS
lista_sedes.info()

consultaSQL = """
               SELECT sede_id, sede_desc_castellano AS sede, pais_castellano AS pais, ciudad_castellano AS ciudad, estado, sede_tipo, pais_iso_3 AS iso_3  
               FROM lista_sedes;
              """

lista_sedes_corregida = sql^ consultaSQL
print(lista_sedes_corregida)

#%% ==========================================================================================

# Las columnas con NULLS son irrelevantes para el analisis 
lista_sedes_datos.info()

consultaSQL = """
               SELECT sede_id, sede_desc_castellano AS sede, pais_castellano AS pais, ciudad_castellano AS ciudad, 
               redes_sociales, pais_iso_3 AS iso_3, region_geografica      
               FROM lista_sedes_datos;
              """

lista_sede_datos_corregida = sql^ consultaSQL
print(lista_sede_datos_corregida)

#%% ==========================================================================================

# Limpieza de datos en la tabla de migraciones, cambie el nombre de las columnas porque me daban problemas
# Quite los nulls.
consultaSQL = """
               SELECT CountryOriginName AS pais_de_origen, CountryOriginCode AS codigo_pais_origen, 
               CountryDestName AS pais_destino, CountryDestCode AS codigo_pais_destino,
               sesentas, setentas, ochentas, noventas, dosmil
               FROM migraciones
               WHERE sesentas IS NOT NULL AND
                     setentas IS NOT NULL AND
                     ochentas IS NOT NULL AND
                     noventas IS NOT NULL AND
                     dosmil IS NOT NULL;
              """
migraciones_sin_nulls = sql^ consultaSQL
# Ademas de los nulls, hay muchas filas con el valor '..' para sacarlo tuve que convertir las columnas a string y despues nuevamente a enteros
consultaSQL = """
               SELECT pais_de_origen, codigo_pais_origen, 
                 pais_destino, codigo_pais_destino,
                 CAST(sesentas AS INT) AS sesentas,
                 CAST(setentas AS INT) AS setentas,
                 CAST(ochentas AS INT) AS ochentas,
                 CAST(noventas AS INT) AS noventas,
                 CAST(dosmil AS INT) AS dosmil
               FROM (
                 SELECT pais_de_origen, codigo_pais_origen, 
                  pais_destino, codigo_pais_destino,
                  CAST(sesentas AS STRING) AS sesentas,
                  CAST(setentas AS STRING) AS setentas,
                  CAST(ochentas AS STRING) AS ochentas,
                  CAST(noventas AS STRING) AS noventas,
                  CAST(dosmil AS STRING) AS dosmil
                  FROM migraciones_sin_nulls
               ) AS subquery
               WHERE 
                 sesentas != '..' AND
                 setentas != '..' AND
                 ochentas != '..' AND
                 noventas != '..' AND
                 dosmil != '..';
              """
migraciones_filtrada = sql^ consultaSQL
migraciones_filtrada

# Y ahora si puedo sacar todas aquellas donde las cinco filas son cero
consultaSQL = """
               SELECT pais_de_origen, codigo_pais_origen, 
               pais_destino, codigo_pais_destino,
               sesentas AS "1960", setentas AS "1970", ochentas AS "1980", noventas AS "1990", dosmil AS "2000"
               FROM migraciones_filtrada
               WHERE sesentas != 0 AND setentas != 0 AND ochentas != 0 AND noventas != 0 AND dosmil != 0;
              """
migraciones_corregida = sql^ consultaSQL
migraciones_corregida

#%% ==========================================================================================
 
## Punto h) 

## I)
# Creo una tabla con la cantidad de secciones promedio por pais
consultaSQL = """
                 SELECT AVG(cantidad_de_secciones) AS secciones_promedio,
                 iso_3, 
                 FROM (
                   SELECT COUNT(tipo_seccion) AS cantidad_de_secciones,
                   lista_secciones_corregida.sede_id,
                   iso_3 
                   FROM lista_secciones_corregida
                   INNER JOIN lista_sede_datos_corregida
                   ON lista_secciones_corregida.sede_id = lista_secciones_corregida.sede_id
                   WHERE tipo_seccion = 'Seccion'
                   GROUP BY lista_secciones_corregida.sede_id, iso_3
                 )
                 GROUP BY iso_3
              """

cantidad_de_secciones_promedio = sql^ consultaSQL
cantidad_de_secciones_promedio

# Creo una tabla con la cantidad de secciones promedio, cantidad de sedes, ordenados por pais
consultaSQL = """
                 SELECT cantidad_de_secciones_promedio.secciones_promedio,
                 cantidad_de_secciones_promedio.iso_3,
                 sedes
                 FROM cantidad_de_secciones_promedio
                 INNER JOIN (
                   SELECT 
                   iso_3,  
                   COUNT(*) AS sedes
                   FROM lista_sede_datos_corregida
                   GROUP BY iso_3
                 ) AS cantidad_de_sedes
                 ON cantidad_de_secciones_promedio.iso_3 = cantidad_de_sedes.iso_3;
              """

secciones_promedio_sedes = sql^ consultaSQL
secciones_promedio_sedes

# Migraciones en el año 2000
consultaSQL = """
                 SELECT (cantidad_de_inmigracion - cantidad_de_emigracion) AS flujo_migratorio_neto,
                 codigo_pais_origen AS iso_3,
                 pais_de_origen AS Pais
                 FROM (
                     SELECT pais_de_origen,
                     codigo_pais_origen,
                     SUM("2000") AS cantidad_de_emigracion
                     FROM migraciones_corregida
                     GROUP BY pais_de_origen, codigo_pais_origen
                 ) AS emigracion
                 INNER JOIN (
                     SELECT pais_destino,
                     codigo_pais_destino,
                     SUM("2000") AS cantidad_de_inmigracion
                     FROM migraciones_corregida
                     GROUP BY pais_destino, codigo_pais_destino
                 ) AS inmigracion
                 ON emigracion.codigo_pais_origen = inmigracion.codigo_pais_destino;
              """

flujo_migratorio = sql^ consultaSQL
flujo_migratorio

# Reporte final 
consultaSQL = """
                 SELECT Pais,
                 sedes,
                 secciones_promedio,
                 flujo_migratorio_neto,
                 FROM secciones_promedio_sedes
                 INNER JOIN flujo_migratorio
                 ON secciones_promedio_sedes.iso_3 = flujo_migratorio.iso_3
                 ORDER BY sedes DESC, Pais ASC;
              """

reporte = sql^ consultaSQL
reporte

# Guardo el reporte en un archivo excel
reporte.to_excel(carpeta+'flujo_migratorio_neto.xlsx', index=False)

#%% ==========================================================================================
##ii)

## filtro las migraciones que estan relacionadas con Argentina
migraciones_ARG = (migraciones_corregida['codigo_pais_origen'].astype(str) == 'ARG') | (migraciones_corregida['codigo_pais_destino'].astype(str) == 'ARG')
migraciones_con_sedes = migraciones_corregida[migraciones_ARG]
migraciones_con_sedes


# Calculo el flujo migratorio de cada pais con Argentina
consultaSQL = """
                 SELECT (cantidad_de_inmigracion - cantidad_de_emigracion) AS flujo_migratorio_neto,
                 codigo_pais_origen AS iso_3,
                 FROM (
                     SELECT codigo_pais_origen,
                     "2000" AS cantidad_de_emigracion
                     FROM migraciones_con_sedes
                     WHERE pais_destino = 'Argentina'
                 ) AS emigracion
                 INNER JOIN (
                     SELECT codigo_pais_destino,
                     "2000" AS cantidad_de_inmigracion
                     FROM migraciones_con_sedes
                     WHERE pais_de_origen = 'Argentina'
                 ) AS inmigracion
                 ON emigracion.codigo_pais_origen = inmigracion.codigo_pais_destino;
              """

flujo_migratorio = sql^ consultaSQL
flujo_migratorio

# Agrupo por region
consultaSQL = """
                SELECT flujo_migratorio.iso_3,
                flujo_migratorio_neto,
                region_geografica,
                FROM flujo_migratorio
                INNER JOIN (
                   SELECT DISTINCT region_geografica,
                   iso_3,
                   FROM lista_sede_datos_corregida
                ) AS region
                ON flujo_migratorio.iso_3 = region.iso_3;

"""
migracion_por_region = sql^ consultaSQL 
migracion_por_region

## Calulo el promedio del fuljo migratorio agrupando por region geografica
# Al realizar el INNER JOIN se perderan los datos de aquellos paises que no tengan sedes, porque el codigo de pais no aparece en la tabla 
consultaSQL = """
                SELECT flujo.region_geografica,
                paises_con_sedes_Argentinas,
                promedio_flujo_con_Argentina
                FROM (
                      SELECT AVG(flujo_migratorio_neto) AS promedio_flujo_con_Argentina,
                      region_geografica,
                      FROM migracion_por_region
                      GROUP BY region_geografica
                ) AS flujo
                INNER JOIN (
                      SELECT region_geografica,
                      COUNT(DISTINCT pais) AS paises_con_sedes_argentinas,
                      FROM lista_sede_datos_corregida
                      GROUP BY region_geografica
                ) AS paises_sedes 
                ON flujo.region_geografica = paises_sedes.region_geografica;

"""
reporte2 = sql^ consultaSQL 
reporte2

# Guardo el reporte en un archivo excel
reporte2.to_excel(carpeta+'flujo_migratorio_arg.xlsx', index=False)
#%% ==========================================================================================
## iii)

# Hay valores nulos en redes_sociales
lista_sede_datos_corregida.info()

# Los saco porque no suman nada en este reporte
consultaSQL = """
               SELECT *
               FROM lista_sede_datos_corregida
               WHERE redes_sociales IS NOT NULL;
              """

redes_sin_nulls = sql^ consultaSQL
redes_sin_nulls

# Creo una nueva tabla con columnas para cada red social, si la sede tiene dicha red social se asigna un 1, de lo contrario 0

redes = redes_sin_nulls

redes['facebook'] = redes_sin_nulls['redes_sociales'].str.contains('facebook', case=False).astype(int)

redes['instagram'] = redes_sin_nulls['redes_sociales'].str.contains('instagram', case=False).astype(int)

redes['twitter'] = redes_sin_nulls['redes_sociales'].str.contains('twitter', case=False).astype(int)

redes['youtube'] = redes_sin_nulls['redes_sociales'].str.contains('youtube', case=False).astype(int)

redes['cantidad_de_redes'] = redes['facebook'] + redes['instagram'] + redes['twitter'] + redes['youtube']

# Creo una tabla que indica que cantidad de redes sociales que tiene cada pais
consultaSQL = """
               SELECT iso_3,
               pais_de_origen AS pais,
               MAX(cantidad_de_redes) AS cantidad_de_redes
               FROM redes
               INNER JOIN migraciones_corregida
               ON iso_3 = codigo_pais_origen
               GROUP BY iso_3, pais_de_origen;
              """

reporte3 = sql^ consultaSQL
reporte3

# Guardo el reporte en un archivo excel
reporte3.to_excel(carpeta+'tipos_de_redes.xlsx', index=False)
#%% ==========================================================================================
## iv)
# creo una tabla separando los url 
redes2= redes_sin_nulls
redes2['redes_sociales'] = redes2['redes_sociales'].astype(str)
redes2['redes_sociales']= redes2['redes_sociales'].str.strip().str.split(' // ')
redes2_separado = redes2.explode('redes_sociales').reset_index(drop= True)

# creo el reporte que muestra al red social y el url correspondiente por pais
consultaSQL = """
SELECT pais, 
       sede_id AS Sede, 
       redes_sociales AS URL,
       CASE 
            WHEN redes_sociales LIKE '%twitter%' THEN 'Twitter'
            WHEN redes_sociales LIKE '%instagram%' THEN 'Instagram'
            WHEN redes_sociales LIKE '%facebook%' THEN 'Facebook'
            WHEN redes_sociales LIKE '%youtube%' THEN 'Youtube'
       END AS 'Red_Social'
FROM redes2_separado
GROUP BY pais, sede_id, Red_Social, URL
ORDER BY pais ASC, Sede, Red_Social, URL

"""

reporte4 = sql^ consultaSQL

# Guardo el reporte en un archivo excel
reporte4.to_excel(carpeta+'redes_sociales.xlsx', index=False)
#%% ==========================================================================================
# VISUALIZACION
## i)

# creo la tabla que me va a servir para hacer el grafico que me piden
consultaSQL = """
SELECT region_geografica,
       COUNT(sede) AS cant_sedes
FROM lista_sede_datos_corregida
GROUP BY region_geografica

"""
cant_sedes_region = sql^ consultaSQL

# acomo el data frame de manera decreciente
cant_sedes_region = cant_sedes_region.sort_values(by='cant_sedes', ascending=False) 

fig, ax = plt.subplots()
ax.bar(data= cant_sedes_region, x='region_geografica', height='cant_sedes') 
fig, ax = plt.subplots()
plt.rcParams['font.family'] = 'sans-serif'           

ax.bar(data= cant_sedes_region , x='region_geografica', height='cant_sedes')
       

ax.set_xlabel('Region geografica', fontsize='medium')                       
ax.set_ylabel('Cantidad sedes', fontsize='medium')    
ax.set_xlim(0, 8)
ax.set_ylim(0, 50)

ax.set_xticks(range(len(cant_sedes_region))) 
ax.set_xlim(-0.5, len(cant_sedes_region) - 0.5)              
ax.set_yticks([])                          
ax.bar_label(ax.containers[0], fontsize=8)
ax.tick_params(axis='x', labelrotation=90)
ax.bar_label(ax.containers[0], fontsize=8)


plt.show()

#%% ==========================================================================================
## ii)
migraciones_ARG = (migraciones_corregida['codigo_pais_origen'].astype(str) == 'ARG') | (migraciones_corregida['codigo_pais_destino'].astype(str) == 'ARG')
migraciones_con_sedes = migraciones_corregida[migraciones_ARG]
migraciones_con_sedes


# Calculo el flujo migratorio de cada pais con Argentina en cada década
consultaSQL = """
              SELECT (emigracion_sesentas - inmigracion_sesentas) AS flujo_migratorio_sesentas,
                     (emigracion_setentas - inmigracion_setentas) AS flujo_migratorio_setentas,
                     (emigracion_ochentas - inmigracion_ochentas) AS flujo_migratorio_ochentas,
                     (emigracion_noventas - inmigracion_noventas) AS flujo_migratorio_noventas,
                     (emigracion_dosmil - inmigracion_dosmil) AS flujo_migratorio_dosmil,
                     codigo_pais_origen AS iso_3,
              FROM (
                SELECT codigo_pais_origen,
                "1960" AS emigracion_sesentas,
                "1970" AS emigracion_setentas,
                "1980" AS emigracion_ochentas,
                "1990" AS emigracion_noventas,
                "2000" AS emigracion_dosmil
                FROM migraciones_con_sedes
                WHERE pais_destino = 'Argentina'
                  ) AS emigracion
              INNER JOIN (
              SELECT codigo_pais_destino,
              "1960" AS inmigracion_sesentas,
              "1970" AS inmigracion_setentas,
              "1980" AS inmigracion_ochentas,
              "1990" AS inmigracion_noventas,
              "2000" AS inmigracion_dosmil
              FROM migraciones_con_sedes
              WHERE pais_de_origen = 'Argentina'
                ) AS inmigracion
              ON emigracion.codigo_pais_origen = inmigracion.codigo_pais_destino;
              """

flujo_migratorio = sql^ consultaSQL
flujo_migratorio

# Agrupo por region
consultaSQL = """
              SELECT AVG(flujo_migratorio_sesentas),
              AVG(flujo_migratorio_setentas),
              AVG(flujo_migratorio_ochentas),
              AVG(flujo_migratorio_noventas),
              AVG(flujo_migratorio_dosmil),
              region_geografica,
              FROM flujo_migratorio
              INNER JOIN (
              SELECT DISTINCT region_geografica,
              iso_3,
              FROM lista_sede_datos_corregida
              ) AS region
              ON flujo_migratorio.iso_3 = region.iso_3
              GROUP BY region_geografica;

              """
migracion_por_region = sql^ consultaSQL
migracion_por_region
#%% ==========================================================================================
## iii) 

consultaSQL = """     
                SELECT iso_3,
                COUNT(pais) AS paises_con_sedes_argentinas,
                FROM lista_sede_datos_corregida
                GROUP BY iso_3
                ORDER BY paises_con_sedes_argentinas DESC;

"""
sedes = sql^ consultaSQL 
sedes

# Uno las tablas con pandas
migraciones_de_cada_pais = pd.merge(sedes, flujo_migratorio, on='iso_3', how='left')
~/Downloads/sql/
# Reemplazamos los nulls en la columna de sedes por cero
migraciones_de_cada_pais['paises_con_sedes_argentinas'] = migraciones_de_cada_pais['paises_con_sedes_argentinas'].fillna(0)

# Hay paises con fuljo migratorio nulos, los reemplazo por cero
migraciones_de_cada_pais['flujo_migratorio_neto'] = migraciones_de_cada_pais['flujo_migratorio_neto'].fillna(0)
migraciones_de_cada_pais

nombres = migraciones_corregida[['codigo_pais_destino', 'pais_destino']]

# Renombrar la columna codigo_pais_destino a iso_3
nombres = nombres.rename(columns={'codigo_pais_destino': 'iso_3'})

# Agregar los nombres
flujo = pd.merge(migraciones_de_cada_pais, nombres, on='iso_3', how='left')


# Elimino las filas repetidas que se generan
flujo = flujo.drop_duplicates()

# Saco los paises que no consegui el nombre
flujo = flujo.dropna()

flujo

# Crear el grafico de puntos
plt.figure(figsize=(10, 6))

# Graficar el flujo migratorio en función de la cantidad de sedes
plt.scatter(flujo['paises_con_sedes_argentinas'], flujo['flujo_migratorio_neto'], color='b')

plt.xlabel('Cantidad de Sedes')
plt.ylabel('Flujo Migratorio')
plt.title('Relación entre Cantidad de Sedes y Flujo Migratorio')

# Configurar los ticks del eje X
plt.xticks(flujo['paises_con_sedes_argentinas'])  
plt.grid(False) 

# Agragar nombres en algunos puntos
for i in range(len(flujo)):
    if flujo['paises_con_sedes_argentinas'].iloc[i] > 2:  
        pais = flujo['pais_destino'].iloc[i]  
        plt.annotate(pais, 
                     (flujo['paises_con_sedes_argentinas'].iloc[i], flujo['flujo_migratorio_neto'].iloc[i]), 
                     textcoords="offset points", 
                     xytext=(0, 5), 
                     ha='center')

plt.show()

# Los paises con mayor cantidad de sedes tienen un mayor flujo migratorio, en terminos de modulo, aunque no se ve una relacion tan directa