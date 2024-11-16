# Análisis de datos de migración en países con sedes de representación argentina
# Franco Algañaraz, Leandro Barrios, Sofía Rodríguez
#%% ==========================================================================================
# Importamos librerias
import pandas as pd
from inline_sql import sql, sql_val
import numpy as np
import matplotlib.pyplot as plt 
from  matplotlib import ticker  
from matplotlib import rcParams
import seaborn as sns 

#%% ==========================================================================================
# Importamos los CSV 
# Creamos una carpeta donde poner las direcciones de cada uno
carpeta = "C:/Users/franc/Documents/Facultad/Laboratorio de datos/TP - 1/"
           
# Creamos los DataFrames
lista_secciones = pd.read_csv(carpeta+"lista-secciones.csv")

lista_sedes = pd.read_csv(carpeta+"lista-sedes.csv")

lista_sedes_datos = pd.read_csv(carpeta+"lista-sedes-datos.csv", on_bad_lines='skip') 

migraciones = pd.read_csv(carpeta+"migraciones.csv") 

#%% ==========================================================================================
# Limpieza de datos

# Hay un Null en sede_id
lista_secciones.info()

# Lo saco porque es la columna mas importante de esta tabla
consultaSQL = """
               SELECT sede_id, tipo_seccion  
               FROM lista_secciones
               WHERE sede_id IS NOT NULL;
              """

seccion = sql^ consultaSQL
print(seccion) 

# Copruebo que ya no hay NULLS
seccion.info()

# Compruebo que no todas las filas de tipo_seccion son 'seccion' porque de lo contrario seria informacion irrelevante 
sin_seccion = seccion['tipo_seccion'] != 'Seccion'
seccion[sin_seccion]



#%% ==========================================================================================

# Las columnas con NULLS son irrelevantes para el analisis 
lista_sedes_datos.info()

consultaSQL = """
               SELECT sede_id, sede_desc_castellano AS nombre_sede, 
               pais_iso_3 AS codigo_pais     
               FROM lista_sedes_datos;
              """

sede = sql^ consultaSQL

#%% ==========================================================================================
consultaSQL = """
SELECT pais_castellano AS nombre_pais, pais_iso_3 AS codigo_pais, region_geografica
FROM lista_sedes_datos
"""
pais= sql^ consultaSQL
pais= pais.drop_duplicates()


#%%

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

# Y ahora si puedo sacar todas aquellas donde las cinco filas son cero
consultaSQL = """
               SELECT codigo_pais_origen, 
               codigo_pais_destino,
               sesentas AS "1960", setentas AS "1970", ochentas AS "1980", noventas AS "1990", dosmil AS "2000"
               FROM migraciones_filtrada
               WHERE sesentas != 0 AND setentas != 0 AND ochentas != 0 AND noventas != 0 AND dosmil != 0;
              """
recibe_gente_de = sql^ consultaSQL
#%%
 #Hay valores nulos en redes_sociales
lista_sedes_datos.info()

# Los saco porque no suman nada en este reporte
consultaSQL = """
               SELECT *
               FROM lista_sedes_datos
               WHERE redes_sociales IS NOT NULL;
              """

redes_sin_nulls = sql^ consultaSQL

# creamos una tabla separando por fila cada URL
redes= redes_sin_nulls
redes['redes_sociales'] = redes['redes_sociales'].astype(str)
redes['redes_sociales']= redes['redes_sociales'].str.strip().str.split(' // ')
red = redes.explode('redes_sociales').reset_index(drop= True)

#creamos la tabla de redes sociales
consultaSQL= """
SELECT sede_id, 
       redes_sociales AS URL,
       CASE 
            WHEN redes_sociales LIKE '%twitter%' THEN 'Twitter'
            WHEN redes_sociales LIKE '%instagram%' THEN 'Instagram'
            WHEN redes_sociales LIKE '%facebook%' THEN 'Facebook'
            WHEN redes_sociales LIKE '%youtube%' THEN 'Youtube'
       END AS 'Red_Social'
FROM red 
GROUP BY sede_id, redes_sociales
"""
red_social = sql^ consultaSQL

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

# Guardo el reporte en un archivo excel
reporte.to_excel(carpeta+'flujo_migratorio_neto.xlsx', index=False)

#%% ==========================================================================================
##ii)
consultaSQL = """
                 SELECT (cantidad_de_inmigracion - cantidad_de_emigracion) AS flujo_migratorio_neto,
                 codigo_pais_origen AS iso_3,
                 FROM (
                     SELECT codigo_pais_origen,
                     "2000" AS cantidad_de_inmigracion
                     FROM recibe_gente_de
                     WHERE codigo_pais_destino = 'ARG'
                 ) AS emigracion
                 INNER JOIN (
                     SELECT codigo_pais_destino,
                     "2000" AS cantidad_de_emigracion
                     FROM recibe_gente_de
                     WHERE 	codigo_pais_origen = 'ARG'
                 ) AS inmigracion
                 ON emigracion.codigo_pais_origen = inmigracion.codigo_pais_destino;
              """

flujo_migratorio = sql^ consultaSQL

consultaSQL = """
                 SELECT flujo_migratorio.flujo_migratorio_neto,
                 flujo_migratorio.iso_3
                 FROM flujo_migratorio
                 INNER JOIN (
                     SELECT DISTINCT codigo_pais 
                     FROM sede
                 ) AS cantidad_sedes
                 ON flujo_migratorio.iso_3 = cantidad_sedes.codigo_pais;
              """

cantidad_de_sedes = sql^ consultaSQL

consultaSQL = """
                 SELECT region_geografica,
                 COUNT(region_geografica) AS paises_con_sedes_argentinas,
                 AVG(flujo_migratorio_neto) AS promedio_flujo_migratorio
                 FROM cantidad_de_sedes
                 INNER JOIN pais
                 ON cantidad_de_sedes.iso_3 = pais.codigo_pais
                 GROUP BY region_geografica
                 ORDER BY region_geografica DESC;
              """

reporte2 = sql^ consultaSQL
reporte2

# Guardo el reporte en un archivo excel
reporte2.to_excel(carpeta+'flujo_migratorio_arg.xlsx', index=False)

#%% ==========================================================================================
## iii)

consultaSQL = """
SELECT nombre_pais, Red_Social
FROM (
      SELECT Red_social, codigo_pais
      FROM red_social
      INNER JOIN sede
      ON red_social.sede_id = sede.sede_id
      ) AS red_codigo
INNER JOIN pais
ON red_codigo.codigo_pais = pais.codigo_pais

"""
red_pais= sql^ consultaSQL

consultaSQL = """
SELECT nombre_pais, COUNT(DISTINCT Red_Social) AS cantidad_redes
FROM red_pais
GROUP BY nombre_pais

"""
reporte3 = sql^ consultaSQL

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

# Guardo las tablas limpias y los reportes en archivos CSV
lista_sede_datos_corregida.to_csv(carpeta+'lista_sede_datos_corregida.csv', index=False)
lista_sedes_corregida.to_csv(carpeta+'lista_sedes_corregida.csv', index=False)
migraciones_corregida.to_csv(carpeta+'migraciones_corregida.csv', index=False)
lista_secciones_corregida.to_csv(carpeta+'lista_secciones_corregida.csv', index=False)
reporte.to_csv(carpeta+'reporte.csv', index=False)
reporte2.to_csv(carpeta+'reporte2.csv', index=False)
reporte3.to_csv(carpeta+'reporte3.csv', index=False)
reporte4.to_csv(carpeta+'reporte4.csv', index=False)

#%% ==========================================================================================
# VISUALIZACION
## i)

# Creo la tabla para el grafico de barras
consultaSQL = """
SELECT region_geografica,
       COUNT(sede) AS cant_sedes
FROM lista_sede_datos_corregida
GROUP BY region_geografica

"""
cant_sedes_region = sql^ consultaSQL

# Acomodo el data frame de manera decreciente
cant_sedes_region = cant_sedes_region.sort_values(by='cant_sedes', ascending=False) 

fig, ax = plt.subplots()
ax.bar(data= cant_sedes_region, x='region_geografica', height='cant_sedes', color='blue') 
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
# Creo un dataframe con los flujos migratorios de cada pais
consultaSQL = """
                 SELECT (cantidad_de_inmigracion - cantidad_de_emigracion) AS flujo_migratorio_neto,
                 codigo_pais_origen AS iso_3,
                 FROM (
                     SELECT codigo_pais_origen,
                     ("1960" + "1970" + "1980" + "1990" + "2000") AS cantidad_de_emigracion
                     FROM migraciones_con_sedes
                     WHERE pais_destino = 'Argentina'
                 ) AS emigracion
                 INNER JOIN (
                     SELECT codigo_pais_destino,
                     ("1960" + "1970" + "1980" + "1990" + "2000") AS cantidad_de_inmigracion
                     FROM migraciones_con_sedes
                     WHERE pais_de_origen = 'Argentina'
                 ) AS inmigracion
                 ON emigracion.codigo_pais_origen = inmigracion.codigo_pais_destino;
              """

flujo_completo = sql^ consultaSQL

# Ahora agrego la region y los agrupo 
consultaSQL = """
              SELECT AVG(flujo_migratorio_neto) AS flujo_migratorio,
              region_geografica,
              flujo_completo.iso_3
              FROM flujo_completo
              INNER JOIN (
              SELECT DISTINCT region_geografica,
              iso_3,
              FROM lista_sede_datos_corregida
              ) AS region
              ON flujo_completo.iso_3 = region.iso_3
              GROUP BY region_geografica, flujo_completo.iso_3;

              """
migracion_por_region = sql^ consultaSQL

# Calculo la mediana de los flujos migratorios por región
medianas = migracion_por_region.groupby('region_geografica')['flujo_migratorio'].median().sort_values()

# Ordeno las regiones según la mediana
migracion_por_region['region_geografica'] = pd.Categorical(migracion_por_region['region_geografica'], categories=medianas.index, ordered=True)

# Creo el boxplot usando seaborn
plt.figure(figsize=(10, 6))
sns.boxplot(x='region_geografica', y='flujo_migratorio', data=migracion_por_region, color='beige')

plt.title('Boxplot de Flujos Migratorios por Región Geográfica')
plt.xlabel('Región Geográfica')
plt.ylabel('Flujo Migratorio')

plt.xticks(rotation=60)

# Cambio la escala del eje y para no mostrar valores en notación científica
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

# Establezco límites del eje y
plt.ylim(-1050000, 370000)

# Agrego líneas de referencia cada 100000
for y in range(-1000000, 400000, 100000):
    plt.axhline(y=y, color='grey', linestyle='--', linewidth=0.7)  

# Guardo el gráfico
plt.savefig('boxplot_flujo_migratorio.png', dpi=300, bbox_inches='tight')

plt.show()

# Boxplot de las regiones que no se ven bien
# Boxplot Asia
asia = migracion_por_region[migracion_por_region['region_geografica'] == 'ASIA']

plt.figure(figsize=(6, 6))
boxplot = sns.boxplot(y='flujo_migratorio', data=asia)
boxplot.set_xlabel('Asia', fontsize=14)
boxplot.set_ylabel('Flujo Migratorio', fontsize=14)

plt.ylim(-40000, 60000)  

plt.title('Boxplot de Flujo migratorio Asia', fontsize=16)
plt.savefig(carpeta+'boxplot_Asia.png', dpi=300, bbox_inches='tight')
plt.show()

# Boxplot Europa central y oriental

# Elimino espacios adicionales y limpio la columna
migracion_por_region['region_geografica'] = (
    migracion_por_region['region_geografica']
    .str.replace(r'\s+', ' ', regex=True)  
    .str.strip()  
)

# Filtro los datos
eur_cent = migracion_por_region[migracion_por_region['region_geografica'] == 'EUROPA CENTRAL Y ORIENTAL']
eur_cent

plt.figure(figsize=(6, 6))
boxplot = sns.boxplot(y='flujo_migratorio', data=eur_cent)
boxplot.set_xlabel('Europa central y oriental', fontsize=14)
boxplot.set_ylabel('Flujo Migratorio', fontsize=14)

plt.ylim(-350000, 10000)  

plt.title('Boxplot de Flujo migratorio Europa central y oriental', fontsize=16)
plt.savefig(carpeta+'boxplot_Europa_central', dpi=300, bbox_inches='tight')
plt.show()

# Boxplot Europa occidental 

# Elimino espacios adicionales y limpio la columna
migracion_por_region['region_geografica'] = (
    migracion_por_region['region_geografica']
    .str.replace(r'\s+', ' ', regex=True)  
    .str.strip()  
)

# Filtro los datos
eur_occ = migracion_por_region[migracion_por_region['region_geografica'] == 'EUROPA OCCIDENTAL']
eur_occ

plt.figure(figsize=(6, 6))
boxplot = sns.boxplot(y='flujo_migratorio', data=eur_occ)
boxplot.set_xlabel('Europa occidental', fontsize=14)
boxplot.set_ylabel('Flujo Migratorio', fontsize=14)

plt.ylim(-150000, 20000)  

plt.title('Boxplot de Flujo migratorio Europa occidental', fontsize=16)
plt.savefig(carpeta+'boxplot_Europa_occidental.png', dpi=300, bbox_inches='tight')
plt.show()

# Boxplot America central y caribe

# Elimino espacios adicionales y limpio la columna
migracion_por_region['region_geografica'] = (
    migracion_por_region['region_geografica']
    .str.replace(r'\s+', ' ', regex=True)  
    .str.strip()  
)

# Filtro los datos
ac_c = migracion_por_region[migracion_por_region['region_geografica'] == 'AMÉRICA CENTRAL Y CARIBE']
ac_c

plt.figure(figsize=(6, 6))
boxplot = sns.boxplot(y='flujo_migratorio', data=ac_c)
boxplot.set_xlabel('America central y caribe', fontsize=14)
boxplot.set_ylabel('Flujo Migratorio', fontsize=14)

plt.ylim(-4000, 6000) 

plt.title('Boxplot de Flujo migratorio America central y caribe', fontsize=16)
plt.savefig(carpeta+'boxplot_America_central.png', dpi=300, bbox_inches='tight')
plt.show()

#%% ==========================================================================================
## iii) 

# Creo un data frame con las sedes necesarias
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

# Reemplazamos los nulls en la columna de sedes por cero
migraciones_de_cada_pais['paises_con_sedes_argentinas'] = migraciones_de_cada_pais['paises_con_sedes_argentinas'].fillna(0)

# Hay paises con flujo migratorio nulos, los reemplazo por cero
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

# Crear el gráfico de puntos
plt.figure(figsize=(8, 6))
 
plt.scatter(flujo['paises_con_sedes_argentinas'], flujo['flujo_migratorio_neto'], color='k')  
plt.xlabel('Cantidad de sedes')
plt.ylabel('Flujo migratorio')
plt.title('Relación entre cantidad de sedes y flujo migratorio')

plt.xticks(flujo['paises_con_sedes_argentinas'])  
plt.grid(False) 

# Guardar la figura 
plt.savefig(carpeta+'grafico_flujo_migratorio.png', dpi=300, bbox_inches='tight')  

plt.show()
