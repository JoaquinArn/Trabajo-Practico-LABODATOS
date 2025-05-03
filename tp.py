# -*- coding: utf-8 -*-
"""

@author: Joaco
"""
import numpy as np
import pandas as pd
import duckdb as dd

#%% Tablas originales
establecimientos_educativos = pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx', sheet_name = 'padron2022', skiprows = 6)
#después de analaizarlo, llego a la conclusión de que no está ni siquiera en primera forma normal
#tiene relaciones dentro de relaciones

bibliotecas_populares = pd.read_csv('bibliotecas-populares.csv')
#padron = pd.read_csv('padron_poblacion.xlsX - Output')

#%%
consultaSQL = """
                SELECT cod_localidad, nombre, COUNT(*) AS cantidad
                FROM bibliotecas_populares
                GROUP BY cod_localidad, nombre;
              """
dataframeResultado = dd.sql(consultaSQL).df()

# con esta consulta, ví que la Bib.Pop Florentino Ameghino (cod_localidad: 6441030) está repetida

#%%
consultaSQL = """
                SELECT *
                FROM bibliotecas_populares
                WHERE (
                    SELECT COUNT(*) 
                    FROM bibliotecas_populares AS b2
                    WHERE b2.cod_localidad = bibliotecas_populares.cod_localidad
                    AND LEVENSHTEIN(b2.nombre, bibliotecas_populares.nombre) <=3
                    ) > 1;
              """
dataframeResultado = dd.sql(consultaSQL).df()
# con esta consulta, veo que no son iguales pero por alguna razón tienen el mismo cod_localidad cuando pertenecen a distintas localidades
# es raro el mail de una de las dos pues su mail no da indicios de que se está hablando de una bp llamada Florentino Ameghino
#%%
consultaSQL = """
                SELECT cod_localidad
                FROM bibliotecas_populares
                WHERE localidad = 'La Plata';
              """
dataframeResultado = dd.sql(consultaSQL).df()
#%%
#Pruebo como hacer la primer tabla
df_raw = pd.read_excel("padron_poblacion.xlsx", header=None)

area_val = df_raw.iloc[13, 1]    # B14: fila 13, columna 1
comuna_val = df_raw.iloc[13, 2]   # C14: fila 13, columna 2

df_tabla = pd.read_excel("padron_poblacion.xlsx", header=15)

df_tabla["Area"] = area_val
df_tabla["Comuna"] = comuna_val

#%%
df = pd.read_excel('padron_poblacion.xlsx', header=None)
n_rows = len(df)

segmentos = []
i = 0
col_area   = 1   # columna B porque indica area
col_nombre = 2   # columna C porque indica nombre

while i < n_rows:
    celda = df.iat[i, col_area] #recorro celdas de la columna b
    # Solo miro en columna B si empieza con "AREA"
    if isinstance(celda, str) and celda.strip().startswith('AREA'):
        area   = celda.strip()[-5:]               # últimos 5 caracteres pues es un patron que se repite
        nombre = df.iat[i, col_nombre]            # valor en columna C

        # Cabecera dos filas abajo
        header_row = i + 2
        if header_row >= n_rows:
            break
        header = df.iloc[header_row].tolist() #pongo como header la de la tabla

        # Leo hasta fila vacía y voy guardando las filas como listas en el df
        j = header_row + 1
        filas = []
        while j < n_rows and not df.iloc[j].isnull().all():
            filas.append(df.iloc[j].tolist())
            j += 1

        # Si hay datos, creo el sub‑DataFrame
        if filas:
            sub = pd.DataFrame(filas, columns=header)
            sub['Area']   = area
            sub['Nombre'] = nombre
            segmentos.append(sub)

        # Salto al final de esta mini‑tabla
        i = j + 1
        continue

    i += 1

#Concateno

resultado = pd.concat(segmentos, ignore_index=True)
resultado = resultado.drop(resultado.columns[0], axis=1) #Elimino la columna A que esta llena de NULLS

