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

#%%
consultaSQL = """
                SELECT nro_conabip, COUNT(*) AS cantidad
                FROM bibliotecas_populares
                GROUP BY nro_conabip;
              """
dataframeResultado = dd.sql(consultaSQL).df()

# con esta consulta, ví que nro_conabip es la PK de BP


#%%
consultaSQL = """
                SELECT DISTINCT Cueanexo
                FROM establecimientos_educativos
              """
dataframeResultado = dd.sql(consultaSQL).df()
#esto verifica que la PK de EE es Cueanexo


#%%
def proporcion_columnas_nulas(df):
    cant_columnas_totales = len(df.columns)
    cant_columnas_vacias = 0
    for columna in df.columns:
        todos_nulos = True
        for elemento in df[columna]:
            if pd.notna(elemento):
                todos_nulos = False
                break
            
        if (todos_nulos):
            cant_columnas_vacias += 1
    return (cant_columnas_vacias * 100)/cant_columnas_totales

cant_columnas_nulas_BP = proporcion_columnas_nulas(bibliotecas_populares)
#%%
def porcentaje_EE_sin_telefono(df):
    cant_EE_sin_telefono = 0
    cant_EE_totales = len(df)
    for fila in df.itertuples():
        if (fila.Teléfono == 'N/D' or fila.Teléfono == 'S/D'):
            cant_EE_sin_telefono +=1
    return (cant_EE_sin_telefono*100)/cant_EE_totales

ee_sin_nro_telefono= porcentaje_EE_sin_telefono(establecimientos_educativos)
















