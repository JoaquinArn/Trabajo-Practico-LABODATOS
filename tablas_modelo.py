# -*- coding: utf-8 -*-
"""
TABLAS A USAR

@author: Joaco
"""

import numpy as np
import pandas as pd
import duckdb as dd

#%% Tablas originales para formación de las que vamos a usar : BP y EE

bibliotecas_populares = pd.read_csv('bibliotecas-populares.csv')

ee = pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx', sheet_name = 'padron2022', skiprows = 5, header = [0,1])
#después de analaizarlo, llego a la conclusión de que no está ni siquiera en primera forma normal
#tiene relaciones dentro de relaciones

#Modificamos la columna Unnamed: 43_level_1 por vacío en ee
# Extrae la lista actual de columnas (cada columna es una tupla)
columnas = ee.columns.tolist()

# Armamos una nueva lista de columnas la cual reemplaza el Unnamed: 43_level_1
nuevas_columnas = []
for columna in columnas:
    if (columna == ('Servicios complementarios', 'Unnamed: 43_level_1')):
        columna = ('Servicios complementarios', ' ')
    nuevas_columnas.append(columna)

# Reasignamos los nombres con el cambio realizado
ee.columns = pd.MultiIndex.from_tuples(nuevas_columnas)

#%% Tablas originales para formación de las que vamos a usar : departamento parte 1
#Pruebo como hacer la primer tabla
df_raw = pd.read_excel("padron_poblacion.xlsx", header=None)

area_val = df_raw.iloc[13, 1]    # B14: fila 13, columna 1
comuna_val = df_raw.iloc[13, 2]   # C14: fila 13, columna 2

df_tabla = pd.read_excel("padron_poblacion.xlsx", header=15)

df_tabla["Area"] = area_val
df_tabla["Comuna"] = comuna_val

#%% Tablas originales para formación de las que vamos a usar : pp parte 2
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


#%% AHORA LAS QUE VAMOS A USAR: bp
consultaSQL = """
                SELECT nro_conabip, id_departamento AS id_depto, mail, fecha_fundacion
                FROM bibliotecas_populares
              """

bp = dd.sql(consultaSQL).df()
#%%
consultaSQL = """
                SELECT nro_conabip, id_departamento AS id_depto, mail, fecha_fundacion
                FROM bibliotecas_populares
                WHERE mail IS NOT NULL
              """

b = dd.sql(consultaSQL).df()

#devolvió 1022 filas

consultaSQL = """
                SELECT nro_conabip, id_departamento AS id_depto, mail, fecha_fundacion
                FROM bibliotecas_populares
                WHERE mail LIKE '%@%'
              """
r = dd.sql(consultaSQL).df()
#devolvió 1022 filas, lo que implica que todos los mails llevan un @
#es así que identifico las que tienen dos (o más) mails contando los @ que tiene

consultaSQL = """
                SELECT nro_conabip, id_departamento AS id_depto, mail, fecha_fundacion
                FROM bibliotecas_populares
                WHERE mail LIKE '%@%@%'
              """
re = dd.sql(consultaSQL).df()
#devolvió una única fila. Observamos que ésta biblioteca no posee dos mails, sino que está el mismo
#hacemos limpieza

bp['mail'] = bp['mail'].replace({'sanestebanbibliotecapopular@yahoo.com.ar <SANESTEBANBIBLIOTECAPOPULAR@YAHOO.COM.AR>': 'sanestebanbibliotecapopular@yahoo.com.ar'})

#Luego, cambiamos el id de Chascomús de 6217 a 6218 que es el que indican las tablas EE y PP
bp['id_depto'] = bp['id_depto'].replace({6217: 6218})



#%% AHORA LAS QUE VAMOS A USAR: localizacion_ee

localizacion_ee = ee[[('Establecimiento - Localización', 'Cueanexo'), ('Establecimiento - Localización', 'Código de localidad')]]
nuevas_columnas = ['cueanexo', 'id_depto'] #renombro las columnas
localizacion_ee.columns = nuevas_columnas
localizacion_ee.astype(int)
localizacion_ee.loc[:, 'id_depto'] = localizacion_ee['id_depto'] // 1000

localizacion_ee = localizacion_ee.where((localizacion_ee['id_depto']//1000) == 2, 2000) # si empieza con 2 es de capi, entonces ponemos 2000
#%% AHORA LAS QUE VAMOS A USAR: nivel_educativo_ee

#Yo quiero armar dos listas a las cuales pasar después a un diccionario
#Estas listas tendrán la misma longitud en cada momento
#La posición i de la lista 'establecimientos' marcará la clave de un establecimiento; y la posicion i de la lista 'tipos' informará el tipo de establecimientos
# Si un establecimiento cumple con más de un tipo, entonces aparecerá otro registro en una posicion j en la que en 'establecimientos' se informe su clave y en 'tipos' su otro tipo  
establecimientos = []
tipos = []
for fila in ee.iterrows(): #en iterrows, [0] es el índice de fila y [1] es la serie en la que están cada uno de los atributos
    if (fila[1][('Común', 'Nivel inicial - Jardín maternal')] == 1 or fila[1][('Común', 'Nivel inicial - Jardín de infantes')] == 1):
        establecimientos.append(fila[1][('Establecimiento - Localización', 'Cueanexo')])
        tipos.append('jardin')
    if (fila[1][('Común', 'Primario')] == 1):
        establecimientos.append(fila[1][('Establecimiento - Localización', 'Cueanexo')])
        tipos.append('primario')
    if (fila[1][('Común', 'Secundario')] == 1 or fila[1][('Común', 'Secundario - INET')] == 1 or fila[1][('Común', 'SNU')] == 1):
        establecimientos.append(fila[1][('Establecimiento - Localización', 'Cueanexo')])
        tipos.append('secundario')


nivel_educativo_ee = {'cueanexo' : [], 'tipo' : []} #inicializo dict vacío donde irán las claves y sus tipos (notar que los arrays están ordenados)
nivel_educativo_ee['cueanexo'] = establecimientos
nivel_educativo_ee['tipo'] = tipos

nivel_educativo_ee = pd.DataFrame(data = nivel_educativo_ee) #la primer columna serán las claves y la segunda los tipos. Como los arrays los dí ordenados, el dataframe contendrá la info correctamente

#%% AHORA LAS QUE VAMOS A USAR: pp

pp = resultado[['Area', 'Nombre']].drop_duplicates() #agarro las columnas area y nombre, sin pares duplicados
pp['Area'] = pp['Area'].astype(str)

data_pp = {'Area' : [], 'pob_infantes': [], 'pob_primaria' : [], 'pob_secundaria' : [], 'pob_total' : []}
sumaPobJardin = 0
sumaPobPrimaria = 0
sumaPobSecundaria = 0
area = '02007' #selecciono el primer área
for fila in resultado.iterrows():
    if (fila[1]['Area'] != area or fila[0] == len(resultado) - 1):
        data_pp['Area'].append(area)
        data_pp['pob_infantes'].append(sumaPobJardin)
        data_pp['pob_primaria'].append(sumaPobPrimaria)
        data_pp['pob_secundaria'].append(sumaPobSecundaria)
        area = fila[1]['Area']
        sumaPobJardin = 0
        sumaPobPrimaria = 0
        sumaPobSecundaria = 0
        
    if (str(fila[1]['Edad']) != 'Total' and int(fila[1]['Edad']) in range(0, 6)):
        sumaPobJardin += int(fila[1]['Casos'])
    
    if (str(fila[1]['Edad']) != 'Total' and int(fila[1]['Edad']) in range(6, 13)):
        sumaPobPrimaria += int(fila[1]['Casos'])
        
    if (str(fila[1]['Edad']) != 'Total' and int(fila[1]['Edad']) in range(13, 19)):
        sumaPobSecundaria += int(fila[1]['Casos'])
    
    if (str(fila[1]['Edad']) == 'Total'):
        data_pp['pob_total'].append(int(fila[1]['Casos']))
    
dfaux = pd.DataFrame(data = data_pp)

consultaSQL = """
                SELECT *
                FROM pp
                NATURAL JOIN dfaux
              """

pp = dd.sql(consultaSQL).df()

for indice, fila in pp.iterrows():
    if (int(fila['Area']) < 10000) :
        pp.at[indice, 'Area'] = fila['Area'][1:]
        

#%%

consultaSQL = """
                SELECT Area AS id_depto, Nombre AS nombre_depto, CAST(Area AS INTEGER)//1000 AS id_provincia, pob_infantes, pob_primaria, pob_secundaria, pob_total 
                FROM pp
              """

departamento = dd.sql(consultaSQL).df()

# Además, vamos a modificar los códigos de area de Ushuaia y Río Grande, pues en las tablas de establecimientos y bibliotecas aparecen con otros
departamento['id_depto'] = departamento['id_depto'].astype(int)

departamento['id_depto'] = departamento['id_depto'].replace({94008: 94007, 94015: 94014})

#Ahora todas las comunas las encapsulamos en una única fila que sea Ciudad de Buenos Aires

for nombre in departamento['nombre_depto']:
    if (nombre.startswith('Comuna')):
        departamento['nombre_depto'] = departamento['nombre_depto'].replace({nombre: 'Ciudad de Buenos Aires'})

# Filtrar solo las filas con nombre "Ciudad de Buenos Aires"
departamentos_de_ciudad_bsas = departamento[departamento['nombre_depto'] == 'Ciudad de Buenos Aires'].groupby('nombre_depto')[['pob_infantes', 'pob_total', 'pob_primaria', 'pob_secundaria']].sum().reset_index()
departamentos_de_ciudad_bsas['id_depto'] = 2000
departamentos_de_ciudad_bsas['id_provincia'] = 2

# Filtrar el resto del DataFrame (sin las filas agrupadas)
departamentos_fuera_de_ciudad_bsas = departamento[departamento['nombre_depto'] != 'Ciudad de Buenos Aires']

# Combinamos ambos
departamento = pd.concat([departamentos_fuera_de_ciudad_bsas, departamentos_de_ciudad_bsas], ignore_index=True)


#%% NO LA VAMOS A USAR, PERO POR LAS DUDAS: jurisdiccion_departamento


jurisdiccion_departamento = ee[[('Establecimiento - Localización', 'Departamento'), ('Establecimiento - Localización', 'Jurisdicción')]].drop_duplicates()
nuevas_columnas = ['departamento', 'jurisdiccion'] #renombro las columnas
jurisdiccion_departamento.columns = nuevas_columnas



#%% TABLA A USAR: PROVINCIA

consultaSQL = """
                SELECT DISTINCT id_provincia, provincia AS nombre_provincia
                FROM bibliotecas_populares
              """

provincia = dd.sql(consultaSQL).df()


#%% NO SIRVE PERO GUARDO POR LAS DUDAS

localidades_en_departamento = pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx', sheet_name = 'padron2022', skiprows = 6)
localidades_en_departamento = localidades_en_departamento[['Código de localidad', 'Departamento']]
localidades_en_departamento.rename(columns = {'Código de localidad' : 'cod_loc'}, inplace = True)

consultaSQL = """
                SELECT DISTINCT cod_loc, UPPER(Departamento) AS departamento
                FROM localidades_en_departamento
              """

localidades_en_departamento = dd.sql(consultaSQL).df()

#noto que acá aparece la antártida pero en departamento no; parece que en pp no está la antartida pero sí en ee
#ushuaia tiene diferente id en ee y bp que en pp




