# -*- coding: utf-8 -*-
import os
import pandas as pd
import sys


dir=os.getcwd()
dir
sys.path.append(dir)

from minfut0_nombres import *
from itertools import product

def combse(df,cod):
    df_mean = df.groupby(['DEPARTAMENTO', 'AÑO'])['VOLUMENES'].mean().reset_index()
    df_mean = df_mean[df_mean['AÑO'] > 2017]
    df_mean = df_mean.dropna(subset=['VOLUMENES'])
    df_mean["COD_PROD"] = cod
    df_mean = df_mean[df_mean["VOLUMENES"]>1000]
    df_mean.drop(columns=["VOLUMENES"],inplace=True)
    return df_mean
    
# Combustibles válidos
print("Combustibles válidos")
df = pd.read_csv(ruta8 + DF_val, encoding="utf-8")
valor_a_verificar = 'LIMA'
if valor_a_verificar in df['DEPARTAMENTO'].values:
    nueva_fila = df[df['DEPARTAMENTO'] == valor_a_verificar].copy()
    nueva_fila['DEPARTAMENTO'] = 'CALLAO'
    df = pd.concat([df, nueva_fila], ignore_index=True)
df.columns = df.columns.str.upper()
cols = df["COMBUSTIBLE"].value_counts()
deps = df["DEPARTAMENTO"].unique()
aos = df["AÑO"].unique()
combinaciones = list(product(deps, aos))
df_gnv = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
df_gnv["COD_PROD"] = nom_prods[b]
df_glpg = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
df_glpg["COD_PROD"] = nom_prods[h]
df_glpe = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
df_glpe["COD_PROD"] = nom_prods[g]

#df['VOLUMENES'] = pd.to_numeric(df['VOLUMENES'].str.replace(',', ''), errors='coerce')

df['NM'] = df['MES'].map({'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 'Setiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12})
df.loc[df['VOLUMENES'] == 0, 'VOLUMENES'] = pd.NA


df_gasohol_premium = df[(df['COMBUSTIBLE'] == 'GASOHOL PREMIUM') | (df['COMBUSTIBLE'] == 'Gasohol 95 Plus')]
df_gasohol_premium = combse(df_gasohol_premium,nom_prods[c])
df_gasolina_regular = df[(df['COMBUSTIBLE'] == 'GASOLINA REGULAR') | (df['COMBUSTIBLE'] == 'Gasolina 90')]
df_gasolina_regular = combse(df_gasolina_regular,nom_prods[f])
df_gasolina_premium = df[(df['COMBUSTIBLE'] == 'GASOLINA PREMIUM') | (df['COMBUSTIBLE'] == 'Gasolina 95')]
df_gasolina_premium = combse(df_gasolina_premium,nom_prods[e])
df_gasohol_regular = df[(df['COMBUSTIBLE'] == 'GASOHOL REGULAR') | (df['COMBUSTIBLE'] == 'Gasohol 90 Plus')]
df_gasohol_regular = combse(df_gasohol_regular,nom_prods[d])
df_diesel = df[(df['COMBUSTIBLE'] == 'DIESEL B5 UV') | (df['COMBUSTIBLE'] == 'DB5 S-50')]
df_diesel = combse(df_diesel,nom_prods[a])
df_diesel2 = df[(df['COMBUSTIBLE'] == 'DIESEL B5 S-50 UV') | (df['COMBUSTIBLE'] == 'Diesel B5')]
df_diesel2 = combse(df_diesel2,nom_prods[m])

combs = pd.concat([df_diesel,df_diesel2,df_gnv,df_glpe,df_glpg,df_gasohol_premium, df_gasolina_regular,df_gasolina_premium, df_gasohol_regular], ignore_index=True)
combs["ok"] = 1
combs.sort_values(by=["DEPARTAMENTO","AÑO"],inplace=True)
combs["ID"]=combs["DEPARTAMENTO"] + "-" + combs["COD_PROD"].astype(str)
fecha_minima = combs['AÑO'].min()
fecha_maxima = combs['AÑO'].max()
rango_fechas_completo = list(range(fecha_minima,fecha_maxima))
combinaciones = pd.DataFrame([(id, fecha) for id in combs['ID'].unique() for fecha in rango_fechas_completo], columns=['ID', 'AÑO'])
combs = pd.merge(combinaciones, combs, on=['ID', 'AÑO'], how='outer')
combs[['DEPARTAMENTO', 'COD_PROD']] = combs['ID'].str.split('-', expand=True)
combs.to_csv(ruta4 + DF_val2,index=False)


