import pandas as pd
import numpy as np
import os
import sys


dir=os.getcwd()
dir
sys.path.append(dir)

import glob
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor
from itertools import product
from minfut0_nombres import *
from minfut3_utils_clean import *

# Desactivar todas las advertencias de pandas
warnings.filterwarnings("ignore")

# Directorio
os.chdir(os.getcwd())

# Base grande (previo obligatorio)
print("Base2")
df = pd.read_csv(ruta4 + BASE_DLC, encoding="utf-8", sep=";")
df=agg_pan(df)

# df['per'] = 1
# df.loc[df['fecha_stata'] >= pd.to_datetime(
#     "01/03/2020", format='%d/%m/%Y'), 'per'] = 2
# df.loc[df['fecha_stata'] >= pd.to_datetime(
#     "01/03/2021", format='%d/%m/%Y'), 'per'] = 3
# for i in cod_prods:
#     print(i)
#     d = df.loc[df["COD_PROD"] == i]
#     reps(d)

# Limpieza
#df = pd.read_csv(r"..\data\interim\BASETOTAL_COMBUSTIBLES3.csv", encoding='utf-8')
#df['fecha_stata'] = pd.to_datetime(df['fecha_stata'], infer_datetime_format=True, errors='coerce')
df_ = df.copy()
print("limpiando")
df_ = limp(nom_prods[a], 25, 5, 8, 2, df_, df)
df_ = limp(nom_prods[b], 5, 1, 3, 0.25, df_, df)
df_ = limp(nom_prods[f], 27, 1.5, 10, 3.5, df_, df)
df_ = limp(nom_prods[e], 30, 7, 10, 3.5, df_, df)
df_ = limp(nom_prods[d], 29, 4.5, 14, 3.5, df_, df)
df_ = limp(nom_prods[c], 32, 4.5, 10, 1.5, df_, df)
df_ = limp(nom_prods[g], 75, 20, 30, 2, df_, df)
df_ = limp(nom_prods[h], 10, 1, 5, 1.5, df_, df)
df_ = limp(nom_prods[m], 30, 4.5, 5, 2.5, df_, df)

# Finale
#df_.to_csv(r"..\data\interim\BASETOTAL_COMBUSTIBLES4.csv", index=False)

# Imputación
#df_ = pd.read_csv(r"..\data\interim\BASETOTAL_COMBUSTIBLES4.csv", encoding="utf-8")
#df_['fecha_stata'] = pd.to_datetime(df_['fecha_stata'], infer_datetime_format=True, errors='coerce')

print("limpiando 2")
for k in cod_prods:
    print(k)
    df_2 = df_.copy()
    df_2 = df_2[df_2['COD_PROD'] == k]
    #df_2 = pd.merge(df_2, dir, on='ID_DIR', how='inner')
    df_2['PRECIOVENTA_'] = df_2['PRECIOVENTA']
    df_2 = df_2.groupby(['fecha_stata', 'ID_DIR']).agg({'PRECIOVENTA': 'mean', 'PRECIOVENTA_': 'last'}).reset_index()
    df_2 = df_2.sort_values(['ID_DIR', 'fecha_stata'])
    df_2['dias_faltantes'] = (df_2['fecha_stata'].diff()).dt.days - 1
    df_2.loc[df_2["dias_faltantes"] < 0, "dias_faltantes"] = np.nan
    df_2['dias_faltantes'] = df_2['dias_faltantes'].fillna(0)
    fecha_minima = df_2['fecha_stata'].min()
    fecha_maxima = df_2['fecha_stata'].max()
    rango_fechas_completo = pd.date_range(fecha_minima, fecha_maxima, freq='D')
    combinaciones = pd.DataFrame([(id, fecha) for id in df_2['ID_DIR'].unique(
    ) for fecha in rango_fechas_completo], columns=['ID_DIR', 'fecha_stata'])
    df_2 = pd.merge(combinaciones, df_2, on=['ID_DIR', 'fecha_stata'], how='outer')
    df_2 = df_2.sort_values(by=['ID_DIR', 'fecha_stata'])
    df_2 = df_2.reset_index(drop=True)
    num_cpus = os.cpu_count()
    print("p1")
    #with ProcessPoolExecutor(max_workers=num_cpus) as executor:
    #    executor.map(process, ["PRECIOVENTA_","dias_faltantes"])
    #df_2=process("PRECIOVENTA",df_2)
    #df_2=process("dias_faltantes",df_2)
    #df_2['PRECIOVENTAx'] = df_2['PRECIOVENTA_']
    print("p2")
    q = 1
    for i in range(1, len(df_2)):
        if q < 14:
            if (df_2.at[i, 'ID_DIR'] == df_2.at[i-1, 'ID_DIR']) and pd.isnull(df_2.at[i, 'PRECIOVENTA']):
                df_2.at[i, 'PRECIOVENTA'] = df_2.at[i-1, 'PRECIOVENTA']
            q += 1

        if i < len(df_2) - 1 and not pd.isnull(df_2.at[i+1, 'PRECIOVENTA']):
             q = 1
    
    #mask = (df_2['ID_DIR'] == df_2['ID_DIR'].shift(1)) & pd.isnull(df_2['PRECIOVENTA'])    
    #df_2['PRECIOVENTA'] = df_2['PRECIOVENTA'].mask(mask, df_2['PRECIOVENTA'].ffill())    
    #reset_mask = ~pd.isnull(df_2['PRECIOVENTA'].shift(-1))    
    #df_2['q'] = (reset_mask.cumsum() % 14) + 1    
    #df_2 = df_2[df_2['q'] < 14].copy()    
    #df_2 = df_2.drop(columns=['q'])
    
    #df_2.drop(columns=['PRECIOVENTAx', 'PRECIOVENTA_','dias_faltantes'], inplace=True)
    df_2["COD_PROD"] = k
    df_2.to_csv(f"{ruta6}base_final_{k}.csv", index=False)

# Append y byes
del df_
dfs = []
p = 1
for k in cod_prods:
    print(k)
    exec(f'base_{p} = pd.read_csv("{ruta6}base_final_{k}.csv", encoding="utf-8")')
    dfs.append(f"base_{p}")
    exec(f'os.remove("{ruta6}base_final_{k}.csv")')
    p += 1
dfs2 = [globals()[f"base_{i}"] for i in range(1, len(dfs) + 1) if f"base_{i}" in globals()]
del base_1,base_2,base_3,base_4,base_5,base_6,base_7,base_8,base_9
df_concatenado = pd.concat(dfs2, ignore_index=True)
del dfs2
#df_concatenado.to_csv(r"..\data\interim\base_final.csv", index=False)

# Variables finales
df_concatenado["ID_COL"] = df_concatenado["ID_DIR"].astype(str) + "-" + df_concatenado["COD_PROD"].astype(str)
df_concatenado["dPRECIOVENTA"] = df_concatenado.groupby("ID_COL")["PRECIOVENTA"].diff()
df_concatenado["dvarPRECIOVENTA"] = df_concatenado.groupby("ID_COL")["PRECIOVENTA"].pct_change() * 100
df_concatenado.loc[abs(df_concatenado["dvarPRECIOVENTA"])>5,"raro"]=1
df_concatenado["raro"] = df_concatenado["raro"].fillna(0)
df_concatenado.loc[abs(df_concatenado["dPRECIOVENTA"])>5,"raro2"]=1
df_concatenado["raro2"] = df_concatenado["raro2"].fillna(0)
df_concatenado.to_csv(ruta + DF_imp, index=False, encoding="utf-8", sep=";")





















