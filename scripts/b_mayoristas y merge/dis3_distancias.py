# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor
from itertools import product
from datetime import timedelta
import sys


dir=os.getcwd()
dir
sys.path.append(dir)


from minfut0_nombres import *
from scipy.spatial.distance import cdist

# Directorio
os.chdir(os.getcwd())

# Ejemplo de DataFrames df1 y df2 con características numéricas
data2 = pd.read_csv(ruta3 + DF_georef_may, encoding="utf-8", sep=";")
data2.drop_duplicates(subset=["RUC-prov"],inplace=True)
data2["id"] = data2.index
data2.to_csv(ruta3 + DF_georef_may, encoding="utf-8", sep=";")
data2["RUC-prov"] = data2["RUC"].astype(str)+"-"+data2["PROVINCIA_VENDEDOR"]
data2["RUC"]=data2["RUC"].astype(str)
data2.to_csv(ruta3 + DF_georef_may, index=False, encoding="utf-8", sep=";")
data2_ = pd.read_csv(ruta7 + DF_may_fin,encoding='utf-8',sep=';')

# Verificamos el producto
data1 = pd.read_csv(ruta4 + DF_dir, encoding="utf-8", sep=";")
data1 = data1.loc[data1["minorista"]==1]
#data1["id"] = data1.index
data1_ = pd.read_csv(ruta4 + BASE_DLC, encoding="utf-8", sep=";")

# Bucles
dfs=[]
p = 1
#k=19
for k in cod_prods:
    try:
        print(k)
        data2__ = data2_.loc[data2_["COD_PROD"]==k]
        #data2__[['RUC', 'PROVINCIA']] = data2__['RUC-prov'].str.split('-', expand=True)
        #data2__ = data2__.groupby(['fecha_stata', "PROVINCIA","COD_PROD"]).agg({'PRECIOVENTA': 'mean'}).reset_index()
        data2__.drop_duplicates(subset=["RUC-prov"],inplace=True)
        data2__=data2.merge(data2__[["RUC-prov","PRECIOVENTA"]],how="inner")
        data2__.drop(["PRECIOVENTA"],axis=1,inplace=True)
        data1__ = data1_.loc[data1_["COD_PROD"]==k]
        data1__.drop_duplicates(subset="ID_DIR",inplace=True)
        data1__=data1.merge(data1__[["ID_DIR","PRECIOVENTA"]],how="inner")
        data1__.drop(["PRECIOVENTA"],axis=1,inplace=True)
        # DFS
        d2 = pd.DataFrame(data2__[["latitude","longitude"]])
        d1 = pd.DataFrame(data1__[["lat","lon"]])
        # Calcular la matriz de distancias
        d1d2 = cdist(d1, d2, 'cityblock')
        #d1d2_ = np.min(d1d2, axis=1)
        d1d2 = pd.DataFrame(d1d2)
        d1d2.index = data1__["ID_DIR"]
        d1d2.columns = data2__["id"]
        arr=d1d2.values.argsort(1)[:,:3]
        c1=[]
        c2=[]
        c3=[]
        for i in range(len(arr)):
            c1.append(d1d2.columns.tolist()[arr[i,0]])
            c2.append(d1d2.columns.tolist()[arr[i,1]])
            c3.append(d1d2.columns.tolist()[arr[i,2]])
        data1__["COD_PROD"] = k
        data1__["id1"] = c1
        data1__["id2"] = c2
        data1__["id3"] = c3
        #data1__ = data1__.merge(data2__[["RUC-prov","id"]],left_on="ID_DIR",right_on="id")
        exec(f"base_{p}=data1__.copy()")
        dfs.append(f"base_{p}")
        p+=1
    except:
        pass
    
dfs2 = [globals()[f"base_{i}"] for i in range(1, len(dfs) + 1) if f"base_{i}" in globals()]
#del base_1,base_2,base_3,base_4,base_5,base_6,base_7,base_8,base_9
df_concatenado = pd.concat(dfs2, ignore_index=True)
df_concatenado.rename(columns={"RUC_x": "RUC", "RUC_y": "RUC_mayorista"},inplace=True)
del dfs2
df_concatenado2 = df_concatenado[["COD_PROD","ID_DIR","id1","id2","id3"]]
df_concatenado2 = pd.melt(df_concatenado2, id_vars=['COD_PROD', 'ID_DIR'], value_vars=['id1', 'id2', 'id3'], var_name='variable', value_name='id')
df_concatenado2 = df_concatenado2.drop(columns=['variable'])
df_concatenado2 = df_concatenado2.merge(data2[["RUC-prov","id","latitude","longitude"]],on="id")
df_concatenado2.to_csv(ruta4 + DF_dir_may, encoding="utf-8", sep=";")
df_concatenado.to_csv(ruta4 + DF_dir_may2, encoding="utf-8", sep=";")




















