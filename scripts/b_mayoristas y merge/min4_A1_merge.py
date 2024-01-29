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
import sys
import concurrent.futures

dir=os.getcwd()
dir
sys.path.append(dir)

from minfut0_nombres import *

# Directorio
os.chdir(os.getcwd())

# Finally
d1 = pd.read_csv(ruta + DF_imp, encoding="utf-8", sep=";")
df = pd.read_csv(ruta4 + DF_dir_may2,encoding='utf-8',sep=";")
df = df[["COD_PROD","ID_DIR","id1","id2","id3"]]
d22 = pd.read_csv(ruta3 + DF_georef_may, encoding="utf-8", sep=";")
d22 = d22[["RUC-prov","id"]]

df = df.merge(d22,left_on="id1",right_on="id")
df.drop(["id"],axis=1,inplace=True)
df.rename(columns={"RUC-prov": "RUC-prov1"},inplace=True)
df = df.merge(d22,left_on="id2",right_on="id")
df.drop(["id"],axis=1,inplace=True)
df.rename(columns={"RUC-prov": "RUC-prov2"},inplace=True)
df = df.merge(d22,left_on="id3",right_on="id")
df.drop(["id"],axis=1,inplace=True)
df.rename(columns={"RUC-prov": "RUC-prov3"},inplace=True)

#df = df.drop_duplicates(subset=["PROVINCIA","DEPARTAMENTO","RUC"])
d2 = pd.read_csv(ruta7 + DF_may_fin,encoding='utf-8',sep=';')
d2.rename(columns={"PRECIOVENTA": "PRECIOVENTA_may"},inplace=True)
dataframes_list = []

def process_k(k):
    print(k)
    df_ = df.loc[df["COD_PROD"] == k]
    df_ = df_["ID_DIR"].unique()
    print(len(df_))
    
    result_list = []
    for j in range(len(df_)):
        df__ = df.loc[(df["COD_PROD"] == k) & (df["ID_DIR"] == df_[j])]
        ver1 = df__.loc[:, "RUC-prov1"].values[0]
        ver2 = df__.loc[:, "RUC-prov2"].values[0]
        ver3 = df__.loc[:, "RUC-prov3"].values[0]
        d2_ = d2.loc[((d2["RUC-prov"] == ver1) | (d2["RUC-prov"] == ver2) | (d2["RUC-prov"] == ver3)) & (d2["COD_PROD"] == k)]
        d2_ = d2_.groupby(['fecha_stata'])["PRECIOVENTA_may"].mean().reset_index()
        d2_["ID_DIR"] = df_[j]
        d2_["COD_PROD"] = k
        result_list.append(d2_)

    return result_list

# Lista para almacenar los resultados
dataframes_list = []

# Número de hilos (ajústalo según sea necesario)
num_threads = 8

# Paralelizar el bucle principal
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = {executor.submit(process_k, k): k for k in cod_prods}
    for future in concurrent.futures.as_completed(futures):
        try:
            dataframes_list.extend(future.result())
        except Exception as e:
            print(f"Error: {e}")

dfs = pd.concat(dataframes_list, ignore_index=True)
dfs = dfs.dropna()
dfs.head()
del dataframes_list
d1 = d1.merge(dfs, on=["COD_PROD","ID_DIR","fecha_stata"],how="left",indicator=True)
d1._merge.value_counts()
d1.drop(["_merge"],axis=1,inplace=True)
#d1["RUC_mayorista"]=d1["RUC_mayorista"].astype(str).str.rstrip('.0')
#print(d1.head())
#d2.rename(columns={"RUC": "RUC_mayorista"},inplace=True)
#d1.head()
d1.to_csv(ruta6 + DF_fin,index=False,encoding='utf-8',sep=";")









