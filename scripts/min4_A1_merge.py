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
from minfut0_nombres import *

# Directorio
os.chdir(os.getcwd())

# Finally
d1 = pd.read_csv(ruta + DF_imp, encoding="utf-8", sep=";")
df = pd.read_csv(ruta7 + DF_may,encoding='utf-8',sep=";")
df = df.drop_duplicates(subset=["PROVINCIA","DEPARTAMENTO","RUC"])
d2 = pd.read_csv(ruta7 + DF_may_fin,encoding='utf-8',sep=';')
d2.rename(columns={"PRECIOVENTA": "PRECIOVENTA_may"},inplace=True)
d2[['RUC', 'PROVINCIA']] = d2['RUC-prov'].str.split('-', expand=True)
d3 = d2.groupby(['fecha_stata', "PROVINCIA","COD_PROD"]).agg({'PRECIOVENTA_may': 'mean'}).reset_index()
df["RUC"]=df["RUC"].astype(str)
d2 = d2.merge(df[["PROVINCIA","DEPARTAMENTO","RUC","COD_PROD"]],on=["RUC","PROVINCIA","COD_PROD"])
d1 = d1.merge(dir,on="ID_DIR",how="left")
d1 = d1[['ID_DIR', 'fecha_stata', 'PRECIOVENTA', 'COD_PROD', 'ID_COL','dPRECIOVENTA', 'dvarPRECIOVENTA', 'raro', 'raro2','PRECIOVENTAx',"PRECIOVENTA_",'ID_DPD','RUC']]
d1 = d1.merge(ubi,on="ID_DPD",how="left")
d1.rename(columns={"departamento": "DEPARTAMENTO", "provincia": "PROVINCIA"}, inplace=True)
d1 = d1[['ID_DIR', 'fecha_stata', 'PRECIOVENTA', 'COD_PROD', 'ID_COL','dPRECIOVENTA', 'dvarPRECIOVENTA', 'raro', 'raro2','PRECIOVENTAx',"PRECIOVENTA_",'DEPARTAMENTO','PROVINCIA','RUC']]
d1["RUC"]=d1["RUC"].astype(str)
d1 = d1.merge(d3,on=["PROVINCIA","fecha_stata","COD_PROD"],how="left",indicator=True)
d1.loc[d1["_merge"]=="both","Tipo"] = "Provincia misma"
print(d1._merge.value_counts())
print(d1.PRECIOVENTA_may.isnull().sum())
d1.drop(["_merge"],axis=1,inplace=True)
d3 = d2.groupby(['fecha_stata', "DEPARTAMENTO","COD_PROD"]).agg({'PRECIOVENTA_may': 'mean'}).reset_index()
d3.rename(columns={"PRECIOVENTA_may": "PRECIOVENTA_may2"},inplace=True)
d1 = d1.merge(d3,on=["DEPARTAMENTO","fecha_stata","COD_PROD"],how="left",indicator=True)
d1.loc[(~pd.isnull(d1["PRECIOVENTA_may2"]) & (pd.isnull(d1["PRECIOVENTA_may"]))),"Tipo"] = "Departamento mismo"
d1.loc[pd.isna(d1["PRECIOVENTA_may"]),"PRECIOVENTA_may"]=d1["PRECIOVENTA_may2"]
d1.drop(["PRECIOVENTA_may2"],axis=1,inplace=True)
print(d1._merge.value_counts())
print(d1.PRECIOVENTA_may.isnull().sum())
d1.drop(["_merge"],axis=1,inplace=True)
d1.drop(columns=["RUC","PROVINCIA","DEPARTAMENTO"],inplace=True)
print(d1.Tipo.value_counts())
d1.to_csv(ruta4 + DF_fin,index=False,encoding='utf-8',sep=";")


