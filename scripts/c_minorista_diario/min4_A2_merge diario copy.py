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


dir=os.getcwd()
dir
sys.path.append(dir)


from minfut0_nombres import *

# Directorio
os.chdir(os.getcwd())

# Finally
d1 = pd.read_csv(ruta4 + DF_fin, encoding="utf-8", sep=";")
d1=d1.drop(columns={"PRECIOVENTA_may"})
df = pd.read_csv(ruta4 + DF_dir_may,encoding='utf-8',sep=";")
#df = df.drop_duplicates(subset=["PROVINCIA","DEPARTAMENTO","RUC"])
d2 = pd.read_csv(ruta7 + DF_may_fin,encoding='utf-8',sep=';')
d2.rename(columns={"PRECIOVENTA": "PRECIOVENTA_may"},inplace=True)
#d2[['RUC', 'PROVINCIA']] = d2['RUC-prov'].str.split('-', expand=True)
#d2=d2.drop_duplicates(subset=["COD_PROD","fecha_stata","RUC-prov"])
d1 = d1.merge(df[["COD_PROD","RUC-prov","ID_DIR"]], on=["COD_PROD","ID_DIR"],how="left",indicator=True)
d1._merge.value_counts()
d1.drop(["_merge"],axis=1,inplace=True)
#d1["RUC_mayorista"]=d1["RUC_mayorista"].astype(str).str.rstrip('.0')
#print(d1.head())
#d2.rename(columns={"RUC": "RUC_mayorista"},inplace=True)
#d1.head()
d1x = d1[["COD_PROD","fecha_stata","RUC-prov"]].merge(d2[["COD_PROD","fecha_stata","RUC-prov","PRECIOVENTA_may"]],on=["RUC-prov","fecha_stata","COD_PROD"],how="left")
d1[["COD_PROD","fecha_stata","RUC-prov"]].tail()
#d1x.tail(20)
d1["PRECIOVENTA_may"] = d1x["PRECIOVENTA_may"]
d1.PRECIOVENTA_may.isnull().sum()
d1.columns
d1.to_csv(ruta4 + DF_fin,index=False,encoding='utf-8',sep=";")


