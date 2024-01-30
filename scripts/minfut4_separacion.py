import pandas as pd
import os
import sys

dir=os.getcwd()
dir
sys.path.append(dir)

from minfut0_nombres import *

# Directorio
os.chdir(os.getcwd())

# Data
print("separando")
d1 = pd.read_csv(ruta6+DF_fin,encoding='utf-8',sep=";")
d1 = d1.dropna(subset=["ID_DIR","fecha_stata","COD_PROD","PRECIOVENTA"])
#d1["ID_fin"] = d1["ID_DIR"].astype(str) + "-" + d1["COD_PROD"].astype(str) + "-" + d1["fecha_stata"]
d1[["ID_DIR","COD_PROD","fecha_stata","PRECIOVENTA"]].to_csv(ruta4 + DF_min_bi, index=False, encoding="utf-8", sep=";")
d1[["ID_DIR","COD_PROD","fecha_stata","PRECIOVENTA_may"]].to_csv(ruta4 + DF_may_bi, index=False, encoding="utf-8", sep=";")
d1[["ID_DIR","COD_PROD","fecha_stata","dPRECIOVENTA","dvarPRECIOVENTA","raro","raro2"]].to_csv(ruta4 + DF_fin, index=False, encoding="utf-8", sep=";")












