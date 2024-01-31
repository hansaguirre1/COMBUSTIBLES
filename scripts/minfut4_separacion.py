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
d1 = d1.merge(dir,how="left")
d1 = d1.merge(ubi,how="left")
d1 = d1[["fecha_stata","PRECIOVENTA","COD_PROD","ID_COL","dPRECIOVENTA","dvarPRECIOVENTA","raro","raro2","ID_DIR","PRECIOVENTA_may","departamento"]]
d1.rename(columns={"departamento": "DEPARTAMENTO"}, inplace=True)
d1.loc[(d1.DEPARTAMENTO=="LIMA") | (d1.DEPARTAMENTO=="CALLAO"), "DEPARTAMENTO"] = "LIMA Y CALLAO"
validos = pd.read_csv(ruta4+DF_val2,encoding="utf-8",sep=";")
validos.loc[(validos.DEPARTAMENTO=="LIMA") | (validos.DEPARTAMENTO=="CALLAO"), "DEPARTAMENTO"] = "LIMA Y CALLAO"
validos=validos.fillna(0)
validos = validos.groupby(['DEPARTAMENTO', 'COD_PROD'])['ok'].mean().reset_index()
validos.loc[validos.ok>0.9,"mirar"]=1
d1 = d1.merge(validos[["DEPARTAMENTO","COD_PROD","mirar"]],how="left",on=["DEPARTAMENTO","COD_PROD"])
d1.to_csv(ruta4 + DF_fin2, index=False, encoding="utf-8", sep=";")
print("fin")











