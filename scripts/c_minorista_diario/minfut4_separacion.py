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
d1['promedio'] = d1.groupby(['COD_PROD', 'fecha_stata'])['PRECIOVENTA'].transform('mean')
d1['conteo'] = d1.groupby(['COD_PROD', 'fecha_stata'])['PRECIOVENTA'].transform('count')
d1=d1.sort_values(by=["COD_PROD","fecha_stata","ID_DIR"])
d1["markup_mm"]=d1["PRECIOVENTA"]-(d1["promedio"]*d1["conteo"]-d1["PRECIOVENTA"])/(d1["conteo"]-1)
d1[["COD_PROD","fecha_stata","ID_DIR","PRECIOVENTA","promedio","conteo"]]
#d1p = d1.loc[(d1["ID_DIR"]==188) | (d1["ID_DIR"]==189)]
#d1p = d1p.sort_values(by=['ID_DIR', 'COD_PROD', 'fecha_stata'])
#zzz
# resultados = []
# for id_dir in d1['ID_DIR'].unique()[100:102]:
#     print(id_dir)
#     df_id_dir = d1[d1['ID_DIR'] != id_dir]
#     df_si_id_dir = d1[d1['ID_DIR'] == id_dir]
#     media_excluyendo_ID_DIR = df_id_dir.groupby(['COD_PROD', 'fecha_stata'])['PRECIOVENTA'].mean().reset_index()
#     resultado_temporal = pd.merge(df_si_id_dir[["ID_DIR","COD_PROD","fecha_stata","PRECIOVENTA"]], media_excluyendo_ID_DIR, on=['COD_PROD', 'fecha_stata'], suffixes=('_ID_DIR', '_excluyendo_ID_DIR'), how="left")
#     resultado_temporal['Diferencia'] = resultado_temporal['PRECIOVENTA_ID_DIR']-resultado_temporal['PRECIOVENTA_excluyendo_ID_DIR']
#     resultados.append(resultado_temporal)

# resultado_final = pd.concat(resultados, ignore_index=True)
# resultado_final = resultado_final.sort_values(by=['ID_DIR', 'COD_PROD', 'fecha_stata'])
# #resultado_final.COD_PROD.value_counts()
# d1["markup_mm"] = resultado_final["Diferencia"]
d1.drop(["promedio","conteo"],axis=1,inplace=True)
#d1p = d1.iloc[:10000,:]
d1.to_csv(ruta4 + DF_fin2, index=False, encoding="utf-8", sep=";")
print("fin")











