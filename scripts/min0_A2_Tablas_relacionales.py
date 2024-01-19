# -*- coding: utf-8 -*-
from glob import glob
import numpy as np
from datetime import datetime, timedelta
from ast import literal_eval
import pandas as pd
import os
from minfut0_nombres import *
from minfut1_utils import *


# Cargando la base de datos general
os.chdir(os.getcwd())

def limpieza_masiva(data, ubi):
    # Tabla Ubicacion
    ubi = ubi[["DPD","ID_DPD"]]
    
    # Carga de datos
    a=1 # Indicador si es nuevo (0) o apilando (1) ¡SE CAMBIA SOLO! NO TOCAR
    data = data.dropna(subset="CODIGOOSINERG")
    for i in ["ACTIVIDAD","DIRECCION","PRODUCTO","DESCRIPCIONPRODUCTO","RAZONSOCIAL"]:
        try:
            data[i]=data[i].str.strip()
        except:
            pass
    
    # RAZON SOCIAL
    data = limpieza_rs(data)
    df2 = data[["RAZONSOCIAL"]]
    df2 = df2.drop_duplicates().sort_values(by=["RAZONSOCIAL"])
    df2 = df2.reset_index(drop=True)
    try:
        df2x = pd.read_csv(ruta4 + DF_rs, on_bad_lines = 'warn', encoding="utf-8", sep=";")
        print("APILANDO")
        df2 = pd.concat([df2x, df2])
        df2 = df2.drop_duplicates(subset=["RAZONSOCIAL"])   
        df2["ID_RS"] = range(1, len(df2) + 1)
    except:
        print("NUEVO")
        a=0
        df2 = df2.drop_duplicates(subset=["RAZONSOCIAL"])   
        df2["ID_RS"] = range(1, len(df2) + 1)
    df2 = df2[["RAZONSOCIAL","ID_RS"]]
    df2.to_csv(ruta4 + DF_rs, index=False, encoding="utf-8", sep=";")
    data = data.merge(df2,how="left")
    data.drop("RAZONSOCIAL",axis=1,inplace=True)
    
    # UBIGEO en data
    data = limpieza_ubi(data)
    data = pd.merge(data, ubi, how='left', on='DPD')
    datau = data[data["ID_DPD"].isna()]
    data.drop(["DPD","DEPARTAMENTO","PROVINCIA","DISTRITO"],axis=1,inplace=True)

    # ACTIVIDAD
    actividad = data[["ACTIVIDAD"]]
    actividad = actividad.drop_duplicates().sort_values(by=["ACTIVIDAD"])
    actividad = actividad.reset_index(drop=True)
    try:
        actividadx = pd.read_csv(ruta4 + DF_act, encoding="utf-8", sep=";")
        actividad = pd.concat([actividadx, actividad])
        actividad = actividad.drop_duplicates(subset=["ACTIVIDAD"])    
        actividad["COD_ACT"] = range(1, len(actividad) + 1)    
    except:
        actividad = actividad.drop_duplicates(subset=["ACTIVIDAD"])    
        actividad["COD_ACT"] = range(1, len(actividad) + 1)    
    actividad.to_csv(ruta4 + DF_act, index=False, encoding="utf-8", sep=";")
    data = pd.merge(data, actividad, how='left', on='ACTIVIDAD')
    data.drop(["ACTIVIDAD"],axis=1,inplace=True)

    # CODIGOOSINERG
    codo = data[["CODIGOOSINERG"]]
    codo = codo.drop_duplicates().sort_values(by=["CODIGOOSINERG"])
    codo = codo.reset_index(drop=True)
    try:
        codox = pd.read_csv(ruta4 + DF_cod, encoding="utf-8", sep=";")
        codo = pd.concat([codox, codo])
        codo = codo.drop_duplicates(subset=["CODIGOOSINERG"])    
        codo["ID_COD"] = range(1, len(codo) + 1)    
    except:
        codo = codo.drop_duplicates(subset=["CODIGOOSINERG"])    
        codo["ID_COD"] = range(1, len(codo) + 1)    
    codo.to_csv(ruta4 + DF_cod, index=False, encoding="utf-8", sep=";")
    data = pd.merge(data, codo, how='left', on='CODIGOOSINERG')
    
    # Tabla localizaciones
    emp = pd.read_excel(ruta3 + DF_georef, sheet_name='ESVP')
    emp.rename(columns={"CODIGOOSINERGMIN": "CODIGOOSINERG", "RAZONSOCIAL": "RAZONSOCIAL_geo"},inplace=True)
    emp = emp[["CODIGOOSINERG","lat","lon","RUC","RAZONSOCIAL_geo","minorista"]]
    dx = data[["ID_COD","ID_RS","COD_ACT","ID_DPD","DIRECCION","CODIGOOSINERG"]]
    data.drop(["CODIGOOSINERG"],axis=1,inplace=True)
    dx = dx.drop_duplicates().sort_values(by=["ID_COD"])
    dx = dx.reset_index(drop=True)
    dx = dx.merge(emp,how="left",on="CODIGOOSINERG")
    dx.drop("CODIGOOSINERG",axis=1,inplace=True)
    try:
        dxx = pd.read_csv(ruta4 + DF_dir, encoding="utf-8", sep=";")
        dx = pd.concat([dxx,dx])
        dx = dx.drop_duplicates(subset=["DIRECCION","ID_DPD"])
        dx["ID_DIR"] = range(1,len(dx)+1)
    except:
        dx = dx.drop_duplicates(subset=["DIRECCION","ID_DPD"])
        dx["ID_DIR"] = range(1,len(dx)+1)
    dx = limpieza_dir(dx)
    dx.to_csv(ruta4 + DF_dir, index=False, encoding="utf-8", sep=";")
    data = pd.merge(data, dx[["DIRECCION","ID_DPD","ID_DIR"]], how='left', on=["DIRECCION","ID_DPD"])
    data.drop(["ID_COD","ID_RS","COD_ACT","ID_DPD","DIRECCION"],axis=1,inplace=True)
    
    # PRECIO
    data['PRECIODEVENTASoles'] = pd.to_numeric(data['PRECIODEVENTASoles'], errors='coerce')
    data['PRECIODEVENTASOLES'] = pd.to_numeric(data['PRECIODEVENTASOLES'], errors='coerce')
    data['PRECIOVENTA'] = data[['PRECIODEVENTASOLES', 'PRECIODEVENTASoles', 'PRECIODEVENTA']].sum(axis=1, skipna=True)
    data.drop(["PRECIODEVENTASoles","PRECIODEVENTASOLES","PRECIODEVENTA"],axis=1,inplace=True)
    
    # PRODUCTO/UNIDAD
    data = limpieza_prod(data)
    producto = data[["NOM_PROD","UNIDAD"]].drop_duplicates()
    producto = producto.sort_values(by="NOM_PROD")
    producto = producto.reset_index(drop=True)
    try:
        prodx = pd.read_csv(ruta4 + DF_prod, encoding="utf-8", sep=";")
        producto = pd.concat([prodx, producto])
        producto = producto.drop_duplicates(subset=["NOM_PROD"])
        producto["COD_PROD"] = range(1, len(producto) + 1)
        producto.to_csv(ruta4 + DF_prod, index=False, encoding="utf-8", sep=";")
    except:
        producto = producto.drop_duplicates(subset=["NOM_PROD"])     
        producto["COD_PROD"] = range(1, len(producto) + 1)
        producto.to_csv(ruta4 + DF_prod, index=False, encoding="utf-8", sep=";")
    data.drop(["PRODUCTO","DESCRIPCIONPRODUCTO","UNIDAD"],axis=1,inplace=True)
    data = pd.merge(data,producto, how='left', on="NOM_PROD")
    data.drop(["NOM_PROD","UNIDAD"],axis=1,inplace=True)
    
    if a==0:
        print("guardando...")
        data.to_stata(ruta4 + BASE_DLC, write_index=False)
    return data

# Verificando si es archivo diario o mensual
arch = glob(ruta2 + "Diario(*.xlsx")

# Cargando compilado y diario
if len(arch)==1:
    print("Archivo diario")
    print(arch[0])
    datax = pd.read_csv(ruta4 + BASE_DLC,encoding="utf-8",sep=";")
    ex1=pd.read_excel(arch[0],sheet_name="GLP_EVP_PEGL_LVGL_COM_PROD_IMP",skiprows=3)
    ex1=limpieza_mini(ex1)
    ex2=pd.read_excel(arch[0],sheet_name="LIQ_EVP_DMAY_CCA_CCE",skiprows=3)
    ex2=limpieza_mini(ex2)
    data = pd.concat([ex1, ex2], ignore_index=True)
    data = limpieza_masiva(data, ubi)
    #os.remove(arch[0])

# Exportando base final
data_concat_f.to_csv(ruta4 + BASE_DLC, index=False, encoding="utf-8", sep=";")


















