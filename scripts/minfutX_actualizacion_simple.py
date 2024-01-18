# -*- coding: utf-8 -*-
from ast import literal_eval
import pandas as pd
import os
from minfut1_nombres import *
from minfut0_utils import limpieza_dir

# Cargando la base de datos general
os.chdir(os.getcwd())

def actualizar_cod_vars():
    # Actualización manual de cadenaHL_georef para df_direccion
    emp = pd.read_excel(ruta3 + DF_georef, sheet_name='ESVP')
    emp.rename(columns={"CODIGOOSINERGMIN": "CODIGOOSINERG"},inplace=True)
    emp.rename(columns={"CODIGOOSINERGMIN": "CODIGOOSINERG", "RAZONSOCIAL": "RAZONSOCIAL_geo"},inplace=True)
    emp = emp[["CODIGOOSINERG","lat","lon","RUC","RAZONSOCIAL_geo","minorista"]]
    codox = pd.read_csv(ruta4 + DF_cod,encoding="utf-8",sep=";")
    dxx = pd.read_csv(ruta4 + DF_dir)
    dxx.drop(columns=["lat","lon","RUC","RAZONSOCIAL_geo","minorista"],inplace=True)
    dxx=dxx.merge(codox,how="left")
    dxx = dxx.merge(emp,how="left",on="CODIGOOSINERG")
    dxx.drop(columns=["CODIGOOSINERG"],inplace=True)
    dxx = limpieza_dir(dxx)
    dxx.to_csv(ruta4 + DF_dir, index=False,encoding="utf-8",sep=";")

actualizar_cod_vars() # Descomentar si desea actualizar manualmente





