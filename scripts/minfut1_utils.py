# -*- coding: utf-8 -*-
import os
import pandas as pd
from minfut0_nombres import *

def limpieza_rs(data):
    data["RAZONSOCIAL"] = data["RAZONSOCIAL"].str.replace("..",".")
    data["RAZONSOCIAL"] = data["RAZONSOCIAL"].str.strip()
    data["RAZONSOCIAL"] = data["RAZONSOCIAL"].str.replace(".","")
    data["RAZONSOCIAL"] = data["RAZONSOCIAL"].str.replace("  "," ")
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('coesti'), 'RAZONSOCIAL'] = 'COESTI S.A.'
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('terpel'), 'RAZONSOCIAL'] = 'TERPEL PERÚ S.A.C.'
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('energigas'), 'RAZONSOCIAL'] = 'ENERGIGAS S.A.C.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('petroperu')) & (data['RAZONSOCIAL'].str.lower().str.contains('s.a.')), 'RAZONSOCIAL'] = 'PETROPERÚ S.A.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('grifo')) & (data['RAZONSOCIAL'].str.lower().str.contains('ignacio')), 'RAZONSOCIAL'] = 'GRIFO SAN IGNANCIO S.A.C.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('repsol')) & (data['RAZONSOCIAL'].str.lower().str.contains('comercial')), 'RAZONSOCIAL'] = 'REPSOL COMERCIAL S.A.C.'
    return data
    
def limpieza_ubi(data):
    data.loc[data['PROVINCIA'] == "CARLOS F. FITZCARRALD", 'PROVINCIA'] = "CARLOS FERMIN FITZCARRALD"
    data.loc[data['DISTRITO'] == "QUENQUEÑA", 'DISTRITO'] = "QUEQUEÑA"
    data.loc[data['DISTRITO'] == "ANDRES AVELINO CACERES", 'DISTRITO'] = "ANDRES AVELINO CACERES DORREGARAY"
    data.loc[data['DISTRITO'] == "ENCANADA", 'DISTRITO'] = "ENCAÑADA"
    data.loc[data['DEPARTAMENTO'] == "PROV. CONST. DEL CALLAO", 'DEPARTAMENTO'] = "CALLAO"
    data.loc[(data['PROVINCIA'] == "PROV. CONST. DEL CALLAO") & (data['PROVINCIA'] == "CALLAO"), 'PROVINCIA'] = "CALLAO"
    data.loc[(data['PROVINCIA'] == "CALLAO") & (data['DEPARTAMENTO'] == "LIMA"), 'DEPARTAMENTO'] = "CALLAO"
    data.loc[data['PROVINCIA'] == "OCOÑA", 'PROVINCIA'] = "CAMANA"
    data.loc[data['DISTRITO'] == "OCONA", 'DISTRITO'] = "OCOÑA"
    data.loc[data['DISTRITO'] == "DANIEL ALOMIA ROBLES", 'DISTRITO'] = "DANIEL ALOMIAS ROBLES"
    data.loc[data['DISTRITO'] == "SAN JUAN DE ISCOS", 'DISTRITO'] = "SAN JUAN DE YSCOS"
    data.loc[data['DISTRITO'] == "MOCUPE", 'DISTRITO'] = "MOTUPE"
    data.loc[(data['DEPARTAMENTO'] == "LAMBAYEQUE") & (data['DISTRITO'] == "MOTUPE"), 'PROVINCIA'] = "LAMBAYEQUE"
    data.loc[data['DISTRITO'] == "SANTO DOMINGO DE LOS OLLERO", 'DISTRITO'] = "SANTO DOMINGO DE LOS OLLEROS"
    data.loc[data['DISTRITO'] == "MUNANI", 'DISTRITO'] = "MUÑANI"
    data.loc[data['DISTRITO'] == "MANAZO", 'DISTRITO'] = "MAÑAZO"
    data.loc[data['DISTRITO'] == "ALEXANDER VON HUMBOLD", 'DISTRITO'] = "ALEXANDER VON HUMBOLDT"
    data.loc[data['DISTRITO'] == "CORONEL GREGORIO ALBARRACIN LANCHIPA", 'DISTRITO'] = "CORONEL GREGORIO ALBARRACIN LANCHIP"
    data.loc[data['DISTRITO'] == "KIMBIRI", 'DISTRITO'] = "QUIMBIRI"
    data.loc[data['DISTRITO'] == "YAURI", 'DISTRITO'] = "ESPINAR"
    data.loc[(data['DEPARTAMENTO'] == "LORETO") & (data['DISTRITO'] == "BARRANCA"), 'PROVINCIA'] = "DATEM DEL MARAÑON"  
    data['DPD'] = data['DEPARTAMENTO'] + "#" + data['PROVINCIA'] + "#" + data['DISTRITO']
    return data
    
def limpieza_dir(dxx):
    # Actualización específica de latitud y longitud en base a ID_DIR
    dxx.loc[dxx["ID_DIR"]==2736,"lat"]=-13.551168
    dxx.loc[dxx["ID_DIR"]==2736,"lon"]=-71.925882
    dxx.loc[dxx["ID_DIR"]==2536,"lat"]=-3.554706
    dxx.loc[dxx["ID_DIR"]==2536,"lon"]=-80.421228
    dxx.loc[dxx["ID_DIR"]==7785,"lat"]=-11.855585
    dxx.loc[dxx["ID_DIR"]==7785,"lon"]=-77.07346
    dxx.loc[dxx["ID_DIR"]==2172,"lat"]=-11.855585
    dxx.loc[dxx["ID_DIR"]==2172,"lon"]=-77.07346 
    dxx.loc[dxx["ID_DIR"]==4986,"lat"]=-12.103093
    dxx.loc[dxx["ID_DIR"]==4986,"lon"]=-76.884684
    dxx.loc[dxx["ID_DIR"]==2180,"lat"]=-11.931979
    dxx.loc[dxx["ID_DIR"]==2180,"lon"]=-76.691007
    dxx.loc[dxx["ID_DIR"]==2180,"lat"]=-12.09994
    dxx.loc[dxx["ID_DIR"]==2180,"lon"]=-77.019067
    dxx.loc[dxx["ID_DIR"]==12888,"lat"]=-12.087754
    dxx.loc[dxx["ID_DIR"]==12888,"lon"]=-77.043034
    dxx.loc[dxx["ID_DIR"]==10144,"lat"]=-12.09994
    dxx.loc[dxx["ID_DIR"]==10144,"lon"]=-77.019067
    dxx.loc[dxx["ID_DIR"]==4489,"lat"]=-12.09994
    dxx.loc[dxx["ID_DIR"]==4489,"lon"]=-77.019067
    dxx.loc[dxx["ID_DIR"]==4466,"lat"]=-12.099903
    dxx.loc[dxx["ID_DIR"]==4466,"lon"]=-77.0190445
    dxx.loc[dxx["ID_DIR"]==4985,"lat"]=-12.085317
    dxx.loc[dxx["ID_DIR"]==4985,"lon"]=-76.969399
    return dxx

def limpieza_prod(data):
    data['NOM_PROD'] = data.apply(lambda row: row['PRODUCTO'] if pd.isna(row['DESCRIPCIONPRODUCTO']) or row['DESCRIPCIONPRODUCTO'] == "" else row['DESCRIPCIONPRODUCTO'], axis=1)
    data["UNIDAD"] = ""
    data.loc[data["UNIDAD"].str.contains("Cilindros"),"UNIDAD"]="Kilogramos"
    data.loc[data["NOM_PROD"]=="GLP - E","UNIDAD"]="Kilogramos"
    data.loc[data["NOM_PROD"]=="GLP - G","UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"]=="GAS NATURAL VEHICULAR","UNIDAD"]="M3"
    data.loc[data["NOM_PROD"]=="GNV","UNIDAD"]="M3"
    data.loc[data["NOM_PROD"].str.contains("DIESEL"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("Diesel"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("GASOHOL"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("GASOLINA"),"UNIDAD"]="GALONES"
    return data
  
def limpieza_mini(df):
    for i in ["PRODUCTO",'PRECIODEVENTASOLES', 'PRECIODEVENTASoles']:
        if i not in df.columns:
            # Crear la nueva columna solo si no existe
            df[i] = ''
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace("(SOLES)","")
    df.columns = df.columns.str.replace("\n","")
    df = df.dropna(subset="CODIGOOSINERG")
    return df
  
def generar_valor_unico(columna):
    conteo = columna.groupby(columna).cumcount() + 1
    sufijo = '-' + conteo.astype(str)
    valores_unicos = columna.astype(str) + sufijo.where(conteo >= 1, '')
    return valores_unicos

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    