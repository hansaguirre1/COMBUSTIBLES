# -*- coding: utf-8 -*-
import os
import pandas as pd
from src.infrastructure.datasources.process.minfut0_nombres import *

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
    dxx.loc[(dxx["DIRECCION"]=="AV. EL TUBO S/N CARRETERA SAN JACINTO KM. 20 SECTOR COCHARCAS") & (dxx["CODIGOOSINERG2"]=="18426-2"),"lat"]=-9.389195
    dxx.loc[(dxx["DIRECCION"]=="AV. EL TUBO S/N CARRETERA SAN JACINTO KM. 20 SECTOR COCHARCAS") & (dxx["CODIGOOSINERG2"]=="18426-2"),"lon"]=-77.864671
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA MOLLENDO-MATARANI KM. 01") & (dxx["CODIGOOSINERG2"]=="18418"),"lat"]=-17.009828
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA MOLLENDO-MATARANI KM. 01") & (dxx["CODIGOOSINERG2"]=="18418"),"lon"]=-72.024352
    dxx.loc[(dxx["DIRECCION"]=="PLANTA DE VENTAS CUSCO") & (dxx["CODIGOOSINERG2"]=="42298-3"),"lat"]=-13.522345
    dxx.loc[(dxx["DIRECCION"]=="PLANTA DE VENTAS CUSCO") & (dxx["CODIGOOSINERG2"]=="42298-3"),"lon"]=-71.983272
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA MARGINAL KM. 74, C.P. LA FLORIDA, SECTOR KIMIRIKI - AGUA DULCE, PRIMERA ETAPA") & (dxx["CODIGOOSINERG2"]=="87590-2"),"lat"]=-10.925214
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA MARGINAL KM. 74, C.P. LA FLORIDA, SECTOR KIMIRIKI - AGUA DULCE, PRIMERA ETAPA") & (dxx["CODIGOOSINERG2"]=="87590-2"),"lon"]=-74.876822
    dxx.loc[(dxx["DIRECCION"]=="AV. SALAVERRY Nº 930") & (dxx["CODIGOOSINERG2"]=="19928-2"),"lat"]=-6.770888
    dxx.loc[(dxx["DIRECCION"]=="AV. SALAVERRY Nº 930") & (dxx["CODIGOOSINERG2"]=="19928-2"),"lon"]=-79.852857
    dxx.loc[(dxx["DIRECCION"]=="U. C. N° 117130 DENOMINADA SANTA MAGDALENA, SECTOR ACHOTAL- VALLE LA LECHE") & (dxx["CODIGOOSINERG2"]=="149211"),"lat"]=-6.395264
    dxx.loc[(dxx["DIRECCION"]=="U. C. N° 117130 DENOMINADA SANTA MAGDALENA, SECTOR ACHOTAL- VALLE LA LECHE") & (dxx["CODIGOOSINERG2"]=="149211"),"lon"]=-79.830308
    dxx.loc[(dxx["DIRECCION"]=="AV. REPUBLICA DE PANAMA N° 3591 OF. 401") & (dxx["CODIGOOSINERG2"]=="105920-1"),"lat"]=-12.09994
    dxx.loc[(dxx["DIRECCION"]=="AV. REPUBLICA DE PANAMA N° 3591 OF. 401") & (dxx["CODIGOOSINERG2"]=="105920-1"),"lon"]=-77.019067
    dxx.loc[(dxx["DIRECCION"]=="AV REPUBLICA DE PANAMA 3591. INT 302") & (dxx["CODIGOOSINERG2"]=="105920-3"),"lat"]=-12.09994
    dxx.loc[(dxx["DIRECCION"]=="AV REPUBLICA DE PANAMA 3591. INT 302") & (dxx["CODIGOOSINERG2"]=="105920-3"),"lon"]=-77.019067
    dxx.loc[(dxx["DIRECCION"]=="AV. CESAR VALLERO NRO.1180-1186 URB.ARANJUEZ") & (dxx["CODIGOOSINERG2"]=="39424-3"),"lat"]=-12.087754
    dxx.loc[(dxx["DIRECCION"]=="AV. CESAR VALLERO NRO.1180-1186 URB.ARANJUEZ") & (dxx["CODIGOOSINERG2"]=="39424-3"),"lon"]=-77.043034
    dxx.loc[(dxx["DIRECCION"]=="AV. REPUBLICA DE PANAMÁ N° 3591. OFICINA 401") & (dxx["CODIGOOSINERG2"]=="105612-1"),"lat"]=-12.09994
    dxx.loc[(dxx["DIRECCION"]=="AV. REPUBLICA DE PANAMÁ N° 3591. OFICINA 401") & (dxx["CODIGOOSINERG2"]=="105612-1"),"lon"]=-77.019067
    dxx.loc[(dxx["DIRECCION"]=="AV. CIRCUNVALACION DEL CLUB GOLF LOS INCAS N° 134 TORRE 1 PISO 18") & (dxx["CODIGOOSINERG2"]=="112622-2"),"lat"]=-12.085317
    dxx.loc[(dxx["DIRECCION"]=="AV. CIRCUNVALACION DEL CLUB GOLF LOS INCAS N° 134 TORRE 1 PISO 18") & (dxx["CODIGOOSINERG2"]=="112622-2"),"lon"]=-76.969399
    dxx.loc[(dxx["DIRECCION"]=="AV. VICTOR ANDRES BELAUNDE Nº 147 INT. 301, EDIFICIO REAL 5") & (dxx["CODIGOOSINERG2"]=="33811-2"),"lat"]=-12.096715
    dxx.loc[(dxx["DIRECCION"]=="AV. VICTOR ANDRES BELAUNDE Nº 147 INT. 301, EDIFICIO REAL 5") & (dxx["CODIGOOSINERG2"]=="33811-2"),"lon"]=-77.037022
    dxx.loc[(dxx["DIRECCION"]=="AV. VÍCTOR ANDRÉS BELAUNDE 147, VÍA PRINCIPAL N° 110, TORRE REAL 5, PISO 8") & (dxx["CODIGOOSINERG2"]=="33800-1"),"lat"]=-12.096715
    dxx.loc[(dxx["DIRECCION"]=="AV. VÍCTOR ANDRÉS BELAUNDE 147, VÍA PRINCIPAL N° 110, TORRE REAL 5, PISO 8") & (dxx["CODIGOOSINERG2"]=="33800-1"),"lon"]=-77.037022
    dxx.loc[(dxx["DIRECCION"]=="AV. VÍCTOR ANDRES BELAÚNDE Nº 147, VÍA PRINCIPAL Nº 110, TORRE 5 PISO 7") & (dxx["CODIGOOSINERG2"]=="33800-2"),"lat"]=-12.096715
    dxx.loc[(dxx["DIRECCION"]=="AV. VÍCTOR ANDRES BELAÚNDE Nº 147, VÍA PRINCIPAL Nº 110, TORRE 5 PISO 7") & (dxx["CODIGOOSINERG2"]=="33800-2"),"lon"]=-77.037022
    dxx.loc[(dxx["DIRECCION"]=="AV. CIRCUNVALACION DEL CLUB GOLF LOS INCAS 134. TORRE 1 PISO 18") & (dxx["CODIGOOSINERG2"]=="112623"),"lat"]=-12.085317
    dxx.loc[(dxx["DIRECCION"]=="AV. CIRCUNVALACION DEL CLUB GOLF LOS INCAS 134. TORRE 1 PISO 18") & (dxx["CODIGOOSINERG2"]=="112623"),"lon"]=-76.969399
    dxx.loc[(dxx["DIRECCION"]=="ESQ. AV. URUBAMBA CON AV. LOS INGENIEROS") & (dxx["CODIGOOSINERG2"]=="ESQ. AV. URUBAMBA CON AV. LOS INGENIEROS"),"lat"]=-12.06061
    dxx.loc[(dxx["DIRECCION"]=="ESQ. AV. URUBAMBA CON AV. LOS INGENIEROS") & (dxx["CODIGOOSINERG2"]=="ESQ. AV. URUBAMBA CON AV. LOS INGENIEROS"),"lon"]=-76.951819
    dxx.loc[(dxx["DIRECCION"]=="AV. MARIANO LINO URQUIETA N° 1003") & (dxx["CODIGOOSINERG2"]=="33767-1"),"lat"]=-17.641764
    dxx.loc[(dxx["DIRECCION"]=="AV. MARIANO LINO URQUIETA N° 1003") & (dxx["CODIGOOSINERG2"]=="33767-1"),"lon"]=-71.34069
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA PANAMERICANA NORTE KM. 4.38") & (dxx["CODIGOOSINERG2"]=="39507-1"),"lat"]=-5.200756
    dxx.loc[(dxx["DIRECCION"]=="CARRETERA PANAMERICANA NORTE KM. 4.38") & (dxx["CODIGOOSINERG2"]=="39507-1"),"lon"]=-80.625589
    dxx.loc[(dxx["DIRECCION"]=="AV. GARCILAZO N° 323") & (dxx["CODIGOOSINERG2"]=="87636"),"lat"]=-14.882013
    dxx.loc[(dxx["DIRECCION"]=="AV. GARCILAZO N° 323") & (dxx["CODIGOOSINERG2"]=="87636"),"lon"]=-70.590184
    dxx.loc[(dxx["DIRECCION"]=="FUNDO EL CHOCHE II PARCELA B - CASERIO NVO. JUANJUI") & (dxx["CODIGOOSINERG2"]=="169148"),"lat"]=-8.639441
    dxx.loc[(dxx["DIRECCION"]=="FUNDO EL CHOCHE II PARCELA B - CASERIO NVO. JUANJUI") & (dxx["CODIGOOSINERG2"]=="169148"),"lon"]=-74.964887
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

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    