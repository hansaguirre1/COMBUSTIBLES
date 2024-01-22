import os
import pandas as pd
from src.infrastructure.datasources.process.minfut0_nombres import *

# Cargando la base de datos general
os.chdir(os.getcwd())

def ubi_ccpp():
    print("ubi_ccpp")
    ubi_ = pd.read_csv(ruta3 + DF_ubiccpp)
    ubi_["tipo"].value_counts()
    ubi_.loc[ubi_["tipo"]=="Urban","pUrban"]=1
    ubi_.loc[ubi_["tipo"]=="Rural","pUrban"]=0
    ubi_.rename(columns={"inei_distrito": "UBIGEO"},inplace=True)
    ubi_ = ubi_.groupby(['UBIGEO'])['pUrban'].mean().reset_index()
    ubi_.loc[ubi_["pUrban"]<=0.9,"rural"]=1
    ubi_.loc[ubi_["pUrban"]>0.9,"rural"]=0
    ubi_["rural"].value_counts()
    return ubi_

def ubigeos():
    print("ubigeos")
    ubi = pd.read_csv(ruta3 + DF_ubigeo)
    ubi = ubi[["ubigeo_inei", "departamento", "provincia", "distrito"]].drop_duplicates()  
    ubi.loc[ubi["provincia"]=="CALLAO","provincia"]="PROV. CONST. DEL CALLAO"
    ubi["DPD"] = ubi["departamento"] + "#" + ubi["provincia"] + "#" + ubi["distrito"] 
    ubi = ubi.rename(columns={'ubigeo_inei': 'UBIGEO'})
    ubi["UBI"] = ubi["UBIGEO"].astype(str).str.zfill(6)
    ubi_ = ubi_ccpp()
    ubi["ID_DPD"] = range(1, len(ubi) + 1)
    ubi = ubi.merge(ubi_,how="left")
    ubi.loc[(ubi["UBI"].str.contains("^1501",regex=True)) | (ubi["departamento"]=="CALLAO"),"Capital"]="LIMA"
    ubi.loc[ubi["UBI"].str.contains("^150[2-9]|^151[0-9]|^152[0-9]",regex=True),"Capital"]="NO LIMA"
    di = {"CHACHAPOYAS": "0101", "HUARAZ": "0201", "ABANCAY": "0301", "AREQUIPA": "0401", "AYACUCHO": "0501", "CAJAMARCA": "0601", "CUSCO": "0801", "HUANCAVELICA": "0901", "HUANUCO": "1001", "ICA": "1101", "HUANCAYO": "1201", "TRUJILLO": "1301", "CHICLAYO": "1401", "MAYNAS" : "1601" , "TAMBOPATA": "1701", "MARISCAL NIETO": "1801", "PASCO": "1901", "PIURA": "2001", "PUNO": "2101", "MOYOBAMBA": "2201", "TACNA": "2301", "TUMBES": "2401", "CORONEL PORTILLO": "2501"}
    for i in di:
        ubi.loc[ubi["UBI"].str.contains(f"^{di[i]}",regex=True),"Capital"]=f"{i}"
        ubi.loc[ubi["UBI"].str.contains(f"^{di[i][:-1]}[2-9]|^{di[i][:-2]}1[0-9]|^{di[i][:-2]}2[0-9]",regex=True),"Capital"]=f"NO {i}"
    ubi.to_csv(ruta4 + DF_ubi, index=False,encoding="utf-8",sep=";")
    ubi = ubi[["DPD","ID_DPD"]]
    return ubi