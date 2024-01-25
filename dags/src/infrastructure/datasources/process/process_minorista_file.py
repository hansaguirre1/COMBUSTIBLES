import pandas as pd
pathMinorista = 'data/interim/minoristas'
from src.infrastructure.datasources.process.minfut1_utils import *
from src.infrastructure.datasources.process.minfut0_nombres import *

# Cargando compilado y diario
def limpiezaMinorista(df):
    for i in ["PRODUCTO",'PRECIODEVENTASOLES', 'PRECIODEVENTASoles']:
        if i not in df.columns:
            # Crear la nueva columna solo si no existe
            df[i] = ''
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace("(SOLES)","")
    df.columns = df.columns.str.replace("\n","")
    df = df.dropna(subset="CODIGOOSINERG")
    return df

def limpiezaMasivaMinorista(data, ubi):
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
    
    
    # -------------------------------------
    # -------------------------------------
    # -------------------------------------
    # -------------------------------------
    # -------------------------------------
    # -------------------------------------
    # Tabla localizaciones
    emp = pd.read_excel(f"{pathMinorista}/cadenaHL_georef.xlsx", sheet_name='ESVP')
    emp.rename(columns={"CODIGOOSINERGMIN": "CODIGOOSINERG"},inplace=True)
    emp = emp[["CODIGOOSINERG","lat","lon"]]
    
    # Tabla Ubicacion I
    ubi_ = pd.read_csv(f"{pathMinorista}/ubigeo_ccpp.csv")
    ubi_["tipo"].value_counts()
    ubi_.loc[ubi_["tipo"]=="Urban","pUrban"]=1
    ubi_.loc[ubi_["tipo"]=="Rural","pUrban"]=0
    ubi_.rename(columns={"inei_distrito": "UBIGEO"},inplace=True)
    ubi_ = ubi_.groupby(['UBIGEO'])['pUrban'].mean().reset_index()
    ubi_.loc[ubi_["pUrban"]<=0.9,"rural"]=1
    ubi_.loc[ubi_["pUrban"]>0.9,"rural"]=0
    ubi_["rural"].value_counts()

    # Tabla Ubicacion II
    ubi = pd.read_csv(f"{pathMinorista}/TB_UBIGEOS.csv")
    ubi = ubi[["ubigeo_inei", "departamento", "provincia", "distrito"]].drop_duplicates()  
    ubi.loc[ubi["provincia"]=="CALLAO","provincia"]="PROV. CONST. DEL CALLAO"
    ubi["DPD"] = ubi["departamento"] + "#" + ubi["provincia"] + "#" + ubi["distrito"] 
    ubi = ubi.rename(columns={'ubigeo_inei': 'UBIGEO'})
    ubi["UBI"] = ubi["UBIGEO"].astype(str).str.zfill(6)
    ubi["ID_DPD"] = range(1, len(ubi) + 1)
    ubi = ubi.merge(ubi_,how="left")
    ubi.loc[(ubi["UBI"].str.contains("^1501",regex=True)) | (ubi["departamento"]=="CALLAO"),"Capital"]="LIMA"
    ubi.loc[ubi["UBI"].str.contains("^150[2-9]|^151[0-9]|^152[0-9]",regex=True),"Capital"]="NO LIMA"
    di = {"CHACHAPOYAS": "0101", "HUARAZ": "0201", "ABANCAY": "0301", "AREQUIPA": "0401", "AYACUCHO": "0501", "CAJAMARCA": "0601", "CUSCO": "0801", "HUANCAVELICA": "0901", "HUANUCO": "1001", "ICA": "1101", "HUANCAYO": "1201", "TRUJILLO": "1301", "CHICLAYO": "1401", "MAYNAS" : "1601" , "TAMBOPATA": "1701", "MARISCAL NIETO": "1801", "PASCO": "1901", "PIURA": "2001", "PUNO": "2101", "MOYOBAMBA": "2201", "TACNA": "2301", "TUMBES": "2401", "CORONEL PORTILLO": "2501"}
    for i in di:
        ubi.loc[ubi["UBI"].str.contains(f"^{di[i]}",regex=True),"Capital"]=f"{i}"
        ubi.loc[ubi["UBI"].str.contains(f"^{di[i][:-1]}[2-9]|^{di[i][:-2]}1[0-9]|^{di[i][:-2]}2[0-9]",regex=True),"Capital"]=f"NO {i}"
    ubi.to_csv(f"{pathMinorista}/df_ubicacion.csv", index=False)
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
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('coesti'), 'RAZONSOCIAL'] = 'COESTI S.A.'
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('terpel'), 'RAZONSOCIAL'] = 'TERPEL PERÚ S.A.C.'
    data.loc[data['RAZONSOCIAL'].str.lower().str.contains('energigas'), 'RAZONSOCIAL'] = 'ENERGIGAS S.A.C.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('petroperu')) & (data['RAZONSOCIAL'].str.lower().str.contains('s.a.')), 'RAZONSOCIAL'] = 'PETROPERÚ S.A.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('grifo')) & (data['RAZONSOCIAL'].str.lower().str.contains('ignacio')), 'RAZONSOCIAL'] = 'GRIFO SAN IGNANCIO S.A.C.'
    data.loc[(data['RAZONSOCIAL'].str.lower().str.contains('repsol')) & (data['RAZONSOCIAL'].str.lower().str.contains('comercial')), 'RAZONSOCIAL'] = 'REPSOL COMERCIAL S.A.C.'
    df2 = data[["RAZONSOCIAL"]]
    df2 = df2.drop_duplicates().sort_values(by=["RAZONSOCIAL"])
    df2 = df2.reset_index(drop=True)
    try:
        df2x = pd.read_csv(f"{pathMinorista}/df_razon_social.csv", on_bad_lines = 'warn')
        print("APILANDO")
        df2 = pd.concat([df2x, df2])
        df2 = df2.drop_duplicates(subset=["RAZONSOCIAL"])   
        df2["ID_RS"] = range(1, len(df2) + 1)
    except:
        print("NUEVO")
        a=0
        df2 = df2.drop_duplicates(subset=["RAZONSOCIAL"])   
        df2["ID_RS"] = range(1, len(df2) + 1)
    #df2 = df2.merge(emp,how='left')
    df2 = df2[["RAZONSOCIAL","ID_RS"]]
    df2.to_csv(f"{pathMinorista}/df_razon_social.csv", index=False)
    data = data.merge(df2,how="left")
    data.drop("RAZONSOCIAL",axis=1,inplace=True)
    
    # UBIGEO    
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
    data = pd.merge(data, ubi, how='left', on='DPD')
    data.drop(["DPD","DEPARTAMENTO","PROVINCIA","DISTRITO"],axis=1,inplace=True)

    # ACTIVIDAD
    actividad = data[["ACTIVIDAD"]]
    actividad = actividad.drop_duplicates().sort_values(by=["ACTIVIDAD"])
    actividad = actividad.reset_index(drop=True)
    try:
        actividadx = pd.read_csv(f"{pathMinorista}/df_actividad.csv")
        actividad = pd.concat([actividadx, actividad])
        actividad = actividad.drop_duplicates(subset=["ACTIVIDAD"])    
        actividad["COD_ACT"] = range(1, len(actividad) + 1)    
    except:
        actividad = actividad.drop_duplicates(subset=["ACTIVIDAD"])    
        actividad["COD_ACT"] = range(1, len(actividad) + 1)    
    actividad.to_csv(f"{pathMinorista}/df_actividad.csv", index=False)
    data = pd.merge(data, actividad, how='left', on='ACTIVIDAD')
    data.drop(["ACTIVIDAD"],axis=1,inplace=True)

    # CODIGOOSINERG
    codo = data[["CODIGOOSINERG"]]
    codo = codo.drop_duplicates().sort_values(by=["CODIGOOSINERG"])
    codo = codo.reset_index(drop=True)
    try:
        codox = pd.read_csv(f"{pathMinorista}/df_codigoosinerg.csv")
        codo = pd.concat([codox, codo])
        codo = codo.drop_duplicates(subset=["CODIGOOSINERG"])    
        codo["COD_ACT"] = range(1, len(actividad) + 1)    
    except:
        codo = codo.drop_duplicates(subset=["CODIGOOSINERG"])    
        codo["ID_COD"] = range(1, len(codo) + 1)    
    codo.to_csv(f"{pathMinorista}/df_codigoosinerg.csv", index=False)
    data = pd.merge(data, codo, how='left', on='CODIGOOSINERG')
    
    # CODIGOOSINERG (TABLA FINAL)
    dx = data[["ID_COD","ID_RS","COD_ACT","ID_DPD","DIRECCION","CODIGOOSINERG"]]
    data.drop(["CODIGOOSINERG"],axis=1,inplace=True)
    dx = dx.drop_duplicates().sort_values(by=["ID_COD"])
    dx = dx.reset_index(drop=True)
    dx = dx.merge(emp,how="left",on="CODIGOOSINERG")
    dx.drop("CODIGOOSINERG",axis=1,inplace=True)
    try:
        dxx = pd.read_csv(f"{pathMinorista}/df_direccion.csv")
        dx = pd.concat([dxx,dx])
        dx = dx.drop_duplicates(subset=["ID_COD"])
        dx["ID_DIR"] = range(1,len(dx)+1)
    except:
        dx = dx.drop_duplicates(subset=["ID_COD"])
        dx["ID_DIR"] = range(1,len(dx)+1)
    dx.to_csv(f"{pathMinorista}/df_direccion.csv", index=False)
    data = pd.merge(data, dx[["ID_COD","ID_DIR"]], how='left', on=["ID_COD"])
    data.drop(["ID_COD","ID_RS","COD_ACT","ID_DPD","DIRECCION"],axis=1,inplace=True)
    
    # PRECIO
    data['PRECIODEVENTASoles'] = pd.to_numeric(data['PRECIODEVENTASoles'], errors='coerce')
    data['PRECIODEVENTASOLES'] = pd.to_numeric(data['PRECIODEVENTASOLES'], errors='coerce')
    data['PRECIOVENTA'] = data[['PRECIODEVENTASOLES', 'PRECIODEVENTASoles', 'PRECIODEVENTA']].sum(axis=1, skipna=True)
    data.drop(["PRECIODEVENTASoles","PRECIODEVENTASOLES","PRECIODEVENTA"],axis=1,inplace=True)
    
    # PRODUCTO/UNIDAD
    data['NOM_PROD'] = data.apply(lambda row: row['PRODUCTO'] if pd.isna(row['DESCRIPCIONPRODUCTO']) or row['DESCRIPCIONPRODUCTO'] == "" else row['DESCRIPCIONPRODUCTO'], axis=1)
    data["UNIDAD"] = ""
    data.loc[data["UNIDAD"].str.contains("Cilindros"),"UNIDAD"]="Kilogramos"
    data.loc[data["NOM_PROD"]=="GLP - E","UNIDAD"]="Kilogramos"
    data.loc[data["NOM_PROD"]=="GAS NATURAL VEHICULAR","UNIDAD"]="M3"
    data.loc[data["NOM_PROD"]=="GNV","UNIDAD"]="M3"
    data.loc[data["NOM_PROD"]=="GLP - G","UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("DIESEL"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("Diesel"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("GASOHOL"),"UNIDAD"]="GALONES"
    data.loc[data["NOM_PROD"].str.contains("GASOLINA"),"UNIDAD"]="GALONES"
    producto = data[["NOM_PROD","UNIDAD"]].drop_duplicates()
    producto = producto.sort_values(by="NOM_PROD")
    producto = producto.reset_index(drop=True)
    try:
        prodx = pd.read_csv(f"{pathMinorista}/df_producto.csv")
        producto = pd.concat([prodx, producto])
        producto = producto.drop_duplicates(subset=["NOM_PROD"])
        producto["COD_PROD"] = range(1, len(producto) + 1)
        producto.to_csv(f"{pathMinorista}/df_producto.csv", index=False)
    except:
        producto = producto.drop_duplicates(subset=["NOM_PROD"])     
        producto["COD_PROD"] = range(1, len(producto) + 1)
        producto.to_csv(f"{pathMinorista}/df_producto.csv", index=False)
    data.drop(["PRODUCTO","DESCRIPCIONPRODUCTO","UNIDAD"],axis=1,inplace=True)
    data = pd.merge(data,producto, how='left', on="NOM_PROD")
    data.drop(["NOM_PROD","UNIDAD"],axis=1,inplace=True)
    
    print("get data success...")
    data.to_csv(f"{pathMinorista}/df_precios.csv", index=False)
    
    # if a==0:
    #     data.to_stata("BASETOTAL_COMBUSTIBLES.dta", write_index=False)
    print("data success...")
    
    return data
