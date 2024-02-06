import pandas as pd
import os

# Directorio
os.chdir(os.getcwd())

# Nombres
BASE_DLCC = "BASETOTAL_DLC.dta"
BASE_DLC = "Base_apilada.csv"
DF_rs = "df_razon_social.csv"
DF_act = "df_actividad.csv"
DF_dir = "df_direccion.csv"
DF_ubi = "df_ubicacion.csv"
DF_cod = "df_codigoosinerg.csv"
DF_prod = "df_producto.csv"
DF_ubiccpp = "ubigeo_ccpp.csv"
DF_ubigeo = "TB_UBIGEOS.csv"
DF_georef = "cadenaHL_georef.xlsx"
DF_val = "df_volumenes_departamento.csv"
DF_val2 = "df_validos_dpt.csv"
DF_imp = "df_ind_imp.csv"
DF_may = "mayoristas_pre_imp.csv"
DF_georef_may = "mayoristas_pre_imp_geo.csv"
DF_may_fin = "precios_mayoristas_imp.csv"
DF_fin = "df_indicadores_cm.csv"
DF_fin2 = "df_indicadores_sm.csv"
DF_base_comb2 = "BASETOTAL_COMBUSTIBLES2.csv"
DF_dir_may = "df_may_min_geo.csv"
DF_dir_may2 = "df_may_min_geo2.csv"
DF_min_bi = "df_minorista.csv"
DF_may_bi = "df_mayorista.csv"
DF_petroperu = "Petroperu_Lista.csv"
ruta = r"..\data\interim\precios minoristas\\"
ruta2 = r"..\data\raw\precios minoristas\\"
ruta3 = r"..\data\external\\"
ruta4 = r"..\data\processed\\"
ruta5 = r"..\data\raw\combustibles validos\\"
ruta6 = r"..\data\interim\\"
ruta7 = r"..\data\interim\precios mayoristas\\"
ruta8 = r"..\data\interim\combustibles validos\\"

# Fechas
#fecha_manual = pd.to_datetime(datetime.now().date() - timedelta(days=1))
#fecha_manual = pd.to_datetime('2024-01-26')  # Reemplaza con la fecha que desees
try:
    with open(ruta6 + "fechas.txt", 'r') as archivo:
        lineas = archivo.readlines()    
    f1 = lineas[0].strip()  # strip() elimina espacios en blanco y saltos de línea
    f1 = pd.to_datetime(f1)
    f2 = lineas[1].strip()
    f2 = pd.to_datetime(f2)
except:
    pass

# Bases para merge
try:
    ubi = pd.read_csv(ruta4 + DF_ubi, encoding='utf-8', sep=";")
    cod = pd.read_csv(ruta4 + DF_cod, encoding='utf-8', sep=";")
    rs = pd.read_csv(ruta4 + DF_rs, encoding='utf-8', sep=";")
    prod = pd.read_csv(ruta4 + DF_prod, encoding='utf-8', sep=";")
    dir = pd.read_csv(ruta4 + DF_dir, encoding='utf-8', sep=";")
    
    # Listas
    a = "DIESEL B5 UV"
    b = "GAS NATURAL VEHICULAR"
    c = "GASOHOL PREMIUM"
    d = "GASOHOL REGULAR"
    e = "GASOLINA PREMIUM"
    f = "GASOLINA REGULAR"
    g = "GLP - E"
    h = "GLP - G"
    m = "Diesel B5 S-50 UV"
    
    # Nombres productos
    aa = prod["COD_PROD"].loc[prod["NOM_PROD"]==a].values[0]
    bb = prod["COD_PROD"].loc[prod["NOM_PROD"]==b].values[0]
    cc = prod["COD_PROD"].loc[prod["NOM_PROD"]==c].values[0]
    dd = prod["COD_PROD"].loc[prod["NOM_PROD"]==d].values[0]
    ee = prod["COD_PROD"].loc[prod["NOM_PROD"]==e].values[0]
    ff = prod["COD_PROD"].loc[prod["NOM_PROD"]==f].values[0]
    gg = prod["COD_PROD"].loc[prod["NOM_PROD"]==g].values[0]
    hh = prod["COD_PROD"].loc[prod["NOM_PROD"]==h].values[0]
    mm = prod["COD_PROD"].loc[prod["NOM_PROD"]==m].values[0]
    nom_prods = {a: aa, b: bb, c: cc, d: dd, e: ee, f: ff, g: gg, h: hh, m: mm}
    cod_prods=list(nom_prods.values())

except:
    pass




