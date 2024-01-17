
import pandas as pd
import numpy as np
from tqdm import tqdm

import os

#Funciones para completar precios y días 
def process2(ii):
    for i in range(1, len(df_2)):
        if (df_2['RUC-prov'][i] == df_2['RUC-prov'][i-1]) and pd.isnull(df_2[ii][i]):
            df_2.at[i, ii] = df_2[ii][i-1]


# Costo otros
df = pd.read_csv("..\\data\\interim\\precios mayoristas\\mayoristas_pre_imp.csv",encoding='utf-8',sep=";")
df.rename(columns={"PRECIO DE VENTA (SOLES)": "PRECIOVENTA"},inplace=True)
df.rename(columns={"PROVINCIA_VENDEDOR": "PROVINCIA"},inplace=True)
df['fecha_stata'] = pd.to_datetime(df['fecha_stata'], infer_datetime_format=True, errors='coerce')
df["RUC-prov"] = df["RUC"].astype(str)+"-"+df["PROVINCIA"]
df.COD_PROD.value_counts()
cod_prods = [ 46, 45, 37, 36, 47, 48, 28, 19]
#Completar precios y dias por producto
""" for k in cod_prods:
    print(k)
    df_2 = df.copy()
    df_2 = df_2[df_2['COD_PROD'] == k]
    df_2['PRECIOVENTA_'] = df_2['PRECIOVENTA']
    df_2 = df_2.groupby(['fecha_stata', "RUC-prov"]).agg({'PRECIOVENTA': 'mean', 'PRECIOVENTA_': 'last'}).reset_index()
    df_2 = df_2.sort_values(['RUC-prov', 'fecha_stata'])
    df_2['dias_faltantes'] = (df_2['fecha_stata'].diff()).dt.days - 1
    df_2.loc[df_2["dias_faltantes"] < 0, "dias_faltantes"] = np.nan
    df_2['dias_faltantes'] = df_2['dias_faltantes'].fillna(0)
    fecha_minima = df_2['fecha_stata'].min()
    fecha_maxima = df_2['fecha_stata'].max()
    rango_fechas_completo = pd.date_range(fecha_minima, fecha_maxima, freq='D')
    combinaciones = pd.DataFrame([(id, fecha) for id in df_2['RUC-prov'].unique(
    ) for fecha in rango_fechas_completo], columns=['RUC-prov', 'fecha_stata'])
    df_2 = pd.merge(combinaciones, df_2, on=['RUC-prov', 'fecha_stata'], how='outer')
    df_2 = df_2.sort_values(by=['RUC-prov', 'fecha_stata'])
    df_2 = df_2.reset_index(drop=True)
    num_cpus = os.cpu_count()
    #with ProcessPoolExecutor(max_workers=num_cpus) as executor:
    #    executor.map(process2, ["PRECIOVENTA_","dias_faltantes"])
    process2("PRECIOVENTA_")
    process2("dias_faltantes")
    df_2['PRECIOVENTAx'] = df_2['PRECIOVENTA_']
    q = 1
    for i in range(1, len(df_2)):
        if q < 100:
            if (df_2.at[i, 'RUC-prov'] == df_2.at[i-1, 'RUC-prov']) and pd.isnull(df_2.at[i, 'PRECIOVENTA']):
                df_2.at[i, 'PRECIOVENTA'] = df_2.at[i-1, 'PRECIOVENTA']
            q += 1
    
        if i < len(df_2) - 1 and not pd.isnull(df_2.at[i+1, 'PRECIOVENTA']):
            q = 1
    #df_2.rename(columns={'CODIGOOSINERG': 'COD'}, inplace=True)
    df_2.drop(columns=['PRECIOVENTAx', 'PRECIOVENTA_','dias_faltantes'], inplace=True)
    df_2["COD_PROD"] = k
    df_2.to_csv(f"..\\data\\interim\\precios mayoristas\\base_final_{k}.csv",  encoding="utf-8",  index=False) """


for k in cod_prods:
    print(k)
    try:
        df_2 = df.copy()
        df_2 = df_2[df_2['COD_PROD'] == k]
        df_2['PRECIOVENTA_'] = df_2['PRECIOVENTA']
        df_2 = df_2.groupby(['fecha_stata', "RUC-prov"]).agg({'PRECIOVENTA': 'mean', 'PRECIOVENTA_': 'last'}).reset_index()
        df_2 = df_2.sort_values(['RUC-prov', 'fecha_stata'])
        df_2['dias_faltantes'] = (df_2['fecha_stata'].diff()).dt.days - 1
        df_2.loc[df_2["dias_faltantes"] < 0, "dias_faltantes"] = np.nan
        df_2['dias_faltantes'] = df_2['dias_faltantes'].fillna(0)
        fecha_minima = df_2['fecha_stata'].min()
        fecha_maxima = df_2['fecha_stata'].max()
        rango_fechas_completo = pd.date_range(fecha_minima, fecha_maxima, freq='D')
        combinaciones = pd.DataFrame([(id, fecha) for id in df_2['RUC-prov'].unique(
        ) for fecha in rango_fechas_completo], columns=['RUC-prov', 'fecha_stata'])
        df_2 = pd.merge(combinaciones, df_2, on=['RUC-prov', 'fecha_stata'], how='outer')
        df_2 = df_2.sort_values(by=['RUC-prov', 'fecha_stata'])
        df_2 = df_2.reset_index(drop=True)
        num_cpus = os.cpu_count()
        #with ProcessPoolExecutor(max_workers=num_cpus) as executor:
        #    executor.map(process2, ["PRECIOVENTA_","dias_faltantes"])
        process2("PRECIOVENTA_")
        process2("dias_faltantes")
        df_2['PRECIOVENTAx'] = df_2['PRECIOVENTA_']
        # q = 1
        # for i in range(1, len(df_2)):
        #     if q < 100:
        #         if (df_2.at[i, 'RUC-prov'] == df_2.at[i-1, 'RUC-prov']) and pd.isnull(df_2.at[i, 'PRECIOVENTA']):
        #             df_2.at[i, 'PRECIOVENTA'] = df_2.at[i-1, 'PRECIOVENTA']
        #         q += 1
        
        #     if i < len(df_2) - 1 and not pd.isnull(df_2.at[i+1, 'PRECIOVENTA']):
        #         q = 1
        mask = (df_2['RUC-prov'] == df_2['RUC-prov'].shift(1)) & pd.isnull(df_2['PRECIOVENTA'])    
        df_2['PRECIOVENTA'] = df_2['PRECIOVENTA'].mask(mask, df_2['PRECIOVENTA'].ffill())    
        reset_mask = ~pd.isnull(df_2['PRECIOVENTA'].shift(-1))    
        df_2['q'] = (reset_mask.cumsum() % 14) + 1    
        df_2 = df_2[df_2['q'] < 14].copy()
        df_2 = df_2.drop(columns=['q'])

        df_2.drop(columns=['PRECIOVENTAx', 'PRECIOVENTA_','dias_faltantes'], inplace=True)
        df_2["COD_PROD"] = k
        df_2.to_csv(f"../data/interim/precios mayoristas/base_final_{k}.csv", index=False, encoding="utf-8")
    except:
        pass


# guardar todos los productos en una lista
dfs = []
p = 1
for k in cod_prods:
    print(k)
    exec(f'base_{p} = pd.read_csv("../data/interim/precios mayoristas/base_final_{k}.csv", encoding="utf-8")')
    dfs.append(f"base_{p}")
    exec(f'os.remove("../data/interim/precios mayoristas/base_final_{k}.csv")')
    p += 1

dfs2 = [globals()[f"base_{i}"] for i in range(1, len(dfs) + 1) if f"base_{i}" in globals()]
del base_1,base_2,base_3,base_4,base_5,base_6,base_7,base_8


#unir todas las bases completas por día
df_concatenado = pd.concat(dfs2, ignore_index=True)
del dfs2

# guardar información previa a la imputación
df_concatenado.to_csv("..\\data\\interim\\precios mayoristas\\precios_mayoristas_imp.csv", encoding="utf-8", index=False,sep=";")

del df_concatenado
#del d


