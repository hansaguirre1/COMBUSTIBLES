# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from minfut0_nombres import *

# Funciones
def clean(df, clean):
    merged_data = pd.merge(df, clean, how='left', indicator=True)
    merged_data['PRECIOVENTA'] = merged_data['p'].where(
        merged_data['_merge'] == 'both', merged_data['PRECIOVENTA'])
    merged_data = merged_data.drop(['p', '_merge'], axis=1)
    return merged_data

def re_global(dfx):
    dfx['raro2'] = dfx.groupby('ID_DIR')['raro'].transform('sum')
    dfx = dfx.sort_values(['ID_DIR', 'fecha_stata'])
    dfx = dfx[dfx['raro2'] > 0]
    dfx = dfx.reset_index(drop=True)
    for i in range(2, len(dfx)-2):
        # Verificar las condiciones de raro y ID_COD
        if (dfx.at[i, 'raro'] == 1 and dfx.at[i, 'ID_DIR'] == dfx.at[i - 1, 'ID_DIR'] == dfx.at[i - 2, 'ID_DIR'] == dfx.at[i + 1, 'ID_DIR'] == dfx.at[i + 2, 'ID_DIR']):
            new_value = (dfx.at[i - 2, 'PRECIOVENTA']+dfx.at[i - 1, 'PRECIOVENTA'] + dfx.at[i + 1, 'PRECIOVENTA']+dfx.at[i + 2, 'PRECIOVENTA']) / 4
            dfx.at[i, 'PRECIOVENTA'] = new_value
    dfx['PRECIOVENTA'] = dfx['PRECIOVENTA'].where(~dfx['raro'], dfx['PRECIOVENTA'].shift(1))
    dfx = dfx[['n', 'PRECIOVENTA']]
    dfx.rename(columns={'PRECIOVENTA': 'p'}, inplace=True)
    return dfx

def re_interno(dfx):
    dfx['raro'] = dfx['chmenos'].astype(int)
    dfx['raro2'] = dfx.groupby('ID_DIR')['raro'].transform('sum')
    dfx = dfx[dfx['raro2'] > 0].reset_index()
    for i in range(2, len(dfx)-2):
        # Verificar las condiciones de raro y ID_COD
        if (dfx.at[i, 'raro'] == 1 and
                dfx.at[i, 'ID_DIR'] == dfx.at[i - 1, 'ID_DIR'] == dfx.at[i - 2, 'ID_DIR'] == dfx.at[i + 1, 'ID_DIR'] == dfx.at[i + 2, 'ID_DIR']):
            new_value = (dfx.at[i - 2, 'PRECIOVENTA']+dfx.at[i - 1, 'PRECIOVENTA'] +
                         dfx.at[i + 1, 'PRECIOVENTA']+dfx.at[i + 2, 'PRECIOVENTA']) / 4
            dfx.at[i, 'PRECIOVENTA'] = new_value
    dfx = dfx[['n', 'PRECIOVENTA']].rename(columns={'PRECIOVENTA': 'p'})
    return dfx

def limp(cod, r1, r2, ch1, chd, df_, df):
    print(cod)
    dfx = df[df['COD_PROD'] == cod]
    dfx['raro'] = ((dfx['PRECIOVENTA'] > r1) | (
        dfx['PRECIOVENTA'] < r2)) & (dfx['PRECIOVENTA'].notna())
    dfx = re_global(dfx)
    df_ = clean(df_, dfx)

    # Internos
    dfx = df[df['COD_PROD'] == cod]
    dfx = dfx.groupby(['ID_DIR', 'fecha_stata']).agg({'PRECIOVENTA': 'mean', 'n': 'first'}).reset_index()
    # dfx = dfx.set_index(['ID_COD', 'fecha_stata'])
    dfx['ch'] = dfx['PRECIOVENTA'].diff()
    dfx['chmenos'] = (np.abs(dfx['ch']) >= ch1) & \
        (np.sign(dfx['ch']) != np.sign(dfx['ch'].shift(-1))) & \
        (np.abs(dfx['ch']) - np.abs(dfx['ch'].shift(-1)) < chd) & \
        (dfx['ch'].notna()) & \
        (dfx['ID_DIR'] == dfx['ID_DIR'].shift(-1))
    dfx = re_interno(dfx)
    df_ = clean(df_, dfx)
    return df_

def process(ii, df_2):
    for i in range(1, len(df_2)):
        if (df_2['ID_DIR'][i] == df_2['ID_DIR'][i-1]) and pd.isnull(df_2[ii][i]):
            df_2.at[i, ii] = df_2[ii][i-1]
    #condicion = (df_2['ID_DIR'] == df_2['ID_DIR'].shift(1)) & pd.isnull(df_2[ii])
    #df_2[ii] = df_2[ii].mask(condicion, df_2[ii].shift(1).ffill())
    return df_2


def agg_pan(df,fecha_manual=""):
    df['fecha_stata'] = pd.to_datetime(
        df['FECHADEREGISTRO'], format='%d/%m/%Y', errors='coerce')
    filas_con_NaT = df[df['fecha_stata'].isna()]
    filas_con_NaT['fecha1'] = pd.to_datetime(
        filas_con_NaT['FECHADEREGISTRO'], errors='coerce', infer_datetime_format=True)
    filas_con_NaT['fecha1'] = filas_con_NaT['fecha1'].dt.date
    df['fecha_stata'].fillna(filas_con_NaT['fecha1'], inplace=True)
    df.drop(["FECHADEREGISTRO"], axis=1, inplace=True)
    if fecha_manual!="":
        print("diario")
        df = df[(df['fecha_stata']<=f2) & (df["fecha_stata"]>=f1)]
    df = df.drop_duplicates()
    df = pd.merge(df, prod, how='left', on='COD_PROD', indicator=True)
    df = df[df['_merge'] == 'both']
    df = df.drop(columns=['_merge'])
    df.drop(["UNIDAD"], axis=1, inplace=True)
    df = df[df['NOM_PROD'] != g]
    df.loc[df['NOM_PROD'] == "Cilindros de 10 Kg de GLP", 'COD_PROD'] = nom_prods[g]
    df.loc[df['NOM_PROD'] == "GASOLINA 90", 'COD_PROD'] = nom_prods[f]
    df.loc[df['NOM_PROD'] == "GASOLINA 90", 'COD_PROD'] = nom_prods[e]
    df.loc[df['NOM_PROD'] == "GASOHOL 90 PLUS", 'COD_PROD'] = nom_prods[d]
    df.loc[df['NOM_PROD'] == "GASOHOL 95 PLUS", 'COD_PROD'] = nom_prods[c]
    df.loc[df['PRECIOVENTA'] < 0.51, 'PRECIOVENTA'] = pd.NA
    #df.to_csv(ruta6 + DF_base_comb2, index=False) # Descomentar si se desea Mercado relevante 
    valores_lista = dir.loc[dir["minorista"]==1]
    valores_lista = valores_lista["ID_DIR"]
    df = df[df['ID_DIR'].isin(valores_lista)]
    
    # Data cleaning
    print("Base 3")
    df['n'] = df.index + 1
    df.sort_values(by=['ID_DIR', 'fecha_stata'], inplace=True)
    df.loc[(df['PRECIOVENTA'] > 100) & (df['COD_PROD'] == nom_prods[m]), 'PRECIOVENTA'] = pd.NA
    df.loc[(df['PRECIOVENTA'] > 100) & (df['COD_PROD'] == nom_prods[d]), 'PRECIOVENTA'] = pd.NA
    df.loc[(df['PRECIOVENTA'] > 100) & (df['COD_PROD'] == nom_prods[c]), 'PRECIOVENTA'] = pd.NA
    df.loc[(df['PRECIOVENTA'] > 100) & (df['COD_PROD'] == nom_prods[g]), 'PRECIOVENTA'] = pd.NA
    df.loc[(df['PRECIOVENTA'] > 10) & (df['COD_PROD'] == nom_prods[b]), 'PRECIOVENTA'] = pd.NA
    df.loc[(df['PRECIOVENTA'] > 5) & (df['COD_PROD'] == nom_prods[h]),'PRECIOVENTA'] = df['PRECIOVENTA'] / 3.78533
    # df.loc[df['PRECIOVENTA'] < 1, 'PRECIOVENTA'] = df['PRECIOVENTA'] * 10
    df.loc[df['COD_PROD'] == nom_prods[h], 'PRECIOVENTA'] = df['PRECIOVENTA'] / 0.5324
    df.dropna(subset=['PRECIOVENTA'], inplace=True)
    return df
