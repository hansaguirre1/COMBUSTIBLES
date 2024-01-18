import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from concurrent.futures import ProcessPoolExecutor

# Teoría
# MR:
# m1: Diesel es para transportistas de carga (MR es distrito)
# m2: GLP-E es los balones de gas
# m3: GNV (2 km) y GLP-G son lo que me pasó hans
# m4: Gasohol, gasolina, son taxistas (MR es KMeans) 

# Desactivar todas las advertencias de pandas
warnings.filterwarnings("ignore")

# Funciones
def generar_valor_unico(columna):
    conteo = columna.groupby(columna).cumcount() + 1
    sufijo = '-' + conteo.astype(str)
    valores_unicos = columna.astype(str) + sufijo.where(conteo >= 1, '')
    return valores_unicos

def m1(df):
    df = df.loc[df["COD_PROD"]==15]
    valores_unicos = df['UBI'].unique()
    df = df.drop_duplicates(subset=["ID_DIR"])
    df['grupo'] = df['UBI'].map(dict(zip(valores_unicos, range(1, len(valores_unicos) + 1))))
    return df

def m3(df,dist,cod,n1,n2):
    df = df.loc[df["COD_PROD"]==cod]
    df = df.drop_duplicates(subset=["ID_DIR"])
    df.sort_values(by='COD', inplace=True) 
    print(len(df))
    df['lat2'] = df['lat']
    df['lon2'] = df['lon']
    df['lat2'] = df['lat2'] * (np.pi / 180)
    df['lon2'] = df['lon2'] * (np.pi / 180)
    df = df.dropna(subset=['lat2','lon2'])
    df['Code'] = range(0, len(df))
    df = df.reset_index(drop=True)
    for i in range(0, len(df)):
        x1 = 6378 * np.cos(df.loc[df['Code'] == i, 'lat2'].values[0]) * np.cos(df.loc[df['Code'] == i, 'lon2'].values[0])
        y1 = 6378 * np.cos(df.loc[df['Code'] == i, 'lat2'].values[0]) * np.sin(df.loc[df['Code'] == i, 'lon2'].values[0])
        z1 = 6378 * np.sin(df.loc[df['Code'] == i, 'lat2'].values[0])
        for z in range(0, len(df)):
            x2 = 6378 * np.cos(df.loc[df['Code'] == z, 'lat2'].values[0]) * np.cos(df.loc[df['Code'] == z, 'lon2'].values[0])
            y2 = 6378 * np.cos(df.loc[df['Code'] == z, 'lat2'].values[0]) * np.sin(df.loc[df['Code'] == z, 'lon2'].values[0])
            z2 = 6378 * np.sin(df.loc[df['Code'] == z, 'lat2'].values[0])
            d = np.sqrt((x1 - x2)**2 + (y1 - y2)**2 + (z1 - z2)**2)
            angle = np.arcsin((d / (2 * (6378**2))) * np.sqrt(4 * (6378**2) - (d**2)))    
            df.loc[df['Code'] == z, f'Desf{i}'] = angle * 6378
            df.loc[df['Code'] == z, f'Dheu{i}'] = d

    base_coord = df.copy()
    dfx = pd.DataFrame()
    for i in range(0, len(df)):
        print(i)
        estacion_df = df.copy()
        estacion_df["Estación"]=i
        #estacion_df = df.loc[df['Estación'] == i]
        estacion_df.rename(columns={f'Desf{i}': 'Desf', f'Dheu{i}': 'Dheu'}, inplace=True)
        dfx = pd.concat([dfx, estacion_df], ignore_index=True)
    dfx = dfx.loc[dfx["Dheu"]<=dist]
    #dfx = dfx.drop_duplicates(subset=["Estación","Desf"])
    dfx["grupo"] = np.nan
    dfx2 = dfx.copy()
    #return dfx, estacion_df

    # Iterar sobre y de 1 a 50
    for y in range(0, n1):
        print(y)
        
        # Filtrar las observaciones con grupo nulo
        dfx = dfx[dfx['grupo'].isnull()]    
        esta_num = dfx['Estación'].min()    
        dfx.loc[dfx['Estación'] == esta_num, 'grupo'] = y
        
        # Guardar el DataFrame actual
        exec(f"dfx.to_csv('base_match_{y}.csv')")
    
        # Iterar sobre z de 1 a 30
        for z in range(0, n2):
            #print(z)
            # Cargar el DataFrame actual
            df_match=pd.read_csv(f'base_match_{y}.csv')
            df_match = df_match[df_match['grupo'] == y]
            df_match = df_match[['Code', 'grupo']]
            df_match.sort_values(by='Code', inplace=True)
            df_match['Code_anterior'] = df_match['Code'].shift(1)
            df_match = df_match[df_match['Code'] != df_match['Code_anterior']]
            df_match.drop(columns=['Code_anterior'], inplace=True)
            df_match=df_match.dropna(subset="Code")
            df_match.rename(columns={'Code': 'Estación'}, inplace=True)
            df_match_estacion = pd.read_csv(f'base_match_{y}.csv')            
            df_match_estacion.drop(columns=['grupo'], inplace=True)            
            df_match_estacion = pd.merge(df_match_estacion, df_match, how='outer', on='Estación', indicator=True)            
            df_match_estacion.drop(columns=['_merge'], inplace=True)            
            df_match_estacion.to_csv(f'base_match_{y}.csv', index=False)

    df = pd.read_csv('base_match_0.csv')
    df = df.dropna(subset=['grupo'])
    for y in range(1, n1):
        df_actual = pd.read_csv(f'base_match_{y}.csv')    
        df_actual = df_actual.dropna(subset=['grupo'])    
        df = pd.concat([df, df_actual], ignore_index=True)    
        os.remove(f'base_match_{y}.csv')
    os.remove('base_match_0.csv')
    for var in ["grupo","Code","Estación"]:
        indice_var1 = df.columns.get_loc(var)    
        columnas_ordenadas = [var] + [col for col in df.columns if col != var]
        df = df[columnas_ordenadas]
    result = pd.merge(base_coord[['Code','ID_DIR','fecha_stata','lat','lon']], df[['Code','fecha_stata','ID_DIR','lat','lon','grupo']], how='outer', on=['Code','fecha_stata','ID_DIR','lat','lon'], indicator=True)
    result.drop_duplicates(subset='ID_DIR', keep='first', inplace=True)
    #result = result[["COD","fecha_stata","COD_PROD","lat","lon","grupo"]]
    result=result.dropna(subset="ID_DIR")
    return result

def m4(df,cod,por):
    df = df.loc[df["COD_PROD"]==cod]
    df = df.drop_duplicates(subset=["ID_DIR"])
    print(len(df))
    X = df[["lat","lon"]]
    inercia = []
    for k in range(1, 30):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X)
        inercia.append(kmeans.inertia_)
    
    percentage_change = np.diff(inercia) / inercia[0] * -100
    posiciones = np.where(percentage_change <= 1)[0]
    print(inercia)
    print(posiciones[0]+1, percentage_change)
    # plt.plot(range(1, 11), inercia, marker='o')
    # plt.title('Método de Elbow para k-means')
    # plt.xlabel('Número de clústeres (k)')
    # plt.ylabel('Inercia')
    # plt.show()
    kmeans = KMeans(n_clusters=posiciones[0]+1, random_state=42)
    kmeans.fit(X)
    predicciones = kmeans.predict(X)
    df["grupo"] = predicciones
    return df

# Listas
cod_prods = [9, 16, 29, 30, 15, 59, 60, 61, 62]
# * GNV: 16
# * Gasohol premium: 59
# * Gasohol regular: 60
# * Gasolina premium: 61
# * Gasolina regular: 62
# * GLP-E: 29
# * GLP-G: 30
# * Diesel B5 S50 UV: 15

# Bases para merge
cod = pd.read_csv(r"../data/processed/df_codigoosinerg.csv", encoding='utf-8')
ubi = pd.read_csv(r"..\data\interim\precios minoristas\df_ubicacion.csv", encoding='utf-8')
rs = pd.read_csv(r"..\data\interim\precios minoristas\df_razon_social.csv", encoding='utf-8')
prod = pd.read_csv(r"..\data\processed\df_producto.csv", encoding='utf-8')
dir = pd.read_csv(r"..\data\interim\precios minoristas\df_direccion.csv", encoding='utf-8')
base = pd.read_csv(r"..\data\processed\base.csv", encoding='utf-8')

# Mercados relevantes (base 2.csv sale del 2.A.1_data_quality_imputacion)
df = pd.read_csv(r"..\data\interim\BASETOTAL_COMBUSTIBLES2.csv", encoding="utf-8")
df = pd.merge(df, dir, how='left', on='ID_DIR', indicator=True)
df = df[df['_merge'] == 'both']
df = df.drop(columns=['_merge'])
df = pd.merge(df, cod, how='left', on='ID_COD', indicator=True)
df = df[df['_merge'] == 'both']
df = df.drop(columns=['_merge'])
#df = df.rename(columns={"CODIGOOSINERG": "COD"})
df = df.groupby(['ID_DIR', 'fecha_stata', 'COD_PROD']).agg({'PRECIOVENTA': 'last'}).reset_index()
df = pd.merge(df, base, how='left', on='ID_DIR', indicator=True)
df = df[df['_merge'] == 'both']
df = df.drop(columns=['_merge'])
df = df[["ID_DIR","fecha_stata","COD_PROD","lat","lon","UBI"]]
df = df.dropna()
df1_15 = m1(df)
#df2_29 = m2(df,29)
df2_9 = m4(df,9,1)
df2_29 = m4(df,29,1)
#df3_16= m3(df,2,16,30,5)
#df3_30= m3(df,2,30,30,5)
df3_16 = m4(df,16,1)
df3_30 = m4(df,30,1)
df4_61 = m4(df,61,1)
df4_62 = m4(df,62,1)
df4_60 = m4(df,60,1)
df4_59 = m4(df,59,1)

df_concat = pd.concat([df1_15,df2_9,df2_29,df3_16,df3_30,df4_61,df4_62,df4_60,df4_59],axis=0)
df_concat = df_concat[["ID_DIR","COD_PROD","grupo"]]
df_concat["ID_COL"] = df_concat["ID_DIR"].astype(str) + "-" + df_concat["COD_PROD"].astype(str)
df_concat.to_csv(r"..\data\processed\df_mr.csv",index=False)


















