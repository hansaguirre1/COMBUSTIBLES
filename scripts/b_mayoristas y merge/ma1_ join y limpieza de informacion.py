# importar libreria

import os
import pandas as pd
import numpy as np
from tqdm import tqdm

import os

# Declarar ruta
dir=os.getcwd()
dir

new_dir_path = f'..\\data\\raw\\precios mayoristas'

# Guardar todo en lista

Precios_Mayoristas_list=[]
for file in os.listdir(new_dir_path):
    if file.endswith('.xlsx'):
        df=pd.read_excel(f'{new_dir_path}\\{file}')
        try:
            row_index = df.index[df['Unnamed: 1'] == 'ACTIVIDAD'].tolist()[0]
            df.columns = df.iloc[row_index]
        except:
            pass
        df["Tipo"]=file
        Precios_Mayoristas_list.append(df)

# Filtar por actividades de interés
b=[]
for a in Precios_Mayoristas_list:
    try:    
        a=a.drop(columns="NaN", errors='ignore')
        b.append(a.loc[ (a['ACTIVIDAD'].str.contains('DISTRIBUIDOR')) |
                        (a['ACTIVIDAD'].str.contains('PRODUCTOR')) |
                        (a['ACTIVIDAD'].str.contains('IMPORTADOR')) |
                        (a['ACTIVIDAD'].str.contains('PLANTA'))|
                        (a['ACTIVIDAD'].str.contains('MAYORISTAS'))|
                        (a['ACTIVIDAD'].str.contains('ACTIVIDAD'))|
                        (a['ACTIVIDAD'].str.contains('PROCESAMIENTO'))|
                        (a['ACTIVIDAD'].str.contains('COMERCIALIZADOR DE GLP'))
                        ])
    except:
        pass
# join todas las bases 

Total=pd.concat(b)

# Renombrar productos y codificar productos de interes

Precios_Mayoristas=Total

# Homogenizar producto
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GASOHOL 95 PLUS","GASOHOL PREMIUM")
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GASOHOL 90 PLUS","GASOHOL REGULAR")

Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GOH95","GASOHOL PREMIUM")
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GOH90","GASOHOL REGULAR")

Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GASOLINA 90","GASOLINA REGULAR")
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("GASOLINA 95","GASOLINA PREMIUM")
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("G90","GASOLINA REGULAR")
Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("G95","GASOLINA PREMIUM")

Precios_Mayoristas['PRODUCTO']=Precios_Mayoristas['PRODUCTO'].str.replace("'Cilindros de 10 Kg de GLP","GLP - E")



# Eliminar productos que no utilizamos

Precios_Mayoristas = Precios_Mayoristas[~(Precios_Mayoristas['PRODUCTO'].str.contains('Cilindros de 5 Kg de GL')) &
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('ASFALTO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('PETRÓLEO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('CEMENTO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('OIL'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('HEXANO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('Cilindros de 45 Kg de GLP'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('Cilindros de 15 Kg de GLP'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('Cilindros de 3 Kg de GLP'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GLP - E'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('MARINO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('TURBO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('IFO - 380 EXPORT'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('PENTANO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('BREA'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOLINA 84'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOHOL 84 PLUS'))&
                                        ~(Precios_Mayoristas['PRODUCTO']=='DIESEL B5')&
                                        ~(Precios_Mayoristas['PRODUCTO']=='DIESEL B5 GE')&
                                        ~(Precios_Mayoristas['PRODUCTO']=="Diesel B5 S-50")&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOLINA 97'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOHOL 97 PLUS'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOLINA 98 BA'))& 
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOHOL 98 PLUS'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('Diesel B5 S-50 GE'))&                                                                                
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('SOLVENTE'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOLINA 100 LL'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('GASOLINA 98'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('CGN SOLVENTE'))&  
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('DIESEL 2'))& 
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('PRODUCTO'))&
                                        ~(Precios_Mayoristas['PRODUCTO'].str.contains('Diesel 2'))
                                        ]


#codificar producto
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="GASOLINA REGULAR", 'COD_PROD'] = 46
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="GASOLINA PREMIUM", 'COD_PROD'] = 45
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="GASOHOL REGULAR", 'COD_PROD'] = 37
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="GASOHOL PREMIUM", 'COD_PROD'] = 36
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="Cilindros de 10 Kg de GLP", 'COD_PROD'] = 47
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="GLP - G", 'COD_PROD'] = 48
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="DIESEL B5 S-50 UV", 'COD_PROD'] = 28
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="DIESEL B5 UV", 'COD_PROD'] = 19
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="Diesel B5 S-50 UV", 'COD_PROD'] = 28
Precios_Mayoristas.loc[Precios_Mayoristas.PRODUCTO=="Diesel B5 UV", 'COD_PROD'] = 19



# Declarar fecha

Precios_Mayoristas['FECHA DE REGISTRO'] = pd.to_datetime(Precios_Mayoristas['FECHA DE REGISTRO'], format='%Y-%m-%d')

Precios_Mayoristas['DIA'] =Precios_Mayoristas['FECHA DE REGISTRO'].dt.day.astype(str)
Precios_Mayoristas['MES'] =Precios_Mayoristas['FECHA DE REGISTRO'].dt.month.astype(str)
Precios_Mayoristas['AÑO'] =Precios_Mayoristas['FECHA DE REGISTRO'].dt.year.astype(str)


#mes

Precios_Mayoristas['MES']='0'+Precios_Mayoristas['MES']
Precios_Mayoristas['MES']=Precios_Mayoristas['MES'].str.replace('010','10')
Precios_Mayoristas['MES']=Precios_Mayoristas['MES'].str.replace('011','11')
Precios_Mayoristas['MES']=Precios_Mayoristas['MES'].str.replace('012','12')

#día


Precios_Mayoristas['fecha_stata']=Precios_Mayoristas['AÑO']+"-"+Precios_Mayoristas['MES']+"-"+Precios_Mayoristas['DIA']
Precios_Mayoristas['fecha_stata'] = pd.to_datetime(Precios_Mayoristas['fecha_stata'], format='%Y-%m-%d')


# Llenar valores nan de GLP-E principalmente

Precios_Mayoristas['PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas['PRECIO DE VENTA (SOLES)'].fillna((Precios_Mayoristas['PRECIO_MIN (SOLES)'] + Precios_Mayoristas['PRECIO_MAX (SOLES)'])/2)

def replace_value(row):
    if row['PRECIO DE VENTA (SOLES)'] > 100 and not np.isnan(row['PRECIO_MIN (SOLES)']) and not np.isnan(row['PRECIO_MAX (SOLES)']):
        return (row['PRECIO_MIN (SOLES)'] + row['PRECIO_MAX (SOLES)'])/2
    else:
        return row['PRECIO DE VENTA (SOLES)']

# Apply the function to the DataFrame
Precios_Mayoristas['PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas.apply(replace_value, axis=1)

Precios_Mayoristas = Precios_Mayoristas[Precios_Mayoristas['PRECIO DE VENTA (SOLES)'] <= 100]


#Conversión de litros a galones
Precios_Mayoristas.reset_index()


def replace_value_litros(row):
    if row['PRECIO DE VENTA (SOLES)'] > 5 and row['COD_PROD'] == 48:
        return (row['PRECIO DE VENTA (SOLES)'])/ 3.78533
    else:
        return row['PRECIO DE VENTA (SOLES)']


# Apply the function to the DataFrame
Precios_Mayoristas['PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas.apply(replace_value_litros, axis=1)


Precios_Mayoristas.loc[Precios_Mayoristas['COD_PROD'] == 48, 'PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas.loc[Precios_Mayoristas['COD_PROD'] == 30, 'PRECIO DE VENTA (SOLES)'] /  0.5324

# Redondear precios a dos decimales

Precios_Mayoristas['PRECIO DE VENTA (SOLES)']=Precios_Mayoristas['PRECIO DE VENTA (SOLES)'].round(2)

# Guardar data de precios mayoristas sin imputar

Precios_Mayoristas.to_csv(f"..//data//interim//precios mayoristas//mayoristas_pre_imp.csv", sep=";", encoding="utf-8")





