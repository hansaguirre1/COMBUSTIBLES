import pdfplumber
import pandas as pd
import PyPDF2
import re
import datetime
from openpyxl import load_workbook
import numpy as np

a
# Definir funcion para extraer tabla

def extraer_tabla(file):

    with pdfplumber.open(pagina_pdf) as pdf:
        page = pdf.pages[0]  
        tables = page.extract_tables()    

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        precio = pd.DataFrame(tables[0])

    row_index = precio.index[precio[0] == 'DEPARTAMENTO'].tolist()[0]
    precio.columns = precio.iloc[row_index]
    row_index_2 = precio.index[precio['DEPARTAMENTO'] == 'AMAZONAS'].tolist()[0]
    precio=precio[row_index_2:]
    precio=precio[:-5]

    precio['Fecha']=pagina_pdf

    return precio

# Definir funcion para hacer lista la tabla

def make_list(precio):
    precio.replace("", np.nan, inplace = True)
    lista = precio.melt(id_vars=["Fecha", "DEPARTAMENTO"], var_name="Combustible", value_name="Precios")
    lista = lista.sort_values(by= ["Fecha", "DEPARTAMENTO"])
    lista = lista.dropna(subset=["Precios"])
    return lista

#  Definir funcion para extraer los nombres de los files pdfs

def list_pdf_files(directory):
    pdf_files = [file for file in os.listdir(directory) if file.endswith(".pdf")]
    return pdf_files

#DECLARAR TODOS LOS PDFS
directory_path = r"..\\data\\raw\\combustibles validos"

file_list = list_pdf_files(directory_path)
file_list
# Acumular información de pdfs

Acumulado =[]
for file in file_list:
    pagina_pdf = f"{directory_path}\{file}"   
    for pagina in range(0,1):
        for tabla in range(0,1):
            print(pagina)
            print(tabla)
            try:
                df=extraer_tabla(pagina_pdf)
                df=make_list(df)
                Acumulado.append(df)
            except:
                pass
            
Demanda_por_region=pd.concat(Acumulado)

#renombrar combustibles
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("Gasohol 95\nPlus","GASOHOL PREMIUM")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("Gasohol 90\nPlus","GASOHOL REGULAR")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("GASOHOL\nPREMIUM","GASOHOL PREMIUM")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("GASOHOL\nREGULAR","GASOHOL REGULAR")


Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("Gasolina\n90","GASOLINA REGULAR")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("Gasolina\n95","GASOLINA PREMIUM")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("GASOLINA\nREGULAR","GASOLINA REGULAR")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("GASOLINA\nPREMIUM","GASOLINA PREMIUM")


Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("Diesel B5","DIESEL B5 UV")
Demanda_por_region['Combustible']=Demanda_por_region['Combustible'].str.replace("DB5 S-50","DIESEL B5 S-50 UV")


#codificar Combustible
Demanda_por_region.loc[Demanda_por_region.Combustible=="GASOLINA REGULAR", 'COD_PROD'] = 62
Demanda_por_region.loc[Demanda_por_region.Combustible=="GASOLINA PREMIUM", 'COD_PROD'] = 61
Demanda_por_region.loc[Demanda_por_region.Combustible=="GASOHOL REGULAR", 'COD_PROD'] = 60
Demanda_por_region.loc[Demanda_por_region.Combustible=="GASOHOL PREMIUM", 'COD_PROD'] = 59
Demanda_por_region.loc[Demanda_por_region.Combustible=="Cilindros de 10 Kg de GLP", 'COD_PROD'] = 29
Demanda_por_region.loc[Demanda_por_region.Combustible=="GLP - G", 'COD_PROD'] = 30
Demanda_por_region.loc[Demanda_por_region.Combustible=="DIESEL B5 S-50 UV", 'COD_PROD'] = 15
Demanda_por_region.loc[Demanda_por_region.Combustible=="DIESEL B5 UV", 'COD_PROD'] = 9
Demanda_por_region.loc[Demanda_por_region.Combustible=="Diesel B5 S-50 UV", 'COD_PROD'] = 15
Demanda_por_region.loc[Demanda_por_region.Combustible=="DIESEL B5 UV", 'COD_PROD'] = 9

# Eliminar productos que no utilizamos

Demanda_por_region = Demanda_por_region[~(Demanda_por_region['Combustible'].str.contains('Gasolina\n84')) &
                                        ~(Demanda_por_region['Combustible'].str.contains('Total'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Turbo'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('TOTAL'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Gasohol 84\nPlus'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('HEXANO'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Pet'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Gas\n100LL'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Diesel\nMGO'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('IFO'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('Gasolina\n97'))&
                                        ~(Demanda_por_region['Combustible'].str.contains('JP 5'))&  
                                        ~(Demanda_por_region['Combustible'].str.contains('Gasolina\n98'))&  
                                        ~(Demanda_por_region['Combustible'].str.contains('Gasohol 97\nPlus'))& 
                                        ~(Demanda_por_region['Combustible'].str.contains('Gasohol 98\nPlus'))
                                        ]


# guardar tabla

Demanda_por_region.to_csv(r"../data/processed/df_validos_dpt.csv", encoding="utf-8", index=False)