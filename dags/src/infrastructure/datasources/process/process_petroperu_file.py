import pdfplumber
import pandas as pd
import PyPDF2
import re
import numpy as np


pagina_pdf = "data/raw/precios_mayoristas_petroperu/petroperu_info.pdf"
Acumulado =[]

def get_date(pagina_pdf):
    with open(pagina_pdf, "rb") as archivo_pdf:
        pdf_reader = PyPDF2.PdfReader(archivo_pdf)
        texto_extraido = ""
        for pagina in pdf_reader.pages:
            
            texto_pagina = pagina.extract_text()
            texto_extraido += texto_pagina

    ### Cambiar texto de los saltos de línea ynombres de variables de interés
    Texto_total=texto_extraido.replace("\n", "abcdef")

    patron_fecha = r'\d{2}\.\d{2}\.\d{4}'
    resultado = re.search(patron_fecha, Texto_total)
    if resultado:
        fecha = resultado.group()
    fecha= fecha.replace(".","/")   
    return fecha

def extraer_tabla(file,n_pagina,n_tab, fecha):
    #Extraer la primera tabla de la hoja 1
    with pdfplumber.open(file) as pdf:
        page = pdf.pages[n_pagina]  
        tables = page.extract_tables()

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        precio = pd.DataFrame(tables[n_tab])

    #Eliminar las columnas que no se necesita

    precio=precio.drop(precio.columns[[0]], axis=1)
    precio=precio.replace('\n',' ', regex=True)
    precio=precio.replace('G L P','GLP', regex=True)
    row_index = precio.index[precio[1] == 'PLANTAS'].tolist()[0]
    precio.columns = precio.iloc[row_index]

    precio=precio[row_index+1:]

    precio['Fecha']=fecha

    return precio

def make_list(precio):
    precio.replace("", np.nan, inplace = True)
    lista = precio.melt(id_vars=["Fecha", "PLANTAS"], var_name="Combustible", value_name="Precios")
    lista = lista.sort_values(by= ["Fecha", "PLANTAS"])
    lista = lista.dropna(subset=["Precios"])
    lista["Fecha"] = pd.to_datetime(lista["Fecha"], format="%d/%m/%Y")
    lista["Fecha"] =lista["Fecha"].dt.date
    return lista


def getDataPetroperu() -> pd.DataFrame:
    fecha=get_date(pagina_pdf)
    for pagina in range(0,4):
        for tabla in range(0,3):
            try:
                df=extraer_tabla(pagina_pdf,pagina,tabla, fecha)
                df=make_list(df)
                Acumulado.append(df)
            except Exception as err:
                print(f'erro--- {err}')
                pass
    
    print('----Acumulado----')
    print(Acumulado)
    print('----Acumulado----')
    return pd.concat(Acumulado)

def extractDataPage1() -> pd.DataFrame: 
    #Extraer la tabla de la hoja 1
    with pdfplumber.open(pagina_pdf) as pdf:
        page = pdf.pages[0]  
        tables = page.extract_tables()

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        print(tables)
        
        df1 = pd.DataFrame(tables[0])
        
    #Eliminar las columnas que no se necesita
    df1=df1.drop(df1.columns[[0, 1, 3, 7, 8, 9, 10]], axis=1)

    #Lo mismo con las filas
    fila_mantener = [7]
    df1 = df1.loc[fila_mantener]
    df1 = df1.rename(index={7: 0})

    #Renombrar las columnas 
    df1= df1.rename(columns={df1.columns[0]: "GLP_E" ,
                        df1.columns[1]: "Gasolina_Premium" ,
                        df1.columns[2]: "Gasolina_Regular" ,
                        df1.columns[3]: "Gasolina_84"})

    return df1


def extractDataPage2() -> pd.DataFrame:
    #Extraer la tabla de la hoja 2
    with pdfplumber.open(pagina_pdf) as pdf:
        page = pdf.pages[1]  
        tables = page.extract_tables()

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        df2 = pd.DataFrame(tables[0])
        
    #eliminar las columnas que no se necesita
    df2=df2.drop(df2.columns[[0, 1, 3]], axis=1)
    print(df2)

    #Lo mismo con las filas
    fila_mantener = [7]
    df2 = df2.loc[fila_mantener]
    df2 = df2.rename(index={7: 0})

    #Renombrar las columnas 
    df2= df2.rename(columns={df2.columns[0]: "DIESEL_B5_UV" ,
                        df2.columns[1]: "Gasohol_Premium" ,
                        df2.columns[2]: "Gasohol_Regular" ,
                        df2.columns[3]: "Gasohol_84"})  
    return df2

def getPricesPage1Page2(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    precio = pd.concat([df1, df2], axis=1)
    # transponer y establecer los nombres de las columnas
    precios = precio.T.reset_index()
    precios.columns = ['Producto', 'Precio']

    return precios

def extract2DataPage1() -> pd.DataFrame:
    with pdfplumber.open(pagina_pdf) as pdf:
        page = pdf.pages[0]  
        tables = page.extract_tables()

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        df3 = pd.DataFrame(tables[1])
        
    #Eliminar las columnas que no se necesita
    df3=df3.drop(df3.columns[[0, 1, 3, 7, 8, 9, 10]], axis=1)

    #Renombrar las columnas 
    df3= df3.rename(columns={df3.columns[0]: "GLP_E" ,
                            df3.columns[1]: "Gasolina_Premium" ,
                            df3.columns[2]: "Gasolina_Regular" ,
                            df3.columns[3]: "Gasolina_84"})
    return df3

def extract2DataPage2() -> pd.DataFrame:
    #Extraer la tabla de la hoja 2
    with pdfplumber.open(pagina_pdf) as pdf:
        page = pdf.pages[1]  
        tables = page.extract_tables()

    if not tables:
        print("No se encontraron tablas en la página.")
    else:
        df4 = pd.DataFrame(tables[1])

    #eliminar las columnas que no se necesita
    df4=df4.drop(df4.columns[[0, 2]], axis=1)

    #Lo mismo con las filas
    fila_mantener = [1, 2, 3]
    df4 = df4.loc[fila_mantener]
    df4 = df4.rename(index={1: 0, 2: 1, 3:2})

    #Renombrar las columnas 
    df4= df4.rename(columns={df4.columns[0]: "DIESEL_B5_UV" ,  
                            df4.columns[1]: "Gasohol_Premium" ,
                            df4.columns[2]: "Gasohol_Regular" ,
                            df4.columns[3]: "Gasohol_84"}) 
    return df4

def joinDf3Df4(df3: pd.DataFrame, df4: pd.DataFrame) -> pd.DataFrame:
    #Unificar ambas tablas de impuestos
    impuesto = pd.concat([df3, df4], axis=1)

    # Transponer 
    impuestos = impuesto.T
    impuestos.reset_index(level=0, inplace=True)
    impuestos.rename(columns={'index': 'Producto'}, inplace=True)

    impuestos= impuestos.rename(columns={impuestos.columns[1]: "Rodaje" ,
                                        impuestos.columns[2]: "ISC" ,
                                        impuestos.columns[3]: "IGV"})
    return impuestos

def joinPriceAndTaxes(precios: pd.DataFrame, impuestos: pd.DataFrame) -> pd.DataFrame:
    # Unificar ambas DataFrames por la columna "Producto"
    df_combinado = pd.merge(precios, impuestos, on="Producto", how="inner")

    df_combinado["Rodaje"] = df_combinado["Rodaje"].str.strip()
    df_combinado["ISC"] = df_combinado["ISC"].str.strip()
    df_combinado["IGV"] = df_combinado["IGV"].str.strip()

    df_combinado["Rodaje"] = df_combinado["Rodaje"].replace('', "0%")
    df_combinado["ISC"] = df_combinado["ISC"].replace('', 0)

    df_combinado["Rodaje"] = df_combinado["Rodaje"].str.rstrip('%').astype(float) / 100
    df_combinado["IGV"] = df_combinado["IGV"].str.rstrip('%').astype(float) / 100

    #convertir los valores de las columnas de string a float
    columnas_a_convertir = ["Precio", "IGV", "ISC", "Rodaje"]
    for columna in columnas_a_convertir:
        df_combinado[columna] = pd.to_numeric(df_combinado[columna], errors="coerce")
        
    # Calcular el precio sin impuestos
    df_combinado["Precio sin Impuestos"] = df_combinado["Precio"] / (1 + df_combinado["IGV"])
    df_combinado["Precio sin Impuestos"] = df_combinado["Precio sin Impuestos"] - df_combinado["ISC"]
    df_combinado["Precio sin Impuestos"] = df_combinado["Precio sin Impuestos"] / (1 + df_combinado["Rodaje"])

    return df_combinado

def deleteInnecesaryColumns(df_combinado) -> pd.DataFrame:
    df_combinado=df_combinado.drop(df_combinado.columns[[ 1, 2, 3, 4]], axis=1)

    df_combinado=df_combinado.transpose()
    df_combinado.columns = df_combinado.iloc[0]
    df_combinado=df_combinado[1:]

    return df_combinado
    
def extractDate() -> str:
    #Extraemos la fecha
    with open(pagina_pdf, "rb") as archivo_pdf:
        pdf_reader = PyPDF2.PdfReader(archivo_pdf)
        texto_extraido = ""
        for pagina in pdf_reader.pages:
            
            texto_pagina = pagina.extract_text()
            texto_extraido += texto_pagina
            
    lineas = texto_extraido.splitlines()
    segunda_linea= lineas[1]

    patron_fecha = r'\d{2}\.\d{2}\.\d{4}'
    resultado = re.search(patron_fecha, segunda_linea)
    if resultado:
        fecha = resultado.group()
    fecha= fecha.replace(".","/")   

    return fecha

def addDateToData(df_combinado: pd.DataFrame, fecha: str) -> pd.DataFrame:
    #Agregamos la columna fecha a la tabla
    df_combinado.insert(0,"Fecha",fecha)
    df_combinado["Fecha"] = pd.to_datetime(df_combinado["Fecha"], format="%d/%m/%Y")
    return df_combinado