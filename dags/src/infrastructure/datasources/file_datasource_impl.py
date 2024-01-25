from pandas import DataFrame
from src.infrastructure.datasources.process.process_minorista_file import limpiezaMinorista, limpiezaMasivaMinorista
from src.domain.datasources.file_datasource import FileDatasource

import src.infrastructure.datasources.process.process_petroperu_file as processFile
import src.infrastructure.datasources.process.process_cv1_file as processCv1File
import src.infrastructure.datasources.process.process_cv2_file as processCv2File
import src.infrastructure.datasources.process.process_ubi0_file as processUbi0File
import src.infrastructure.datasources.process.process_m2_file as processM2File
import pandas as pd
from glob import glob
import numpy as np
import os
from datetime import datetime
from ast import literal_eval
import zipfile
import PyPDF2
import re
from itertools import product
from src.infrastructure.datasources.process.minfut0_nombres import *

pathMinorista = 'data/interim/minoristas'

class FileDatasourceImpl(FileDatasource):
    
    def unzipFile(self):
        directory = 'data'
        prefijo = 'InformeSemanal'
        pathDestiny = 'data/raw/referencia'
        files_zip = glob(os.path.join(directory, f'{prefijo}*.zip'))
        if len(files_zip) == 0:
            raise FileNotFoundError(f"No se encontraron archivos ZIP con el prefijo '{prefijo}' en el directorio '{directory}'")
        file_zip = files_zip[0]

        with zipfile.ZipFile(file_zip, 'r') as zip_ref:
            zip_ref.extractall(f'{pathDestiny}')
        os.remove(file_zip)
        
        # Buscar archivos PDF en el directorio de destino
        pdf_files = glob(os.path.join(f'{pathDestiny}', '*.pdf'))
        if len(files_zip) == 0:
            raise FileNotFoundError(f"No se encontró un archivo PDF con el prefijo '{prefijo}' en el directorio '{pathDestiny}'")
        
        for pdf_file in pdf_files:
            # Generar un nuevo nombre de archivo con prefijo y fecha actual
            nuevo_nombre = f'{prefijo}.pdf'

            # Renombrar el archivo PDF
            nuevo_path = os.path.join(f'{pathDestiny}', nuevo_nombre)
            os.rename(pdf_file, nuevo_path)
    
    def processFileOsinergminReferencia(self) -> DataFrame:
        # self.unzipFile()
        directoryDestiny = 'data/raw/referencia'
        
        ruta_pdf = f"{directoryDestiny}/InformeSemanal.pdf"

        with open(ruta_pdf, "rb") as archivo_pdf:
            pdf_reader = PyPDF2.PdfReader(archivo_pdf)
            texto_extraido = ""
            for pagina in pdf_reader.pages:
                
                texto_pagina = pagina.extract_text()
                texto_extraido += texto_pagina

        ### Cambiar texto de los saltos de línea ynombres de variables de interés
        Texto_total=texto_extraido.replace("\n", "abcdef")
        Texto_total=Texto_total.replace("\xa0", "")
        Texto_total=Texto_total.replace("abcdef %abcdef", "abcdef%abcdef")
        Texto_total=Texto_total.replace("GLP -E", "GLP")
        Texto_total=Texto_total.replace("GLP -G", "GLP-G")
        Texto_total=Texto_total.replace("Gasolina 84", "Gasolina84")
        Texto_total=Texto_total.replace("Gasolina Regular", "GasolinaRegular")
        Texto_total=Texto_total.replace("Gasohol Premium", "GasoholPremium")
        Texto_total=Texto_total.replace("Gasohol   Premium", "GasoholPremium")
        Texto_total=Texto_total.replace("Gasohol Regular", "GasoholRegular")
        Texto_total=Texto_total.replace("Gasohol 84", "Gasohol84")
        Texto_total=Texto_total.replace("Diesel B5 UV  S-50", "DieselB5UV")

        ### Extraer texto relevante 
        pattern = r"abcdef%abcdef(.*?)Residual"
        match = re.search(pattern, Texto_total)
        data=match.group(1).replace("abcdef", "\n")

        ### Agregar denominación de fila Fecha

        data="Fecha "+data
        #convertir texto en dataframe
        data_lines = [line.split() for line in data.split('\n')]

        df = pd.DataFrame(data_lines)
        
        df=df.drop(df.columns[[ 1, 2, 3, 5, 6]], axis=1)
        df= df.rename(columns={df.columns[0]: "Producto" ,  
                            df.columns[1]: "Precio"}) 

        df["Precio"] = df["Precio"].str.replace(",",".", regex=True)  
        fecha=df["Precio"][0]
        df["Fecha"] = fecha
        df=df[1:]
        df=df[:-1]
        return df
        
    def processFilesPetroperu(self) -> pd.DataFrame:
        data = processFile.getDataPetroperu()
        # df1 = processFile.extractDataPage1()
        # df2 = processFile.extractDataPage2()
        # precios = processFile.getPricesPage1Page2(df1, df2)
        # df3 = processFile.extract2DataPage1()
        # df4 = processFile.extract2DataPage2()
        # taxex = processFile.joinDf3Df4(df3, df4)
        # df_combinado = processFile.joinPriceAndTaxes(precios, taxex)
        # df_combinado = processFile.deleteInnecesaryColumns(df_combinado)
        # date = processFile.extractDate()
        # df_combinado = processFile.addDateToData(df_combinado,date)
        
        # return df_combinado
        return data
        
    def saveDataPetroperuToCSV(self, df_combinado: pd.DataFrame):
        data_existente = pd.read_csv("data/raw/petroperu/Petroperu_Lista.csv", sep=';')
 
        petroperu = pd.concat([data_existente, df_combinado], ignore_index=True)

        petroperu.to_csv("data/raw/petroperu/Petroperu_Lista.csv", index=False, sep=';')
    
    def saveDataPetroperuToExcel(self, df_combinado: pd.DataFrame):
        df_combinado = df_combinado.rename(columns={df_combinado.columns[1]: "GLP-E" ,
                                            df_combinado.columns[2]: "Gasolina Premium" ,
                                            df_combinado.columns[3]: "Gasolina Regular" ,
                                            df_combinado.columns[4]: "Gasolina 84" ,
                                            df_combinado.columns[5]: "Diesel B5 UV" ,
                                            df_combinado.columns[6]: "Gasohol Premium" ,
                                            df_combinado.columns[7]: "Gasohol Regular" ,
                                            df_combinado.columns[8]: "Gasohol 84"}) 
        datos_excel = pd.read_excel("data/raw/petroperu/petroperu.xlsx", sheet_name='Petroperu')

        nuevos_datos = pd.concat([datos_excel, df_combinado], ignore_index=True)
        nuevos_datos['Fecha']=nuevos_datos['Fecha'].dt.date

        with pd.ExcelWriter("data/raw/petroperu/petroperu.xlsx", mode='a', engine='openpyxl', if_sheet_exists ='replace') as writer:
            nuevos_datos.to_excel(writer, sheet_name='Petroperu', index=False)

    def saveDataMarcadoresToCsv(self, df_combinado: pd.DataFrame):
        data = pd.read_csv("data/raw/marcadores/marcadores.csv")
        data ["Fecha"] = pd.to_datetime(data["Fecha"], format="%Y-%m-%d")

        data_final = pd.concat([data, df_combinado], ignore_index=True)
        data_final["Fecha"] = pd.to_datetime(data_final["Fecha"], format="%Y-%m-%d")
        data_final["Fecha"] = data_final["Fecha"].dt.date

        data_final.to_csv("data/raw/marcadores/marcadores.csv", index=False)
        
    def processFileMinoristasDiario(self) -> pd.DataFrame:
        t = datetime.now()
        dateStr = t.strftime('%d-%m-%Y')
        print("Archivo diario")
        datax = pd.read_csv(ruta4 + BASE_DLC,encoding="utf-8",sep=";")
        
        pathExcel = f'data/raw/precios_minoristas/precios_combustibles_minorista_{dateStr}.xlsx'
        ex1=pd.read_excel(pathExcel,sheet_name="GLP_EVP_PEGL_LVGL_COM_PROD_IMP",skiprows=3)
        ex1=limpiezaMinorista(ex1)
        ex2=pd.read_excel(pathExcel,sheet_name="LIQ_EVP_DMAY_CCA_CCE",skiprows=3)
        ex2=limpiezaMinorista(ex2)
        data = pd.concat([ex1, ex2], ignore_index=True)
        df_ubi = pd.read_csv(ruta4 + DF_ubi, encoding='utf-8', sep=";")
        data = limpiezaMasivaMinorista(data, df_ubi)
        os.remove(pathExcel)
        print("excel eliminado.")
        
        data_concat_f = pd.concat([datax, data], ignore_index=True)
        data_concat_f.to_csv(ruta4 + BASE_DLC, index=False, encoding="utf-8", sep=";")


        return data_concat_f
    
    def saveDataRelapasaToCsv(self, df_combinado: DataFrame):
        # Relapasa
        relapasa = pd.read_excel("data/raw/relapasa/relapasa.xlsx")
        relapasaMerge = pd.concat([relapasa, df_combinado])
        relapasaMerge.to_excel("data/raw/relapasa/relapasa.xlsx", index=False)
        # Fin - Relapasa
    
    def exportFinalDta(self):
        # try:
        data_concat = pd.read_csv(f"{pathMinorista}/df_precios.csv")
        
        chunk_size = 30000
        
        iterator = pd.read_stata(f"{pathMinorista}/BASETOTAL_COMBUSTIBLES.dta", chunksize=chunk_size)
        datax = pd.DataFrame()
        
        for chunk in iterator:
            datax = pd.concat([datax, chunk], ignore_index=True)
        # datax = pd.read_stata(f"{pathMinorista}/BASETOTAL_COMBUSTIBLES.dta")
        data_concat_f = pd.concat([datax, data_concat], ignore_index=True)
        data_concat_f.to_stata(f"{pathMinorista}/BASETOTAL_COMBUSTIBLES.dta", write_index=False)
        # except Exception as e:
        #     print("Error al escribir el archivo:", e)
        
    def cv1_processReadAndCleanNewValues(self) -> DataFrame:
                
        #DECLARAR TODOS LOS PDFS
        directory_path = "data/raw/combustibles_validos"

        file_list = processCv1File.list_pdf_files(directory_path)
        file_list
        # Acumular información de pdfs

        Acumulado =[]
        for file in file_list:
            pagina_pdf = f"{directory_path}/{file}"   
            for pagina in range(0,1):
                for tabla in range(0,1):
                    print(pagina)
                    print(tabla)
                    try:
                        df=processCv1File.extraer_tabla(pagina_pdf)
                        df=processCv1File.make_list(df)
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

        #Arreglar fecha

        Demanda_por_region['Fecha']=Demanda_por_region['Fecha'].str.replace('-',' ')

        Demanda_por_region['Año']=Demanda_por_region['Fecha'].str.split().str[-1:].str[0].str.replace('pdf','').str.replace('.','')
        Demanda_por_region['Mes']=Demanda_por_region['Fecha'].str.split().str[-2:-1].str[0].str.replace('-' ,'')


        Demanda_por_region.drop('Fecha', axis=1, inplace=True)

        Demanda_por_region['Volumenes']=Demanda_por_region['Volumenes'].str.replace(',','')

        # guardar tabla

        Demanda_por_region.to_csv("data/interim/combustibles_validos/df_volumenes_departamento.csv", encoding="utf-8", index=False)
        print('guardo con exito')
    def cv2_processCombustiblesValidos(self) -> DataFrame:
        DF_val = "df_volumenes_departamento.csv"
        DF_val2 = "df_validos_dpt.csv"
        ruta8 = "data/interim/combustibles_validos/"
        ruta4 = "data/processed/combustibles_validos/"
        # Combustibles válidos
        print("Combustibles válidos")
        df = pd.read_csv(f'{ruta8}{DF_val}', encoding="utf-8")
        valor_a_verificar = 'LIMA'
        if valor_a_verificar in df['DEPARTAMENTO'].values:
            nueva_fila = df[df['DEPARTAMENTO'] == valor_a_verificar].copy()
            nueva_fila['DEPARTAMENTO'] = 'CALLAO'
            df = pd.concat([df, nueva_fila], ignore_index=True)
        df.columns = df.columns.str.upper()
        cols = df["COMBUSTIBLE"].value_counts()
        deps = df["DEPARTAMENTO"].unique()
        aos = df["AÑO"].unique()
        combinaciones = list(product(deps, aos))
        df_gnv = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
        df_gnv["COD_PROD"] = 16
        df_glpg = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
        df_glpg["COD_PROD"] = 30
        df_glpe = pd.DataFrame(combinaciones, columns=['DEPARTAMENTO', 'AÑO'])
        df_glpe["COD_PROD"] = 29

        #df['VOLUMENES'] = pd.to_numeric(df['VOLUMENES'].str.replace(',', ''), errors='coerce')

        df['NM'] = df['MES'].map({'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 'Setiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12})
        df.loc[df['VOLUMENES'] == 0, 'VOLUMENES'] = pd.NA


        df_gasohol_premium = df[(df['COMBUSTIBLE'] == 'GASOHOL PREMIUM') | (df['COMBUSTIBLE'] == 'Gasohol 95 Plus')]
        df_gasohol_premium = processCv2File.combse(df_gasohol_premium,59)
        df_gasolina_regular = df[(df['COMBUSTIBLE'] == 'GASOLINA REGULAR') | (df['COMBUSTIBLE'] == 'Gasolina 90')]
        df_gasolina_regular = processCv2File.combse(df_gasolina_regular,62)
        df_gasolina_premium = df[(df['COMBUSTIBLE'] == 'GASOLINA PREMIUM') | (df['COMBUSTIBLE'] == 'Gasolina 95')]
        df_gasolina_premium = processCv2File.combse(df_gasolina_premium,61)
        df_gasohol_regular = df[(df['COMBUSTIBLE'] == 'GASOHOL REGULAR') | (df['COMBUSTIBLE'] == 'Gasohol 90 Plus')]
        df_gasohol_regular = processCv2File.combse(df_gasohol_regular,60)
        df_diesel = df[(df['COMBUSTIBLE'] == 'DIESEL B5 UV') | (df['COMBUSTIBLE'] == 'DB5 S-50')]
        df_diesel = processCv2File.combse(df_diesel,15)
        df_diesel2 = df[(df['COMBUSTIBLE'] == 'DIESEL B5 S-50 UV') | (df['COMBUSTIBLE'] == 'Diesel B5')]
        df_diesel2 = processCv2File.combse(df_diesel2,9)

        combs = pd.concat([df_diesel,df_diesel2,df_gnv,df_glpe,df_glpg,df_gasohol_premium, df_gasolina_regular,df_gasolina_premium, df_gasohol_regular], ignore_index=True)
        combs["ok"] = 1
        combs.sort_values(by=["DEPARTAMENTO","AÑO"],inplace=True)
        combs["ID"]=combs["DEPARTAMENTO"] + "-" + combs["COD_PROD"].astype(str)
        fecha_minima = combs['AÑO'].min()
        fecha_maxima = combs['AÑO'].max()
        rango_fechas_completo = list(range(fecha_minima,fecha_maxima))
        combinaciones = pd.DataFrame([(id, fecha) for id in combs['ID'].unique() for fecha in rango_fechas_completo], columns=['ID', 'AÑO'])
        combs = pd.merge(combinaciones, combs, on=['ID', 'AÑO'], how='outer')
        combs.to_csv(ruta4 + DF_val2,index=False)

    def ubi0_processUbigeo(self) -> DataFrame:
        print("ubigeos")
        ubi = processUbi0File.ubigeos()
        print("ubi")
        print(ubi)
        print("-ubi-")
        
        return ubi

    def m1_processCleanAndJoin(self):
        new_dir_path = 'data/raw/precios_mayoristas'


        # Guardar todo en lista

        Precios_Mayoristas_list=[]
        for file in os.listdir(new_dir_path):
            if file.endswith('.xlsx'):
                df=pd.read_excel(f'{new_dir_path}/{file}')
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
            if row['PRECIO DE VENTA (SOLES)'] > 5 and row['COD_PROD'] == 30:
                return (row['PRECIO DE VENTA (SOLES)'])/ 3.78533
            else:
                return row['PRECIO DE VENTA (SOLES)']


        # Apply the function to the DataFrame
        Precios_Mayoristas['PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas.apply(replace_value_litros, axis=1)


        Precios_Mayoristas.loc[Precios_Mayoristas['COD_PROD'] == 30, 'PRECIO DE VENTA (SOLES)'] = Precios_Mayoristas.loc[Precios_Mayoristas['COD_PROD'] == 30, 'PRECIO DE VENTA (SOLES)'] /  0.5324

        # Redondear precios a dos decimales

        Precios_Mayoristas['PRECIO DE VENTA (SOLES)']=Precios_Mayoristas['PRECIO DE VENTA (SOLES)'].round(2)

        # Guardar data de precios mayoristas sin imputar

        Precios_Mayoristas.to_csv(f"data/interim/precios_mayoristas/mayoristas_pre_imp.csv", sep=";", encoding="utf-8")

    def m2_processRucAndDays(self):
        # Costo otros
        df = pd.read_csv("data/interim/precios_mayoristas/mayoristas_pre_imp.csv",encoding='utf-8',sep=";")
        df.rename(columns={"PRECIO DE VENTA (SOLES)": "PRECIOVENTA"},inplace=True)
        df.rename(columns={"PROVINCIA_VENDEDOR": "PROVINCIA"},inplace=True)
        df['fecha_stata'] = pd.to_datetime(df['fecha_stata'], infer_datetime_format=True, errors='coerce')
        df["RUC-prov"] = df["RUC"].astype(str)+"-"+df["PROVINCIA"]
        df.COD_PROD.value_counts()
        cod_prods = [ 46, 45, 37, 36, 47, 48, 28, 19]
        
        # test
        codigos_unicos = df['COD_PROD'].unique()
        print(codigos_unicos)
        # fin - test
        
        for k in cod_prods:
            print(k)
            df_2 = df.copy()
            df_2 = df_2[df_2['COD_PROD'].astype(int) == k]
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
            #process2("PRECIOVENTA_")
            #process2("dias_faltantes")
            #df_2['PRECIOVENTAx'] = df_2['PRECIOVENTA_']
            q = 1
            for i in range(1, len(df_2)):
                if q < 100:
                    if (df_2.at[i, 'RUC-prov'] == df_2.at[i-1, 'RUC-prov']) and pd.isnull(df_2.at[i, 'PRECIOVENTA']):
                        df_2.at[i, 'PRECIOVENTA'] = df_2.at[i-1, 'PRECIOVENTA']
                    q += 1
            
                if i < len(df_2) - 1 and not pd.isnull(df_2.at[i+1, 'PRECIOVENTA']):
                    q = 1
            #df_2.rename(columns={'CODIGOOSINERG': 'COD'}, inplace=True)
            df_2.drop(columns=['PRECIOVENTA_','dias_faltantes'], inplace=True)
            df_2["COD_PROD"] = k
            df_2.to_csv(f"data/interim/precios_mayoristas/base_final_{k}.csv",  encoding="utf-8",  index=False)


        # guardar todos los productos en una lista
        dfs = {}
        p = 1
        for k in cod_prods:
            print(k)
            dfk = pd.read_csv(f"data/interim/precios_mayoristas/base_final_{k}.csv", encoding="utf-8")
            dfs[f"base_{p}"] = dfk
            os.remove(f"data/interim/precios_mayoristas/base_final_{k}.csv")
            p += 1

        # dfs2 = [globals()[f"base_{i}"] for i in range(1, len(dfs) + 1) if f"base_{i}" in globals()]
        # dfs2 = [dfs[f"base_{i}"] for i in range(1, len(dfs) + 1) if f"base_{i}" in dfs]
        dfs2 = [dfs[key] for key in sorted(dfs.keys()) if key.startswith("base_")]

        # del base_1,base_2,base_3,base_4,base_5,base_6,base_7,base_8


        #unir todas las bases completas por día
        df_concatenado = pd.concat(dfs2, ignore_index=True)
        del dfs2

        # guardar información previa a la imputación
        df_concatenado.to_csv("data/interim/precios_mayoristas/precios_mayoristas_imp.csv", encoding="utf-8", index=False,sep=";")

        del df_concatenado
        

