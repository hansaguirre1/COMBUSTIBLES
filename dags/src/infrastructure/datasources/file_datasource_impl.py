from pandas import DataFrame
from src.infrastructure.datasources.process.process_minorista_file import limpiezaMinorista, limpiezaMasivaMinorista
from src.domain.datasources.file_datasource import FileDatasource

import src.infrastructure.datasources.process.process_petroperu_file as processFile
import pandas as pd
from glob import glob
import numpy as np
import os
from datetime import datetime
from ast import literal_eval
import zipfile
import PyPDF2
import re

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
        pathExcel = f'data/raw/precios_minoristas/precios_combustibles_minorista_{dateStr}.xlsx'
        ex1=pd.read_excel(pathExcel,sheet_name="GLP_EVP_PEGL_LVGL_COM_PROD_IMP",skiprows=3)
        ex1=limpiezaMinorista(ex1)
        ex2=pd.read_excel(pathExcel,sheet_name="LIQ_EVP_DMAY_CCA_CCE",skiprows=3)
        ex2=limpiezaMinorista(ex2)
        data_concat = pd.concat([ex1, ex2], ignore_index=True)
        print("excel eliminado.")
        data_concat=limpiezaMasivaMinorista(data_concat)
        return data_concat
    
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
        