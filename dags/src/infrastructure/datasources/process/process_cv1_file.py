import pdfplumber
import pandas as pd
import PyPDF2
import re
import datetime
from openpyxl import load_workbook
import numpy as np
import os

def extraer_tabla(file):

    with pdfplumber.open(file) as pdf:
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

    precio['Fecha']=file

    return precio

# Definir funcion para hacer lista la tabla

def make_list(precio):
    precio.replace("", np.nan, inplace = True)
    lista = precio.melt(id_vars=["Fecha", "DEPARTAMENTO"], var_name="Combustible", value_name="Volumenes")
    lista = lista.sort_values(by= ["Fecha", "DEPARTAMENTO"])
    lista = lista.dropna(subset=["Volumenes"])
    return lista

#  Definir funcion para extraer los nombres de los files pdfs

def list_pdf_files(directory):
    pdf_files = [file for file in os.listdir(directory) if file.endswith(".pdf")]
    return pdf_files
