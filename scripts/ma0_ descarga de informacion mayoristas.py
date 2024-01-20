#importar libreria

from selenium import webdriver
import re
import time 
import os
import random
import pandas as pd
import numpy as np
from tqdm import tqdm
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date, datetime, timedelta
import json
import pytz 
from selenium.webdriver import ActionChains
import statistics
import requests
import os, zipfile


# Configuración e inicio de navegación

dir=os.getcwd()
pre_dir=os.path.dirname(dir)
pre_dir
ner_dir=r"\data\raw\precios mayoristas"
new_dir_path=f"{pre_dir}{ner_dir}"
new_dir_path

chrome_options = webdriver.ChromeOptions()
service = Service(executable_path=r'../chromedriver.exe')
home_dir = './scraper_combustibles'
prefs = {'download.default_directory' : new_dir_path,  
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "plugins.always_open_pdf_externally": True}  # Esto evita que Chrome abra el PDF en el visor incorporado
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
chrome_options.add_experimental_option('prefs', prefs)

# Abrir navegador 

driver = webdriver.Chrome(service=service, options=chrome_options)
url_1 ='https://www.osinergmin.gob.pe/empresas/hidrocarburos/scop/documentos-scop'
driver.get(url_1)

# Encontrar elementos de interés
driver.find_element(By.XPATH, '//*[@id="browser"]/li[13]/div').click()
container=driver.find_element(By.XPATH, '//*[@id="browser"]/li[13]/ul')
container_list=container.find_elements(By.TAG_NAME,'li')


# Descarga de información

for element in container_list:
    for i in range(0,1):
        element.click()
        time.sleep(5)

# Unzip files

extension = ".zip"
os.chdir(new_dir_path)

for file in os.listdir(new_dir_path):   # get the list of files
    if zipfile.is_zipfile(file): # if it is a zipfile, extract it
        with zipfile.ZipFile(file) as item: # treat the file as a zip
           item.extractall()


# Eliminar todo lo que no es excel
try:
    for file in os.listdir(new_dir_path):
        if file.endswith('.zip') or file.endswith('.xml'):
            file_path = os.path.join(new_dir_path, file)
            os.remove(file_path)
            print(f"Deleted: {file_path}")
except:
    pass

