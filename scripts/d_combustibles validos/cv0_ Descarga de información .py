from selenium import webdriver

import time 
import os
import pandas as pd
from tqdm import tqdm
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date, datetime, timedelta

from selenium.webdriver import ActionChains


import pandas as pd

from openpyxl import load_workbook


# declarar correctamente los paths en selenium
dir=os.getcwd()
pre_dir=os.path.dirname(dir)
pre_dir
ner_dir=r"\data\raw\combustibles validos"
new_dir_path=f"{pre_dir}{ner_dir}"
new_dir_path

# Configuración e inicio de navegación

chrome_options = webdriver.ChromeOptions()
service = Service(executable_path=r'../chromedriver.exe')
home_dir = './scraper_combustibles'
prefs = {'download.default_directory' : new_dir_path,  
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "plugins.always_open_pdf_externally": True}  # Esto evita que Chrome abra el PDF en el visor incorporado
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
chrome_options.add_experimental_option('prefs', prefs)



driver = webdriver.Chrome(service=service, options=chrome_options)
url_1 ='https://www.osinergmin.gob.pe/empresas/hidrocarburos/scop/documentos-scop'
driver.get(url_1)

#Encontrar elmentos de interés
driver.find_element(By.XPATH, '//*[@id="browser"]/li[1]/span').click()
container=driver.find_element(By.XPATH, '//*[@id="browser"]/li[1]/ul')
container_list=container.find_elements(By.TAG_NAME,'span')

len(container_list)

#Descargar elementos y rango de descarga

a=(datetime.now().year-2018-2)*13

for element in container_list[0:a]:
    for i in range(0,1):
        element.click()
        time.sleep(5)
