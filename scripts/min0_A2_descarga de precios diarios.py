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
from twocaptcha import TwoCaptcha


import pandas as pd

from openpyxl import load_workbook

dir=os.getcwd()
pre_dir=os.path.dirname(dir)
pre_dir
ner_dir=r"\data\raw\precios minoristas"
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

# Abrir navegador

driver = webdriver.Chrome(service=service, options=chrome_options)
url_6 = "https://tools.signeblock.com/default.aspx?m=0"
driver.get(url_6)

# Credenciales para entrar a la sesión:

user = 'hpalacios' 
key =  'Lima123%$'

user_input = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/input[1]')
user_input.send_keys(user)
key_input = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/div[2]/div/input[3]')
key_input.send_keys(key)


#resolver catpcha
api_key = 'da495832199f64fb89456be02236ac1e'

solver = TwoCaptcha(api_key)


sitekey='6LddE3UUAAAAAJo3jCCqG7iSiThGXicFCBgRvg0S'
response = solver.recaptcha(sitekey=sitekey, url=url_6)

code = response['code']
code

# Set the solved Captcha
recaptcha_response_element = driver.find_element(By.ID, 'g-recaptcha-response')
driver.execute_script(f'arguments[0].value = "{code}";', recaptcha_response_element)

submit_btn = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/input[2]')
submit_btn.click()
time.sleep(15)

# descargar data
data_download_button = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/form/div[5]/div[2]/div[1]/table/tbody/tr[3]/td[4]/input')
driver.execute_script("arguments[0].click();", data_download_button)


driver.close()