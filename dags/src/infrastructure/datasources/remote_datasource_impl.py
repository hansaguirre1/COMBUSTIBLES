from pandas import DataFrame
import os, zipfile
import time 
from selenium.webdriver.common.by import By
import shutil
from src.domain.datasources.remote_datasource import RemoteDatasource
from src.config.selenium_config import configure_selenium
from datetime import datetime, timedelta
import pandas as pd
import requests
from twocaptcha import TwoCaptcha
new_dir_path = '/home/seluser/Downloads'

class RemoteDatasourceImpl(RemoteDatasource):
    def getDataPetroperu(self, url: str):
        try:            
            driver = configure_selenium()
            driver.get(url)
            dowload_button = driver.find_element(By.XPATH, '/html/body/div[2]/div/main/div[2]/div[1]/ul/li[1]/div[1]/div[2]/a')
            driver.execute_script("arguments[0].click();", dowload_button)
            time.sleep(15)

            # Renombre del archivo descargado:

            archivos = os.listdir(f'{new_dir_path}')
            ruta_original = f'{new_dir_path}/{archivos[0]}'
            ruta_nueva = 'data/raw/precios_mayoristas_petroperu/petroperu_info.pdf'
            shutil.copyfile(ruta_original, ruta_nueva)
            os.remove(ruta_original)
            print('archivo copiado y eliminado con éxito!')
            time.sleep(5)
        finally:
            driver.quit()    
    
    def getDataBCRP(self, url: str) -> DataFrame:
        try:
            driver = configure_selenium()
            driver.get(url)

            fecha_actual = datetime.now()
            fecha_inicio = fecha_actual - timedelta(days=10)
            fecha_inicio = fecha_inicio.strftime("%d/%m/%Y")

            fecha_inicio_input = driver.find_element(By.XPATH, '/html/body/div[3]/table/tbody/tr/td[2]/form/div[1]/div[1]/div[1]/div[1]/div[2]/input')
            fecha_inicio_input.clear()
            time.sleep(1)
            fecha_inicio_input.send_keys(fecha_inicio)

            fecha_final = fecha_actual - timedelta(days=4)
            fecha_final = fecha_final.strftime("%d/%m/%Y")

            fecha_final_input = driver.find_element(By.XPATH, '/html/body/div[3]/table/tbody/tr/td[2]/form/div[1]/div[1]/div[1]/div[2]/div[2]/input')
            fecha_final_input.clear()
            time.sleep(1)
            fecha_final_input.send_keys(fecha_final)

            filtro_tabla = driver.find_element(By.XPATH, '/html/body/div[3]/table/tbody/tr/td[2]/form/div[1]/div[1]/div[2]/p/input[1]')
            driver.execute_script("arguments[0].click();", filtro_tabla)

            tabla = driver.find_element(By.XPATH, '/html/body/div[3]/table/tbody/tr/td[2]/form/div[3]/table')

            filas = tabla.find_elements(By.TAG_NAME,"tr")

            # Inicializa una lista para almacenar los datos de la tabla
            datos_tabla = []

            filas = filas[1:]

            # Itera a través de las filas de la tabla

            for fila in filas:
                # Localiza las celdas en cada fila
                celdas = fila.find_elements(By.TAG_NAME,"td")  
                # Inicializa una lista para almacenar los datos de una fila
                fila_datos = []
                # Itera a través de las celdas y agrega sus datos a la lista de fila_datos
                for celda in celdas:
                    fila_datos.append(celda.text)
                # Agrega la lista de datos de fila a la lista principal de datos_tabla
                datos_tabla.append(fila_datos)
                
            data_tc_monthly = pd.DataFrame(datos_tabla, columns=['Fecha', 'TC'])

            # Diccionario de mapeo de nombres de meses a abreviaturas
            meses = {
                'Ene': 'Jan',
                'Feb': 'Feb',
                'Mar': 'Mar',
                'Abr': 'Apr',
                'May': 'May',
                'Jun': 'Jun',
                'Jul': 'Jul',
                'Ago': 'Aug',
                'Set': 'Sep',
                'Oct': 'Oct',
                'Nov': 'Nov',
                'Dic': 'Dec'
            }

            # Reemplazar los nombres de los meses en la columna 'fecha'
            data_tc_monthly['Fecha'] = data_tc_monthly['Fecha'].replace(meses, regex=True)
            data_tc_monthly['Fecha'] = pd.to_datetime(data_tc_monthly['Fecha'], format='%d%b%y')
            return data_tc_monthly

        finally:
            driver.quit()    
    
    def getDataEIA1(self, url: str) -> DataFrame:
        fecha_inicio_2 = datetime.now() - timedelta(days=10)
        fecha_inicio_2 = fecha_inicio_2.strftime("%Y-%m-%d")
        api_key_EIA = "bxDriVfB3EiL8y2byKs4wPkau6actWIRWgYbcFlv"
        url_3 = f"{url}?frequency=daily&data[0]=value&facets[series][]=RWTC&start={fecha_inicio_2}&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key={api_key_EIA}"
        response_wti = requests.get(url_3, verify=False)
        data_wti_fob = response_wti.json()

        data_wti_fob = pd.DataFrame(data_wti_fob['response']['data'])
        data_wti_fob = data_wti_fob[['period', 'product-name', 'value' , 'units']]
        return data_wti_fob

    def getDataEIA2(self, url: str) -> DataFrame:
        # Precio de gas propano Mont Belvieu (FOB)
        fecha_inicio_2 = datetime.now() - timedelta(days=10)
        fecha_inicio_2 = fecha_inicio_2.strftime("%Y-%m-%d")
        api_key_EIA = "bxDriVfB3EiL8y2byKs4wPkau6actWIRWgYbcFlv"
        url_4 = f"{url}?frequency=daily&data[0]=value&facets[series][]=EER_EPLLPA_PF4_Y44MB_DPG&start={fecha_inicio_2}&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key={api_key_EIA}"
        response_belvieu = requests.get(url_4, verify=False)
        data_belvieu_fob = response_belvieu.json()

        data_belvieu_fob = pd.DataFrame(data_belvieu_fob['response']['data'])
        data_belvieu_fob = data_belvieu_fob[['period', 'product-name', 'value' , 'units']]
        return data_belvieu_fob
    
    def joinBcrpAndEia(self, dfBcrp: DataFrame, dfEia1: DataFrame, dfEia2: DataFrame) -> DataFrame:
        #UNIFICAR DATOS PARA LA TABLA DE MARCADORES
        dfEia1 = dfEia1.drop(dfEia1.columns[[1, 3]], axis=1)
        dfEia1 = dfEia1.rename(columns={dfEia1.columns[0]: "Fecha" ,
                                                    dfEia1.columns[1]: "WTI"})
        dfEia2 = dfEia2.drop(dfEia2.columns[[1, 3]], axis=1)
        dfEia2 = dfEia2.rename(columns={dfEia2.columns[0]: "Fecha" ,
                                                            dfEia2.columns[1]: "MontBelvieu"})

        Marcadores = pd.merge(dfEia1, dfEia2, on="Fecha", how="outer")
        Marcadores["Fecha"] = pd.to_datetime(Marcadores["Fecha"], format="%Y-%m-%d")
        Marcadores["Fecha"] = Marcadores["Fecha"].dt.date

        dfBcrp["Fecha"] = pd.to_datetime(dfBcrp["Fecha"], format="%Y-%m-%d")
        dfBcrp["Fecha"] = dfBcrp["Fecha"].dt.date

        Marcadores = pd.merge(Marcadores, dfBcrp, on="Fecha", how="outer") 
        Marcadores = Marcadores.sort_values("Fecha")
        return Marcadores
        
        
    def getDataOsinergmin(self, url: str):
        try:
            driver = configure_selenium()
            fecha_actual = datetime.now()
            año = str(fecha_actual.year)

            urlOsinergmin = f'{url}?Codigo={año}'
            driver.get(urlOsinergmin)

            tipo_doc = driver.find_element(By.XPATH, '/html/body/form/div[12]/div/div[2]/div/div[2]/div[2]/div[2]/div[8]/div[1]/div/div/div/div/div/div/div[1]/div[1]/table/tbody/tr[3]/td[2]/select/option[2]')
            tipo_doc.click() 
            time.sleep(5)
            
            año_input = driver.find_element(By.XPATH, '/html/body/form/div[12]/div/div[2]/div/div[2]/div[2]/div[2]/div[8]/div[1]/div/div/div/div/div/div/div[1]/div[1]/table/tbody/tr[3]/td[1]/select/option[1]') 
            año_input.click() 
            time.sleep(5)
            
            search_button = driver.find_element(By.XPATH, '/html/body/form/div[12]/div/div[2]/div/div[2]/div[2]/div[2]/div[8]/div[1]/div/div/div/div/div/div/div[1]/div[1]/table/tbody/tr[3]/td[2]/input')
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)

            descarga_informe = driver.find_element(By.XPATH, '/html/body/form/div[12]/div/div[2]/div/div[2]/div[2]/div[2]/div[8]/div[1]/div/div/div/div/div/div/div[1]/div[2]/div/table/tbody/tr[4]/td/div/li/a')
            driver.execute_script("arguments[0].click();", descarga_informe)
            time.sleep(10)
            
        finally:
            driver.quit()    
    
    def min0_A2_descarga(self, url: str):
        try:
            fecha_actual = datetime.now()
            driver = configure_selenium()                        
            urlSigneBlock = f"{url}?m=0"
            driver.get(urlSigneBlock)
            
                # Credenciales para entrar a la sesión:

            user = 'hpalacios' 
            key =  'Lima123%$'

            user_input = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/input[1]')
            user_input.send_keys(user)
            key_input = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/div[2]/div/input[3]')
            key_input.send_keys(key)

            api_key = 'da495832199f64fb89456be02236ac1e'

            solver = TwoCaptcha(api_key)
            sitekey='6LddE3UUAAAAAJo3jCCqG7iSiThGXicFCBgRvg0S'
            response = solver.recaptcha(sitekey=sitekey, url=urlSigneBlock)

            code = response['code']
            print ( code )

           # Set the solved Captcha
            recaptcha_response_element = driver.find_element(By.ID, 'g-recaptcha-response')
            driver.execute_script(f'arguments[0].value = "{code}";', recaptcha_response_element)

            submit_btn = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/form/div[3]/input[2]')
            submit_btn.click()
            time.sleep(15)

            data_download_button = driver.find_element(By.XPATH, '/html/body/div[3]/div/div/form/div[5]/div[2]/div[1]/table/tbody/tr[3]/td[4]/input')
            driver.execute_script("arguments[0].click();", data_download_button)

            time.sleep(15)
            archivos = os.listdir(f'{new_dir_path}')
            for archivo in archivos:
                if archivo.endswith(".xlsx"):
                    ruta_original = os.path.join(f'{new_dir_path}', archivo)
                    ruta_nueva = os.path.join('data/raw/precios_minoristas', f'precios_combustibles_minorista_{fecha_actual.strftime("%d-%m-%Y")}.xlsx')
                    shutil.copy2(ruta_original, ruta_nueva)
                    os.remove(ruta_original)
                    
            time.sleep(3)
            
        finally:
            driver.quit()
    
    def getDataCombustiblesValidos(self, url: str):
        try:
            driver = configure_selenium(path='/raw/combustibles_validos') 
            url_1 ='https://www.osinergmin.gob.pe/empresas/hidrocarburos/scop/documentos-scop'
            
            # # declarar correctamente los paths en selenium
            # dir=os.getcwd()
            # pre_dir=os.path.dirname(dir)
            # pre_dir
            # ner_dir=r"\data\raw\combustibles validos"
            # new_dir_path=f"{pre_dir}{ner_dir}"
            # new_dir_path


            driver.get(url_1)
            time.sleep(10)

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
            time.sleep(5)
        finally:
            driver.quit()
    
    def m0_descarga_mayorista(self):     
        try:
            
            new_dir_path_mayorista = 'data/raw/precios_mayoristas'

            #Eliminar excel de la anterior corrida

            for file in os.listdir(new_dir_path_mayorista):
                if file.endswith('.xlsx'):
                    os.remove(f'{new_dir_path_mayorista}/{file}')

            #Nuevos driver

            

            driver = configure_selenium(path='/raw/precios_mayoristas') 
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


            for file in os.listdir(new_dir_path_mayorista):   # get the list of files
                file_path = os.path.join(new_dir_path_mayorista, file)
                print(f'file path {file} - {file_path}')
                try:
                    if zipfile.is_zipfile(file_path): # if it is a zipfile, extract it
                        with zipfile.ZipFile(file_path) as item: # treat the file as a zip
                            item.extractall(new_dir_path_mayorista)
                            time.sleep(10)
                except Exception as e:
                    print(f"Error al descomprimir '{file_path}': {e}")

            time.sleep(10)

            # Eliminar todo lo que no es excel
            for file in os.listdir(new_dir_path_mayorista):
                if file.endswith('.zip') or file.endswith('.xml'):
                    file_path = os.path.join(new_dir_path_mayorista, file)
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
                
        except Exception as e:
            # Captura la excepción y la imprime
            print(f"Se produjo una excepción: {e}")

        finally:
            driver.quit()