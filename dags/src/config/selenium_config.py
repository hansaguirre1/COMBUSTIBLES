from selenium import webdriver
from src.config.get_env import hub_selenium

def configure_selenium() -> webdriver:

    __chrome_options = webdriver.ChromeOptions()
    __new_dir_path = '/home/seluser/Downloads'
    __prefs = {'download.default_directory' : __new_dir_path,  
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True}  # Esto evita que Chrome abra el PDF en el visor incorporado
    # chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
    __chrome_options.add_argument("--start-maximized")
    __chrome_options.add_argument("--no-sandbox")
    __chrome_options.add_argument('--disable-dev-shm-usage')
    __chrome_options.add_experimental_option('prefs', __prefs)

    __hub_url = hub_selenium
    driver = webdriver.Remote(command_executor=__hub_url, options=__chrome_options)
    return driver

    

