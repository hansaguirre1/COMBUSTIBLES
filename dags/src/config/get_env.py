import os

from dotenv import load_dotenv
load_dotenv()

hub_selenium = os.getenv("HOST_SELENIUM")
url_petroperu = os.getenv("URL_PETROPERU")
url_bcrp = os.getenv("URL_BCRP")
url_eia = os.getenv("URL_EIA")
url_osinergmin = os.getenv("URL_OSINERGMIN")
url_signeblock = os.getenv("URL_SIGNEBLOCK")