#login.py
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from selenium import webdriver    
from selenium.webdriver.common.action_chains import ActionChains
from tempfile import mkdtemp
import logging
from fake_useragent import UserAgent
from human_functions import human_type, human_click
from notify_error import notify_error
# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def login(user_dgr, password_dgr):
    try:
        service = Service("/opt/chromedriver")

        options = Options()
        options.binary_location = '/opt/chrome/chrome'
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280x1696")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--no-zygote")
        
        
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        options.add_argument(f"--data-path={mkdtemp()}")
        options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        
        ua = UserAgent()
        random_user_agent = ua.chrome
        options.add_argument(f'user-agent={random_user_agent}')
        
        prefs = {
        "download.default_directory": "/tmp",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
        }
        options.add_experimental_option("prefs", prefs)
        

        driver = webdriver.Chrome(service=service, options=options)
        
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": "/tmp"
            }
        )
        ########## LOGIN ###################
        driver.get("https://www.dgr.gub.uy/sr/principal.jsf")
        
        # Encuentra y llena el campo de usuario
        username_field = driver.find_element(By.ID, "j_username")
        human_type(username_field,user_dgr)
        
        
        # Encuentra y llena el campo de contraseña
        password_field = driver.find_element(By.ID, "j_password")
        human_type(password_field, password_dgr)
        
        login_button = driver.find_element(By.XPATH, "//input[@value='ingresar' and @type='submit']")
        human_click(driver,login_button)        
        # Defino el wait. 10sec
        wait = WebDriverWait(driver, 20)
        
        # Espera hasta que la página se cargue y el elemento esté disponible
        wait.until(EC.presence_of_element_located((By.ID, "j_id15:j_id30")))
        
        return driver
        
    except Exception as e:
        logger.error(f"ERROR: An error occurred during login: {e}")
        notify_error(f"ERROR: An error occurred during login: {e}")
        if 'driver' in locals():
            driver.quit()
        raise
    