from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys


from bson import ObjectId
import json 

import logging
from datetime import datetime


from human_functions import human_click, human_type, human_select
from notify_error import notify_error

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
    
    
def fill_form_Ampliacion(ampliacion_id, driver, client):
    wait = WebDriverWait(driver, 5)

    """
    Rellena el formulario de Inmuebles en DGR para cada observación del bucket_id.
    Devuelve un dict con:
      - "status": "success", "critical_error" o "submission_error"
      - "errors": lista de mensajes en caso de fallo
    """
    critical_errors = []
    submission_errors = []
    logger.info(f"Starting Inmuebles for Bucket ID: {str(ampliacion_id)}")
    
    try:
        logger.info("Connecting to MongoDB...")
        db = client['production']
        ampliacion_db = db['ampliacion']
        bucket_db = db['bucket']
        logger.info("Connected to MongoDB.")
    except Exception as e:
        msg = f"Critical: Error connecting to MongoDB for ampliacion_id {str(ampliacion_id)}: {e}"
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    try:
        logger.info(f"Retrieving observations for ampliacion_id: {str(ampliacion_id)}...")
        # Defino el filtro
        observaciones = ampliacion_db.find_one({"_id": ObjectId(ampliacion_id)})
        logger.info(f"Observacion {observaciones}")
        ampliacion_fecha = observaciones.get("emisionAt","")
        ampliacion_fecha = ampliacion_fecha.strftime("%d/%m/%Y")  # "19/06/2025"
        logger.info(f"Ampliacion Fecha {ampliacion_fecha}")
        
        if not ampliacion_fecha:
            msg = (f"No se obutov ampliacion fecha para ampliacion {str(ampliacion_id)}")
            critical_errors.append(msg)
            return {"status": "critical_error", "errors": critical_errors}
        
        logger.info("Obteniendo bucket_id para dicha ampliacion")
        
        bucket_id = observaciones.get("id_bucket","")
        bucket_id = ObjectId(bucket_id)
        
        filtro = {"_id": bucket_id}
        observaciones = bucket_db.find_one(filtro)
        logger.info(f"Encontrada observacion en collection bucket: {observaciones}")
        
        numero_solicitud = observaciones.get("dgr_id", "")
        logger.info(f"Numero Solicitud: {numero_solicitud}")
        if not numero_solicitud:
            msg = f"No Numero found for bucket_id : {str(bucket_id)}"
            critical_errors.append(msg)
            return {"status": "critical_error", "errors": critical_errors}
            
    except Exception as e:
        msg = f"Critical: Error retrieving observations from MongoDB for bucket_id {str(bucket_id)}. Error: {e}"
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}
    
    try:
        driver.get("https://www.dgr.gub.uy/etimbreapp/servlet/hsolicitudampliacion")
        logger.info("Navegado al formulario Ampiacion.")
    except Exception as e:
        msg = f"Critical: al cargar página Ampliacion. Error {e} "
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}
    
    
    try:
        
        logger.info(f"  Ingresando numero de solicitud: {numero_solicitud}")
        fld = driver.find_element(By.ID, "_NROSOLIC")
        human_type(fld, numero_solicitud)
        logger.info(f"Se llenó Numero de Solicitud: {numero_solicitud}")
        
           
    except Exception as e:
        msg = f"ERROR Ingrsando Numero de Solcitud {numero_solicitud} para ampliacion_id {str(ampliacion_id)}. Error: {e}"
        logger.error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}
    try:
        wait.until(EC.presence_of_element_located((By.NAME, "BUTTON1")))
        button = driver.find_element(By.NAME, "BUTTON1")
        human_click(driver, button)
        logger.info("Click correcto en 'Recuperar'.")
    except Exception as e:
        logger.error(f"ERROR: Error al hacer clic en 'Recuperar': {e}")
        notify_error(f"ERROR: Error al hacer clic en 'Recuperar' para ampliacion_id {str(ampliacion_id)}: {e}")
        return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al hacer clic en 'Recuperar': {e}"}) }
    
    try:
        logger.info(f"Clicking on ampliacion fecha")
        field = driver.find_element(By.ID, "_FCHEM")
        human_click(driver, field)
        field.send_keys(Keys.END)
        for _ in range(12):
            field.send_keys(Keys.BACKSPACE)
            
        human_type(field, ampliacion_fecha)
        logger.info(f"Successfully entered ampliacion: {ampliacion_fecha}")
    except Exception as e:
        msg = f"Error ingresando ampliacion fecha. Error: {e}"
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}
        
    
    
    logger.info(f"fill_form_Ampliacion completed successfully for bucket_id {str(ampliacion_id)}")
    return {"status": "success", "errors": []}
    




