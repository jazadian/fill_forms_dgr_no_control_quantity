import os
import boto3
from bson import ObjectId
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

import json
import logging

from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from notify_error import notify_error
from human_functions import human_click
from botocore.config import Config


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

sqs_client = boto3.client('sqs')
sqs_config = Config(retries={'max_attempts': 3, 'mode': 'standard'})
sqs_client = boto3.client('sqs', config=sqs_config)


SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

def submit_form_and_generate_talon(driver, bucket_id, bucket_type):
    client = MongoClient(os.environ.get('MONGODB_URI'))
    db = client['production']
    collection = db['bucket']
    if bucket_type != "Ampliación":
        logger.info(f"Starting submiting for {bucket_type}")
        try:
            # 1) Asegurar bucket_id como ObjectId
            if not isinstance(bucket_id, ObjectId):
                bucket_id = ObjectId(bucket_id)
            wait = WebDriverWait(driver, 10)
        except Exception as e:
            logger.error(f"ERROR: Error definiendo ObjectId: {e}")
            notify_error(f"ERROR: Error definiendo ObjectId: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': "Error definiendo ObjectId"}) }

        # 2) Seleccionar "MONTEVIDEO"
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "CTLSEDESOL")))
            select_element = driver.find_element(By.NAME, "CTLSEDESOL")
            Select(select_element).select_by_value('X')
            logger.info("Opción 'MONTEVIDEO' seleccionada.")
        except Exception as e:
            logger.error(f"ERROR: Error seleccionando Opción de Montevideo: {e}")
            notify_error(f"ERROR: Error seleccionando Opción de Montevideo para bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error en seleccionar Opción de Montevideo: {e}"}) }

        # 3) Hacer clic en "Enviar Solicitud"
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "BUTTON1")))
            button = driver.find_element(By.NAME, "BUTTON1")
            human_click(driver, button)
            logger.info("Click correcto en 'Enviar Solicitud'.")
        except Exception as e:
            logger.error(f"ERROR: Error al hacer clic en 'Enviar Solicitud': {e}")
            notify_error(f"ERROR: Error al hacer clic en 'Enviar Solicitud' para bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al hacer clic en 'Enviar Solicitud': {e}"}) }

        # 4) Esperar y obtener número de consulta (j_id78 → número)
        try:
            logger.info("Looking for element with class 'colHeader2' (j_id78)...")
            element = WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.ID, "j_id78")))
            logger.info("Element 'j_id78' encontrado.")
        except TimeoutException:
            logger.warning("j_id78 no apareció en 40s; espero otros 5s manualmente...")
            time.sleep(5)

        try:
            xpath = (
                "//td[@class='colHeader1' and normalize-space(.//span)='Número']"
                "/following-sibling::td[contains(@class,'colHeader2')][1]"
            )
            element = driver.find_element(By.XPATH, xpath)
            numero = element.text
            logger.info(f"Número de consulta: {numero}")
            
            # 2) Información al día
            xpath_info = (
            "//span[normalize-space(text())='Información al día']"
            "/ancestor::td/following-sibling::td[1]"
            )
            info_txt = driver.find_element(By.XPATH, xpath_info).text.strip()
            informacionAlDiaDate = datetime.strptime(info_txt, "%d/%m/%Y %H:%M")
            logger.info(f"Información al día: {informacionAlDiaDate}")
        except Exception as e:
            logger.error(f"ERROR: Numero no encontrando. Error: {e}")
            notify_error(f"ERROR: Numero no encontrando para bucket_id {str(bucket_id)}. Error: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al capturar el número: {e}"}) }
        try:                    
            collection.update_one(
                {"_id": bucket_id},
                {"$set": {"dgr_id": numero,"informacionAlDia": informacionAlDiaDate}},
                upsert=True
            )
            logger.info(f"Number {numero} saved to MongoDB en el documento {str(bucket_id)}.")
        except Exception as e:
            logger.error(f"ERROR: mongo")
            notify_error(f"ERROR: mongo")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al guardar en MongoDB: {e}"}) }

        # 5) LIMPIAR cualquier PDF/CRDOWNLOAD viejo en /tmp
        download_dir = "/tmp"
        try:
            for fname in os.listdir(download_dir):
                if fname.lower().endswith(".pdf") or fname.lower().endswith(".crdownload"):
                    full_path = os.path.join(download_dir, fname)
                    os.remove(full_path)
                    logger.info(f"[cleanup] Borré archivo viejo en /tmp: {fname}")
        except Exception as e:
            logger.warning(f"[cleanup] No pude limpiar /tmp antes de generar el talón: {e}")
            # Aunque falle, continuamos: quizá no haya permisos, pero igual intentamos descargar.

        # 6) Hacer clic en “Generar Talón”
        try:
            field = driver.find_element(By.ID, "j_id78:generarTalon")
            # Tomamos timestamp justo antes de hacer clic:
            click_time = time.time()
            logger.info(f"About to click 'Generate PDF' button for bucket_id {str(bucket_id)} (timestamp={click_time})")
            human_click(driver, field)
            logger.info(f"Clicked 'Generate PDF' button for bucket_id {str(bucket_id)}")
        except Exception as e:
            logger.error(f"ERROR: No pude hacer clic en 'Generar Talón': {e}")
            notify_error(f"ERROR: No pude hacer clic en 'Generar Talón' para bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error haciendo clic en 'Generar Talón': {e}"}) }

        # 7) Esperar a que aparezca o se modifique un *.pdf en /tmp DESPUÉS de click_time
        try:
            logger.info(f"Waiting for PDF to appear en {download_dir} (timeout=60s)...")
            downloaded_pdf_path = wait_for_download_to_complete(download_dir, click_time, timeout=60)
            logger.info(f"wait_for_download_to_complete returned: {downloaded_pdf_path}")
        except Exception as e:
            logger.error(f"ERROR inesperado en wait_for_download_to_complete: {e}")
            notify_error(f"ERROR inesperado en wait_for_download_to_complete para bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error en esperar descarga de PDF: {e}"}) }

        src_path = downloaded_pdf_path.get("path")
        if not src_path:
            logger.error("ERROR: PDF no fue encontrado luego de esperar. Abortando.")
            notify_error(f"ERROR: PDF no encontrado para bucket_id {str(bucket_id)} después de 60s")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error: PDF file not found en {download_dir}"}) }

        # 8) Renombrar y subir a S3
        try:
            new_pdf_path = os.path.join(download_dir, f"{bucket_id}.pdf")
            os.rename(src_path, new_pdf_path)
            logger.info(f"Talon descargado y renombrado a: {new_pdf_path}")
        except Exception as e:
            logger.error(f"ERROR al renombrar {src_path} a {str(bucket_id)}.pdf: {e}")
            notify_error(f"ERROR al renombrar PDF para bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al renombrar PDF: {e}"}) }

        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        if not s3_bucket:
            logger.error("ERROR: S3_BUCKET_NAME no está definido.")
            notify_error("ERROR: S3_BUCKET_NAME no está definido.")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': "S3_BUCKET_NAME environment variable is not set."}) }

        try:
            s3_client.upload_file(new_pdf_path, s3_bucket, f"bills/{os.path.basename(new_pdf_path)}")
            logger.info(f"Uploaded {os.path.basename(new_pdf_path)} to S3 bucket {s3_bucket} con key bills/{os.path.basename(new_pdf_path)}")
            # Insertar estado en MongoDB
            client = MongoClient(os.environ.get('MONGODB_URI'))
            db = client['production']
            collection_status = db['status']
            status_doc = {
                "id_bucket": bucket_id,
                "description": "pdf_downloaded",
                "createdAt": datetime.utcnow()
            }
            result = collection_status.insert_one(status_doc)
            logger.info(f"Inserted document en status con id: {result.inserted_id}")
        except Exception as e:
            logger.error(f"ERROR al subir el PDF a S3 o insertar status: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al subir PDF o insertar estado: {e}"}) }

        # 9) Enviar mensaje a SQS
        try:
            if not SQS_QUEUE_URL:
                logger.error("ERROR: No SQS_QUEUE_URL definido.")
                notify_error("ERROR: No hay SQS_QUEUE_URL")
                return { 'statusCode': 500, 'body': json.dumps({'ERROR': "SQS_QUEUE_URL environment variable is not set."}) }
            message_body = json.dumps({"bucket_id": str(bucket_id)})
            sqs_client.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=message_body)
            logger.info(f"Successfully sent bucket_id {str(bucket_id)} a la cola SQS.")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Successfully queued bucket_id {str(bucket_id)} for processing',
                    'bucket_id': str(bucket_id)
                })
        }
        except Exception as e:
            logger.error(f"ERROR al enviar mensaje a SQS: {e}")
            notify_error(f"ERROR al enviar mensaje a SQS for bucket_id {str(bucket_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al enviar mensaje a SQS: {e}"}) }
    
    
    ################################################################################################
    ################################ AMPLIACION ####################################################
    ################################################################################################
     #ampliacion_id es pasado como bucket_id.
    else:
        try:
            ampliacion_id = ObjectId(bucket_id)
            wait = WebDriverWait(driver, 10)
        except Exception as e:
            logger.error(f"ERROR: Error definiendo ObjectId: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': "Error definiendo ObjectId"}) }

        logger.info(f"Starting submiting for {bucket_type}")
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "BUTTON2")))
            button = driver.find_element(By.NAME, "BUTTON2")
            human_click(driver, button)
            logger.info("Click correcto en 'Enviar Solicitud'.")
        except Exception as e:
            logger.error(f"ERROR: Error al hacer clic en 'Enviar Solicitud': {e}")
            notify_error(f"ERROR: Error al hacer clic en 'Enviar Solicitud' para ampliacion {str(ampliacion_id)}: {e}")
            return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al hacer clic en 'Enviar Solicitud': {e}"}) }
              
        try:
            elem = WebDriverWait(driver, 2).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "span.ErrorViewer"))
            )
            msg = elem.text.strip()
            if msg == "SOLICITUD PROCESADA CON EXITO":
                logger.info("Se procesa correctamente la extensión")
            else:
                logger.error(f"Error en la página de Solicitud. Mensaje: {msg}")
                notify_error(f"Error en la página de Solicitud. Mensaje: {msg}")
                return {'statusCode': 500, 'body': json.dumps({'ERROR': f"Error en la página de Solicitud: {msg}"})}
        except (TimeoutException, NoSuchElementException):
            # No llegó ningún mensaje de error en 2s → asumimos éxito
            pass
            
        driver.get("https://www.dgr.gub.uy/sr/principal.jsf")
        try:
            wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "50"))).click()
            tbody = wait.until(EC.presence_of_element_located((By.ID, "j_id78:dataTable:tb")))
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            logger.info(f"Found {len(rows)} rows in the table.")
        except Exception as e:
            logger.error(f"Falla ampliando tabla para ver todas las solicitudes recientes: {e}")
            notify_error(f"Falla ampliando tabla para ver todas las solicitudes recientes: {e}")
            return {'statusCode': 500, 'body': json.dumps({'ERROR': f"Falla ampliando tabla: {e}"})}
        
        
         # 4) Consultar ampliaciones en MongoDB
        db = client['production']
        ampliacion_col = db['ampliacion']
        bucket_col = db['bucket']
        try:
            # Buscar la ampliación por _id y obtener su id_bucket
            ampliacion_doc = ampliacion_col.find_one({"_id": ampliacion_id})
            if not ampliacion_doc or "id_bucket" not in ampliacion_doc:
                logger.error(f"No se encontró ampliación con _id={ampliacion_id} o falta id_bucket")
                notify_error(f"No se encontró ampliación con _id={ampliacion_id} o falta id_bucket")
                return {'statusCode': 500, 'body': json.dumps({'ERROR': f"No se encontró ampliación con _id={ampliacion_id} o falta id_bucket"})}
            id_bucket = ampliacion_doc["id_bucket"]
            # Buscar todas las observaciones con ese id_bucket
            obs_list = list(ampliacion_col.find({"id_bucket": id_bucket}))
            
        except Exception as e:
            logger.error(f"Error en conexión a mongo (ampliacion): {e}")
            notify_error(f"Error en conexión a mongo: {e}")
            return {'statusCode': 500, 'body': json.dumps({'ERROR': f"Error en mongo: {e}"})}

        
        if len(obs_list) == 1:
            try:
                status_col = db["status_ampliacion"]
                docs = [
                    {"id_ampliacion": ampliacion_id, "description": "pdf_downloaded", "createdAt": datetime.utcnow(), "exonerado": True},
                    {"id_ampliacion": ampliacion_id, "description": "sent_abitab", "createdAt": datetime.utcnow() + timedelta(seconds=3), "exonerado": True},
                    {"id_ampliacion": ampliacion_id, "description": "pago", "createdAt": datetime.utcnow() + timedelta(seconds=6), "exonerado": True}
                ]
                status_col.insert_many(docs)
                logger.info("Guardados pdf_downloaded y sent_abitab. No se necesita nada más")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'Successfully queued ampliacion_id {ampliacion_id}',
                        'ampliacion_id': str(ampliacion_id)
                    })
                }
            except Exception as e:
                logger.error(f"Error al guardar primera ampliacion: {e}")
                notify_error(f"Error al guardar primera ampliacion: {e}")
                return {'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al guardar primera ampliacion: {e}"})}
                    
        else:
            logger.info(f"Se encontraron {len(obs_list)} ampliaciones (no necesariamente pagas) para ese bucket.")
            resp = count_non_active_ampliaciones(id_bucket, ampliacion_col, db)
            logger.info(f"Found these ampliaciones payed: {resp}")
            length_resp = len(resp)
            if length_resp == 1:
                logger.info("No hace falta mandar a SQS dado que es la primera ampliación posterior.")
                status_col = db["status_ampliacion"]
                docs = [
                    {"id_ampliacion": ampliacion_id, "description": "pdf_downloaded", "createdAt": datetime.utcnow(), "exonerado": True},
                    {"id_ampliacion": ampliacion_id, "description": "sent_abitab", "createdAt": datetime.utcnow() + timedelta(seconds=3), "exonerado": True},
                    {"id_ampliacion": ampliacion_id, "description": "pago", "createdAt": datetime.utcnow() + timedelta(seconds=6), "exonerado": True}
                ]
                status_col.insert_many(docs)
                logger.info("Guardados pdf_downloaded y sent_abitab. No se necesita nada más")
                
                return {'statusCode': 200, 'body': json.dumps({'message': f'Successfully queued ampliacion_id {str(ampliacion_id)}', 'ampliacion_id': str(ampliacion_id)})}
            
            else:
                bucket_doc = bucket_col.find_one({"_id": bucket_id})
                numero_solicitud = bucket_doc.get("dgr_id", "")
                logger.info(f"Numero Solicitud: {numero_solicitud}")
            
                now = datetime.utcnow()
                target_tipo = f"ampliación {len(resp)}"
                found = False

                for row in rows:
                    try:
                        numero = row.find_element(By.CSS_SELECTOR, "span[id$=':numero']").text.strip()
                        estado = row.find_element(By.CSS_SELECTOR, "span[id$=':estado_deuda']").text.strip()
                        tipo = row.find_element(By.CSS_SELECTOR, "span[id$=':tipo']").text.strip()
                        fecha_info_txt = row.find_element(By.CSS_SELECTOR, "span[id$=':fecha_emision']").text.strip()
                        fecha_dt = datetime.strptime(fecha_info_txt, "%d/%m/%Y %H:%M")
                        print(f"Processing row: numero={numero}, estado={estado}, tipo={tipo}")

                    except Exception as e:
                        notify_error(f"Error leyendo fila: {e}")
                        print(f"Error leyendo fila: {e}")
                        continue
                    if numero == numero_solicitud and target_tipo in tipo.lower() and abs(now - fecha_dt) <= timedelta(hours=8):
                        found = True
                        break  
                        
                    if not found:
                        logger.error("No se encontró una solicitud de ampliacion esperada. Chequear pagina de DGR")
                        notify_error("No se encontró una solicitud de ampliacion esperada. Chequear pagina de DGR")
                            
                      # 9) Insertar estado exonerado o generar PDF
                status_col = db["status_ampliacion"] 
                download_dir = "/tmp"
                try:
                    for fname in os.listdir(download_dir):
                        if fname.lower().endswith(".pdf") or fname.lower().endswith(".crdownload"):
                            full_path = os.path.join(download_dir, fname)
                            os.remove(full_path)
                            logger.info(f"[cleanup] Borré archivo viejo en /tmp: {fname}")
                except Exception as e:
                    logger.warning(f"[cleanup] No pude limpiar /tmp antes de generar el talón: {e}")
                    # Aunque falle, continuamos: quizá no haya permisos, pero igual intentamos descargar.

                # 1) Find and click the checkbox in this row
                try:
                    print("Selecting checkbox for download...")
                    checkbox = row.find_element(By.CSS_SELECTOR, "input[type='checkbox']")
                    if not checkbox.is_selected():
                        checkbox.click()
                        # small pause to allow any JS to register the click
                        time.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error haciendo click en checkbox previo a descargar talon de pago. Error {e}")    
                    notify_error(f"Error haciendo click en checkbox previo a descargar talon de pago. Error {e}")    

                # 6) Hacer clic en “Generar Talón”
                try:
                    field = driver.find_element(By.ID, "j_id78:generarTalon")
                    # Tomamos timestamp justo antes de hacer clic:
                    click_time = time.time()
                    logger.info(f"About to click 'Generate PDF' button for ampliacion_id {str(ampliacion_id)} (timestamp={click_time})")
                    human_click(driver, field)
                    logger.info(f"Clicked 'Generate PDF' button for ampliacion_id {str(ampliacion_id)}")
                except Exception as e:
                    logger.error(f"ERROR: No pude hacer clic en 'Generar Talón': {e}")
                    notify_error(f"ERROR: No pude hacer clic en 'Generar Talón' para ampliacion_id {str(ampliacion_id)}. Error: {e}")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error haciendo clic en 'Generar Talón': {e}"}) }

                # 7) Esperar a que aparezca o se modifique un *.pdf en /tmp DESPUÉS de click_time
                try:
                    logger.info(f"Waiting for PDF to appear en {download_dir} (timeout=60s)...")
                    downloaded_pdf_path = wait_for_download_to_complete(download_dir, click_time, timeout=60)
                    logger.info(f"wait_for_download_to_complete returned: {downloaded_pdf_path}")
                except Exception as e:
                    logger.error(f"ERROR inesperado en wait_for_download_to_complete: {e}")
                    notify_error(f"ERROR inesperado en wait_for_download_to_complete para ampliacion_id {str(ampliacion_id)}. Error: {e}")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error en esperar descarga de PDF: {e}"}) }

                src_path = downloaded_pdf_path.get("path")
                if not src_path:
                    logger.error("ERROR: PDF no fue encontrado luego de esperar. Abortando.")
                    notify_error(f"ERROR: PDF no encontrado para ampliacion_id {str(ampliacion_id)} después de 60s")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error: PDF file not found en {download_dir}"}) }

                # 8) Renombrar y subir a S3
                try:
                    new_pdf_path = os.path.join(download_dir, f"ampliacion_{ampliacion_id}.pdf")
                    os.rename(src_path, new_pdf_path)
                    logger.info(f"Talon de ampliación descargado y renombrado a: {new_pdf_path}")
                except Exception as e:
                    logger.error(f"ERROR al renombrar {src_path} a {str(ampliacion_id)}.pdf: {e}")
                    notify_error(f"ERROR al renombrar PDF para ampliacion_id {str(ampliacion_id)}: {e}")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al renombrar PDF: {e}"}) }

                s3_bucket = os.environ.get('S3_BUCKET_NAME')
                if not s3_bucket:
                    logger.error("ERROR: S3_BUCKET_NAME no está definido.")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': "S3_BUCKET_NAME environment variable is not set."}) }
                
                                
                try:
                    s3_client.upload_file(new_pdf_path, s3_bucket, f"bills/{os.path.basename(new_pdf_path)}")
                    logger.info(f"Uploaded {os.path.basename(new_pdf_path)} to S3 bucket {s3_bucket} con key bills/{os.path.basename(new_pdf_path)}")
                    # Insertar estado en MongoDB
                    client = MongoClient(os.environ.get('MONGODB_URI'))
                    db = client['production']
                    collection_status = db['status_ampliacion']
                    status_doc = {
                        "id_bucket": bucket_id,
                        "description": "pdf_downloaded",
                        "createdAt": datetime.utcnow()
                    }
                    result = collection_status.insert_one(status_doc)
                    logger.info(f"Inserted document en status con id: {result.inserted_id}")
                except Exception as e:
                    logger.error(f"ERROR al subir el PDF a S3 o insertar status: {e}")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al subir PDF o insertar estado: {e}"}) }

                # 9) Enviar mensaje a SQS
                try:
                    if not SQS_QUEUE_URL:
                        logger.error("ERROR: No SQS_QUEUE_URL definido.")
                        notify_error("ERROR: No hay SQS_QUEUE_URL")
                        return { 'statusCode': 500, 'body': json.dumps({'ERROR': "SQS_QUEUE_URL environment variable is not set."}) }
                    message_body = json.dumps({"ampliacion_id": str(ampliacion_id)})
                    sqs_client.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=message_body)
                    logger.info(f"Successfully sent ampliacion_id {str(ampliacion_id)} a la cola SQS.")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': f'Successfully queued ampliacion_id {str(ampliacion_id)} for processing',
                            'ampliacion_id': str(ampliacion_id)
                        })
                }
                except Exception as e:
                    logger.error(f"ERROR al enviar mensaje a SQS: {e}")
                    notify_error(f"ERROR al enviar mensaje a SQS for ampliacion_id {str(ampliacion_id)}: {e}")
                    return { 'statusCode': 500, 'body': json.dumps({'ERROR': f"Error al enviar mensaje a SQS: {e}"}) }
            
                
                
def count_non_active_ampliaciones(bucket_id, ampliacion_coll, db):
    # 1) Fetch all ampliación _ids for this bucket
    ampliaciones = ampliacion_coll.find(
        { "id_bucket": ObjectId(bucket_id) },
        { "_id": 1 }
    )
    ampliacion_ids = [doc["_id"] for doc in ampliaciones]
    if not ampliacion_ids:
        return []

    pipeline = [
        # only statuses for those ampliaciones
        { "$match": { "id_ampliacion": { "$in": ampliacion_ids } } },

        # sort so that the most recent createdAt is first
        { "$sort": { "createdAt": -1 } },

        # group by ampliación, taking the first description (i.e. latest)
        { "$group": {
            "_id":           "$id_ampliacion",
            "lastDesc":      { "$first": "$description" }
        }},

        # keep only those whose lastDesc != "active"
        { "$match": { "lastDesc": { "$ne": "active" } } },

        # count how many
       
    ]

    cursor = db["status_ampliacion"].aggregate(pipeline)

    return [doc["_id"] for doc in cursor]
        
        
        
# ──────────────────────────────────────────────────────────────────────────────

def wait_for_download_to_complete(download_dir, click_timestamp, timeout=60):
    """
    Espera hasta que aparezca o se modifique un archivo .pdf en download_dir
    posterior a click_timestamp. Retorna {"path": ruta_del_pdf} o {"path": None, ...} en timeout.
    """
    start_time = time.time()
    logger.info(f"[wait_for_download] Iniciando. Observando {download_dir} desde timestamp={click_timestamp}")

    while time.time() - start_time < timeout:
        for fname in os.listdir(download_dir):
            if fname.lower().endswith(".pdf"):
                file_path = os.path.join(download_dir, fname)
                try:
                    mtime = os.path.getmtime(file_path)
                except Exception as e:
                    logger.warning(f"[wait_for_download] No pude leer la fecha de '{file_path}': {e}")
                    continue

                logger.debug(f"[wait_for_download] Archivo '{fname}' con mtime={mtime}, click_timestamp={click_timestamp}")

                if mtime >= click_timestamp:
                    logger.info(f"[wait_for_download] Encontré PDF nuevo (o modificado): {file_path} (mtime={mtime})")
                    return {"path": file_path}

        current_listing = os.listdir(download_dir)
        logger.debug(f"[wait_for_download] Aún no detectado; contenidos: {current_listing}")
        time.sleep(0.7)

    final_list = os.listdir(download_dir)
    logger.error(f"[wait_for_download] Timeout tras {timeout}s. Contenido final de {download_dir}: {final_list}")
    notify_error(f"[wait_for_download] Timeout tras {timeout}s. Contenido final de {download_dir}: {final_list}")
    return {
        "path": None,
        "statusCode": 500,
        "body": json.dumps({'ERROR': "PDF download timed out"})
    }
    
    
    
