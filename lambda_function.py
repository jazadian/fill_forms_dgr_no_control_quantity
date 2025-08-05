import os
import boto3
import json
from bson import ObjectId
from pymongo import MongoClient
import logging

from fill_form_AM import fill_form_AM
from fill_form_PJ import fill_form_PJ
from fill_form_PF import fill_form_PF
from fill_form_I import fill_form_I
from fill_form_Ampliacion import fill_form_Ampliacion

from notify_error import notify_error

from login import login
from submit import submit_form_and_generate_talon

s3_client = boto3.client('s3')

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        uri = os.environ.get('MONGODB_URI')
        client = MongoClient(uri)
        db = client['production']
        bucket_db = db['bucket']
        ampliacion_db = db['ampliacion']
        driver = None  # Initialize driver to None
        for record in event.get('Records', []):
            try:
                payload = json.loads(record['body'])
            except Exception as e:
                logger.error("No pude parsear record.body como JSON", exc_info=e)
                notify_error("No pude parsear record.body como JSON", exc_info=e)

            try:
                bucket_type = payload.get('bucket_type')
                
                # Determine which collection/id to use
                if bucket_type == "Ampliación":
                    doc_id_key = 'ampliacion_id'
                    db_collection = ampliacion_db
                else:
                    doc_id_key = 'bucket_id'
                    db_collection = bucket_db

                doc_id_value = payload.get(doc_id_key)
                if not doc_id_value:
                    message = f"ERROR: Falló al pasar a la lambda {doc_id_key}. No se encontro ningun valor."
                    logger.error(message)
                    notify_error(message)
                    return {
                        "statusCode": 400,
                        "body": json.dumps(f'Invalid {doc_id_key}')
                    }

                doc_object_id = ObjectId(doc_id_value)
                logger.info(f"{doc_id_key}: {doc_object_id}")
                logger.info(f"Bucket_type: {bucket_type}")

                try:
                    document = db_collection.find_one({"_id": doc_object_id})
                    logger.info(f"{doc_id_key} found on collection")
                    if document is None:
                        message = f"ERROR: No document found with the provided {doc_id_key} {doc_object_id}"
                        logger.error(message)
                        raise Exception(message)
                    logger.info(f"Document found: {document}")
                except Exception as e:
                    message = f"ERROR: An error occurred while trying to find the document: {e}"
                    logger.error(message)
                    notify_error(message)
                    raise

                # Log in to DGR system
                try:
                    logger.info("Logging in to DGR system...")
                    user_dgr = os.environ.get('DGR_USERNAME')
                    password_dgr = os.environ.get('DGR_PASSWORD')
                    driver = login(user_dgr, password_dgr)
                except Exception as e:
                    message = f"Critical: No se pudo iniciar la sesión. Verifique las credenciales o la conexión: {e}"
                    logger.error(message)
                    notify_error(message)
                    return {
                        "statusCode": 500,
                        "body": json.dumps(message)
                    }

                if driver is None:
                    message = "Critical: No se pudo iniciar la sesión. Verifique las credenciales o la conexión."
                    logger.error(message)
                    notify_error(message)
                    return {
                        "statusCode": 500,
                        "body": json.dumps(message)
                    }

                logger.info(f"Driver initialized successfully: {driver}")

                # Depending on bucket_type, call the appropriate fill_form_* function
                form_result = None

                if bucket_type == "Automotor":
                    logger.info("Starting Automotor...")
                    form_result = fill_form_AM(doc_object_id, driver, client)

                elif bucket_type == "Inmueble":
                    logger.info("Starting Inmueble...")
                    form_result = fill_form_I(doc_object_id, driver, client)

                elif bucket_type == "Ampliación":
                    logger.info("Starting Ampliacion...")
                    form_result = fill_form_Ampliacion(doc_object_id, driver, client)

                elif bucket_type == "ACF":
                    logger.info("Starting ACF (Persona Jurídica)...")
                    form_result = fill_form_PJ(doc_object_id, driver, client, fisica_true=False)

                elif bucket_type in ["Persona", "Rubrica", "Comercio", "Prendas"]:
                    persona_fisica_coll = db['persona_fisica']
                    logger.info(f"Starting {bucket_type} flow...")
                    fisica_count = persona_fisica_coll.count_documents({'id_bucket': doc_object_id})
                    logger.info(f"Found {fisica_count} documents in persona_fisica for bucket_id: {doc_object_id}")

                    form_result = {"status": "success", "errors": []}

                    # If there are persona_fisica documents, fill PF first
                    if fisica_count > 0:
                        logger.info("Comenzando llenado Persona Física.")
                        result_pf = fill_form_PF(doc_object_id, driver, client)
                        if result_pf["status"] != "success":
                            notify_error(f"Error critico en Persona Fisica. Error: {result_pf['errors']}")
                            return {
                                "statusCode": 500,
                                "body": json.dumps({
                                    "stage": "PersonaFísica",
                                    "status": "critical_error",
                                    "errors": result_pf["errors"]
                                })
                            }
                        
                        logger.info("Persona Física llenado")

                    # If count is not exactly 10 (and ≥ 0), assume PJ is needed
                    if fisica_count != 10:
                        logger.info("Comenzando llenado Persona Jurídica")
                        fisica_true = (fisica_count > 0)
                        result_pj = fill_form_PJ(doc_object_id, driver, client, fisica_true)
                        if result_pj["status"] == "critical_error":
                            return {
                                "statusCode": 500,
                                "body": json.dumps({
                                    "stage": "PersonaJurídica",
                                    "status": "critical_error",
                                    "errors": result_pj["errors"]
                                })
                            }
                        if result_pj["status"] == "submission_error":
                            logger.warning(f"Warnings during PJ: {result_pj['errors']}")
                        logger.info("Persona Jurídica llenado")

                else:
                    message = f"ERROR: Bucket Type '{bucket_type}' no encontrado"
                    logger.error(message)
                    notify_error(message)
                    return {
                        "statusCode": 400,
                        "body": json.dumps(message)
                    }

                # Handle form_result errors/warnings
                if form_result:
                    if form_result["status"] != "success":
                        notify_error(f"Error critico para {doc_id_key} {doc_object_id}. Error(es): {form_result['errors']}")
                        return {
                            "statusCode": 500,
                            "body": json.dumps({
                                "stage": bucket_type,
                                "status": "critical_error",
                                "errors": form_result["errors"]
                            })
                        }
                    
                # Submit form and generate talon
                try:
                    if driver is not None:
                        logger.info(f"Submitting form and generating talon for {doc_id_key}: {doc_object_id}...")
                        resp = submit_form_and_generate_talon(driver, doc_object_id, bucket_type)
                        if resp.get('statusCode') == 200:
                            logger.info(f"Talon generated for {doc_id_key}: {doc_object_id}.")
                            return {
                                "statusCode": 200,
                                "body": json.dumps("Processed bucket successfully")
                            }
                        else:
                            error_msg = f"Error submitting talon para {doc_id_key} {doc_object_id}: statusCode {resp.get('statusCode')}"
                            logger.error(error_msg)
                            notify_error(error_msg)
                            return {
                                "statusCode": resp.get('statusCode', 500),
                                "body": json.dumps(error_msg)
                            }
                    else:
                        error_msg = "Driver is not initialized, cannot submit form."
                        logger.error(error_msg)
                        notify_error(error_msg)
                        return {
                            "statusCode": 500,
                            "body": json.dumps(error_msg)
                        }

                except Exception as e:
                    error_msg = f"Critical: Error submitting form or generating talon: {e}"
                    logger.error(error_msg)
                    notify_error(error_msg)
                    return {
                        "statusCode": 500,
                        "body": json.dumps(error_msg)
                    }

            except Exception as e:
                logger.error(f"ERROR: An error occurred: {e}")
                notify_error(f"Error en lambda_handler: {e}")
                return {
                    "statusCode": 500,
                    "body": json.dumps(f'Internal server error: {e}')
                }
    except Exception as e:
        logger.error(f"ERROR: An error occurred: {e}")
        notify_error(f"Error en lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f'Internal server error: {e}')
        }
