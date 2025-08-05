import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,StaleElementReferenceException
from human_functions import human_click, human_type, human_select
from notify_error import notify_error
import time
import random

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def pick_option_by_text(select_el, text_busqueda):
    sel = Select(select_el)
    wanted = text_busqueda.strip().upper()
    for o in sel.options:
        if o.text.strip().upper() == wanted:
            return o, sel
    return None, sel

def force_change(driver, select_el, value):
    driver.execute_script("""
        const el = arguments[0];
        const val = arguments[1];
        el.value = val;
        // Disparamos los eventos que usa GeneXus
        el.dispatchEvent(new MouseEvent('click', {bubbles:true}));
        if (typeof gxonchange === 'function') { gxonchange(el); }
        el.dispatchEvent(new Event('change', {bubbles:true}));
        el.blur();
    """, select_el, value)
    

def fill_form_AM(bucket_id, driver, client):
    logger.info(f"Starting Automotor for Bucket ID: {bucket_id}")
    critical_errors = []     # Errores que implican fallo antes de iterar observaciones
    submission_errors = []   # Errores de cada envío de formulario

    # 1) Intentar conectar a MongoDB
    try:
        logger.info("Connecting to MongoDB...")
        db = client['production']
        automotores_db = db['automotores']
        logger.info("Connected to MongoDB.")
    except Exception as e:
        msg = f"Critical: Error connecting to MongoDB for bucket_id {bucket_id}. Error: {str(e)}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 2) Intentar recuperar observaciones
    try:
        logger.info(f"Retrieving observations for bucket_id: {bucket_id}...")
        filtro = {"id_bucket": bucket_id}
        observaciones = automotores_db.find(filtro)
        total = automotores_db.count_documents(filtro)
        logger.info(f"Retrieved {total} observations for bucket_id: {bucket_id}.")
    except Exception as e:
        msg = f"Critical: Error retrieving observations from MongoDB for bucket_id {bucket_id}. Error: {str(e)}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 3) Cargar la página del formulario
    try:
        driver.get("http://www.dgr.gub.uy/etimbreapp/servlet/hsolicitudform?3")
        logger.info("Logged in and navigated to the form page.")
    except Exception as e:
        msg = f"Critical: Error initializing the WebDriver or loading form for bucket_id {bucket_id}. Error: {str(e)}"
        logger.error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 4) Iterar cada observación
    for observacion in observaciones:
        observation_id = observacion.get('_id', 'desconocido')
        obs_errors = []   # Errores solo de esta observación
        logger.info(f"Processing observation ID: {observation_id}...")

        try:            
            # Extraer campos de la observación
            padronActual         = observacion.get("padronActual", "")
            departamento         = observacion.get("departamento", "")
            localidad            = observacion.get("localidad", "")
            marca                = observacion.get("marca", "")
            modelo               = observacion.get("modelo", "")
            tipo                 = observacion.get("tipoAutomotor", "")
            placaMunicipal       = observacion.get("placaMunicipal", "")
            ano                  = observacion.get("year", "")
            padronAnterior1      = observacion.get("padronAnterior1", "")
            departamentoAnterior1 = observacion.get("departamentoAnterior1", "")
            localidadAnterior1   = observacion.get("localidadAnterior1", "")
            placaMunicipal1      = observacion.get("placaMunicipal1", "")
            padronAnterior2      = observacion.get("padronAnterior2", "")
            departamentoAnterior2 = observacion.get("departamentoAnterior2", "")
            localidadAnterior2   = observacion.get("localidadAnterior2", "")
            placaMunicipal2      = observacion.get("placaMunicipal2", "")

            # —— PADRON ACTUAL ——
            try:
                if padronActual:
                    logger.info(f"Entering Padron Actual: {padronActual}")
                    field = driver.find_element(By.ID, "CTLPADRONAUT2")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, padronActual)
                    logger.info(f"Successfully entered Padron Actual: {padronActual}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Padron Actual: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— DEPARTAMENTO ——
            try:
                if departamento:
                    logger.info(f"Selecting Departamento: {departamento}")
                    select_element = driver.find_element(By.NAME, "_DEPAUT")
                    select = Select(select_element)
                    human_select(select, departamento)
                    logger.info(f"Successfully selected Departamento: {departamento}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Departamento: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            
                # ----- LOCALIDAD -----
            try:
                if localidad:
                    logger.info(f"Selecting localidad: {localidad}")
                    select_element = driver.find_element(By.NAME, "CTLLOCAUT2")
                    select = Select(select_element)
                    time.sleep(random.uniform(0.1, 0.3))
                    human_select(select, localidad)
                    logger.info(f"Successfully selected localidad: {localidad}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting localidad: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

                 
            marca_css = 'select[name="_MARCASAUT"][gxrow="0001"]'

            # —— MARCA ——
            try:
                if marca:
                    logger.info(f"Selecting Marca: {marca}")

                    wait = WebDriverWait(driver, 15)
                    marca_sel = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, marca_css)))
                    human_click(driver, marca_sel)

                    wait.until(lambda d: len(Select(d.find_element(By.CSS_SELECTOR, marca_css)).options) > 2)

                    opt, sel_obj = pick_option_by_text(marca_sel, marca)
                    if not opt:
                        opts = [o.text for o in sel_obj.options]
                        msg = f"[Obs {observation_id}] Marca '{marca}' no encontrada. Opciones: {opts}"
                        logger.error(msg)
                        obs_errors.append(msg)
                    else:
                        value = opt.get_attribute("value")
                        sel_obj.select_by_value(value)
                        force_change(driver, marca_sel, value)

                        wait.until(lambda d: d.find_element(By.CSS_SELECTOR, marca_css).get_attribute("value") == value)
                        logger.info(f"Successfully selected Marca: {marca} (value={value})")

            except StaleElementReferenceException as e:
                # ¿Quedó bien seleccionada? Si sí → warning y seguimos.
                try:
                    sel_now = Select(driver.find_element(By.CSS_SELECTOR, marca_css))
                    txt_now = sel_now.first_selected_option.text.strip().upper()
                    if txt_now == marca.strip().upper():
                        logger.warning(f"Stale en Marca pero ya está seleccionada. Ignorando.")
                    else:
                        raise
                except Exception:
                    msg = f"[Obs {observation_id}] Error selecting Marca: {e}"
                    logger.error(f"ERROR: {msg}")
                    obs_errors.append(msg)

            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Marca: {e}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— MODELO ——
            try:
                if modelo:
                    logger.info(f"Selecting modelo: {modelo}")

                    modelo_css = 'select[name="CTLIDENTRG_MODELOS_AUTOMOTOREDIT"][gxrow="0001"]'
                    modelo_sel = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, modelo_css)))

                    human_click(driver, modelo_sel)

                    # Esperar opciones reales
                    wait.until(lambda d: len(Select(d.find_element(By.CSS_SELECTOR, modelo_css)).options) > 1)

                    # Re-find y pick
                    modelo_sel = driver.find_element(By.CSS_SELECTOR, modelo_css)
                    opt, sel_obj = pick_option_by_text(modelo_sel, modelo)
                    if not opt:
                        opts = [o.text for o in sel_obj.options]
                        msg = f"[Obs {observation_id}] Modelo '{modelo}' no encontrado. Opciones: {opts}"
                        logger.error(msg); obs_errors.append(msg)
                    else:
                        value = opt.get_attribute("value")
                        sel_obj.select_by_value(value)
                        force_change(driver, modelo_sel, value)
                        wait.until(lambda d: d.find_element(By.CSS_SELECTOR, modelo_css).get_attribute("value") == value)
                        logger.info(f"Successfully selected modelo: {modelo} (value={value})")

            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting modelo: {e}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)


            # —— TIPO ——
            try:
                if tipo:
                    logger.info(f"Selecting tipo: {tipo!r}")  # <-- logea el valor para confirmar que no está vacío

                    tipo_css = 'select[name="CTLIDENTRG_TIPOS_AUTOMOTOREDIT"][gxrow="0001"]'

                    # 1) Esperá a que el select exista y sea clickable
                    tipo_sel = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, tipo_css)))
                    human_click(driver, tipo_sel)

                    # 2) Esperar a que aparezca la opción buscada (no solo >1 opción)
                    wait.until(
                        lambda d: any(
                            o.text.strip().upper() == tipo.strip().upper()
                            for o in Select(d.find_element(By.CSS_SELECTOR, tipo_css)).options
                        )
                    )

                    # 3) Re-find (por si el DOM se regeneró)
                    tipo_sel = driver.find_element(By.CSS_SELECTOR, tipo_css)

                    opt, sel_obj = pick_option_by_text(tipo_sel, tipo)
                    if not opt:
                        opts = [o.text for o in sel_obj.options]
                        msg = f"[Obs {observation_id}] Tipo '{tipo}' no encontrado. Opciones: {opts}"
                        logger.error(msg); obs_errors.append(msg)
                    else:
                        value = opt.get_attribute("value")
                        sel_obj.select_by_value(value)
                        force_change(driver, tipo_sel, value)

                        # 4) Validar con un find_element nuevo para evitar stale
                        wait.until(
                            lambda d: d.find_element(By.CSS_SELECTOR, tipo_css).get_attribute("value") == value
                        )
                        logger.info(f"Successfully selected tipo: {tipo} (value={value})")

            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting tipo: {e}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)
            # —— PLACA MUNICIPAL ——
            try:
                if placaMunicipal:
                    logger.info(f"Entering Placa Municipal: {placaMunicipal}")
                    field = driver.find_element(By.ID, "CTLPLACAMUNICIPALAUTEDIT")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, placaMunicipal)
                    logger.info(f"Successfully entered Placa Municipal: {placaMunicipal}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Placa Municipal: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— AÑO ——
            try:
                if ano:
                    logger.info(f"Entering Año: {ano}")
                    field = driver.find_element(By.ID, "CTLANOAUTEDIT")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, ano)
                    logger.info(f"Successfully entered Año: {ano}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Año: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— PADRON ANTERIOR 1 ——
            try:
                if padronAnterior1:
                    logger.info(f"Entering Padron Anterior 1: {padronAnterior1}")
                    field = driver.find_element(By.ID, "CTLPADRONAUT3")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, padronAnterior1)
                    logger.info(f"Successfully entered Padron Anterior 1: {padronAnterior1}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Padron Anterior 1: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— DEPARTAMENTO ANTERIOR 1 ——
            try:
                if departamentoAnterior1:
                    logger.info(f"Selecting Departamento Anterior 1: {departamentoAnterior1}")
                    select_element = driver.find_element(By.NAME, "_DEPAUT2")
                    select = Select(select_element)
                    human_select(select, departamentoAnterior1)
                    logger.info(f"Successfully selected Departamento Anterior 1: {departamentoAnterior1}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Departamento Anterior 1: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— LOCALIDAD ANTERIOR 1 ——
            try:
                if localidadAnterior1:
                    logger.info(f"Selecting Localidad Anterior 1: {localidadAnterior1}")
                    select_localidad1_element = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH,
                            '/html/body/form/h2/table/tbody/tr[3]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/div/table/tbody/tr[1]/td/p/table[1]/tbody/tr[7]/td[4]/select'))
                    )
                    human_click(driver, select_localidad1_element)
                    select = Select(select_localidad1_element)
                    human_select(select, localidadAnterior1)
                    logger.info(f"Successfully selected Localidad Anterior 1: {localidadAnterior1}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Localidad Anterior 1: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— PLACA MUNICIPAL 1 ——
            try:
                if placaMunicipal1:
                    logger.info(f"Entering Placa Municipal 1: {placaMunicipal1}")
                    field = driver.find_element(By.ID, "CTLPLACAMUNICIPALAUT2")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, placaMunicipal1)
                    logger.info(f"Successfully entered Placa Municipal 1: {placaMunicipal1}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Placa Municipal 1: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— PADRON ANTERIOR 2 ——
            try:
                if padronAnterior2:
                    logger.info(f"Entering Padron Anterior 2: {padronAnterior2}")
                    field = driver.find_element(By.ID, "CTLPADRONAUT4")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, padronAnterior2)
                    logger.info(f"Successfully entered Padron Anterior 2: {padronAnterior2}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Padron Anterior 2: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— DEPARTAMENTO ANTERIOR 2 ——
            try:
                if departamentoAnterior2:
                    logger.info(f"Selecting Departamento Anterior 2: {departamentoAnterior2}")
                    select_element = driver.find_element(By.NAME, "_DEPAUT3")
                    select = Select(select_element)
                    human_select(select, departamentoAnterior2)
                    logger.info(f"Successfully selected Departamento Anterior 2: {departamentoAnterior2}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Departamento Anterior 2: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— LOCALIDAD ANTERIOR 2 ——
            try:
                if localidadAnterior2:
                    logger.info(f"Selecting Localidad Anterior 2: {localidadAnterior2}")
                    select_localidad2_element = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH,
                            '/html/body/form/h2/table/tbody/tr[3]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/div/table/tbody/tr[1]/td/p/table[1]/tbody/tr[8]/td[4]/select'))
                    )
                    human_click(driver, select_localidad2_element)
                    select = Select(select_localidad2_element)
                    human_select(select, localidadAnterior2)
                    logger.info(f"Successfully selected Localidad Anterior 2: {localidadAnterior2}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error selecting Localidad Anterior 2: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— PLACA MUNICIPAL 2 ——
            try:
                if placaMunicipal2:
                    logger.info(f"Entering Placa Municipal 2: {placaMunicipal2}")
                    field = driver.find_element(By.ID, "CTLPLACAMUNICIPALAUT3")
                    human_click(driver, field)
                    field.clear()
                    human_type(field, placaMunicipal2)
                    logger.info(f"Successfully entered Placa Municipal 2: {placaMunicipal2}")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error entering Placa Municipal 2: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)

            # —— PRESIONAR “AGREGAR” ——
            
            try:
                logger.info("Clicking the 'Agregar' button...")

                # A veces los botones tipo SUBMIT requieren un submit explícito del form.
                # Buscamos el botón por NAME y verificamos que sea el correcto.
                button = driver.find_element(By.NAME, "BUTTON2")

                # Intentamos hacer scroll hacia el botón por si no está visible
                driver.execute_script("arguments[0].scrollIntoView(true);", button)

                # Intentamos clickear con Selenium normal
                try:
                    human_click(driver, button)
                except Exception as click_exc:
                    logger.warning(f"No se pudo clickear el botón con human_click: {click_exc}. Intentando con JavaScript.")
                    # Si falla el click normal, probamos con JS
                    driver.execute_script("arguments[0].click();", button)

                
                time.sleep(1.2)
                # Luego de hacer clic en "Agregar", verificamos que el campo CTLPADRONAUT2 haya quedado vacío.
                try:
                    padronaut2_field = driver.find_element(By.ID, "CTLPADRONAUT2")
                    padronaut2_value = padronaut2_field.get_attribute("value")
                    if padronaut2_value not in ["", None,"0"]:
                        logger.warning(f"El campo CTLPADRONAUT2 no quedó vacío tras 'Agregar'. Valor actual: '{padronaut2_value}'. Intentando hacer clic nuevamente de otra manera.")
                        # Intentar hacer clic con JavaScript como alternativa
                        try:
                            driver.execute_script("arguments[0].click();", button)
                            time.sleep(1.5)
                            padronaut2_value_retry = padronaut2_field.get_attribute("value")
                            if padronaut2_value_retry not in ["", None,"0"]:
                                logger.warning(f"El campo CTLPADRONAUT2 sigue sin quedar vacío tras segundo intento. Valor actual: '{padronaut2_value_retry}'")
                            else:
                                logger.info("El campo CTLPADRONAUT2 quedó vacío correctamente tras segundo intento de 'Agregar'.")
                        except Exception as retry_exc:
                            logger.error(f"Error al intentar hacer clic nuevamente en 'Agregar' con JS: {retry_exc}")
                    else:
                        logger.info("El campo CTLPADRONAUT2 quedó vacío correctamente tras 'Agregar'.")
                except Exception as e:
                    logger.error(f"No se pudo verificar el campo CTLPADRONAUT2 tras 'Agregar': {e}")

                logger.info("Form submission (Agregar) intentado para esta observación.")
            except Exception as e:
                msg = f"[Obs {observation_id}] Error clicking 'Agregar' button: {str(e)}"
                logger.error(f"ERROR: {msg}")
                obs_errors.append(msg)
                
            try:
            # Esperamos hasta 5 segundos para que aparezca un span.ErrorViewer, si no aparece lanza TimeoutException.
                error_elem = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.ErrorViewer"))
                )
                # Si llegamos aquí, el elemento apareció:
                error_text = error_elem.text.strip()   # Ej.: "Debe marcar alguna sección"
                logger.error(f"ErrorViewer detectado para bucket_id {bucket_id}: '{error_text}'")
                # Llamamos a notify_error pasándole el bucket_id y el mensaje de ErrorViewer
                notify_error(f"bucket_id={bucket_id} → ErrorViewer: {error_text}")
                
                # (Opcional: aquí podrías lanzar una excepción o retornar algo para abortar,
                #  si no quieres continuar con el flujo cuando hay ese error.)
            
            except TimeoutException:
                # No apareció ningún span.ErrorViewer en esos 5 segundos: seguimos normalmente.
                logger.info(f"No apareció ErrorViewer tras 'Agregar' para bucket_id {bucket_id}. Continuo con el flujo.")

        except Exception as e:
            # Captura cualquier otro fallo inesperado al procesar la observación
            msg = f"[Obs {observation_id}] Unhandled error during processing: {str(e)}"
            logger.error(f"ERROR: {msg}")
            obs_errors.append(msg)

        # Si hubo errores en esta observación, los agrego a la lista global de envío
        if obs_errors:
            submission_errors.append({
                "observation_id": observation_id,
                "messages": obs_errors
            })

    # __— Al terminar todas las observaciones—__
    if submission_errors:
        # Formateamos un resumen de todos los errores de envío
        error_summary_lines = []
        for entry in submission_errors:
            oid = entry["observation_id"]
            for msg in entry["messages"]:
                error_summary_lines.append(f"[Obs {oid}] {msg}")
        error_summary = "\n".join(error_summary_lines)

        final_msg = (
            f"Submission errors in fill_form_AM for bucket_id {bucket_id}:\n"
            + error_summary
        )
        logger.error(final_msg)
        notify_error(final_msg)
        return {
            "status": "submission_error",
            "errors": submission_errors
        }
        
    try:
        nro_automot_field = driver.find_element(By.ID, "_NROAUTOMOT")
        nro_automot_value = nro_automot_field.get_attribute("value")
        print(f"Automotores ingresados segun DGR: {nro_automot_value}")
        
    except Exception as e:
        msg = f"Error al verificar el campo Nro de Autos Solicitados: {str(e)}"
        logger.error(msg)
        notify_error(f"bucket_id={bucket_id} → {msg}")
        return {
            "status": "submission_error",
            "errors": msg
        }

    # Si llegamos hasta aquí, no hubo ningún error crítico ni de envío
    logger.info(f"fill_form_AM completed successfully for bucket_id {bucket_id}")
    return {"status": "success", "errors": []}