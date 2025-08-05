from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import logging
from selenium.common.exceptions import TimeoutException


from human_functions import human_type, human_click, human_select
from notify_error import notify_error

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fill_form_PJ(bucket_id, driver, client, fisica_true):
    """
    Rellena el formulario de Persona Jurídica en DGR para cada observación del bucket_id.
    Devuelve un dict con:
      - "status": "success", "critical_error" o "submission_error"
      - "errors": lista de mensajes en caso de fallo
    """
    critical_errors = []
    submission_errors = []
    logger.info(f"Starting PJ for Bucket ID: {bucket_id}")

    # 1) Conexión a MongoDB (crítico si falla)
    try:
        logger.info("Connecting to MongoDB...")
        db = client['production']
        persona_juridica_db = db['persona_juridica']
        logger.info("Connected to MongoDB.")
    except Exception as e:
        msg = f"Critical: Error connecting to MongoDB for bucket_id {bucket_id}: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 2) Recuperar observaciones (crítico si falla)
    try:
        logger.info(f"Retrieving observations for bucket_id: {bucket_id}...")
        filtro = {"id_bucket": bucket_id}
        observaciones = persona_juridica_db.find(filtro)
        total = persona_juridica_db.count_documents(filtro)
        logger.info(f"Retrieved {total} observations for bucket_id: {bucket_id}.")
    except Exception as e:
        msg = f"Critical: Error retrieving observations from MongoDB for bucket_id {bucket_id}: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 3) Seleccionar pestaña “Persona Jurídica” o navegar directamente (crítico si falla)
    if fisica_true:
        try:
            element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/form/h2/table/tbody/tr[3]/td/table/tbody/tr[1]/td/span/table/tbody/tr/td[2]')
                )
            )
            human_click(driver, element)
        except Exception as e:
            msg = (
                f"Critical: Error haciendo click en Persona Juridica después de Persona Fisica "
                f"for bucket_id {bucket_id}: {e}"
            )
            logger.error(msg)
            notify_error(msg)
            critical_errors.append(msg)
            return {"status": "critical_error", "errors": critical_errors}
    else:
        try:
            driver.get("https://www.dgr.gub.uy/etimbreapp/servlet/hpersolicitudform?2")
        except Exception as e:
            msg = f"Critical: Error initializing the WebDriver or loading PJ form for bucket_id {bucket_id}: {e}"
            logger.error(msg)
            notify_error(msg)
            critical_errors.append(msg)
            return {"status": "critical_error", "errors": critical_errors}

    # 4) Iterar y completar cada observación
    for observacion in observaciones:
        observation_id = observacion.get("_id", "desconocido")
        obs_errors = []
        logger.info(f"Processing observation ID: {observation_id}...")

        try:
            # Extraer campos de la observación
            rut                         = observacion.get("rut", "").strip()
            bps                         = observacion.get("bps", "").strip()
            nombre                      = observacion.get("nombre", "").strip()
            interdicciones              = observacion.get("interdicciones", False)
            mandatosDia                 = observacion.get("mandatosDia", "").strip()
            mandatosMes                 = observacion.get("mandatosMes", "").strip()
            mandatosAno                 = observacion.get("mandatosAno", "").strip()
            mandatoDesde                = observacion.get("mandatoDesde", "").strip()
            mandatoHasta                = observacion.get("mandatoHasta", "").strip()
            sociedadCivilDesde          = observacion.get("sociedadCivilDesde", "").strip()
            sociedadCivilHasta          = observacion.get("sociedadCivilHasta", "").strip()
            registroComercio            = observacion.get("comercio", False)
            prendasSinDesplazamiento     = observacion.get("prendas", False)
            rubrica                     = observacion.get("rubrica", False)
            rubricaYear                 = observacion.get("rubricaYear", "").strip()
            acf                         = observacion.get("acf", False)
        except Exception as e:
            msg = f"[{observation_id}] ERROR extracting fields: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4a) RUT
        try:
            if rut:
                logger.info(f"Entering RUT: {rut}")
                field = driver.find_element(By.ID, "_RUCTEMP")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(12):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, rut)
                logger.info(f"Successfully entered RUT: {rut}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering RUT: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4b) BPS
        try:
            if bps:
                logger.info(f"Entering BPS: {bps}")
                field = driver.find_element(By.ID, "CTLBPSJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(12):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, bps)
                logger.info(f"Successfully entered BPS: {bps}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering BPS: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4c) Nombre PJ
        try:
            if nombre:
                logger.info(f"Entering Nombre PJ: {nombre}")
                field = driver.find_element(By.ID, "CTLNOMBREJUR")
                human_click(driver, field)
                field.clear()
                human_type(field, nombre)
                logger.info(f"Successfully entered Nombre PJ: {nombre}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Nombre PJ: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4d) Interdicciones (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLINTERJUR")
            logger.info(f"Setting Interdicciones: {interdicciones}")
            if interdicciones:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
            logger.info(f"Successfully set Interdicciones to: {interdicciones}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Interdicciones: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4e) Mandatos (Día, Mes, Año)
        try:
            if mandatosDia:
                logger.info(f"Entering Mandatos Dia: {mandatosDia}")
                field = driver.find_element(By.ID, "CTLDD_PJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(2):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatosDia)
                logger.info(f"Successfully entered Mandatos Dia: {mandatosDia}")

            if mandatosMes:
                logger.info(f"Entering Mandatos Mes: {mandatosMes}")
                field = driver.find_element(By.ID, "CTLMM_PJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(2):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatosMes)
                logger.info(f"Successfully entered Mandatos Mes: {mandatosMes}")

            if mandatosAno:
                logger.info(f"Entering Mandatos Ano: {mandatosAno}")
                field = driver.find_element(By.ID, "CTLAA_PJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatosAno)
                logger.info(f"Successfully entered Mandatos Ano: {mandatosAno}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandatos: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4f) Mandatos Desde–Hasta
        try:
            if mandatoDesde:
                logger.info(f"Entering Mandato Desde: {mandatoDesde}")
                field = driver.find_element(By.ID, "CTLPOD_DESJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatoDesde)
                logger.info(f"Successfully entered Mandato Desde: {mandatoDesde}")

            if mandatoHasta:
                logger.info(f"Entering Mandato Hasta: {mandatoHasta}")
                field = driver.find_element(By.ID, "CTLPOD_HASJUR")
                human_click(driver, field)
                field.clear()
                human_type(field, mandatoHasta)
                logger.info(f"Successfully entered Mandato Hasta: {mandatoHasta}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandatos Desde–Hasta: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4g) Sociedad Civil de Propiedad Horizontal (Desde–Hasta)
        try:
            if sociedadCivilDesde:
                logger.info(f"Entering Sociedad Civil Desde: {sociedadCivilDesde}")
                field = driver.find_element(By.ID, "CTLSOC_DESJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, sociedadCivilDesde)
                logger.info(f"Successfully entered Sociedad Civil Desde: {sociedadCivilDesde}")

            if sociedadCivilHasta:
                logger.info(f"Entering Sociedad Civil Hasta: {sociedadCivilHasta}")
                field = driver.find_element(By.ID, "CTLSOC_HASJUR")
                field.clear()
                human_type(field, sociedadCivilHasta)
                logger.info(f"Successfully entered Sociedad Civil Hasta: {sociedadCivilHasta}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Sociedad Civil: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4h) Registro Comercio (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLCOMERCIOJUR")
            logger.info(f"Setting Registro Comercio: {registroComercio}")
            if registroComercio:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Registro Comercio: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4i) Prendas sin Desplazamiento (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLPRENDASJUR")
            logger.info(f"Setting Prendas sin Desplazamiento: {prendasSinDesplazamiento}")
            if prendasSinDesplazamiento:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Prendas sin Desplazamiento: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4j) Rubrica Year + checkbox
        try:
            if rubrica:
                logger.info(f"Entering Rubrica Year: {rubricaYear}")
                field = driver.find_element(By.ID, "CTLANORUBJUR")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, rubricaYear)
                logger.info(f"Successfully entered Rubrica Year: {rubricaYear}")

            checkbox = driver.find_element(By.NAME, "CTLRUBJUR")
            logger.info(f"Setting Rubrica: {rubrica}")
            if rubrica:
                if not checkbox.is_selected():
                    driver.find_element(By.TAG_NAME, "body").click()
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Rubrica: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4k) ACF (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLACF")
            logger.info(f"Setting ACF: {acf}")
            if acf:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting ACF: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 4l) Clic en "Agregar"
        try:
            logger.info("Clicking the 'Agregar' button...")
            button = driver.find_element(By.NAME, "BUTTON9")
            human_click(driver, button)
            logger.info("Successfully clicked 'Agregar' button")
        except Exception as e:
            msg = f"[{observation_id}] ERROR clicking 'Agregar' button: {e}"
            logger.error(msg)
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

        # Si hubo errores en esta observación, agregarlos a submission_errors
        if obs_errors:
            submission_errors.append({
                "observation_id": observation_id,
                "messages": obs_errors
            })

    # 5) Al terminar todas las observaciones
    if submission_errors:
        summary_lines = []
        for entry in submission_errors:
            oid = entry["observation_id"]
            for msg in entry["messages"]:
                summary_lines.append(f"[Obs {oid}] {msg}")
        final_msg = (
            f"Submission errors in fill_form_PJ for bucket_id {bucket_id}:\n"
            + "\n".join(summary_lines)
        )
        logger.error(final_msg)
        notify_error(final_msg)
        return {"status": "submission_error", "errors": submission_errors}

    # Si no hubo errores críticos ni de envío
    logger.info(f"fill_form_PJ completed successfully for bucket_id {bucket_id}")
    return {"status": "success", "errors": []}