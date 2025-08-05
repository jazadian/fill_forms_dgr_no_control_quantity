from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import TimeoutException

import logging

from human_functions import human_type, human_click, human_select
from notify_error import notify_error

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fill_form_PF(bucket_id, driver, client):
    """
    Rellena el formulario de Persona Física en DGR para cada observación del bucket_id.
    Devuelve un dict con:
      - "status": "success", "critical_error" o "submission_error"
      - "errors": lista de mensajes en caso de fallo
    """
    critical_errors = []
    submission_errors = []
    logger.info(f"Starting PF for Bucket ID: {bucket_id}")

    # 1) Conexión a MongoDB (crítico si falla)
    try:
        logger.info("Connecting to MongoDB...")
        db = client['production']
        persona_fisica_db = db['persona_fisica']
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
        # Defino el filtro
        filtro = {"id_bucket": bucket_id}
        # 1) Recupero el cursor para iterar (si es que vas a procesar cada documento)
        observaciones = persona_fisica_db.find(filtro)
        observaciones = persona_fisica_db.find({"id_bucket": bucket_id})
        total = persona_fisica_db.count_documents(filtro)
        logger.info(f"Retrieved {total} observations for bucket_id: {bucket_id}.")
    except Exception as e:
        msg = f"Critical: Error retrieving observations from MongoDB for bucket_id {bucket_id}: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 3) Navegar a la página de Persona Física (crítico si falla)
    try:
        driver.get("https://www.dgr.gub.uy/etimbreapp/servlet/hpersolicitudform?1")
        logger.info("Navigated to PF form page.")
    except Exception as e:
        msg = f"Critical: Error initializing WebDriver or loading PF form for bucket_id {bucket_id}: {e}"
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
            ci                                 = observacion.get("ci", "").strip()
            primerApellido                     = observacion.get("primerApellido", "").strip()
            segundoApellido                    = observacion.get("segundoApellido", "").strip()
            primerNombre                       = observacion.get("primerNombre", "").strip()
            segundoNombre                      = observacion.get("segundoNombre", "").strip()
            tercerNombre                       = observacion.get("tercerNombre", "").strip()
            comercio                           = observacion.get("comercio", False)
            interdicciones                     = observacion.get("interdicciones", False)
            prendas                            = observacion.get("prendas", False)
            rubrica                            = observacion.get("rubrica", False)
            cesionDerechosHereditariosDesde    = observacion.get("cesionDerechosHereditariosDesde", "").strip()
            cesionDerechosHereditariosHasta    = observacion.get("cesionDerechosHereditariosHasta", "").strip()
            negociosExGanancialesDesde         = observacion.get("negociosExGanancialesDesde", "").strip()
            negociosExGanancialesHasta         = observacion.get("negociosExGanancialesHasta", "").strip()
            mandatoDia                         = observacion.get("mandatosDia", "").strip()
            mandatoMes                         = observacion.get("mandatosMes", "").strip()
            mandatoAno                         = observacion.get("mandatosAno", "").strip()
            mandatoDesde                       = observacion.get("mandatoDesde", "").strip()
            mandatoHasta                       = observacion.get("mandatoHasta", "").strip()
            rubricaYear                        = observacion.get("rubicaYear", "").strip()
        except Exception as e:
            msg = f"[{observation_id}] ERROR extracting fields: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # a) CI (parte numérica y dígito verificador)
        try:
            if ci:
                logger.info(f"Entering CI: {ci}")
                # Parte numérica
                field_num = driver.find_element(By.ID, "_CITEMP")
                human_click(driver, field_num)
                field_num.clear()
                human_type(field_num, ci[:7])
                logger.info(f"Successfully entered CI number: {ci[:-1]}")
                if len(ci) == 8:
                    # Dígito verificador
                    field_dv = driver.find_element(By.ID, "CTLDVFIS")
                    human_click(driver, field_dv)
                    field_dv.clear()
                    human_type(field_dv, ci[-1:])
                    logger.info(f"Successfully entered CI verification digit: {ci[-1:]}")
                else: 
                    pass
                
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering CI: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # b) Primer Apellido
        try:
            if primerApellido:
                logger.info(f"Entering Primer Apellido: {primerApellido}")
                field = driver.find_element(By.ID, "CTLAPE1FIS")
                human_click(driver, field)
                field.clear()
                human_type(field, primerApellido)
                logger.info(f"Successfully entered Primer Apellido: {primerApellido}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Primer Apellido: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # c) Segundo Apellido
        try:
            if segundoApellido:
                logger.info(f"Entering Segundo Apellido: {segundoApellido}")
                field = driver.find_element(By.ID, "CTLAPE2FIS")
                human_click(driver, field)
                field.clear()
                human_type(field, segundoApellido)
                logger.info(f"Successfully entered Segundo Apellido: {segundoApellido}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Segundo Apellido: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # d) Primer Nombre
        try:
            if primerNombre:
                logger.info(f"Entering Primer Nombre: {primerNombre}")
                field = driver.find_element(By.ID, "CTLNOM1FIS")
                human_click(driver, field)
                field.clear()
                human_type(field, primerNombre)
                logger.info(f"Successfully entered Primer Nombre: {primerNombre}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Primer Nombre: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # e) Segundo Nombre
        try:
            if segundoNombre:
                logger.info(f"Entering Segundo Nombre: {segundoNombre}")
                field = driver.find_element(By.ID, "CTLNOM2FIS")
                human_click(driver, field)
                field.clear()
                human_type(field, segundoNombre)
                logger.info(f"Successfully entered Segundo Nombre: {segundoNombre}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Segundo Nombre: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # f) Tercer Nombre
        try:
            if tercerNombre:
                logger.info(f"Entering Tercer Nombre: {tercerNombre}")
                field = driver.find_element(By.ID, "CTLNOM3FIS")
                human_click(driver, field)
                field.clear()
                human_type(field, tercerNombre)
                logger.info(f"Successfully entered Tercer Nombre: {tercerNombre}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Tercer Nombre: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # g) Interdicciones (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLINTERFIS")
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

        # h) Cesión Derechos Hereditarios Desde
        try:
            if cesionDerechosHereditariosDesde:
                logger.info(
                    f"Entering Cesion Derechos Hereditarios Desde: {cesionDerechosHereditariosDesde}"
                )
                field = driver.find_element(By.ID, "CTLFALL_ANOFIS")
                human_click(driver, field)
                field.clear()
                human_type(field, cesionDerechosHereditariosDesde)
                logger.info(
                    f"Successfully entered Cesion Derechos Hereditarios Desde: {cesionDerechosHereditariosDesde}"
                )
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Cesion Derechos Hereditarios Desde: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # i) Cesión Derechos Hereditarios Hasta
        try:
            if cesionDerechosHereditariosHasta:
                logger.info(
                    f"Entering Cesion Derechos Hereditarios Hasta: {cesionDerechosHereditariosHasta}"
                )
                field = driver.find_element(By.ID, "CTLCES_HASFIS")
                human_click(driver, field)
                field.clear()
                human_type(field, cesionDerechosHereditariosHasta)
                logger.info(
                    f"Successfully entered Cesion Derechos Hereditarios Hasta: {cesionDerechosHereditariosHasta}"
                )
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Cesion Derechos Hereditarios Hasta: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # j) Negocios Ex Gananciales Desde
        try:
            if negociosExGanancialesDesde:
                logger.info(f"Entering Negocios Ex Gananciales Desde: {negociosExGanancialesDesde}")
                field = driver.find_element(By.ID, "CTLNG_DESFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, negociosExGanancialesDesde)
                logger.info(f"Successfully entered Negocios Ex Gananciales Desde: {negociosExGanancialesDesde}")

                # Cerrar el campo de fecha si es necesario
                field = driver.find_element(By.ID, "CTLDCM_DESFIS")
                human_click(driver, field)
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Negocios Ex Gananciales Desde: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # k) Negocios Ex Gananciales Hasta
        try:
            if negociosExGanancialesHasta:
                logger.info(f"Entering Negocios Ex Gananciales Hasta: {negociosExGanancialesHasta}")
                field = driver.find_element(By.ID, "CTLNG_HASFIS")
                human_click(driver, field)
                field.clear()
                human_type(field, negociosExGanancialesHasta)
                logger.info(f"Successfully entered Negocios Ex Gananciales Hasta: {negociosExGanancialesHasta}")

                # Cerrar el campo de fecha si es necesario
                field = driver.find_element(By.ID, "CTLDCM_HASFIS")
                human_click(driver, field)
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Negocios Ex Gananciales Hasta: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # l) Mandato Día
        try:
            if mandatoDia:
                logger.info(f"Entering Mandato Dia: {mandatoDia}")
                field = driver.find_element(By.ID, "CTLDD_PFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                field.send_keys(Keys.BACKSPACE)
                field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatoDia)
                logger.info(f"Successfully entered Mandato Dia: {mandatoDia}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandato Dia: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # m) Mandato Mes
        try:
            if mandatoMes:
                logger.info(f"Entering Mandato Mes: {mandatoMes}")
                field = driver.find_element(By.ID, "CTLMM_PFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                field.send_keys(Keys.BACKSPACE)
                field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatoMes)
                logger.info(f"Successfully entered Mandato Mes: {mandatoMes}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandato Mes: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # n) Mandato Año
        try:
            if mandatoAno:
                logger.info(f"Entering Mandato Ano: {mandatoAno}")
                field = driver.find_element(By.ID, "CTLAA_PFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatoAno)
                logger.info(f"Successfully entered Mandato Ano: {mandatoAno}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandato Ano: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # o) Mandato Desde
        try:
            if mandatoDesde:
                logger.info(f"Entering Mandato Desde: {mandatoDesde}")
                field = driver.find_element(By.ID, "CTLPOD_DESFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, mandatoDesde)
                logger.info(f"Successfully entered Mandato Desde: {mandatoDesde}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandato Desde: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # p) Mandato Hasta
        try:
            if mandatoHasta:
                logger.info(f"Entering Mandato Hasta: {mandatoHasta}")
                field = driver.find_element(By.ID, "CTLPOD_HASFIS")
                human_click(driver, field)
                field.clear()
                human_type(field, mandatoHasta)
                logger.info(f"Successfully entered Mandato Hasta: {mandatoHasta}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Mandato Hasta: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # q) Comercio (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLCOMERCIOFIS")
            logger.info(f"Setting Comercio: {comercio}")
            if comercio:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
            logger.info(f"Successfully set Comercio to: {comercio}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Comercio: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # r) Prendas (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLPRENDASFIS")
            logger.info(f"Setting Prendas: {prendas}")
            if prendas:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
            logger.info(f"Successfully set Prendas to: {prendas}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Prendas: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # s) Rubrica Year
        try:
            if rubricaYear:
                logger.info(f"Entering Rubrica Year: {rubricaYear}")
                field = driver.find_element(By.ID, "CTLANORUBFIS")
                human_click(driver, field)
                field.send_keys(Keys.END)
                for _ in range(4):
                    field.send_keys(Keys.BACKSPACE)
                human_type(field, rubricaYear)
                logger.info(f"Successfully entered Rubrica Year: {rubricaYear}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR entering Rubrica Year: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # t) Rubrica (checkbox)
        try:
            checkbox = driver.find_element(By.NAME, "CTLRUBFIS")
            logger.info(f"Setting Rubrica: {rubrica}")
            if rubrica:
                if not checkbox.is_selected():
                    human_click(driver, checkbox)
            else:
                if checkbox.is_selected():
                    human_click(driver, checkbox)
            logger.info(f"Successfully set Rubrica to: {rubrica}")
        except Exception as e:
            msg = f"[{observation_id}] ERROR setting Rubrica: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # u) Clic en "Agregar"
        try:
            logger.info(f"Clicking 'Agregar' for observation {observation_id}...")
            button = driver.find_element(By.NAME, "BUTTON5")
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

        # Si hubo errores en esta observación, los agrego a submission_errors
        if obs_errors:
            submission_errors.append({
                "observation_id": observation_id,
                "messages": obs_errors
            })

    # 5) Al terminar todas las observaciones
    if submission_errors:
        # Construir resumen de errores de envío
        summary_lines = []
        for entry in submission_errors:
            oid = entry["observation_id"]
            for msg in entry["messages"]:
                summary_lines.append(f"[Obs {oid}] {msg}")
        final_msg = (
            f"Submission errors in fill_form_PF for bucket_id {bucket_id}:\n" 
            + "\n".join(summary_lines)
        )
        logger.error(final_msg)
        notify_error(final_msg)
        return {"status": "submission_error", "errors": submission_errors}

    # Si no hubo errores críticos ni de envío
    logger.info(f"fill_form_PF completed successfully for bucket_id {bucket_id}")
    return {"status": "success", "errors": []}