from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys

import logging
import time
import random

from human_functions import human_click, human_type, human_select
from notify_error import notify_error

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
    

def fill_form_I(bucket_id, driver, client):
    """
    Rellena el formulario de Inmuebles en DGR para cada observación del bucket_id.
    Devuelve un dict con:
      - "status": "success", "critical_error" o "submission_error"
      - "errors": lista de mensajes en caso de fallo
    """
    critical_errors = []
    submission_errors = []
    logger.info(f"Starting Inmuebles for Bucket ID: {bucket_id}")
    
    try:
        logger.info("Connecting to MongoDB...")
        db = client['production']
        imuebles_db = db['inmuebles']
        logger.info("Connected to MongoDB.")
    except Exception as e:
        msg = f"Critical: Error connecting to MongoDB for bucket_id {bucket_id}: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}


    # 1) Conexión a MongoDB y fetch de observaciones (crítico si falla)
    try:
        logger.info(f"Retrieving observations for bucket_id: {bucket_id}...")
        # Defino el filtro
        filtro = {"id_bucket": bucket_id}
        # 1) Recupero el cursor para iterar (si es que vas a procesar cada documento)
        observaciones = imuebles_db.find(filtro)
        total = imuebles_db.count_documents(filtro)
        logger.info(f"Retrieved {total} observations for bucket_id: {bucket_id}.")
    except Exception as e:
        msg = f"Critical: Error retrieving observations from MongoDB for bucket_id {bucket_id}: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # 2) Navegar a la página de Inmuebles (crítico si falla)
    try:
        driver.get("https://www.dgr.gub.uy/etimbreapp/servlet/hformauxremotas?4")
        logger.info("Navegado al formulario Inmuebles.")
    except Exception as e:
        msg = f"Critical: al cargar página Inmuebles: {e}"
        logger.error(msg)
        notify_error(msg)
        critical_errors.append(msg)
        return {"status": "critical_error", "errors": critical_errors}

    # Mapas para IDs dinámicos de padrones y localidades
    padron_map = {
        7: "CTLPADRONINM22",
        8: "CTLPADRONINM7",
        9: "CTLPADRONINM8",
        10: "CTLPADRONINM9",
        11: "CTLPADRONINM10",
        12: "CTLPADRONINM11"
    }
    loc_map = {
        6: "CTLLOCINM12",
        7: "CTLLOCINM6",
        8: "CTLLOCINM7",
        9: "CTLLOCINM8",
        10: "CTLLOCINM13",
        11: "CTLLOCINM10",
        12: "CTLLOCINM11",
    }

    # 3) Iterar cada observación y completar campos
    for obs in observaciones:
        obs_id = obs.get("_id", "desconocido")
        obs_errors = []
        logger.info(f"Procesando observación {obs_id}…")

        # a) Departamento
        try:
            dep = obs.get("departamento", "").strip()
            if dep:
                dep = dep.upper()  # Aseguramos que esté en mayúsculas
                logger.info(f"  Seleccionando Departamento: {dep}")
                sel = Select(WebDriverWait(driver, 30)
                             .until(EC.element_to_be_clickable((By.NAME, "_DEPINM"))))
                human_select(sel, dep)
                logger.info(f"Se llenó Departamento: {dep}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Departamento: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # b) Localidad
        try:
            loc = obs.get("localidad", "").strip()
            if loc:
                loc = loc.upper()
                logger.info(f"  Seleccionando Localidad: {loc}")
                sel = Select(WebDriverWait(driver, 10)
                             .until(EC.element_to_be_clickable((By.NAME, "CTLLOCINM2"))))
                human_select(sel, loc)
                logger.info(f"Se llenó Localidad: {loc}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Localidad: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # c) Padrón Actual
        try:
            pad = obs.get("padronActual", "").strip()
            if pad:
                logger.info(f"  Ingresando Padrón Actual: {pad}")

                # 1) Esperar a que el campo esté visible y clickeable
                fld_pad = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "_PADONINMAUX"))
                )

                # 2) Asegurarse de que esté en el viewport
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", fld_pad)

                # 3) (Opcional) Limpiar contenido previo
                try:
                    fld_pad.clear()
                except Exception:
                    pass
                time.sleep(0.1)

                # 4) Tipear el número
                human_type(fld_pad, pad)
                logger.info(f"Se llenó Padrón Actual: {pad}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Padrón Actual: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # d) Sección Judicial
        try:
            sec = obs.get("seccionJudicial", "").strip()
            if sec:
                logger.info(f"  Ingresando Sección Judicial: {sec}")
                fld = driver.find_element(By.ID, "CTLSJINM")
                human_type(fld, sec)
                logger.info(f"Se llenó Sección Judicial: {sec}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Sección Judicial: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # e) Block
        try:
            blk = obs.get("block", "").strip()
            if blk:
                logger.info(f"  Ingresando Block: {blk}")
                fld = driver.find_element(By.ID, "CTLBLOCKINM2")
                human_type(fld, blk)
                logger.info(f"Se llenó Block: {blk}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Block: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # f) Nivel
        try:
            niv = obs.get("nivel", "").strip()
            if niv:
                logger.info(f"  Seleccionando Nivel: {niv}")
                sel = Select(driver.find_element(By.NAME, "CTLNIVELINM2"))
                human_select(sel, niv)
                logger.info(f"Se llenó Nivel: {niv}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Nivel: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # g) Unidad
        try:
            uni = obs.get("unidad", "").strip()
            if uni:
                logger.info(f"  Ingresando Unidad: {uni}")
                fld = driver.find_element(By.ID, "CTLUNIDADINM2")
                human_type(fld, uni)
                logger.info(f"Se llenó Unidad: {uni}")
        except Exception as e:
            msg = f"[{obs_id}] ERROR Unidad: {e}"
            logger.error(msg)
            obs_errors.append(msg)

        # 1) Detectar índices existentes de padrones anteriores (i = 1..10 → HTML idx = i+2)
        indices_existentes = []
        for i in range(1, 11):
            key_pad = f"padronAnterior{i}"
            key_loc = f"localidadAnterior{i}"
            val_pad = obs.get(key_pad, "").strip()
            val_loc = obs.get(key_loc, "").strip()
            if val_pad or val_loc:
                indices_existentes.append(i + 2)

        # 2) Rellenar para idx = 3 y 4 si existen
        for idx in [3, 4]:
            if idx not in indices_existentes:
                continue
            i = idx - 2
            valor_pad = obs.get(f"padronAnterior{i}", "").strip()
            valor_loc = obs.get(f"localidadAnterior{i}", "").strip().upper()

            # a) Ingresar Padrón idx
            if valor_pad:
                try:
                    logger.info(f"  Ingresando PadrónAnterior{i} (HTML idx={idx}): {valor_pad}")
                    fld_pad_id = f"CTLPADRONINM{idx}"
                    fld_pad = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.ID, fld_pad_id))
                    )
                    human_type(fld_pad, valor_pad)
                    human_click(driver, driver.find_element(By.ID, "tab4"))
                    time.sleep(0.2)
                    logger.info(f"Se llenó PadrónAnterior{i} (ID={fld_pad_id}): {valor_pad}")
                except Exception as e:
                    msg = f"[{obs_id}] ERROR PadrónAnterior{i} (ID={fld_pad_id}): {e}"
                    logger.error(msg)
                    obs_errors.append(msg)

            # b) Seleccionar Localidad idx
            if valor_loc:
                try:
                    valor_loc = valor_loc.upper()  # Aseguramos que esté en mayúsculas
                    logger.info(f"  Seleccionando localidadAnterior{i} (HTML idx={idx}): {valor_loc}")
                    sel_name = f"CTLLOCINM{idx}"
                    sel_loc = Select(WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.NAME, sel_name))
                    ))
                    human_select(sel_loc, valor_loc)
                    human_click(driver, driver.find_element(By.ID, "tab4"))
                    time.sleep(0.2)
                    logger.info(f"Se llenó localidadAnterior{i} (NAME={sel_name}): {valor_loc}")
                except Exception as e:
                    msg = f"[{obs_id}] ERROR localidadAnterior{i} (NAME={sel_name}): {e}"
                    logger.error(msg)
                    obs_errors.append(msg)

        # 3) Para cada índice ≥ 5
        for idx in indices_existentes:
            if idx <= 4:
                continue

            boton_id = f"CargarOtro{idx - 2}"
            try:
                human_click(driver, WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, boton_id))
                ))
                logger.info(f"  Clic en '{boton_id}' (HTML idx={idx}).")
                time.sleep(0.5)
            except Exception as e:
                msg = f"[{obs_id}] WARNING No pude clicar '{boton_id}' (fila {idx}): {e}"
                logger.warning(msg)
                obs_errors.append(msg)

            i = idx - 2
            valor_pad = obs.get(f"padronAnterior{i}", "").strip()
            valor_loc = obs.get(f"localidadAnterior{i}", "").strip()

            # a) Ingresar Padrón idx
            if valor_pad:
                try:
                    real_padron_id = padron_map.get(idx, f"CTLPADRONINM{idx}")
                    logger.info(f"  Ingresando PadrónAnterior{i} (HTML idx={idx}, ID={real_padron_id}): {valor_pad}")
                    fld_pad = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.ID, real_padron_id))
                    )
                    human_click(driver, fld_pad)
                    fld_pad.send_keys(Keys.END)
                    fld_pad.send_keys(Keys.BACKSPACE)
                    human_type(fld_pad, valor_pad)
                    human_click(driver, driver.find_element(By.ID, "tab4"))
                    time.sleep(random.uniform(0.2, 0.4))
                    logger.info(f"Se llenó PadrónAnterior{i} (ID={real_padron_id}): {valor_pad}")
                except Exception as e:
                    msg = f"[{obs_id}] ERROR PadrónAnterior{i} (ID={real_padron_id}): {e}"
                    logger.error(msg)
                    obs_errors.append(msg)

            # b) Seleccionar Localidad idx
            if valor_loc:
                try:
                    valor_loc = valor_loc.upper()  # Aseguramos que esté en mayúsculas
                    real_loc_name = loc_map.get(idx, f"CTLLOCINM{idx}")
                    logger.info(f"  Seleccionando localidadAnterior{i} (HTML idx={idx}, NAME={real_loc_name}): {valor_loc}")
                    sel_loc = Select(WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.NAME, real_loc_name))
                    ))
                    human_select(sel_loc, valor_loc)
                    human_click(driver, driver.find_element(By.ID, "tab4"))
                    time.sleep(0.2)
                    logger.info(f"Se llenó localidadAnterior{i} (NAME={real_loc_name}): {valor_loc}")
                except Exception as e:
                    msg = f"[{obs_id}] ERROR localidadAnterior{i} (NAME={real_loc_name}): {e}"
                    logger.error(msg)
                    obs_errors.append(msg)

        # h) Clic en "Agregar"
        try:
            logger.info(f"  Haciendo clic en 'Agregar' para {obs_id}")
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.NAME, "BUTTON3"))
            )
            human_click(driver, btn)
            logger.info(f"Observación {obs_id} enviada correctamente.")
        except Exception as e:
            msg = f"[{obs_id}] ERROR al enviar observación: {e}"
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
             
        except TimeoutException:
            # No apareció ningún span.ErrorViewer en esos 5 segundos: seguimos normalmente.
            logger.info(f"No apareció ErrorViewer tras 'Agregar' para bucket_id {bucket_id}. Continuo con el flujo.")

        # Si hubo errores en esta observación, los agrego a la lista de submission_errors
        if obs_errors:
            submission_errors.append({
                "observation_id": obs_id,
                "messages": obs_errors
            })

    # 4) Al finalizar todas las observaciones
    if submission_errors:
        # Armar resumen de errores de envío
        error_summary_lines = []
        for entry in submission_errors:
            oid = entry["observation_id"]
            for msg in entry["messages"]:
                error_summary_lines.append(f"[Obs {oid}] {msg}")

        final_msg = (
            f"Submission errors in fill_form_I for bucket_id {bucket_id}:\n"
            + "\n".join(error_summary_lines)
        )
        logger.error(final_msg)
        notify_error(final_msg)
        return {"status": "submission_error", "errors": submission_errors}
    
    try:
        nro_inmuebles_field = driver.find_element(By.ID, "_NROINM")
        nro_inmuebles_value = nro_inmuebles_field.get_attribute("value")
        logger.info(f"Automotores ingresados segun DGR: {nro_inmuebles_value}")
        
    except Exception as e:
        msg = f"Error al verificar el campo Nro de Inmuebles Solicitados: {str(e)}"
        logger.error(msg)
        notify_error(f"bucket_id={bucket_id} → {msg}")
        return {
            "status": "submission_error",
            "errors": msg
        }

    # Si llegamos aquí, fue exitoso
    logger.info(f"fill_form_I completed successfully for bucket_id {bucket_id}")
    return {"status": "success", "errors": []}