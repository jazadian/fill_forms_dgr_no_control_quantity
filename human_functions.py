
import time
import random
from selenium.webdriver.common.action_chains import ActionChains
from notify_error import notify_error


def human_type(element, text, min_delay=0.02, max_delay=0.08):
    """Escribe en un campo como lo haría un humano."""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(min_delay, max_delay))
        
        
def human_click(driver, element):
    """Mueve el mouse hacia un elemento antes de hacer clic, simulando el movimiento humano."""

    try:
        actions = ActionChains(driver)
        actions.move_to_element(element).pause(random.uniform(0.1, 0.3)).click().perform()
        time.sleep(random.uniform(0.1, 0.3))
    except Exception as e:
        notify_error(f"Error haciendo human click:{e}")
        print(f"Error en human_click: {e}")
        
        
def human_select(select_element, visible_text):
    """Selecciona una opción como si fuera un humano (esperando antes de interactuar)."""
    try:
        time.sleep(random.uniform(0.2, 0.4))
        select_element.select_by_visible_text(visible_text)
        time.sleep(random.uniform(0.2, 0.4))
    except Exception as e:
        print(f"Error en human_select: {e}")
        notify_error(f"Error en human_select: {e}")