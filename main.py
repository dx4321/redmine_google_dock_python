from google_connector.google_api import Google
from orv_parser import parse_all_users_for_current_time

from utils.utils import get_config


def runer():
    # Считать конфиг
    config = get_config('config.yaml')
    try:
        # Сохранить данные получение из редмайна
        data = parse_all_users_for_current_time()
        google = Google(config, data)
        google.update_the_data_for_the_current_month()
        google.save_the_data_from_the_urv()
        google.close()
    except Exception as z:
        print(z, z.__traceback__.__str__())


runer()
