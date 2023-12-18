import os
import subprocess
import time
import schedule

import datetime


def run_script():
    # Создаем список с командой для активации виртуального окружения
    activate_cmd = os.path.join(r'C:\Users\fishzon\PycharmProjects\redmine_google_dock_ python\venv\Scripts',
                                'activate.bat')
    # Указываем путь к скрипту, который нужно запустить и его аргументы (если есть)
    script_path = os.path.join(r"C:\Users\fishzon\PycharmProjects\redmine_google_dock_ python", "main.py")
    # Собираем команду с активацией виртуального окружения и запуском скрипта
    command = f'"{activate_cmd}" && python "{script_path}"'

    try:
        # Запускаем команду в новом процессе и ждем ее завершения
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(e)

    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%d %B %Y %H:%M")

    print("Время парсинга:", formatted_time)


run_script()
schedule.every(15).minutes.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(1)
