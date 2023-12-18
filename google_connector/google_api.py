import base64
import datetime
from typing import List, Optional, Dict

import socks
import socket
import gspread
from dateutil.relativedelta import relativedelta
from gspread import Worksheet
from oauth2client.service_account import ServiceAccountCredentials

from my_sql.redmine_db_data import Redmine
from orv_parser import OrvUser, parse_all_users_for_current_time
from utils.utils import Config, get_config


class Google:
    def __init__(self, con: Config, data_by_date):
        self.data_by_date = data_by_date
        self.config = con

        # настройки прокси-сервера
        if self.config.proxy:
            # установка соединения с прокси сервером
            socks.setdefaultproxy(
                socks.PROXY_TYPE_HTTP,
                self.config.proxy.proxy_host,
                self.config.proxy.proxy_port,
                True,
                self.config.proxy.proxy_user,
                self.config.proxy.proxy_pass
            )
            self.socket = socket
            self.socket.socket = socks.socksocket

        # учетные данные для доступа к Google API
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            con.oauth2client_service_account_file,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])

        # подключение к таблице Google Sheets
        self.client = gspread.authorize(creds)

        self.url = con.google_url
        self.con_url = self.google_con_url

    def close(self):
        socks.setdefaultproxy()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    @staticmethod
    def get_current_data():
        """ Получить текущую дату в формате 18.04.2023 """
        return datetime.date.today()

    def convert_datetime_for_google_format(self):
        """ Конвертировать текущую дату в гугл формат """
        return self.get_current_data().strftime('%Y.%m.%d')

    @property
    def google_con_url(self):
        """ Получить подключение к таблице по url """
        return self.client.open_by_url(self.url)

    def save_rm_time_entries_parse_data(self, sheet: str, values: List[List]):
        """ Сохранить собранные трудозатраты """

        # Получаем последнюю пустую строку
        sheet = self.con_url.worksheet(sheet)
        start_cell = sheet.cell(3, 1).address
        try:
            end_cell = sheet.cell(len(values) + 3, len(values[0])).address
        except IndexError:
            # Если нет значений, то ничего не делать
            return
        range_update = f'{start_cell}:{end_cell}'
        print(range_update)
        sheet.update(range_update, values, value_input_option='USER_ENTERED')

    def save_reports_parse_data(self, sheet: str, values: List[List]):
        """ Сохранить отчеты """

        sheet = self.con_url.worksheet(sheet)
        start_cell = "DH3"
        len_row = len(values)
        print(f"всего строк - {len_row}")
        end_cell = f"DS{(3 + len_row)}"
        range_update = f'{start_cell}:{end_cell}'
        sheet.update(range_update, values)

    def save_urv_parse_data(self, sheet: str, values: List[List]):
        """ Сохранить собранные данные в конец файла """

        # Получаем последнюю пустую строку
        sheet = self.con_url.worksheet(sheet)

        start_cell = sheet.cell(row=1 + 2, col=1 + 14).address
        end_cell = sheet.cell(len(values) + 1 + 2, len(values[0]) + 1 + 14).address
        range_update = f'{start_cell}:{end_cell}'
        sheet.update(range_update, values, value_input_option='USER_ENTERED')

    def update_the_data_for_the_current_month(self):
        """ Обновить данные за текущий месяц """

        def get_previous_month():
            today = datetime.date.today()  # получаем сегодняшнюю дату
            first_day = today.replace(day=1)  # получаем первый день текущего месяца
            last_month = first_day - datetime.timedelta(
                days=1)  # получаем день, предшествующий первому дню текущего месяца
            return last_month

        def copy_format(source_ws: Worksheet, destination_ws: Worksheet):
            body = {
                "requests": [
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": source_ws.id,
                                "startRowIndex": 0,
                                "endRowIndex": 2,
                                "startColumnIndex": 0,
                                "endColumnIndex": source_ws.col_count
                            },
                            "destination": {
                                "sheetId": destination_ws.id,
                                "startRowIndex": 0,
                                "endRowIndex": 2,
                                "startColumnIndex": 0,
                                "endColumnIndex": source_ws.col_count
                            },
                            "pasteType": "PASTE_NORMAL",
                            "pasteOrientation": "NORMAL"
                        }
                    },
                ]
            }
            res = self.google_con_url.batch_update(body)
            return res

        current_date = self.get_current_data().strftime('%m.%Y')
        list_sheets = self.con_url.worksheets()
        previous_month = get_previous_month()

        if current_date in [_list.title for _list in list_sheets]:
            print("лист с нужной датой есть " + current_date)
        else:
            print("листа с нужной датой нет " + current_date)
            # создать лист с нужной датой
            self.con_url.add_worksheet(current_date, 2500, 124)

            # выбрать лист и скопировать первые 2 строки
            source_ws = self.con_url.worksheet(previous_month.strftime('%m.%Y'))
            target_ws = self.con_url.worksheet(current_date)
            copy_format(source_ws, target_ws)
        # Нужно сделать так что бы данные за текущий месяц брались с листа, затем брались с рм, и добавлялись только
        # новые данные за новый месяц по полю create_on

        rm_data = self.get_update_data_rm_for_current_month()
        self.save_rm_time_entries_parse_data(current_date, rm_data)

        rm = Redmine(self.config)
        reports_data = rm.get_all_reports_for_this_month()
        if len(reports_data) > 0:
            self.save_reports_parse_data(current_date, reports_data)

        sheet = self.con_url.worksheet(current_date)
        sheet.update('L1', f"Время последнего обновления данных")
        sheet.update('L2',
                     datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S'), value_input_option='USER_ENTERED')

    def save_the_data_from_the_urv(self):
        """ Сохранить полученные данные из урв  """

        def data_converter(days: Dict[str, List[Optional[OrvUser]]]):
            """ """
            # конвертер из парсера в гугл
            t = []
            # в таблице первые 3 столбца id_dep, id_user, bio

            # заполнить первые 3 столбца
            for date in days:
                if len(days[date]) > 0:
                    # заполнить шапку
                    for user in days[date]:
                        line = [user.id_dep, user.orv_id, user.bio]

                        # заполнить по каждому пользователю все даты - за 1 дату 3 столбца (дата_прих, дата_ух, итого)
                        for d in days:
                            # если за дату никто не пришел, то добавить в строку пусто за нужную дату
                            if len(days[d]) == 0:
                                cells = ['-', '-', '-']
                                line.extend(cells)
                            else:
                                for u in days[d]:
                                    if u.bio == user.bio:
                                        cells = [u.arrival_time, u.leaving_time, u.parse_date]
                                        line.extend(cells)

                        t.append(line)
                    break

            return t

        # Получить спарсенные данные

        # Конвертировать в двумерный массив
        table: List[List] = data_converter(self.data_by_date)

        current_date = datetime.datetime.now().strftime('%m.%Y')
        list_sheets = self.con_url.worksheets()

        if current_date in [_list.title for _list in list_sheets]:
            print("лист с нужной датой есть")
        else:
            print("листа с нужной датой нет")
            # создать лист с нужной датой
            self.con_url.add_worksheet(current_date, 4000, 150)

        self.save_urv_parse_data(current_date, table)

    def delete_data_from_list(self, sheet_name: str):
        """ Удалить данные с листа """

        sheet = self.con_url.worksheet(sheet_name)

        max_rows = sheet.row_count - 1
        sheet.delete_rows(2, max_rows)

    def get_update_data_rm_for_current_month(self) -> List[List]:
        """ Получить дату за текущий месяц в формате рм и получить данные из рм по полю time_entries.created_on """

        def last_day_of_month(date):
            """ Возвращает последний день месяца этой даты """
            if date.month == 12:
                return date.replace(day=31)
            return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)

        today = self.get_current_data()  # .strftime('%Y-%m-%d')

        # Получаем первый день текущего месяца
        first_day = today.replace(day=1).strftime('%Y-%m-%d')

        # Получаем последний день текущего месяца
        last_day = str(last_day_of_month(today))

        rm = Redmine(self.config)
        return rm.time_entries_get_two_arr_for_google(first_day, last_day)


if __name__ == '__main__':
    config = get_config('../config.yaml')
    config.oauth2client_service_account_file = '../google_key.json'

    google = Google(config, [1, 2])

    # Сохранить данные получение из редмайна
    google.update_the_data_for_the_current_month()

    # Сохранить данные из urv
    # google.save_urv_data_for_current_day_and_current_date()

    # google.test()
