import asyncio
import calendar
import logging
import re
import datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup as Bs
from pydantic import BaseModel


# pip install lxml

# нужно по датам
#   у даты список всех пользователей
#       у пользователя время прихода и ухода

class OrvUser(BaseModel):
    orv_id: int
    id_dep: int
    bio: str
    parse_date: str
    arrival_time: str
    leaving_time: str


class OrvParser:
    def __init__(self, departaments_id: List[int]):
        """
        :param departament_id: id департамента
        """
        self.first_day = None
        self.last_day = None
        self.format_date = '%d.%m.%Y'  # формат даты времени urv
        self.get_update_data_rm_for_current_month()
        self.departaments_id = departaments_id
        self.today = (datetime.datetime.now()).strftime("%d.%m.%Y")
        self.list_orv_users: List[OrvUser] = []
        self.session = requests.Session()
        self.visiting_days_dictionary = {}  # словарь дней посещений

    def get_update_data_rm_for_current_month(self, format_date: bool = True):
        """ Получить дату за текущий месяц в формате рм и получить данные из рм по полю time_entries.created_on """

        def last_day_of_month(date):
            if date.month == 12:
                return date.replace(day=31)
            if format_date:
                return (date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)).strftime(format_date)
            else:
                return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)

        today = datetime.datetime.now()

        format_date = '%d.%m.%Y'
        # Получаем первый день текущего месяца
        if format_date:
            first_day = today.replace(day=1).strftime(format_date)
        else:
            first_day = today.replace(day=1)

        # Получаем последний день текущего месяца
        last_day = last_day_of_month(today)
        self.first_day = first_day
        self.last_day = last_day
        return first_day, last_day

    async def save_info_by_member(
            self,
            departament_id,
            orv_user_id: str,
            member: str,
            start_date,
            end_date,
    ):
        """
        Для каждого отдельного пользователя, получить его время прихода и время ухода за период,
        сохранить всю информацию о пользователе в list_orv_users

        :param member:
        :param end_date:
        :param start_date:

        :arg orv_user_id: - id юзера orv
        :return : время прихода, время ухода
        """

        # дата окончания месяца
        req_get_user_date_time = f"http://portal.bolid.ru/urv/person/?persrn={orv_user_id}&date_b={start_date}&date_e={end_date}&isNaked=1"

        orv_get_data_html = requests.get(req_get_user_date_time)
        _soup = Bs(orv_get_data_html.content, "lxml")

        # находим все tr в tbody
        table = _soup.find('table')

        try:
            tds = table.find_all("tr")
        except Exception as ex:
            return ex

        info = tds[2:-1]  # Выборка всех значений кроме первых 2-х и последней

        await asyncio.sleep(0)

        def split_time_str(time_str):
            """
            Разъединить слитую строку со временем
            Пример:
                time_str = '08:5017:2200:0306:4510:0012:3015:0019:4821:30'
                на выходе 08:50 17:22 00:03 06:45 10:00 12:30 15:00 19:48 21:30
             """
            result = ''
            for i in range(0, len(time_str), 5):
                part = time_str[i:i + 5]
                result += part[:2] + part[2:] + ' '
            return result[:-1]

        for td in info:
            num = td.find('td', {'class': 'urv_num'}).text

            pers_date = td.find('td', {'class': 'urv_pers_date'}).text
            use_date1 = split_time_str(td.find_all('td', {'class': 'urv_pers_cell'})[0].text)
            use_date2 = split_time_str(td.find_all('td', {'class': 'urv_pers_cell'})[1].text)
            itogo = td.find('td', {'class': 'urv_itogo'}).text

            # print(f"{orv_user_id}, {member}, {num}, {pers_date}, {use_date1}, {use_date2}, {itogo}")
            # user = OrvU(
            #     id_dep=self.departament_id,
            #             orv_id=orv_user_id,
            #             bio=member,
            # )

            self.list_orv_users.append(
                OrvUser(
                    id_dep=departament_id,
                    orv_id=orv_user_id,
                    bio=member,
                    parse_date=pers_date,
                    arrival_time=use_date1,
                    leaving_time=use_date2,
                )
            )

    def get_two_lists_users_bio_and_users_ids(self, departament_id, date_per_day: str):
        """
        Получить информацию о id пользователей и о фио пользователей

        :param date_per_day: Дата в формате - "%d.%m.%Y"
        :param departament_id: айди отдела
        :return : Вернет два списка - ФИО пользователей и их айди
        """

        # Сектор тестирования
        orw_html = self.session.get(
            f"http://portal.bolid.ru/urv/?date_b={date_per_day}&date_e={date_per_day}&deptrn={departament_id}"
        )
        soup = Bs(orw_html.content, "lxml")

        # Найти весь код с пользователями (ФИО)
        members = soup.find_all("td", {"class": "urv_pers"})

        # Найти весь код с айди пользователей, он есть только в таблице
        table = soup.findAll('table')[3].findAll("td", {"class": "urv_cell"})

        list_members_info = []
        list_users_id = []

        for member, row in zip(members, table):
            """ Получить 2-а списка, - список пользователей и список их ай-ди """

            user_id = re.sub(r"(.+?)(\d+$)", r"\2", row['id'])
            list_members_info.append(member.text)
            list_users_id.append(user_id)

        return list_members_info, list_users_id

    def get_a_list_of_all_days_in_the_month(self):
        """ Получить список всех дней в месяце """

        def get_month_days(_year: int, _month: int):
            """ Получение списка всех дней месяца """
            num_days = calendar.monthrange(year, month)[1]
            return [datetime.date(_year, _month, day) for day in range(1, num_days + 1)]

        today = datetime.datetime.now()
        year = today.year
        month = today.month

        # Получаем список всех дней текущего месяца
        days = get_month_days(year, month)
        # Вернуть отформатированные дни
        return [day.strftime(self.format_date) for day in days]

    async def parse_data(self):
        """ Асинхронный парсер данных, получает информацию о пользователях и асинхронно парсит данные """

        days_in_the_current_month = self.get_a_list_of_all_days_in_the_month()
        # текущая дата

        # Получаем первый день текущего месяца
        present_day = days_in_the_current_month[0]
        employees = {}  # словарь сотрудников id:bio
        tasks = []

        # Получить всех сотрудников по департаментам
        for departament_id in self.departaments_id:
            # Получить информацию о коллегах в урв ид и фио
            list_members_info, list_users_id = self.get_two_lists_users_bio_and_users_ids(departament_id, self.today)

            for member, user_id in zip(list_members_info, list_users_id):
                employees[user_id] = [member, departament_id]

                # Пройтись по всем дням в месяце, получить информацию о коллегах за текущий месяц
                tasks.append(
                    self.save_info_by_member(
                        departament_id,
                        user_id,
                        member,
                        days_in_the_current_month[0],
                        days_in_the_current_month[-1])
                )
        # Запустить получение по всем сотрудникам
        await asyncio.gather(*tasks)

        # Заполнить словарь с датами за все месяца полученными данными
        data_by_date = {}
        for date in days_in_the_current_month:
            users_for_date = []
            for user in self.list_orv_users:
                if user.parse_date == date:
                    users_for_date.append(user)
            data_by_date[date] = users_for_date

        # Сортировать список пользователей по id
        sorted_users = sorted(employees.items(), key=lambda x: x[1])
        employees = {user[0]: user[1] for user in sorted_users}

        def sort_orv_users(
                orv_users: List[OrvUser],
                data,
                employees_info) -> List[OrvUser]:
            """  """

            def find_employee_info_by_name(name: str) -> tuple:
                for key, value in employees_info.items():
                    if value[0] == name:
                        # id_user, id_dep
                        return key, value[1]
                return None, None

            # сколько ключей пользователей за сегодня
            orv_ids = list(_user.orv_id for _user in orv_users)

            # всего ключей пользователей
            all_info_list_id = list(employees_info.keys())
            all_info_list_id.sort()

            for _id in all_info_list_id:  # пройти по всем id большего списка
                if int(_id) not in orv_ids:
                    id_user, id_dep, = find_employee_info_by_name(employees_info[_id][0])
                    orv_user = OrvUser(orv_id=id_user, id_dep=id_dep, bio=employees_info[_id][0],
                                       parse_date=data, arrival_time="", leaving_time="")
                    orv_users.append(orv_user)

            filtered_users = sorted(orv_users, key=lambda u: u.bio)
            #############################
            return filtered_users

        # Проверить что за дату указаны все сотрудники, если в день никто не пришел ни делать ничего
        # list_sorted_users = [user_info[0] for user_info in list(employees.values())]
        for day in data_by_date:
            # Если за день пришли сотрудники, то
            if len(data_by_date[day]) > 0:
                data_by_date[day] = sort_orv_users(data_by_date[day], day, employees)

        return data_by_date

    @staticmethod
    def sort_orv_users(orv_users: List[OrvUser], employee_list: List[str]) -> List[OrvUser]:
        sorted_orv_users = []

        # Создаем словарь сотрудников из списка сотрудников
        employee_dict = {}
        for employee in employee_list:
            employee_dict[employee] = None

        # Добавляем оставшихся сотрудников из списка сотрудников в отсортированный список
        for employee in employee_dict:
            sorted_orv_users.append(
                OrvUser(orv_id=0, id_dep=0, bio=employee, parse_date='', arrival_time='', leaving_time=''))

        # Сортировка списка объектов OrvUser
        for orv_user in orv_users:
            # Если сотрудник из списка сотрудников найден в списке объектов OrvUser, добавляем его в отсортированный
            # список
            if orv_user.bio in employee_dict:
                sorted_orv_users.append(orv_user)
                # Удаляем сотрудника из словаря, чтобы не дублировать
                del employee_dict[orv_user.bio]

        return sorted_orv_users

    def run(self):
        """ Запустить парсер """

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        data_by_date = loop.run_until_complete(self.parse_data())

        return data_by_date


def parse_all_users_for_current_time():
    """
    Получить всех пользователей с датой и временем с урв за отделы
    "Сектор тестирования ПО", "Сектор тестирования ТС"

    """
    # сектор тестирования ПО
    sector_testing_po_id_dep = 1286219861
    sector_testing_ts_id_dep = 1685919598
    belov = 468907074

    orv_parser_po = OrvParser([sector_testing_po_id_dep, sector_testing_ts_id_dep, belov])
    data_by_date: Dict[str, List[Optional[OrvUser]]] = orv_parser_po.run()
    print("Орв получено")

    # Вывод таблицы
    # for day in data_by_date:
    #     print(day)
    #     for u in data_by_date[day]:
    #         print(u)

    return data_by_date


# Нужно реализовать механизм напоминания приложить пропуск
if __name__ == '__main__':
    parse_all_users_for_current_time()
