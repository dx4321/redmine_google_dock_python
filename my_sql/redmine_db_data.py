import re
from datetime import timedelta, datetime
import locale
from typing import List

import pymysql
from dateutil.relativedelta import relativedelta

from utils.utils import Config, get_config


class Redmine:
    def __init__(self, config: Config):
        self.config = config
        self.con = self.get_conn

    @property
    def get_conn(self):
        return pymysql.connect(
            host=self.config.redmine_host,
            user=self.config.redmine_user,
            password=self.config.redmine_password,
            db=self.config.redmine_db
        )

    def time_entries_get_two_arr_for_google(
            self,
            start_date: str = "2023-03-01",
            and_date: str = "2023-04-20"
    ) -> List[List]:
        """
        Получить трудозатраты за период по ролям - Тестировщик, Сектор тестирования ТС

        :arg start_date: дата начала в формате (2023-03-01)
        :arg and_date: дата окончания в формате (2023-03-01)

        """
        # к дню окончания нужно добавить 1 день
        and_date = datetime.strptime(and_date, "%Y-%m-%d")
        new_date = and_date + timedelta(days=1)
        new_end_date = new_date.strftime("%Y-%m-%d")

        with self.con.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT DISTINCT 
                    users.id, 
                    users.lastname, 
                    users.firstname, 
                    time_entries.issue_id, 
                    issues.subject, 
                    time_entries.id,
                    time_entries.hours, 
                    time_entries.comments, 
                    time_entries.created_on, 
                    time_entries.updated_on, 
                    time_entries.spent_on 
                FROM users 
                    JOIN members ON members.user_id = users.id 
                    JOIN member_roles ON member_roles.member_id = members.id 
                    JOIN roles ON roles.id = member_roles.role_id 
                    JOIN projects ON projects.id = members.project_id 
                    JOIN time_entries ON time_entries.user_id = users.id 
                    JOIN issues On time_entries.issue_id = issues.id 
                WHERE roles.name = 'Тестировщик' 
                    AND 
                        (projects.name = 'Сектор тестирования ПО' 
                        OR projects.name = 'Сектор тестирования ТС') 
                    AND time_entries.updated_on >= '{start_date}' AND time_entries.updated_on <= '{new_end_date}';
                """
                # AND time_entries.created_on >= '{start_date}' AND time_entries.created_on <= '{new_end_date}';
            )
            results = cursor.fetchall()

        two_arr_info_for_google_sheet = []

        for res in results:
            # print(res)

            mask = '%d.%m.%Y %H:%M:%S'
            created_on = res[8].strftime(mask)
            updated_on = res[9].strftime(mask)
            spent_on = res[10].strftime('%d.%m.%Y')
            # print(
            #     f"rm_id = {res[0]}, user_name = {res[1]} {res[2]}, task - ,"
            #     f" issue_id = {res[3]}, issue_name = {res[4]}, hours = {res[5]}, comment - {res[6]}"
            #     f"created_on = {created_on}, updated_on = {updated_on}, spent_on = {spent_on}"
            # )
            temp = []
            for i, val in enumerate(res):
                if i == 1:
                    temp.append(f"{res[1]} {res[2]}")
                elif i == 2:
                    pass
                elif i == 8:
                    temp.append(created_on)
                elif i == 9:
                    temp.append(updated_on)
                elif i == 10:
                    temp.append(spent_on)
                else:
                    temp.append(val)

            two_arr_info_for_google_sheet.append(temp)

        for x in two_arr_info_for_google_sheet:
            print(x)

        print(f"\nВсего трудозатрат - {len(results)}")
        # conn.close()
        return two_arr_info_for_google_sheet

    def _parse_reports(self,
                       start_date: str = "2023-04-01"):
        """
        Нужно получить из redmine все поля

            Нужные поля:
            0) По задачам (ИД задачи)
            1) Названия "План на ХХ"
            2) Назначен
            3) Владелец
            4) Исполнитель
            4) дата начала
            5) Срок завершения
            6) Закрыта
            7) Статус
            8) Описание

         пользователей по ролям "Сектор тестирования ТС" и "Тестировщик" за период с начала
         текущего месяца по конец текущего месяца
         """
        with self.con.cursor() as cursor:
            cursor.execute(
                f"""
            SELECT DISTINCT
              issues.id,
              issues.subject,
              trackers.name AS tracker_name,
              
              # 1 Автор
              CONCAT(authors.firstname, ' ', authors.lastname) AS author_name,
              # 2 Назначен
              CONCAT(u.firstname, ' ', u.lastname) AS assigned_to_name,
              # 3 Владелец
              (
                SELECT CONCAT(u2.firstname, ' ', u2.lastname)
                FROM custom_values cv
                  JOIN users u2 ON u2.id = cv.value
                  JOIN custom_fields cf ON cf.id = cv.custom_field_id AND cf.name = 'Владелец задачи'
                WHERE cv.customized_type = 'Issue' AND cv.customized_id = issues.id
              ) AS owner,
              # 4 Исполнитель
              (
                SELECT CONCAT(u2.firstname, ' ', u2.lastname)
                FROM custom_values cv
                  JOIN users u2 ON u2.id = cv.value
                  JOIN custom_fields cf ON cf.id = cv.custom_field_id AND cf.name = 'Исполнитель(и)'
                WHERE cv.customized_type = 'Issue' AND cv.customized_id = issues.id
              ) AS executor_name,
              
              issues.created_on,
              issues.start_date,
              issues.due_date,
              issues.closed_on,
              issue_statuses.name AS status_name,
              issues.description

            FROM issues
              JOIN users authors ON authors.id = issues.author_id
              JOIN trackers ON trackers.id = issues.tracker_id
              JOIN issue_statuses ON issue_statuses.id = issues.status_id 
              JOIN (
                SELECT members.user_id
                FROM members
                  JOIN member_roles ON member_roles.member_id = members.id
                  JOIN roles ON roles.id = member_roles.role_id AND roles.name = 'Тестировщик'
                  JOIN projects ON projects.id = members.project_id AND projects.name IN ('Сектор тестирования ПО', 'Сектор тестирования ТС')
              ) AS testers ON testers.user_id = authors.id
              JOIN users u ON u.id = issues.assigned_to_id
            WHERE issues.start_date >= '{start_date}' 
              AND trackers.name IN ('Еженедельный отчет', 'Отчет за день', 'План на месяц')
                        
                """
            )
            results = cursor.fetchall()

            two_arr_info_for_google_sheet = []
            mask = '%d.%m.%Y %H:%M:%S'

            for res in results:
                issues_id = res[0]
                issues_subject = res[1]
                trackers_name = res[2]

                author = res[3]
                designated = res[4]

                if res[5] and res[6]:
                    owner_or_performer = res[5]
                elif res[5]:
                    owner_or_performer = res[5]
                else:
                    owner_or_performer = res[6]

                if isinstance(owner_or_performer, str):
                    owner_or_performer = re.sub(r"\sВ\.", "", owner_or_performer)

                issues_create_on = f"{res[7].strftime(mask)}"
                issues_start_date = str(res[8].strftime(mask))
                issues_closed = res[9]
                if issues_closed:
                    issues_closed = str(issues_closed.strftime(mask))
                issues_closed_on = res[10]
                if issues_closed_on:
                    issues_closed_on = str(issues_closed_on.strftime(mask))
                issue_statuses_name = res[11]
                issues_description = res[12]

                two_arr_info_for_google_sheet.append(
                    [
                        issues_id,
                        issues_subject,
                        trackers_name,
                        author,
                        designated,
                        owner_or_performer,
                        issues_create_on,
                        issues_start_date,
                        issues_closed,
                        issues_closed_on,
                        issue_statuses_name,
                        issues_description,
                    ]
                )
            print(f"\nВсего трудозатрат - {len(results)}")
            # conn.close()
            return two_arr_info_for_google_sheet

    def _get_the_plan_of_the_month(self, date_of_the_month: str):
        """

        :param date_of_the_month: Дата текущего месяца, пример - Май 2023
        :return: List[List]
        """
        with self.con.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT  
                  issues.id, 
                  issues.subject, 
                  trackers.name, 
                  assigned_to.id as assigned_to_id, 
                  assigned_to.lastname as assigned_to_lastname, 
                  assigned_to.firstname as assigned_to_firstname, 
                  authors.id as author_id, 
                  authors.lastname as author_lastname, 
                  authors.firstname as author_firstname, 
                  issues.created_on, 
                  issues.start_date, 
                  issues.closed_on, 
                  issue_statuses.name, 
                  issues.description,
                  custom_values.value
                  
                FROM users as authors
                    JOIN members as author_members ON author_members.user_id = authors.id
                    JOIN member_roles as author_member_roles ON author_member_roles.member_id = author_members.id
                    JOIN roles as author_roles ON author_roles.id = author_member_roles.role_id
                    JOIN projects as author_projects ON author_projects.id = author_members.project_id
                    JOIN issues ON issues.author_id = authors.id
                    JOIN users as assigned_to ON assigned_to.id = issues.assigned_to_id
                    JOIN trackers ON trackers.id = issues.tracker_id
                    JOIN issue_statuses ON issue_statuses.id = issues.status_id
                    LEFT JOIN issues as sub_issues ON sub_issues.parent_id = issues.id
                    LEFT JOIN custom_values ON custom_values.customized_id = issues.id
                  
                WHERE author_roles.name = 'Тестировщик'  
                  AND (author_projects.name = 'Сектор тестирования ПО' OR author_projects.name = 'Сектор тестирования ТС') 
                  AND (trackers.name = 'Еженедельный отчет' OR trackers.name = 'Отчет за день' OR trackers.name = 'План на месяц') 
                  AND issues.id IN (
                    SELECT customized_id 
                    FROM custom_values 
                    WHERE custom_field_id = (SELECT id FROM custom_fields WHERE name = 'План месяца') 
                    AND value = '{date_of_the_month}'
                  )
                AND custom_values.value = '{date_of_the_month}';
                """
            )
        results = cursor.fetchall()

        two_arr_info_for_google_sheet = []
        mask = '%d.%m.%Y %H:%M:%S'

        for res in results:
            issues_id = res[0]
            issues_subject = res[1]
            trackers_name = res[2]
            assigned = f"{res[4]} {res[5]}"
            author = f"{res[7]} {res[8]}"
            issues_create_on = f"{res[9].strftime(mask)}"
            issues_start_date = str(res[10].strftime(mask))
            issues_closed_on = res[11]
            if issues_closed_on:
                issues_closed_on = str(issues_closed_on.strftime(mask))
            issue_statuses_name = res[12]
            issues_description = res[13]

            two_arr_info_for_google_sheet.append(
                [
                    issues_id,
                    issues_subject,
                    trackers_name,
                    assigned,
                    author,
                    issues_create_on,
                    issues_start_date,
                    issues_closed_on,
                    issue_statuses_name,
                    issues_description,
                ]
            )

        return two_arr_info_for_google_sheet

    def get_all_reports_for_this_month(self):
        """ Получить все отчеты за текущий месяц """

        def convert_format_date(
                date_string: str  # ='01.05.2023 00:00:00'
        ):
            """
            Конвертер формата даты
            Из формата '%d.%m.%Y %H:%M:%S' в '%Y-%m-%d'
            """
            input_format = '%d.%m.%Y %H:%M:%S'
            output_format = '%Y-%m-%d'
            date_object = datetime.strptime(date_string, input_format)
            output_string = date_object.strftime(output_format)
            print(output_string)
            return output_string

        # создаем объект datetime для даты мая 2023 года
        date = datetime.now()
        # устанавливаем локаль для России
        locale.setlocale(locale.LC_TIME, 'ru_RU')
        # форматируем дату в строку с помощью метода strftime
        date_string = date.strftime('%B %Y')
        # получаем срок постановки задачи
        # получить все планы за текущий месяц по кастомному полю план месяца
        arr_month_reports = self._get_the_plan_of_the_month(date_string)
        #
        if len(arr_month_reports) > 0:
            target_date: str = arr_month_reports[0][6]
            print(f"target_date - {target_date}")
            # target_date = date.strftime('%d.%m.%Y %H:%M:%S')
            return self._parse_reports(convert_format_date(target_date))
        else:
            return []


# По create_on раз в день добавлять все что было создано
# Данные добавлять по названию листа (месяц) закидывать по полю created_on

if __name__ == '__main__':
    con = get_config("../config.yaml")
    # Redmine(con).time_entries_get_two_arr_for_google()
    # Redmine(con).parse_reports()
    x = Redmine(con).get_all_reports_for_this_month()
    for z in x:
        print(x)
