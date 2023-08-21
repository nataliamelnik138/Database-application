import psycopg2


class DBManager:

    @staticmethod
    def get_companies_and_vacancies_count(params):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        :return:
        """
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT e.company_name, count(v.vacancy_id) FROM employers AS e 
                INNER JOIN vacancies as v
                USING (employer_id)
                GROUP BY employer_id
                ORDER BY e.company_name""")
                rows = cur.fetchall()
        conn.close()
        return rows

    @staticmethod
    def get_all_vacancies(params):
        """
        получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        :return:
        """
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT v.job_title, e.company_name, v.salary, v.job_url FROM vacancies AS v
                INNER JOIN employers AS e
                USING (employer_id)
                """)
                rows = cur.fetchall()
                # print("Список всех вакансий:")
                # for row in rows:
                #     print(row)
        conn.close()
        return rows

    @staticmethod
    def get_avg_salary(params):
        """
        получает среднюю зарплату по вакансиям
        :return:
        """
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT CEILING(AVG(salary)) FROM vacancies """)
                rows = cur.fetchall()
                # print("Средняя зарплата по вакансиям:")
                # print(rows[0][0])
        conn.close()
        return rows[0][0]

    @staticmethod
    def get_vacancies_with_higher_salary(params):
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        :return:
        """
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT job_title FROM vacancies
                WHERE salary > (SELECT CEILING(AVG(salary)) FROM vacancies)
                """)
                rows = cur.fetchall()
                # print("Список всех вакансий, у которых зарплата выше средней по всем вакансиям:")
                # for row in rows:
                #     print(row[0])
        conn.close()
        return rows

    @staticmethod
    def get_vacancies_with_keyword(params, keyword):
        """
        получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python
        :return:
        """
        list_keyword = keyword.split()
        str_keyword = '%'.join(list_keyword)

        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT job_title FROM vacancies
                WHERE job_title LIKE %s
                """, ('%'+str_keyword+'%',))
                rows = cur.fetchall()
                # print(f'Список всех вакансий, в названии которых содержится "{keyword}":')
                # for row in rows:
                #     print(row[0])
        conn.close()
        return rows

