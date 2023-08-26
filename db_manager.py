import psycopg2


class DBManager:

    def __init__(self, params):
        self.params = params

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        :return:
        """
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT e.company_name, count(v.vacancy_id) FROM employers AS e 
                INNER JOIN vacancies as v
                USING (employer_id)
                GROUP BY employer_id
                ORDER BY e.company_name""")
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_all_vacancies(self):
        """
        получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        :return:
        """
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT v.job_title, e.company_name, v.salary, v.job_url FROM vacancies AS v
                INNER JOIN employers AS e
                USING (employer_id)
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_avg_salary(self):
        """
        получает среднюю зарплату по вакансиям
        :return:
        """
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT CEILING(AVG(salary)) FROM vacancies """)
                rows = cur.fetchall()
        conn.close()
        return rows[0][0]

    def get_vacancies_with_higher_salary(self):
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        :return:
        """
        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT job_title FROM vacancies
                WHERE salary > (SELECT CEILING(AVG(salary)) FROM vacancies)
                """)
                rows = cur.fetchall()
        conn.close()
        return rows

    def get_vacancies_with_keyword(self, keyword):
        """
        получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python
        :return:
        """
        list_keyword = keyword.split()
        str_keyword = '%'.join(list_keyword)

        with psycopg2.connect(**self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT job_title FROM vacancies
                WHERE job_title LIKE %s
                """, ('%'+str_keyword+'%',))
                rows = cur.fetchall()
        conn.close()
        return rows
