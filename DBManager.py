import psycopg2
from typing import Any, Dict, List, Tuple


class DBManager:

    def __init__(self, host: str, database: str, user: str, password: str) -> None:
        """
        инициализатор класса для подключения к БД, автосохранения новой информации
        """
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def create_tables(self) -> None:
        """
        метод для создания двух таблиц с вакансиями и компаниями и организации
        связи между ними
        """

        create_companies_table = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            vacancies_count INT
        )
        """

        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            company_id INT REFERENCES companies(id),
            title VARCHAR(255) NOT NULL,
            salary_from INT,
            salary_to INT,
            currency VARCHAR(10),
            city VARCHAR(255),
            link VARCHAR(255)
        )
        """
        self.cursor.execute(create_companies_table)
        self.cursor.execute(create_vacancies_table)

    def insert_data(self, employers_info: Dict[str, Dict[str, Any]]) -> None:
        """
        метод для вставки данных о вакансиях и работодателях в
        соответствующие таблицы
        """
        for employer_name, employer_info in employers_info.items():
            insert_company_query = """
                    INSERT INTO companies (name, vacancies_count)
                    VALUES (%s, %s)
                    RETURNING id
                    """
            self.cursor.execute(insert_company_query, (employer_name, employer_info['vacancies_count']))
            company_id = self.cursor.fetchone()[0]

            for vacancy in employer_info['vacancies']:
                insert_vacancy_query = """
                INSERT INTO vacancies (company_id, title, salary_from, salary_to, currency, city, link)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                self.cursor.execute(
                    insert_vacancy_query,
                    (company_id, vacancy['title'], vacancy['salary_from'], vacancy['salary_to'],
                     vacancy['currency'], vacancy['city'], vacancy['link'])
                )

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        метод для реализации запроса получения списка компаний и количества вакансий
        """
        query = """
        SELECT name, vacancies_count
        FROM companies
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_all_vacancies(self) -> List[Tuple[str, str, int, int, str, str]]:
        '''
        метод для реализации запроса отображения списка всех вакансий с информацией
        о них
        '''
        query = """
        SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
        FROM vacancies AS v
        JOIN companies AS c ON v.company_id = c.id
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def get_avg_salary(self) -> float:
        """
        метод для реализации запроса о средней зарплате для вакансий, где
        указаны зарплаты
        """
        query = """
        SELECT AVG((salary_from + salary_to) / 2) AS avg_salary
        FROM vacancies
        WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
        """
        self.cursor.execute(query)
        avg_salary = self.cursor.fetchone()[0]
        return avg_salary

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, int, int, str, str]]:
        """
        метод для реализации запроса о вакансиях, у которых зарплата
        выше средней
        """
        avg_salary = self.get_avg_salary()
        query = """
        SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
        FROM vacancies AS v
        JOIN companies AS c ON v.company_id = c.id
        WHERE (v.salary_from + v.salary_to) / 2 > %s
        """
        self.cursor.execute(query, (avg_salary,))
        results = self.cursor.fetchall()
        return results

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, int, int, str, str]]:
        """
        метод для реализации запроса о получение вакансий по ключевому слову
        """
        query = """
        SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
        FROM vacancies AS v
        JOIN companies AS c ON v.company_id = c.id
        WHERE v.title ILIKE %s
        """
        self.cursor.execute(query, (f'%{keyword}%',))
        results = self.cursor.fetchall()
        return results

    def close_connection(self) -> None:
        """
        метол для закрытия подключения к БД и курсору
        """
        self.cursor.close()
        self.conn.close()

