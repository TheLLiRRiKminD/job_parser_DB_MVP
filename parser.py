import requests
from typing import Optional, Dict, Any, List, Union


def parse_vacancy(vacancy: Dict[str, Any]) -> Dict[str, Any]:
    """
    функция для получения информации о вакансии с корректным указанием зарплаты
    """
    salary = vacancy.get('salary')

    if salary:
        salary_from = salary.get('from')
        salary_to = salary.get('to')
        currency = salary.get('currency')

        return {
            'title': vacancy['name'],
            'salary_from': salary_from,
            'salary_to': salary_to,
            'currency': currency,
            'city': vacancy.get('area', {}).get('name', 'Не указан'),
            'link': vacancy['alternate_url']
        }
    else:
        return {
            'title': vacancy['name'],
            'salary_from': 0,
            'salary_to': 0,
            'currency': "",
            'city': vacancy.get('area', {}).get('name', 'Не указан'),
            'link': vacancy['alternate_url']
        }


def parse_employer_vacancies(employer_id: int) -> List[Dict[str, Any]]:
    """
    функция для получения списка вакансий от работодателя на основе его id
    """
    vacancies_url = f'https://api.hh.ru/vacancies?employer_id={employer_id}'
    vacancies_response = requests.get(vacancies_url)
    vacancies_data = vacancies_response.json()

    if 'items' in vacancies_data:
        parsed_vacancies = []
        for vacancy in vacancies_data['items']:
            parsed_vacancies.append(parse_vacancy(vacancy))
        return parsed_vacancies
    return []


def parse_employer(employer_name: str) -> Optional[Dict[str, Union[int, List[Dict[str, Any]]]]]:
    """
    функция для получения информации от работадателя на основе его названия
    """
    employer_search_url = f'https://api.hh.ru/employers?text={employer_name}'
    employer_search_response = requests.get(employer_search_url)
    employer_search_data = employer_search_response.json()

    if 'items' in employer_search_data:
        for employer in employer_search_data['items']:
            if employer['name'] == employer_name:
                employer_id = employer['id']
                return {
                    'vacancies_count': len(employer_search_data['items']),
                    'vacancies': parse_employer_vacancies(employer_id)
                }
    return None


def parse_employers_info(target_employers: List[str]) -> Dict[str, Optional[Dict[str, Union[int, List[Dict[str, Any]]]]]]:
    """
    получает информацию о работодателях на основе списка с их названием
    """
    employers_info = {}
    for employer_name in target_employers:
        employer_info = parse_employer(employer_name)
        if employer_info:
            employers_info[employer_name] = employer_info
    return employers_info
