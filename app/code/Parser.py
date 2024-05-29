from bs4 import BeautifulSoup
import requests

base_url = "https://alfa.kz/phones/telefony-i-smartfony"


def get_links():
    """
    Получение ссылок на смартфоны.

    :return: Ссылки на объявления.
    """
    links = set()

    try:
        response = requests.get(base_url)
        phones = BeautifulSoup(response.text, "lxml").find_all("div", class_="title")

        for phone in phones:
            links.add(phone.find("a").get("href"))
    except Exception as e:
        print("Ошибка получения ссылок:", e)
        raise

    return links


def get_index_by_title(html_data, value):
    """
    Получение индекса из таблицы по наименованию заголовка.

    :param p1: Html.
    :param p2: Наименование для поиска.
    :return: Индекс.
    """
    names = []
    for line in html_data:
        names.append(line.get("title"))

    return names.index(value)


def get_phones():
    """
    Получение данных по смартфонам.

    :return: Список кортежей (ид, наименование, цена, продавец, ОЗУ, память).
    """
    print("Получение данных по смартфонам")

    phones = []
    for link in get_links():
        try:
            response = requests.get(link)
            bs = BeautifulSoup(response.text, "lxml")

            id = link.split("/")[-1]
            name = bs.find(
                "h1",
                class_="col-12 float-md-right order-md-2 col-sm-6 pull-right title single-product-title",
            ).text.strip()
            price = (
                bs.find("span", class_="__price")
                .find("span", class_="num")
                .text.replace(" ", "")
            )
            seller = (
                bs.find("h4", class_="inline alfa-seller active").find("a").text.strip()
            )

            property_names = bs.findAll("dt", class_="col-sm-3 text-md-right")
            property_values = bs.findAll("dd", class_="col-sm-9 text-md-left")

            try:
                ramIndex = get_index_by_title(property_names, "Оперативная память")
                ram = property_values[ramIndex].text
            except ValueError:
                ram = 0

            try:
                memoryIndex = get_index_by_title(property_names, "Встроенная память")
                memory = property_values[memoryIndex].text
            except ValueError:
                memory = 0

            phones.append((id, name, price, seller, ram, memory))
        except Exception as e:
            print("Ошибка получения данных сайта:", e)
            continue

    print("Получены данные по смартфонам", phones)
    return phones