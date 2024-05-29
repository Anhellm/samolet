import csv


def save_to_csv(data, file_name: str):
    """
    Запись данных в csv файл.

    :param p1: Набор данных.
    :param p2: Путь к файлу.
    """
    print("Запись данных в csv", file_name)

    try:
        with open(file_name, mode="w", encoding="utf-8") as w_file:
            file_writer = csv.writer(w_file, dialect="excel")
            file_writer.writerows(data)
    except Exception as e:
        print("Ошибка записи в файл:", e)
        raise


def read_from_csv(file_name: str):
    """
    Чтение данных из csv файла.

    :param p1: Путь к файлу.
    :return: Список данных.
    """
    print("Чтение данных из csv", file_name)

    data = []
    try:
        with open(file_name, encoding="utf-8") as r_file:
            file_reader = csv.reader(r_file, dialect="excel")
            for row in file_reader:
                if any(x.strip() for x in row):
                    data.append(row)
    except Exception as e:
        print("Ошибка чтения из файла:", e)
        raise

    return data