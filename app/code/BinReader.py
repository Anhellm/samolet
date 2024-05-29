from datetime import datetime


def parse_bin(file_path: str):
    """
    Чтение данных бинарного файла.

    :param p1: Путь к файлу.
    :return: Список данных (номер, номер телефона, дата).
    """
    print("Чтение данных бин файла", file_path)

    data = []
    try:
        with open(file_path, "rb") as file:
            while True:
                try:
                    number = int.from_bytes(file.read(4), byteorder="big")
                    if not number:
                        break

                    phone = file.read(10).decode()

                    year = int.from_bytes(file.read(2), byteorder="little")
                    month = int.from_bytes(file.read(1), byteorder="little")
                    day = int.from_bytes(file.read(1), byteorder="little")
                    hour = int.from_bytes(file.read(1), byteorder="little")
                    minute = int.from_bytes(file.read(1), byteorder="little")
                    second = int.from_bytes(file.read(1), byteorder="little")
                    date = datetime(year, month, day, hour, minute, second)

                    data.append((number, phone, date))
                except Exception as e:
                    print("Ошибка чтения бинарных данных:", e)
                    raise
    except Exception as e:
        print("Ошибка при работе с файлом:", e)
        raise

    print("Получены данные из бин файла", data)
    return data