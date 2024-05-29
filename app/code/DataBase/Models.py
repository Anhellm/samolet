from sqlalchemy import Column, Integer, DateTime, String
from datetime import datetime
from DataBase.Engine import DataBase, Base


class Abonent(Base):
    """
    Класс описания абонента черного списка.
    """

    __tablename__ = "blacklist"

    id = Column(Integer, primary_key=True)
    number = Column(String, index=True)
    date = Column(DateTime)

    @staticmethod
    def add_or_update(data):
        """
        Добавление/обновление абонента в БД.

        :param p1: Набор данных.
        """
        print("Внесение данных абонентов", data)

        try:
            with DataBase.Session() as db:
                query = db.query(Abonent)

                for abonentData in data:
                    abonentId = int(abonentData[0])

                    abonent = query.filter(Abonent.id == abonentId).first()
                    if abonent is None:
                        abonent = Abonent(
                            id=abonentId,
                        )

                    abonent.number = abonentData[1]
                    abonent.date = datetime.strptime(
                        abonentData[2], "%Y-%m-%d %H:%M:%S"
                    )

                    db.add(abonent)

                db.commit()
        except Exception as e:
            print("Ошибка добавления данных:", e)
            raise

    @staticmethod
    def is_in_blacklist(ctn):
        """
        Проверить наличие абонента в черном списке.

        :param p1: Номер телефона.
        :return: true/false.
        """
        print("Проверка наличия в черном списке", ctn)
        if ctn is None:
            return False

        try:
            with DataBase.Session() as db:
                return db.query(
                    db.query(Abonent).filter(Abonent.number == str(ctn)).exists()
                ).scalar()
        except Exception as e:
            print("Ошибка получения данных из БД:", e)
            raise


class Smartphone(Base):
    """
    Класс описания смартфона.
    """

    __tablename__ = "smartphones"

    id = Column(Integer)
    brand = Column(String)
    model = Column(String, primary_key=True)
    price = Column(Integer)

    @staticmethod
    def add_or_update(data):
        """
        Добавление/обновление смартфона в БД.

        :param p1: Набор данных.
        """
        print("Внесение данных смартфонов", data)

        try:
            with DataBase.Session() as db:
                query = db.query(Smartphone)

                for smartphoneData in data:
                    smartphoneModel = smartphoneData[2]

                    smartphone = query.filter(
                        Smartphone.model == smartphoneModel
                    ).first()
                    if smartphone is None:
                        smartphone = Smartphone(model=smartphoneModel)

                    smartphone.id = int(smartphoneData[0])
                    smartphone.brand = smartphoneData[1]
                    smartphone.price = int(smartphoneData[3])

                    db.add(smartphone)

                db.commit()
        except Exception as e:
            print("Ошибка добавления данных:", e)
            raise

    @staticmethod
    def get(id: int):
        """
        Получить смартфон.

        :param p1: ИД записи.
        :return: Информация по смартфону.
        """
        print("Получение данных смартфона", id)
        if id is None:
            return None

        try:
            with DataBase.Session() as db:
                return db.query(Smartphone).filter(Smartphone.id == id).first()
        except Exception as e:
            print("Ошибка получения данных:", e)
            raise


class Log(Base):
    """
    Класс описания таблицы логирования.
    """

    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    url = Column(String)
    ctn = Column(String)
    result = Column(String)
    log_timestamp = Column(DateTime)

    @staticmethod
    def add(url: str, ctn: str, result: str, log_timestamp: datetime):
        """
        Добавить запись в лог.

        :param p1: url.
        :param p2: Номер абонента.
        :param p3: Результат.
        :param p4: Дата.
        """
        print("Запись в лог", url, ctn, result)
        try:
            with DataBase.Session() as db:
                log = Log()
                log.url = url
                log.ctn = ctn
                log.result = result
                log.log_timestamp = log_timestamp

                db.add(log)
                db.commit()
        except Exception as e:
            print("Ошибка добавления данных:", e)
            raise