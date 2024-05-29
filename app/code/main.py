import datetime
from fastapi import FastAPI, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
import Parser
import CSV_Service
import BinReader
import DataBase.Models as db
import DataBase.Engine
import KafkaService
import SparkService
from datetime import date, datetime

filedir = "/files/"
phonedata = filedir + "phonedata.csv"
blacklist = filedir + "blacklist.csv"

app = FastAPI()
DataBase.Engine.DataBase.init()


@app.get("/")
def read_root():
    html_content = """
    <h1><u><strong>Запросы:</strong></u></h1>
    <p><em><strong>GetAnnouncements</strong></em> - Чтение данных сайта и занесение в csv.</p>
    <p><em><strong>UpdatePhoneData</strong></em> - Обработка csv в Spark, отправка данных в hdfs, БД.</p>
    <p><em><strong>GetBlackList</strong></em> - Чтение бинарных данных и занесение в csv.</p>
    <p><em><strong>UpdateBlackList</strong></em> - Отправка данных из csv в Kafka, чтение из Kafka и занесение в БД.</p>
    <p><em><strong>Decide</strong></em> - Принятие решения.</p>
    """
    return HTMLResponse(content=html_content)


@app.post("/GetAnnouncements")
def get_phone_info():
    try:
        phones = Parser.get_phones()
        CSV_Service.save_to_csv(phones, phonedata)
    except Exception as e:
        return {"message": "Ошибка обработки запроса"}


@app.post("/UpdatePhoneData")
def update_phone_info():
    try:
        df = SparkService.read(phonedata)
        SparkService.send_to_hdfs(df)
        SparkService.send_to_db(df, db.Smartphone.add_or_update)
    except Exception as e:
        return {"message": "Ошибка обработки запроса"}


@app.post("/GetBlackList")
def get_blacklist():
    try:
        bin_data = BinReader.parse_bin("data.bin")
        CSV_Service.save_to_csv(bin_data, blacklist)
    except Exception as e:
        return {"message": "Ошибка обработки запроса"}


@app.post("/UpdateBlackList")
def update_blacklist():
    try:
        csv_data = CSV_Service.read_from_csv(blacklist)

        KafkaService.send(csv_data)
        kafka_data = KafkaService.recieve()

        db.Abonent.add_or_update(kafka_data)
    except Exception as e:
        return {"message": "Ошибка обработки запроса"}


@app.post("/Decide")
def decide(
    request: Request,
    ctn: int = Body(embed=True),
    inn: int = Body(embed=True),
    smartphoneID: int = Body(embed=True),
    income: int = Body(embed=True),
    creationDate: date = Body(embed=True),
):
    result = get_decision(ctn, smartphoneID, income)
    try:
        db.Log.add(str(request.url_for("decide")), ctn, result["result"], datetime.now())
    except Exception as e:
        print("Ошибка записи в лог", e)

    json_data = jsonable_encoder(result)
    return JSONResponse(content=json_data)


def get_decision(ctn: int, smartphoneID: int, income: int):
    """
    Принять решение об одобрении или отказе в рассрочке .

    :param p1: Номер телефона клиента.
    :param p2: Код желаемого смартфона.
    :param p3: Средний ежемесячный доход клиента.
    :return: Результат принятия решения (словарь).
    """

    result = {"result": "Отказ", "reason": ""}
    try:
        if db.Abonent.is_in_blacklist(ctn):
            result["reason"] = "Клиент находится в черном списке"
            return result

        smartphone = db.Smartphone.get(smartphoneID)
        if smartphone is None:
            result["reason"] = "Не найдена модель телефона"
            return result

        payment = smartphone.price / 12
        if payment > income / 5:
            result["reason"] = "Клиент не имеет достаточный уровень дохода"
            return result

        del result["reason"]
        result["result"] = "Одобрение"
    except Exception as e:
        print("Ошибка обработки решения", e)
        result["reason"] = "Ошибка обработки"

    return result