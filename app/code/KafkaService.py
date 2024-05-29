# -*- coding: utf-8 -*-
from confluent_kafka import Producer, Consumer
import os

kafka_host = os.environ.get("KAFKA_HOST")
kafka_port = os.environ.get("KAFKA_PORT")
topic = os.environ.get("KAFKA_TOPIC")

broker = f"{kafka_host}:{kafka_port}"


def send(data):
    """
    Отправка данных в Кафка.

    :param p1: Набор данных.
    """
    print("Отправка данных в Кафка", data)

    try:
        producer = Producer(
            {
                "bootstrap.servers": broker,
                "socket.timeout.ms": 100,
                "enable.idempotence": True,
            }
        )

        for row in data:
            producer.produce(topic, key=row[0], value=";".join(row))

        producer.flush()
    except Exception as e:
        print("Ошибка передачи данных:", e)
        raise


def recieve():
    """
    Получение данных из Кафка.

    :return: Список данных
    """
    print("Получение данных из Кафка")

    result = []
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": broker,
                "group.id": "FApi",
                "auto.offset.reset": "smallest",
            }
        )

        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(20)
            if msg is None:
                break

            consumer.commit(message=msg, asynchronous=False)
            in_kafka_message = msg.value().decode("utf-8")
            result.append(in_kafka_message.split(";"))
    except Exception as e:
        print("Ошибка получения данных:", e)
        raise
    finally:
        consumer.close()

    print("Получен набор данных из Кафка", result)
    return result