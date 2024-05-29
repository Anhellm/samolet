from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase
import os

pg_host = os.environ.get("PG_HOST", "")
pg_user = os.environ.get("POSTGRES_USER", "")
pg_passsword = os.environ.get("POSTGRES_PASSWORD", "")
pg_db = os.environ.get("POSTGRES_DB", "")


class Base(DeclarativeBase):
    pass


class DataBase:
    """
    Класс для описания БД.
    """

    engine = create_engine(f"postgresql://{pg_user}:{pg_passsword}@{pg_host}/{pg_db}")
    Session = sessionmaker(autoflush=False, bind=engine)

    @staticmethod
    def init():
        """
        Инициализация БД.
        """
        Base.metadata.create_all(bind=DataBase.engine)
