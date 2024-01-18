import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, orm, Table
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager, AbstractContextManager
from typing import Callable
import logging


load_dotenv()

logger = logging.getLogger(__name__)

host = os.getenv('HOST_NAME')
port = os.getenv('PORT')
database = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

database_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

Base = declarative_base()

class Database:

    def __init__(self) -> None:
        self._engine = create_engine(database_url, echo=False)
        self._session_factory = orm.scoped_session(
            orm.sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine,
            ),
        )
        
    def create_database(self) -> None:
        Base.metadata.create_all(self._engine, checkfirst=True)
    
    def get_engine(self):
        return self._engine

    @contextmanager
    def session(self) -> Callable[..., AbstractContextManager[Session]]:
        session: Session = self._session_factory()
        try:
            yield session
        except Exception:
            logger.exception("***Session rollback because of exception**")
            session.rollback()
            raise
        finally:
            session.close()