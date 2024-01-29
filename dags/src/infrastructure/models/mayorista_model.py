from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base


class MayoristaModel(Base):
    __tablename__ = "mayorista"

    id = Column(String, primary_key=True)
    precio_venta_may = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())