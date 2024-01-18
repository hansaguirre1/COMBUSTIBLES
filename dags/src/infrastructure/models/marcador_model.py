
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base


class MarcadorModel(Base):
    __tablename__ = "marcador"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tipo_cambio = Column(String, nullable=True)
    wti = Column(String, nullable=True)
    mont_belvieu = Column(String, nullable=True)
    fecha = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
