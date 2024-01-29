from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base


class MinoristaModel(Base):
    __tablename__ = "minorista"
    
    id = Column(String, primary_key=True)
    id_dir = Column(String, nullable=True)
    cod_prod = Column(String, nullable=True)
    fecha_stata = Column(String, nullable=True)
    precio_venta = Column(String, nullable=True)
    
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())