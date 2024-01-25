
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class UbicacionModel(Base):
    __tablename__ = "ubicacion"

    id = Column(Integer, primary_key=True)
    departamento = Column(String, nullable=True)
    provincia = Column(String, nullable=True)
    distrito = Column(String, nullable=True)
    ubigeo = Column(String, nullable=True)
    dpd = Column(String, nullable=True)
    ubi = Column(String, nullable=True)
    p_urban = Column(String, nullable=True)
    rural = Column(String, nullable=True)
    capital = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    # Relación con Direcciones
    # direcciones = relationship("DireccionModel", back_populates="ubicacion")
