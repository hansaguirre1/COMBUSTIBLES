
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class ActividadModel(Base):
    __tablename__ = "actividad"

    id = Column(Integer, primary_key=True)
    actividad = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    # Relación con Direcciones
    # direcciones = relationship("DireccionModel", back_populates="actividad")
