
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship, sessionmaker


class PlantaModel(Base):
    __tablename__ = "planta"

    id = Column(Integer, primary_key=True, autoincrement=True)
    planta = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    precio_mayoristas = relationship("PricesMayoristasModel", back_populates="planta")
