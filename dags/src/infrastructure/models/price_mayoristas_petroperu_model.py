
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship, sessionmaker


class PricesMayoristasPetroperuModel(Base):
    __tablename__ = "precio_mayoristas_petroperu"

    id = Column(String, primary_key=True)
    precio_con_impuesto = Column(String, nullable=True)
    fecha = Column(String, nullable=True)
    
    producto_id = Column(String, nullable=True)
    planta_id = Column(String, nullable=True)
    
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())
    
    # producto = relationship("ProductoModel", back_populates="precio_mayoristas")
    # planta = relationship("PlantaModel", back_populates="precio_mayoristas")
    
    
