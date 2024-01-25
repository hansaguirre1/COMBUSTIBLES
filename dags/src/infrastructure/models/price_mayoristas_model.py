
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship, sessionmaker


class PricesMayoristasModel(Base):
    __tablename__ = "precio_mayoristas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    precio_con_impuesto = Column(String, nullable=True)
    fecha = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    # producto_id = Column(Integer, ForeignKey('producto.id'))
    # planta_id = Column(Integer, ForeignKey('planta.id'))
    
    
    # producto = relationship("ProductoModel", back_populates="precio_mayoristas")
    # planta = relationship("PlantaModel", back_populates="precio_mayoristas")
    
    
