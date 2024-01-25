
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class PriceReferenciaModel(Base):
    __tablename__ = "precio_referencia"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_registro = Column(String, nullable=True)
    precio = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    producto_id = Column(Integer, nullable=True)
    
    # Relaciones
    # producto = relationship('ProductoModel', back_populates="precios_referencia")
