
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class PriceReferenciaModel(Base):
    __tablename__ = "precio_referencia"

    id = Column(String, primary_key=True)
    fecha_registro = Column(String, nullable=True)
    precio = Column(String, nullable=True)
    producto_id = Column(String, nullable=True)
    
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())
    
    
    # Relaciones
    # producto = relationship('ProductoModel', back_populates="precios_referencia")
