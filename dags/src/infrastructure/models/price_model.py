
from sqlalchemy import Column, DateTime, Integer, ForeignKey, String
from datetime import datetime
from sqlalchemy.orm import relationship, sessionmaker

from src.config.db_config import Base


class PriceModel(Base):
    __tablename__ = "precio"

    id = Column(Integer, primary_key=True, autoincrement=True)
    producto_id = Column(Integer, ForeignKey('producto.id'))
    direccion_id = Column(Integer, ForeignKey('direccion.id'))
    fecha_registro = Column(String, nullable=True)
    hora_registro = Column(String, nullable=True)
    precio = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    # Relación con Direcciones y productos
    direccion = relationship("DireccionModel", back_populates="precios")
    producto = relationship("ProductoModel", back_populates="precios")
