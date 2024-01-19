
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship
from src.infrastructure.models.price_referencia_model import PriceReferenciaModel
import re


class ProductoModel(Base):
    __tablename__ = "producto"

    id = Column(Integer, primary_key=True)
    nom_prod = Column(String, nullable=True)
    unidad = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    # Relación con Precio
    precios = relationship("PriceModel", back_populates="producto")
    precio_mayoristas = relationship("PricesMayoristasModel", back_populates="producto")
    precios_referencia = relationship("PriceReferenciaModel", back_populates="producto")
    relapasas = relationship("RelapasaModel", back_populates="producto")
