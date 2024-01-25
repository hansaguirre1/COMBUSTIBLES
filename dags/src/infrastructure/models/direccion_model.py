
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime
from src.infrastructure.models.codigoosinerg_model import CodigoosinergModel
from src.infrastructure.models.producto_model import ProductoModel

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class DireccionModel(Base):
    __tablename__ = "direccion"

    id = Column(Integer, primary_key=True)
    direccion_name = Column(String, nullable=True)
    latitude = Column(String, nullable=True)
    longitude = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    
    razon_id = Column(Integer, nullable=True)
    actividad_id = Column(Integer, nullable=True)
    ubicacion_id = Column(Integer, nullable=True)
    codigoosinerg_id = Column(Integer, nullable=True)
    
    # Relaciones
    # razon_social = relationship("RazonSocialModel", back_populates="direcciones")
    # actividad = relationship("ActividadModel", back_populates="direcciones")
    # ubicacion = relationship("UbicacionModel", back_populates="direcciones")
    # codigoosinerg = relationship('CodigoosinergModel', back_populates="direcciones")
    # precios = relationship("PriceModel", back_populates="direccion")
    