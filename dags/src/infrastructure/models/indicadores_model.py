
from sqlalchemy import Column, DateTime, Integer, ForeignKey, String
from datetime import datetime
from sqlalchemy.orm import relationship, sessionmaker

from src.config.db_config import Base


class IndicadoresModel(Base):
    __tablename__ = "indicador"

    id = Column(String, primary_key=True)
    id_dir  = Column(String, nullable=True)
    fecha_stata  = Column(String, nullable=True)
    precioventa  = Column(String, nullable=True)
    precioventa_  = Column(String, nullable=True)
    dias_faltantes  = Column(String, nullable=True)
    cod_prod  = Column(String, nullable=True)
    id_col  = Column(String, nullable=True)
    dprecioventa  = Column(String, nullable=True)
    dvarprecioventa  = Column(String, nullable=True)
    raro  = Column(String, nullable=True)
    raro2  = Column(String, nullable=True)
    ruc_prov  = Column(String, nullable=True)
    precioventa_may = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())
    
    
    # # Relación con Direcciones y productos
    # direccion = relationship("DireccionModel", back_populates="precios")
    # producto = relationship("ProductoModel", back_populates="precios")


