
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship, sessionmaker


class MayMinGeoModel(Base):
    __tablename__ = "may_min_geo"

    id = Column(String, primary_key=True)  
    lat = Column(String, nullable=True)
    lon = Column(String, nullable=True)
    id_dir = Column(String, nullable=True)
    cod_prod = Column(String, nullable=True)
    ruc_prov = Column(String, nullable=True)
    
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())
    # precio_mayoristas = relationship("PricesMayoristasModel", back_populates="planta")