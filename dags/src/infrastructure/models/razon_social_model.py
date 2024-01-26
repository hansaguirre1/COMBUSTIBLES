
from sqlalchemy import Column, DateTime, Integer, String
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class RazonSocialModel(Base):
    __tablename__ = "razon_social"

    id = Column(Integer, primary_key=True)
    razon_social = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    updated_at = Column(DateTime(), default=datetime.now())
    
    # direcciones = relationship("DireccionModel", back_populates="razon_social")

    
