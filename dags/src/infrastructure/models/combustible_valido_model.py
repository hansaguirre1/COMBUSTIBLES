
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class CombustibleValidoModel(Base):
    __tablename__ = "combustible_valido"

    id = Column(String, primary_key=True)
    id_cv = Column(String, nullable=True)
    anio = Column(String, nullable=True)
    departamento = Column(String, nullable=True)
    ok = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    producto_id = Column(Integer, ForeignKey('producto.id'))
    
    # Relaciones
    producto = relationship('ProductoModel', back_populates="combustibles_validos")
