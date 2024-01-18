from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from datetime import datetime

from src.config.db_config import Base
from sqlalchemy.orm import relationship


class RelapasaModel(Base):
    __tablename__ = "relapasa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_registro = Column(String, nullable=True)
    precio = Column(String, nullable=True)
    created_at = Column(DateTime(), default=datetime.now())
    producto_id = Column(Integer, ForeignKey('producto.id'))
    
    # Relaciones
    producto = relationship('ProductoModel', back_populates="relapasas")
